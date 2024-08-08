import { RestService } from './rest-service';
import { AxiosResponse } from 'axios';
import stringify from "json-stringify-deterministic";
import { LRUCache } from 'lru-cache';
import { Mutex } from 'async-mutex';

/*
 * Confluent-Schema-Registry-TypeScript - Node.js wrapper for Confluent Schema Registry
 *
 * Copyright (c) 2024 Confluent, Inc.
 *
 * This software may be modified and distributed under the terms
 * of the MIT license.  See the LICENSE.txt file for details.
 */

enum Compatibility {
  None = "NONE",
  Backward = "BACKWARD",
  Forward = "FORWARD",
  Full = "FULL",
  BackwardTransitive = "BACKWARD_TRANSITIVE",
  ForwardTransitive = "FORWARD_TRANSITIVE",
  FullTransitive = "FULL_TRANSITIVE"
}

interface CompatibilityLevel {
  compatibility?: Compatibility;
  compatibilityLevel?: Compatibility;
}

interface Result<T> {
  data?: T;
  error?: Error;
}

interface Rule {
  name: string;
  subject: string;
  version: number;
}

interface SchemaInfo {
  schema?: string;
  schemaType?: string;
  references?: Reference[];
  metadata?: Metadata;
  ruleSet?: RuleSet;
}

interface SchemaMetadata extends SchemaInfo {
  id: number;
  subject?: string;
  version?: number;
}

interface Reference {
  Name: string;
  Subject: string;
  Version: number;
}

interface Metadata {
  tags?: { [key: string]: string[] };
  properties?: { [key: string]: string };
  sensitive?: string[];
}

interface RuleSet {
  migrationRules: Rule[];
  compatibilityRules: Rule[];
}

interface ServerConfig {
  alias?: string;
  normalize?: boolean;
  compatibility?: Compatibility;
  compatibilityLevel?: Compatibility;
  compatibilityGroup?: string;
  defaultMetadata?: Metadata;
  overrideMetadata?: Metadata;
  defaultRuleSet?: RuleSet;
  overrideRuleSet?: RuleSet;
}

interface isCompatibleResponse {
  is_compatible: boolean;
}

class SchemaRegistryClient {
  private restService: RestService;

  private schemaToIdCache: LRUCache<string, number>;
  private idToSchemaInfoCache: LRUCache<string, SchemaInfo>;
  private infoToSchemaCache: LRUCache<string, SchemaMetadata>;
  private latestToSchemaCache: LRUCache<string, SchemaMetadata>;
  private schemaToVersionCache: LRUCache<string, number>;
  private versionToSchemaCache: LRUCache<string, SchemaMetadata>;
  private metadataToSchemaCache: LRUCache<string, SchemaMetadata>;

  private schemaToIdMutex: Mutex;
  private idToSchemaInfoMutex: Mutex;
  private infoToSchemaMutex: Mutex;
  private latestToSchemaMutex: Mutex;
  private schemaToVersionMutex: Mutex;
  private versionToSchemaMutex: Mutex;
  private metadataToSchemaMutex: Mutex;

  constructor(restService: RestService, cacheSize: number = 512, cacheTTL?: number) {
    const cacheOptions = {
      max: cacheSize,
      ...(cacheTTL !== undefined && { maxAge: cacheTTL })
    };

    this.restService = restService;
    this.schemaToIdCache = new LRUCache(cacheOptions);
    this.idToSchemaInfoCache = new LRUCache(cacheOptions);
    this.infoToSchemaCache = new LRUCache(cacheOptions);
    this.latestToSchemaCache = new LRUCache(cacheOptions);
    this.schemaToVersionCache = new LRUCache(cacheOptions);
    this.versionToSchemaCache = new LRUCache(cacheOptions);
    this.metadataToSchemaCache = new LRUCache(cacheOptions);
    this.schemaToIdMutex = new Mutex();
    this.idToSchemaInfoMutex = new Mutex();
    this.infoToSchemaMutex = new Mutex();
    this.latestToSchemaMutex = new Mutex();
    this.schemaToVersionMutex = new Mutex();
    this.versionToSchemaMutex = new Mutex();
    this.metadataToSchemaMutex = new Mutex();
  }

  public async register(subject: string, schema: SchemaInfo, normalize: boolean = false): Promise<number> {
    const metadataResult = await this.registerFullResponse(subject, schema, normalize);

    return metadataResult.id;
  }

  public async registerFullResponse(subject: string, schema: SchemaInfo, normalize: boolean = false): Promise<SchemaMetadata> {
    const cacheKey = stringify({ subject, schema });

    return await this.infoToSchemaMutex.runExclusive(async () => {
      const cachedSchemaMetadata: SchemaMetadata | undefined = this.infoToSchemaCache.get(cacheKey);
      if (cachedSchemaMetadata) {
        return cachedSchemaMetadata;
      }

      const response: AxiosResponse<SchemaMetadata> = await this.restService.sendHttpRequest(
        `/subjects/${subject}/versions?normalize=${normalize}`,
        'POST',
        schema
      );
      this.infoToSchemaCache.set(cacheKey, response.data);
      return response.data;
    });
  }

  public async getBySubjectAndId(subject: string, id: number): Promise<SchemaInfo> {
    const cacheKey = stringify({ subject, id });
    return await this.idToSchemaInfoMutex.runExclusive(async () => {
      const cachedSchema: SchemaInfo | undefined = this.idToSchemaInfoCache.get(cacheKey);
      if (cachedSchema) {
        return cachedSchema;
      }

      const response: AxiosResponse<SchemaInfo> = await this.restService.sendHttpRequest(
        `/schemas/ids/${id}?subject=${subject}`,
        'GET'
      );
      this.idToSchemaInfoCache.set(cacheKey, response.data);
      return response.data;
    });
  }

  public async getId(subject: string, schema: SchemaInfo, normalize: boolean = false): Promise<number> {
    const cacheKey = stringify({ subject, schema });

    return await this.schemaToIdMutex.runExclusive(async () => {
      const cachedId: number | undefined = this.schemaToIdCache.get(cacheKey);
      if (cachedId) {
        return cachedId;
      }

      const response: AxiosResponse<SchemaMetadata> = await this.restService.sendHttpRequest(
        `/subjects/${subject}?normalize=${normalize}`,
        'POST',
        schema
      );
      this.schemaToIdCache.set(cacheKey, response.data.id);
      return response.data.id;
    });
  }

  public async getLatestSchemaMetadata(subject: string): Promise<SchemaMetadata> {
    return await this.latestToSchemaMutex.runExclusive(async () => {
      const cachedSchema: SchemaMetadata | undefined = this.latestToSchemaCache.get(subject);
      if (cachedSchema) {
        return cachedSchema;
      }

      const response: AxiosResponse<SchemaMetadata> = await this.restService.sendHttpRequest(
        `/subjects/${subject}/versions/latest`,
        'GET'
      );
      this.latestToSchemaCache.set(subject, response.data);
      return response.data;
    });
  }

  public async getSchemaMetadata(subject: string, version: number, deleted: boolean = false): Promise<SchemaMetadata> {
    const cacheKey = stringify({ subject, version, deleted });

    return await this.versionToSchemaMutex.runExclusive(async () => {
      const cachedSchemaMetadata: SchemaMetadata | undefined = this.versionToSchemaCache.get(cacheKey);
      if (cachedSchemaMetadata) {
        return cachedSchemaMetadata;
      }

      const response: AxiosResponse<SchemaMetadata> = await this.restService.sendHttpRequest(
        `/subjects/${subject}/versions/${version}?deleted=${deleted}`,
        'GET'
      );
      this.versionToSchemaCache.set(cacheKey, response.data);
      return response.data;
    });
  }

  private convertToQueryParams(metadata: Metadata): string {
    const params = new URLSearchParams();

    if (metadata.tags) {
      for (const [key, values] of Object.entries(metadata.tags)) {
        values.forEach((value, index) => {
          params.append(`tags.${key}[${index}]`, value);
        });
      }
    }

    if (metadata.properties) {
      for (const [key, value] of Object.entries(metadata.properties)) {
        params.append(`properties.${key}`, value);
      }
    }

    if (metadata.sensitive) {
      metadata.sensitive.forEach((value, index) => {
        params.append(`sensitive[${index}]`, value);
      });
    }

    return params.toString();
  }

  //TODO: Get clarification with getLatestWithMetadata
  public async getLatestWithMetadata(subject: string, metadata: Metadata, deleted: boolean = false): Promise<SchemaMetadata> {
    const cacheKey = stringify({ subject, metadata, deleted });

    return await this.metadataToSchemaMutex.runExclusive(async () => {
      const cachedSchemaMetadata: SchemaMetadata | undefined = this.metadataToSchemaCache.get(cacheKey);
      if (cachedSchemaMetadata) {
        return cachedSchemaMetadata;
      }

      const queryParams = this.convertToQueryParams(metadata);

      const response: AxiosResponse<SchemaMetadata> = await this.restService.sendHttpRequest(
        `/subjects/${subject}/metadata?deleted=${deleted}&${queryParams}`,
        'GET'
      );
      this.metadataToSchemaCache.set(cacheKey, response.data);
      return response.data;
    });
  }


  public async getAllVersions(subject: string): Promise<number[]> {
    const response: AxiosResponse<number[]> = await this.restService.sendHttpRequest(
      `/subjects/${subject}/versions`,
      'GET'
    );
    return response.data;
  }

  public async getVersion(subject: string, schema: SchemaInfo, normalize: boolean = false): Promise<number> {
    const cacheKey = stringify({ subject, schema });

    return await this.schemaToVersionMutex.runExclusive(async () => {
      const cachedVersion: number | undefined = this.schemaToVersionCache.get(cacheKey);
      if (cachedVersion) {
        return cachedVersion;
      }

      const response: AxiosResponse<SchemaMetadata> = await this.restService.sendHttpRequest(
        `/subjects/${subject}?normalize=${normalize}`,
        'POST',
        schema
      );
      this.schemaToVersionCache.set(cacheKey, response.data.version);
      return response.data.version!;
    });
  }

  public async getAllSubjects(): Promise<string[]> {
    const response: AxiosResponse<string[]> = await this.restService.sendHttpRequest(
      `/subjects`,
      'GET'
    );
    return response.data;
  }

  public async deleteSubject(subject: string, permanent: boolean = false): Promise<number[]> {
    await this.infoToSchemaMutex.runExclusive(async () => {
      this.infoToSchemaCache.forEach((_, key) => {
        const parsedKey = JSON.parse(key);
        if (parsedKey.subject === subject) {
          this.infoToSchemaCache.delete(key);
        }
      });
    });

    await this.schemaToVersionMutex.runExclusive(async () => {
      this.schemaToVersionCache.forEach((_, key) => {
        const parsedKey = JSON.parse(key);
        if (parsedKey.subject === subject) {
          this.schemaToVersionCache.delete(key);
        }
      });
    });

    await this.versionToSchemaMutex.runExclusive(async () => {
      this.versionToSchemaCache.forEach((_, key) => {
        const parsedKey = JSON.parse(key);
        if (parsedKey.subject === subject) {
          this.versionToSchemaCache.delete(key);
        }
      });
    });

    await this.idToSchemaInfoMutex.runExclusive(async () => {
      this.idToSchemaInfoCache.forEach((_, key) => {
        const parsedKey = JSON.parse(key);
        if (parsedKey.subject === subject) {
          this.idToSchemaInfoCache.delete(key);
        }
      });
    });

    const response: AxiosResponse<number[]> = await this.restService.sendHttpRequest(
      `/subjects/${subject}?permanent=${permanent}`,
      'DELETE'
    );
    return response.data;
  }

  public async deleteSubjectVersion(subject: string, version: number, permanent: boolean = false): Promise<number> {
    return await this.schemaToVersionMutex.runExclusive(async () => {
      let metadataValue: SchemaMetadata | undefined;

      this.schemaToVersionCache.forEach((value, key) => {
        const parsedKey = JSON.parse(key);
        if (parsedKey.subject === subject && value === version) {
          this.schemaToVersionCache.delete(key);
          const infoToSchemaCacheKey = stringify({ subject: subject, schema: parsedKey.schema });

          this.infoToSchemaMutex.runExclusive(async () => {
            metadataValue = this.infoToSchemaCache.get(infoToSchemaCacheKey);
            if (metadataValue) {
              this.infoToSchemaCache.delete(infoToSchemaCacheKey);
              const cacheKeyID = stringify({ subject: subject, id: metadataValue.id });

              this.idToSchemaInfoMutex.runExclusive(async () => {
                this.idToSchemaInfoCache.delete(cacheKeyID);
              });
            }
          });
        }
      });

      const cacheKey = stringify({ subject: subject, version: version });
      this.versionToSchemaMutex.runExclusive(async () => {
        this.versionToSchemaCache.delete(cacheKey);
      });

      const response: AxiosResponse<number> = await this.restService.sendHttpRequest(
        `/subjects/${subject}/versions/${version}?permanent=${permanent}`,
        'DELETE'
      );
      return response.data;
    });
  }

  public async testSubjectCompatibility(subject: string, schema: SchemaInfo): Promise<boolean> {
    const response: AxiosResponse<isCompatibleResponse> = await this.restService.sendHttpRequest(
      `/compatibility/subjects/${subject}/versions/latest`,
      'POST',
      schema
    );
    return response.data.is_compatible;
  }

  public async testCompatibility(subject: string, version: number, schema: SchemaInfo): Promise<boolean> {
    const response: AxiosResponse<isCompatibleResponse> = await this.restService.sendHttpRequest(
      `/compatibility/subjects/${subject}/versions/${version}`,
      'POST',
      schema
    );
    return response.data.is_compatible;
  }

  public async getCompatibility(subject: string): Promise<Compatibility> {
    const response: AxiosResponse<CompatibilityLevel> = await this.restService.sendHttpRequest(
      `/config/${subject}`,
      'GET'
    );
    return response.data.compatibilityLevel!;
  }

  public async updateCompatibility(subject: string, update: Compatibility): Promise<Compatibility> {
    const response: AxiosResponse<CompatibilityLevel> = await this.restService.sendHttpRequest(
      `/config/${subject}`,
      'PUT',
      { compatibility: update }
    );
    return response.data.compatibility!;
  }

  public async getDefaultCompatibility(): Promise<Compatibility> {
      const response: AxiosResponse<CompatibilityLevel> = await this.restService.sendHttpRequest(
        `/config`,
        'GET'
      );
      return response.data.compatibilityLevel!;
  }

  public async updateDefaultCompatibility(update: Compatibility): Promise<Compatibility> {
      const response: AxiosResponse<CompatibilityLevel> = await this.restService.sendHttpRequest(
        `/config`,
        'PUT',
        { compatibility: update }
      );
      return response.data.compatibility!;
  }

  public async getConfig(subject: string): Promise<ServerConfig> {
      const response: AxiosResponse<ServerConfig> = await this.restService.sendHttpRequest(
        `/config/${subject}`,
        'GET'
      );
      return response.data;
  }

  public async updateConfig(subject: string, update: ServerConfig): Promise<ServerConfig> {
      const response: AxiosResponse<ServerConfig> = await this.restService.sendHttpRequest(
        `/config/${subject}`,
        'PUT',
        update
      );
      return response.data;
  }

  public async getDefaultConfig(): Promise<ServerConfig> {
      const response: AxiosResponse<ServerConfig> = await this.restService.sendHttpRequest(
        `/config`,
        'GET'
      );
      return response.data;
  }

  public async updateDefaultConfig(update: ServerConfig): Promise<ServerConfig> {
      const response: AxiosResponse<ServerConfig> = await this.restService.sendHttpRequest(
        `/config`,
        'PUT',
        update
      );
      return response.data;
  }

  public close(): void {
    this.infoToSchemaCache.clear();
    this.schemaToVersionCache.clear();
    this.versionToSchemaCache.clear();
    this.idToSchemaInfoCache.clear();

    return;
  }

  // Cache methods for testing
  public async addToInfoToSchemaCache(subject: string, schema: SchemaInfo, metadata: SchemaMetadata): Promise<void> {
    const cacheKey = stringify({ subject, schema });
    await this.infoToSchemaMutex.runExclusive(async () => {
      this.infoToSchemaCache.set(cacheKey, metadata);
    });
  }

  public async addToSchemaToVersionCache(subject: string, schema: SchemaInfo, version: number): Promise<void> {
    const cacheKey = stringify({ subject, schema });
    await this.schemaToVersionMutex.runExclusive(async () => {
      this.schemaToVersionCache.set(cacheKey, version);
    });
  }

  public async addToVersionToSchemaCache(subject: string, version: number, metadata: SchemaMetadata): Promise<void> {
    const cacheKey = stringify({ subject, version });
    await this.versionToSchemaMutex.runExclusive(async () => {
      this.versionToSchemaCache.set(cacheKey, metadata);
    });
  }

  public async addToIdToSchemaInfoCache(subject: string, id: number, schema: SchemaInfo): Promise<void> {
    const cacheKey = stringify({ subject, id });
    await this.idToSchemaInfoMutex.runExclusive(async () => {
      this.idToSchemaInfoCache.set(cacheKey, schema);
    });
  }

  public async getInfoToSchemaCacheSize(): Promise<number> {
    return await this.infoToSchemaMutex.runExclusive(async () => {
      return this.infoToSchemaCache.size;
    });
  }

  public async getSchemaToVersionCacheSize(): Promise<number> {
    return await this.schemaToVersionMutex.runExclusive(async () => {
      return this.schemaToVersionCache.size;
    });
  }

  public async getVersionToSchemaCacheSize(): Promise<number> {
    return await this.versionToSchemaMutex.runExclusive(async () => {
      return this.versionToSchemaCache.size;
    });
  }

  public async getIdToSchemaInfoCacheSize(): Promise<number> {
    return await this.idToSchemaInfoMutex.runExclusive(async () => {
      return this.idToSchemaInfoCache.size;
    });
  }

}

export {
  SchemaRegistryClient, SchemaInfo, Metadata, Compatibility,
  CompatibilityLevel, ServerConfig, RuleSet, Rule, Reference, SchemaMetadata, Result
};
