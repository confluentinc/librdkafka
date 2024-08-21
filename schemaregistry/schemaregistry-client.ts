import { RestService, ClientConfig } from './rest-service';
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

interface Client {
  register(subject: string, schema: SchemaInfo, normalize: boolean): Promise<number>;
  registerFullResponse(subject: string, schema: SchemaInfo, normalize: boolean): Promise<SchemaMetadata>;
  getBySubjectAndId(subject: string, id: number): Promise<SchemaInfo>;
  getId(subject: string, schema: SchemaInfo, normalize: boolean): Promise<number>;
  getLatestSchemaMetadata(subject: string): Promise<SchemaMetadata>;
  getSchemaMetadata(subject: string, version: number, deleted: boolean): Promise<SchemaMetadata>;
  getLatestWithMetadata(subject: string, metadata: { [key: string]: string }, deleted: boolean): Promise<SchemaMetadata>;
  getAllVersions(subject: string): Promise<number[]>;
  getVersion(subject: string, schema: SchemaInfo, normalize: boolean): Promise<number>;
  getAllSubjects(): Promise<string[]>;
  deleteSubject(subject: string, permanent: boolean): Promise<number[]>;
  deleteSubjectVersion(subject: string, version: number, permanent: boolean): Promise<number>;
  testSubjectCompatibility(subject: string, schema: SchemaInfo): Promise<boolean>;
  testCompatibility(subject: string, version: number, schema: SchemaInfo): Promise<boolean>;
  getCompatibility(subject: string): Promise<Compatibility>;
  updateCompatibility(subject: string, update: Compatibility): Promise<Compatibility>;
  getDefaultCompatibility(): Promise<Compatibility>;
  updateDefaultCompatibility(update: Compatibility): Promise<Compatibility>;
  getConfig(subject: string): Promise<ServerConfig>;
  updateConfig(subject: string, update: ServerConfig): Promise<ServerConfig>;
  getDefaultConfig(): Promise<ServerConfig>;
  updateDefaultConfig(update: ServerConfig): Promise<ServerConfig>;
  close(): void;
}

class SchemaRegistryClient implements Client {
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

  constructor(config: ClientConfig) {
    const cacheOptions = {
      max: config.cacheCapacity,
      ...(config.cacheLatestTtlSecs !== undefined && { maxAge: config.cacheLatestTtlSecs * 1000 })
    };

    this.restService = new RestService(config.createAxiosDefaults, config.baseURLs, config.isForward);  

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

      subject = encodeURIComponent(subject);

      const response: AxiosResponse<SchemaMetadata> = await this.restService.handleRequest(
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

      subject = encodeURIComponent(subject);

      const response: AxiosResponse<SchemaInfo> = await this.restService.handleRequest(
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

      subject = encodeURIComponent(subject);

      const response: AxiosResponse<SchemaMetadata> = await this.restService.handleRequest(
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

      subject = encodeURIComponent(subject);

      const response: AxiosResponse<SchemaMetadata> = await this.restService.handleRequest(
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

      subject = encodeURIComponent(subject);

      const response: AxiosResponse<SchemaMetadata> = await this.restService.handleRequest(
        `/subjects/${subject}/versions/${version}?deleted=${deleted}`,
        'GET'
      );
      this.versionToSchemaCache.set(cacheKey, response.data);
      return response.data;
    });
  }

  public async getLatestWithMetadata(subject: string, metadata: { [key: string]: string }, deleted: boolean = false): Promise<SchemaMetadata> {
    const cacheKey = stringify({ subject, metadata, deleted });

    return await this.metadataToSchemaMutex.runExclusive(async () => {
      const cachedSchemaMetadata: SchemaMetadata | undefined = this.metadataToSchemaCache.get(cacheKey);
      if (cachedSchemaMetadata) {
        return cachedSchemaMetadata;
      }

      subject = encodeURIComponent(subject);

      let metadataStr = '';

      for (const key in metadata) {
        const encodedKey = encodeURIComponent(key);
        const encodedValue = encodeURIComponent(metadata[key]);
        metadataStr += `&key=${encodedKey}&value=${encodedValue}`;
      }

      const response: AxiosResponse<SchemaMetadata> = await this.restService.handleRequest(
        `/subjects/${subject}/metadata?deleted=${deleted}&${metadataStr}`,
        'GET'
      );
      this.metadataToSchemaCache.set(cacheKey, response.data);
      return response.data;
    });
  }


  public async getAllVersions(subject: string): Promise<number[]> {
    const response: AxiosResponse<number[]> = await this.restService.handleRequest(
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

      subject = encodeURIComponent(subject);

      const response: AxiosResponse<SchemaMetadata> = await this.restService.handleRequest(
        `/subjects/${subject}?normalize=${normalize}`,
        'POST',
        schema
      );
      this.schemaToVersionCache.set(cacheKey, response.data.version);
      return response.data.version!;
    });
  }

  public async getAllSubjects(): Promise<string[]> {
    const response: AxiosResponse<string[]> = await this.restService.handleRequest(
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

    subject = encodeURIComponent(subject);

    const response: AxiosResponse<number[]> = await this.restService.handleRequest(
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

      subject = encodeURIComponent(subject);

      const response: AxiosResponse<number> = await this.restService.handleRequest(
        `/subjects/${subject}/versions/${version}?permanent=${permanent}`,
        'DELETE'
      );
      return response.data;
    });
  }

  public async testSubjectCompatibility(subject: string, schema: SchemaInfo): Promise<boolean> {
    subject = encodeURIComponent(subject);

    const response: AxiosResponse<isCompatibleResponse> = await this.restService.handleRequest(
      `/compatibility/subjects/${subject}/versions/latest`,
      'POST',
      schema
    );
    return response.data.is_compatible;
  }

  public async testCompatibility(subject: string, version: number, schema: SchemaInfo): Promise<boolean> {
    subject = encodeURIComponent(subject);

    const response: AxiosResponse<isCompatibleResponse> = await this.restService.handleRequest(
      `/compatibility/subjects/${subject}/versions/${version}`,
      'POST',
      schema
    );
    return response.data.is_compatible;
  }

  public async getCompatibility(subject: string): Promise<Compatibility> {
    subject = encodeURIComponent(subject);

    const response: AxiosResponse<CompatibilityLevel> = await this.restService.handleRequest(
      `/config/${subject}`,
      'GET'
    );
    return response.data.compatibilityLevel!;
  }

  public async updateCompatibility(subject: string, update: Compatibility): Promise<Compatibility> {
    subject = encodeURIComponent(subject);

    const response: AxiosResponse<CompatibilityLevel> = await this.restService.handleRequest(
      `/config/${subject}`,
      'PUT',
      { compatibility: update }
    );
    return response.data.compatibility!;
  }

  public async getDefaultCompatibility(): Promise<Compatibility> {
    const response: AxiosResponse<CompatibilityLevel> = await this.restService.handleRequest(
      `/config`,
      'GET'
    );
    return response.data.compatibilityLevel!;
  }

  public async updateDefaultCompatibility(update: Compatibility): Promise<Compatibility> {
    const response: AxiosResponse<CompatibilityLevel> = await this.restService.handleRequest(
      `/config`,
      'PUT',
      { compatibility: update }
    );
    return response.data.compatibility!;
  }

  public async getConfig(subject: string): Promise<ServerConfig> {
    subject = encodeURIComponent(subject);

    const response: AxiosResponse<ServerConfig> = await this.restService.handleRequest(
      `/config/${subject}`,
      'GET'
    );
    return response.data;
  }

  public async updateConfig(subject: string, update: ServerConfig): Promise<ServerConfig> {
    const response: AxiosResponse<ServerConfig> = await this.restService.handleRequest(
      `/config/${subject}`,
      'PUT',
      update
    );
    return response.data;
  }

  public async getDefaultConfig(): Promise<ServerConfig> {
    const response: AxiosResponse<ServerConfig> = await this.restService.handleRequest(
      `/config`,
      'GET'
    );
    return response.data;
  }

  public async updateDefaultConfig(update: ServerConfig): Promise<ServerConfig> {
    const response: AxiosResponse<ServerConfig> = await this.restService.handleRequest(
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
  Client, SchemaRegistryClient, SchemaInfo, Metadata, Compatibility,
  CompatibilityLevel, ServerConfig, RuleSet, Rule, Reference, SchemaMetadata
};
