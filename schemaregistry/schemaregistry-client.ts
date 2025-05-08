import { RestService, ClientConfig } from './rest-service';
import { AxiosResponse } from 'axios';
import stringify from "json-stringify-deterministic";
import { LRUCache } from 'lru-cache';
import { Mutex } from 'async-mutex';
import { MockClient } from "./mock-schemaregistry-client";

/*
 * Confluent-Schema-Registry-TypeScript - Node.js wrapper for Confluent Schema Registry
 *
 * Copyright (c) 2024 Confluent, Inc.
 *
 * This software may be modified and distributed under the terms
 * of the MIT license.  See the LICENSE.txt file for details.
 */

export enum Compatibility {
  NONE = "NONE",
  BACKWARD = "BACKWARD",
  FORWARD = "FORWARD",
  FULL = "FULL",
  BACKWARD_TRANSITIVE = "BACKWARD_TRANSITIVE",
  FORWARD_TRANSITIVE = "FORWARD_TRANSITIVE",
  FULL_TRANSITIVE = "FULL_TRANSITIVE"
}

export interface CompatibilityLevel {
  compatibility?: Compatibility;
  compatibilityLevel?: Compatibility;
}

/**
 * Rule represents a data contract rule
 */
export interface Rule {
  name: string
  doc?: string
  kind?: string
  mode?: RuleMode
  type: string
  tags?: string[]
  params?: { [key: string]: string }
  expr?: string
  onSuccess?: string
  onFailure?: string
  disabled?: boolean
}

export enum RuleMode {
  UPGRADE = 'UPGRADE',
  DOWNGRADE = 'DOWNGRADE',
  UPDOWN = 'UPDOWN',
  WRITE = 'WRITE',
  READ = 'READ',
  WRITEREAD = 'WRITEREAD',
}

/**
 * SchemaInfo represents a schema and its associated information
 */
export interface SchemaInfo {
  schema: string;
  schemaType?: string;
  references?: Reference[];
  metadata?: Metadata;
  ruleSet?: RuleSet;
}

// Ensure that SchemaMetadata fields are removed from the SchemaInfo
export function minimize(info: SchemaInfo): SchemaInfo {
  return {
    schemaType: info.schemaType,
    schema: info.schema,
    references: info.references,
    metadata: info.metadata,
    ruleSet: info.ruleSet
  }
}

/**
 * SchemaMetadata extends SchemaInfo with additional metadata
 */
export interface SchemaMetadata extends SchemaInfo {
  id?: number;
  guid?: string;
  subject?: string;
  version?: number;
}

/**
 * Reference represents a schema reference
 */
export interface Reference {
  name: string;
  subject: string;
  version: number;
}

/**
 * Metadata represents user-defined metadata
 */
export interface Metadata {
  tags?: { [key: string]: string[] };
  properties?: { [key: string]: string };
  sensitive?: string[];
}

/**
 * RuleSet represents a data contract rule set
 */
export interface RuleSet {
  migrationRules?: Rule[];
  domainRules?: Rule[];
}

/**
 * ServerConfig represents config params for Schema Registry
 */
export interface ServerConfig {
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

export interface isCompatibleResponse {
  is_compatible: boolean;
}

/**
 * Client is an interface for clients interacting with the Confluent Schema Registry.
 * The Schema Registry's REST interface is further explained in Confluent's Schema Registry API documentation
 * https://github.com/confluentinc/schema-registry/blob/master/client/src/main/java/io/confluent/kafka/schemaregistry/client/SchemaRegistryClient.java
 */
export interface Client {
  config(): ClientConfig;
  register(subject: string, schema: SchemaInfo, normalize: boolean): Promise<number>;
  registerFullResponse(subject: string, schema: SchemaInfo, normalize: boolean): Promise<SchemaMetadata>;
  getBySubjectAndId(subject: string, id: number, format?: string): Promise<SchemaInfo>;
  getByGuid(guid: string, format?: string): Promise<SchemaInfo>;
  getId(subject: string, schema: SchemaInfo, normalize: boolean): Promise<number>;
  getIdFullResponse(subject: string, schema: SchemaInfo, normalize: boolean): Promise<SchemaMetadata>;
  getLatestSchemaMetadata(subject: string, format?: string): Promise<SchemaMetadata>;
  getSchemaMetadata(subject: string, version: number, deleted: boolean, format?: string): Promise<SchemaMetadata>;
  getLatestWithMetadata(subject: string, metadata: { [key: string]: string },
                        deleted: boolean, format?: string): Promise<SchemaMetadata>;
  getAllVersions(subject: string): Promise<number[]>;
  getVersion(subject: string, schema: SchemaInfo, normalize: boolean, deleted: boolean): Promise<number>;
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
  clearLatestCaches(): void;
  clearCaches(): void;
  close(): void;
}

/**
 * SchemaRegistryClient is a client for interacting with the Confluent Schema Registry.
 * This client will cache responses from Schema Registry to reduce network requests.
 */
export class SchemaRegistryClient implements Client {
  private clientConfig: ClientConfig;
  private restService: RestService;

  private idToSchemaInfoCache: LRUCache<string, SchemaInfo>;
  private guidToSchemaInfoCache: LRUCache<string, SchemaInfo>;
  private infoToSchemaCache: LRUCache<string, SchemaMetadata>;
  private latestToSchemaCache: LRUCache<string, SchemaMetadata>;
  private schemaToVersionCache: LRUCache<string, number>;
  private versionToSchemaCache: LRUCache<string, SchemaMetadata>;
  private metadataToSchemaCache: LRUCache<string, SchemaMetadata>;

  private schemaToIdMutex: Mutex;
  private idToSchemaInfoMutex: Mutex;
  private guidToSchemaInfoMutex: Mutex;
  private infoToSchemaMutex: Mutex;
  private latestToSchemaMutex: Mutex;
  private schemaToVersionMutex: Mutex;
  private versionToSchemaMutex: Mutex;
  private metadataToSchemaMutex: Mutex;

  /**
   * Create a new Schema Registry client.
   * @param config - The client configuration.
   */
  constructor(config: ClientConfig) {
    this.clientConfig = config
    const cacheOptions = {
      max: config.cacheCapacity !== undefined ? config.cacheCapacity : 1000,
      ...(config.cacheLatestTtlSecs !== undefined && { ttl: config.cacheLatestTtlSecs * 1000 })
    };

    this.restService = new RestService(config.baseURLs, config.isForward, config.createAxiosDefaults,
      config.basicAuthCredentials, config.bearerAuthCredentials,
      config.maxRetries, config.retriesWaitMs, config.retriesMaxWaitMs);

    this.idToSchemaInfoCache = new LRUCache(cacheOptions);
    this.guidToSchemaInfoCache = new LRUCache(cacheOptions);
    this.infoToSchemaCache = new LRUCache(cacheOptions);
    this.latestToSchemaCache = new LRUCache(cacheOptions);
    this.schemaToVersionCache = new LRUCache(cacheOptions);
    this.versionToSchemaCache = new LRUCache(cacheOptions);
    this.metadataToSchemaCache = new LRUCache(cacheOptions);
    this.schemaToIdMutex = new Mutex();
    this.idToSchemaInfoMutex = new Mutex();
    this.guidToSchemaInfoMutex = new Mutex();
    this.infoToSchemaMutex = new Mutex();
    this.latestToSchemaMutex = new Mutex();
    this.schemaToVersionMutex = new Mutex();
    this.versionToSchemaMutex = new Mutex();
    this.metadataToSchemaMutex = new Mutex();
  }

  static newClient(config: ClientConfig): Client {
    let url = config.baseURLs[0]
    if (url.startsWith("mock://")) {
      return new MockClient(config)
    }
    return new SchemaRegistryClient(config)
  }

  config(): ClientConfig {
    return this.clientConfig
  }

  /**
   * Register a schema with the Schema Registry and return the schema ID.
   * @param subject - The subject under which to register the schema.
   * @param schema - The schema to register.
   * @param normalize - Whether to normalize the schema before registering.
   */
  async register(subject: string, schema: SchemaInfo, normalize: boolean = false): Promise<number> {
    const metadataResult = await this.registerFullResponse(subject, schema, normalize);

    return metadataResult.id!;
  }

  /**
   * Register a schema with the Schema Registry and return the full response.
   * @param subject - The subject under which to register the schema.
   * @param schema - The schema to register.
   * @param normalize - Whether to normalize the schema before registering.
   */
  async registerFullResponse(subject: string, schema: SchemaInfo, normalize: boolean = false): Promise<SchemaMetadata> {
    const cacheKey = stringify({ subject, schema: minimize(schema) });

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

  /**
   * Get a schema by subject and ID.
   * @param subject - The subject under which the schema is registered.
   * @param id - The schema ID.
   * @param format - The format of the schema.
   */
  async getBySubjectAndId(subject: string, id: number, format?: string): Promise<SchemaInfo> {
    const cacheKey = stringify({ subject, id });
    return await this.idToSchemaInfoMutex.runExclusive(async () => {
      const cachedSchema: SchemaInfo | undefined = this.idToSchemaInfoCache.get(cacheKey);
      if (cachedSchema) {
        return cachedSchema;
      }

      subject = encodeURIComponent(subject);

      let formatStr = format != null ? `&format=${format}` : '';

      const response: AxiosResponse<SchemaInfo> = await this.restService.handleRequest(
        `/schemas/ids/${id}?subject=${subject}${formatStr}`,
        'GET'
      );
      this.idToSchemaInfoCache.set(cacheKey, response.data);
      return response.data;
    });
  }

  /**
   * Get a schema by GUID.
   * @param guid - The schema GUID.
   * @param format - The format of the schema.
   */
  async getByGuid(guid: string, format?: string): Promise<SchemaInfo> {
    return await this.guidToSchemaInfoMutex.runExclusive(async () => {
      const cachedSchema: SchemaInfo | undefined = this.guidToSchemaInfoCache.get(guid);
      if (cachedSchema) {
        return cachedSchema;
      }

      let formatStr = format != null ? `?format=${format}` : '';

      const response: AxiosResponse<SchemaInfo> = await this.restService.handleRequest(
        `/schemas/guids/${guid}${formatStr}`,
        'GET'
      );
      this.guidToSchemaInfoCache.set(guid, response.data);
      return response.data;
    });
  }

  /**
   * Get the ID for a schema.
   * @param subject - The subject under which the schema is registered.
   * @param schema - The schema whose ID to get.
   * @param normalize - Whether to normalize the schema before getting the ID.
   */
  async getId(subject: string, schema: SchemaInfo, normalize: boolean = false): Promise<number> {
    const metadataResult = await this.getIdFullResponse(subject, schema, normalize);

    return metadataResult.id!;
  }

  /**
   * Get the ID for a schema.
   * @param subject - The subject under which the schema is registered.
   * @param schema - The schema whose ID to get.
   * @param normalize - Whether to normalize the schema before getting the ID.
   */
  async getIdFullResponse(subject: string, schema: SchemaInfo, normalize: boolean = false): Promise<SchemaMetadata> {
    const cacheKey = stringify({ subject, schema: minimize(schema) });

    return await this.schemaToIdMutex.runExclusive(async () => {
      const cachedSchemaMetadata: SchemaMetadata | undefined = this.infoToSchemaCache.get(cacheKey);
      if (cachedSchemaMetadata) {
        // Allow the schema to be looked up again if version is not valid
        // This is for backward compatibility with versions before CP 8.0
        if (cachedSchemaMetadata.version != null && cachedSchemaMetadata.version > 0) {
          return cachedSchemaMetadata;
        }
      }

      subject = encodeURIComponent(subject);

      const response: AxiosResponse<SchemaMetadata> = await this.restService.handleRequest(
        `/subjects/${subject}?normalize=${normalize}`,
        'POST',
        schema
      );
      this.infoToSchemaCache.set(cacheKey, response.data);
      return response.data;
    });
  }

  /**
   * Get the latest schema metadata for a subject.
   * @param subject - The subject for which to get the latest schema metadata.
   * @param format - The format of the schema.
   */
  async getLatestSchemaMetadata(subject: string, format?: string): Promise<SchemaMetadata> {
    return await this.latestToSchemaMutex.runExclusive(async () => {
      const cachedSchema: SchemaMetadata | undefined = this.latestToSchemaCache.get(subject);
      if (cachedSchema) {
        return cachedSchema;
      }

      subject = encodeURIComponent(subject);

      let formatStr = format != null ? `?format=${format}` : '';

      const response: AxiosResponse<SchemaMetadata> = await this.restService.handleRequest(
        `/subjects/${subject}/versions/latest${formatStr}`,
        'GET'
      );
      this.latestToSchemaCache.set(subject, response.data);
      return response.data;
    });
  }

  /**
   * Get the schema metadata for a subject and version.
   * @param subject - The subject for which to get the schema metadata.
   * @param version - The version of the schema.
   * @param deleted - Whether to include deleted schemas.
   * @param format - The format of the schema.
   */
  async getSchemaMetadata(subject: string, version: number, deleted: boolean = false, format?: string): Promise<SchemaMetadata> {
    const cacheKey = stringify({ subject, version, deleted });

    return await this.versionToSchemaMutex.runExclusive(async () => {
      const cachedSchemaMetadata: SchemaMetadata | undefined = this.versionToSchemaCache.get(cacheKey);
      if (cachedSchemaMetadata) {
        return cachedSchemaMetadata;
      }

      subject = encodeURIComponent(subject);

      let formatStr = format != null ? `&format=${format}` : '';

      const response: AxiosResponse<SchemaMetadata> = await this.restService.handleRequest(
        `/subjects/${subject}/versions/${version}?deleted=${deleted}${formatStr}`,
        'GET'
      );
      this.versionToSchemaCache.set(cacheKey, response.data);
      return response.data;
    });
  }

  /**
   * Get the latest schema metadata for a subject with the given metadata.
   * @param subject - The subject for which to get the latest schema metadata.
   * @param metadata - The metadata to match.
   * @param deleted - Whether to include deleted schemas.
   * @param format - The format of the schema.
   */
  async getLatestWithMetadata(subject: string, metadata: { [key: string]: string },
                              deleted: boolean = false, format?: string): Promise<SchemaMetadata> {
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

      let formatStr = format != null ? `&format=${format}` : '';

      const response: AxiosResponse<SchemaMetadata> = await this.restService.handleRequest(
        `/subjects/${subject}/metadata?deleted=${deleted}&${metadataStr}${formatStr}`,
        'GET'
      );
      this.metadataToSchemaCache.set(cacheKey, response.data);
      return response.data;
    });
  }

  /**
   * Get all versions of a schema for a subject.
   * @param subject - The subject for which to get all versions.
   */
  async getAllVersions(subject: string): Promise<number[]> {
    const response: AxiosResponse<number[]> = await this.restService.handleRequest(
      `/subjects/${subject}/versions`,
      'GET'
    );
    return response.data;
  }

  /**
   * Get the version of a schema for a subject.
   * @param subject - The subject for which to get the version.
   * @param schema - The schema for which to get the version.
   * @param normalize - Whether to normalize the schema before getting the version.
   */
  async getVersion(subject: string, schema: SchemaInfo,
                   normalize: boolean = false, deleted: boolean = false): Promise<number> {
    const cacheKey = stringify({ subject, schema: minimize(schema), deleted });

    return await this.schemaToVersionMutex.runExclusive(async () => {
      const cachedVersion: number | undefined = this.schemaToVersionCache.get(cacheKey);
      if (cachedVersion) {
        return cachedVersion;
      }

      subject = encodeURIComponent(subject);

      const response: AxiosResponse<SchemaMetadata> = await this.restService.handleRequest(
        `/subjects/${subject}?normalize=${normalize}&deleted=${deleted}`,
        'POST',
        schema
      );
      this.schemaToVersionCache.set(cacheKey, response.data.version);
      return response.data.version!;
    });
  }

  /**
   * Get all subjects in the Schema Registry.
   */
  async getAllSubjects(): Promise<string[]> {
    const response: AxiosResponse<string[]> = await this.restService.handleRequest(
      `/subjects`,
      'GET'
    );
    return response.data;
  }

  /**
   * Delete a subject from the Schema Registry.
   * @param subject - The subject to delete.
   * @param permanent - Whether to permanently delete the subject.
   */
  async deleteSubject(subject: string, permanent: boolean = false): Promise<number[]> {
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

  /**
   * Delete a version of a subject from the Schema Registry.
   * @param subject - The subject to delete.
   * @param version - The version to delete.
   * @param permanent - Whether to permanently delete the version.
   */
  async deleteSubjectVersion(subject: string, version: number, permanent: boolean = false): Promise<number> {
    return await this.schemaToVersionMutex.runExclusive(async () => {
      let metadataValue: SchemaMetadata | undefined;

      this.schemaToVersionCache.forEach((value, key) => {
        const parsedKey = JSON.parse(key);
        if (parsedKey.subject === subject && value === version) {
          this.schemaToVersionCache.delete(key);
          const infoToSchemaCacheKey = stringify({ subject: subject, schema: minimize(parsedKey.schema) });

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

  /**
   * Test the compatibility of a schema with the latest schema for a subject.
   * @param subject - The subject for which to test compatibility.
   * @param schema - The schema to test compatibility.
   */
  async testSubjectCompatibility(subject: string, schema: SchemaInfo): Promise<boolean> {
    subject = encodeURIComponent(subject);

    const response: AxiosResponse<isCompatibleResponse> = await this.restService.handleRequest(
      `/compatibility/subjects/${subject}/versions/latest`,
      'POST',
      schema
    );
    return response.data.is_compatible;
  }

  /**
   * Test the compatibility of a schema with a specific version of a subject.
   * @param subject - The subject for which to test compatibility.
   * @param version - The version of the schema for which to test compatibility.
   * @param schema - The schema to test compatibility.
   */
  async testCompatibility(subject: string, version: number, schema: SchemaInfo): Promise<boolean> {
    subject = encodeURIComponent(subject);

    const response: AxiosResponse<isCompatibleResponse> = await this.restService.handleRequest(
      `/compatibility/subjects/${subject}/versions/${version}`,
      'POST',
      schema
    );
    return response.data.is_compatible;
  }

  /**
   * Get the compatibility level for a subject.
   * @param subject - The subject for which to get the compatibility level.
   */
  async getCompatibility(subject: string): Promise<Compatibility> {
    subject = encodeURIComponent(subject);

    const response: AxiosResponse<CompatibilityLevel> = await this.restService.handleRequest(
      `/config/${subject}`,
      'GET'
    );
    return response.data.compatibilityLevel!;
  }

  /**
   * Update the compatibility level for a subject.
   * @param subject - The subject for which to update the compatibility level.
   * @param update - The compatibility level to update to.
   */
  async updateCompatibility(subject: string, update: Compatibility): Promise<Compatibility> {
    subject = encodeURIComponent(subject);

    const response: AxiosResponse<CompatibilityLevel> = await this.restService.handleRequest(
      `/config/${subject}`,
      'PUT',
      { compatibility: update }
    );
    return response.data.compatibility!;
  }

  /**
   * Get the default/global compatibility level.
   */
  async getDefaultCompatibility(): Promise<Compatibility> {
    const response: AxiosResponse<CompatibilityLevel> = await this.restService.handleRequest(
      `/config`,
      'GET'
    );
    return response.data.compatibilityLevel!;
  }

  /**
   * Update the default/global compatibility level.
   * @param update - The compatibility level to update to.
   */
  async updateDefaultCompatibility(update: Compatibility): Promise<Compatibility> {
    const response: AxiosResponse<CompatibilityLevel> = await this.restService.handleRequest(
      `/config`,
      'PUT',
      { compatibility: update }
    );
    return response.data.compatibility!;
  }

  /**
   * Get the config for a subject.
   * @param subject - The subject for which to get the config.
   */
  async getConfig(subject: string): Promise<ServerConfig> {
    subject = encodeURIComponent(subject);

    const response: AxiosResponse<ServerConfig> = await this.restService.handleRequest(
      `/config/${subject}`,
      'GET'
    );
    return response.data;
  }

  /**
   * Update the config for a subject.
   * @param subject - The subject for which to update the config.
   * @param update - The config to update to.
   */
  async updateConfig(subject: string, update: ServerConfig): Promise<ServerConfig> {
    const response: AxiosResponse<ServerConfig> = await this.restService.handleRequest(
      `/config/${subject}`,
      'PUT',
      update
    );
    return response.data;
  }

  /**
   * Get the default/global config.
   */
  async getDefaultConfig(): Promise<ServerConfig> {
    const response: AxiosResponse<ServerConfig> = await this.restService.handleRequest(
      `/config`,
      'GET'
    );
    return response.data;
  }

  /**
   * Update the default/global config.
   * @param update - The config to update to.
   */
  async updateDefaultConfig(update: ServerConfig): Promise<ServerConfig> {
    const response: AxiosResponse<ServerConfig> = await this.restService.handleRequest(
      `/config`,
      'PUT',
      update
    );
    return response.data;
  }

  /**
   * Clear the latest caches.
   */
  clearLatestCaches(): void {
    this.latestToSchemaCache.clear();
    this.metadataToSchemaCache.clear();
  }

  /**
   * Clear all caches.
   */
  clearCaches(): void {
    this.idToSchemaInfoCache.clear();
    this.guidToSchemaInfoCache.clear();
    this.infoToSchemaCache.clear();
    this.latestToSchemaCache.clear();
    this.schemaToVersionCache.clear();
    this.versionToSchemaCache.clear();
    this.metadataToSchemaCache.clear();
  }

  /**
   * Close the client.
   */
  async close(): Promise<void> {
    this.clearCaches();
  }

  // Cache methods for testing
  async addToInfoToSchemaCache(subject: string, schema: SchemaInfo, metadata: SchemaMetadata): Promise<void> {
    const cacheKey = stringify({ subject, schema: minimize(schema) });
    await this.infoToSchemaMutex.runExclusive(async () => {
      this.infoToSchemaCache.set(cacheKey, metadata);
    });
  }

  async addToSchemaToVersionCache(subject: string, schema: SchemaInfo, version: number): Promise<void> {
    const cacheKey = stringify({ subject, schema: minimize(schema) });
    await this.schemaToVersionMutex.runExclusive(async () => {
      this.schemaToVersionCache.set(cacheKey, version);
    });
  }

  async addToVersionToSchemaCache(subject: string, version: number, metadata: SchemaMetadata): Promise<void> {
    const cacheKey = stringify({ subject, version });
    await this.versionToSchemaMutex.runExclusive(async () => {
      this.versionToSchemaCache.set(cacheKey, metadata);
    });
  }

  async addToIdToSchemaInfoCache(subject: string, id: number, schema: SchemaInfo): Promise<void> {
    const cacheKey = stringify({ subject, id });
    await this.idToSchemaInfoMutex.runExclusive(async () => {
      this.idToSchemaInfoCache.set(cacheKey, schema);
    });
  }

  async addToGuidToSchemaInfoCache(guid: string, schema: SchemaInfo): Promise<void> {
    await this.guidToSchemaInfoMutex.runExclusive(async () => {
      this.guidToSchemaInfoCache.set(guid, schema);
    });
  }

  async getInfoToSchemaCacheSize(): Promise<number> {
    return await this.infoToSchemaMutex.runExclusive(async () => {
      return this.infoToSchemaCache.size;
    });
  }

  async getSchemaToVersionCacheSize(): Promise<number> {
    return await this.schemaToVersionMutex.runExclusive(async () => {
      return this.schemaToVersionCache.size;
    });
  }

  async getVersionToSchemaCacheSize(): Promise<number> {
    return await this.versionToSchemaMutex.runExclusive(async () => {
      return this.versionToSchemaCache.size;
    });
  }

  async getIdToSchemaInfoCacheSize(): Promise<number> {
    return await this.idToSchemaInfoMutex.runExclusive(async () => {
      return this.idToSchemaInfoCache.size;
    });
  }

  async getGuidToSchemaInfoCacheSize(): Promise<number> {
    return await this.guidToSchemaInfoMutex.runExclusive(async () => {
      return this.guidToSchemaInfoCache.size;
    });
  }
}
