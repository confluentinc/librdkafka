
import { Client, Compatibility, Metadata, SchemaInfo, SchemaMetadata, ServerConfig } from './schemaregistry-client';
import stringify from "json-stringify-deterministic";

interface VersionCacheEntry {
  version: number;
  softDeleted: boolean;
}

interface InfoCacheEntry {
  info: SchemaInfo;
  softDeleted: boolean;
}

interface MetadataCacheEntry {
  metadata: SchemaMetadata;
  softDeleted: boolean;
}

class Counter {
  private count: number = 0;

  currentValue(): number {
    return this.count;
  }

  increment(): number {
    this.count++;
    return this.count;
  }
}

const noSubject = "";

class MockClient implements Client {
  private infoToSchemaCache: Map<string, MetadataCacheEntry>;
  private idToSchemaCache: Map<string, InfoCacheEntry>;
  private schemaToVersionCache: Map<string, VersionCacheEntry>;
  private configCache: Map<string, ServerConfig>;
  private counter: Counter;

  constructor() {
    this.infoToSchemaCache = new Map();
    this.idToSchemaCache = new Map();
    this.schemaToVersionCache = new Map();
    this.configCache = new Map();
    this.counter = new Counter();
  }

  public async register(subject: string, schema: SchemaInfo, normalize: boolean = false): Promise<number> {
    const metadata = await this.registerFullResponse(subject, schema, normalize);
    if (!metadata) {
      throw new Error("Failed to register schema");
    }
    return metadata.id;
  }

  public async registerFullResponse(subject: string, schema: SchemaInfo, normalize: boolean = false): Promise<SchemaMetadata> {
    const cacheKey = stringify({ subject, schema });

    const cacheEntry = this.infoToSchemaCache.get(cacheKey);
    if (cacheEntry && !cacheEntry.softDeleted) {
      return cacheEntry.metadata;
    }

    const id = await this.getIDFromRegistry(subject, schema);
    if (id === -1) {
      throw new Error("Failed to retrieve schema ID from registry");
    }

    const metadata: SchemaMetadata = { ...schema, id };
    this.infoToSchemaCache.set(cacheKey, { metadata, softDeleted: false });

    return metadata;
  }

  private async getIDFromRegistry(subject: string, schema: SchemaInfo): Promise<number> {
    let id = -1;

    for (const [key, value] of this.idToSchemaCache.entries()) {
      const parsedKey = JSON.parse(key);
      if (parsedKey.subject === subject && this.schemasEqual(value.info, schema)) {
        id = parsedKey.id;
        break;
      }
    }

    await this.generateVersion(subject, schema);
    if (id < 0) {
      id = this.counter.increment();
      const idCacheKey = stringify({ subject, id });
      this.idToSchemaCache.set(idCacheKey, { info: schema, softDeleted: false });
    }

    return id;
  }

  private async generateVersion(subject: string, schema: SchemaInfo): Promise<void> {
    const versions = await this.allVersions(subject);
    let newVersion: number;

    if (versions.length === 0) {
      newVersion = 1;
    } else {
      newVersion = versions[versions.length - 1] + 1;
    }

    const cacheKey = stringify({ subject, schema: schema });
    this.schemaToVersionCache.set(cacheKey, { version: newVersion, softDeleted: false });
  }

  public async getBySubjectAndId(subject: string, id: number): Promise<SchemaInfo> {
    const cacheKey = stringify({ subject, id });
    const cacheEntry = this.idToSchemaCache.get(cacheKey);

    if (!cacheEntry || cacheEntry.softDeleted) {
      throw new Error("Schema not found");
    }
    return cacheEntry.info;
  }

  public async getId(subject: string, schema: SchemaInfo): Promise<number> {
    const cacheKey = stringify({ subject, schema });
    const cacheEntry = this.infoToSchemaCache.get(cacheKey);
    if (!cacheEntry || cacheEntry.softDeleted) {
      throw new Error("Schema not found");
    }
    return cacheEntry.metadata.id;
  }

  public async getLatestSchemaMetadata(subject: string): Promise<SchemaMetadata> {
    const version = await this.latestVersion(subject);
    if (version === -1) {
      throw new Error("No versions found for subject");
    }

    return this.getSchemaMetadata(subject, version);
  }

  public async getSchemaMetadata(subject: string, version: number, deleted: boolean = false): Promise<SchemaMetadata> {
    let json;
    for (const [key, value] of this.schemaToVersionCache.entries()) {
      const parsedKey = JSON.parse(key);
      if (parsedKey.subject === subject && value.version === version && value.softDeleted === deleted) {
        json = parsedKey;
      }
    }

    if (!json) {
      throw new Error("Schema not found");
    }

    let id: number = -1;
    for (const [key, value] of this.idToSchemaCache.entries()) {
      const parsedKey = JSON.parse(key);
      if (parsedKey.subject === subject && value.info.schema === json.schema.schema) {
        id = parsedKey.id;
      }
    }
    if (id === -1) {
      throw new Error("Schema not found");
    }


    return {
      id,
      version,
      subject,
      schema: json.schema.schema
    };
  }

  public async getLatestWithMetadata(subject: string, metadata: { [key: string]: string }, deleted: boolean = false): Promise<SchemaMetadata> {
    let metadataStr = '';

    for (const key in metadata) {
      const encodedKey = encodeURIComponent(key);
      const encodedValue = encodeURIComponent(metadata[key]);
      metadataStr += `&key=${encodedKey}&value=${encodedValue}`;
    }

    let results: SchemaMetadata[] = [];

    for (const [key, value] of this.schemaToVersionCache.entries()) {
      const parsedKey = JSON.parse(key);
      if (parsedKey.subject === subject && (!value.softDeleted || deleted)) {
        if (parsedKey.schema.metadata && this.isSubset(metadata, parsedKey.schema.metadata.properties)) {
          results.push({ id: parsedKey.schema.id, version: value.version, subject, schema: parsedKey.schema.schema });
        }
      }
    }

    if (results.length === 0) {
      throw new Error("Schema not found");
    }

    let latest: SchemaMetadata = results[0];

    results.forEach((result) => {
      if (result.version! > latest.version!) {
        latest = result;
      }
    });

    return latest;
  }

  private isSubset(containee: { [key: string]: string }, container: { [key: string]: string }){
    for (const key in containee) {
      if (containee[key] !== container[key]) {
        return false;
      }
    }
    return true;
  }

  public async getAllVersions(subject: string): Promise<number[]> {
    const results = await this.allVersions(subject);

    if (results.length === 0) {
      throw new Error("No versions found for subject");
    }
    return results;
  }

  private async allVersions(subject: string): Promise<number[]> {
    const versions: number[] = [];

    for (const [key, value] of this.schemaToVersionCache.entries()) {
      const parsedKey = JSON.parse(key);
      if (parsedKey.subject === subject && !value.softDeleted) {
        versions.push(value.version);
      }
    }
    return versions;
  }

  private async latestVersion(subject: string): Promise<number> {
    const versions = await this.allVersions(subject);
    if (versions.length === 0) {
      return -1;
    }
    return versions[versions.length - 1];
  }

  private async deleteVersion(cacheKey: string, version: number, permanent: boolean): Promise<void> {
    if (permanent) {
      this.schemaToVersionCache.delete(cacheKey);
    } else {
      this.schemaToVersionCache.set(cacheKey, { version, softDeleted: true });
    }
  }

  private async deleteInfo(cacheKey: string, info: SchemaInfo, permanent: boolean): Promise<void> {
    if (permanent) {
      this.idToSchemaCache.delete(cacheKey);
    } else {
      this.idToSchemaCache.set(cacheKey, { info, softDeleted: true });
    }
  }

  private async deleteMetadata(cacheKey: string, metadata: SchemaMetadata, permanent: boolean): Promise<void> {
    if (permanent) {
      this.infoToSchemaCache.delete(cacheKey);
    } else {
      this.infoToSchemaCache.set(cacheKey, { metadata, softDeleted: true });
    }
  }

  public async getVersion(subject: string, schema: SchemaInfo, normalize: boolean = false): Promise<number> {
    const cacheKey = stringify({ subject, schema });
    const cacheEntry = this.schemaToVersionCache.get(cacheKey);

    if (!cacheEntry || cacheEntry.softDeleted) {
      throw new Error("Schema not found");
    }

    return cacheEntry.version;
  }

  public async getAllSubjects(): Promise<string[]> {
    const subjects: string[] = [];
    for (const [key, value] of this.schemaToVersionCache.entries()) {
      const parsedKey = JSON.parse(key);
      if (!value.softDeleted && !subjects.includes(parsedKey.subject)) {
        subjects.push(parsedKey.subject);
      }
    }
    return subjects.sort();
  }

  public async deleteSubject(subject: string, permanent: boolean = false): Promise<number[]> {
    const deletedVersions: number[] = [];
    for (const [key, value] of this.infoToSchemaCache.entries()) {
      const parsedKey = JSON.parse(key);
      if (parsedKey.subject === subject && (permanent || !value.softDeleted)) {
        await this.deleteMetadata(key, value.metadata, permanent);
      }
    }

    for (const [key, value] of this.schemaToVersionCache.entries()) {
      const parsedKey = JSON.parse(key);
      if (parsedKey.subject === subject && (permanent || !value.softDeleted)) {
        await this.deleteVersion(key, value.version, permanent);
        deletedVersions.push(value.version);
      }
    }

    this.configCache.delete(subject);

    if (permanent) {
      for (const [key, value] of this.idToSchemaCache.entries()) {
        const parsedKey = JSON.parse(key);
        if (parsedKey.subject === subject && (!value.softDeleted)) {
          await this.deleteInfo(key, value.info, permanent);
        }
      }
    }

    return deletedVersions;
  }

  public async deleteSubjectVersion(subject: string, version: number, permanent: boolean = false): Promise<number> {
    for (const [key, value] of this.schemaToVersionCache.entries()) {
      const parsedKey = JSON.parse(key);
      if (parsedKey.subject === subject && value.version === version) {
        await this.deleteVersion(key, version, permanent);
        
        const cacheKeySchema = stringify({ subject, schema: parsedKey.schema });
        const cacheEntry = this.infoToSchemaCache.get(cacheKeySchema);
        if (cacheEntry) {
          await this.deleteMetadata(cacheKeySchema, cacheEntry.metadata, permanent);
        }

        if (permanent && cacheEntry) {
          const cacheKeyInfo = stringify({ subject, id: cacheEntry.metadata.id });
          const cacheSchemaEntry = this.idToSchemaCache.get(cacheKeyInfo);
          if (cacheSchemaEntry) {
            await this.deleteInfo(cacheKeyInfo, cacheSchemaEntry.info, permanent);
          }
        }
      }
    }

    return version;
  }

  public async testSubjectCompatibility(subject: string, schema: SchemaInfo): Promise<boolean> {
    throw new Error("Unsupported operation");
  }

  public async testCompatibility(subject: string, version: number, schema: SchemaInfo): Promise<boolean> {
    throw new Error("Unsupported operation");
  }

  public async getCompatibility(subject: string): Promise<Compatibility> {
    const cacheEntry = this.configCache.get(subject);
    if (!cacheEntry) {
      throw new Error("Subject not found");
    }
    return cacheEntry.compatibilityLevel as Compatibility;
  }

  public async updateCompatibility(subject: string, compatibility: Compatibility): Promise<Compatibility> {
    this.configCache.set(subject, { compatibilityLevel: compatibility });
    return compatibility;
  }

  public async getDefaultCompatibility(): Promise<Compatibility> {
    const cacheEntry = this.configCache.get(noSubject);
    if (!cacheEntry) {
      throw new Error("Default compatibility not found");
    }
    return cacheEntry.compatibilityLevel as Compatibility;
  }

  public async updateDefaultCompatibility(compatibility: Compatibility): Promise<Compatibility> {
    this.configCache.set(noSubject, { compatibilityLevel: compatibility });
    return compatibility;
  }

  public async getConfig(subject: string): Promise<ServerConfig> {
    const cacheEntry = this.configCache.get(subject);
    if (!cacheEntry) {
      throw new Error("Subject not found");
    }
    return cacheEntry;
  }

  public async updateConfig(subject: string, config: ServerConfig): Promise<ServerConfig> {
    this.configCache.set(subject, config);
    return config;
  }

  public async getDefaultConfig(): Promise<ServerConfig> {
    const cacheEntry = this.configCache.get(noSubject);
    if (!cacheEntry) {
      throw new Error("Default config not found");
    }
    return cacheEntry;
  }

  public async updateDefaultConfig(config: ServerConfig): Promise<ServerConfig> {
    this.configCache.set(noSubject, config);
    return config;
  }

  public async close(): Promise<void> {
    return;
  }

  private schemasEqual(schema1: SchemaInfo, schema2: SchemaInfo): boolean {
    return stringify(schema1) === stringify(schema2);
  }
}

export { MockClient };