import { LRUCache } from 'lru-cache';
import { Mutex } from 'async-mutex';
import { ClientConfig, RestService } from '../rest-service';
import stringify from 'json-stringify-deterministic';

/*
 * Confluent-Schema-Registry-TypeScript - Node.js wrapper for Confluent Schema Registry
 *
 * Copyright (c) 2024 Confluent, Inc.
 *
 * This software may be modified and distributed under the terms
 * of the MIT license.  See the LICENSE.txt file for details.
 */

interface Kek {
  name?: string;
  kmsType?: string;
  kmsKeyId?: string;
  kmsProps?: { [key: string]: string };
  doc?: string;
  shared?: boolean;
  ts?: number;
  deleted?: boolean;
}

interface CreateKekRequest {
  name?: string;
  kmsType?: string;
  kmsKeyId?: string;
  kmsProps?: { [key: string]: string };
  doc?: string;
  shared?: boolean;
}

interface Dek {
  kekName?: string;
  subject?: string;
  version?: number;
  algorithm?: string;
  encryptedKeyMaterial?: string;
  encryptedKeyMaterialBytes?: Buffer;
  keyMaterial?: string;
  keyMaterialBytes?: Buffer;
  ts?: number;
  deleted?: boolean;
}

interface Client {
  registerKek(name: string, kmsType: string, kmsKeyId: string, kmsProps: { [key: string]: string }, doc: string, shared: boolean): Promise<Kek>;
  getKek(name: string, deleted: boolean): Promise<Kek>;
  registerDek(kekName: string, subject: string, algorithm: string, encryptedKeyMaterial: string, version: number): Promise<Dek>;
  getDek(kekName: string, subject: string, algorithm: string, version: number, deleted: boolean): Promise<Dek>;
  close(): Promise<void>;
}

class DekRegistryClient implements Client {
  private restService: RestService;
  private kekCache: LRUCache<string, Kek>;
  private dekCache: LRUCache<string, Dek>;
  private kekMutex: Mutex;
  private dekMutex: Mutex;

  constructor(config: ClientConfig) {
    const cacheOptions = {
      max: config.cacheCapacity,
      ...(config.cacheLatestTtlSecs !== undefined && { maxAge: config.cacheLatestTtlSecs * 1000 }),
    };


    this.restService = new RestService(config.createAxiosDefaults, config.baseURLs, config.isForward);
    this.kekCache = new LRUCache<string, Kek>(cacheOptions);
    this.dekCache = new LRUCache<string, Dek>(cacheOptions);
    this.kekMutex = new Mutex();
    this.dekMutex = new Mutex();
  }

  public static getEncryptedKeyMaterialBytes(dek: Dek): Buffer | null {
    if (!dek.encryptedKeyMaterial) {
      return null;
    }

    if (!dek.encryptedKeyMaterialBytes) {
      try {
        const bytes = Buffer.from(dek.encryptedKeyMaterial, 'base64');
        dek.encryptedKeyMaterialBytes = bytes;
      } catch (err) {
        if (err instanceof Error) {
          throw new Error(`Failed to decode base64 string: ${err.message}`);
        }
        throw new Error(`Unknown error: ${err}`);
      }
    }

    return dek.encryptedKeyMaterialBytes;
  }

  public static getKeyMaterialBytes(dek: Dek): Buffer | null {
    if (!dek.keyMaterial) {
      return null;
    }

    if (!dek.keyMaterialBytes) {
      try {
        const bytes = Buffer.from(dek.keyMaterial, 'base64');
        dek.keyMaterialBytes = bytes;
      } catch (err) {
        if (err instanceof Error) {
          throw new Error(`Failed to decode base64 string: ${err.message}`);
        }
        throw new Error(`Unknown error: ${err}`);
      }
    }

    return dek.keyMaterialBytes;
  }

  public static setKeyMaterial(dek: Dek, keyMaterialBytes: Buffer): void {
    if (keyMaterialBytes) {
      const str = keyMaterialBytes.toString('base64');
      dek.keyMaterial = str;
    }
  }

  public async registerKek(name: string, kmsType: string, kmsKeyId: string,
    kmsProps: { [key: string]: string }, doc: string, shared: boolean): Promise<Kek> {
    const cacheKey = stringify({ name, deleted: false });

    return await this.kekMutex.runExclusive(async () => {
      const kek = this.kekCache.get(cacheKey);
      if (kek) {
        return kek;
      }

      const request: CreateKekRequest = {
        name,
        kmsType,
        kmsKeyId,
        kmsProps,
        doc,
        shared,
      };

      const response = await this.restService.handleRequest<Kek>(
        '/dek-registry/v1/keks',
        'POST',
        request);
      this.kekCache.set(cacheKey, response.data);
      return response.data;
    });
  }

  public async getKek(name: string, deleted: boolean = false): Promise<Kek> {
    const cacheKey = stringify({ name, deleted });

    return await this.kekMutex.runExclusive(async () => {
      const kek = this.kekCache.get(cacheKey);
      if (kek) {
        return kek;
      }
      name = encodeURIComponent(name);

      const response = await this.restService.handleRequest<Kek>(
        `/dek-registry/v1/keks/${name}?deleted=${deleted}`,
        'GET');
      this.kekCache.set(cacheKey, response.data);
      return response.data;
    });
  }

  public async registerDek(kekName: string, subject: string,
    algorithm: string, encryptedKeyMaterial: string, version: number = 1): Promise<Dek> {
    const cacheKey = stringify({ kekName, subject, version, algorithm, deleted: false });

    return await this.dekMutex.runExclusive(async () => {
      const dek = this.dekCache.get(cacheKey);
      if (dek) {
        return dek;
      }

      const request: Dek = {
        subject,
        version,
        algorithm,
        encryptedKeyMaterial,
      };
      kekName = encodeURIComponent(kekName);

      const response = await this.restService.handleRequest<Dek>(
        `/dek-registry/v1/keks/${kekName}/deks`,
        'POST',
        request);
      this.dekCache.set(cacheKey, response.data);

      this.dekCache.delete(stringify({ kekName, subject, version: -1, algorithm, deleted: false }));
      this.dekCache.delete(stringify({ kekName, subject, version: -1, algorithm, deleted: true }));

      return response.data;
    });
  }

  public async getDek(kekName: string, subject: string,
    algorithm: string, version: number = 1, deleted: boolean = false): Promise<Dek> {
    const cacheKey = stringify({ kekName, subject, version, algorithm, deleted });

    return await this.dekMutex.runExclusive(async () => {
      const dek = this.dekCache.get(cacheKey);
      if (dek) {
        return dek;
      }
      kekName = encodeURIComponent(kekName);
      subject = encodeURIComponent(subject);

      const response = await this.restService.handleRequest<Dek>(
        `/dek-registry/v1/keks/${kekName}/deks/${subject}/versions/${version}?deleted=${deleted}`,
        'GET');
      this.dekCache.set(cacheKey, response.data);
      return response.data;
    });
  }

  public async close(): Promise<void> {
    return;
  }

  //Cache methods for testing
  public async checkLatestDekInCache(kekName: string, subject: string, algorithm: string): Promise<boolean> {
    const cacheKey = stringify({ kekName, subject, version: -1, algorithm, deleted: false });
    const cachedDek = this.dekCache.get(cacheKey);
    return cachedDek !== undefined;
  }
}

export { DekRegistryClient, Client, Kek, Dek };

