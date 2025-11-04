import { LRUCache } from 'lru-cache';
import { Mutex } from 'async-mutex';
import { ClientConfig, RestService } from '../../../rest-service';
import stringify from 'json-stringify-deterministic';
import {MockDekRegistryClient} from "./mock-dekregistry-client";

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

interface DekClient {
  config(): ClientConfig;
  registerKek(name: string, kmsType: string, kmsKeyId: string, shared: boolean,
              kmsProps?: { [key: string]: string }, doc?: string): Promise<Kek>;
  getKek(name: string, deleted: boolean): Promise<Kek>;
  registerDek(kekName: string, subject: string, algorithm: string, version: number,
              encryptedKeyMaterial?: string): Promise<Dek>;
  getDek(kekName: string, subject: string, algorithm: string, version: number, deleted: boolean): Promise<Dek>;
  getDekEncryptedKeyMaterialBytes(dek: Dek): Promise<Buffer | null>;
  getDekKeyMaterialBytes(dek: Dek): Promise<Buffer | null>;
  setDekKeyMaterial(dek: Dek, keyMaterialBytes: Buffer): Promise<void>;
  close(): Promise<void>;
}

class DekRegistryClient implements DekClient {
  private clientConfig: ClientConfig;
  private restService: RestService;
  private kekCache: LRUCache<string, Kek>;
  private dekCache: LRUCache<string, Dek>;
  private kekMutex: Mutex;
  private dekMutex: Mutex;

  constructor(config: ClientConfig) {
    this.clientConfig = config;
    const cacheOptions = {
      max: config.cacheCapacity !== undefined ? config.cacheCapacity : 1000,
      ...(config.cacheLatestTtlSecs !== undefined && { ttl: config.cacheLatestTtlSecs * 1000 }),
    };


    this.restService = new RestService(config.baseURLs, config.isForward, config.createAxiosDefaults,
      config.basicAuthCredentials, config.bearerAuthCredentials,
      config.maxRetries, config.retriesWaitMs, config.retriesMaxWaitMs);
    this.kekCache = new LRUCache<string, Kek>(cacheOptions);
    this.dekCache = new LRUCache<string, Dek>(cacheOptions);
    this.kekMutex = new Mutex();
    this.dekMutex = new Mutex();
  }

  static newClient(config: ClientConfig): DekClient {
    const url = config.baseURLs[0];
    if (url.startsWith("mock://")) {
      return new MockDekRegistryClient(config)
    }
    return new DekRegistryClient(config)
  }

  config(): ClientConfig {
    return this.clientConfig;
  }

  async registerKek(name: string, kmsType: string, kmsKeyId: string, shared: boolean,
    kmsProps?: { [key: string]: string }, doc?: string): Promise<Kek> {
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
        ...kmsProps && { kmsProps },
        ...doc && { doc },
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

  async getKek(name: string, deleted: boolean = false): Promise<Kek> {
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

  async registerDek(kekName: string, subject: string, algorithm: string,
    version: number = 1, encryptedKeyMaterial?: string): Promise<Dek> {
    const cacheKey = stringify({ kekName, subject, version, algorithm, deleted: false });

    return await this.dekMutex.runExclusive(async () => {
      let dek = this.dekCache.get(cacheKey);
      if (dek) {
        return dek;
      }

      const request: Dek = {
        subject,
        version,
        algorithm,
        ...encryptedKeyMaterial && { encryptedKeyMaterial },
      };

      dek = await this.createDek(kekName, request);
      this.dekCache.set(cacheKey, dek);

      this.dekCache.delete(stringify({ kekName, subject, version: -1, algorithm, deleted: false }));
      this.dekCache.delete(stringify({ kekName, subject, version: -1, algorithm, deleted: true }));

      return dek;
    });
  }

  private async createDek(kekName: string, request: Dek): Promise<Dek> {
    try {
      // Try newer API with subject in the path
      const encodedKekName = encodeURIComponent(kekName);
      const encodedSubject = encodeURIComponent(request.subject || '');
      const path = `/dek-registry/v1/keks/${encodedKekName}/deks/${encodedSubject}`;

      const response = await this.restService.handleRequest<Dek>(path, 'POST', request);
      return response.data;
    } catch (error: any) {
      if (error.response && error.response.status === 405) {
        // Try fallback to older API that does not have subject in the path
        const encodedKekName = encodeURIComponent(kekName);
        const path = `/dek-registry/v1/keks/${encodedKekName}/deks`;

        const response = await this.restService.handleRequest<Dek>(path, 'POST', request);
        return response.data;
      } else {
        throw error;
      }
    }
  }

  async getDek(kekName: string, subject: string,
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
        `/dek-registry/v1/keks/${kekName}/deks/${subject}/versions/${version}?algorithm=${algorithm}&deleted=${deleted}`,
        'GET');
      this.dekCache.set(cacheKey, response.data);
      return response.data;
    });
  }

  async getDekEncryptedKeyMaterialBytes(dek: Dek): Promise<Buffer | null> {
    if (!dek.encryptedKeyMaterial) {
      return null;
    }

    if (!dek.encryptedKeyMaterialBytes) {
      await this.dekMutex.runExclusive(async () => {
        if (!dek.encryptedKeyMaterialBytes) {
          try {
            const bytes = Buffer.from(dek.encryptedKeyMaterial!, 'base64');
            dek.encryptedKeyMaterialBytes = bytes;
          } catch (err) {
            if (err instanceof Error) {
              throw new Error(`Failed to decode base64 string: ${err.message}`);
            }
            throw new Error(`Unknown error: ${err}`);
          }
        }
      })
    }

    return dek.encryptedKeyMaterialBytes!;
  }

  async getDekKeyMaterialBytes(dek: Dek): Promise<Buffer | null> {
    if (!dek.keyMaterial) {
      return null;
    }

    if (!dek.keyMaterialBytes) {
      await this.dekMutex.runExclusive(async () => {
        if (!dek.keyMaterialBytes) {
          try {
            const bytes = Buffer.from(dek.keyMaterial!, 'base64');
            dek.keyMaterialBytes = bytes;
          } catch (err) {
            if (err instanceof Error) {
              throw new Error(`Failed to decode base64 string: ${err.message}`);
            }
            throw new Error(`Unknown error: ${err}`);
          }
        }
      })
    }

    return dek.keyMaterialBytes!;
  }

  async setDekKeyMaterial(dek: Dek, keyMaterialBytes: Buffer): Promise<void> {
    await this.dekMutex.runExclusive(async () => {
      if (keyMaterialBytes) {
        const str = keyMaterialBytes.toString('base64');
        dek.keyMaterial = str;
      }
    })
  }

  async close(): Promise<void> {
    return;
  }

  //Cache methods for testing
  async checkLatestDekInCache(kekName: string, subject: string, algorithm: string): Promise<boolean> {
    const cacheKey = stringify({ kekName, subject, version: -1, algorithm, deleted: false });
    const cachedDek = this.dekCache.get(cacheKey);
    return cachedDek !== undefined;
  }
}

export { DekRegistryClient, DekClient, Kek, Dek };

