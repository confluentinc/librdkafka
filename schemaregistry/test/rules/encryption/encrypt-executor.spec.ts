import { describe, expect, it } from '@jest/globals';
import {FieldEncryptionExecutor} from "../../../rules/encryption/encrypt-executor";
import {ClientConfig} from "../../../rest-service";

describe('FieldEncryptionExecutor', () => {
  it('configure error', () => {
    const executor = new FieldEncryptionExecutor()
    const clientConfig: ClientConfig = {
      baseURLs: ['mock://'],
      cacheCapacity: 1000
    }
    const config = new Map<string, string>();
    config.set('key', 'value');
    executor.configure(clientConfig, config);
    // configure with same args is fine
    executor.configure(clientConfig, config);
    const config2 = new Map<string, string>();
    config2.set('key2', 'value2');
    // configure with additional config keys is fine
    executor.configure(clientConfig, config);

    const clientConfig2: ClientConfig = {
      baseURLs: ['blah://'],
      cacheCapacity: 1000
    }
    expect(() => executor.configure(clientConfig2, config)).toThrowError()

    const config3  = new Map<string, string>();
    config3.set('key', 'value3');
    expect(() => executor.configure(clientConfig, config3)).toThrowError()
  })
})
