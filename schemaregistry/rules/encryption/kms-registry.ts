import {SecurityException} from "./tink/exception/security_exception";

/**
 * Key management service (KMS) driver.
 */
export interface KmsDriver {
  getKeyUrlPrefix(): string
  newKmsClient(config: Map<string, string>, keyUrl: string): KmsClient
}

/**
 * Key management service (KMS) client.
 */
export interface KmsClient {
  supported(keyUri: string): boolean
  encrypt(plaintext: Buffer): Promise<Buffer>
  decrypt(ciphertext: Buffer): Promise<Buffer>
}

const kmsDrivers: KmsDriver[] = []

const kmsClients: KmsClient[] = []


/**
 * Register a KMS driver.
 * @param kmsDriver - the KMS driver to register
 */
export function registerKmsDriver(kmsDriver: KmsDriver): void {
  kmsDrivers.push(kmsDriver)
}

/**
 * Get the KMS driver for the given key URL.
 * @param keyUrl - the key URL
 */
export function getKmsDriver(keyUrl: string): KmsDriver {
  for (let driver of kmsDrivers) {
    if (keyUrl.startsWith(driver.getKeyUrlPrefix())) {
      return driver
    }
  }
  throw new SecurityException('no KMS driver found for key URL: ' + keyUrl)
}

/**
 * Register a KMS client.
 * @param kmsClient - the KMS client to register
 */
export function registerKmsClient(kmsClient: KmsClient): void {
  kmsClients.push(kmsClient)
}

/**
 * Get the KMS client for the given key URL.
 * @param keyUrl - the key URL
 */
export function getKmsClient(keyUrl: string): KmsClient | null {
  for (let client of kmsClients) {
    if (client.supported(keyUrl)) {
      return client
    }
  }
  return null
}

/**
 * Clear the KMS clients.
 */
export function clearKmsClients(): void {
  kmsClients.length = 0
}


