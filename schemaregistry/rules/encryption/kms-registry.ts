import {SecurityException} from "./tink/exception/security_exception";

export interface KmsDriver {
  getKeyUrlPrefix(): string
  newKmsClient(config: Map<string, string>, keyUrl: string): KmsClient
}

export interface KmsClient {
  supported(keyUri: string): boolean
  encrypt(plaintext: Buffer): Promise<Buffer>
  decrypt(ciphertext: Buffer): Promise<Buffer>
}

const kmsDrivers: KmsDriver[] = []

const kmsClients: KmsClient[] = []


export function registerKmsDriver(kmsDriver: KmsDriver): void {
  kmsDrivers.push(kmsDriver)
}

export function getKmsDriver(keyUrl: string): KmsDriver {
  for (let driver of kmsDrivers) {
    if (keyUrl.startsWith(driver.getKeyUrlPrefix())) {
      return driver
    }
  }
  throw new SecurityException('no KMS driver found for key URL: ' + keyUrl)
}

export function registerKmsClient(kmsClient: KmsClient): void {
  kmsClients.push(kmsClient)
}

export function getKmsClient(keyUrl: string): KmsClient | null {
  for (let client of kmsClients) {
    if (client.supported(keyUrl)) {
      return client
    }
  }
  return null
}

export function clearKmsClients(): void {
  kmsClients.length = 0
}


