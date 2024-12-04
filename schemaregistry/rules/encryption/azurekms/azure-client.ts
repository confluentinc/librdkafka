import {KmsClient} from "../kms-registry";
import {AzureKmsDriver} from "./azure-driver";
import {TokenCredential} from "@azure/identity";
import {CryptographyClient, EncryptionAlgorithm} from "@azure/keyvault-keys";

export class AzureKmsClient implements KmsClient {
  private static ALGORITHM: EncryptionAlgorithm = 'RSA-OAEP-256'

  private kmsClient: CryptographyClient
  private keyUri: string
  private keyId: string

  constructor(keyUri: string, creds: TokenCredential) {
    if (!keyUri.startsWith(AzureKmsDriver.PREFIX)) {
      throw new Error(`key uri must start with ${AzureKmsDriver.PREFIX}`)
    }
    this.keyUri = keyUri
    this.keyId = keyUri.substring(AzureKmsDriver.PREFIX.length)
    this.kmsClient = new CryptographyClient(this.keyId, creds)
  }

  supported(keyUri: string): boolean {
    return this.keyUri === keyUri
  }

  async encrypt(plaintext: Buffer): Promise<Buffer> {
    const result = await this.kmsClient.encrypt(AzureKmsClient.ALGORITHM, plaintext)
    return Buffer.from(result.result)
  }

  async decrypt(ciphertext: Buffer): Promise<Buffer> {
    const result = await this.kmsClient.decrypt(AzureKmsClient.ALGORITHM, ciphertext)
    return Buffer.from(result.result)
  }
}
