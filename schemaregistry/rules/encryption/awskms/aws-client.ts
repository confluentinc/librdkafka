import {KmsClient} from "../kms-registry";
import {AwsKmsDriver} from "./aws-driver";
import {
  DecryptCommand,
  EncryptCommand,
  KMSClient
} from '@aws-sdk/client-kms'
import {AwsCredentialIdentity, AwsCredentialIdentityProvider} from "@smithy/types";

export class AwsKmsClient implements KmsClient {

  private kmsClient: KMSClient
  private keyId: string

  constructor(keyUri: string, creds?: AwsCredentialIdentity | AwsCredentialIdentityProvider) {
    if (!keyUri.startsWith(AwsKmsDriver.PREFIX)) {
      throw new Error(`key uri must start with ${AwsKmsDriver.PREFIX}`)
    }
    this.keyId = keyUri.substring(AwsKmsDriver.PREFIX.length)
    const tokens = this.keyId.split(':')
    if (tokens.length < 4) {
      throw new Error(`invalid key uri ${this.keyId}`)
    }
    const regionName = tokens[3]
    this.kmsClient = new KMSClient({
      region: regionName,
      ...creds && {credentials: creds}
    })
  }

  supported(keyUri: string): boolean {
    return keyUri.startsWith(AwsKmsDriver.PREFIX)
  }

  async encrypt(plaintext: Buffer): Promise<Buffer> {
    const encryptCommand = new EncryptCommand({KeyId: this.keyId, Plaintext: plaintext});
    const data = await this.kmsClient.send(encryptCommand)
    return Buffer.from(data.CiphertextBlob!);
  }

  async decrypt(ciphertext: Buffer): Promise<Buffer> {
    const decryptCommand = new DecryptCommand({KeyId: this.keyId, CiphertextBlob: ciphertext});
    const data = await this.kmsClient.send(decryptCommand);
    return Buffer.from(data.Plaintext!)
  }
}
