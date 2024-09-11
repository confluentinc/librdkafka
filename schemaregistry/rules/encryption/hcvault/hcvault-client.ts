import {KmsClient} from "../kms-registry";
import {HcVaultDriver} from "./hcvault-driver";
import NodeVault from "node-vault";

export class HcVaultClient implements KmsClient {

  private kmsClient: NodeVault.client
  private keyId: string
  private keyName: string

  constructor(keyUri: string, namespace?: string, token?: string) {
    if (token == null)
    {
      namespace = process.env["VAULT_NAMESPACE"]
    }
    if (!keyUri.startsWith(HcVaultDriver.PREFIX)) {
      throw new Error(`key uri must start with ${HcVaultDriver.PREFIX}`)
    }
    this.keyId = keyUri.substring(HcVaultDriver.PREFIX.length)
    let url = new URL(this.keyId)
    let parts = url.pathname.split('/')
    if (parts.length === 0) {
      throw new Error('key uri must contain a key name')
    }
    this.keyName = parts.pop()!
    this.kmsClient = NodeVault({
      endpoint: url.protocol + '//' + url.host,
      ...namespace && { namespace },
      ...token && { token },
      apiVersion: 'v1',
    })
  }

  supported(keyUri: string): boolean {
    return keyUri.startsWith(HcVaultDriver.PREFIX)
  }

  async encrypt(plaintext: Buffer): Promise<Buffer> {
    const data = await this.kmsClient.encryptData({name: this.keyName, plainText: plaintext.toString('base64') })
    return Buffer.from(data.ciphertext, 'base64')
  }

  async decrypt(ciphertext: Buffer): Promise<Buffer> {
    const data = await this.kmsClient.decryptData({name: this.keyName, cipherText: ciphertext.toString('base64') })
    return Buffer.from(data.plaintext, 'base64')
  }
}
