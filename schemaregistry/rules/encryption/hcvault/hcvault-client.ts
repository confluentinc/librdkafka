import {KmsClient} from "../kms-registry";
import {HcVaultDriver} from "./hcvault-driver";
import NodeVault from "node-vault";

export class HcVaultClient implements KmsClient {

  private kmsClient: NodeVault.client
  private keyUri: string
  private keyId: string
  private keyName: string
  private authPromise?: Promise<void>

  constructor(keyUri: string, namespace?: string, token?: string,
              roleId?: string, secretId?: string) {
    if (!keyUri.startsWith(HcVaultDriver.PREFIX)) {
      throw new Error(`key uri must start with ${HcVaultDriver.PREFIX}`)
    }
    this.keyUri = keyUri
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
    if (roleId != null && secretId != null) {
      this.authPromise = this.kmsClient.approleLogin({role_id: roleId, secret_id: secretId})
        .then((result) => {
          this.kmsClient.token = result.auth.client_token
        })
    }
  }

  supported(keyUri: string): boolean {
    return this.keyUri === keyUri
  }

  private async ensureAuthenticated(): Promise<void> {
    if (this.authPromise) {
      await this.authPromise
      this.authPromise = undefined // Clear after first use
    }
  }

  async encrypt(plaintext: Buffer): Promise<Buffer> {
    await this.ensureAuthenticated()
    const response = await this.kmsClient.encryptData({name: this.keyName, plaintext: plaintext.toString('base64') })
    let data = response.data.ciphertext
    return Buffer.from(data, 'utf8')
  }

  async decrypt(ciphertext: Buffer): Promise<Buffer> {
    await this.ensureAuthenticated()
    const response = await this.kmsClient.decryptData({name: this.keyName, ciphertext: ciphertext.toString('utf8') })
    let data = response.data.plaintext
    return Buffer.from(data, 'base64');
  }
}
