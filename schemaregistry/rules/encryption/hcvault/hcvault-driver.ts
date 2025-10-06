import {KmsClient, KmsDriver, registerKmsDriver} from "../kms-registry";
import {HcVaultClient} from "./hcvault-client";

export class HcVaultDriver implements KmsDriver {

  static PREFIX = 'hcvault://'
  static TOKEN_ID = 'token.id'
  static NAMESPACE = 'namespace'
  static APPROLE_ROLE_ID = 'approle.role.id'
  static APPROLE_SECRET_ID = 'approle.secret.id'

  /**
   * Register the HashiCorp Vault driver with the KMS registry.
   */
  static register(): void {
    registerKmsDriver(new HcVaultDriver())
  }

  getKeyUrlPrefix(): string {
    return HcVaultDriver.PREFIX
  }

  newKmsClient(config: Map<string, string>, keyUrl?: string): KmsClient {
    const uriPrefix = keyUrl != null ? keyUrl : HcVaultDriver.PREFIX
    let tokenId = config.get(HcVaultDriver.TOKEN_ID)
    if (tokenId == null)
    {
      tokenId = process.env["VAULT_TOKEN"]
    }
    let ns = config.get(HcVaultDriver.NAMESPACE)
    if (ns == null)
    {
      ns = process.env["VAULT_NAMESPACE"]
    }
    let roleId = config.get(HcVaultDriver.APPROLE_ROLE_ID)
    if (roleId == null)
    {
      roleId = process.env["VAULT_APPROLE_ROLE_ID"]
    }
    let secretId = config.get(HcVaultDriver.APPROLE_SECRET_ID)
    if (secretId == null)
    {
      secretId = process.env["VAULT_APPROLE_SECRET_ID"]
    }
    return new HcVaultClient(uriPrefix, ns, tokenId, roleId, secretId)
  }
}
