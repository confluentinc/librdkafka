import {KmsClient, KmsDriver, registerKmsDriver} from "../kms-registry";
import {HcVaultClient} from "./hcvault-client";

export class HcVaultDriver implements KmsDriver {

  static PREFIX = 'hcvault://'
  static TOKEN_ID = 'token.id'
  static NAMESPACE = 'namespace'

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
    const tokenId = config.get(HcVaultDriver.TOKEN_ID)
    const ns = config.get(HcVaultDriver.NAMESPACE)
    return new HcVaultClient(uriPrefix, ns, tokenId)
  }
}
