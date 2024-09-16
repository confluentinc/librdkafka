import {KmsClient, KmsDriver, registerKmsDriver} from "../kms-registry";
import {ClientSecretCredential, DefaultAzureCredential, TokenCredential} from '@azure/identity'
import {AzureKmsClient} from "./azure-client";

export class AzureKmsDriver implements KmsDriver {

  static PREFIX = 'azure-kms://'
  static TENANT_ID = 'tenant.id'
  static CLIENT_ID = 'client.id'
  static CLIENT_SECRET = 'client.secret'

  static register(): void {
    registerKmsDriver(new AzureKmsDriver())
  }

  getKeyUrlPrefix(): string {
    return AzureKmsDriver.PREFIX
  }

  newKmsClient(config: Map<string, string>, keyUrl?: string): KmsClient {
    const uriPrefix = keyUrl != null ? keyUrl : AzureKmsDriver.PREFIX
    const tenantId = config.get(AzureKmsDriver.TENANT_ID)
    const clientId = config.get(AzureKmsDriver.CLIENT_ID)
    const clientSecret = config.get(AzureKmsDriver.CLIENT_SECRET)
    let creds: TokenCredential
    if (tenantId != null && clientId != null && clientSecret != null) {
      creds = new ClientSecretCredential(tenantId, clientId, clientSecret)
    } else {
      creds = new DefaultAzureCredential()
    }
    return new AzureKmsClient(uriPrefix, creds)
  }
}
