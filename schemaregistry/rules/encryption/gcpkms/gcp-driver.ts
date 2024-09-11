import {KmsClient, KmsDriver, registerKmsDriver} from "../kms-registry";
import {GcpKmsClient} from "./gcp-client";

export class GcpKmsDriver implements KmsDriver {

  static PREFIX = 'gcp-kms://'
  static ACCOUNT_TYPE = "account.type";
  static CLIENT_ID= "client.id";
  static CLIENT_EMAIL = "client.email";
  static PRIVATE_KEY_ID = "private.key.id";
  static PRIVATE_KEY= "private.key";

  static register(): void {
    registerKmsDriver(new GcpKmsDriver())
  }

  getKeyUrlPrefix(): string {
    return GcpKmsDriver.PREFIX
  }

  newKmsClient(config: Map<string, string>, keyUrl?: string): KmsClient {
    const uriPrefix = keyUrl != null ? keyUrl : GcpKmsDriver.PREFIX
    let accountType = config.get(GcpKmsDriver.ACCOUNT_TYPE)
    const clientId = config.get(GcpKmsDriver.CLIENT_ID)
    const clientEmail = config.get(GcpKmsDriver.CLIENT_EMAIL)
    const privateKeyId = config.get(GcpKmsDriver.PRIVATE_KEY_ID)
    const privateKey = config.get(GcpKmsDriver.PRIVATE_KEY)
    let creds: GcpCredentials | undefined
    if (clientId != null && clientEmail != null && privateKeyId != null && privateKey != null) {
      if (accountType == null) {
        accountType = "service_account"
      }
      creds = {
        ...accountType && {type: accountType},
        private_key_id: privateKeyId,
        private_key: privateKey,
        client_email: clientEmail,
        client_id: clientId,
      }
    }
    return new GcpKmsClient(uriPrefix, creds)
  }
}

export interface GcpCredentials {
  type?: string
  private_key_id?: string
  private_key?: string
  client_email?: string
  client_id?: string
}
