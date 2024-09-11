import {KmsClient, KmsDriver, registerKmsDriver} from "../kms-registry";
import {AwsKmsClient} from "./aws-client";
import {AwsCredentialIdentity} from "@smithy/types";

export class AwsKmsDriver implements KmsDriver {

  static PREFIX = 'aws-kms://'
  static ACCESS_KEY_ID = 'access.key.id'
  static SECRET_ACCESS_KEY = 'secret.access.key'

  static register(): void {
    registerKmsDriver(new AwsKmsDriver())
  }

  getKeyUrlPrefix(): string {
    return AwsKmsDriver.PREFIX
  }

  newKmsClient(config: Map<string, string>, keyUrl?: string): KmsClient {
    const uriPrefix = keyUrl != null ? keyUrl : AwsKmsDriver.PREFIX
    const key = config.get(AwsKmsDriver.ACCESS_KEY_ID)
    const secret = config.get(AwsKmsDriver.SECRET_ACCESS_KEY)
    let creds: AwsCredentialIdentity | undefined
    if (key != null && secret != null) {
      creds = {accessKeyId: key, secretAccessKey: secret}
    }
    return new AwsKmsClient(uriPrefix, creds)
  }
}
