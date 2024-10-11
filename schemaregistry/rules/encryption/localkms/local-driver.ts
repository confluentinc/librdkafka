import {KmsClient, KmsDriver, registerKmsDriver} from "../kms-registry";
import {LocalKmsClient} from "./local-client";

export class LocalKmsDriver implements KmsDriver {

  static PREFIX: string = 'local-kms://'
  static SECRET: string = 'secret'

  /**
   * Register the local KMS driver with the KMS registry.
   */
  static register(): void {
    registerKmsDriver(new LocalKmsDriver())
  }

  getKeyUrlPrefix(): string {
    return LocalKmsDriver.PREFIX
  }

  newKmsClient(config: Map<string, string>, keyUrl: string): KmsClient {
    const secret = config.get(LocalKmsDriver.SECRET)
    return new LocalKmsClient(secret)
  }
}
