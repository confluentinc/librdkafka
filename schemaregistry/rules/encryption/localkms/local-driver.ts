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
    let secret = config.get(LocalKmsDriver.SECRET)
    if (secret == null) {
      secret = process.env['LOCAL_SECRET']
    }
    if (secret == null) {
      throw new Error('cannot load secret')
    }
    return new LocalKmsClient(secret)
  }
}
