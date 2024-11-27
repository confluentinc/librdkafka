import {KmsClient} from "../kms-registry";
import {Cryptor, DekFormat} from "../encrypt-executor";
import * as Hkdf from '../tink/hkdf';
import {LocalKmsDriver} from "./local-driver";
import {AesGcmKey, AesGcmKeySchema} from "../tink/proto/aes_gcm_pb";
import {create, toBinary} from "@bufbuild/protobuf";

export class LocalKmsClient implements KmsClient {

  private secret: string
  private cryptor: Cryptor

  constructor(secret: string) {
    this.secret = secret
    this.cryptor = new Cryptor(DekFormat.AES128_GCM)
  }

  async getKey(): Promise<Buffer> {
    const rawKey = await Hkdf.compute(16, 'SHA-256', Buffer.from(this.secret, 'utf8'), new Uint8Array(0));
    const aesGcmKey: AesGcmKey = create(AesGcmKeySchema, {
      version: 0,
      keyValue: rawKey
    });
    return Buffer.from(toBinary(AesGcmKeySchema, aesGcmKey))
  }

  supported(keyUri: string): boolean {
    return keyUri.startsWith(LocalKmsDriver.PREFIX)
  }

  async encrypt(plaintext: Buffer): Promise<Buffer> {
    return this.cryptor.encrypt(await this.getKey(), plaintext)
  }

  async decrypt(ciphertext: Buffer): Promise<Buffer> {
    return this.cryptor.decrypt(await this.getKey(), ciphertext)
  }
}
