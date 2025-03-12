import {
  FieldContext,
  FieldRuleExecutor,
  FieldTransform,
  FieldType,
  MAGIC_BYTE,
  RuleContext,
  RuleError,
} from "../../serde/serde";
import {RuleMode,} from "../../schemaregistry-client";
import {DekClient, Dek, DekRegistryClient, Kek} from "./dekregistry/dekregistry-client";
import {RuleRegistry} from "../../serde/rule-registry";
import {ClientConfig} from "../../rest-service";
import {RestError} from "../../rest-error";
import * as Random from './tink/random';
import * as Registry from './kms-registry'
import {KmsClient} from "./kms-registry";
import {AesGcmKey, AesGcmKeySchema} from "./tink/proto/aes_gcm_pb";
import {AesSivKey, AesSivKeySchema} from "./tink/proto/aes_siv_pb";
import {create, fromBinary, toBinary} from "@bufbuild/protobuf";
import {fromRawKey as aesGcmFromRawKey} from "./tink/aes_gcm";
import {fromRawKey as aesSivFromRawKey} from "./tink/aes_siv";
import {deepEqual} from "../../serde/json-util";

// EncryptKekName represents a kek name
const ENCRYPT_KEK_NAME = 'encrypt.kek.name'
// EncryptKmsKeyId represents a kms key ID
const ENCRYPT_KMS_KEY_ID = 'encrypt.kms.key.id'
// EncryptKmsType represents a kms type
const ENCRYPT_KMS_TYPE = 'encrypt.kms.type'
// EncryptDekAlgorithm represents a dek algorithm
const ENCRYPT_DEK_ALGORITHM = 'encrypt.dek.algorithm'
// EncryptDekExpiryDays represents dek expiry days
const ENCRYPT_DEK_EXPIRY_DAYS = 'encrypt.dek.expiry.days'

// MillisInDay represents number of milliseconds in a day
const MILLIS_IN_DAY = 24 * 60 * 60 * 1000

export enum DekFormat {
  AES128_GCM = 'AES128_GCM',
  AES256_GCM = 'AES256_GCM',
  AES256_SIV = 'AES256_SIV',
}

interface KekId {
  name: string
  deleted: boolean
}

interface DekId {
  kekName: string
  subject: string
  version: number | null
  algorithm: string
  deleted: boolean
}

export class Clock {
  now(): number {
    return Date.now()
  }
}

export class FieldEncryptionExecutor extends FieldRuleExecutor {
  client: DekClient | null = null
  clock: Clock

  /**
   * Register the field encryption executor with the rule registry.
   */
  static register(): FieldEncryptionExecutor {
    return this.registerWithClock(new Clock())
  }

  static registerWithClock(clock: Clock): FieldEncryptionExecutor {
    const executor = new FieldEncryptionExecutor(clock)
    RuleRegistry.registerRuleExecutor(executor)
    return executor
  }

  constructor(clock: Clock = new Clock()) {
    super()
    this.clock = clock
  }

  override configure(clientConfig: ClientConfig, config: Map<string, string>) {
    if (this.client != null) {
      if (!deepEqual(this.client.config(), clientConfig)) {
        throw new RuleError('executor already configured')
      }
    } else {
      this.client = DekRegistryClient.newClient(clientConfig)
    }

    if (this.config != null) {
      for (let [key, value] of config) {
        let v = this.config.get(key)
        if (v != null) {
          if (v !== value) {
            throw new RuleError('rule config key already set: {key}')
          }
        } else {
          this.config.set(key, value)
        }
      }
    } else {
      this.config = config
    }
  }

  override type(): string {
    return 'ENCRYPT'
  }

  override newTransform(ctx: RuleContext): FieldTransform {
    const cryptor = this.getCryptor(ctx)
    const kekName = this.getKekName(ctx)
    const dekExpiryDays = this.getDekExpiryDays(ctx)
    const transform =
      new FieldEncryptionExecutorTransform(this, cryptor, kekName, dekExpiryDays)
    return transform
  }

  override async close(): Promise<void> {
    if (this.client != null) {
      await this.client.close()
    }
  }

  private getCryptor(ctx: RuleContext): Cryptor {
    let dekAlgorithm = DekFormat.AES256_GCM
    const dekAlgorithmStr = ctx.getParameter(ENCRYPT_DEK_ALGORITHM)
    if (dekAlgorithmStr != null) {
      dekAlgorithm = DekFormat[dekAlgorithmStr as keyof typeof DekFormat]
    }
    const cryptor = new Cryptor(dekAlgorithm)
    return cryptor
  }

  private getKekName(ctx: RuleContext): string {
    const kekName = ctx.getParameter(ENCRYPT_KEK_NAME)
    if (kekName == null) {
      throw new RuleError('no kek name found')
    }
    if (kekName === '') {
      throw new RuleError('empty kek name')
    }
    return kekName
  }

  private getDekExpiryDays(ctx: RuleContext): number {
    const expiryDaysStr = ctx.getParameter(ENCRYPT_DEK_EXPIRY_DAYS)
    if (expiryDaysStr == null) {
      return 0
    }
    const expiryDays = Number(expiryDaysStr)
    if (isNaN(expiryDays)) {
      throw new RuleError('invalid expiry days')
    }
    if (expiryDays < 0) {
      throw new RuleError('negative expiry days')
    }
    return expiryDays
  }
}

export class Cryptor {
  static readonly EMPTY_AAD = Buffer.from([])

  dekFormat: DekFormat
  isDeterministic: boolean

  constructor(dekFormat: DekFormat) {
    this.dekFormat = dekFormat
    this.isDeterministic = dekFormat === DekFormat.AES256_SIV
  }

  private keySize(): number {
    switch (this.dekFormat) {
      case DekFormat.AES256_SIV:
        // Generate 2 256-bit keys
        return 64
      case DekFormat.AES128_GCM:
        // Generate 128-bit key
        return 16
      case DekFormat.AES256_GCM:
        // Generate 256-bit key
        return 32
      default:
        throw new RuleError('unsupported dek format')
    }
  }

  generateKey(): Buffer {
    let rawKey = Random.randBytes(this.keySize())
    switch (this.dekFormat) {
      case DekFormat.AES256_SIV:
        const aesSivKey: AesSivKey = create(AesSivKeySchema, {
          version: 0,
          keyValue: rawKey
        });
        return Buffer.from(toBinary(AesSivKeySchema, aesSivKey))
      case DekFormat.AES128_GCM:
      case DekFormat.AES256_GCM:
        const aesGcmKey: AesGcmKey = create(AesGcmKeySchema, {
          version: 0,
          keyValue: rawKey
        });
        return Buffer.from(toBinary(AesGcmKeySchema, aesGcmKey))
      default:
        throw new RuleError('unsupported dek format')
    }
  }

  async encrypt(dek: Buffer, plaintext: Buffer): Promise<Buffer> {
    let rawKey
    switch (this.dekFormat) {
      case DekFormat.AES256_SIV:
        const aesSivKey = fromBinary(AesSivKeySchema, dek)
        rawKey = aesSivKey.keyValue
        return Buffer.from(await this.encryptWithAesSiv(rawKey, plaintext))
      case DekFormat.AES128_GCM:
      case DekFormat.AES256_GCM:
        const aesGcmKey = fromBinary(AesGcmKeySchema, dek)
        rawKey = aesGcmKey.keyValue
        return Buffer.from(await this.encryptWithAesGcm(rawKey, plaintext))
      default:
        throw new RuleError('unsupported dek format')
    }
  }

  async decrypt(dek: Buffer, ciphertext: Buffer): Promise<Buffer> {
    let rawKey
    switch (this.dekFormat) {
      case DekFormat.AES256_SIV:
        const aesSivKey = fromBinary(AesSivKeySchema, dek)
        rawKey = aesSivKey.keyValue
        return Buffer.from(await this.decryptWithAesSiv(rawKey, ciphertext))
      case DekFormat.AES128_GCM:
      case DekFormat.AES256_GCM:
        const aesGcmKey = fromBinary(AesGcmKeySchema, dek)
        rawKey = aesGcmKey.keyValue
        return Buffer.from(await this.decryptWithAesGcm(rawKey, ciphertext))
      default:
        throw new RuleError('unsupported dek format')
    }
  }

  async encryptWithAesSiv(key: Uint8Array, plaintext: Uint8Array): Promise<Uint8Array> {
    const aead = await aesSivFromRawKey(key)
    return aead.encrypt(plaintext, Cryptor.EMPTY_AAD)
  }

  async decryptWithAesSiv(key: Uint8Array, ciphertext: Uint8Array): Promise<Uint8Array> {
    const aead = await aesSivFromRawKey(key)
    return aead.decrypt(ciphertext, Cryptor.EMPTY_AAD)
  }

  async encryptWithAesGcm(key: Uint8Array, plaintext: Uint8Array): Promise<Uint8Array> {
    const aead = await aesGcmFromRawKey(key)
    return aead.encrypt(plaintext, Cryptor.EMPTY_AAD)
  }

  async decryptWithAesGcm(key: Uint8Array, ciphertext: Uint8Array): Promise<Uint8Array> {
    const aead = await aesGcmFromRawKey(key)
    return aead.decrypt(ciphertext, Cryptor.EMPTY_AAD)
  }
}

export class FieldEncryptionExecutorTransform implements FieldTransform {
  private executor: FieldEncryptionExecutor
  private cryptor: Cryptor
  private kekName: string
  private kek: Kek | null = null
  private dekExpiryDays: number

  constructor(
    executor: FieldEncryptionExecutor,
    cryptor: Cryptor,
    kekName: string,
    dekExpiryDays: number,
  ) {
    this.executor = executor
    this.cryptor = cryptor
    this.kekName = kekName
    this.dekExpiryDays = dekExpiryDays
  }

  isDekRotated() {
    return this.dekExpiryDays > 0
  }

  async getKek(ctx: RuleContext) {
    if (this.kek == null) {
      this.kek = await this.getOrCreateKek(ctx)
    }
    return this.kek
  }

  async getOrCreateKek(ctx: RuleContext): Promise<Kek> {
    const isRead = ctx.ruleMode === RuleMode.READ
    const kmsType = ctx.getParameter(ENCRYPT_KMS_TYPE)
    const kmsKeyId = ctx.getParameter(ENCRYPT_KMS_KEY_ID)
    const kekId: KekId = {
      name: this.kekName,
      deleted: false,
    }
    let kek = await this.retrieveKekFromRegistry(kekId)
    if (kek == null) {
      if (isRead) {
        throw new RuleError(`no kek found for ${this.kekName} during consume`)
      }
      if (kmsType == null || kmsType.length === 0) {
        throw new RuleError(`no kms type found for ${this.kekName} during produce`)
      }
      if (kmsKeyId == null || kmsKeyId.length === 0) {
        throw new RuleError(`no kms key id found for ${this.kekName} during produce`)
      }
      kek = await this.storeKekToRegistry(kekId, kmsType, kmsKeyId, false)
      if (kek == null) {
        // handle conflicts (409)
        kek = await this.retrieveKekFromRegistry(kekId)
      }
      if (kek == null) {
        throw new RuleError(`no kek found for ${this.kekName} during produce`)
      }
    }
    if (kmsType != null && kmsType.length !== 0 && kmsType !== kek.kmsType) {
      throw new RuleError(
        `found ${this.kekName} with kms type ${kek.kmsType} which differs from rule kms type ${kmsType}`,
      )
    }
    if (kmsKeyId != null && kmsKeyId.length !== 0 && kmsKeyId !== kek.kmsKeyId) {
      throw new RuleError(
        `found ${this.kekName} with kms key id ${kek.kmsKeyId} which differs from rule kms keyId ${kmsKeyId}`,
      )
    }
    return kek
  }

  async retrieveKekFromRegistry(key: KekId): Promise<Kek | null> {
    try {
      return await this.executor.client!.getKek(key.name, key.deleted)
    } catch (err) {
      if (err instanceof RestError && err.status === 404) {
        return null
      }
      throw new RuleError(`could not get kek ${key.name}: ${err}`)
    }
  }

  async storeKekToRegistry(key: KekId, kmsType: string, kmsKeyId: string, shared: boolean): Promise<Kek | null> {
    try {
      return await this.executor.client!.registerKek(key.name, kmsType, kmsKeyId, shared)
    } catch (err) {
      if (err instanceof RestError && err.status === 409) {
        return null
      }
      throw new RuleError(`could not register kek ${key.name}: ${err}`)
    }
  }

  async getOrCreateDek(ctx: RuleContext, version: number | null): Promise<Dek> {
    const kek = await this.getKek(ctx)
    const isRead = ctx.ruleMode === RuleMode.READ
    if (version == null || version === 0) {
      version = 1
    }
    const dekId: DekId = {
      kekName: this.kekName,
      subject: ctx.subject,
      version,
      algorithm: this.cryptor.dekFormat,
      deleted: isRead
    }
    let dek = await this.retrieveDekFromRegistry(dekId)
    const isExpired = this.isExpired(ctx, dek)
    let kmsClient: KmsClient | null = null
    if (dek == null || isExpired) {
      if (isRead) {
        throw new RuleError(`no dek found for ${this.kekName} during consume`)
      }
      let encryptedDek: Buffer | null = null
      if (!kek.shared) {
        kmsClient = getKmsClient(this.executor.config!, kek)
        // Generate new dek
        const rawDek = this.cryptor.generateKey()
        encryptedDek = await kmsClient.encrypt(rawDek)
      }
      const newVersion = isExpired ? dek!.version! + 1 : null
      const newDekId: DekId = {
        kekName: this.kekName,
        subject: ctx.subject,
        version: newVersion,
        algorithm: this.cryptor.dekFormat,
        deleted: isRead,
      }
      // encryptedDek may be passed as null if kek is shared
      dek = await this.storeDekToRegistry(newDekId, encryptedDek)
      if (dek == null) {
        // handle conflicts (409)
        dek = await this.retrieveDekFromRegistry(dekId)
      }
      if (dek == null) {
        throw new RuleError(`no dek found for ${this.kekName} during produce`)
      }
    }

    const keyMaterialBytes = await this.executor.client!.getDekKeyMaterialBytes(dek)
    if (keyMaterialBytes == null) {
      if (kmsClient == null) {
        kmsClient = getKmsClient(this.executor.config!, kek)
      }
      const encryptedKeyMaterialBytes = await this.executor.client!.getDekEncryptedKeyMaterialBytes(dek)
      const rawDek = await kmsClient.decrypt(encryptedKeyMaterialBytes!)
      await this.executor.client!.setDekKeyMaterial(dek, rawDek)
    }

    return dek
  }

  async retrieveDekFromRegistry(key: DekId): Promise<Dek | null> {
    try {
        let dek: Dek
        let version = key.version
        if (version == null || version === 0) {
          version = 1
        }
        dek = await this.executor.client!.getDek(key.kekName, key.subject, key.algorithm, version, key.deleted)
        return dek != null && dek.encryptedKeyMaterial != null ? dek : null
      } catch (err) {
        if (err instanceof RestError && err.status === 404) {
          return null
        }
        throw new RuleError(`could not get dek for kek ${key.kekName}, subject ${key.subject}: ${err}`)
      }
    }

    async storeDekToRegistry(key: DekId, encryptedDek: Buffer | null): Promise<Dek | null> {
      try {
        let dek: Dek
        let encryptedDekStr: string | undefined = undefined
        if (encryptedDek != null) {
          encryptedDekStr = encryptedDek.toString('base64')
        }
        let version = key.version
        if (version == null || version === 0) {
          version = 1
        }
        dek = await this.executor.client!.registerDek(key.kekName, key.subject, key.algorithm, version, encryptedDekStr)
        return dek
      } catch (err) {
        if (err instanceof RestError && err.status === 409) {
          return null
        }
        throw new RuleError(`could not register dek for kek ${key.kekName}, subject ${key.subject}: ${err}`)
      }
  }

  isExpired(ctx: RuleContext, dek: Dek | null): boolean {
    const now = this.executor.clock.now()
    return ctx.ruleMode !== RuleMode.READ &&
      this.dekExpiryDays > 0 &&
      dek != null &&
      (now - dek.ts!) / MILLIS_IN_DAY >= this.dekExpiryDays
  }

  async transform(ctx: RuleContext, fieldCtx: FieldContext, fieldValue: any): Promise<any> {
    if (fieldValue == null) {
      return null
    }
    switch (ctx.ruleMode) {
      case RuleMode.WRITE: {
        let plaintext = this.toBytes(fieldCtx.type, fieldValue)
        if (plaintext == null) {
          throw new RuleError(`type ${fieldCtx.type} not supported for encryption`)
        }
        let version: number | null = null
        if (this.isDekRotated()) {
          version = -1
        }
        let dek = await this.getOrCreateDek(ctx, version)
        let keyMaterialBytes = await this.executor.client!.getDekKeyMaterialBytes(dek)
        let ciphertext = await this.cryptor.encrypt(keyMaterialBytes!, plaintext)
        if (this.isDekRotated()) {
          ciphertext = this.prefixVersion(dek.version!, ciphertext)
        }
        if (fieldCtx.type === FieldType.STRING) {
          return ciphertext.toString('base64')
        } else {
          return this.toObject(fieldCtx.type, ciphertext)
        }
      }
      case RuleMode.READ: {
        let ciphertext
        if (fieldCtx.type === FieldType.STRING) {
          ciphertext = Buffer.from(fieldValue, 'base64')
        } else {
          ciphertext = this.toBytes(fieldCtx.type, fieldValue)
        }
        if (ciphertext == null) {
          return fieldValue
        }
        let version: number | null = null
        if (this.isDekRotated()) {
          version = this.extractVersion(ciphertext)
          if (version == null) {
            throw new RuleError('no version found in ciphertext')
          }
          ciphertext = ciphertext.subarray(5)
        }
        let dek = await this.getOrCreateDek(ctx, version)
        let keyMaterialBytes = await this.executor.client!.getDekKeyMaterialBytes(dek)
        let plaintext = await this.cryptor.decrypt(keyMaterialBytes!, ciphertext)
        return this.toObject(fieldCtx.type, plaintext)
      }
      default:
        throw new RuleError(`unsupported rule mode ${ctx.ruleMode}`)
    }
  }

  prefixVersion(version: number, ciphertext: Buffer): Buffer {
    const versionBuf = Buffer.alloc(4)
    versionBuf.writeInt32BE(version)
    return Buffer.concat([MAGIC_BYTE, versionBuf, ciphertext])
  }

  extractVersion(ciphertext: Buffer): number | null {
    let magicByte = ciphertext.subarray(0, 1)
    if (!magicByte.equals(MAGIC_BYTE)) {
      throw new RuleError(
        `Message encoded with magic byte ${JSON.stringify(magicByte)}, expected ${JSON.stringify(
          MAGIC_BYTE,
        )}`,
      )
    }
    return ciphertext.subarray(1, 5).readInt32BE(0)
  }

  toBytes(type: FieldType, value: any): Buffer | null {
    switch (type) {
      case FieldType.BYTES:
        return value as Buffer
      case FieldType.STRING:
        return Buffer.from(value as string)
      default:
        return null
    }
  }

  toObject(type: FieldType, value: Buffer): any {
    switch (type) {
      case FieldType.BYTES:
        return value
      case FieldType.STRING:
        return value.toString()
      default:
        return null
    }
  }
}

function getKmsClient(config: Map<string, string>, kek: Kek): KmsClient {
  let keyUrl = kek.kmsType + '://' + kek.kmsKeyId
  let kmsClient = Registry.getKmsClient(keyUrl)
  if (kmsClient == null) {
    let kmsDriver = Registry.getKmsDriver(keyUrl)
    kmsClient = kmsDriver.newKmsClient(config, keyUrl)
    Registry.registerKmsClient(kmsClient)
  }
  return kmsClient
}
