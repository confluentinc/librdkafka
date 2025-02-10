import {match} from './wildcard-matcher';
import {
  Client,
  Rule,
  RuleMode,
  RuleSet,
  SchemaInfo,
  SchemaMetadata
} from "../schemaregistry-client";
import {RuleRegistry} from "./rule-registry";
import {ClientConfig} from "../rest-service";

export enum SerdeType {
  KEY = 'KEY',
  VALUE = 'VALUE'
}

export const MAGIC_BYTE = Buffer.alloc(1)

/**
 * SerializationError represents a serialization error
 */
export class SerializationError extends Error {

  constructor(message?: string) {
    super(message)
  }
}

export interface SerdeConfig {
  // useLatestVersion specifies whether to use the latest schema version
  useLatestVersion?: boolean
  // useLatestWithMetadata specifies whether to use the latest schema with metadata
  useLatestWithMetadata?: { [key: string]: string };
  // cacheCapacity specifies the cache capacity
  cacheCapacity?: number,
  // cacheLatestTtlSecs specifies the cache latest TTL in seconds
  cacheLatestTtlSecs?: number
  // ruleConfig specifies configuration options to the rules
  ruleConfig?: { [key: string]: string };
  // subjectNameStrategy specifies a function to generate a subject name
  subjectNameStrategy?: SubjectNameStrategyFunc
}

export type RefResolver = (client: Client, info: SchemaInfo) => Promise<Map<string, string>>

/**
 * Serde represents a serializer/deserializer
 */
export abstract class Serde {
  client: Client
  serdeType: SerdeType
  conf: SerdeConfig
  fieldTransformer: FieldTransformer | null = null
  ruleRegistry: RuleRegistry

  protected constructor(client: Client, serdeType: SerdeType, conf: SerdeConfig, ruleRegistry?: RuleRegistry) {
    this.client = client
    this.serdeType = serdeType
    this.conf = conf
    this.ruleRegistry = ruleRegistry ?? RuleRegistry.getGlobalInstance()
  }

  abstract config(): SerdeConfig

  close(): void {
    return
  }

  subjectName(topic: string, info?: SchemaInfo): string {
    const strategy = this.conf.subjectNameStrategy ?? TopicNameStrategy
    return strategy(topic, this.serdeType, info)
  }

  async resolveReferences(client: Client, schema: SchemaInfo, deps: Map<string, string>, format?: string): Promise<void> {
    let references = schema.references
    if (references == null) {
      return
    }
    for (let ref of references) {
      let metadata = await client.getSchemaMetadata(ref.subject, ref.version, true, format)
      deps.set(ref.name, metadata.schema)
      await this.resolveReferences(client, metadata, deps)
    }
  }

  async executeRules(subject: string, topic: string, ruleMode: RuleMode,
               source: SchemaInfo | null, target: SchemaInfo | null, msg: any,
               inlineTags: Map<string, Set<string>> | null): Promise<any> {
    if (msg == null || target == null) {
      return msg
    }
    let rules: Rule[] | undefined
    switch (ruleMode) {
      case RuleMode.UPGRADE:
        rules = target.ruleSet?.migrationRules
        break
      case RuleMode.DOWNGRADE:
        rules = source?.ruleSet?.migrationRules?.map(x => x).reverse()
        break
      default:
        rules = target.ruleSet?.domainRules
        if (ruleMode === RuleMode.READ) {
          // Execute read rules in reverse order for symmetry
          rules = rules?.map(x => x).reverse()
        }
        break
    }
    if (rules == null) {
      return msg
    }
    for (let i = 0; i < rules.length; i++ ) {
      let rule = rules[i]
      if (this.isDisabled(rule)) {
        continue
      }
      let mode = rule.mode
      switch (mode) {
        case RuleMode.WRITEREAD:
          if (ruleMode !== RuleMode.WRITE && ruleMode !== RuleMode.READ) {
            continue
          }
          break
        case RuleMode.UPDOWN:
          if (ruleMode !== RuleMode.UPGRADE && ruleMode !== RuleMode.DOWNGRADE) {
            continue
          }
          break
        default:
          if (mode !== ruleMode) {
            continue
          }
          break
      }
      let ctx = new RuleContext(source, target, subject, topic,
        this.serdeType === SerdeType.KEY, ruleMode, rule, i, rules, inlineTags, this.fieldTransformer!)
      let ruleExecutor = this.ruleRegistry.getExecutor(rule.type)
      if (ruleExecutor == null) {
        await this.runAction(ctx, ruleMode, rule, this.getOnFailure(rule), msg,
          new Error(`could not find rule executor of type ${rule.type}`), 'ERROR')
        return msg
      }
      try {
        let result = await ruleExecutor.transform(ctx, msg)
        switch (rule.kind) {
          case 'CONDITION':
            if (result === false) {
              throw new RuleConditionError(rule)
            }
            break
          case 'TRANSFORM':
            msg = result
            break
        }
        await this.runAction(ctx, ruleMode, rule, msg != null
            ? this.getOnSuccess(rule) : this.getOnFailure(rule),
          msg, null, msg != null ? 'NONE' : 'ERROR')
      } catch (error) {
        if (error instanceof SerializationError) {
          throw error
        }
        await this.runAction(ctx, ruleMode, rule, this.getOnFailure(rule), msg, error as Error, 'ERROR')
      }
    }
    return msg
  }

  getOnSuccess(rule: Rule): string | undefined {
    let override = this.ruleRegistry.getOverride(rule.type)
    if (override != null && override.onSuccess != null) {
      return override.onSuccess
    }
    return rule.onSuccess
  }

  getOnFailure(rule: Rule): string | undefined {
    let override = this.ruleRegistry.getOverride(rule.type)
    if (override != null && override.onFailure != null) {
      return override.onFailure
    }
    return rule.onFailure
  }

  isDisabled(rule: Rule): boolean | undefined {
    let override = this.ruleRegistry.getOverride(rule.type)
    if (override != null && override.disabled != null) {
      return override.disabled
    }
    return rule.disabled
  }

  async runAction(ctx: RuleContext, ruleMode: RuleMode, rule: Rule, action: string | undefined,
            msg: any, err: Error | null, defaultAction: string): Promise<void> {
    let actionName = this.getRuleActionName(rule, ruleMode, action)
    if (actionName == null) {
      actionName = defaultAction
    }
    let ruleAction = this.getRuleAction(ctx, actionName)
    if (ruleAction == null) {
      throw new RuleError(`Could not find rule action of type ${actionName}`)
    }
    try {
      await ruleAction.run(ctx, msg, err)
    } catch (error) {
      if (error instanceof SerializationError) {
        throw error
      }
      console.warn("could not run post-rule action %s: %s", actionName, error)
    }
  }

  getRuleActionName(rule: Rule, ruleMode: RuleMode, actionName: string | undefined): string | null {
    if (actionName == null || actionName === '') {
      return null
    }
    if ((rule.mode === RuleMode.WRITEREAD || rule.mode === RuleMode.UPDOWN) && actionName.includes(',')) {
      let parts = actionName.split(',')
      switch (ruleMode) {
        case RuleMode.WRITE:
        case RuleMode.UPGRADE:
          return parts[0]
        case RuleMode.READ:
        case RuleMode.DOWNGRADE:
          return parts[1]
      }
    }
    return actionName
  }

  getRuleAction(ctx: RuleContext, actionName: string): RuleAction | undefined {
    if (actionName === 'ERROR') {
      return new ErrorAction();
    } else if (actionName === 'NONE') {
      return new NoneAction()
    }
    return this.ruleRegistry.getAction(actionName)
  }
}

/**
 * SerializerConfig represents a serializer configuration
 */
export interface SerializerConfig extends SerdeConfig {
  // autoRegisterSchemas determines whether to automatically register schemas
  autoRegisterSchemas?: boolean
  // useSchemaID specifies a schema ID to use
  useSchemaId?: number
  // normalizeSchemas determines whether to normalize schemas
  normalizeSchemas?: boolean
}

/**
 * Serializer represents a serializer
 */
export abstract class Serializer extends Serde {
  protected constructor(client: Client, serdeType: SerdeType, conf: SerializerConfig, ruleRegistry?: RuleRegistry) {
    super(client, serdeType, conf, ruleRegistry)
  }

  override config(): SerializerConfig {
    return this.conf as SerializerConfig
  }

  /**
   * Serialize serializes a message
   * @param topic - the topic
   * @param msg - the message
   */
  abstract serialize(topic: string, msg: any): Promise<Buffer>

  // GetID returns a schema ID for the given schema
  async getId(topic: string, msg: any, info?: SchemaInfo, format?: string): Promise<[number, SchemaInfo]> {
    let autoRegister = this.config().autoRegisterSchemas
    let useSchemaId = this.config().useSchemaId
    let useLatestWithMetadata = this.config().useLatestWithMetadata
    let useLatest = this.config().useLatestVersion
    let normalizeSchema = this.config().normalizeSchemas

    let id = -1
    let subject = this.subjectName(topic, info)
    if (autoRegister) {
      id = await this.client.register(subject, info!, Boolean(normalizeSchema))
    } else if (useSchemaId != null && useSchemaId >= 0) {
      info = await this.client.getBySubjectAndId(subject, useSchemaId, format)
      id = useSchemaId
    } else if (useLatestWithMetadata != null && Object.keys(useLatestWithMetadata).length !== 0) {
      let metadata = await this.client.getLatestWithMetadata(subject, useLatestWithMetadata, true, format)
      info = metadata
      id = metadata.id
    } else if (useLatest) {
      let metadata = await this.client.getLatestSchemaMetadata(subject, format)
      info = metadata
      id = metadata.id
    } else {
      id = await this.client.getId(subject, info!, Boolean(normalizeSchema))
    }
    return [id, info!]
  }

  writeBytes(id: number, msgBytes: Buffer): Buffer {
    const idBuffer = Buffer.alloc(4)
    idBuffer.writeInt32BE(id, 0)
    return Buffer.concat([MAGIC_BYTE, idBuffer, msgBytes])
  }
}

/**
 * DeserializerConfig represents a deserializer configuration
 */
export type DeserializerConfig = SerdeConfig

/**
 * Migration represents a migration
 */
export interface Migration {
  ruleMode: RuleMode
  source: SchemaMetadata | null
  target: SchemaMetadata | null
}

/**
 * Deserializer represents a deserializer
 */
export abstract class Deserializer extends Serde {
  protected constructor(client: Client, serdeType: SerdeType, conf: DeserializerConfig, ruleRegistry?: RuleRegistry) {
    super(client, serdeType, conf, ruleRegistry)
  }

  override config(): DeserializerConfig {
    return this.conf as DeserializerConfig
  }

  /**
   * Deserialize deserializes a message
   * @param topic - the topic
   * @param payload - the payload
   */
  abstract deserialize(topic: string, payload: Buffer): Promise<any>

  async getSchema(topic: string, payload: Buffer, format?: string): Promise<SchemaInfo> {
    const magicByte = payload.subarray(0, 1)
    if (!magicByte.equals(MAGIC_BYTE)) {
      throw new SerializationError(
        `Message encoded with magic byte ${JSON.stringify(magicByte)}, expected ${JSON.stringify(
          MAGIC_BYTE,
        )}`,
      )
    }
    const id = payload.subarray(1, 5).readInt32BE(0)
    let subject = this.subjectName(topic)
    return await this.client.getBySubjectAndId(subject, id, format)
  }

  async getReaderSchema(subject: string, format?: string): Promise<SchemaMetadata | null> {
    let useLatestWithMetadata = this.config().useLatestWithMetadata
    let useLatest = this.config().useLatestVersion
    if (useLatestWithMetadata != null && Object.keys(useLatestWithMetadata).length !== 0) {
      return await this.client.getLatestWithMetadata(subject, useLatestWithMetadata, true, format)
    }
    if (useLatest) {
      return await this.client.getLatestSchemaMetadata(subject, format)
    }
    return null
  }

  hasRules(ruleSet: RuleSet, mode: RuleMode): boolean {
    switch (mode) {
      case RuleMode.UPGRADE:
      case RuleMode.DOWNGRADE:
        return this.checkRules(ruleSet?.migrationRules, (ruleMode: RuleMode): boolean =>
          ruleMode === mode || ruleMode === RuleMode.UPDOWN)
      case RuleMode.UPDOWN:
        return this.checkRules(ruleSet?.migrationRules, (ruleMode: RuleMode): boolean =>
          ruleMode === mode)
      case RuleMode.WRITE:
      case RuleMode.READ:
        return this.checkRules(ruleSet?.domainRules, (ruleMode: RuleMode): boolean =>
          ruleMode === mode || ruleMode === RuleMode.WRITEREAD)
      case RuleMode.WRITEREAD:
        return this.checkRules(ruleSet?.domainRules, (ruleMode: RuleMode): boolean =>
          ruleMode === mode)
    }
  }

  checkRules(rules: Rule[] | undefined, filter: (ruleMode: RuleMode) => boolean): boolean {
    if (rules == null) {
      return false
    }
    for (let rule of rules) {
      let ruleMode = rule.mode
      if (ruleMode && filter(ruleMode)) {
        return true
      }
    }
    return false
  }

  async getMigrations(subject: string, sourceInfo: SchemaInfo,
                target: SchemaMetadata, format?: string): Promise<Migration[]> {
    let version = await this.client.getVersion(subject, sourceInfo, false, true)
    let source: SchemaMetadata = {
      id: 0,
      version:    version,
      schema: sourceInfo.schema,
      references: sourceInfo.references,
      metadata: sourceInfo.metadata,
      ruleSet: sourceInfo.ruleSet,
    }
    let migrationMode: RuleMode
    let migrations: Migration[] = []
    let first: SchemaMetadata
    let last: SchemaMetadata
    if (source.version! < target.version!) {
      migrationMode = RuleMode.UPGRADE
      first = source
      last = target
    } else if (source.version! > target.version!) {
      migrationMode = RuleMode.DOWNGRADE
      first = target
      last = source
    } else {
      return migrations
    }
    let previous: SchemaMetadata | null = null
    let versions = await this.getSchemasBetween(subject, first, last, format)
    for (let i = 0; i < versions.length; i++) {
      let version = versions[i]
      if (i === 0) {
        previous = version
        continue
      }
      if (version.ruleSet != null && this.hasRules(version.ruleSet, migrationMode)) {
        let m: Migration
        if (migrationMode === RuleMode.UPGRADE) {
          m = {
            ruleMode: migrationMode,
            source: previous,
            target: version,
          }
        } else {
          m = {
            ruleMode: migrationMode,
            source: version,
            target: previous,
          }
        }
        migrations.push(m)
      }
      previous = version
    }
    if (migrationMode === RuleMode.DOWNGRADE) {
      migrations = migrations.reverse()
    }
    return migrations
  }

  async getSchemasBetween(subject: string, first: SchemaMetadata,
                    last: SchemaMetadata, format?: string): Promise<SchemaMetadata[]> {
    if (last.version!-first.version! <= 1) {
      return [first, last]
    }
    let version1 = first.version!
    let version2 = last.version!
    let result = [first]
    for (let i = version1 + 1; i < version2; i++) {
      let meta = await this.client.getSchemaMetadata(subject, i, true, format)
      result.push(meta)
    }
    result.push(last)
    return result
  }

  async executeMigrations(migrations: Migration[], subject: string, topic: string, msg: any): Promise<any> {
    for (let migration of migrations) {
      // TODO fix source, target?
      msg = await this.executeRules(subject, topic, migration.ruleMode, migration.source, migration.target, msg, null)
    }
    return msg
  }
}

/**
 * SubjectNameStrategyFunc determines the subject from the given parameters
 */
export type SubjectNameStrategyFunc = (
  topic: string,
  serdeType: SerdeType,
  schema?: SchemaInfo,
) => string

/**
 * TopicNameStrategy creates a subject name by appending -[key|value] to the topic name.
 * @param topic - the topic name
 * @param serdeType - the serde type
 */
export const TopicNameStrategy: SubjectNameStrategyFunc = (topic: string, serdeType: SerdeType) => {
  let suffix = '-value'
  if (serdeType === SerdeType.KEY) {
    suffix = '-key'
  }
  return topic + suffix
}

/**
 * RuleContext represents a rule context
 */
export class RuleContext {
  source: SchemaInfo | null
  target: SchemaInfo
  subject: string
  topic: string
  isKey: boolean
  ruleMode: RuleMode
  rule: Rule
  index: number
  rules: Rule[]
  inlineTags: Map<string, Set<string>> | null
  fieldTransformer: FieldTransformer
  private fieldContexts: FieldContext[]

  constructor(source: SchemaInfo | null, target: SchemaInfo, subject: string, topic: string,
              isKey: boolean, ruleMode: RuleMode, rule: Rule, index: number, rules: Rule[],
              inlineTags: Map<string, Set<string>> | null, fieldTransformer: FieldTransformer) {
    this.source = source
    this.target = target
    this.subject = subject
    this.topic = topic
    this.isKey = isKey
    this.ruleMode = ruleMode
    this.rule = rule
    this.index = index
    this.rules = rules
    this.inlineTags = inlineTags
    this.fieldTransformer = fieldTransformer
    this.fieldContexts = []
  }

  getParameter(name: string): string | null {
    const params = this.rule.params
    if (params != null) {
      let value = params[name]
      if (value != null) {
        return value
      }
    }
    let metadata = this.target.metadata
    if (metadata != null && metadata.properties != null) {
      let value = metadata.properties[name]
      if (value != null) {
        return value
      }
    }
    return null
  }

  getInlineTags(name: string): Set<string> {
    let tags = this.inlineTags?.get(name)
    if (tags != null) {
      return tags
    }
    return new Set<string>()
  }

  currentField(): FieldContext | null {
    let size = this.fieldContexts.length
    if (size === 0) {
      return null
    }
    return this.fieldContexts[size - 1]
  }

  enterField(containingMessage: any, fullName: string, name: string, fieldType: FieldType,
             tags: Set<string> | null): FieldContext {
    let allTags = new Set<string>(tags ?? this.getInlineTags(fullName))
    for (let v of this.getTags(fullName)) {
      allTags.add(v)
    }
    let fieldContext = new FieldContext(
      containingMessage,
      fullName,
      name,
      fieldType,
      allTags
    )
    this.fieldContexts.push(fieldContext)
    return fieldContext
  }

  getTags(fullName: string): Set<string> {
    let tags = new Set<string>()
    let metadata = this.target.metadata
    if (metadata?.tags != null) {
      for (let [k, v] of Object.entries(metadata.tags)) {
        if (match(fullName, k)) {
          for (let tag of v) {
            tags.add(tag)
          }
        }
      }
    }
    return tags
  }

  leaveField(): void {
    let size = this.fieldContexts.length - 1
    this.fieldContexts = this.fieldContexts.slice(0, size)
  }
}

export interface RuleBase {
  configure(clientConfig: ClientConfig, config: Map<string, string>): void

  type(): string;

  close(): void
}

/**
 * RuleExecutor represents a rule executor
 */
export interface RuleExecutor extends RuleBase {
  transform(ctx: RuleContext, msg: any): Promise<any>
}

/**
 * FieldTransformer represents a field transformer
 */
export type FieldTransformer = (ctx: RuleContext, fieldTransform: FieldTransform, msg: any) => any;

/**
 * FieldTransform represents a field transform
 */
export interface FieldTransform {
  transform(ctx: RuleContext, fieldCtx: FieldContext, fieldValue: any): Promise<any>;
}

/**
 * FieldRuleExecutor represents a field rule executor
 */
export abstract class FieldRuleExecutor implements RuleExecutor {
  config: Map<string, string> | null = null

  abstract configure(clientConfig: ClientConfig, config: Map<string, string>): void

  abstract type(): string;

  abstract newTransform(ctx: RuleContext): FieldTransform;

  async transform(ctx: RuleContext, msg: any): Promise<any> {
    // TODO preserve source
    switch (ctx.ruleMode) {
      case RuleMode.WRITE:
      case RuleMode.UPGRADE:
        for (let i = 0; i < ctx.index; i++) {
          let otherRule = ctx.rules[i]
          if (areTransformsWithSameTag(ctx.rule, otherRule)) {
            // ignore this transform if an earlier one has the same tag
            return msg
          }
        }
        break
      case RuleMode.READ:
      case RuleMode.DOWNGRADE:
        for (let i = ctx.index + 1; i < ctx.rules.length; i++) {
          let otherRule = ctx.rules[i]
          if (areTransformsWithSameTag(ctx.rule, otherRule)) {
            // ignore this transform if a later one has the same tag
            return msg
          }
        }
        break
    }
    let fieldTransform = this.newTransform(ctx)
    return ctx.fieldTransformer(ctx, fieldTransform, msg)
  }

  abstract close(): void
}

function areTransformsWithSameTag(rule1: Rule, rule2: Rule): boolean {
  return rule1.tags != null && rule1.tags.length > 0
    && rule1.kind === 'TRANSFORM'
    && rule1.kind === rule2.kind
    && rule1.mode === rule2.mode
    && rule1.type === rule2.type
    && rule1.tags === rule2.tags
}

/**
 * FieldContext represents a field context
 */
export class FieldContext {
  containingMessage: any
  fullName: string
  name: string
  type: FieldType
  tags: Set<string>

  constructor(containingMessage: any, fullName: string, name: string, fieldType: FieldType, tags: Set<string>) {
    this.containingMessage = containingMessage
    this.fullName = fullName
    this.name = name
    this.type = fieldType
    this.tags = new Set<string>(tags)
  }

  isPrimitive(): boolean {
    let t = this.type
    return t === FieldType.STRING || t === FieldType.BYTES || t === FieldType.INT
      || t === FieldType.LONG || t === FieldType.FLOAT || t === FieldType.DOUBLE
      || t === FieldType.BOOLEAN || t === FieldType.NULL
  }

  typeName(): string {
    return this.type.toString()
  }
}

export enum FieldType {
  RECORD = 'RECORD',
  ENUM = 'ENUM',
  ARRAY = 'ARRAY',
  MAP = 'MAP',
  COMBINED = 'COMBINED',
  FIXED = 'FIXED',
  STRING = 'STRING',
  BYTES = 'BYTES',
  INT = 'INT',
  LONG = 'LONG',
  FLOAT = 'FLOAT',
  DOUBLE = 'DOUBLE',
  BOOLEAN = 'BOOLEAN',
  NULL = 'NULL',
}

/**
 * RuleAction represents a rule action
 */
export interface RuleAction extends RuleBase {
  run(ctx: RuleContext, msg: any, err: Error | null): Promise<void>
}

/**
 * ErrorAction represents an error action
 */
export class ErrorAction implements RuleAction {
  configure(clientConfig: ClientConfig, config: Map<string, string>): void {
  }

  type(): string {
    return 'ERROR'
  }

  async run(ctx: RuleContext, msg: any, err: Error): Promise<void> {
    throw new SerializationError(err.message)
  }

  close(): void {
  }
}

/**
 * NoneAction represents a no-op action
 */
export class NoneAction implements RuleAction {
  configure(clientConfig: ClientConfig, config: Map<string, string>): void {
  }

  type(): string {
    return 'NONE'
  }

  async run(ctx: RuleContext, msg: any, err: Error): Promise<void> {
    return
  }

  close(): void {
  }
}

/**
 * RuleError represents a rule error
 */
export class RuleError extends Error {

  /**
   * Creates a new rule error.
   * @param message - The error message.
   */
  constructor(message?: string) {
    super(message)
  }
}

/**
 * RuleConditionError represents a rule condition error
 */
export class RuleConditionError extends RuleError {
  rule: Rule

  /**
   * Creates a new rule condition error.
   * @param rule - The rule.
   */
  constructor(rule: Rule) {
    super(RuleConditionError.error(rule))
    this.rule = rule
  }

  static error(rule: Rule): string {
    let errMsg = rule.doc
    if (!errMsg) {
      if (rule.expr !== '') {
        return `Expr failed: '${rule.expr}'`
      }
      return `Condition failed: '${rule.name}'`
    }
    return errMsg
  }
}
