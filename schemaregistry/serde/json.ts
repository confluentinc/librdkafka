import {
  Deserializer, DeserializerConfig,
  FieldTransform,
  FieldType, Migration, RefResolver, RuleConditionError,
  RuleContext,
  SerdeType, SerializationError,
  Serializer, SerializerConfig
} from "./serde";
import {
  Client, RuleMode,
  SchemaInfo
} from "../schemaregistry-client";
import Ajv, {ErrorObject} from "ajv";
import Ajv2019 from "ajv/dist/2019";
import Ajv2020 from "ajv/dist/2020";
import * as draft6MetaSchema from 'ajv/dist/refs/json-schema-draft-06.json'
import * as draft7MetaSchema from 'ajv/dist/refs/json-schema-draft-07.json'
import {
  DereferencedJSONSchemaDraft07,
  DereferencedJSONSchemaDraft2020_12,
} from '@criteria/json-schema'
import {
  dereferenceJSONSchema as dereferenceJSONSchemaDraft2020_12,
} from '@criteria/json-schema/draft-2020-12'
import {
  dereferenceJSONSchema as dereferenceJSONSchemaDraft07,
} from '@criteria/json-schema/draft-07'
import { validateJSON } from '@criteria/json-schema-validation'
import { LRUCache } from "lru-cache";
import { generateSchema } from "./json-util";
import {RuleRegistry} from "./rule-registry";
import stringify from "json-stringify-deterministic";

export interface ValidateFunction {
  (this: any, data: any): boolean
  errors?: null | ErrorObject[]
}

export type DereferencedJSONSchema = DereferencedJSONSchemaDraft07 | DereferencedJSONSchemaDraft2020_12

export type JsonSerdeConfig = ConstructorParameters<typeof Ajv>[0] & {
  validate?: boolean
}

export interface JsonSerde {
  schemaToTypeCache: LRUCache<string, DereferencedJSONSchema>
  schemaToValidateCache: LRUCache<string, ValidateFunction>
}

/**
 * JsonSerializerConfig is the configuration for the JsonSerializer.
 */
export type JsonSerializerConfig = SerializerConfig & JsonSerdeConfig

/**
 * JsonSerializer is a serializer for JSON messages.
 */
export class JsonSerializer extends Serializer implements JsonSerde {
  schemaToTypeCache: LRUCache<string, DereferencedJSONSchema>
  schemaToValidateCache: LRUCache<string, ValidateFunction>

  /**
   * Creates a new JsonSerializer.
   * @param client - the schema registry client
   * @param serdeType - the serializer type
   * @param conf - the serializer configuration
   * @param ruleRegistry - the rule registry
   */
  constructor(client: Client, serdeType: SerdeType, conf: JsonSerializerConfig, ruleRegistry?: RuleRegistry) {
    super(client, serdeType, conf, ruleRegistry)
    this.schemaToTypeCache = new LRUCache<string, DereferencedJSONSchema>({ max: this.config().cacheCapacity ?? 1000 })
    this.schemaToValidateCache = new LRUCache<string, ValidateFunction>({ max: this.config().cacheCapacity ?? 1000 })
    this.fieldTransformer = async (ctx: RuleContext, fieldTransform: FieldTransform, msg: any) => {
      return await this.fieldTransform(ctx, fieldTransform, msg)
    }
    for (const rule of this.ruleRegistry.getExecutors()) {
      rule.configure(client.config(), new Map<string, string>(Object.entries(conf.ruleConfig ?? {})))
    }
  }

  /**
   * Serializes a message.
   * @param topic - the topic
   * @param msg - the message
   */
  override async serialize(topic: string, msg: any): Promise<Buffer> {
    if (this.client == null) {
      throw new Error('client is not initialized')
    }
    if (msg == null) {
      throw new Error('message is empty')
    }

    let schema: SchemaInfo | undefined = undefined
    // Don't derive the schema if it is being looked up in the following ways
    if (this.config().useSchemaId == null &&
      !this.config().useLatestVersion &&
      this.config().useLatestWithMetadata == null) {
      const jsonSchema = JsonSerializer.messageToSchema(msg)
      schema = {
        schemaType: 'JSON',
        schema: JSON.stringify(jsonSchema),
      }
    }
    const [id, info] = await this.getId(topic, msg, schema)
    const subject = this.subjectName(topic, info)
    msg = await this.executeRules(subject, topic, RuleMode.WRITE, null, info, msg, null)
    const msgBytes = Buffer.from(JSON.stringify(msg))
    if ((this.conf as JsonSerdeConfig).validate) {
      const validate = await this.toValidateFunction(info)
      if (validate != null && !validate(msg)) {
        throw new SerializationError('Invalid message')
      }
    }
    return this.writeBytes(id, msgBytes)
  }

  async fieldTransform(ctx: RuleContext, fieldTransform: FieldTransform, msg: any): Promise<any> {
    const schema = await this.toType(ctx.target)
    if (typeof schema === 'boolean') {
      return msg
    }
    return await transform(ctx, schema, '$', msg, fieldTransform)
  }

  async toType(info: SchemaInfo): Promise<DereferencedJSONSchema> {
    return toType(this.client, this.conf as JsonDeserializerConfig, this, info, async (client, info) => {
      const deps = new Map<string, string>()
      await this.resolveReferences(client, info, deps)
      return deps
    })
  }

  async toValidateFunction(info: SchemaInfo): Promise<ValidateFunction | undefined> {
    return await toValidateFunction(this.client, this.conf as JsonDeserializerConfig, this, info, async (client, info) => {
        const deps = new Map<string, string>()
        await this.resolveReferences(client, info, deps)
        return deps
      },
    )
  }

  static messageToSchema(msg: any): DereferencedJSONSchema {
    return generateSchema(msg)
  }
}

/**
 * JsonDeserializerConfig is the configuration for the JsonDeserializer.
 */
export type JsonDeserializerConfig = DeserializerConfig & JsonSerdeConfig

/**
 * JsonDeserializer is a deserializer for JSON messages.
 */
export class JsonDeserializer extends Deserializer implements JsonSerde {
  schemaToTypeCache: LRUCache<string, DereferencedJSONSchema>
  schemaToValidateCache: LRUCache<string, ValidateFunction>

  /**
   * Creates a new JsonDeserializer.
   * @param client - the schema registry client
   * @param serdeType - the deserializer type
   * @param conf - the deserializer configuration
   * @param ruleRegistry - the rule registry
   */
  constructor(client: Client, serdeType: SerdeType, conf: JsonDeserializerConfig, ruleRegistry?: RuleRegistry) {
    super(client, serdeType, conf, ruleRegistry)
    this.schemaToTypeCache = new LRUCache<string, DereferencedJSONSchema>({ max: this.config().cacheCapacity ?? 1000 })
    this.schemaToValidateCache = new LRUCache<string, ValidateFunction>({ max: this.config().cacheCapacity ?? 1000 })
    this.fieldTransformer = async (ctx: RuleContext, fieldTransform: FieldTransform, msg: any) => {
      return await this.fieldTransform(ctx, fieldTransform, msg)
    }
    for (const rule of this.ruleRegistry.getExecutors()) {
      rule.configure(client.config(), new Map<string, string>(Object.entries(conf.ruleConfig ?? {})))
    }
  }

  /**
   * Deserializes a message.
   * @param topic - the topic
   * @param payload - the message payload
   */
  override async deserialize(topic: string, payload: Buffer): Promise<any> {
    if (!Buffer.isBuffer(payload)) {
      throw new Error('Invalid buffer')
    }
    if (payload.length === 0) {
      return null
    }

    const info = await this.getSchema(topic, payload)
    const subject = this.subjectName(topic, info)
    const readerMeta = await this.getReaderSchema(subject)
    let migrations: Migration[] = []
    if (readerMeta != null) {
      migrations = await this.getMigrations(subject, info, readerMeta)
    }
    const msgBytes = payload.subarray(5)
    let msg = JSON.parse(msgBytes.toString())
    if (migrations.length > 0) {
      msg = await this.executeMigrations(migrations, subject, topic, msg)
    }
    let target: SchemaInfo
    if (readerMeta != null) {
      target = readerMeta
    } else {
      target = info
    }
    msg = this.executeRules(subject, topic, RuleMode.READ, null, target, msg, null)
    if ((this.conf as JsonSerdeConfig).validate) {
      const validate = await this.toValidateFunction(info)
      if (validate != null && !validate(JSON.parse(msg))) {
        throw new SerializationError('Invalid message')
      }
    }
    return msg
  }

  async fieldTransform(ctx: RuleContext, fieldTransform: FieldTransform, msg: any): Promise<any> {
    const schema = await this.toType(ctx.target)
    return await transform(ctx, schema, '$', msg, fieldTransform)
  }

  toType(info: SchemaInfo): DereferencedJSONSchema {
    return toType(this.client, this.conf as JsonDeserializerConfig, this, info, async (client, info) => {
      const deps = new Map<string, string>()
      await this.resolveReferences(client, info, deps)
      return deps
    })
  }

  async toValidateFunction(info: SchemaInfo): Promise<ValidateFunction | undefined> {
    return await toValidateFunction(this.client, this.conf as JsonDeserializerConfig, this, info, async (client, info) => {
        const deps = new Map<string, string>()
        await this.resolveReferences(client, info, deps)
        return deps
      },
    )
  }
}

async function toValidateFunction(
  client: Client,
    conf: JsonSerdeConfig,
    serde: JsonSerde,
    info: SchemaInfo,
    refResolver: RefResolver,
): Promise<ValidateFunction | undefined> {
  let fn = serde.schemaToValidateCache.get(stringify(info.schema))
  if (fn != null) {
    return fn
  }

  const deps = await refResolver(client, info)

  const json = JSON.parse(info.schema)
  const spec = json.$schema
  if (spec === 'http://json-schema.org/draft/2020-12/schema'
    || spec === 'https://json-schema.org/draft/2020-12/schema') {
    const ajv2020 = new Ajv2020(conf as JsonSerdeConfig)
    ajv2020.addKeyword("confluent:tags")
    deps.forEach((schema, name) => {
      ajv2020.addSchema(JSON.parse(schema), name)
    })
    fn = ajv2020.compile(json)
  } else {
    const ajv = new Ajv2019(conf as JsonSerdeConfig)
    ajv.addKeyword("confluent:tags")
    ajv.addMetaSchema(draft6MetaSchema)
    ajv.addMetaSchema(draft7MetaSchema)
    deps.forEach((schema, name) => {
      ajv.addSchema(JSON.parse(schema), name)
    })
    fn = ajv.compile(json)
  }
  serde.schemaToValidateCache.set(stringify(info.schema), fn)
  return fn
}

async function toType(
  client: Client,
  conf: JsonSerdeConfig,
  serde: JsonSerde,
  info: SchemaInfo,
  refResolver: RefResolver,
): Promise<DereferencedJSONSchema> {
  let type = serde.schemaToTypeCache.get(stringify(info.schema))
  if (type != null) {
    return type
  }

  const deps = await refResolver(client, info)

  const retrieve = (uri: string) => {
    const data = deps.get(uri)
    if (data == null) {
      throw new SerializationError(`Schema not found: ${uri}`)
    }
    return JSON.parse(data)
  }

  const json = JSON.parse(info.schema)
  const spec = json.$schema
  let schema
  if (spec === 'http://json-schema.org/draft/2020-12/schema'
    || spec === 'https://json-schema.org/draft/2020-12/schema') {
    schema = await dereferenceJSONSchemaDraft2020_12(json, { retrieve })
  } else {
    schema = await dereferenceJSONSchemaDraft07(json, { retrieve })
  }
  serde.schemaToTypeCache.set(stringify(info.schema), schema)
  return schema
}

async function transform(ctx: RuleContext, schema: DereferencedJSONSchema, path:string, msg: any, fieldTransform: FieldTransform): Promise<any> {
  if (msg == null || schema == null || typeof schema === 'boolean') {
    return msg
  }
  let fieldCtx = ctx.currentField()
  if (fieldCtx != null) {
    fieldCtx.type = getType(schema)
  }
  if (schema.allOf != null && schema.allOf.length > 0) {
    let subschema = validateSubschemas(schema.allOf, msg)
    if (subschema != null) {
      return await transform(ctx, subschema, path, msg, fieldTransform)
    }
  }
  if (schema.anyOf != null && schema.anyOf.length > 0) {
    let subschema = validateSubschemas(schema.anyOf, msg)
    if (subschema != null) {
      return await transform(ctx, subschema, path, msg, fieldTransform)
    }
  }
  if (schema.oneOf != null && schema.oneOf.length > 0) {
    let subschema = validateSubschemas(schema.oneOf, msg)
    if (subschema != null) {
      return await transform(ctx, subschema, path, msg, fieldTransform)
    }
  }
  if (schema.items != null) {
    if (Array.isArray(msg)) {
      for (let i = 0; i < msg.length; i++) {
        msg[i] = await transform(ctx, schema.items, path, msg[i], fieldTransform)
      }
      return msg
    }
  }
  if (schema.$ref != null) {
    return await transform(ctx, schema.$ref, path, msg, fieldTransform)
  }
  let type = getType(schema)
  switch (type) {
    case FieldType.RECORD:
      if (schema.properties != null) {
        for (let [propName, propSchema] of Object.entries(schema.properties)) {
          await transformField(ctx, path, propName, msg, propSchema, fieldTransform)
        }
      }
      return msg
    case FieldType.ENUM:
    case FieldType.STRING:
    case FieldType.INT:
    case FieldType.DOUBLE:
    case FieldType.BOOLEAN:
      if (fieldCtx != null) {
        const ruleTags = ctx.rule.tags
        if (ruleTags == null || ruleTags.length === 0 || !disjoint(new Set<string>(ruleTags), fieldCtx.tags)) {
          return await fieldTransform.transform(ctx, fieldCtx, msg)
        }
      }
  }

  return msg
}

async function transformField(ctx: RuleContext, path: string, propName: string, msg: any,
                              propSchema: DereferencedJSONSchema,
                              fieldTransform: FieldTransform): Promise<void> {
  const fullName = path + '.' + propName
  try {
    ctx.enterField(msg, fullName, propName, getType(propSchema), getInlineTags(propSchema))
    let value = msg[propName]
    const newVal = await transform(ctx, propSchema, fullName, value, fieldTransform)
    if (ctx.rule.kind === 'CONDITION') {
      if (newVal === false) {
        throw new RuleConditionError(ctx.rule)
      }
    } else {
      msg[propName] = newVal
    }
  } finally {
    ctx.leaveField()
  }
}

function validateSubschemas(subschemas: DereferencedJSONSchema[], msg: any): DereferencedJSONSchema | null {
  for (let subschema of subschemas) {
    try {
      validateJSON(msg, subschema)
      return subschema
    } catch (error) {
      // ignore
    }
  }
  return null
}

function getType(schema: DereferencedJSONSchema): FieldType {
  if (typeof schema === 'boolean') {
    return FieldType.NULL
  }
  if (schema.type == null) {
    return FieldType.NULL
  }
  if (Array.isArray(schema.type)) {
    return FieldType.COMBINED
  }
  if (schema.const != null || schema.enum != null) {
    return FieldType.ENUM
  }
  switch (schema.type) {
    case 'object':
      if (schema.properties == null || Object.keys(schema.properties).length === 0) {
        return FieldType.MAP
      }
      return FieldType.RECORD
    case 'array':
      return FieldType.ARRAY
    case 'string':
      return FieldType.STRING
    case 'integer':
      return FieldType.INT
    case 'number':
      return FieldType.DOUBLE
    case 'boolean':
      return FieldType.BOOLEAN
    case 'null':
      return FieldType.NULL
    default:
      return FieldType.NULL
  }
}

function getInlineTags(schema: DereferencedJSONSchema): Set<string> {
  let tagsKey = 'confluent:tags' as keyof DereferencedJSONSchema
  return new Set<string>(schema[tagsKey])
}

function disjoint(tags1: Set<string>, tags2: Set<string>): boolean {
  for (let tag of tags1) {
    if (tags2.has(tag)) {
      return false
    }
  }
  return true
}



