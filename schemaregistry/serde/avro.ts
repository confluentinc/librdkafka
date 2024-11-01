import {
  Deserializer, DeserializerConfig,
  FieldTransform,
  FieldType, Migration, RefResolver,
  RuleConditionError,
  RuleContext, SerdeType,
  Serializer, SerializerConfig
} from "./serde";
import {
  Client, RuleMode,
  SchemaInfo
} from "../schemaregistry-client";
import avro, {ForSchemaOptions, Type, types} from "avsc";
import UnwrappedUnionType = types.UnwrappedUnionType
import WrappedUnionType = types.WrappedUnionType
import ArrayType = types.ArrayType
import MapType = types.MapType
import RecordType = types.RecordType
import Field = types.Field
import { LRUCache } from 'lru-cache'
import {RuleRegistry} from "./rule-registry";
import stringify from "json-stringify-deterministic";

type TypeHook = (schema: avro.Schema, opts: ForSchemaOptions) => Type | undefined

export type AvroSerdeConfig = Partial<ForSchemaOptions>

export interface AvroSerde {
  schemaToTypeCache: LRUCache<string, [avro.Type, Map<string, string>]>
}

/**
 * AvroSerializerConfig is used to configure the AvroSerializer.
 */
export type AvroSerializerConfig = SerializerConfig & AvroSerdeConfig

/**
 * AvroSerializer is used to serialize messages using Avro.
 */
export class AvroSerializer extends Serializer implements AvroSerde {
  schemaToTypeCache: LRUCache<string, [avro.Type, Map<string, string>]>

  /**
   * Create a new AvroSerializer.
   * @param client - the schema registry client
   * @param serdeType - the type of the serializer
   * @param conf - the serializer configuration
   * @param ruleRegistry - the rule registry
   */
  constructor(client: Client, serdeType: SerdeType, conf: AvroSerializerConfig, ruleRegistry?: RuleRegistry) {
    super(client, serdeType, conf, ruleRegistry)
    this.schemaToTypeCache = new LRUCache<string, [Type, Map<string, string>]>({ max: this.conf.cacheCapacity ?? 1000 })
    this.fieldTransformer = async (ctx: RuleContext, fieldTransform: FieldTransform, msg: any) => {
      return await this.fieldTransform(ctx, fieldTransform, msg)
    }
    for (const rule of this.ruleRegistry.getExecutors()) {
      rule.configure(client.config(), new Map<string, string>(Object.entries(conf.ruleConfig ?? {})))
    }
  }

  /**
   * serialize is used to serialize a message using Avro.
   * @param topic - the topic to serialize the message for
   * @param msg - the message to serialize
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
      const avroSchema = AvroSerializer.messageToSchema(msg)
      schema = {
        schemaType: 'AVRO',
        schema: JSON.stringify(avroSchema),
      }
    }
    const [id, info] = await this.getId(topic, msg, schema)
    let avroSchema: avro.Type
    let deps: Map<string, string>
    [avroSchema, deps] = await this.toType(info)
    const subject = this.subjectName(topic, info)
    msg = await this.executeRules(
      subject, topic, RuleMode.WRITE, null, info, msg, getInlineTags(info, deps))
    const msgBytes = avroSchema.toBuffer(msg)
    return this.writeBytes(id, msgBytes)
  }

  async fieldTransform(ctx: RuleContext, fieldTransform: FieldTransform, msg: any): Promise<any> {
    const [schema, ] = await this.toType(ctx.target)
    return await transform(ctx, schema, msg, fieldTransform)
  }

  async toType(info: SchemaInfo): Promise<[Type, Map<string, string>]> {
    return toType(this.client, this.conf as AvroDeserializerConfig, this, info, async (client, info) => {
      const deps = new Map<string, string>()
      await this.resolveReferences(client, info, deps)
      return deps
    })
  }

  static messageToSchema(msg: any): avro.Type {
    let enumIndex = 1
    let fixedIndex = 1
    let recordIndex = 1

    const namingHook: TypeHook = (
      avroSchema: avro.Schema,
      opts: ForSchemaOptions,
    ) => {
      let schema = avroSchema as any
      switch (schema.type) {
        case 'enum':
          schema.name = `Enum${enumIndex++}`;
          break;
        case 'fixed':
          schema.name = `Fixed${fixedIndex++}`;
          break;
        case 'record':
          schema.name = `Record${recordIndex++}`;
          break;
        default:
      }
      return undefined
    }

    return Type.forValue(msg, { typeHook: namingHook })
  }
}

/**
 * AvroDeserializerConfig is used to configure the AvroDeserializer.
 */
export type AvroDeserializerConfig = DeserializerConfig & AvroSerdeConfig

/**
 * AvroDeserializer is used to deserialize messages using Avro.
 */
export class AvroDeserializer extends Deserializer implements AvroSerde {
  schemaToTypeCache: LRUCache<string, [avro.Type, Map<string, string>]>

  /**
   * Create a new AvroDeserializer.
   * @param client - the schema registry client
   * @param serdeType - the type of the deserializer
   * @param conf - the deserializer configuration
   * @param ruleRegistry - the rule registry
   */
  constructor(client: Client, serdeType: SerdeType, conf: AvroDeserializerConfig, ruleRegistry?: RuleRegistry) {
    super(client, serdeType, conf, ruleRegistry)
    this.schemaToTypeCache = new LRUCache<string, [Type, Map<string, string>]>({ max: this.conf.cacheCapacity ?? 1000 })
    this.fieldTransformer = async (ctx: RuleContext, fieldTransform: FieldTransform, msg: any) => {
      return await this.fieldTransform(ctx, fieldTransform, msg)
    }
    for (const rule of this.ruleRegistry.getExecutors()) {
      rule.configure(client.config(), new Map<string, string>(Object.entries(conf.ruleConfig ?? {})))
    }
  }

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
    const [writer, deps] = await this.toType(info)

    let msg: any
    const msgBytes = payload.subarray(5)
    if (migrations.length > 0) {
      msg = writer.fromBuffer(msgBytes)
      msg = await this.executeMigrations(migrations, subject, topic, msg)
    } else {
      if (readerMeta != null) {
        const [reader, ] = await this.toType(readerMeta)
        if (reader.equals(writer)) {
          msg = reader.fromBuffer(msgBytes)
        } else {
          msg = reader.fromBuffer(msgBytes, reader.createResolver(writer))
        }
      } else {
        msg = writer.fromBuffer(msgBytes)
      }
    }
    let target: SchemaInfo
    if (readerMeta != null) {
      target = readerMeta
    } else {
      target = info
    }
    msg = await this.executeRules(
      subject, topic, RuleMode.READ, null, target, msg, getInlineTags(info, deps))
    return msg
  }

  async fieldTransform(ctx: RuleContext, fieldTransform: FieldTransform, msg: any): Promise<any> {
    const [schema, ] = await this.toType(ctx.target)
    return await transform(ctx, schema, msg, fieldTransform)
  }

  async toType(info: SchemaInfo): Promise<[Type, Map<string, string>]> {
    return toType(this.client, this.conf as AvroDeserializerConfig, this, info, async (client, info) => {
      const deps = new Map<string, string>()
      await this.resolveReferences(client, info, deps)
      return deps
    })
  }
}

async function toType(
  client: Client,
  conf: AvroSerdeConfig,
  serde: AvroSerde,
  info: SchemaInfo,
  refResolver: RefResolver,
): Promise<[Type, Map<string, string>]> {
  let tuple = serde.schemaToTypeCache.get(stringify(info.schema))
  if (tuple != null) {
    return tuple
  }

  const deps = await refResolver(client, info)

  const addReferencedSchemas = (userHook?: TypeHook): TypeHook | undefined => (
    schema: avro.Schema,
    opts: ForSchemaOptions,
  ) => {
    const avroOpts = opts as AvroSerdeConfig
    deps.forEach((schema, _name) => {
      avroOpts.typeHook = userHook
      avro.Type.forSchema(JSON.parse(schema), avroOpts)
    })
    if (userHook) {
      return userHook(schema, opts)
    }
    return
  }

  const avroOpts = conf
  let type = avro.Type.forSchema(JSON.parse(info.schema), {
    ...avroOpts,
    typeHook: addReferencedSchemas(avroOpts?.typeHook),
  })
  serde.schemaToTypeCache.set(stringify(info.schema), [type, deps])
  return [type, deps]
}

async function transform(ctx: RuleContext, schema: Type, msg: any, fieldTransform: FieldTransform): Promise<any> {
  if (msg == null || schema == null) {
    return msg
  }
  const fieldCtx = ctx.currentField()
  if (fieldCtx != null) {
    fieldCtx.type = getType(schema)
  }
  switch (schema.typeName) {
    case 'union:unwrapped':
    case 'union:wrapped':
      const subschema = resolveUnion(schema, msg)
      if (subschema == null) {
        return null
      }
      return await transform(ctx, subschema, msg, fieldTransform)
    case 'array':
      const arraySchema = schema as ArrayType
      const array = msg as any[]
      return await Promise.all(array.map(item => transform(ctx, arraySchema.itemsType, item, fieldTransform)))
    case 'map':
      const mapSchema = schema as MapType
      const map = msg as { [key: string]: any }
      for (const key of Object.keys(map)) {
        map[key] = await transform(ctx, mapSchema.valuesType, map[key], fieldTransform)
      }
      return map
    case 'record':
      const recordSchema = schema as RecordType
      const record = msg as Record<string, any>
      for (const field of recordSchema.fields) {
        await transformField(ctx, recordSchema, field, record, fieldTransform)
      }
      return record
    default:
      if (fieldCtx != null) {
        const ruleTags = ctx.rule.tags ?? []
        if (ruleTags == null || ruleTags.length === 0 || !disjoint(new Set<string>(ruleTags), fieldCtx.tags)) {
          return await fieldTransform.transform(ctx, fieldCtx, msg)
        }
      }
      return msg
  }
}

async function transformField(
  ctx: RuleContext,
  recordSchema: RecordType,
  field: Field,
  record: Record<string, any>,
  fieldTransform: FieldTransform,
): Promise<void> {
  const fullName = recordSchema.name + '.' + field.name
  try {
    ctx.enterField(
      record,
      fullName,
      field.name,
      getType(field.type),
      null
    )
    const newVal = await transform(ctx, field.type, record[field.name], fieldTransform)
    if (ctx.rule.kind === 'CONDITION') {
      if (!newVal) {
        throw new RuleConditionError(ctx.rule)
      }
    } else {
      record[field.name] = newVal
    }
  } finally {
    ctx.leaveField()
  }
}

function getType(schema: Type): FieldType {
  switch (schema.typeName) {
    case 'record':
      return FieldType.RECORD
    case 'enum':
      return FieldType.ENUM
    case 'array':
      return FieldType.ARRAY
    case 'map':
      return FieldType.MAP
    case 'union:unwrapped':
    case 'union:wrapped':
      return FieldType.COMBINED
    case 'fixed':
      return FieldType.FIXED
    case 'string':
      return FieldType.STRING
    case 'bytes':
      return FieldType.BYTES
    case 'int':
      return FieldType.INT
    case 'abstract:long':
    case 'long':
      return FieldType.LONG
    case 'float':
      return FieldType.FLOAT
    case 'double':
      return FieldType.DOUBLE
    case 'boolean':
      return FieldType.BOOLEAN
    case 'null':
      return FieldType.NULL
    default:
      return FieldType.NULL
  }
}

function disjoint(slice1: Set<string>, map1: Set<string>): boolean {
  for (const v of slice1) {
    if (map1.has(v)) {
      return false
    }
  }
  return true
}

function resolveUnion(schema: Type, msg: any): Type | null {
  let unionTypes = null
  if (schema.typeName === 'union:unwrapped') {
    const union = schema as UnwrappedUnionType
    unionTypes = union.types.slice()
  } else if (schema.typeName === 'union:wrapped') {
    const union = schema as WrappedUnionType
    unionTypes = union.types.slice()
  }
  if (unionTypes != null) {
    for (let i = 0; i < unionTypes.length; i++) {
      if (unionTypes[i].isValid(msg)) {
        return unionTypes[i]
      }
    }
  }
  return null
}

function getInlineTags(info: SchemaInfo, deps: Map<string, string>): Map<string, Set<string>> {
  const inlineTags = new Map<string, Set<string>>()
  getInlineTagsRecursively('', '', JSON.parse(info.schema), inlineTags)
  for (const depSchema of deps.values()) {
    getInlineTagsRecursively('', '', JSON.parse(depSchema), inlineTags)
  }
  return inlineTags
}

// iterate over the object and get all properties named 'confluent:tags'
function getInlineTagsRecursively(ns: string, name: string, schema: any, tags: Map<string, Set<string>>): void {
  if (schema == null || typeof schema === 'string') {
    return
  } else if (Array.isArray(schema)) {
    for (let i = 0; i < schema.length; i++) {
      getInlineTagsRecursively(ns, name, schema[i], tags)
    }
  } else if (typeof schema === 'object') {
    const type = schema['type']
    if (type === 'record') {
      let recordNs = schema['namespace']
      let recordName = schema['name']
      if (recordNs === undefined) {
        recordNs = impliedNamespace(name)
      }
      if (recordNs == null) {
        recordNs = ns
      }
      if (recordNs !== '' && !recordName.startsWith(recordNs)) {
        recordName = recordNs + '.' + recordName
      }
      const fields = schema['fields']
      for (const field of fields) {
        const fieldTags = field['confluent:tags']
        const fieldName = field['name']
        if (fieldTags !== undefined && fieldName !== undefined) {
          tags.set(recordName + '.' + fieldName, new Set(fieldTags))
        }
        const fieldType = field['type']
        if (fieldType !== undefined) {
          getInlineTagsRecursively(recordNs, recordName, fieldType, tags)
        }
      }
    }
  }
}

function impliedNamespace(name: string): string | null {
  const match = /^(.*)\.[^.]+$/.exec(name)
  return match ? match[1] : null
}


