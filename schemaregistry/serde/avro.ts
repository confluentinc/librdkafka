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
import avro, { ForSchemaOptions, Type, types } from "avsc";
import UnwrappedUnionType = types.UnwrappedUnionType
import WrappedUnionType = types.WrappedUnionType
import ArrayType = types.ArrayType
import MapType = types.MapType
import RecordType = types.RecordType
import Field = types.Field
import { LRUCache } from 'lru-cache'
import {getRuleExecutors} from "./rule-registry";
import stringify from "json-stringify-deterministic";

type TypeHook = (schema: avro.Schema, opts: ForSchemaOptions) => Type | undefined

export type AvroSerdeConfig = Partial<ForSchemaOptions>

export interface AvroSerde {
  schemaToTypeCache: LRUCache<string, avro.Type>
}

export type AvroSerializerConfig = SerializerConfig & AvroSerdeConfig

export class AvroSerializer extends Serializer implements AvroSerde {
  schemaToTypeCache: LRUCache<string, avro.Type>

  constructor(client: Client, serdeType: SerdeType, conf: AvroSerializerConfig) {
    super(client, serdeType, conf)
    this.schemaToTypeCache = new LRUCache<string, Type>({ max: this.conf.cacheCapacity ?? 1000 })
    this.fieldTransformer = async (ctx: RuleContext, fieldTransform: FieldTransform, msg: any) => {
      return await this.fieldTransform(ctx, fieldTransform, msg)
    }
    for (const rule of getRuleExecutors()) {
      rule.configure(client.config(), conf.ruleConfig ?? new Map<string, string>)
    }
  }

  async serialize(topic: string, msg: any): Promise<Buffer> {
    if (this.client == null) {
      throw new Error('client is not initialized')
    }
    if (msg == null) {
      throw new Error('message is empty')
    }

    let avroSchema = Type.forValue(msg)
    const schema: SchemaInfo = {
      schemaType: 'AVRO',
      schema: JSON.stringify(avroSchema),
    }
    const [id, info] = await this.getId(topic, msg, schema)
    avroSchema = await this.toType(info)
    const subject = this.subjectName(topic, info)
    msg = await this.executeRules(subject, topic, RuleMode.WRITE, null, info, msg, getInlineTags(avroSchema))
    const msgBytes = avroSchema.toBuffer(msg)
    return this.writeBytes(id, msgBytes)
  }

  async fieldTransform(ctx: RuleContext, fieldTransform: FieldTransform, msg: any): Promise<any> {
    const schema = await this.toType(ctx.target)
    return await transform(ctx, schema, msg, fieldTransform)
  }

  async toType(info: SchemaInfo): Promise<Type> {
    return toType(this.client, this.conf as AvroDeserializerConfig, this, info, async (client, info) => {
      const deps = new Map<string, string>()
      await this.resolveReferences(client, info, deps)
      return deps
    })
  }
}

export type AvroDeserializerConfig = DeserializerConfig & AvroSerdeConfig

export class AvroDeserializer extends Deserializer implements AvroSerde {
  schemaToTypeCache: LRUCache<string, avro.Type>

  constructor(client: Client, serdeType: SerdeType, conf: AvroDeserializerConfig) {
    super(client, serdeType, conf)
    this.schemaToTypeCache = new LRUCache<string, Type>({ max: this.conf.cacheCapacity ?? 1000 })
    this.fieldTransformer = async (ctx: RuleContext, fieldTransform: FieldTransform, msg: any) => {
      return await this.fieldTransform(ctx, fieldTransform, msg)
    }
    for (const rule of getRuleExecutors()) {
      rule.configure(client.config(), conf.ruleConfig ?? new Map<string, string>)
    }
  }

  async deserialize(topic: string, payload: Buffer): Promise<any> {
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
    const writer = await this.toType(info)

    let msg: any
    const msgBytes = payload.subarray(5)
    if (migrations.length > 0) {
      msg = writer.fromBuffer(msgBytes)
      msg = await this.executeMigrations(migrations, subject, topic, msg)
    } else {
      if (readerMeta != null) {
        const reader = await this.toType(readerMeta)
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
    msg = await this.executeRules(subject, topic, RuleMode.READ, null, target, msg, getInlineTags(writer))
    return msg
  }

  async fieldTransform(ctx: RuleContext, fieldTransform: FieldTransform, msg: any): Promise<any> {
    const schema = await this.toType(ctx.target)
    return await transform(ctx, schema, msg, fieldTransform)
  }

  async toType(info: SchemaInfo): Promise<Type> {
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
): Promise<Type> {
  let type = serde.schemaToTypeCache.get(stringify(info.schema))
  if (type != null) {
    return type
  }

  const deps = await refResolver(client, info)

  const addReferencedSchemas = (userHook?: TypeHook): TypeHook | undefined => (
    schema: avro.Schema,
    opts: ForSchemaOptions,
  ) => {
    deps.forEach((_name, schema) => {
      avro.Type.forSchema(JSON.parse(schema), opts)
    })
    if (userHook) {
      return userHook(schema, opts)
    }
    return
  }

  const avroOpts = conf
  type = avro.Type.forSchema(JSON.parse(info.schema), {
    ...avroOpts,
    typeHook: addReferencedSchemas(avroOpts?.typeHook),
  })
  serde.schemaToTypeCache.set(stringify(info.schema), type)
  return type
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
      const map = msg as Map<string, any>
      for (const key of Object.keys(map)) {
        map.set(key, await transform(ctx, mapSchema.valuesType, map.get(key), fieldTransform))
      }
      return map
    case 'record':
      const recordSchema = schema as RecordType
      const record = msg as Record<string, any>
      for (const field of recordSchema.fields) {
        await transformField(ctx, recordSchema, field, record, record[field.name], fieldTransform)
      }
      return record
    default:
      if (fieldCtx != null) {
        const ruleTags = ctx.rule.tags
        if (ruleTags == null || ruleTags.size === 0 || !disjoint(ruleTags, fieldCtx.tags)) {
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
  val: any,
  fieldTransform: FieldTransform,
): Promise<void> {
  const fullName = recordSchema.name + '.' + field.name
  try {
    ctx.enterField(
      val.Interface(),
      fullName,
      field.name,
      getType(field.type),
      ctx.getInlineTags(fullName),
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

function getInlineTags(schema: object): Map<string, Set<string>> {
  const inlineTags = new Map<string, Set<string>>()
  getInlineTagsRecursively('', '', schema, inlineTags)
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
