import * as tls from 'tls'

export type BrokersFunction = () => string[] | Promise<string[]>

export type Mechanism = {
  mechanism: string
}

type SASLMechanismOptionsMap = {
  plain: { username: string; password: string }
  'scram-sha-256': { username: string; password: string }
  'scram-sha-512': { username: string; password: string }
}

export type SASLMechanism = keyof SASLMechanismOptionsMap
type SASLMechanismOptions<T> = T extends SASLMechanism
  ? { mechanism: T } & SASLMechanismOptionsMap[T]
  : never
export type SASLOptions = SASLMechanismOptions<SASLMechanism>

export interface KafkaConfig {
  brokers: string[] | BrokersFunction
  ssl?: boolean
  sasl?: SASLOptions | Mechanism
  clientId?: string
  connectionTimeout?: number
  authenticationTimeout?: number
  reauthenticationThreshold?: number
  requestTimeout?: number
  enforceRequestTimeout?: boolean
}

export interface ProducerConfig {
  metadataMaxAge?: number
  allowAutoTopicCreation?: boolean
  idempotent?: boolean
  transactionalId?: string
  transactionTimeout?: number
  maxInFlightRequests?: number
}

export interface IHeaders {
  [key: string]: Buffer | string | (Buffer | string)[] | undefined
}

export interface Message {
  key?: Buffer | string | null
  value: Buffer | string | null
  partition?: number
  headers?: IHeaders
  timestamp?: string
}

export enum CompressionTypes {
  None = 0,
  GZIP = 1,
  Snappy = 2,
  LZ4 = 3,
  ZSTD = 4,
}

export var CompressionCodecs: {
  [CompressionTypes.GZIP]: () => any
  [CompressionTypes.Snappy]: () => any
  [CompressionTypes.LZ4]: () => any
  [CompressionTypes.ZSTD]: () => any
}

export interface ProducerRecord {
  topic: string
  messages: Message[]
  acks?: number
  timeout?: number
  compression?: CompressionTypes
}

export type RecordMetadata = {
  topicName: string
  partition: number
  errorCode: number
  offset?: string
  timestamp?: string
  baseOffset?: string
  logAppendTime?: string
  logStartOffset?: string
}

export class Kafka {
  constructor(config: KafkaConfig)
  producer(config?: ProducerConfig): Producer
}

type Sender = {
  send(record: ProducerRecord): Promise<RecordMetadata[]>
}

export type Producer = Sender & {
  connect(): Promise<void>
  disconnect(): Promise<void>
}

