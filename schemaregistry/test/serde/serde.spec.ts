import { describe, expect, it } from '@jest/globals';
import {SchemaId} from "../../serde/serde";

describe('SchemaGuid', () => {
  it('schema guid', () => {
    const schemaId = new SchemaId("AVRO")
    const input = new Uint8Array([
      0x01, 0x89, 0x79, 0x17, 0x62, 0x23, 0x36, 0x41, 0x86, 0x96, 0x74, 0x29, 0x9b, 0x90,
      0xa8, 0x02, 0xe2,
    ])
    schemaId.fromBytes(Buffer.from(input))
    const guid = schemaId.guid
    expect(guid).toEqual("89791762-2336-4186-9674-299b90a802e2")

    const output = new Uint8Array(schemaId.guidToBytes())
    for (let i = 0; i < output.length; i++) {
      expect(output[i]).toEqual(input[i])
    }
  })
  it('schema id', () => {
    const schemaId = new SchemaId("AVRO")
    const input = new Uint8Array([
      0x00, 0x00, 0x00, 0x00, 0x01,
    ])
    schemaId.fromBytes(Buffer.from(input))
    const id = schemaId.id
    expect(id).toEqual(1)

    const output = new Uint8Array(schemaId.idToBytes())
    for (let i = 0; i < output.length; i++) {
      expect(output[i]).toEqual(input[i])
    }
  })
  it('schema guid with message indexes', () => {
    const schemaId = new SchemaId("PROTOBUF")
    const input = new Uint8Array([
      0x01, 0x89, 0x79, 0x17, 0x62, 0x23, 0x36, 0x41, 0x86, 0x96, 0x74, 0x29, 0x9b, 0x90,
      0xa8, 0x02, 0xe2, 0x06, 0x02, 0x04, 0x06,
    ])
    schemaId.fromBytes(Buffer.from(input))
    const guid = schemaId.guid
    expect(guid).toEqual("89791762-2336-4186-9674-299b90a802e2")

    const msgIndexes = schemaId.messageIndexes
    expect(msgIndexes).toEqual([1, 2, 3])

    const output = new Uint8Array(schemaId.guidToBytes())
    for (let i = 0; i < output.length; i++) {
      expect(output[i]).toEqual(input[i])
    }
  })
  it('schema id with message indexes', () => {
    const schemaId = new SchemaId("PROTOBUF")
    const input = new Uint8Array([
      0x00, 0x00, 0x00, 0x00, 0x01, 0x06, 0x02, 0x04, 0x06,
    ])
    schemaId.fromBytes(Buffer.from(input))
    const id = schemaId.id
    expect(id).toEqual(1)

    const msgIndexes = schemaId.messageIndexes
    expect(msgIndexes).toEqual([1, 2, 3])

    const output = new Uint8Array(schemaId.idToBytes())
    for (let i = 0; i < output.length; i++) {
      expect(output[i]).toEqual(input[i])
    }
  })
})
