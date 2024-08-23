import { describe, expect, it } from '@jest/globals';
import { BufferWrapper, MAX_VARINT_LEN_32 } from "../../../schemaregistry/serde/buffer-wrapper";

describe('BufferWrapper', () => {
  it('write and read 100', () => {
    const buf = new Buffer(MAX_VARINT_LEN_32)
    const bw = new BufferWrapper(buf)
    bw.writeVarInt(100)
    const bw2 = new BufferWrapper(bw.buf.subarray(0, bw.pos))
    expect(bw2.readVarInt()).toBe(100)
  })
  it('write and read max pos int', () => {
    const buf = new Buffer(MAX_VARINT_LEN_32)
    const bw = new BufferWrapper(buf)
    bw.writeVarInt(2147483647)
    const bw2 = new BufferWrapper(bw.buf.subarray(0, bw.pos))
    expect(bw2.readVarInt()).toBe(2147483647)
  })
  it('write and read max neg int', () => {
    const buf = new Buffer(MAX_VARINT_LEN_32)
    const bw = new BufferWrapper(buf)
    bw.writeVarInt(-2147483648)
    const bw2 = new BufferWrapper(bw.buf.subarray(0, bw.pos))
    expect(bw2.readVarInt()).toBe(-2147483648)
  })
})
