export const MAX_VARINT_LEN_16 = 3
export const MAX_VARINT_LEN_32 = 5
export const MAX_VARINT_LEN_64 = 10

export class BufferWrapper {
  buf: Buffer
  pos: number

  constructor(buf: Buffer) {
    this.buf = buf
    this.pos = 0
  }

  // Adapted from avro-js
  writeVarInt(n: number): void {
    let f, m

    if (n >= -1073741824 && n < 1073741824) {
      // Won't overflow, we can use integer arithmetic.
      m = n >= 0 ? n << 1 : (~n << 1) | 1
      do {
        this.buf[this.pos] = m & 0x7f
        m >>= 7
      } while (m && (this.buf[this.pos++] |= 0x80))
    } else {
      // We have to use slower floating arithmetic.
      f = n >= 0 ? n * 2 : -n * 2 - 1
      do {
        this.buf[this.pos] = f & 0x7f
        f /= 128
      } while (f >= 1 && (this.buf[this.pos++] |= 0x80))
    }
    this.pos++
  }

  // Adapted from avro-js
  readVarInt(): number {
    let n = 0
    let k = 0
    let b, h, f, fk

    do {
      b = this.buf[this.pos++]
      h = b & 0x80
      n |= (b & 0x7f) << k
      k += 7
    } while (h && k < 28)

    if (h) {
      // Switch to float arithmetic, otherwise we might overflow.
      f = n
      fk = 268435456 // 2 ** 28.
      do {
        b = this.buf[this.pos++]
        f += (b & 0x7f) * fk
        fk *= 128
      } while (b & 0x80)
      return (f % 2 ? -(f + 1) : f) / 2
    }

    return (n >> 1) ^ -(n & 1)
  }
}
