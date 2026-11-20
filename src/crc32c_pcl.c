/*
 * librdkafka - Apache Kafka C library
 *
 * CRC32C using full PCLMULQDQ polynomial folding.
 *
 * Based on "Fast CRC Computation for Generic Polynomials Using
 * PCLMULQDQ Instruction" (Gopal, Ozturk et al., Intel 2009).
 *
 * The algorithm treats the input as a polynomial in GF(2) and
 * folds 128-bit blocks in parallel using 4 XMM registers.
 * After all data is processed, the 4 registers are reduced
 * to a single 32-bit CRC via Barrett reduction.
 */

#include "rd.h"
#include "rdcrc32.h"
#include "crc32c.h"

#if WITH_CRC32C_HW
#include <smmintrin.h>
#include <wmmintrin.h>
#include <emmintrin.h>

/* ── folding constants ───────────────────────────────────── */

/* K for Phase 1: fold each XMM register forward by 64 bytes
 * (4 registers × 16 bytes interleave = 64-byte stride).
 * Packed as [K_hi : K_lo] where:
 *   K_lo = x^(64*8)   mod P  = x^512  mod P  (low 64-bit half)
 *   K_hi = x^(64*8+64) mod P = x^576  mod P  (high 64-bit half)
 *
 * For Phase 2 reduction (4→1):
 *   K16_lo = x^128 mod P,  K16_hi = x^192 mod P  (16-byte fold) */

static __m128i K64;   /* K for 64-byte fold: [K64_hi : K64_lo] */
static __m128i K16;   /* K for 16-byte fold: [K16_hi : K16_lo] */

/* ── GF(2) helpers ───────────────────────────────────────── */

/* CRC-32C (iSCSI) reflected polynomial */
#define P_CRC32C 0x82f63b78

/* GF(2) matrix × vector multiplication (same as in crc32c.c).
 * mat has 32 rows; vec is a 32-bit vector. */
static RD_INLINE uint32_t gf2_mv(uint32_t *mat, uint32_t vec) {
        uint32_t sum = 0;
        while (vec) {
                if (vec & 1) sum ^= *mat;
                vec >>= 1;
                mat++;
        }
        return sum;
}

/* GF(2) matrix square: square = mat × mat (32 rows). */
static RD_INLINE void gf2_msq(uint32_t *square, uint32_t *mat) {
        int n;
        for (n = 0; n < 32; n++)
                square[n] = gf2_mv(mat, mat[n]);
}

/* Build the 32×32 GF(2) operator for "multiply by x^len" mod P,
 * where len is in BYTES (must be a power of 2).
 * Returns the operator in even[32]. */
static void gf2_shift_op(uint32_t *even, size_t len) {
        int n;
        uint32_t row;
        uint32_t odd[32];

        odd[0] = P_CRC32C;
        row = 1;
        for (n = 1; n < 32; n++) { odd[n] = row; row <<= 1; }

        gf2_msq(even, odd);     /* even = 2-bit shift */
        gf2_msq(odd, even);     /* odd  = 4-bit shift */

        do {
                gf2_msq(even, odd);     /* even *= 2 (bits) */
                len >>= 1;
                if (len == 0) return;
                gf2_msq(odd, even);     /* odd *= 2 */
                len >>= 1;
        } while (len);

        /* answer ended up in odd */
        for (n = 0; n < 32; n++)
                even[n] = odd[n];
}

/* Compute x^(N*8) mod P in reflected form (N in bytes).
 * This is the 32-bit GF(2) constant to fold a CRC forward by N bytes. */
static uint32_t gf2_x_pow_mod_p(size_t n_bytes) {
        uint32_t op[32];
        gf2_shift_op(op, n_bytes);
        return op[0];
}

/* ── constant initialization ─────────────────────────────── */

__attribute__((target("pclmul,sse4.2")))
static void pcl_constants_init(void) {
        uint32_t k_lo, k_hi;

        /* Phase 1: 64-byte fold constant */
        k_lo = gf2_x_pow_mod_p(64);       /* x^512  mod P */
        k_hi = gf2_x_pow_mod_p(64 + 8);   /* x^576  mod P */
        K64 = _mm_set_epi64x(k_hi, k_lo);

        /* Phase 2: 16-byte fold constant */
        k_lo = gf2_x_pow_mod_p(16);       /* x^128  mod P */
        k_hi = gf2_x_pow_mod_p(16 + 8);   /* x^192  mod P */
        K16 = _mm_set_epi64x(k_hi, k_lo);
}

/* Phase 1 helper: fold xmm by 64 bytes and XOR in new data.
 * result = pclmulqdq(xmm, K64, 0x00) ^ pclmulqdq(xmm, K64, 0x11) ^ data */
__attribute__((target("pclmul,sse4.2")))
static RD_INLINE __m128i fold64(__m128i xmm, __m128i data) {
        __m128i lo, hi;
        lo = _mm_clmulepi64_si128(xmm, K64, 0x00);  /* low  × K_lo */
        hi = _mm_clmulepi64_si128(xmm, K64, 0x11);  /* high × K_hi */
        return _mm_xor_si128(_mm_xor_si128(lo, hi), data);
}

/* Phase 2 helper: fold xmm by 16 bytes.
 * result = pclmulqdq(xmm, K16, 0x00) ^ pclmulqdq(xmm, K16, 0x11) */
__attribute__((target("pclmul,sse4.2")))
static RD_INLINE __m128i fold16(__m128i xmm) {
        __m128i lo, hi;
        lo = _mm_clmulepi64_si128(xmm, K16, 0x00);
        hi = _mm_clmulepi64_si128(xmm, K16, 0x11);
        return _mm_xor_si128(lo, hi);
}

/* Phase 3: reduce 128-bit polynomial in xmm to 32-bit CRC.
 * poly(x) = lo(x) + hi(x) * x^64, where lo = bits[63:0], hi = bits[127:64].
 * We want: poly * x^32 mod P.
 * = (lo * x^32) mod P ^ (hi * x^96) mod P
 * = crc32q(0, lo) ^ crc32q(crc32q(0, hi), 0...no.
 *
 * crc32q(init, data) = data * x^32 mod P ^ init * x^64 mod P.
 * So: crc32q(0, hi) = hi * x^32.
 *     crc32q(crc32q(0, hi), lo) = lo * x^32 ^ (hi * x^32) * x^64 = lo * x^32 ^ hi * x^96.
 * This matches poly * x^32 mod P. */
__attribute__((target("pclmul,sse4.2")))
static RD_INLINE uint32_t poly128_to_crc32(__m128i xmm) {
        uint64_t lo = _mm_extract_epi64(xmm, 0);
        uint64_t hi = _mm_extract_epi64(xmm, 1);
        uint64_t crc;

        /* poly * x^32 mod P = lo * x^32 mod P ^ hi * x^96 mod P */
        crc = _mm_crc32_u64(0, hi);       /* hi * x^32 mod P */
        crc = _mm_crc32_u64(crc, lo);     /* lo * x^32 mod P ^ crc * x^64 mod P
                                             = lo * x^32 mod P ^ hi * x^96 mod P */
        return (uint32_t)crc;
}

/* ── main entry point ──────────────────────────────────────
 *
 * Hybrid approach:
 *   Phase 0: crc32b → crc32q for first 64+ bytes to establish
 *            a proper CRC-32C state with correct init-xor handling.
 *   Phase 1: 4-way PCLMULQDQ folding for subsequent 64-byte blocks.
 *   Phase 2: reduce 4 XMM registers → 1.
 *   Phase 3: convert 128-bit polynomial → 32-bit CRC.
 *   Phase 4: crc32q/crc32b for trailing bytes.
 */
__attribute__((target("pclmul,sse4.2")))
uint32_t crc32c_pcl_full(uint32_t crc, const void *buf, size_t len) {
        const unsigned char *p = (const unsigned char *)buf;
        uint64_t state;  /* 64-bit CRC accumulator */
        __m128i x0, x1, x2, x3;
        __m128i d0, d1, d2, d3;

        /* Pre-process: invert bits per CRC-32C convention.
         * crc32 u64/u8 instructions directly update the state. */
        state = crc ^ 0xffffffff;

        /* ── Phase 0a: align to 8 bytes ── */
        while (len && ((uintptr_t)p & 7) != 0) {
                state = _mm_crc32_u8((uint32_t)state, *p++);
                len--;
        }

        /* ── Phase 0b: crc32q for leading data.
         * Always process at least 64 bytes with crc32q so the CRC
         * state machine produces a well-defined polynomial value.  Only
         * switch to PCLMULQDQ when we have another 64+ bytes remaining. */
        while (len >= 64 && len - 64 >= 64) {
                /* Process 64 bytes with crc32q, then switch to PCLMULQDQ
                 * for the rest.  Break after the first 64 to set up state. */
                int i;
                for (i = 0; i < 8; i++) {
                        state = _mm_crc32_u64(state,
                            *(const uint64_t *)(p + i * 8));
                }
                p += 64;
                len -= 64;
                break;  /* Only consume ONE 64-byte block with crc32q */
        }

        if (len < 64) {
                /* All remaining data fits in crc32q tail path */
                while (len >= 8) {
                        state = _mm_crc32_u64(state,
                            *(const uint64_t *)p);
                        p += 8; len -= 8;
                }
                while (len) {
                        state = _mm_crc32_u8((uint32_t)state, *p++);
                        len--;
                }
                return (uint32_t)state ^ 0xffffffff;
        }

        /* ── Phase 1: 4-way parallel PCLMULQDQ folding ── */
        /* Load first 64 bytes of remaining data into 4 XMM registers.
         * The 32-bit CRC state (from crc32q) seeds the polynomial folding. */
        x0 = _mm_loadu_si128((const __m128i *)(p +  0));
        x1 = _mm_loadu_si128((const __m128i *)(p + 16));
        x2 = _mm_loadu_si128((const __m128i *)(p + 32));
        x3 = _mm_loadu_si128((const __m128i *)(p + 48));

        /* XOR the crc32q state into the low dword of x0.
         * The crc32q state represents: data_so_far * x^32 mod P ^ init_factor.
         * In polynomial form: it's a 32-bit GF(2) value whose bits are
         * coefficients of x^0 .. x^31.  Xor-ing into x0's low dword
         * continues the polynomial accumulation. */
        x0 = _mm_xor_si128(x0, _mm_cvtsi32_si128((int)state));

        p += 64;
        len -= 64;

        while (len >= 64) {
                _mm_prefetch((const char *)(p + 256), _MM_HINT_T0);

                d0 = _mm_loadu_si128((const __m128i *)(p +  0));
                d1 = _mm_loadu_si128((const __m128i *)(p + 16));
                d2 = _mm_loadu_si128((const __m128i *)(p + 32));
                d3 = _mm_loadu_si128((const __m128i *)(p + 48));

                x0 = fold64(x0, d0);
                x1 = fold64(x1, d1);
                x2 = fold64(x2, d2);
                x3 = fold64(x3, d3);

                p += 64;
                len -= 64;
        }

        /* ── Phase 2: reduce 4 XMM → 1 ── */
        x0 = _mm_xor_si128(fold16(x0), x1);
        x2 = _mm_xor_si128(fold16(x2), x3);
        x0 = _mm_xor_si128(fold16(x0), x2);

        /* ── Phase 3: 128-bit polynomial → 32-bit CRC ── */
        state = poly128_to_crc32(x0);

        /* ── Phase 4: tail bytes ── */
        while (len >= 8) {
                state = _mm_crc32_u64(state, *(const uint64_t *)p);
                p += 8; len -= 8;
        }
        while (len) {
                state = _mm_crc32_u8((uint32_t)state, *p++);
                len--;
        }

        return (uint32_t)state ^ 0xffffffff;
}

/* ── init ────────────────────────────────────────────────── */

/* Called once at startup. */
void crc32c_pcl_init(void) {
        pcl_constants_init();
}

#else /* !WITH_CRC32C_HW */
/* Stub: PCL path requires hardware CRC support */
void crc32c_pcl_init(void) {}
#endif /* WITH_CRC32C_HW */
