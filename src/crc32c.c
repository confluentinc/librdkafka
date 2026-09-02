/* Copied from http://stackoverflow.com/a/17646775/1821055
 * with the following modifications:
 *   * remove test code
 *   * global hw/sw initialization to be called once per process
 *   * HW support is determined by configure's WITH_CRC32C_HW
 *   * Windows porting (no hardware support on Windows yet)
 *
 * FIXME:
 *   * Hardware support on Windows (MSVC assembler)
 *   * Hardware support on ARM
 */

/* crc32c.c -- compute CRC-32C using the Intel crc32 instruction
 * Copyright (C) 2013 Mark Adler
 * Version 1.1  1 Aug 2013  Mark Adler
 */

/*
  This software is provided 'as-is', without any express or implied
  warranty.  In no event will the author be held liable for any damages
  arising from the use of this software.

  Permission is granted to anyone to use this software for any purpose,
  including commercial applications, and to alter it and redistribute it
  freely, subject to the following restrictions:

  1. The origin of this software must not be misrepresented; you must not
     claim that you wrote the original software. If you use this software
     in a product, an acknowledgment in the product documentation would be
     appreciated but is not required.
  2. Altered source versions must be plainly marked as such, and must not be
     misrepresented as being the original software.
  3. This notice may not be removed or altered from any source distribution.

  Mark Adler
  madler@alumni.caltech.edu
 */

/* Use hardware CRC instruction on Intel SSE 4.2 processors.  This computes a
   CRC-32C, *not* the CRC-32 used by Ethernet and zip, gzip, etc.  A software
   version is provided as a fall-back, as well as for speed comparisons. */

/* Version history:
   1.0  10 Feb 2013  First version
   1.1   1 Aug 2013  Correct comments on why three crc instructions in parallel
 */

#include "rd.h"

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#ifndef _WIN32
#include <unistd.h>
#endif

#include "rdunittest.h"
#include "rdendian.h"

#include "crc32c.h"
#include "crc32c_impl.h"

#if WITH_CRC32C_HW
#include <smmintrin.h>
#include <wmmintrin.h>
#include <emmintrin.h>
#endif

/* CRC-32C (iSCSI) polynomial in reversed bit order. */
#define POLY 0x82f63b78

/* Table for a quadword-at-a-time software crc. */
static uint32_t crc32c_table[8][256];

/* Construct table for software CRC-32C calculation. */
static void crc32c_init_sw(void)
{
    uint32_t n, crc, k;

    for (n = 0; n < 256; n++) {
        crc = n;
        crc = crc & 1 ? (crc >> 1) ^ POLY : crc >> 1;
        crc = crc & 1 ? (crc >> 1) ^ POLY : crc >> 1;
        crc = crc & 1 ? (crc >> 1) ^ POLY : crc >> 1;
        crc = crc & 1 ? (crc >> 1) ^ POLY : crc >> 1;
        crc = crc & 1 ? (crc >> 1) ^ POLY : crc >> 1;
        crc = crc & 1 ? (crc >> 1) ^ POLY : crc >> 1;
        crc = crc & 1 ? (crc >> 1) ^ POLY : crc >> 1;
        crc = crc & 1 ? (crc >> 1) ^ POLY : crc >> 1;
        crc32c_table[0][n] = crc;
    }
    for (n = 0; n < 256; n++) {
        crc = crc32c_table[0][n];
        for (k = 1; k < 8; k++) {
            crc = crc32c_table[0][crc & 0xff] ^ (crc >> 8);
            crc32c_table[k][n] = crc;
        }
    }
}

/* Table-driven software version as a fall-back.  This is about 15 times slower
   than using the hardware instructions.  This assumes little-endian integers,
   as is the case on Intel processors that the assembler code here is for. */
static uint32_t crc32c_sw(uint32_t crci, const void *buf, size_t len)
{
    const unsigned char *next = buf;
    uint64_t crc;

    crc = crci ^ 0xffffffff;
    while (len && ((uintptr_t)next & 7) != 0) {
        crc = crc32c_table[0][(crc ^ *next++) & 0xff] ^ (crc >> 8);
        len--;
    }
    while (len >= 8) {
        /* Alignment-safe */
        uint64_t ncopy;
        memcpy(&ncopy, next, sizeof(ncopy));
        crc ^= le64toh(ncopy);
        crc = crc32c_table[7][crc & 0xff] ^
              crc32c_table[6][(crc >> 8) & 0xff] ^
              crc32c_table[5][(crc >> 16) & 0xff] ^
              crc32c_table[4][(crc >> 24) & 0xff] ^
              crc32c_table[3][(crc >> 32) & 0xff] ^
              crc32c_table[2][(crc >> 40) & 0xff] ^
              crc32c_table[1][(crc >> 48) & 0xff] ^
              crc32c_table[0][crc >> 56];
        next += 8;
        len -= 8;
    }
    while (len) {
        crc = crc32c_table[0][(crc ^ *next++) & 0xff] ^ (crc >> 8);
        len--;
    }
    return (uint32_t)crc ^ 0xffffffff;
}


#if WITH_CRC32C_HW
static int sse42;  /* Cached SSE42 support */
static int clmul;  /* Cached PCLMULQDQ support */
int crc32c_have_sse42;  /* Exported for PCL path */
int crc32c_have_clmul;  /* Exported for PCL path */

/* Dispatch table entry for the active CRC32C implementation */
static uint32_t (*crc32c_impl)(uint32_t, const void *, size_t);

/* Profiling counters (enable via RD_CRC32C_PROFILE=1 at init time) */
static struct {
        uint64_t calls;
        uint64_t total_bytes;
        uint64_t long_bytes;     /* bytes processed in LONG blocks */
        uint64_t short_bytes;    /* bytes processed in SHORT blocks */
        uint64_t residual_bytes; /* bytes processed in 8-byte residual */
        uint64_t align_bytes;    /* leading bytes for 8-byte alignment */
        uint64_t long_folds;     /* number of LONG block folds */
        uint64_t short_folds;    /* number of SHORT block folds */
        uint64_t clmul_folds;    /* folds using PCLMULQDQ */
        uint64_t table_folds;    /* folds using table-based shift */
        int      enabled;
} crc32c_profile;

static void crc32c_profile_print(void) {
        if (!crc32c_profile.enabled || !crc32c_profile.calls)
                return;
        fprintf(stderr,
                "=== CRC32C Profile ===\n"
                "  calls:          %"PRIu64"\n"
                "  total bytes:    %"PRIu64" (avg %.1f KB/call)\n"
                "  LONG bytes:     %"PRIu64" (%.1f%%)\n"
                "  SHORT bytes:    %"PRIu64" (%.1f%%)\n"
                "  residual bytes: %"PRIu64" (%.1f%%)\n"
                "  align bytes:    %"PRIu64"\n"
                "  LONG folds:     %"PRIu64"\n"
                "  SHORT folds:    %"PRIu64"\n"
                "  CLMUL folds:    %"PRIu64" (%.1f%%)\n"
                "  table folds:    %"PRIu64" (%.1f%%)\n",
                crc32c_profile.calls,
                crc32c_profile.total_bytes,
                crc32c_profile.calls ?
                (double)crc32c_profile.total_bytes / crc32c_profile.calls / 1024.0
                : 0.0,
                crc32c_profile.long_bytes,
                crc32c_profile.total_bytes ?
                100.0 * crc32c_profile.long_bytes / crc32c_profile.total_bytes
                : 0.0,
                crc32c_profile.short_bytes,
                crc32c_profile.total_bytes ?
                100.0 * crc32c_profile.short_bytes / crc32c_profile.total_bytes
                : 0.0,
                crc32c_profile.residual_bytes,
                crc32c_profile.total_bytes ?
                100.0 * crc32c_profile.residual_bytes / crc32c_profile.total_bytes
                : 0.0,
                crc32c_profile.align_bytes,
                crc32c_profile.long_folds,
                crc32c_profile.short_folds,
                crc32c_profile.clmul_folds,
                crc32c_profile.long_folds + crc32c_profile.short_folds ?
                100.0 * crc32c_profile.clmul_folds /
                (crc32c_profile.long_folds + crc32c_profile.short_folds) : 0.0,
                crc32c_profile.table_folds,
                crc32c_profile.long_folds + crc32c_profile.short_folds ?
                100.0 * crc32c_profile.table_folds /
                (crc32c_profile.long_folds + crc32c_profile.short_folds) : 0.0);
}

/* Multiply a matrix times a vector over the Galois field of two elements,
   GF(2).  Each element is a bit in an unsigned integer.  mat must have at
   least as many entries as the power of two for most significant one bit in
   vec. */
static RD_INLINE uint32_t gf2_matrix_times(uint32_t *mat, uint32_t vec)
{
    uint32_t sum;

    sum = 0;
    while (vec) {
        if (vec & 1)
            sum ^= *mat;
        vec >>= 1;
        mat++;
    }
    return sum;
}

/* Multiply a matrix by itself over GF(2).  Both mat and square must have 32
   rows. */
static RD_INLINE void gf2_matrix_square(uint32_t *square, uint32_t *mat)
{
    int n;

    for (n = 0; n < 32; n++)
        square[n] = gf2_matrix_times(mat, mat[n]);
}

/* Construct an operator to apply len zeros to a crc.  len must be a power of
   two.  If len is not a power of two, then the result is the same as for the
   largest power of two less than len.  The result for len == 0 is the same as
   for len == 1.  A version of this routine could be easily written for any
   len, but that is not needed for this application. */
static void crc32c_zeros_op(uint32_t *even, size_t len)
{
    int n;
    uint32_t row;
    uint32_t odd[32];       /* odd-power-of-two zeros operator */

    /* put operator for one zero bit in odd */
    odd[0] = POLY;              /* CRC-32C polynomial */
    row = 1;
    for (n = 1; n < 32; n++) {
        odd[n] = row;
        row <<= 1;
    }

    /* put operator for two zero bits in even */
    gf2_matrix_square(even, odd);

    /* put operator for four zero bits in odd */
    gf2_matrix_square(odd, even);

    /* first square will put the operator for one zero byte (eight zero bits),
       in even -- next square puts operator for two zero bytes in odd, and so
       on, until len has been rotated down to zero */
    do {
        gf2_matrix_square(even, odd);
        len >>= 1;
        if (len == 0)
            return;
        gf2_matrix_square(odd, even);
        len >>= 1;
    } while (len);

    /* answer ended up in odd -- copy to even */
    for (n = 0; n < 32; n++)
        even[n] = odd[n];
}

/* Take a length and build four lookup tables for applying the zeros operator
   for that length, byte-by-byte on the operand. */
static void crc32c_zeros(uint32_t zeros[][256], size_t len)
{
    uint32_t n;
    uint32_t op[32];

    crc32c_zeros_op(op, len);
    for (n = 0; n < 256; n++) {
        zeros[0][n] = gf2_matrix_times(op, n);
        zeros[1][n] = gf2_matrix_times(op, n << 8);
        zeros[2][n] = gf2_matrix_times(op, n << 16);
        zeros[3][n] = gf2_matrix_times(op, n << 24);
    }
}

/* Apply the zeros operator table to crc. */
static RD_INLINE uint32_t crc32c_shift(uint32_t zeros[][256], uint32_t crc)
{
    return zeros[0][crc & 0xff] ^ zeros[1][(crc >> 8) & 0xff] ^
           zeros[2][(crc >> 16) & 0xff] ^ zeros[3][crc >> 24];
}

/* Block sizes for three-way parallel crc computation.  LONG and SHORT must
   both be powers of two.  The associated string constants must be set
   accordingly, for use in constructing the assembler instructions. */
#define LONG 8192
#define LONGx1 "8192"
#define LONGx2 "16384"
#define SHORT 256
#define SHORTx1 "256"
#define SHORTx2 "512"

/* Tables for hardware crc that shift a crc by LONG and SHORT zeros. */
static uint32_t crc32c_long[4][256];
static uint32_t crc32c_short[4][256];

/* K constants for folding CRC stripes using PCLMULQDQ.
 * K_long2 = x^(2*LONG*8) mod P  (fold crc0 by 16384 bytes)
 * K_long  = x^(LONG*8) mod P     (fold crc1 by 8192 bytes)
 * K_short2 = x^(2*SHORT*8) mod P (fold crc0 by 512 bytes)
 * K_short  = x^(SHORT*8) mod P   (fold crc1 by 256 bytes) */
static uint32_t crc32c_k_long2;
static uint32_t crc32c_k_long;
static uint32_t crc32c_k_short2;
static uint32_t crc32c_k_short;

/* Compute the PCLMULQDQ folding constant K' such that
 * crc32q(0, K') = x^(fold_distance_bytes * 8) mod P.
 *
 * The CRC32Q instruction computes: msg * x^32 mod P (in reflected form).
 * So K' must satisfy: K' * x^32 mod P = target = x^(N*8) mod P,
 * i.e. K' = x^(N*8 - 32) mod P.
 *
 * We compute this by building the 32x32 GF(2) matrix for the
 * crc32q(0, *) transform, inverting it, and applying to the target. */
__attribute__((target("sse4.2")))
static uint32_t crc32c_compute_k_clmul(uint32_t target) {
        uint32_t M[32];    /* M[i] = crc32q(0, 1 << i) */
        uint32_t inv[32];  /* starts as I, becomes M^(-1) */
        uint32_t K = 0;
        int row, col, pivot;

        for (row = 0; row < 32; row++) {
                M[row]   = (uint32_t)_mm_crc32_u64(0, 1ULL << row);
                inv[row] = 1u << row;
        }

        /* Gaussian elimination over GF(2) to compute M^(-1). */
        for (col = 0; col < 32; col++) {
                pivot = -1;
                for (row = col; row < 32; row++) {
                        if (M[row] & (1u << col)) {
                                pivot = row;
                                break;
                        }
                }
                if (pivot < 0)
                        continue;
                if (pivot != col) {
                        uint32_t t = M[col];
                        M[col] = M[pivot]; M[pivot] = t;
                        t = inv[col];
                        inv[col] = inv[pivot]; inv[pivot] = t;
                }
                for (row = 0; row < 32; row++) {
                        if (row != col && (M[row] & (1u << col))) {
                                M[row] ^= M[col];
                                inv[row] ^= inv[col];
                        }
                }
        }

        /* K = M^(-1) * target */
        for (row = 0; row < 32; row++) {
                if (target & M[row]) {
                        K ^= inv[row];
                        target ^= M[row];
                }
        }

        return K;
}

/* Initialize tables for shifting crcs. */
static void crc32c_init_hw(void)
{
    uint32_t target;

    crc32c_zeros(crc32c_long, LONG);
    crc32c_zeros(crc32c_short, SHORT);

    /* K_long: fold by LONG bytes. target = x^(LONG*8) mod P */
    target = crc32c_shift(crc32c_long, 1);
    crc32c_k_long = crc32c_compute_k_clmul(target);

    /* K_long2: fold by 2*LONG bytes. target = shift(target, LONG) */
    target = crc32c_shift(crc32c_long, target);
    crc32c_k_long2 = crc32c_compute_k_clmul(target);

    /* K_short: fold by SHORT bytes */
    target = crc32c_shift(crc32c_short, 1);
    crc32c_k_short = crc32c_compute_k_clmul(target);

    /* K_short2: fold by 2*SHORT bytes */
    target = crc32c_shift(crc32c_short, target);
    crc32c_k_short2 = crc32c_compute_k_clmul(target);
}

/* Process exactly 3*SHORT (768) bytes using fully unrolled three-way
 * parallel CRC32Q with PCLMULQDQ folding and data prefetch.
 * Optimized for the SHORT block path — eliminates loop overhead. */
__attribute__((target("pclmul,sse4.2")))
static RD_UNUSED uint32_t
crc32c_hw_clmul_768(const unsigned char *next, uint64_t crc0_in) {
        uint64_t crc0 = crc0_in;  /* 64-bit for crc32q */

        __asm__ __volatile__(
            "xor          %%r11d, %%r11d  # crc1 = 0\n\t"
            "xor          %%r10d, %%r10d  # crc2 = 0\n\t"
            "movl           $4, %%r8d     # loop counter: 256/64 = 4\n\t"

            /* Prefetch ahead: next + 512 (2 stripes ahead) */
            "prefetcht0  512(%[in])\n\t"

            ".L_crc32c_768_loop:\n\t"

            /* Three-way parallel CRC32Q.  Offset fixed at 0; caller
             * sees next+0..next+767 as consumed; crc32q offsets encode
             * stripe positions directly. */
            "crc32q    0(%[in]), %[c0]\n\t"
            "crc32q  256(%[in]), %%r11\n\t"
            "crc32q  512(%[in]), %%r10\n\t"

            "crc32q    8(%[in]), %[c0]\n\t"
            "crc32q  264(%[in]), %%r11\n\t"
            "crc32q  520(%[in]), %%r10\n\t"

            "crc32q   16(%[in]), %[c0]\n\t"
            "crc32q  272(%[in]), %%r11\n\t"
            "crc32q  528(%[in]), %%r10\n\t"

            "crc32q   24(%[in]), %[c0]\n\t"
            "crc32q  280(%[in]), %%r11\n\t"
            "crc32q  536(%[in]), %%r10\n\t"

            "crc32q   32(%[in]), %[c0]\n\t"
            "crc32q  288(%[in]), %%r11\n\t"
            "crc32q  544(%[in]), %%r10\n\t"

            "crc32q   40(%[in]), %[c0]\n\t"
            "crc32q  296(%[in]), %%r11\n\t"
            "crc32q  552(%[in]), %%r10\n\t"

            "crc32q   48(%[in]), %[c0]\n\t"
            "crc32q  304(%[in]), %%r11\n\t"
            "crc32q  560(%[in]), %%r10\n\t"

            "crc32q   56(%[in]), %[c0]\n\t"
            "crc32q  312(%[in]), %%r11\n\t"
            "crc32q  568(%[in]), %%r10\n\t"

            "add            $64, %[in]\n\t"
            "decl           %%r8d\n\t"
            "jnz .L_crc32c_768_loop\n\t"
            "sub           $256, %[in]\n\t"

            /* Fold three stripes: crc0 * K_short2 ^ crc1 * K_short ^ crc2. */
            "movq        %[c0], %%xmm1\n\t"
            "movd    %[k_short2], %%xmm3\n\t"
            "movq        %%r11, %%xmm2\n\t"
            "movd     %[k_short], %%xmm4\n\t"
            "pclmulqdq $0x00, %%xmm3, %%xmm1\n\t"
            "pclmulqdq $0x00, %%xmm4, %%xmm2\n\t"
            "xor          %%ecx, %%ecx\n\t"
            "xor          %%r11d, %%r11d\n\t"
            "movq        %%xmm1, %%r8\n\t"
            "movq        %%xmm2, %%r9\n\t"
            "crc32q        %%r8, %%rcx\n\t"
            "crc32q        %%r9, %%r11\n\t"
            "xor          %%r10d, %%ecx\n\t"
            "xor          %%r11d, %%ecx\n\t"

            : [c0] "+c" (crc0), [in] "+r" (next)
            : [k_short2] "r" ((uint64_t)crc32c_k_short2),
              [k_short]  "r" ((uint64_t)crc32c_k_short)
            : "r8", "r9", "r10", "r11",
              "xmm1", "xmm2", "xmm3", "xmm4", "cc", "memory"
        );

        return (uint32_t)crc0;
}

/* Compute CRC-32C using the Intel hardware instruction. */
__attribute__((target("pclmul,sse4.2")))
static uint32_t crc32c_hw(uint32_t crc, const void *buf, size_t len)
{
    const unsigned char *next = buf;
    const unsigned char *end;
    uint64_t crc0, crc1, crc2;      /* need to be 64 bits for crc32q */

    /* pre-process the crc */
    crc0 = crc ^ 0xffffffff;

    if (crc32c_profile.enabled) {
            crc32c_profile.calls++;
            crc32c_profile.total_bytes += len;
    }

    /* compute the crc for up to seven leading bytes to bring the data pointer
       to an eight-byte boundary */
    while (len && ((uintptr_t)next & 7) != 0) {
        __asm__("crc32b\t" "(%1), %0"
                : "=r"(crc0)
                : "r"(next), "0"(crc0));
        next++;
        len--;
        if (crc32c_profile.enabled)
                crc32c_profile.align_bytes++;
    }

    /* compute the crc on sets of LONG*3 bytes, executing three independent crc
       instructions, each on LONG bytes -- this is optimized for the Nehalem,
       Westmere, Sandy Bridge, and Ivy Bridge architectures, which have a
       throughput of one crc per cycle, but a latency of three cycles */
    while (len >= LONG*3) {
        crc1 = 0;
        crc2 = 0;
        end = next + LONG;
        do {
            __asm__("crc32q\t" "(%3), %0\n\t"
                    "crc32q\t" LONGx1 "(%3), %1\n\t"
                    "crc32q\t" LONGx2 "(%3), %2"
                    : "=r"(crc0), "=r"(crc1), "=r"(crc2)
                    : "r"(next), "0"(crc0), "1"(crc1), "2"(crc2));
            next += 8;
        } while (next < end);
        if (clmul) {
                /* Fold three stripes using PCLMULQDQ:
                 * crc0 * K_long2 mod P ^ crc1 * K_long mod P ^ crc2 */
                __m128i a, b, p0, p1;
                uint64_t f0, f1, r;
                a = _mm_set_epi64x(0, (uint32_t)crc0);
                b = _mm_set_epi64x(0, crc32c_k_long2);
                p0 = _mm_clmulepi64_si128(a, b, 0x00);
                f0 = _mm_extract_epi64(p0, 0);
                a = _mm_set_epi64x(0, (uint32_t)crc1);
                b = _mm_set_epi64x(0, crc32c_k_long);
                p1 = _mm_clmulepi64_si128(a, b, 0x00);
                f1 = _mm_extract_epi64(p1, 0);
                r = _mm_crc32_u64(0, f0) ^ _mm_crc32_u64(0, f1) ^ (uint32_t)crc2;
                crc0 = r;
                if (crc32c_profile.enabled)
                        crc32c_profile.clmul_folds++;
        } else {
                crc0 = crc32c_shift(crc32c_long, (uint32_t)crc0) ^ crc1;
                crc0 = crc32c_shift(crc32c_long, (uint32_t)crc0) ^ crc2;
                if (crc32c_profile.enabled)
                        crc32c_profile.table_folds++;
        }
        if (crc32c_profile.enabled) {
                crc32c_profile.long_bytes += LONG * 3;
                crc32c_profile.long_folds++;
        }
        next += LONG*2;
        len -= LONG*3;
    }

    /* do the same thing, but now on SHORT*3 blocks for the remaining data less
       than a LONG*3 block */
    while (len >= SHORT*3) {
        if (clmul) {
                /* Use unrolled 768-byte function with prefetch */
                crc0 = crc32c_hw_clmul_768(next, crc0);
                next += SHORT * 3;   /* consumed 768 bytes */
                len -= SHORT * 3;
                if (crc32c_profile.enabled) {
                        crc32c_profile.short_bytes += SHORT * 3;
                        crc32c_profile.short_folds++;
                        crc32c_profile.clmul_folds++;
                }
        } else {
                crc1 = 0;
                crc2 = 0;
                end = next + SHORT;
                do {
                    __asm__("crc32q\t" "(%3), %0\n\t"
                            "crc32q\t" SHORTx1 "(%3), %1\n\t"
                            "crc32q\t" SHORTx2 "(%3), %2"
                            : "=r"(crc0), "=r"(crc1), "=r"(crc2)
                            : "r"(next), "0"(crc0), "1"(crc1), "2"(crc2));
                    next += 8;
                } while (next < end);
                crc0 = crc32c_shift(crc32c_short, (uint32_t)crc0) ^ crc1;
                crc0 = crc32c_shift(crc32c_short, (uint32_t)crc0) ^ crc2;
                if (crc32c_profile.enabled) {
                        crc32c_profile.short_bytes += SHORT * 3;
                        crc32c_profile.short_folds++;
                        crc32c_profile.table_folds++;
                }
                next += SHORT*2;
                len -= SHORT*3;
        }
    }

    /* compute the crc on the remaining eight-byte units less than a SHORT*3
       block */
    if (crc32c_profile.enabled)
            crc32c_profile.residual_bytes += len;
    end = next + (len - (len & 7));
    while (next < end) {
        __asm__("crc32q\t" "(%1), %0"
                : "=r"(crc0)
                : "r"(next), "0"(crc0));
        next += 8;
    }
    len &= 7;

    /* compute the crc for up to seven trailing bytes */
    while (len) {
        __asm__("crc32b\t" "(%1), %0"
                : "=r"(crc0)
                : "r"(next), "0"(crc0));
        next++;
        len--;
    }

    /* return a post-processed crc */
    return (uint32_t)crc0 ^ 0xffffffff;
}

/* Check for SSE 4.2.  SSE 4.2 was first supported in Nehalem processors
   introduced in November, 2008.  This does not check for the existence of the
   cpuid instruction itself, which was introduced on the 486SL in 1992, so this
   will fail on earlier x86 processors.  cpuid works on all Pentium and later
   processors. */
#define SSE42(have) \
    do { \
        uint32_t eax, ecx; \
        eax = 1; \
        __asm__("cpuid" \
                : "=c"(ecx) \
                : "a"(eax) \
                : "%ebx", "%edx"); \
        (have) = (ecx >> 20) & 1; \
    } while (0)

/* Check for PCLMULQDQ via CPUID.01H:ECX bit 1.
 * PCLMULQDQ was introduced in Westmere (2010), after SSE4.2 in Nehalem (2008).
 * This reuses the same CPUID call but checks a different bit. */
#define CLMUL(have) \
    do { \
        uint32_t eax, ecx; \
        eax = 1; \
        __asm__("cpuid" \
                : "=c"(ecx) \
                : "a"(eax) \
                : "%ebx", "%edx"); \
        (have) = (ecx >> 1) & 1; \
    } while (0)

#endif /* WITH_CRC32C_HW */

/* Compute a CRC-32C.  If the crc32 instruction is available, use the hardware
   version.  Otherwise, use the software version. */
uint32_t rd_crc32c(uint32_t crc, const void *buf, size_t len)
{
        return crc32c_impl(crc, buf, len);
}






/**
 * @brief Populate shift tables once
 */
void rd_crc32c_global_init (void) {
#if WITH_CRC32C_HW
        SSE42(sse42);
        crc32c_have_sse42 = sse42;
        if (sse42) {
                CLMUL(clmul);
                crc32c_have_clmul = clmul;
                if (clmul && getenv("RD_CRC32C_NO_CLMUL"))
                        clmul = 0;
                if (getenv("RD_CRC32C_PROFILE")) {
                        crc32c_profile.enabled = 1;
                        atexit(crc32c_profile_print);
                }
                crc32c_init_hw();

                /* Default: crc32q + PCLMULQDQ fold (proven + correct).
                 * Opt-in: full PCLMULQDQ folding via RD_CRC32C_PCL_FULL=1 */
                if (clmul && getenv("RD_CRC32C_PCL_FULL")) {
                        crc32c_pcl_init();
                        crc32c_impl = crc32c_pcl_full;
                } else {
                        crc32c_impl = crc32c_hw;
                }
        } else
#endif
        {
                crc32c_init_sw();
                crc32c_impl = crc32c_sw;
        }
}

int unittest_rd_crc32c (void) {
        const char *buf =
"  This software is provided 'as-is', without any express or implied\n"
"  warranty.  In no event will the author be held liable for any damages\n"
"  arising from the use of this software.\n"
"\n"
"  Permission is granted to anyone to use this software for any purpose,\n"
"  including commercial applications, and to alter it and redistribute it\n"
"  freely, subject to the following restrictions:\n"
"\n"
"  1. The origin of this software must not be misrepresented; you must not\n"
"     claim that you wrote the original software. If you use this software\n"
"     in a product, an acknowledgment in the product documentation would be\n"
"     appreciated but is not required.\n"
"  2. Altered source versions must be plainly marked as such, and must not be\n"
"     misrepresented as being the original software.\n"
"  3. This notice may not be removed or altered from any source distribution.";
        const uint32_t expected_crc = 0x7dcde113;
        uint32_t crc;
        const char *how;

#if WITH_CRC32C_HW
        if (sse42) {
                if (clmul)
                        how = "hardware (SSE42+PCLMULQDQ)";
                else
                        how = "hardware (SSE42)";
        } else
                how = "software (SSE42 supported in build but not at runtime)";
#else
        how = "software";
#endif
        RD_UT_SAY("Calculate CRC32C using %s", how);

        crc = rd_crc32c(0, buf, strlen(buf));
        RD_UT_ASSERT(crc == expected_crc,
                     "Calculated CRC (%s) 0x%"PRIx32
                     " not matching expected CRC 0x%"PRIx32,
                     how, crc, expected_crc);

        /* Verify software version too, regardless of which
         * version was used above. */
        crc32c_init_sw();
        RD_UT_SAY("Calculate CRC32C using software");
        crc = crc32c_sw(0, buf, strlen(buf));
        RD_UT_ASSERT(crc == expected_crc,
                     "Calculated CRC (software) 0x%"PRIx32
                     " not matching expected CRC 0x%"PRIx32,
                     crc, expected_crc);

        RD_UT_PASS();
}
