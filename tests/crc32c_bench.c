/*
 * librdkafka - Apache Kafka C library
 *
 * CRC32C three-way benchmark:
 *   SW       - pure software table-driven (slice-by-8)
 *   HW_old   - SSE4.2 crc32q + GF(2) table-based stripe folding
 *   HW_new   - SSE4.2 crc32q + PCLMULQDQ folding + unrolled 768B function
 *
 * Build:
 *   gcc -g -O2 -Wall -I../src -o crc32c_bench crc32c_bench.c \
 *       ../src/librdkafka.a -lpthread -lrt -ldl -lm -lz -lssl -lcrypto \
 *       -lzstd -lcurl -llz4
 * Run:
 *   ./crc32c_bench
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <time.h>

#include "../src/crc32c.h"

/* --- software CRC (identical to crc32c_sw logic) --- */
#define POLY 0x82f63b78

static uint32_t sw_table[8][256];
static int sw_ready;

static void sw_init(void) {
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
                sw_table[0][n] = crc;
        }
        for (n = 0; n < 256; n++) {
                crc = sw_table[0][n];
                for (k = 1; k < 8; k++) {
                        crc = sw_table[0][crc & 0xff] ^ (crc >> 8);
                        sw_table[k][n] = crc;
                }
        }
        sw_ready = 1;
}

static uint32_t sw_crc32c(uint32_t crci, const void *buf, size_t len) {
        const unsigned char *next = buf;
        uint64_t crc;
        if (!sw_ready) sw_init();
        crc = crci ^ 0xffffffff;
        while (len && ((uintptr_t)next & 7) != 0) {
                crc = sw_table[0][(crc ^ *next++) & 0xff] ^ (crc >> 8);
                len--;
        }
        while (len >= 8) {
                uint64_t ncopy;
                memcpy(&ncopy, next, sizeof(ncopy));
                crc ^= ncopy;
                crc = sw_table[7][crc & 0xff] ^
                      sw_table[6][(crc >> 8) & 0xff] ^
                      sw_table[5][(crc >> 16) & 0xff] ^
                      sw_table[4][(crc >> 24) & 0xff] ^
                      sw_table[3][(crc >> 32) & 0xff] ^
                      sw_table[2][(crc >> 40) & 0xff] ^
                      sw_table[1][(crc >> 48) & 0xff] ^
                      sw_table[0][crc >> 56];
                next += 8;
                len -= 8;
        }
        while (len) {
                crc = sw_table[0][(crc ^ *next++) & 0xff] ^ (crc >> 8);
                len--;
        }
        return (uint32_t)crc ^ 0xffffffff;
}

/* --- helpers --- */
static uint64_t rd_clock(void) {
        struct timespec ts;
        clock_gettime(CLOCK_MONOTONIC, &ts);
        return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}

typedef uint32_t (*crc32c_fn)(uint32_t, const void *, size_t);

static double bench(crc32c_fn fn, const unsigned char *buf, size_t size,
                    int iters, uint32_t *out_crc) {
        uint32_t crc = 0;
        uint64_t start, elapsed;
        int i;
        for (i = 0; i < 100; i++) crc = fn(crc, buf, size);
        start = rd_clock();
        for (i = 0; i < iters; i++) crc = fn(crc, buf, size);
        elapsed = rd_clock() - start;
        *out_crc = crc;
        return ((double)size * iters) / ((double)elapsed / 1e9) / 1e6;
}

/* --- main --- */
int main(void) {
        size_t sizes[] = {64, 256, 512, 1024, 4096, 8192,
                          16384, 65536, 262144, 1048576};
        int n_sizes = sizeof(sizes) / sizeof(sizes[0]);
        unsigned char *buf = malloc(1048576 + 8);
        double sw_mbps, old_mbps, new_mbps;
        uint32_t sw_crc, old_crc, new_crc;
        int i, iters;

        sw_init();
        srand(42);
        for (i = 0; i < (int)(1048576 + 8); i++) buf[i] = (unsigned char)rand();

        printf("=== CRC32C Three-Way Comparison ===\n");
        printf("SW  = pure software (slice-by-8)\n");
        printf("OLD = SSE4.2 crc32q + GF(2) table-based fold (before this PR)\n");
        printf("NEW = SSE4.2 crc32q + PCLMULQDQ fold + unrolled 768B (this PR)\n\n");

        printf("%-10s %12s %12s %12s %10s %10s\n",
               "Size", "SW", "HW_old", "HW_new", "old/SW", "new/old");
        printf("%-10s %12s %12s %12s %10s %10s\n",
               "----------", "--------", "--------", "--------",
               "------", "------");

        for (i = 0; i < n_sizes; i++) {
                size_t sz = sizes[i];
                const unsigned char *data = buf + 1;

                if (sz < 256)       iters = 500000;
                else if (sz < 8192) iters = 100000;
                else if (sz < 65536) iters = 10000;
                else if (sz < 262144) iters = 5000;
                else                iters = 1000;

                /* SW */
                sw_mbps = bench(sw_crc32c, data, sz, iters, &sw_crc);

                /* HW_old: disable CLMUL to get original table-fold path */
                setenv("RD_CRC32C_NO_CLMUL", "1", 1);
                extern void rd_crc32c_global_init(void);
                rd_crc32c_global_init();
                old_mbps = bench(rd_crc32c, data, sz, iters, &old_crc);

                /* HW_new: default (CLMUL + unrolled) */
                unsetenv("RD_CRC32C_NO_CLMUL");
                rd_crc32c_global_init();
                new_mbps = bench(rd_crc32c, data, sz, iters, &new_crc);

                /* Verify */
                if (sw_crc != old_crc || old_crc != new_crc) {
                        printf("  *** CRC MISMATCH sw=0x%08x old=0x%08x new=0x%08x ***\n",
                               sw_crc, old_crc, new_crc);
                }

                printf("%-10s ", "");
                switch (sz) {
                case 64:     printf("64 B       "); break;
                case 256:    printf("256 B      "); break;
                case 512:    printf("512 B      "); break;
                case 1024:   printf("1 KB       "); break;
                case 4096:   printf("4 KB       "); break;
                case 8192:   printf("8 KB       "); break;
                case 16384:  printf("16 KB      "); break;
                case 65536:  printf("64 KB      "); break;
                case 262144: printf("256 KB     "); break;
                case 1048576:printf("1 MB       "); break;
                }
                printf("%9.0f MB/s %9.0f MB/s %9.0f MB/s %8.1fx %8.1fx\n",
                       sw_mbps, old_mbps, new_mbps,
                       old_mbps / sw_mbps, new_mbps / old_mbps);
        }

        printf("\n=== Summary ===\n");
        printf("HW_old vs SW:  %.1fx (hardware CRC32Q with table-based fold)\n",
               old_mbps / sw_mbps);
        printf("HW_new vs old: %.2fx (PCLMULQDQ fold + unrolled 768B function)\n",
               new_mbps / old_mbps);
        printf("HW_new vs SW:  %.1fx (total speedup over software)\n",
               new_mbps / sw_mbps);

        free(buf);
        return 0;
}
