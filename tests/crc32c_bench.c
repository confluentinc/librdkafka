/*
 * librdkafka - Apache Kafka C library
 *
 * CRC32C benchmark: Before (SSE4.2 + table fold) vs After (SSE4.2 + PCLMULQDQ).
 *
 * Block sizes: 256 B → 16 MB (×16 geometric progression).
 * Each size: warmup + timed iterations, reports MB/s.
 *
 * Build and run:
 *   cd tests && ./bench-crc32c.sh
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <time.h>

#include "../src/crc32c.h"

/* ── helpers ────────────────────────────────────────────── */

static uint64_t now_ns(void) {
        struct timespec ts;
        clock_gettime(CLOCK_MONOTONIC, &ts);
        return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}

static void fill_random(unsigned char *buf, size_t len) {
        for (size_t i = 0; i < len; i++)
                buf[i] = (unsigned char)(rand() & 0xff);
}

typedef uint32_t (*crc32c_fn)(uint32_t, const void *, size_t);

/* Run benchmark, return MB/s. */
static double bench(crc32c_fn fn, const unsigned char *buf, size_t size,
                    int min_iter, double min_time_s,
                    uint32_t *out_crc) {
        int iters = min_iter;
        uint32_t crc = 0;
        uint64_t start, elapsed;
        double elapsed_s;

        /* Warmup: run until at least 10ms */
        crc = 0;
        start = now_ns();
        do {
                crc = fn(crc, buf, size);
                elapsed = now_ns() - start;
        } while (elapsed < 10000000ULL);  /* 10 ms */

        /* Timed run: run min_iter times or until min_time_s elapsed */
        crc = 0;
        start = now_ns();
        for (int i = 0; i < iters; i++)
                crc = fn(crc, buf, size);
        elapsed = now_ns() - start;
        elapsed_s = (double)elapsed / 1e9;

        /* If too fast, scale up iterations to hit min_time_s */
        if (elapsed_s < min_time_s) {
                int scale = (int)(min_time_s / elapsed_s) + 1;
                if (scale > 100) scale = 100;
                iters *= scale;
                crc = 0;
                start = now_ns();
                for (int i = 0; i < iters; i++)
                        crc = fn(crc, buf, size);
                elapsed = now_ns() - start;
                elapsed_s = (double)elapsed / 1e9;
        }

        *out_crc = crc;
        return ((double)size * iters) / elapsed_s / 1e6;
}

/* ── main ───────────────────────────────────────────────── */

int main(void) {
        /* Geometric progression: ×16 from 256 B to 16 MB */
        size_t sizes[] = {
                256, 4096, 65536, 1048576, 16777216
        };
        int n_sizes = sizeof(sizes) / sizeof(sizes[0]);
        size_t max_size = sizes[n_sizes - 1];
        unsigned char *buf;
        uint32_t old_crc, new_crc;

        /* Init */
        srand(42);
        buf = malloc(max_size + 8);
        fill_random(buf, max_size + 8);

        printf("=== CRC32C Benchmark: Before (SSE4.2) vs After (PCLMULQDQ) ===\n");
        printf("%-8s %14s %14s %8s\n",
               "Size", "Before_MB/s", "After_MB/s", "Speedup");
        printf("%-8s %14s %14s %8s\n",
               "--------", "-------------", "------------", "-------");

        for (int i = 0; i < n_sizes; i++) {
                size_t sz = sizes[i];
                const unsigned char *data = buf + 1;  /* unaligned */
                double old_mbps, new_mbps;

                /* ── Before: disable CLMUL to get original table-fold path ── */
                setenv("RD_CRC32C_NO_CLMUL", "1", 1);
                extern void rd_crc32c_global_init(void);
                rd_crc32c_global_init();
                old_mbps = bench(rd_crc32c, data, sz, 100, 0.5, &old_crc);

                /* ── After: default path (CLMUL + unrolled) ── */
                unsetenv("RD_CRC32C_NO_CLMUL");
                rd_crc32c_global_init();
                new_mbps = bench(rd_crc32c, data, sz, 100, 0.5, &new_crc);

                /* Verify correctness */
                if (old_crc != new_crc) {
                        printf("  *** CRC MISMATCH old=0x%08x new=0x%08x ***\n",
                               old_crc, new_crc);
                }

                /* Format size label */
                const char *label;
                char buf_label[32];
                if (sz < 1024) {
                        snprintf(buf_label, sizeof(buf_label), "%zu B", sz);
                        label = buf_label;
                } else if (sz < 1048576) {
                        snprintf(buf_label, sizeof(buf_label), "%zu KB",
                                 sz / 1024);
                        label = buf_label;
                } else {
                        snprintf(buf_label, sizeof(buf_label), "%zu MB",
                                 sz / 1048576);
                        label = buf_label;
                }

                printf("%-8s %12.0f MB/s %12.0f MB/s %6.2fx\n",
                       label, old_mbps, new_mbps,
                       old_mbps > 0 ? new_mbps / old_mbps : 0);
        }

        free(buf);
        return 0;
}
