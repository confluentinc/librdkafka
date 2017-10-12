#ifndef __RDMURMUR2___H__
#define __RDMURMUR2___H__

#include "MurMurHash2.h"
#include "rd.h"

typedef uint32_t rd_murmur2_t;

#ifdef __cplusplus
extern "C" {
#endif

static uint32_t rd_murmur2_seed = 0x9747b28c;

static RD_INLINE rd_murmur2_t rd_murmur2 (const char *data, size_t data_len) {
    return MurmurHash2(data, data_len, rd_murmur2_seed);
}

#ifdef __cplusplus
}
#endif

#endif // __RDMURMUR2___H__
