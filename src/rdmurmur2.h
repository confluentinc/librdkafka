#ifndef __RDMURMUR2___H__
#define __RDMURMUR2___H__

#ifdef __cplusplus
extern "C" {
#endif

#include "MurMurHash2.h"
#include "rd.h"

typedef uint32_t rd_murmur2_t;

#define MURMUR2_SEED 0x9747b28c

static RD_INLINE rd_murmur2_t rd_murmur2 (const char *data, size_t data_len) {
    return MurmurHashNeutral2(data, data_len, MURMUR2_SEED);
}

#ifdef __cplusplus
}
#endif

#endif // __RDMURMUR2___H__
