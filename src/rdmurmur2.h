#ifndef __RDMURMUR2___H__
#define __RDMURMUR2___H__

#include "MurMurHash2.h"

typedef uint32_t rd_murmur2_t;

#define MURMUR2_SEED 0x9747b28c

#define INT32_TO_UINT32 0x7FFFFFFF

static RD_INLINE rd_murmur2_t rd_murmur2(const char *data, size_t data_len) {
  /*
   * last bit is set to 0 because the java implementation uses int_32
   * and then sets to positive number flipping last bit to 1
   */
  return MurmurHashNeutral2(data, data_len, MURMUR2_SEED) & INT32_TO_UINT32;
}

int unittest_murmurhashneutral2(void);

#endif // __RDMURMUR2___H__
