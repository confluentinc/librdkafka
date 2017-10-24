/* MurmurHash2, by abrandoned
 * hosted on https://github.com/abrandoned/murmur2
 * with the following modifications:
 *   * added unitest to compare to kafka java client's murmurhash2

 */

#ifndef MURMUR_PLATFORM_H
#define MURMUR_PLATFORM_H

  #ifdef __cplusplus
    extern "C" {
  #endif

  #include <stdint.h>

  #define BIG_CONSTANT(x) (x##LLU)

  uint32_t MurmurHash2        ( const void * key, int len, uint32_t seed );
  uint32_t MurmurHash2A       ( const void * key, int len, uint32_t seed );
  uint32_t MurmurHashNeutral2 ( const void * key, int len, uint32_t seed );
  uint32_t MurmurHashAligned2 ( const void * key, int len, uint32_t seed );
  uint64_t MurmurHash64A      ( const void * key, int len, uint64_t seed );
  uint64_t MurmurHash64B      ( const void * key, int len, uint64_t seed );

  #ifdef __cplusplus
    }
  #endif

#endif /* MURMUR_PLATFORM_H */
