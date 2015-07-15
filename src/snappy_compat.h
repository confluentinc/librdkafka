#include <stdlib.h>
#include <assert.h>
#include <string.h>
#include <errno.h>
#include <stdbool.h>
#include <limits.h>
#if !defined(_MSC_VER) && !defined(MINGW_VER)
#include <sys/uio.h>
#endif

#include "rd.h"
#include "rdendian.h"

#if defined(__arm__) && \
	!defined(__ARM_ARCH_4__) &&		\
	!defined(__ARM_ARCH_4T__) &&		\
	!defined(__ARM_ARCH_5__) &&		\
	!defined(__ARM_ARCH_5T__) &&		\
	!defined(__ARM_ARCH_5TE__) &&		\
	!defined(__ARM_ARCH_5TEJ__) &&		\
	!defined(__ARM_ARCH_6__) &&		\
	!defined(__ARM_ARCH_6J__) &&		\
	!defined(__ARM_ARCH_6K__) &&		\
	!defined(__ARM_ARCH_6Z__) &&		\
	!defined(__ARM_ARCH_6ZK__) &&		\
	!defined(__ARM_ARCH_6T2__)
#define  UNALIGNED64_REALLYS_SLOW 1
#endif

#ifndef htole16
# if __BYTE_ORDER == __LITTLE_ENDIAN
#  define htole16(x) (x)
#  define le32toh(x) (x)
# else
#  define htole16(x) __bswap_16 (x)
#  define le32toh(x) __bswap_32 (x)
#endif
#endif

typedef unsigned char u8;
typedef unsigned short u16;
typedef unsigned u32;
typedef unsigned long long u64;

#define BUG_ON(x) assert(!(x))

#define get_unaligned(x) (*(x))
#define get_unaligned_le32(x) (le32toh(*(u32 *)(x)))
#define put_unaligned(v,x) (*(x) = (v))
#define put_unaligned_le16(v,x) (*(u16 *)(x) = htole16(v))

/* You may want to define this on various ARM architectures */
#ifdef UNALIGNED64_REALLYS_SLOW
static __inline u64 get_unaligned64(const void *p)
{
	u64 t;
	memcpy(&t, p, 8);
	return t;
}
static __inline u64 put_unaligned64(u64 t, void *p)
{
	memcpy(p, &t, 8);
	return t;
}
#else
#define get_unaligned64(x) get_unaligned(x)
#define put_unaligned64(x,p) put_unaligned(x,p)
#endif

#define vmalloc(x) malloc(x)
#define vfree(x) free(x)

#define EXPORT_SYMBOL(x)

#define ARRAY_SIZE(x) (sizeof(x) / sizeof(*(x)))

#define min_t(t,x,y) ((x) < (y) ? (x) : (y))
#define max_t(t,x,y) ((x) > (y) ? (x) : (y))

#if __BYTE_ORDER == __LITTLE_ENDIAN
#define __LITTLE_ENDIAN__ 1
#endif

#define BITS_PER_LONG (__SIZEOF_LONG__ * 8)
