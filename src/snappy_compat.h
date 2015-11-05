#ifdef __FreeBSD__
#  include <sys/endian.h>
#elif defined(__APPLE_CC_) || defined(__MACH__)  /* MacOS/X support */
#  include <machine/endian.h>

#if    __DARWIN_BYTE_ORDER == __DARWIN_LITTLE_ENDIAN
#  define	htole16(x) (x)
#  define	le32toh(x) (x)
#elif  __DARWIN_BYTE_ORDER == __DARWIN_BIG_ENDIAN
#  define	htole16(x) __DARWIN_OSSwapInt16(x)
#  define	le32toh(x) __DARWIN_OSSwapInt32(x)
#else
#  error "Endianness is undefined"
#endif


#elif !defined(__WIN32__)
#  include <endian.h>
#endif

#include <stdlib.h>
#include <assert.h>
#include <string.h>
#include <errno.h>
#include <stdbool.h>
#include <limits.h>
#ifndef __WIN32__
#include <sys/uio.h>
#endif

#ifdef __ANDROID__
#define le32toh letoh32
#endif

#if defined(__WIN32__) && defined(SG)
struct iovec {
	void *iov_base;	/* Pointer to data.  */
	size_t iov_len;	/* Length of data.  */
};
#endif

#define get_unaligned_memcpy(x) ({ \
		typeof(*(x)) _ret; \
		memcpy(&_ret, (x), sizeof(*(x))); \
		_ret; })
#define put_unaligned_memcpy(v,x) ({ \
		typeof((v)) _v = (v); \
		memcpy((x), &_v, sizeof(*(x))); })

#define get_unaligned_direct(x) (*(x))
#define put_unaligned_direct(v,x) (*(x) = (v))

// Potentially unaligned loads and stores.
// x86 and PowerPC can simply do these loads and stores native.
#if defined(__i386__) || defined(__x86_64__) || defined(__powerpc__)

#define get_unaligned get_unaligned_direct
#define put_unaligned put_unaligned_direct
#define get_unaligned64 get_unaligned_direct
#define put_unaligned64 put_unaligned_direct

// ARMv7 and newer support native unaligned accesses, but only of 16-bit
// and 32-bit values (not 64-bit); older versions either raise a fatal signal,
// do an unaligned read and rotate the words around a bit, or do the reads very
// slowly (trip through kernel mode). There's no simple #define that says just
// “ARMv7 or higher”, so we have to filter away all ARMv5 and ARMv6
// sub-architectures.
//
// This is a mess, but there's not much we can do about it.
#elif defined(__arm__) && \
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

#define get_unaligned get_unaligned_direct
#define put_unaligned put_unaligned_direct
#define get_unaligned64 get_unaligned_memcpy
#define put_unaligned64 put_unaligned_memcpy

// These macroses are provided for architectures that don't support
// unaligned loads and stores.
#else

#define get_unaligned get_unaligned_memcpy
#define put_unaligned put_unaligned_memcpy
#define get_unaligned64 get_unaligned_memcpy
#define put_unaligned64 put_unaligned_memcpy

#endif

#define get_unaligned_le32(x) (le32toh(get_unaligned((u32 *)(x))))
#define put_unaligned_le16(v,x) (put_unaligned(htole16(v), (u16 *)(x)))

typedef unsigned char u8;
typedef unsigned short u16;
typedef unsigned u32;
typedef unsigned long long u64;

#define BUG_ON(x) assert(!(x))

#define vmalloc(x) malloc(x)
#define vfree(x) free(x)

#define EXPORT_SYMBOL(x)

#define ARRAY_SIZE(x) (sizeof(x) / sizeof(*(x)))

#define likely(x) __builtin_expect((x), 1)
#define unlikely(x) __builtin_expect((x), 0)

#define min_t(t,x,y) ((x) < (y) ? (x) : (y))
#define max_t(t,x,y) ((x) > (y) ? (x) : (y))

#if __BYTE_ORDER == __LITTLE_ENDIAN
#define __LITTLE_ENDIAN__ 1
#endif

#if __LITTLE_ENDIAN__ == 1 && (defined(__LSB_VERSION__) || defined(__WIN32__))
#define htole16(x) (x)
#define le32toh(x) (x)
#endif

#define BITS_PER_LONG (__SIZEOF_LONG__ * 8)
