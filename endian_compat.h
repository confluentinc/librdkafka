/*
 * librdkafka - Apache Kafka C library
 *
 * Copyright (c) 2012 - 2014 Magnus Edenhill
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#ifdef __FreeBSD__
  #include <sys/endian.h>
#elif defined __linux__
  #include <endian.h>
#elif defined __CYGWIN__
 #include <endian.h>
#elif defined __BSD__
  #include <sys/endian.h>
#elif defined __APPLE__
  #include <sys/_endian.h>
  #include <libkern/OSByteOrder.h>
  #define __bswap_64(x)      OSSwapInt64(x)
  #define __bswap_32(x)      OSSwapInt32(x)
  #define __bswap_16(x)      OSSwapInt16(x)

  #if __DARWIN_BYTE_ORDER == __DARWIN_BIG_ENDIAN
    #define htobe16(x) (x)
    #define htole16(x) __bswap_16 (x)
    #define be16toh(x) (x)
    #define le16toh(x) __bswap_16 (x)

    #define htobe32(x) (x)
    #define htole32(x) __bswap_32 (x)
    #define be32toh(x) (x)
    #define le32toh(x) __bswap_32 (x)

    #define htobe64(x) (x)
    #define htole64(x) __bswap_64 (x)
    #define be64toh(x) (x)
    #define le64toh(x) __bswap_64 (x)
  #else
    #define htobe16(x) __bswap_16 (x)
    #define htole16(x) (x)
    #define be16toh(x) __bswap_16 (x)
    #define le16toh(x) (x)

    #define htobe32(x) __bswap_32 (x)
    #define htole32(x) (x)
    #define be32toh(x) __bswap_32 (x)
    #define le32toh(x) (x)

    #define htobe64(x) __bswap_64 (x)
    #define htole64(x) (x)
    #define be64toh(x) __bswap_64 (x)
    #define le64toh(x) (x)
  #endif
#else
  #error Unknown location for endian.h
#endif
