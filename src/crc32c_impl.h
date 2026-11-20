/*
 * librdkafka - Apache Kafka C library
 *
 * Internal declarations shared across CRC32C implementation files.
 */

#ifndef _SRC_CRC32C_IMPL_H_
#define _SRC_CRC32C_IMPL_H_

#include "crc32c.h"

/* Full PCLMULQDQ folding path (from crc32c_pcl.c) */
#if WITH_CRC32C_HW
uint32_t crc32c_pcl_full(uint32_t crc, const void *buf, size_t len);
void     crc32c_pcl_init(void);
#endif

#endif /* _SRC_CRC32C_IMPL_H_ */
