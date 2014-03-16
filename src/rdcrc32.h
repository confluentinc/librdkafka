/**
 * \file rdcrc32.h
 * Functions and types for CRC checks.
 *
 * Generated on Tue May  8 17:36:59 2012,
 * by pycrc v0.7.10, http://www.tty1.net/pycrc/
 *
 * NOTE: Contains librd modifications:
 *       - rd_crc32() helper.
 *       - __RDCRC32___H__ define (was missing the '32' part).
 *
 * using the configuration:
 *    Width        = 32
 *    Poly         = 0x04c11db7
 *    XorIn        = 0xffffffff
 *    ReflectIn    = True
 *    XorOut       = 0xffffffff
 *    ReflectOut   = True
 *    Algorithm    = table-driven
 *****************************************************************************/
#ifndef __RDCRC32___H__
#define __RDCRC32___H__

#include <stdlib.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif


/**
 * The definition of the used algorithm.
 *****************************************************************************/
#define CRC_ALGO_TABLE_DRIVEN 1


/**
 * The type of the CRC values.
 *
 * This type must be big enough to contain at least 32 bits.
 *****************************************************************************/
typedef uint32_t rd_crc32_t;


/**
 * Reflect all bits of a \a data word of \a data_len bytes.
 *
 * \param data         The data word to be reflected.
 * \param data_len     The width of \a data expressed in number of bits.
 * \return             The reflected data.
 *****************************************************************************/
rd_crc32_t rd_crc32_reflect(rd_crc32_t data, size_t data_len);


/**
 * Calculate the initial crc value.
 *
 * \return     The initial crc value.
 *****************************************************************************/
static inline rd_crc32_t rd_crc32_init(void)
{
    return 0xffffffff;
}


/**
 * Update the crc value with new data.
 *
 * \param crc      The current crc value.
 * \param data     Pointer to a buffer of \a data_len bytes.
 * \param data_len Number of bytes in the \a data buffer.
 * \return         The updated crc value.
 *****************************************************************************/
rd_crc32_t rd_crc32_update(rd_crc32_t crc, const unsigned char *data, size_t data_len);


/**
 * Calculate the final crc value.
 *
 * \param crc  The current crc value.
 * \return     The final crc value.
 *****************************************************************************/
static inline rd_crc32_t rd_crc32_finalize(rd_crc32_t crc)
{
    return crc ^ 0xffffffff;
}


/**
 * Wrapper for performing CRC32 on the provided buffer.
 */
static inline rd_crc32_t rd_crc32 (const char *data, size_t data_len) {
	return rd_crc32_finalize(rd_crc32_update(rd_crc32_init(),
						 (const unsigned char *)data,
						 data_len));
}

#ifdef __cplusplus
}           /* closing brace for extern "C" */
#endif

#endif      /* __RDCRC32___H__ */
