/*
 * librdkafka - Apache Kafka C library
 *
 * Copyright (c) 2012-2015, Magnus Edenhill
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
#pragma once

#include "rdkafka_int.h"

typedef struct rd_kafka_broker_s rd_kafka_broker_t;

#define RD_KAFKA_HEADERS_IOV_CNT   2
#define RD_KAFKA_PAYLOAD_IOV_MAX  (IOV_MAX-RD_KAFKA_HEADERS_IOV_CNT)


/* Advance/allocate used space in marshall buffer.
 * Point PTR to available space of size LEN on success. */
#define _MSH_ALLOC(PTR,LEN)  do {                                      \
                int __LEN = (LEN);                                      \
                if (msh_of + __LEN >= msh_size)                         \
                        _FAIL("Not enough room in marshall buffer: "    \
                              "%i+%i > %i",                             \
                              msh_of, __LEN, msh_size);                 \
                (PTR) = (void *)(msh_buf+msh_of);                       \
                msh_of += __LEN;                                        \
        } while(0)



/**
 * Memory reading helper macros to be used when parsing network responses.
 *
 * Assumptions:
 *   - buffer to parse is in 'rkbuf'
 *   - current read offset is in 'size_t of' which must be initialized to 0.
 *   - the broker the message was received from must be 'rkb'
 *   - an 'err:' label must be available for error bailouts.
 */

#define _FAIL(...) do {						\
                if (log_decode_errors) {                                \
                        rd_rkb_log(rkb, LOG_WARNING, "PROTOERR",        \
                                   "Protocol parse failure at %s:%i",   \
                                   __FUNCTION__, __LINE__);             \
                        rd_rkb_log(rkb, LOG_WARNING, "PROTOERR", __VA_ARGS__);  \
                }                                                       \
                goto err;                                               \
	} while (0)

#define _REMAIN() (int)(rkbuf->rkbuf_len - of)

#define _CHECK_LEN(len) do {						\
		int _LEN = (int)(len);					\
	if (unlikely(_LEN > _REMAIN())) {				\
		_FAIL("expected %i bytes > %i remaining bytes",		\
		      _LEN, (int)_REMAIN());				\
		goto err;						\
	}								\
	} while (0)

#define _SKIP(len) do {				\
		_CHECK_LEN(len);		\
		of += (len);			\
	} while (0)


#define _READ(dstptr,len) do {			\
		_CHECK_LEN(len);		\
		memcpy((dstptr), rkbuf->rkbuf_buf2+(of), (len));	\
		of += (len);				\
	} while (0)

#define _READ_I64(dstptr) do {						\
		_READ(dstptr, 8);					\
		*(int64_t *)(dstptr) = be64toh(*(int64_t *)(dstptr));	\
	} while (0)

#define _READ_I32(dstptr) do {						\
		_READ(dstptr, 4);					\
		*(int32_t *)(dstptr) = be32toh(*(int32_t *)(dstptr));	\
	} while (0)

/* Same as _READ_I32 but does a direct assignment.
 * dst is assumed to be a scalar, not pointer. */
#define _READ_I32A(dst) do {                                            \
                int32_t _v;                                             \
		_READ(&_v, 4);                                          \
		dst = (int32_t) be32toh(_v);                 \
	} while (0)

#define _READ_I16(dstptr) do {						\
		_READ(dstptr, 2);					\
		*(int16_t *)(dstptr) = be16toh(*(int16_t *)(dstptr));	\
	} while (0)

#define _READ_I16A(dst) do {                                            \
                int16_t _v;                                             \
		_READ(&_v, 2);                                          \
                dst = (int16_t)be16toh(_v);                 \
	} while (0)


/* Read Kafka String representation (2+N) */
#define _READ_STR(kstr) do {					\
		int _klen;					\
		_CHECK_LEN(2);					\
		kstr = (rd_kafkap_str_t *)((char *)rkbuf->rkbuf_buf2+of);	\
		_klen = RD_KAFKAP_STR_SIZE(kstr);		\
                _CHECK_LEN(_klen);                              \
		of += _klen;					\
	} while (0)

/* Read Kafka String representation (2+N) into nul-terminated C string.
 * Depends on a marshalling environment. */
#define _READ_STR_MSH(dst) do {                                 \
                rd_kafkap_str_t *_kstr;                         \
		int _klen;					\
		_CHECK_LEN(2);					\
		_kstr = (rd_kafkap_str_t *)((char *)rkbuf->rkbuf_buf2+of);	\
		_klen = RD_KAFKAP_STR_SIZE(_kstr);		\
                _CHECK_LEN(_klen);                              \
		of += _klen;					\
                _MSH_ALLOC(dst, _klen+1);                       \
                memcpy(dst, _kstr->str, _klen);                  \
                dst[_klen] = '\0';                              \
	} while (0)

/* Read Kafka Bytes representation (4+N) */
#define _READ_BYTES(kbytes) do {				\
		int32_t _klen;					\
		_CHECK_LEN(4);					\
		kbytes = (rd_kafkap_bytes_t *)((char *)rkbuf->rkbuf_buf2+of);	\
		_klen = RD_KAFKAP_BYTES_SIZE(kbytes);		\
                _CHECK_LEN(_klen);                              \
		of += (_klen);					\
	} while (0)

/* Reference memory, dont copy it */
#define _READ_REF(dstptr,len) do {			\
		_CHECK_LEN(len);			\
		(dstptr) = (void *)((char *)rkbuf->rkbuf_buf2+of);	\
		of += (len);				\
	} while(0)



typedef struct rd_kafka_buf_s {
	TAILQ_ENTRY(rd_kafka_buf_s) rkbuf_link;

	int32_t rkbuf_corrid;

	rd_ts_t rkbuf_ts_retry;    /* Absolute send retry time */

	int     rkbuf_flags; /* RD_KAFKA_OP_F */
	struct msghdr rkbuf_msg;
	struct iovec *rkbuf_iov;
	int           rkbuf_iovcnt;
	size_t  rkbuf_of;          /* recv/send: byte offset */
	size_t  rkbuf_len;         /* send: total length */
	size_t  rkbuf_size;        /* allocated size */

	char   *rkbuf_buf;         /* Main buffer */
	char   *rkbuf_buf2;        /* Aux buffer */

        char   *rkbuf_wbuf;        /* Write buffer pointer (into rkbuf_buf) */
        size_t  rkbuf_wof;         /* Write buffer offset */

	struct rd_kafkap_reqhdr rkbuf_reqhdr;
	struct rd_kafkap_reshdr rkbuf_reshdr;

	int32_t rkbuf_expected_size;  /* expected size of message */

	/* Response callback */
	void  (*rkbuf_cb) (struct rd_kafka_broker_s *,
			   rd_kafka_resp_err_t err,
			   struct rd_kafka_buf_s *reprkbuf,
			   struct rd_kafka_buf_s *reqrkbuf,
			   void *opaque);

        void  (*rkbuf_parse_cb) (struct rd_kafka_buf_s *respbuf, void *opaque);

        rd_kafka_resp_err_t       rkbuf_err;
        struct rd_kafka_broker_s *rkbuf_rkb;
        /* Handler callback: called after response has been parsed.
         * The arguments are not predefined but varies depending on
         * response type. */
        void  (*rkbuf_hndcb) (void *);
        void   *rkbuf_hndopaque;

	rd_atomic32_t rkbuf_refcnt;
	void   *rkbuf_opaque;

        int32_t rkbuf_op_version;    /* Originating queue version,
                                      * NOT THE PROTOCOL VERSION! */
	int     rkbuf_retries;

	rd_ts_t rkbuf_ts_enq;
	rd_ts_t rkbuf_ts_sent;    /* Initially: Absolute time of transmission,
				   * after response: RTT. */
	rd_ts_t rkbuf_ts_timeout;

        int64_t rkbuf_offset;  /* Used by OffsetCommit */

	rd_kafka_msgq_t rkbuf_msgq;
} rd_kafka_buf_t;


typedef struct rd_kafka_bufq_s {
	TAILQ_HEAD(, rd_kafka_buf_s) rkbq_bufs;
	rd_atomic32_t  rkbq_cnt;
	rd_atomic32_t  rkbq_msg_cnt;
} rd_kafka_bufq_t;


#define rd_kafka_buf_keep(rkbuf) (void)rd_atomic32_add(&(rkbuf)->rkbuf_refcnt, 1)
void rd_kafka_buf_destroy (rd_kafka_buf_t *rkbuf);


/**
 * Set request API type version
 */
static __inline void rd_kafka_buf_version_set (rd_kafka_buf_t *rkbuf,
                                               int16_t version) {
        rkbuf->rkbuf_reqhdr.ApiVersion = htobe16(version);
}

void rd_kafka_buf_grow (rd_kafka_buf_t *rkbuf, size_t needed_len);

/**
 * Write (copy) data to buffer at current write-buffer position.
 * There must be enough space allocated in the rkbuf.
 * Returns offset to written destination buffer.
 */
static __inline int rd_kafka_buf_write (rd_kafka_buf_t *rkbuf,
                                        const void *data, size_t len) {
        int remain = rkbuf->rkbuf_size - (rkbuf->rkbuf_wof + len);

        /* Make sure there's enough room, else increase buffer. */
        if (remain < 0)
                rd_kafka_buf_grow(rkbuf, rkbuf->rkbuf_wof + len);

        rd_kafka_assert(NULL, rkbuf->rkbuf_wof + len <= rkbuf->rkbuf_size);
        memcpy(rkbuf->rkbuf_wbuf + rkbuf->rkbuf_wof, data, len);
        rkbuf->rkbuf_wof += len;


        return rkbuf->rkbuf_wof - len;
}


/**
 * Write (copy) 'data' to buffer at 'ptr'.
 * There must be enough space to fit 'len'.
 * This will overwrite the buffer at given location and length.
 */
static __inline void rd_kafka_buf_update (rd_kafka_buf_t *rkbuf, int of,
                                          const void *data, size_t len) {
        int remain = rkbuf->rkbuf_size - (of + len);
        rd_kafka_assert(NULL, remain >= 0);
        rd_kafka_assert(NULL, of >= 0 && of < rkbuf->rkbuf_size);


        memcpy(rkbuf->rkbuf_wbuf+of, data, len);
}

/**
 * Write int32_t to buffer.
 * The value will be endian-swapped before write.
 */
static __inline int rd_kafka_buf_write_i32 (rd_kafka_buf_t *rkbuf,
                                                 int32_t v) {
        v = htobe32(v);
        return rd_kafka_buf_write(rkbuf, &v, sizeof(v));
}

/**
 * Update int32_t in buffer at offset 'of'.
 * 'of' should have been previously returned by `.._buf_write_i32()`.
 */
static __inline void rd_kafka_buf_update_i32 (rd_kafka_buf_t *rkbuf,
                                              int of, int32_t v) {
        v = htobe32(v);
        rd_kafka_buf_update(rkbuf, of, &v, sizeof(v));
}


/**
 * Write int64_t to buffer.
 * The value will be endian-swapped before write.
 */
static __inline int rd_kafka_buf_write_i64 (rd_kafka_buf_t *rkbuf, int64_t v) {
        v = htobe64(v);
        return rd_kafka_buf_write(rkbuf, &v, sizeof(v));
}

/**
 * Update int64_t in buffer at address 'ptr'.
 * 'of' should have been previously returned by `.._buf_write_i64()`.
 */
static __inline void rd_kafka_buf_update_i64 (rd_kafka_buf_t *rkbuf,
                                              int of, int64_t v) {
        v = htobe64(v);
        rd_kafka_buf_update(rkbuf, of, &v, sizeof(v));
}


/**
 * Write (copy) Kafka string to buffer.
 */
static __inline int rd_kafka_buf_write_kstr (rd_kafka_buf_t *rkbuf,
                                             const rd_kafkap_str_t *kstr) {
        return rd_kafka_buf_write(rkbuf, kstr, RD_KAFKAP_STR_SIZE(kstr));
}


void rd_kafka_buf_destroy (rd_kafka_buf_t *rkbuf);
void rd_kafka_buf_auxbuf_add (rd_kafka_buf_t *rkbuf, void *auxbuf);
void rd_kafka_buf_rewind (rd_kafka_buf_t *rkbuf, int iovindex);
struct iovec *rd_kafka_buf_iov_next (rd_kafka_buf_t *rkbuf);
void rd_kafka_buf_push (rd_kafka_buf_t *rkbuf, void *buf, size_t len);
void rd_kafka_buf_autopush (rd_kafka_buf_t *rkbuf);
rd_kafka_buf_t *rd_kafka_buf_new_growable (int iovcnt, size_t init_size);
rd_kafka_buf_t *rd_kafka_buf_new (int iovcnt, size_t size);
rd_kafka_buf_t *rd_kafka_buf_new_shadow (void *ptr, size_t size);
void rd_kafka_bufq_enq (rd_kafka_bufq_t *rkbufq, rd_kafka_buf_t *rkbuf);
void rd_kafka_bufq_deq (rd_kafka_bufq_t *rkbufq, rd_kafka_buf_t *rkbuf);
void rd_kafka_bufq_init(rd_kafka_bufq_t *rkbufq);
void rd_kafka_bufq_concat (rd_kafka_bufq_t *dst, rd_kafka_bufq_t *src);
void rd_kafka_bufq_purge (rd_kafka_broker_t *rkb,
                          rd_kafka_bufq_t *rkbufq,
                          rd_kafka_resp_err_t err);
