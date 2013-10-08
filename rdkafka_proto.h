/*
 * librdkafka - Apache Kafka C library
 *
 * Copyright (c) 2012,2013 Magnus Edenhill
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


/*
 * Kafka protocol definitions.
 */

#define RD_KAFKA_PORT      9092
#define RD_KAFKA_PORT_STR "9092"


/**
 * Request header
 */
struct rd_kafkap_reqhdr {
	int32_t  Size;
	int16_t  ApiKey;
#define RD_KAFKAP_Produce       0
#define RD_KAFKAP_Fetch         1
#define RD_KAFKAP_Offset        2
#define RD_KAFKAP_Metadata      3
#define RD_KAFKAP_LeaderAndIsr  4
#define RD_KAFKAP_StopReplica   5
#define RD_KAFKAP_OffsetCommit  6
#define RD_KAFKAP_OffsetFetch   7
	int16_t  ApiVersion;
	int32_t  CorrId;
	/* ClientId follows */
} RD_PACKED;

/**
 * Response header
 */
struct rd_kafkap_reshdr {
	int32_t  Size;
	int32_t  CorrId;
} RD_PACKED;





/**
 *
 * Kafka protocol string representation: { uint16, data.. }
 *
 */
typedef struct rd_kafkap_str_s {
	int16_t len;    /* big endian */
	char    str[0]; /* allocated dynamically */
} RD_PACKED rd_kafkap_str_t;

#define RD_KAFKAP_STR_LEN_NULL -1
/* Returns the actual size of a kafka protocol string representation. */
#define RD_KAFKAP_STR_SIZE(kstr) (int16_t)(sizeof((kstr)->len) +	\
					   (ntohs((kstr)->len) ==	\
					    RD_KAFKAP_STR_LEN_NULL ?	\
					    0 : ntohs((kstr)->len)))
/* Returns the length of the string of a kafka protocol string representation */
#define RD_KAFKAP_STR_LEN(kstr) (int)((ntohs((kstr)->len) ==		\
				       RD_KAFKAP_STR_LEN_NULL ?		\
				       0 : ntohs((kstr)->len)))


/* Macro suitable for "%.*s" printing. */
#define RD_KAFKAP_STR_PR(kstr)  \
	(ntohs((kstr)->len) == RD_KAFKAP_STR_LEN_NULL ?	\
	 0 : (int)ntohs((kstr)->len)), (kstr)->str

#define RD_KAFKAP_STR_IS_NULL(kstr) \
	(ntohs((kstr)->len) == RD_KAFKAP_STR_LEN_NULL)

static inline int rd_kafkap_str_cmp (const rd_kafkap_str_t *a,
				     const rd_kafkap_str_t *b) RD_UNUSED;
static inline int rd_kafkap_str_cmp (const rd_kafkap_str_t *a,
				     const rd_kafkap_str_t *b) {
	if (a->len != b->len)
		return -1;
	return memcmp(a->str, b->str, ntohs(a->len));
}

static inline int rd_kafkap_str_cmp_str (const rd_kafkap_str_t *a,
					 const char *str) RD_UNUSED;
static inline int rd_kafkap_str_cmp_str (const rd_kafkap_str_t *a,
					 const char *str) {
	int len = strlen(str);
	if (ntohs(a->len) != len)
		return -1;
	return memcmp(a->str, str, ntohs(a->len));
}


static inline rd_kafkap_str_t *rd_kafkap_str_new (const char *str) RD_UNUSED;
static inline rd_kafkap_str_t *rd_kafkap_str_new (const char *str) {
	rd_kafkap_str_t *kstr;
	int len = 0;

	if (str)
		len = strlen(str);
	else
		len = 0;

	/* We allocate one more byte so we can null-terminate the string.
	 * This null-termination is not included in the length so it
	 * is not sent over the wire. */
	kstr = malloc(sizeof(*kstr) + len + 1);

	if (str) {
		kstr->len = ntohs(len);
		memcpy(kstr->str, str, len+1);
	} else
		kstr->len = ntohs(RD_KAFKAP_STR_LEN_NULL);

	return kstr;
}

#define rd_kafkap_str_destroy(kstr) free(kstr)

#define rd_kafkap_strdupa(kstr)  strndupa((kstr)->str,\
					  RD_KAFKAP_STR_SIZE((kstr)))




/**
 *
 * Kafka protocol bytes representation: { uint32, data.. }
 *
 */
typedef struct rd_kafkap_bytes_s {
	int32_t len;     /* big endian */
	char    data[0]; /* allocated dynamically */
} RD_PACKED rd_kafkap_bytes_t;

#define RD_KAFKAP_BYTES_LEN_NULL -1
/* Returns the actual size of a kafka protocol bytes representation. */
#define RD_KAFKAP_BYTES_SIZE(kbytes) (int32_t)(sizeof((kbytes)->len) +	\
					       (ntohl((kbytes)->len) ==	\
						RD_KAFKAP_BYTES_LEN_NULL ? \
						0 : ntohl((kbytes)->len)))
/* Returns the length of the string of a kafka protocol bytes representation */
#define RD_KAFKAP_BYTES_LEN(kbytes) (int32_t)((ntohl((kbytes)->len) ==	\
					       RD_KAFKAP_BYTES_LEN_NULL ? \
					       0 : ntohl((kbytes)->len)))

#define RD_KAFKAP_BYTES_IS_NULL(kbytes) \
	(ntohs((kbytes)->len) == RD_KAFKAP_STR_LEN_NULL)


static inline int rd_kafkap_bytes_cmp (const rd_kafkap_bytes_t *a,
				       const rd_kafkap_bytes_t *b) RD_UNUSED;
static inline int rd_kafkap_bytes_cmp (const rd_kafkap_bytes_t *a,
				       const rd_kafkap_bytes_t *b) {
	if (a->len != b->len)
		return -1;
	return memcmp(a->data, b->data, ntohl(a->len));
}

static inline int rd_kafkap_bytes_cmp_bytes (const rd_kafkap_bytes_t *a,
					     const void *data, size_t datalen)
	RD_UNUSED;
static inline int rd_kafkap_bytes_cmp_bytes (const rd_kafkap_bytes_t *a,
					     const void *data, size_t datalen) {
	if (a->len != datalen)
		return -1;
	return memcmp(a->data, data, ntohl(a->len));
}


static inline rd_kafkap_bytes_t *rd_kafkap_bytes_new (const void *data,
						      size_t datalen) RD_UNUSED;
static inline rd_kafkap_bytes_t *rd_kafkap_bytes_new (const void *data,
						      size_t datalen) {
	rd_kafkap_bytes_t *kbytes;

	kbytes = malloc(sizeof(*kbytes) + datalen);

	if (data) {
		kbytes->len = ntohl(datalen);
		memcpy(kbytes->data, data, datalen);
	} else
		kbytes->len = ntohl(RD_KAFKAP_BYTES_LEN_NULL);
	
	return kbytes;
}

#define rd_kafkap_bytes_destroy(kbytes) free(kbytes)



struct rd_kafkap_FetchRequest {
	int32_t ReplicaId;
	int32_t MaxWaitTime;
	int32_t MinBytes;
	int32_t TopicArrayCnt;
} RD_PACKED;
