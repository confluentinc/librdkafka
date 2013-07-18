/*
 * librdkafka - Apache Kafka C library
 *
 * Copyright (c) 2012, Magnus Edenhill
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

#include <sys/socket.h>
#include <netinet/in.h>
#include <fcntl.h>
#include <sys/queue.h>

#ifndef WITH_LIBRD

#include "rd.h"
#include "rdaddr.h"

#else

#include <librd/rd.h>
#include <librd/rdaddr.h>
#endif

#define RD_POLL_INFINITE  -1
#define RD_POLL_NOWAIT     0
#define RD_KAFKA_TOPIC_MAXLEN  256

typedef enum {
	RD_KAFKA_PRODUCER,
	RD_KAFKA_CONSUMER,
} rd_kafka_type_t;

typedef enum {
	RD_KAFKA_STATE_DOWN,
	RD_KAFKA_STATE_CONNECTING,
	RD_KAFKA_STATE_UP,
} rd_kafka_state_t;


typedef enum {
	/* Internal errors to rdkafka: */
	RD_KAFKA_RESP_ERR__BAD_MSG = -199,
	RD_KAFKA_RESP_ERR__BAD_COMPRESSION = -198,
	RD_KAFKA_RESP_ERR__FAIL = -197,  /* See rko_payload for error string */
	/* Standard Kafka errors: */
	RD_KAFKA_RESP_ERR_UNKNOWN = -1,
	RD_KAFKA_RESP_ERR_NO_ERROR = 0,
	RD_KAFKA_RESP_ERR_OFFSET_OUT_OF_RANGE = 1,
	RD_KAFKA_RESP_ERR_INVALID_MSG = 2,
	RD_KAFKA_RESP_ERR_WRONG_PARTITION = 3,
	RD_KAFKA_RESP_ERR_INVALID_FETCH_SIZE = 4,
} rd_kafka_resp_err_t;


/**
 * Optional configuration struct passed to rd_kafka_new*().
 * See head of rdkafka.c for defaults.
 * See comment below for rd_kafka_defaultconf use.
 */
typedef struct rd_kafka_conf_s {
	int max_msg_size;             /* Maximum receive message size.
				       * This is a safety precaution to
				       * avoid memory exhaustion in case of
				       * protocol hickups. */

	int flags;
#define RD_KAFKA_CONF_F_APP_OFFSET_STORE  0x1  /* No automatic offset storage
						* will be performed. The
						* application needs to
						* call rd_kafka_offset_store()
						* explicitly.
						* This may be used to make sure
						* a message is properly handled
						* before storing the offset.
						* If not set, and an offset
						* storage is available, the
						* offset will be stored
						* just prior to passing the
						* message to the application.*/

	struct {
		int poll_interval;    /* Time in milliseconds to sleep before
				       * trying to FETCH again if the broker
				       * did not return any messages for
				       * the last FETCH call.
				       * I.e.: idle poll interval. */

		int replyq_low_thres; /* The low water threshold for the
				       * reply queue.
				       * I.e.: how many messages we'll try
				       * to keep in the reply queue at any
				       * given time. 
				       * The reply queue is the queue of
				       * read messages from the broker
				       * that are still to be passed to
				       * the application. */

		uint32_t max_size;    /* The maximum size to be returned
				       * by FETCH. */

		char *offset_file;    /* File to read/store current
				       * offset from/in.
				       * If the path is a directory then a
				       * filename is generated (including
				       * the topic and partition) and
				       * appended. */
		int offset_file_flags; /* open(2) flags. */
#define RD_KAFKA_OFFSET_FILE_FLAGMASK (O_SYNC|O_ASYNC)
		

		/* For internal use.
		 * Use the rd_kafka_new_consumer() API instead. */
		char *topic;          /* Topic to consume. */
		uint32_t partition;   /* Partition to consume. */
		uint64_t offset;      /* Initial offset. */

	} consumer;


	struct {
		int max_outq_msg_cnt;  /* Maximum number of messages allowed
					* in the output queue.
					* If this number is exceeded the
					* rd_kafka_produce() call will
					* return with -1 and errno
					* set to ENOBUFS. */
	} producer;

} rd_kafka_conf_t;



typedef enum {
	RD_KAFKA_OP_PRODUCE,  /* Application  -> Kafka thread */
	RD_KAFKA_OP_FETCH,    /* Kafka thread -> Application */
	RD_KAFKA_OP_ERR,      /* Kafka thread -> Application */
} rd_kafka_op_type_t;

typedef struct rd_kafka_op_s {
	TAILQ_ENTRY(rd_kafka_op_s) rko_link;
	rd_kafka_op_type_t rko_type;
	char     *rko_topic;
	uint32_t  rko_partition;
	int       rko_flags;
#define RD_KAFKA_OP_F_FREE       0x1  /* Free the payload when done with it. */
#define RD_KAFKA_OP_F_FREE_TOPIC 0x2  /* Free the topic when done with it. */
	/* For PRODUCE and ERR */
	char     *rko_payload;
	int       rko_len;
	/* For FETCH */
	uint64_t  rko_offset;
#define           rko_max_size rko_len
	/* For replies */
	rd_kafka_resp_err_t rko_err;
	int8_t    rko_compression;
	int64_t   rko_offset_len;  /* Length to use to advance the offset. */
} rd_kafka_op_t;


typedef struct rd_kafka_q_s {
	pthread_mutex_t rkq_lock;
	pthread_cond_t  rkq_cond;
	TAILQ_HEAD(, rd_kafka_op_s) rkq_q;
	int             rkq_qlen;
} rd_kafka_q_t;





/**
 * Kafka handle.
 */
typedef struct rd_kafka_s {
	rd_kafka_q_t rk_op;    /* application -> kafka operation queue */
	rd_kafka_q_t rk_rep;   /* kafka -> application reply queue */
	struct {
		char                name[128];
		rd_sockaddr_list_t *rsal;
		int                 curr_addr;
		int                 s;  /* TCP socket */
		struct {
			uint64_t tx_bytes;
			uint64_t tx;    /* Kafka-messages (not payload msgs) */
			uint64_t rx_bytes;
			uint64_t rx;    /* Kafka messages (not payload msgs) */
		} stats;
	} rk_broker;
	rd_kafka_conf_t  rk_conf;
	int              rk_flags;
	int              rk_terminate;
	pthread_t        rk_thread;
	pthread_mutex_t  rk_lock;
	int              rk_refcnt;
	rd_kafka_type_t  rk_type;
	rd_kafka_state_t rk_state;
	struct timeval   rk_tv_state_change;
	union {
		struct {
			char    *topic;
			uint32_t partition;
			uint64_t offset;
			uint64_t app_offset;
			int      offset_file_fd;
		} consumer;
	} rk_u;
#define rk_consumer rk_u.consumer
	struct {
		char msg[512];
		int  err;  /* errno */
	} rk_err;
} rd_kafka_t;


/**
 * Accessor functions.
 *
 * Locality: any thread
 */
#define rd_kafka_name(rk)  ((rk)->rk_broker.name)
#define rd_kafka_state(rk) ((rk)->rk_state)


/**
 * Destroy the Kafka handle.
 * 
 * Locality: application thread
 */
void        rd_kafka_destroy (rd_kafka_t *rk);


/**
 * Creates a new Kafka handle and starts its operation according to the
 * specified 'type'.
 *
 * The 'broker' argument depicts the address to the Kafka broker (sorry,
 * no ZooKeeper support at this point) in the standard "<host>[:<port>]" format
 *
 * If 'broker' is NULL it defaults to "localhost:9092".
 *
 * If the 'broker' node name resolves to multiple addresses (and possibly
 * address families) all will be used for connection attempts in
 * round-robin fashion.
 *
 * 'conf' is an optional struct that will be copied to replace rdkafka's
 * default configuration. See the 'rd_kafka_conf_t' type for more information.
 *
 * NOTE: Make sure SIGPIPE is either ignored or handled by the calling application.
 *
 *
 * Returns the Kafka handle.
 *
 * To destroy the Kafka handle, use rd_kafka_destroy().
 * 
 * Locality: application thread
 */
rd_kafka_t *rd_kafka_new (rd_kafka_type_t type, const char *broker,
			  const rd_kafka_conf_t *conf);

/**
 * Creates a new Kafka consumer handle and sets it up for fetching messages
 * from 'topic' + 'partion', beginning at 'offset'.
 *
 * If 'conf->consumer.offset_file' is non-NULL then the 'offset' parameter is
 * ignored and the file's offset is used instead.
 *
 * Returns the Kafka handle.
 *
 * To destroy the Kafka handle, use rd_kafka_destroy().
 *
 * Locality: application thread
 */
rd_kafka_t *rd_kafka_new_consumer (const char *broker,
				   const char *topic,
				   uint32_t partition,
				   uint64_t offset,
				   const rd_kafka_conf_t *conf);

/**
 * Fetches kafka messages from the internal reply queue that the kafka
 * thread tries to keep populated.
 *
 * Will block until 'timeout_ms' expires (milliseconds, RD_POLL_NOWAIT or
 * RD_POLL_INFINITE) or until a message is returned.
 *
 * The caller must check the reply's rko_err (RD_KAFKA_ERR_*) to distinguish
 * between errors and actual data messages.
 *
 * Communication failure propagation:
 * If rko_err is RD_KAFKA_ERR__FAIL it means a critical error has occured
 * and the connection to the broker has been torn down. The application
 * does not need to take any action but should log the contents of
 * rko->rko_payload.
 *
 * Returns NULL on timeout or an 'rd_kafka_op_t *' reply on success.
 *
 * Locality: application thread
 */
rd_kafka_op_t *rd_kafka_consume (rd_kafka_t *rk, int timeout_ms);

/**
 * Stores the current offset in whatever storage the handle has defined.
 * Must only be called by the application if RD_KAFKA_CONF_F_APP_OFFSET_STORE
 * is set in conf.flags.
 *
 * Locality: any thread
 */
int rd_kafka_offset_store (rd_kafka_t *rk, uint64_t offset);



/**
 * Produce and send a single message to the broker.
 *
 * There are two alternatives for 'payload':
 *   1) static data that will not change or go away during the lifetime
 *      of the rd_kafka_t handle. *This is uncommon*.
 *
 *   2) malloc():ed data that librdkafka will free when done with it,
 *      this requires the RD_KAFKA_OP_F_FREE flag to be set in msgflags.
 *
 * There is currently no way for the application to know when librdkafka is
 * done with the payload, so the control of freeing the payload must be left
 * to librdkafka as described in alternative 2) above.
 *
 *
 * Returns 0 on success or -1 on error (see errno for details)
 *
 * errno:
 *   ENOBUFS - The conf.producer.max_outq_msg_cnt would be exceeded.
 *
 * Locality: application thread
 */
int         rd_kafka_produce (rd_kafka_t *rk, char *topic, uint32_t partition,
			      int msgflags, char *payload, size_t len);

/**
 * Destroys an op as returned by rd_kafka_consume().
 *
 * Locality: any thread
 */
void        rd_kafka_op_destroy (rd_kafka_t *rk, rd_kafka_op_t *rko);


/**
 * Returns a human readable representation of a kafka error.
 */
const char *rd_kafka_err2str (rd_kafka_resp_err_t err);


/**
 * Returns the current out queue length (ops waiting to be sent to the broker).
 *
 * Locality: any thread
 */
static inline int rd_kafka_outq_len (rd_kafka_t *rk) __attribute__((unused));
static inline int rd_kafka_outq_len (rd_kafka_t *rk) {
	return rk->rk_op.rkq_qlen;
}


/**
 * Returns the current reply queue length (messages from the broker waiting
 * for the application thread to consume).
 *
 * Locality: any thread
 */
static inline int rd_kafka_replyq_len (rd_kafka_t *rk) __attribute__((unused));
static inline int rd_kafka_replyq_len (rd_kafka_t *rk) {
	return rk->rk_rep.rkq_qlen;
}




/**
 * The default configuration.
 * When providing your own configuration to the rd_kafka_new_*() calls
 * its advisable to base it on this default configuration and only
 * change the relevant parts.
 * I.e.:
 *
 *   rd_kafka_conf_t myconf = rd_kafka_defaultconf;
 *   myconf.consumer.offset_file = "/var/kafka/offsets/";
 *   rk = rd_kafka_new_consumer(, ... &myconf);
 */ 
extern const rd_kafka_conf_t rd_kafka_defaultconf;


/**
 * Builtin (default) log sink: print to stderr
 */
void rd_kafka_log_print (const rd_kafka_t *rk, int level,
			 const char *fac, const char *buf);


/**
 * Builtin log sink: print to syslog.
 */
void rd_kafka_log_syslog (const rd_kafka_t *rk, int level,
			  const char *fac, const char *buf);


/**
 * Set logger function.
 * The default is to print to stderr, but a syslog is also available,
 * see rd_kafka_log_(print|syslog) for the builtin alternatives.
 * Alternatively the application may provide its own logger callback.
 * Or pass 'func' as NULL to disable logging.
 *
 * NOTE: 'rk' may be passed as NULL.
 */
void rd_kafka_set_logger (void (*func) (const rd_kafka_t *rk, int level,
					const char *fac, const char *buf));



#ifdef NEED_RD_KAFKAPROTO_DEF
/*
 * Kafka protocol definitions.
 * This is kept as an opt-in ifdef-space to avoid name space cluttering
 * for the application while still keeping the implementation to
 * just two files for easy inclusion in applications in case the library
 * variant is not desired.
 */


#define RD_KAFKA_PORT      9092
#define RD_KAFKA_PORT_STR "9092"

/**
 * Generic Request header.
 */
struct rd_kafkap_req {
	uint32_t rkpr_len;
	uint16_t rkpr_type;
#define RD_KAFKAP_PRODUCE       0
#define RD_KAFKAP_FETCH         1
#define RD_KAFKAP_MULTIFETCH    2
#define RD_KAFKAP_MULTIPRODUCE  3
#define RD_KAFKAP_OFFSETS       4
	uint16_t rkpr_topic_len;
	char     rkpr_topic[0]; /* TOPIC and PARTITION follows */
} RD_PACKED;


/**
 * Generic Multi-Request header.
 */
struct rd_kafkap_multireq {
	uint32_t rkpmr_len;
	uint16_t rkpmr_type;
	uint16_t rkpmr_topicpart_cnt;

	uint32_t rkpr_topic_len;
	char     rkpr_topic[0]; /* TOPIC and PARTITION follows */
} RD_PACKED;


/**
 * Generic Response header.
 */
struct rd_kafkap_resp {
	uint32_t rkprp_len;
	int16_t  rkprp_error;  /* rd_kafka_resp_err_t */
} RD_PACKED;



/**
 * MESSAGE header
 */
struct rd_kafkap_msg {
	uint32_t rkpm_len;
	uint8_t  rkpm_magic;
#define RD_KAFKAP_MSG_MAGIC_NO_COMPRESSION_ATTR   0  /* Not supported. */
#define RD_KAFKAP_MSG_MAGIC_COMPRESSION_ATTR      1 
	uint8_t  rkpm_compression;
#define RD_KAFKAP_MSG_COMPRESSION_NONE            0
#define RD_KAFKAP_MSG_COMPRESSION_GZIP            1
#define RD_KAFKAP_MSG_COMPRESSION_SNAPPY          2
	uint32_t rkpm_cksum;
	char     rkpm_payload[0];
} RD_PACKED;

/**
 * PRODUCE header, directly follows the request header.
 */
struct rd_kafkap_produce {
	uint32_t             rkpp_msgs_len;
	struct rd_kafkap_msg rkpp_msgs[0];
} RD_PACKED;


/**
 * FETCH request header, directly follows the request header.
 */
struct rd_kafkap_fetch_req {
	uint64_t rkpfr_offset;
	uint32_t rkpfr_max_size;
} RD_PACKED;

/**
 * FETCH response header, directly follows the response header.
 */
struct rd_kafkap_fetch_resp {
	struct rd_kafkap_msg rkpfrp_msgs[0];
} RD_PACKED;




/**
 * Helper struct containing a protocol-encoded topic+partition.
 */
struct rd_kafkap_topicpart {
	int  rkptp_len;
	char rkptp_buf[0];
};


#endif /* NEED_KAFKAPROTO_DEF */

