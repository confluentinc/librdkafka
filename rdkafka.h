/*
 * librdkafka - Apache Kafka C library
 *
 * Copyright (c) 2012-2013 Magnus Edenhill
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

/**
 * Apache Kafka consumer & producer
 */

#pragma once

#include <stdio.h>
#include <inttypes.h>


/**
 * librdkafka version
 *
 * interpreted as MM.mm.rr.xx:
 *   MM = Major
 *   mm = minor
 *   rr = revision
 *   xx = currently unused
 */
#define RD_KAFKA_VERSION  0x00080000

#define RD_KAFKA_TOPIC_MAXLEN  256

typedef enum {
	RD_KAFKA_PRODUCER,
	RD_KAFKA_CONSUMER,
} rd_kafka_type_t;



typedef struct rd_kafka_s rd_kafka_t;
typedef struct rd_kafka_topic_s rd_kafka_topic_t;

/**
 * Kafka protocol error codes (version 0.8)
 */
typedef enum {
	/* Internal errors to rdkafka: */
	RD_KAFKA_RESP_ERR__BEGIN = -200,     /* begin internal error codes */
	RD_KAFKA_RESP_ERR__BAD_MSG = -199,   /* Received message is incorrect */
	RD_KAFKA_RESP_ERR__BAD_COMPRESSION = -198, /* Bad/unknown compression */
	RD_KAFKA_RESP_ERR__DESTROY = -197,   /* Broker is going away */
	RD_KAFKA_RESP_ERR__FAIL = -196,      /* Generic failure */
	RD_KAFKA_RESP_ERR__TRANSPORT = -195, /* Broker transport error */
	RD_KAFKA_RESP_ERR__END = -100,       /* end internal error codes */

	/* Standard Kafka errors: */
	RD_KAFKA_RESP_ERR_UNKNOWN = -1,
	RD_KAFKA_RESP_ERR_NO_ERROR = 0,
	RD_KAFKA_RESP_ERR_OFFSET_OUT_OF_RANGE = 1,
	RD_KAFKA_RESP_ERR_INVALID_MSG = 2,
	RD_KAFKA_RESP_ERR_UNKNOWN_TOPIC_OR_PART = 3,
	RD_KAFKA_RESP_ERR_INVALID_MSG_SIZE = 4,
	RD_KAFKA_RESP_ERR_LEADER_NOT_AVAILABLE = 5,
	RD_KAFKA_RESP_ERR_NOT_LEADER_FOR_PARTITION = 6,
	RD_KAFKA_RESP_ERR_REQUEST_TIMED_OUT = 7,
	RD_KAFKA_RESP_ERR_BROKER_NOT_AVAILABLE = 8,
	RD_KAFKA_RESP_ERR_REPLICA_NOT_AVAILABLE = 9,
	RD_KAFKA_RESP_ERR_MSG_SIZE_TOO_LARGE = 10,
	RD_KAFKA_RESP_ERR_STALE_CTRL_EPOCH = 11,
	RD_KAFKA_RESP_ERR_OFFSET_METADATA_TOO_LARGE = 12,
} rd_kafka_resp_err_t;

/**
 * Returns a human readable representation of a kafka error.
 */
const char *rd_kafka_err2str (rd_kafka_t *rk, rd_kafka_resp_err_t err);



/**
 * MessageSet compression codecs
 */
typedef enum {
	RD_KAFKA_COMPRESSION_NONE,
	RD_KAFKA_COMPRESSION_GZIP,   /* FIXME: not supported */
	RD_KAFKA_COMPRESSION_SNAPPY, /* FIXME: not supported */
} rd_kafka_compression_t;


/**
 * Optional configuration struct passed to rd_kafka_new*().
 *
 * The struct can also be populated through string properties
 * by calling rd_kafka_conf_set().
 *
 * The properties are identical to the Apache Kafka configuration properties
 * whenever possible.
 *
 * See rdkafka_defaultconf.c for defaults.
 * See rd_kafka_defaultconf_set() for usage.
 * See rd_kafka_conf_set().
 */
typedef struct rd_kafka_conf_s {
	/* Property: client.id
	 *
	 * Client identifier */
	char   *clientid;

	/* Property: metadata.broker.list
	 *
	 * Initial list of brokers.
	 * The application may also use rd_kafka_brokers_add() */
	char   *brokerlist;

         /* Property: message.max.bytes
	  *
	  * Maximum receive message size.
	  * This is a safety precaution to avoid memory exhaustion in case of
	  * protocol hickups. */
	int     max_msg_size;  

	/* Property: metadata.request.timeout.ms
	 *
	 * Non-topic request timeout in milliseconds.
	 * This is for metadata requests, etc. */
	int     metadata_request_timeout_ms;

	/* Boolean flags: RD_KAFKA_CONF_F_... */
	int     flags;

	/* No automatic offset storage will be performed.
	 * The application needs to call rd_kafka_offset_store() explicitly.
	 * This may be used to make sure a message is properly handled
	 * before storing the offset.
	 * If not set, and an offset storage is available, the
	 * offset will be stored just prior to passing the
	 * message to the application.*/
#define RD_KAFKA_CONF_F_APP_OFFSET_STORE  0x1  


	/* How long to cache the broker address resolving results. */
	int     broker_addr_lifetime; 

	/* Error callback */
	void  (*error_cb) (rd_kafka_t *rk, int err,
			   const char *reason, void *opaque);

	/* Opaque passed to all registered callbacks. */
	void   *opaque;

	/* Consumer configuration */
	/* FIXME: 0.8 consumer not yet implemented */
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


	/* Producer configuration */
	struct {
		/* Property: queue.buffering.max.messages
		 *
		 * Maximum number of messages allowed on the producer queue. */
		int max_messages;

		/* Property: queue.buffering.max.ms
		 *
		 * Maximum time, in milliseconds, for buffering data
		 * on the producer queue. */
		int buffering_max_ms;

		/* Property: topic.metadata.refresh.interval.ms
		 *
		 * Topic metadata refresh interval in milliseconds.
		 * The metadata is automatically refreshed on
		 * error and connect.
		 * FIXME: Not implemented. */
		/* FIXME: This should be controlled by the 'rk'
		 *        and only assigned to one broker at the time. */
		int metadata_refresh_interval_ms;

		/* Property: message.send.max.retries
		 *
		 * How many times to retry sending a failing MessageSet.
		 * Note: retrying can cause reordering. */
		int max_retries;

		/* Property: retry.backoff.ms
		 *
		 * The backoff time in milliseconds before retrying. */
		int retry_backoff_ms;

		/* Property: compression.codec
		 *
		 * Specify compression codec for all data generated by
		 * this produced. */
		/* FIXME: Not yet implemented */
		rd_kafka_compression_t compression_codec;

		/* Property: queue.enqueue.timeout.ms
		 *
		 * Message enqueue timeout:
		 *  0   Messages will be enqueued immediately or dropped 
		 *      if the queue is full.
		 * <0   Enqueue will block indefinately if the queue is full.
		 * >0   Enqueue will block up to this many milliseconds if
		 *      the queue is full. */
		/* FIXME: Not implemented */
		int enqueue_timeout_ms;

		/* Property: batch.num.messages
		 *
		 * Maximum number of messages batched in one MessageSet. */
		int batch_num_messages;

		/* Message delivery report callback.
		 * Called once for each produced message, either on
		 * succesful and acknowledged delivery to the broker in which
		 * case 'err' is 0, or if the message could not be delivered
		 * in which case 'err' is non-zero (use rd_kafka_err2str()
		 * to obtain a human-readable error reason).
		 *
		 * If the message was produced with neither RD_KAFKA_MSG_F_FREE
		 * or RD_KAFKA_MSG_F_COPY set then 'payload' is the original
		 * pointer provided to rd_kafka_produce().
		 * rdkafka will not perform any further actions on 'payload'
		 * at this point and the application may free the payload data
		 * at this point.
		 *
		 * 'opaque' is 'conf.opaque', while 'msg_opaque' is
		 * the opaque pointer provided in the rd_kafka_produce() call.
		 */
		void (*dr_cb) (rd_kafka_t *rk,
			       void *payload, size_t len,
			       rd_kafka_resp_err_t err,
			       void *opaque, void *msg_opaque);

	} producer;

} rd_kafka_conf_t;



typedef struct rd_kafka_topic_conf_s {
	/* Property: request.required.acks
	 *
	 * This field indicates how many acknowledgements the brokers
	 * should receive before responding to the request:
	 *  0   The broker does not send any response.
	 *  1   The broker will wait until the data is written to the 
	 *      local log before sending a response.
	 * -1   The broker will block until the message is committed by all
	 *      in sync replicas before sending a response.
	 * >1   For any number > 1 the broker will block waiting for this
	 *      number of acknowledgements to occur
	 *      (but the server will never wait for more acknowledgements
	 *       than there are in-sync replicas). */
	int16_t required_acks;

	/* Property: request.timeout.ms
	 *
	 * The ack timeout of the producer request in milliseconds.
	 * This value is only enforced by the broker and relies
	 * on required_acks being > 0. */
	int32_t request_timeout_ms;

	/* Property: message.timeout.ms
	 *
	 * Local message timeout.
	 * This value is only enforced locally and limits the time a
	 * produced message waits for succesful delivery. */
	int     message_timeout_ms;

	/* Application provided message partitioner.
	 * The partitioner may be called in any thread at any time,
	 * it may be called multiple times for the same key.
	 * Partitioner function constraints:
	 *    - MUST NOT call any rd_kafka_*() functions
	 *    - MUST NOT block or execute for prolonged periods of time.
	 *    - MUST return a value between 0 and partition_cnt-1, or the
	 *      special RD_KAFKA_PARTITION_UA value if partitioning
	 *      could not be performed.
	 */

	int32_t (*partitioner) (const void *keydata,
				size_t keylen,
				int32_t partition_cnt,
				void *rkt_opaque,
				void *msg_opaque);

	/* Application provided opaque pointer (this is rkt_opaque) */
	void   *opaque;
} rd_kafka_topic_conf_t;

/*******************************************************************
 * Partitioners provided by rdkafka                                *
 *******************************************************************/

/**
 * Random partitioner.
 * This is the default partitioner.
 *
 * Returns a random partition between 0 and 'partition_cnt'-1.
 *
 */
int32_t rd_kafka_msg_partitioner_random (const void *key,
					 size_t keylen,
					 int32_t partition_cnt,
					 void *opaque, void *msg_opaque);



/**
 * The default configuration.
 * When providing your own configuration to the rd_kafka_*_new_*() calls
 * its advisable to base it on these default configurations and only
 * change the relevant parts.
 * I.e.:
 *
 *   rd_kafka_conf_t myconf;
 *   rd_kafka_defaultconf_set(&myconf);
 *   myconf.consumer.offset_file = "/var/kafka/offsets/";
 *   rk = rd_kafka_new_consumer(, ... &myconf);
 *
 * Please see rdkafka_defaultconf.c for the default settings.
 */
void rd_kafka_defaultconf_set (rd_kafka_conf_t *conf);

typedef enum {
	RD_KAFKA_CONF_UNKNOWN = -2, /* Unknown configuration name. */
	RD_KAFKA_CONF_INVALID = -1, /* Invalid configuration value. */
	RD_KAFKA_CONF_OK = 0,  /* Configuration okay */
} rd_kafka_conf_res_t;


/**
 * Sets a single rd_kafka_conf_t value by canonical name.
 * 'conf' should have been previously set up with rd_kafka_defaultconf_set().
 *
 * Returns rd_kafka_conf_res_t to indicate success or failure.
 */
rd_kafka_conf_res_t rd_kafka_conf_set (rd_kafka_conf_t *conf,
				       const char *name,
				       const char *value,
				       char *errstr, size_t errstr_size);



/**
 * Topic default configuration
 *
 * Same semantics as for rd_kafka_defaultconf_set().
 */
void rd_kafka_topic_defaultconf_set (rd_kafka_topic_conf_t *topic_conf);

/**
 * Sets a single rd_kafka_topic_conf_t value by canonical name.
 * 'topic_conf' should have been previously set up
 *  with rd_kafka_topic_defaultconf_set().
 *
 * Returns rd_kafka_conf_res_t to indicate success or failure.
 */
rd_kafka_conf_res_t rd_kafka_topic_conf_set (rd_kafka_topic_conf_t *conf,
					     const char *name,
					     const char *value,
					     char *errstr, size_t errstr_size);












/**
 * Creates a new Kafka handle and starts its operation according to the
 * specified 'type'.
 *
 * 'conf' is an optional struct that will be copied to replace rdkafka's
 * default configuration. See the 'rd_kafka_conf_t' type for more information.
 *
 * 'errstr' must be a pointer to memory of at least size 'errstr_size' where
 * rd_kafka_new() may write a human readable error message in case the
 * creation of a new handle fails. In which case the function returns NULL.
 *
 * NOTE: Make sure SIGPIPE is either ignored or handled by the
 *       calling application.
 *
 * Returns the Kafka handle on success or NULL on error.
 *
 * To destroy the Kafka handle, use rd_kafka_destroy().
 * 
 * Locality: any thread
 */
rd_kafka_t *rd_kafka_new (rd_kafka_type_t type, const rd_kafka_conf_t *conf,
			  char *errstr, size_t errstr_size);


/**
 * Destroy the Kafka handle.
 * 
 * Locality: any thread
 */
void        rd_kafka_destroy (rd_kafka_t *rk);


/**
 * Returns the kafka handle name.
 */
const char *rd_kafka_name (const rd_kafka_t *rk);



/**
 * Creates a new topic handle for topic named 'topic'.
 *
 * 'conf' is an optional configuration for the topic
 * (see rd_kafka_topic_t above).
 *
 * Returns the new topic handle or NULL on error (see errno).
 */
rd_kafka_topic_t *rd_kafka_topic_new (rd_kafka_t *rk, const char *topic,
				      const rd_kafka_topic_conf_t *conf);



/**
 * Destroy a topic handle previously created with rd_kafka_topic_new().
 */
void rd_kafka_topic_destroy (rd_kafka_topic_t *rkt);


/**
 * Returns the name for a topic.
 */
const char *rd_kafka_topic_name (const rd_kafka_topic_t *rkt);


/**
 * Unassigned partition.
 */
#define RD_KAFKA_PARTITION_UA  ((int32_t)-1)  



/**
 * Produce and send a single message to the broker.
 *
 * 'rkt' is the target topic which must have been previously created with
 * rd_kafka_topic_new().
 *
 * 'partition' is the target partition, either:
 *   - RD_KAFKA_PARTITION_UA (unassigned) for
 *     automatic partitioning using the topic's partitioner function, or
 *   - a fixed partition (0..N)
 *
 * 'msgflags' is zero or more of the following flags OR:ed together:
 *    RD_KAFKA_MSG_F_FREE - rdkafka will free(3) 'payload' when it is done
 *                          with it.
 *    RD_KAFKA_MSG_F_COPY - the 'payload' data will be copied and the 'payload'
 *                          pointer will not be used by rdkafka after the
 *                          call returns.
 *
 *    .._F_FREE and .._F_COPY are mutually exclusive.
 *    (FIXME: describe freeing)   
 *
 * 'payload' is the message payload of size 'len' bytes.
 *
 * 'key' is an optional message key of size 'keylen' bytes, if non-NULL it
 * will be passed to the topic partitioner as well as be sent with the
 * message to the broker.
 *
 * 'msg_opaque' is an optional application-provided per-message opaque
 * pointer that will provided in callbacks functions referencing this message.
 * (i.e., the delivery report).
 *
 *
 * Returns 0 on success or -1 if the maximum number of outstanding messages
 * (conf.producer.max_messages) has been reached.
 *
 */

#define RD_KAFKA_MSG_F_FREE  0x1  /* Delegate freeing of payload to rdkafka. */
#define RD_KAFKA_MSG_F_COPY  0x2  /* rdkafka will make a copy of the payload. */

int rd_kafka_produce (rd_kafka_topic_t *rkt, int32_t partitition,
		      int msgflags,
		      char *payload, size_t len,
		      const void *key, size_t keylen,
		      void *msg_opaque);






/**
 * Adds a one or more brokers to the kafka handle's list of initial brokers.
 * Additional brokers will be discovered automatically as soon as rdkafka
 * connects to a broker by querying the broker metadata.
 *
 * If a broker name resolves to multiple addresses (and possibly
 * address families) all will be used for connection attempts in
 * round-robin fashion.
 *
 * 'brokerlist' is a ,-separated list of brokers in the format:
 *   <host1>[:<port1>],<host2>[:<port2>]...
 *
 * Returns the number of brokers succesfully added.
 */
int rd_kafka_brokers_add (rd_kafka_t *rk, const char *brokerlist);


/**
 * Polls the provided kafka handle for events.
 *
 * Events will cause application provided callbacks to be called.
 *
 * The 'timeout_ms' argument specifies the minimum amount of time
 * (in milliseconds) that the call will block waiting for events.
 * For non-blocking calls, provide 0 as 'timeout_ms'.
 * To wait indefinately for an event, provide -1.
 *
 * Returns the number of events served.
 */
int rd_kafka_poll (rd_kafka_t *rk, int timeout_ms);



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
void rd_kafka_set_logger (rd_kafka_t *rk,
			  void (*func) (const rd_kafka_t *rk, int level,
					const char *fac, const char *buf));


/**
 * Specifies the maximum logging level produced by
 * internal kafka logging and debugging.
 * Set to LOG_DEBUG (7) to enable debugging.
 */
void rd_kafka_set_log_level (rd_kafka_t *rk, int level);







/**
 * Returns the current out queue length:
 * messages waiting to be sent to, or acknowledged by, the broker.
 */
int         rd_kafka_outq_len (rd_kafka_t *rk);



/**
 * Dumps rdkafka's internal state for handle 'rk' to stream 'fp'
 * This is only useful for debugging rdkafka.
 */
void rd_kafka_dump (FILE *fp, rd_kafka_t *rk);
