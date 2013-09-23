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


/* Supported debug contexts (CSV "debug" configuration property) */
#define RD_KAFKA_DEBUG_CONTEXTS \
	"all,generic,broker,topic,metadata,producer,queue,msg"



typedef struct rd_kafka_s rd_kafka_t;
typedef struct rd_kafka_topic_s rd_kafka_topic_t;
typedef struct rd_kafka_conf_s rd_kafka_conf_t;
typedef struct rd_kafka_topic_conf_s rd_kafka_topic_conf_t;

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
	RD_KAFKA_RESP_ERR__CRIT_SYS_RESOURCE = -194, /* Critical system resource
						      * failure */
	RD_KAFKA_RESP_ERR__RESOLVE = -193,   /* Failed to resolve broker */
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
 * the rd_kafka_conf_t objects needs to be created with this function
 * which will set up the defaults.
 * I.e.:
 *
 *   rd_kafka_conf_t *myconf;
 *   myconf = rd_kafka_conf_new();
 *   rd_kafka_conf_set(myconf, "socket.timeout.ms", "600");
 *   
 *   rk = rd_kafka_new_consumer(, ... myconf);
 *
 * Please see rdkafka_defaultconf.c for the default settings.
 */
rd_kafka_conf_t *rd_kafka_conf_new (void);

typedef enum {
	RD_KAFKA_CONF_UNKNOWN = -2, /* Unknown configuration name. */
	RD_KAFKA_CONF_INVALID = -1, /* Invalid configuration value. */
	RD_KAFKA_CONF_OK = 0,  /* Configuration okay */
} rd_kafka_conf_res_t;


/**
 * Sets a single rd_kafka_conf_t value by property name.
 * 'conf' must have been previously set up with rd_kafka_conf_new().
 *
 * Returns rd_kafka_conf_res_t to indicate success or failure.
 * In case of failure 'errstr' is updated to contain a human readable
 * error string.
 */
rd_kafka_conf_res_t rd_kafka_conf_set (rd_kafka_conf_t *conf,
				       const char *name,
				       const char *value,
				       char *errstr, size_t errstr_size);



/**
 * Producer:
 * Set delivery report callback in provided conf object.
 */
void rd_kafka_conf_set_dr_cb (rd_kafka_conf_t *conf,
			      void (*dr_cb) (rd_kafka_t *rk,
					     void *payload, size_t len,
					     rd_kafka_resp_err_t err,
					     void *opaque, void *msg_opaque));

/**
 * Set error callback in provided conf object.
 * The error callback is used by librdkafka to signal critical errors
 * back to the application.
 */
void rd_kafka_conf_set_error_cb (rd_kafka_conf_t *conf,
				 void  (*error_cb) (rd_kafka_t *rk, int err,
						    const char *reason,
						    void *opaque));


/**
 * Sets the application's opaque pointer that will be passed to all callbacks
 * as the 'opaque' argument.
 *
 * FIXME: exception consume_callback
 */
void rd_kafka_conf_set_opaque (rd_kafka_conf_t *conf, void *opaque);



/**
 * Consumer:
 * Set consume callback in provided conf object.
 */
void rd_kafka_conf_set_consume_cb (rd_kafka_conf_t *conf,
				   void (*consume_cb) (rd_kafka_t *rk,
						       rd_kafka_topic_t *rkt,
						       int32_t partition,
						       void *payload,
						       size_t len,
						       void *key,
						       size_t key_len,
						       int64_t offset,
						       void *opaque));

/**
 * Create topic configuration object
 *
 * Same semantics as for rd_kafka_conf_new().
 */
rd_kafka_topic_conf_t *rd_kafka_topic_conf_new (void);

/**
 * Sets a single rd_kafka_topic_conf_t value by property name.
 * 'topic_conf' should have been previously set up
 *  with rd_kafka_topic_conf_new().
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
rd_kafka_t *rd_kafka_new (rd_kafka_type_t type, rd_kafka_conf_t *conf,
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
				      rd_kafka_topic_conf_t *conf);



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
 * Consumes messages from topic 'rkt', waiting at most 'timeout_ms'
 * milliseconds for one or more messages to arrive.
 *
 * Calls consume_cb() for each message with the following arguments:
 *     * rkt,partition - Topic and partition
 *     * payload,len   - Message payload. The memory belogns to librdkafka
 *                       and must not be referenced after the callback returns.
 *     * key,key_len   - Message key if any.
 *     * next_offset   - The offset of the next message.
 *     * opaque        - Application 'opaque' pointer.
 *
 * Returns the number of messages processed.
 */
int rd_kafka_consume_callback (rd_kafka_topic_t *rkt, int32_t partition,
			       int timeout_ms,
			       void (*consume_cb) (rd_kafka_topic_t *rkt,
						   int32_t partition,
						   void *payload, size_t len,
						   void *key, size_t key_len,
						   int64_t next_offset,
						   void *opaque),
			       void *opaque);



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



/**
 * Retrieve the current number of threads in use by librdkafka.
 * Used by regression tests.
 */
int rd_kafka_thread_cnt (void);


/* FIXME */
void rd_kafka_consume_start (rd_kafka_topic_t *rkt, int32_t partition,
			     int64_t offset);
