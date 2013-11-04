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
 *
 * rdkafka.h contains the public API for librdkafka.
 * The API isdocumented in this file as comments prefixing the function, type,
 * enum, define, etc.
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


/**
 * rd_kafka_t handle type
 */
typedef enum {
	RD_KAFKA_PRODUCER,
	RD_KAFKA_CONSUMER,
} rd_kafka_type_t;


/**
 * Supported debug contexts (CSV "debug" configuration property)
 */
#define RD_KAFKA_DEBUG_CONTEXTS \
	"all,generic,broker,topic,metadata,producer,queue,msg"


/* Private types to provide ABI compatibility */
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
	RD_KAFKA_RESP_ERR__MSG_TIMED_OUT = -192, /* Produced message timed out*/
	RD_KAFKA_RESP_ERR__PARTITION_EOF = -191, /* Reached the end of the
						  * topic+partition queue on
						  * the broker.
						  * Not really an error. */
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
const char *rd_kafka_err2str (rd_kafka_resp_err_t err);






/*******************************************************************
 *								   *
 * Main configuration property interface			   *
 *								   *
 *******************************************************************/

/**
 * Configuration result type
 */
typedef enum {
	RD_KAFKA_CONF_UNKNOWN = -2, /* Unknown configuration name. */
	RD_KAFKA_CONF_INVALID = -1, /* Invalid configuration value. */
	RD_KAFKA_CONF_OK = 0,  /* Configuration okay */
} rd_kafka_conf_res_t;


/**
 * Create configuration object.
 * When providing your own configuration to the rd_kafka_*_new_*() calls
 * the rd_kafka_conf_t objects needs to be created with this function
 * which will set up the defaults.
 * I.e.:
 *
 *   rd_kafka_conf_t *myconf;
 *   rd_kafka_conf_res_t res;
 *
 *   myconf = rd_kafka_conf_new();
 *   res = rd_kafka_conf_set(myconf, "socket.timeout.ms", "600",
 *                           errstr, sizeof(errstr));
 *   if (res != RD_KAFKA_CONF_OK)
 *      die("%s\n", errstr);
 *   
 *   rk = rd_kafka_new(..., myconf);
 *
 * Please see CONFIGURATION.md for the default settings or use
 * `rd_kafka_conf_properties_show()` to provide the information at runtime.
 *
 * The properties are identical to the Apache Kafka configuration properties
 * whenever possible.
 */
rd_kafka_conf_t *rd_kafka_conf_new (void);



/**
 * Sets a configuration property.
 * 'conf' must have been previously created with rd_kafka_conf_new().
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
 * Set statistics callback in provided conf object.
 * The statistics callback is called from `rd_kafka_poll()` every
 * `statistics.interval.ms` (needs to be configured separately).
 * Function arguments:
 *   'rk' - Kafka handle
 *   'json' - String containing the statistics data in JSON format
 *   'json_len' - Length of 'json' string.
 *   'opaque' - Application-provided opaque.
 *
 * If the application wishes to hold on to the 'json' pointer and free
 * it at a later time it must return 1 from the `stats_cb`.
 * If the application returns 0 from the `stats_cb` then librdkafka
 * will immediately free the 'json' pointer.
 */
void rd_kafka_conf_set_stats_cb (rd_kafka_conf_t *conf,
				 int (*stats_cb) (rd_kafka_t *rk,
						  char *json,
						  size_t json_len,
						  void *opaque));


/**
 * Sets the application's opaque pointer that will be passed to `dr_cb`
 * and `error_cb_` callbacks as the 'opaque' argument.
 */
void rd_kafka_conf_set_opaque (rd_kafka_conf_t *conf, void *opaque);


/**
 * Prints a table to 'fp' of all supported configuration properties,
 * their default values as well as a description.
 */
void rd_kafka_conf_properties_show (FILE *fp);



/*******************************************************************
 *								   *
 * Topic configuration property interface			   *
 *								   *
 *******************************************************************/

/**
 * Create topic configuration object
 *
 * Same semantics as for rd_kafka_conf_new().
 */
rd_kafka_topic_conf_t *rd_kafka_topic_conf_new (void);

/**
 * Sets a single rd_kafka_topic_conf_t value by property name.
 * 'topic_conf' should have been previously set up
 *  with `rd_kafka_topic_conf_new()`.
 *
 * Returns rd_kafka_conf_res_t to indicate success or failure.
 */
rd_kafka_conf_res_t rd_kafka_topic_conf_set (rd_kafka_topic_conf_t *conf,
					     const char *name,
					     const char *value,
					     char *errstr, size_t errstr_size);

/**
 * Sets the application's opaque pointer that will be passed to all topic
 * callbacks as the 'rkt_opaque' argument.
 */
void rd_kafka_topic_conf_set_opaque (rd_kafka_topic_conf_t *conf, void *opaque);


/**
 * Producer:
 * Set partitioner callback in provided topic conf object.
 *
 * The partitioner may be called in any thread at any time,
 * it may be called multiple times for the same message/key.
 *
 * Partitioner function constraints:
 *    - MUST NOT call any rd_kafka_*() functions except:
 *        rd_kafka_topic_partition_available()
 *    - MUST NOT block or execute for prolonged periods of time.
 *    - MUST return a value between 0 and partition_cnt-1, or the
 *      special RD_KAFKA_PARTITION_UA value if partitioning
 *      could not be performed.
 */
void
rd_kafka_topic_conf_set_partitioner_cb (rd_kafka_topic_conf_t *topic_conf,
					int32_t (*partitioner) (
						const rd_kafka_topic_t *rkt,
						const void *keydata,
						size_t keylen,
						int32_t partition_cnt,
						void *rkt_opaque,
						void *msg_opaque));

/**
 * Check if partition is available (has a leader broker).
 *
 * Returns 1 if the partition is available, else 0.
 *
 * NOTE: This function must only be called from inside a partitioner function.
 */
int rd_kafka_topic_partition_available (const rd_kafka_topic_t *rkt,
					int32_t partition);


/*******************************************************************
 *								   *
 * Partitioners provided by rdkafka                                *
 *								   *
 *******************************************************************/

/**
 * Random partitioner.
 * This is the default partitioner.
 *
 * Returns a random partition between 0 and 'partition_cnt'-1.
 */
int32_t rd_kafka_msg_partitioner_random (const rd_kafka_topic_t *rkt,
					 const void *key, size_t keylen,
					 int32_t partition_cnt,
					 void *opaque, void *msg_opaque);




/*******************************************************************
 *								   *
 * Kafka object handle                                             *
 *								   *
 *******************************************************************/



/**
 * Creates a new Kafka handle and starts its operation according to the
 * specified 'type'.
 *
 * 'conf' is an optional struct created with `rd_kafka_conf_new()` that will
 * be used instead of the default configuration.
 * See `rd_kafka_conf_set()` et.al for more information.
 *
 * 'errstr' must be a pointer to memory of at least size 'errstr_size' where
 * `rd_kafka_new()` may write a human readable error message in case the
 * creation of a new handle fails. In which case the function returns NULL.
 *
 * NOTE: Make sure the SIGPIPE signal is either ignored or handled by the
 *       calling application.
 *
 * Returns the Kafka handle on success or NULL on error.
 *
 * To destroy the Kafka handle, use rd_kafka_destroy().
 */
rd_kafka_t *rd_kafka_new (rd_kafka_type_t type, rd_kafka_conf_t *conf,
			  char *errstr, size_t errstr_size);


/**
 * Destroy Kafka handle.
 * 
 */
void        rd_kafka_destroy (rd_kafka_t *rk);



/**
 * Returns Kafka handle name.
 */
const char *rd_kafka_name (const rd_kafka_t *rk);



/**
 * Creates a new topic handle for topic named 'topic'.
 *
 * 'conf' is an optional configuration for the topic created with
 * `rd_kafka_topic_conf_new()` that will be used instead of the default
 * topic configuration.
 * See `rd_kafka_topic_conf_set()` et.al for more information.
 *
 * Returns the new topic handle or NULL on error (see `errno`).
 */
rd_kafka_topic_t *rd_kafka_topic_new (rd_kafka_t *rk, const char *topic,
				      rd_kafka_topic_conf_t *conf);



/**
 * Destroy topic handle previously created with `rd_kafka_topic_new()`.
 */
void rd_kafka_topic_destroy (rd_kafka_topic_t *rkt);


/**
 * Returns the topic name.
 */
const char *rd_kafka_topic_name (const rd_kafka_topic_t *rkt);


/**
 * Unassigned partition.
 *
 * The unassigned partition is used by the producer API for messages
 * that should be partitioned using the configured or default partitioner.
 */
#define RD_KAFKA_PARTITION_UA  ((int32_t)-1)  




/*******************************************************************
 *								   *
 * Kafka messages                                                  *
 *								   *
 *******************************************************************/


/**
 * A Kafka message as returned by the `rd_kafka_consume*()` family
 * of functions.
 *
 * This object has two purposes:
 *  - provide the application with a consumed message. ('err' == 0)
 *  - report per-topic+partition consumer errors ('err' != 0)
 *
 * The application must check 'err' to decide what action to take.
 *
 * When the application is finished with a message it must call
 * `rd_kafka_message_destroy()`.
 */
typedef struct rd_kafka_message_s {
	rd_kafka_resp_err_t err;   /* Non-zero for error signaling. */
	rd_kafka_topic_t *rkt;     /* Topic */
	int32_t partition;         /* Partition */
	void   *payload;           /* err==0: Message payload
				    * err!=0: Error string */
	size_t  len;               /* err==0: Message payload length
				    * err!=0: Error string length */
	void   *key;               /* err==0: Optional message key */
	size_t  key_len;           /* err==0: Optional message key length */
	int64_t offset;            /* Message offset (or offset for error
				    * if err!=0 if applicable). */
	void  *_private;           /* rdkafka private pointer: DO NOT MODIFY */
} rd_kafka_message_t;


/**
 * Frees resources for 'rkmessage' and hands ownership back to rdkafka.
 */
void rd_kafka_message_destroy (rd_kafka_message_t *rkmessage);


/**
 * Returns the error string for an errored rd_kafka_message_t or NULL if
 * there was no error.
 */
static inline const char * 
__attribute__((unused))
rd_kafka_message_errstr (const rd_kafka_message_t *rkmessage) {
	if (!rkmessage->err)
		return NULL;

	if (rkmessage->payload)
		return rkmessage->payload;

	return rd_kafka_err2str(rkmessage->err);
}




/*******************************************************************
 *								   *
 * Consumer API                                                    *
 *								   *
 *******************************************************************/


#define RD_KAFKA_OFFSET_BEGINNING -2  /* Start consuming from beginning of
				       * kafka partition queue: oldest msg */
#define RD_KAFKA_OFFSET_END       -1  /* Start consuming from end of kafka
				       * partition queue: next msg */


/**
 * Start consuming messages for topic 'rkt' and 'partition'
 * at offset 'offset' which may either be a proper offset (0..N)
 * or one of the the special offsets:
 *  `RD_KAFKA_OFFSET_BEGINNING` or `RD_KAFKA_OFFSET_END`.
 *
 * rdkafka will attempt to keep 'queued.min.messages' (config property)
 * messages in the local queue by repeatedly fetching batches of messages
 * from the broker until the threshold is reached.
 *
 * The application shall use one of the `rd_kafka_consume*()` functions
 * to consume messages from the local queue, each kafka message being
 * represented as a `rd_kafka_message_t *` object.
 *
 * `rd_kafka_consume_start()` must not be called multiple times without
 * stopping consumption first with `rd_kafka_consume_stop()`.
 *
 * Returns 0 on success or -1 on error (see `errno`).
 */
int rd_kafka_consume_start (rd_kafka_topic_t *rkt, int32_t partition,
			     int64_t offset);

/**
 * Stop consuming messages for topic 'rkt' and 'partition', purging
 * all messages currently in the local queue.
 *
 * The application needs to be stop all consumers before calling
 * `rd_kafka_destroy()` on the main object handle.
 *
 * Returns 0 on success or -1 on error (see `errno`).
 */
int rd_kafka_consume_stop (rd_kafka_topic_t *rkt, int32_t partition);



/**
 * Consume a single message from topic 'rkt' and 'partition'.
 *
 * 'timeout_ms' is maximum amount of time to wait for a message to be received.
 * Consumer must have been previously started with `rd_kafka_consume_start()`.
 *
 * Returns a message object on success and NULL on error.
 *
 * Errors (when returning NULL):
 *   ETIMEDOUT - 'timeout_ms' was reached with no new messages fetched.
 *   ENOENT    - 'rkt'+'partition' is unknown.
 *                (no prior `rd_kafka_consume_start()` call)
 *
 * The returned message's '..->err' must be checked for errors.
 */
rd_kafka_message_t *rd_kafka_consume (rd_kafka_topic_t *rkt, int32_t partition,
				      int timeout_ms);



/**
 * Consume up to 'rkmessages_size' from topic 'rkt' and 'partition',
 * putting a pointer to each message in the application provided
 * array 'rkmessages' (of size 'rkmessages_size' entries).
 *
 * `rd_kafka_consume_batch()` provides higher throughput performance
 * than `rd_kafka_consume()`.
 *
 * 'timeout_ms' is the maximum amount of time to wait for all of
 * 'rkmessages_size' messages to be put into 'rkmessages'.
 * This differs somewhat from `rd_kafka_consume()`.
 *
 * Returns the number of rkmessages added in 'rkmessages',
 * or -1 on error (same error codes as for `rd_kafka_consume()`.
 */
ssize_t rd_kafka_consume_batch (rd_kafka_topic_t *rkt, int32_t partition,
				int min_wait_ms,
				rd_kafka_message_t **rkmessages,
				size_t rkmessages_size);



/**
 * Consumes messages from topic 'rkt' and 'partition', calling
 * the provided callback for each consumed messsage.
 *
 * `rd_kafka_consume_callback()` provides higher throughput performance
 * than both `rd_kafka_consume()` and `rd_kafka_consume_batch()`.
 *
 * 'timeout_ms' is the maximum amount of time to wait for one or more messages
 * to arrive.
 *
 * The provided 'consume_cb' function is called for each message,
 * the application must not call `rd_kafka_message_destroy()` on the provided
 * 'rkmessage'.
 *
 * The 'opaque' argument is passed to the 'consume_cb' as 'opaque'.
 *
 * Returns the number of messages processed or -1 on error.
 */
int rd_kafka_consume_callback (rd_kafka_topic_t *rkt, int32_t partition,
			       int timeout_ms,
			       void (*consume_cb) (rd_kafka_message_t
						   *rkmessage,
						   void *opaque),
			       void *opaque);








/*******************************************************************
 *								   *
 * Producer API                                                    *
 *								   *
 *******************************************************************/


/**
 * Produce and send a single message to broker.
 *
 * 'rkt' is the target topic which must have been previously created with
 * `rd_kafka_topic_new()`.
 *
 * `rd_kafka_produce()` is an asynch non-blocking API.
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
 * message to the broker and passed on to the consumer.
 *
 * 'msg_opaque' is an optional application-provided per-message opaque
 * pointer that will provided in the delivery report callback (`dr_cb`) for
 * referencing this message.
 *
 * Returns 0 on success or -1 if the maximum number of outstanding messages
 * (conf.producer.max_messages) has been reached (`errno==ENOBUFS`).
 *
 */

#define RD_KAFKA_MSG_F_FREE  0x1  /* Delegate freeing of payload to rdkafka. */
#define RD_KAFKA_MSG_F_COPY  0x2  /* rdkafka will make a copy of the payload. */

int rd_kafka_produce (rd_kafka_topic_t *rkt, int32_t partitition,
		      int msgflags,
		      char *payload, size_t len,
		      const void *key, size_t keylen,
		      void *msg_opaque);






/*******************************************************************
 *								   *
 * Misc API                                                        *
 *								   *
 *******************************************************************/

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
 * Events:
 *   - delivery report callbacks  (if dr_cb is configured) [producer]
 *   - error callbacks (if error_cb is configured) [producer & consumer]
 *
 * Returns the number of events served.
 */
int rd_kafka_poll (rd_kafka_t *rk, int timeout_ms);


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
 * Returns the number of brokers successfully added.
 *
 * NOTE: Brokers may also be defined with the 'metadata.broker.list'
 *       configuration property.
 */
int rd_kafka_brokers_add (rd_kafka_t *rk, const char *brokerlist);




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
 * If the 'debug' configuration property is set the level is automatically
 * adjusted to LOG_DEBUG (7).
 */
void rd_kafka_set_log_level (rd_kafka_t *rk, int level);


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
 * Returns the current out queue length:
 * messages waiting to be sent to, or acknowledged by, the broker.
 */
int         rd_kafka_outq_len (rd_kafka_t *rk);



/**
 * Dumps rdkafka's internal state for handle 'rk' to stream 'fp'
 * This is only useful for debugging rdkafka, showing state and statistics
 * for brokers, topics, partitions, etc.
 */
void rd_kafka_dump (FILE *fp, rd_kafka_t *rk);



/**
 * Retrieve the current number of threads in use by librdkafka.
 * Used by regression tests.
 */
int rd_kafka_thread_cnt (void);


/**
 * Wait for all rd_kafka_t objects to be destroyed.
 * Returns 0 if all kafka objects are now destroyed, or -1 if the
 * timeout was reached.
 * Since `rd_kafka_destroy()` is an asynch operation the 
 * `rd_kafka_wait_destroyed()` function can be used for applications where
 * a clean shutdown is required.
 */
int rd_kafka_wait_destroyed (int timeout_ms);


