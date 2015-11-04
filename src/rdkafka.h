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
#include <sys/types.h>

#ifdef __cplusplus
extern "C" {
#if 0
} /* Restore indent */
#endif
#endif

#ifdef _MSC_VER
#include <basetsd.h>
typedef SSIZE_T ssize_t;
#define RD_UNUSED
#define RD_DEPRECATED
#undef RD_EXPORT
#ifdef LIBRDKAFKA_EXPORTS
#define RD_EXPORT __declspec(dllexport)
#else
#define RD_EXPORT __declspec(dllimport)
#endif

#else
#define RD_UNUSED __attribute__((unused))
#define RD_EXPORT
#define RD_DEPRECATED __attribute__((deprecated))
#endif

/**
 * librdkafka version
 *
 * Interpreted as hex MM.mm.rr.xx:
 *   MM = Major
 *   mm = minor
 *   rr = revision
 *   xx = currently unused
 *
 * I.e.: 0x00080100 = 0.8.1
 */
#define RD_KAFKA_VERSION  0x00080600

/**
 * Returns the librdkafka version as integer.
 */
RD_EXPORT
int rd_kafka_version(void);

/**
 * Returns the librdkafka version as string.
 */
RD_EXPORT
const char *rd_kafka_version_str (void);


/**
 * rd_kafka_t handle type
 */
typedef enum rd_kafka_type_t {
	RD_KAFKA_PRODUCER,
	RD_KAFKA_CONSUMER
} rd_kafka_type_t;


/**
 * Supported debug contexts (CSV "debug" configuration property)
 */
RD_EXPORT
const char *rd_kafka_get_debug_contexts(void);

/* Same as define (deprecated) */
#define RD_KAFKA_DEBUG_CONTEXTS \
	"all,generic,broker,topic,metadata,producer,queue,msg,protocol,cgrp"

/* Private types to provide ABI compatibility */
typedef struct rd_kafka_s rd_kafka_t;
typedef struct rd_kafka_topic_s rd_kafka_topic_t;
typedef struct rd_kafka_conf_s rd_kafka_conf_t;
typedef struct rd_kafka_topic_conf_s rd_kafka_topic_conf_t;
typedef struct rd_kafka_queue_s rd_kafka_queue_t;

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
	RD_KAFKA_RESP_ERR__UNKNOWN_PARTITION = -190, /* Permanent:
						      * Partition does not
						      * exist in cluster. */
	RD_KAFKA_RESP_ERR__FS = -189,        /* File or filesystem error */
	RD_KAFKA_RESP_ERR__UNKNOWN_TOPIC = -188, /* Permanent:
						  * Topic does not exist
						  * in cluster. */
	RD_KAFKA_RESP_ERR__ALL_BROKERS_DOWN = -187, /* All broker connections
						     * are down. */
	RD_KAFKA_RESP_ERR__INVALID_ARG = -186,  /* Invalid argument, or
						 * invalid configuration */
	RD_KAFKA_RESP_ERR__TIMED_OUT = -185,    /* Operation timed out */
	RD_KAFKA_RESP_ERR__QUEUE_FULL = -184,   /* Queue is full */
        RD_KAFKA_RESP_ERR__ISR_INSUFF = -183,   /* ISR count < required.acks */
        RD_KAFKA_RESP_ERR__NODE_UPDATE = -182,  /* Broker node update */
	RD_KAFKA_RESP_ERR__SSL = -181,          /* SSL error */
        RD_KAFKA_RESP_ERR__WAIT_COORD = -180,   /* Waiting for coordinator
                                                 * to become available. */
        RD_KAFKA_RESP_ERR__UNKNOWN_GROUP = -179,/* Unknown client group */
        RD_KAFKA_RESP_ERR__IN_PROGRESS = -178,  /* Operation in progress */
        RD_KAFKA_RESP_ERR__PREV_IN_PROGRESS = -177, /* Previous operation
                                                     * in progress, wait for
                                                     * it to finish. */
        RD_KAFKA_RESP_ERR__EXISTING_SUBSCRIPTION = -176, /* This operation
                                                          * would interfer
                                                          * with an existing
                                                          * subscription */
        RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS = -175, /* For use w rebalance_cb*/
        RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS = -174, /* For use w rebalance_cb*/
        RD_KAFKA_RESP_ERR__CONFLICT = -173,     /* Conflicting use */
        RD_KAFKA_RESP_ERR__STATE = -172,        /* Wrong state */
        RD_KAFKA_RESP_ERR__UNKNOWN_PROTOCOL = -171, /* Unknown protocol */
        RD_KAFKA_RESP_ERR__NOT_IMPLEMENTED = -170, /* Not implemented */

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
        RD_KAFKA_RESP_ERR_OFFSET_LOAD_IN_PROGRESS = 14,
        RD_KAFKA_RESP_ERR_CONSUMER_COORDINATOR_NOT_AVAILABLE = 15,
        RD_KAFKA_RESP_ERR_NOT_COORDINATOR_FOR_CONSUMER = 16,
        RD_KAFKA_RESP_ERR_TOPIC_EXCEPTION = 17,
        RD_KAFKA_RESP_ERR_RECORD_LIST_TOO_LARGE = 18,
        RD_KAFKA_RESP_ERR_NOT_ENOUGH_REPLICAS = 19,
        RD_KAFKA_RESP_ERR_NOT_ENOUGH_REPLICAS_AFTER_APPEND = 20,
        RD_KAFKA_RESP_ERR_INVALID_REQUIRED_ACKS = 21,
        RD_KAFKA_RESP_ERR_ILLEGAL_GENERATION = 22,
        RD_KAFKA_RESP_ERR_INCONSISTENT_GROUP_PROTOCOL = 23,
        /* NOTE: 24 is not defined */
        RD_KAFKA_RESP_ERR_UNKNOWN_MEMBER = 25,
        RD_KAFKA_RESP_ERR_INVALID_SESSION_TIMEOUT = 26,
        /* NOTE: 27 is not defined */
        RD_KAFKA_RESP_ERR_INVALID_COMMIT_OFFSET_SIZE = 28,
        RD_KAFKA_RESP_ERR_AUTHORIZATION_FAILED = 29,
        RD_KAFKA_RESP_ERR_REBALANCE_IN_PROGRESS = 30
} rd_kafka_resp_err_t;


/**
 * Returns a human readable representation of a kafka error.
 */
RD_EXPORT
const char *rd_kafka_err2str (rd_kafka_resp_err_t err);


/**
 * Converts `errno` to a `rd_kafka_resp_err_t` error code
 * upon failure from the following functions:
 *  - rd_kafka_topic_new()
 *  - rd_kafka_consume_start()
 *  - rd_kafka_consume_stop()
 *  - rd_kafka_consume()
 *  - rd_kafka_consume_batch()
 *  - rd_kafka_consume_callback()
 *  - rd_kafka_produce()
 */
RD_EXPORT
rd_kafka_resp_err_t rd_kafka_errno2err(int errnox);




/*******************************************************************
 *								   *
 * Topic+Partition place holder                                    *
 *								   *
 *******************************************************************/

typedef struct rd_kafka_topic_partition_s {
        char      *topic;
        int32_t    partition;
        void     *_private;    /* INTERNAL USE ONLY, DO NOT TOUCH:
                                * : shptr_rd_kafka_toppar_t */
} rd_kafka_topic_partition_t;



typedef struct rd_kafka_topic_partition_list_s {
        int cnt;               /* Current number of elements */
        int size;              /* Allocated size */
        rd_kafka_topic_partition_t *elems;
} rd_kafka_topic_partition_list_t;

RD_EXPORT
rd_kafka_topic_partition_list_t *rd_kafka_topic_partition_list_new (int size);
RD_EXPORT
void rd_kafka_topic_partition_list_destroy (rd_kafka_topic_partition_list_t *rkparlist);
RD_EXPORT
void rd_kafka_topic_partition_list_add (rd_kafka_topic_partition_list_t *rktparlist,
                                        const char *topic, int32_t partition);

RD_EXPORT
void
rd_kafka_topic_partition_list_add_range (rd_kafka_topic_partition_list_t
                                         *rktparlist,
                                         const char *topic,
                                         int32_t start, int32_t stop);


RD_EXPORT
rd_kafka_topic_partition_list_t *
rd_kafka_topic_partition_list_copy (const rd_kafka_topic_partition_list_t *src);


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
	int64_t offset;            /* Consume:
                                    *   Message offset (or offset for error
				    *   if err!=0 if applicable).
                                    * dr_msg_cb:
                                    *   Message offset assigned by broker.
                                    *   If produce.offset.report is set then
                                    *   each message will have this field set,
                                    *   otherwise only the last message in
                                    *   each produced internal batch will
                                    *   have this field set, otherwise 0. */
	void  *_private;           /* Consume:
                                    *   rdkafka private pointer: DO NOT MODIFY
                                    * dr_msg_cb:
                                    *   mgs_opaque from produce() call */
} rd_kafka_message_t;


/**
 * Frees resources for 'rkmessage' and hands ownership back to rdkafka.
 */
RD_EXPORT
void rd_kafka_message_destroy(rd_kafka_message_t *rkmessage);


/**
 * Returns the error string for an errored rd_kafka_message_t or NULL if
 * there was no error.
 */

static __inline const char *
RD_UNUSED 
rd_kafka_message_errstr(const rd_kafka_message_t *rkmessage) {
	if (!rkmessage->err)
		return NULL;

	if (rkmessage->payload)
		return (const char *)rkmessage->payload;

	return rd_kafka_err2str(rkmessage->err);
}


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
	RD_KAFKA_CONF_OK = 0        /* Configuration okay */
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
RD_EXPORT
rd_kafka_conf_t *rd_kafka_conf_new(void);

/**
 * Destroys a conf object.
 */
RD_EXPORT
void rd_kafka_conf_destroy(rd_kafka_conf_t *conf);


/**
 * Creates a copy/duplicate of configuration object 'conf'.
 */
RD_EXPORT
rd_kafka_conf_t *rd_kafka_conf_dup(const rd_kafka_conf_t *conf);


/**
 * Sets a configuration property.
 * 'conf' must have been previously created with rd_kafka_conf_new().
 *
 * Returns rd_kafka_conf_res_t to indicate success or failure.
 * In case of failure 'errstr' is updated to contain a human readable
 * error string.
 */
RD_EXPORT
rd_kafka_conf_res_t rd_kafka_conf_set(rd_kafka_conf_t *conf,
				       const char *name,
				       const char *value,
				       char *errstr, size_t errstr_size);



/**
 * Producer:
 * Set delivery report callback in provided conf object.
 */
RD_EXPORT
void rd_kafka_conf_set_dr_cb(rd_kafka_conf_t *conf,
			      void (*dr_cb) (rd_kafka_t *rk,
					     void *payload, size_t len,
					     rd_kafka_resp_err_t err,
					     void *opaque, void *msg_opaque));

/**
 * Producer:
 * Set delivery report callback in provided conf object.
 */
RD_EXPORT
void rd_kafka_conf_set_dr_msg_cb(rd_kafka_conf_t *conf,
                                  void (*dr_msg_cb) (rd_kafka_t *rk,
                                                     const rd_kafka_message_t *
                                                     rkmessage,
                                                     void *opaque));


/**
 * Consumer:
 * Set consume callback for use with `rd_kafka_consumer_poll()`.
 */
RD_EXPORT
void rd_kafka_conf_set_consume_cb (rd_kafka_conf_t *conf,
                                   void (*consume_cb) (rd_kafka_message_t *
                                                       rkmessage,
                                                       void *opaque));

/**
 * Consumer:
 * Set rebalance callback for use with coordinated consumer group balancing.
 * The 'err' field is set to either RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS
 * or RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS and 'partitions'
 * contains the partitions that were either assigned or revoked.
 */
RD_EXPORT
void rd_kafka_conf_set_rebalance_cb (
        rd_kafka_conf_t *conf,
        void (*rebalance_cb) (rd_kafka_t *rk,
                              rd_kafka_resp_err_t err,
                              const rd_kafka_topic_partition_list_t *partitions,
                              void *opaque));

/**
 * Set error callback in provided conf object.
 * The error callback is used by librdkafka to signal critical errors
 * back to the application.
 */
RD_EXPORT
void rd_kafka_conf_set_error_cb(rd_kafka_conf_t *conf,
				 void  (*error_cb) (rd_kafka_t *rk, int err,
						    const char *reason,
						    void *opaque));


/**
 * Set logger callback.
 * The default is to print to stderr, but a syslog logger is also available,
 * see rd_kafka_log_(print|syslog) for the builtin alternatives.
 * Alternatively the application may provide its own logger callback.
 * Or pass 'func' as NULL to disable logging.
 *
 * This is the configuration alternative to `rd_kafka_set_logger()`.
 */
RD_EXPORT
void rd_kafka_conf_set_log_cb(rd_kafka_conf_t *conf,
			  void (*log_cb) (const rd_kafka_t *rk, int level,
                                          const char *fac, const char *buf));


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
RD_EXPORT
void rd_kafka_conf_set_stats_cb(rd_kafka_conf_t *conf,
				 int (*stats_cb) (rd_kafka_t *rk,
						  char *json,
						  size_t json_len,
						  void *opaque));



/**
 * Set socket callback.
 * The socket callback is responsible for opening a socket
 * according to the supplied domain, type and protocol.
 * The socket shall be created with CLOEXEC set in a racefree fashion, if
 * possible.
 *
 * Default:
 *  on linux: racefree CLOEXEC
 *  others  : non-racefree CLOEXEC
 */
RD_EXPORT
void rd_kafka_conf_set_socket_cb(rd_kafka_conf_t *conf,
                                  int (*socket_cb) (int domain, int type,
                                                    int protocol,
                                                    void *opaque));


#ifndef _MSC_VER
/**
 * Set open callback.
 * The open callback is responsible for opening the file specified by
 * pathname, flags and mode.
 * The file shall be opened with CLOEXEC set in a racefree fashion, if
 * possible.
 *
 * Default:
 *  on linux: racefree CLOEXEC
 *  others  : non-racefree CLOEXEC
 */
RD_EXPORT
void rd_kafka_conf_set_open_cb (rd_kafka_conf_t *conf,
                                int (*open_cb) (const char *pathname,
                                                int flags, mode_t mode,
                                                void *opaque));
#endif

/**
 * Sets the application's opaque pointer that will be passed to `dr_cb`
 * and `error_cb_` callbacks as the 'opaque' argument.
 */
RD_EXPORT
void rd_kafka_conf_set_opaque(rd_kafka_conf_t *conf, void *opaque);

/**
 * Retrieves the opaque pointer previously set with rd_kafka_conf_set_opaque()
 */
RD_EXPORT
void *rd_kafka_opaque(const rd_kafka_t *rk);



/**
 * Sets the default topic configuration to use for automatically
 * subscribed topics (e.g., through pattern-matched topics).
 * The topic config object is not usable after this call.
 */
RD_EXPORT
void rd_kafka_conf_set_default_topic_conf (rd_kafka_conf_t *conf,
                                           rd_kafka_topic_conf_t *tconf);



/**
 * Retrieve configuration value for property `name`.
 *
 * If `dest` is non-NULL the value will be written to `dest` with at
 * most `dest_size`.
 *
 * `*dest_size` is updated to the full length of the value, thus if
 * `*dest_size` initially is smaller than the full length the application
 * may reallocate `dest` to fit the returned `*dest_size` and try again.
 *
 * If `dest` is NULL only the full length of the value is returned.
 *
 * Returns RD_KAFKA_CONF_OK if the property name matched, else
 * RD_KAFKA_CONF_UNKNOWN.
 */
RD_EXPORT
rd_kafka_conf_res_t rd_kafka_conf_get (const rd_kafka_conf_t *conf,
                                       const char *name,
                                       char *dest, size_t *dest_size);


/**
 * Same as `rd_kafka_conf_get()` but for topic configuration objects.
 */
RD_EXPORT
rd_kafka_conf_res_t rd_kafka_topic_conf_get (const rd_kafka_topic_conf_t *conf,
                                             const char *name,
                                             char *dest, size_t *dest_size);


/**
 * Dump the configuration properties and values of `conf` to an array
 * with "key", "value" pairs. The number of entries in the array is
 * returned in `*cntp`.
 *
 * The dump must be freed with `rd_kafka_conf_dump_free()`.
 */
RD_EXPORT
const char **rd_kafka_conf_dump(rd_kafka_conf_t *conf, size_t *cntp);


/**
 * Dump the topic configuration properties and values of `conf` to an array
 * with "key", "value" pairs. The number of entries in the array is
 * returned in `*cntp`.
 *
 * The dump must be freed with `rd_kafka_conf_dump_free()`.
 */
RD_EXPORT
const char **rd_kafka_topic_conf_dump(rd_kafka_topic_conf_t *conf,
				       size_t *cntp);

/**
 * Frees a configuration dump returned from `rd_kafka_conf_dump()` or
 * `rd_kafka_topic_conf_dump().
 */
RD_EXPORT
void rd_kafka_conf_dump_free(const char **arr, size_t cnt);

/**
 * Prints a table to 'fp' of all supported configuration properties,
 * their default values as well as a description.
 */
RD_EXPORT
void rd_kafka_conf_properties_show(FILE *fp);



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
RD_EXPORT
rd_kafka_topic_conf_t *rd_kafka_topic_conf_new(void);


/**
 * Creates a copy/duplicate of topic configuration object 'conf'.
 */
RD_EXPORT
rd_kafka_topic_conf_t *rd_kafka_topic_conf_dup(const rd_kafka_topic_conf_t
						*conf);


/**
 * Destroys a topic conf object.
 */
RD_EXPORT
void rd_kafka_topic_conf_destroy(rd_kafka_topic_conf_t *topic_conf);


/**
 * Sets a single rd_kafka_topic_conf_t value by property name.
 * 'topic_conf' should have been previously set up
 *  with `rd_kafka_topic_conf_new()`.
 *
 * Returns rd_kafka_conf_res_t to indicate success or failure.
 */
RD_EXPORT
rd_kafka_conf_res_t rd_kafka_topic_conf_set(rd_kafka_topic_conf_t *conf,
					     const char *name,
					     const char *value,
					     char *errstr, size_t errstr_size);

/**
 * Sets the application's opaque pointer that will be passed to all topic
 * callbacks as the 'rkt_opaque' argument.
 */
RD_EXPORT
void rd_kafka_topic_conf_set_opaque(rd_kafka_topic_conf_t *conf, void *opaque);


/**
 * Return topic opaque as previously set by `rd_kafka_topic_conf_set_opaque()`
 */
RD_EXPORT
void *rd_kafka_topic_opaque (rd_kafka_topic_t *rkt);


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
RD_EXPORT
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
RD_EXPORT
int rd_kafka_topic_partition_available(const rd_kafka_topic_t *rkt,
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
RD_EXPORT
int32_t rd_kafka_msg_partitioner_random(const rd_kafka_topic_t *rkt,
					 const void *key, size_t keylen,
					 int32_t partition_cnt,
					 void *opaque, void *msg_opaque);

/**
 * Consistent partitioner
 * Uses consistent hashing to map identical keys onto identical partitions.
 *
 * Returns a 'random' partition between 0 and partition_cnt - 1 based on the crc value of the key
 */
int32_t rd_kafka_msg_partitioner_consistent (const rd_kafka_topic_t *rkt,
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
 * The 'conf' object is freed by this function and must not be used or
 * destroyed by the application sub-sequently.
 * See `rd_kafka_conf_set()` et.al for more information.
 *
 * 'errstr' must be a pointer to memory of at least size 'errstr_size' where
 * `rd_kafka_new()` may write a human readable error message in case the
 * creation of a new handle fails. In which case the function returns NULL.
 *
 * Returns the Kafka handle on success or NULL on error.
 *
 * To destroy the Kafka handle, use rd_kafka_destroy().
 */
RD_EXPORT
rd_kafka_t *rd_kafka_new(rd_kafka_type_t type, rd_kafka_conf_t *conf,
			  char *errstr, size_t errstr_size);


/**
 * Destroy Kafka handle.
 * 
 */
RD_EXPORT
void        rd_kafka_destroy(rd_kafka_t *rk);



/**
 * Returns Kafka handle name.
 */
RD_EXPORT
const char *rd_kafka_name(const rd_kafka_t *rk);



/**
 * Creates a new topic handle for topic named 'topic'.
 *
 * 'conf' is an optional configuration for the topic created with
 * `rd_kafka_topic_conf_new()` that will be used instead of the default
 * topic configuration.
 * The 'conf' object is freed by this function and must not be used or
 * destroyed by the application sub-sequently.
 * See `rd_kafka_topic_conf_set()` et.al for more information.
 *
 * Returns the new topic handle or NULL on error (see `errno`).
 */
RD_EXPORT
rd_kafka_topic_t *rd_kafka_topic_new(rd_kafka_t *rk, const char *topic,
				      rd_kafka_topic_conf_t *conf);



/**
 * Destroy topic handle previously created with `rd_kafka_topic_new()`.
 */
RD_EXPORT
void rd_kafka_topic_destroy(rd_kafka_topic_t *rkt);


/**
 * Returns the topic name.
 */
RD_EXPORT
const char *rd_kafka_topic_name(const rd_kafka_topic_t *rkt);


/**
 * Unassigned partition.
 *
 * The unassigned partition is used by the producer API for messages
 * that should be partitioned using the configured or default partitioner.
 */
#define RD_KAFKA_PARTITION_UA  ((int32_t)-1)  






/*******************************************************************
 *								   *
 * Queue API                                                       *
 *								   *
 *******************************************************************/

/**
 * Create a new message queue.
 * Message queues allows the application to re-route consumed messages
 * from multiple topic+partitions into one single queue point.
 * This queue point, containing messages from a number of topic+partitions,
 * may then be served by a single rd_kafka_consume*_queue() call,
 * rather than one per topic+partition combination.
 *
 * See rd_kafka_consume_start_queue(), rd_kafka_consume_queue(), et.al.
 */
RD_EXPORT
rd_kafka_queue_t *rd_kafka_queue_new(rd_kafka_t *rk);

/**
 * Destroy a queue, purging all of its enqueued messages.
 */
RD_EXPORT
void rd_kafka_queue_destroy(rd_kafka_queue_t *rkqu);


/*******************************************************************
 *								   *
 * Simple Consumer API                                             *
 *								   *
 *******************************************************************/


#define RD_KAFKA_OFFSET_BEGINNING -2  /* Start consuming from beginning of
				       * kafka partition queue: oldest msg */
#define RD_KAFKA_OFFSET_END       -1  /* Start consuming from end of kafka
				       * partition queue: next msg */
#define RD_KAFKA_OFFSET_STORED -1000  /* Start consuming from offset retrieved
				       * from offset store */

#define RD_KAFKA_OFFSET_TAIL_BASE -2000 /* internal: do not use */

/* Start consuming `CNT` messages from topic's current `.._END` offset.
 * That is, if current end offset is 12345 and `CNT` is 200, it will start
 * consuming from offset 12345-200 = 12145. */
#define RD_KAFKA_OFFSET_TAIL(CNT)  (RD_KAFKA_OFFSET_TAIL_BASE - (CNT))

/**
 * Start consuming messages for topic 'rkt' and 'partition'
 * at offset 'offset' which may either be an absolute (0..N)
 * or one of the logical offsets:
 *  `RD_KAFKA_OFFSET_BEGINNING`, `RD_KAFKA_OFFSET_END`,
 *  `RD_KAFKA_OFFSET_STORED`, `RD_KAFKA_OFFSET_TAIL(..)`
 *
 * rdkafka will attempt to keep 'queued.min.messages' (config property)
 * messages in the local queue by repeatedly fetching batches of messages
 * from the broker until the threshold is reached.
 *
 * The application shall use one of the `rd_kafka_consume*()` functions
 * to consume messages from the local queue, each kafka message being
 * represented as a `rd_kafka_message_t *` object.
 *
 * `rd_kafka_consume_start()` must not be called multiple times for the same
 * topic and partition without stopping consumption first with
 * `rd_kafka_consume_stop()`.
 *
 * Returns 0 on success or -1 on error in which case errno is set accordingly:
 *   EBUSY    - Conflicts with an existing or previous subscription
 *              (RD_KAFKA_RESP_ERR__CONFLICT)
 *   EINVAL   - Invalid offset, or incomplete configuration (lacking group.id)
 *              (RD_KAFKA_RESP_ERR__INVALID_ARG)
 *   ESRCH    - requested 'partition' is invalid.
 *              (RD_KAFKA_RESP_ERR__UNKNOWN_PARTITION)
 *   ENOENT   - topic is unknown in the Kafka cluster.
 *              (RD_KAFKA_RESP_ERR__UNKNOWN_TOPIC)
 *
 * Use `rd_kafka_errno2err()` to convert `errno` to `rd_kafka_resp_err_t`.
 */
RD_EXPORT
int rd_kafka_consume_start(rd_kafka_topic_t *rkt, int32_t partition,
			    int64_t offset);

/**
 * Same as rd_kafka_consume_start() but re-routes incoming messages to
 * the provided queue 'rkqu' (which must have been previously allocated
 * with `rd_kafka_queue_new()`.
 * The application must use one of the `rd_kafka_consume_*_queue()` functions
 * to receive fetched messages.
 *
 * `rd_kafka_consume_start_queue()` must not be called multiple times for the
 * same topic and partition without stopping consumption first with
 * `rd_kafka_consume_stop()`.
 * `rd_kafka_consume_start()` and `rd_kafka_consume_start_queue()` must not
 * be combined for the same topic and partition.
 */
RD_EXPORT
int rd_kafka_consume_start_queue(rd_kafka_topic_t *rkt, int32_t partition,
				  int64_t offset, rd_kafka_queue_t *rkqu);

/**
 * Stop consuming messages for topic 'rkt' and 'partition', purging
 * all messages currently in the local queue.
 *
 * NOTE: To enforce synchronisation this call will block until the internal
 *       fetcher has terminated and offsets are commited to configured
 *       storage method.
 *
 * The application needs to be stop all consumers before calling
 * `rd_kafka_destroy()` on the main object handle.
 *
 * Returns 0 on success or -1 on error (see `errno`).
 */
RD_EXPORT
int rd_kafka_consume_stop(rd_kafka_topic_t *rkt, int32_t partition);



/**
 * Seek consumer for topic+partition to `offset` which is either an
 * absolute or logical offset.
 *
 * If `timeout_ms` is not 0 the call will wait this long for the
 * seek to be performed. If the timeout is reached the internal state
 * will be unknown and this function returns `RD_KAFKA_RESP_ERR__TIMED_OUT`.
 * If `timeout_ms` is 0 it will initiate the seek but return
 * immediately without any error reporting.
 *
 * This call triggers a fetch queue barrier flush.
 *
 * Returns `RD_KAFKA_RESP_ERR__NO_ERROR` on success else an error code.
 */
RD_EXPORT
rd_kafka_resp_err_t rd_kafka_seek (rd_kafka_topic_t *rkt,
                                   int32_t partition,
                                   int64_t offset,
                                   int timeout_ms);

/**
 * Consume a single message from topic 'rkt' and 'partition'.
 *
 * 'timeout_ms' is maximum amount of time to wait for a message to be received.
 * Consumer must have been previously started with `rd_kafka_consume_start()`.
 *
 * Returns a message object on success and NULL on error.
 * The message object must be destroyed with `rd_kafka_message_destroy()`
 * when the application is done with it.
 *
 * Errors (when returning NULL):
 *   ETIMEDOUT - 'timeout_ms' was reached with no new messages fetched.
 *   ENOENT    - 'rkt'+'partition' is unknown.
 *                (no prior `rd_kafka_consume_start()` call)
 *
 * NOTE: The returned message's '..->err' must be checked for errors.
 * NOTE: '..->err == RD_KAFKA_RESP_ERR__PARTITION_EOF' signals that the end
 *       of the partition has been reached, which should typically not be
 *       considered an error. The application should handle this case
 *       (e.g., ignore).
 */
RD_EXPORT
rd_kafka_message_t *rd_kafka_consume(rd_kafka_topic_t *rkt, int32_t partition,
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
 * If no messages were available within the timeout period this function
 * returns 0 and `rkmessages` remains untouched.
 * This differs somewhat from `rd_kafka_consume()`.
 *
 * The message objects must be destroyed with `rd_kafka_message_destroy()`
 * when the application is done with it.
 *
 * Returns the number of rkmessages added in 'rkmessages',
 * or -1 on error (same error codes as for `rd_kafka_consume()`.
 *
 * See: rd_kafka_consume
 */
RD_EXPORT
ssize_t rd_kafka_consume_batch(rd_kafka_topic_t *rkt, int32_t partition,
				int timeout_ms,
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
 * the application must NOT call `rd_kafka_message_destroy()` on the provided
 * 'rkmessage'.
 *
 * The 'opaque' argument is passed to the 'consume_cb' as 'opaque'.
 *
 * Returns the number of messages processed or -1 on error.
 *
 * See: rd_kafka_consume
 */
RD_EXPORT
int rd_kafka_consume_callback(rd_kafka_topic_t *rkt, int32_t partition,
			       int timeout_ms,
			       void (*consume_cb) (rd_kafka_message_t
						   *rkmessage,
						   void *opaque),
			       void *opaque);


/**
 * Queue consumers
 *
 * The following `..._queue()` functions are analogue to the functions above
 * but reads messages from the provided queue `rkqu` instead.
 * `rkqu` must have been previously created with `rd_kafka_queue_new()`
 * and the topic consumer must have been started with
 * `rd_kafka_consume_start_queue()` utilising the the same queue.
 */

/**
 * See `rd_kafka_consume()` above.
 */
RD_EXPORT
rd_kafka_message_t *rd_kafka_consume_queue(rd_kafka_queue_t *rkqu,
					    int timeout_ms);

/**
 * See `rd_kafka_consume_batch()` above.
 */
RD_EXPORT
ssize_t rd_kafka_consume_batch_queue(rd_kafka_queue_t *rkqu,
				      int timeout_ms,
				      rd_kafka_message_t **rkmessages,
				      size_t rkmessages_size);

/**
 * See `rd_kafka_consume_callback()` above.
 */
RD_EXPORT
int rd_kafka_consume_callback_queue(rd_kafka_queue_t *rkqu,
				     int timeout_ms,
				     void (*consume_cb) (rd_kafka_message_t
							 *rkmessage,
							 void *opaque),
				     void *opaque);






/**
 * Topic+partition offset store.
 *
 * If auto.commit.enable is true the offset is stored automatically prior to
 * returning of the message(s) in each of the rd_kafka_consume*() functions
 * above.
 */


/**
 * Store offset 'offset' for topic 'rkt' partition 'partition'.
 * The offset will be commited (written) to the offset store according
 * to `auto.commit.interval.ms`.
 *
 * NOTE: `auto.commit.enable` must be set to "false" when using this API.
 *
 * Returns RD_KAFKA_RESP_ERR_NO_ERROR on success or an error code on error.
 */
RD_EXPORT
rd_kafka_resp_err_t rd_kafka_offset_store(rd_kafka_topic_t *rkt,
					   int32_t partition, int64_t offset);




/*******************************************************************
 *								   *
 * High-level Consumer API                                         *
 *								   *
 *******************************************************************/

typedef struct rd_kafka_consumer_s rd_kafka_consumer_t;
/* By config:
 *   ConsumerRebalanceCallback
 *   KeyDeserializer
 *   ValueDeserializer
 */
RD_EXPORT
rd_kafka_resp_err_t rd_kafka_subscribe_partition (rd_kafka_t *rk,
                                                  const char *topic,
                                                  int32_t partition);


RD_EXPORT rd_kafka_resp_err_t
rd_kafka_subscribe (rd_kafka_t *rk,
                    const rd_kafka_topic_partition_list_t *topics);

RD_EXPORT
rd_kafka_resp_err_t rd_kafka_unsubscribe (rd_kafka_t *rk);


RD_EXPORT
rd_kafka_resp_err_t rd_kafka_unsubscribe_partition(rd_kafka_t *rk,
const char *topic, int32_t partition);

RD_EXPORT
rd_kafka_message_t *rd_kafka_consumer_poll (rd_kafka_t *rk, int timeout_ms);

RD_EXPORT
rd_kafka_resp_err_t rd_kafka_consumer_close (rd_kafka_t *rk);

RD_EXPORT
rd_kafka_resp_err_t rd_kafka_consumer_get_offset (rd_kafka_topic_t *rkt,
                                                  int32_t partition,
                                                  int64_t *offsetp,
                                                  int timeout_ms);

RD_EXPORT rd_kafka_resp_err_t
rd_kafka_assign (rd_kafka_t *rk,
                 const rd_kafka_topic_partition_list_t *partitions);

RD_EXPORT rd_kafka_resp_err_t
rd_kafka_assignment (rd_kafka_t *rk,
                     rd_kafka_topic_partition_list_t **partitions);

RD_EXPORT rd_kafka_resp_err_t
rd_kafka_subscription (rd_kafka_t *rk,
                       rd_kafka_topic_partition_list_t **topics);


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
 *    If the function returns -1 and RD_KAFKA_MSG_F_FREE was specified, then
 *    the memory associated with the payload is still the caller's
 *    responsibility.
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
 * Returns 0 on success or -1 on error in which case errno is set accordingly:
 *   ENOBUFS  - maximum number of outstanding messages has been reached:
 *              "queue.buffering.max.messages"
 *              (RD_KAFKA_RESP_ERR__QUEUE_FULL)
 *   EMSGSIZE - message is larger than configured max size:
 *              "messages.max.bytes".
 *              (RD_KAFKA_RESP_ERR_MSG_SIZE_TOO_LARGE)
 *   ESRCH    - requested 'partition' is unknown in the Kafka cluster.
 *              (RD_KAFKA_RESP_ERR__UNKNOWN_PARTITION)
 *   ENOENT   - topic is unknown in the Kafka cluster.
 *              (RD_KAFKA_RESP_ERR__UNKNOWN_TOPIC)
 *
 * NOTE: Use `rd_kafka_errno2err()` to convert `errno` to rdkafka error code.
 */

#define RD_KAFKA_MSG_F_FREE  0x1  /* Delegate freeing of payload to rdkafka. */
#define RD_KAFKA_MSG_F_COPY  0x2  /* rdkafka will make a copy of the payload. */

RD_EXPORT
int rd_kafka_produce(rd_kafka_topic_t *rkt, int32_t partitition,
		      int msgflags,
		      void *payload, size_t len,
		      const void *key, size_t keylen,
		      void *msg_opaque);



/**
 * Produce multiple messages.
 *
 * If partition is RD_KAFKA_PARTITION_UA the configured partitioner will
 * be run for each message (slower), otherwise the messages will be enqueued
 * to the specified partition directly (faster).
 *
 * The messages are provided in the array `rkmessages` of count `message_cnt`
 * elements.
 * The `partition` and `msgflags` are used for all provided messages.
 *
 * Honoured `rkmessages[]` fields are:
 *   payload,len     - Message payload and length
 *   key,key_len     - Optional message key
 *   _private        - Message opaque pointer (msg_opaque)
 *   err             - Will be set according to success or failure.
 *                     Application only needs to check for errors if
 *                     return value != `message_cnt`.
 *
 * Returns the number of messages succesfully enqueued for producing.
 */
RD_EXPORT
int rd_kafka_produce_batch(rd_kafka_topic_t *rkt, int32_t partition,
                            int msgflags,
                            rd_kafka_message_t *rkmessages, int message_cnt);




/*******************************************************************
 *								   *
 * Metadata API                                                    *
 *								   *
 *******************************************************************/


/**
 * Metadata: Broker information
 */
typedef struct rd_kafka_metadata_broker {
        int32_t     id;             /* Broker Id */
        char       *host;           /* Broker hostname */
        int         port;           /* Broker listening port */
} rd_kafka_metadata_broker_t;

/**
 * Metadata: Partition information
 */
typedef struct rd_kafka_metadata_partition {
        int32_t     id;             /* Partition Id */
        rd_kafka_resp_err_t err;    /* Partition error reported by broker */
        int32_t     leader;         /* Leader broker */
        int         replica_cnt;    /* Number of brokers in 'replicas' */
        int32_t    *replicas;       /* Replica brokers */
        int         isr_cnt;        /* Number of ISR brokers in 'isrs' */
        int32_t    *isrs;           /* In-Sync-Replica brokers */
} rd_kafka_metadata_partition_t;

/**
 * Metadata: Topic information
 */
typedef struct rd_kafka_metadata_topic {
        char       *topic;          /* Topic name */
        int         partition_cnt;  /* Number of partitions in 'partitions' */
        struct rd_kafka_metadata_partition *partitions; /* Partitions */
        rd_kafka_resp_err_t err;    /* Topic error reported by broker */
} rd_kafka_metadata_topic_t;


/**
 * Metadata container
 */
typedef struct rd_kafka_metadata {
        int         broker_cnt;     /* Number of brokers in 'brokers' */
        struct rd_kafka_metadata_broker *brokers;  /* Brokers */

        int         topic_cnt;      /* Number of topics in 'topics' */
        struct rd_kafka_metadata_topic *topics;    /* Topics */

        int32_t     orig_broker_id; /* Broker originating this metadata */
        char       *orig_broker_name; /* Name of originating broker */
} rd_kafka_metadata_t;


/**
 * Request Metadata from broker.
 *
 *  all_topics - if non-zero: request info about all topics in cluster,
 *               if zero: only request info about locally known topics.
 *  only_rkt   - only request info about this topic
 *  metadatap  - pointer to hold metadata result.
 *               The '*metadatap' pointer must be released
 *               with rd_kafka_metadata_destroy().
 *  timeout_ms - maximum response time before failing.
 *
 * Returns RD_KAFKA_RESP_ERR_NO_ERROR on success (in which case *metadatap)
 * will be set, else RD_KAFKA_RESP_ERR__TIMED_OUT on timeout or
 * other error code on error.
 */
RD_EXPORT
rd_kafka_resp_err_t
rd_kafka_metadata (rd_kafka_t *rk, int all_topics,
                   rd_kafka_topic_t *only_rkt,
                   const struct rd_kafka_metadata **metadatap,
                   int timeout_ms);

/**
 * Release metadata memory.
 */
RD_EXPORT
void rd_kafka_metadata_destroy(const struct rd_kafka_metadata *metadata);








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
 *   - stats callbacks (if stats_cb is configured) [producer & consumer]
 *
 * Returns the number of events served.
 */
RD_EXPORT
int rd_kafka_poll(rd_kafka_t *rk, int timeout_ms);


/**
 * Cancels the current callback dispatcher (rd_kafka_poll(),
 * rd_kafka_consume_callback(), etc).
 *
 * A callback may use this to force an immediate return to the calling
 * code (caller of e.g. rd_kafka_poll()) without processing any further
 * events.
 *
 * NOTE: This function MUST ONLY be called from within a librdkafka callback.
 */
RD_EXPORT
void rd_kafka_yield (rd_kafka_t *rk);

/**
 * Adds one or more brokers to the kafka handle's list of initial brokers.
 * Additional brokers will be discovered automatically as soon as rdkafka
 * connects to a broker by querying the broker metadata.
 *
 * If a broker name resolves to multiple addresses (and possibly
 * address families) all will be used for connection attempts in
 * round-robin fashion.
 *
 * 'brokerlist' is a ,-separated list of brokers in the format:
 *   <broker1>,<broker2>,..
 * Where each broker is in either the host or URL based format:
 *   <host>[:<port>]
 *   <proto>://<host>[:port]
 * <proto> is either PLAINTEXT or SSL.
 * The two formats can be mixed but ultimately the value of the
 * `security.protocol` config property decides what brokers are allowed.
 *
 * Example:
 *    brokerlist = "broker1:10000,broker2"
 *    brokerlist = "SSL://broker3:9000,broker1:10000,ssl://broker2"
 *
 * Returns the number of brokers successfully added.
 *
 * NOTE: Brokers may also be defined with the 'metadata.broker.list'
 *       configuration property.
 */
RD_EXPORT
int rd_kafka_brokers_add(rd_kafka_t *rk, const char *brokerlist);




/**
 * Set logger function.
 * The default is to print to stderr, but a syslog logger is also available,
 * see rd_kafka_log_(print|syslog) for the builtin alternatives.
 * Alternatively the application may provide its own logger callback.
 * Or pass 'func' as NULL to disable logging.
 *
 * DEPRECATED, use rd_kafka_conf_set_log_cb()
 *
 * NOTE: 'rk' may be passed as NULL in the callback.
 */
RD_EXPORT RD_DEPRECATED
void rd_kafka_set_logger(rd_kafka_t *rk,
			  void (*func) (const rd_kafka_t *rk, int level,
					const char *fac, const char *buf));


/**
 * Specifies the maximum logging level produced by
 * internal kafka logging and debugging.
 * If the 'debug' configuration property is set the level is automatically
 * adjusted to LOG_DEBUG (7).
 */
RD_EXPORT
void rd_kafka_set_log_level(rd_kafka_t *rk, int level);


/**
 * Builtin (default) log sink: print to stderr
 */
RD_EXPORT
void rd_kafka_log_print(const rd_kafka_t *rk, int level,
			 const char *fac, const char *buf);


/**
 * Builtin log sink: print to syslog.
 */
RD_EXPORT
void rd_kafka_log_syslog(const rd_kafka_t *rk, int level,
			  const char *fac, const char *buf);


/**
 * Returns the current out queue length:
 * messages waiting to be sent to, or acknowledged by, the broker.
 */
RD_EXPORT
int         rd_kafka_outq_len(rd_kafka_t *rk);



/**
 * Dumps rdkafka's internal state for handle 'rk' to stream 'fp'
 * This is only useful for debugging rdkafka, showing state and statistics
 * for brokers, topics, partitions, etc.
 */
RD_EXPORT
void rd_kafka_dump(FILE *fp, rd_kafka_t *rk);



/**
 * Retrieve the current number of threads in use by librdkafka.
 * Used by regression tests.
 */
RD_EXPORT
int rd_kafka_thread_cnt(void);


/**
 * Wait for all rd_kafka_t objects to be destroyed.
 * Returns 0 if all kafka objects are now destroyed, or -1 if the
 * timeout was reached.
 * Since `rd_kafka_destroy()` is an asynch operation the 
 * `rd_kafka_wait_destroyed()` function can be used for applications where
 * a clean shutdown is required.
 */
RD_EXPORT
int rd_kafka_wait_destroyed(int timeout_ms);







/* FIXME */
RD_EXPORT
rd_kafka_topic_partition_t *rd_kafka_topic_partition_new (const char *topic,
                                                          int32_t partition);

RD_EXPORT
void rd_kafka_topic_partition_destroy (rd_kafka_topic_partition_t *rktpar);



#ifdef __cplusplus
}
#endif
