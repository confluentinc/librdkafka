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

#include <string.h>
#include <stdlib.h>
#include <ctype.h>
#include <stddef.h>

#include "rdkafka.h"
#include "rdkafka_int.h"
#include "rd.h"


struct rd_kafka_property {
	rd_kafka_conf_scope_t scope;
	const char *name;
	enum {
		_RK_C_STR,
		_RK_C_INT,
		_RK_C_S2I,  /* String to Integer mapping */
		_RK_C_S2F,  /* CSV String to Integer flag mapping (OR:ed) */
		_RK_C_BOOL,
		_RK_C_PTR,  /* Only settable through special set functions */
	} type;
	int   offset;
	const char *desc;
	int   vmin;
	int   vmax;
	int   vdef;        /* Default value (int) */
	const char *sdef;  /* Defalut value (string) */
	struct {
		int val;
		const char *str;
	} s2i[10];  /* _RK_C_S2I and _RK_C_S2F */
};


#define _RK(field)  offsetof(rd_kafka_conf_t, field)
#define _RKT(field) offsetof(rd_kafka_topic_conf_t, field)


/**
 * librdkafka configuration property definitions.
 */
static const struct rd_kafka_property rd_kafka_properties[] = {
	/* Global properties */
	{ _RK_GLOBAL, "client.id", _RK_C_STR, _RK(clientid),
	  "Client identifier.",
	  .sdef =  "rdkafka" },
	{ _RK_GLOBAL, "metadata.broker.list", _RK_C_STR, _RK(brokerlist),
	  "Initial list of brokers. "
	  "The application may also use `rd_kafka_brokers_add()` to add "
	  "brokers during runtime." },
	{ _RK_GLOBAL, "message.max.bytes", _RK_C_INT, _RK(max_msg_size),
	  "Maximum transmit message size.",
	  1000, 1000000000, 4000000 },
	{ _RK_GLOBAL, "receive.message.max.bytes", _RK_C_INT,
          _RK(recv_max_msg_size),
	  "Maximum receive message size. "
	  "This is a safety precaution to avoid memory exhaustion in case of "
	  "protocol hickups. "
          "The value should be at least fetch.message.max.bytes * number of "
          "partitions consumed from.",
	  1000, 1000000000, 100000000 },
	{ _RK_GLOBAL, "metadata.request.timeout.ms", _RK_C_INT,
	  _RK(metadata_request_timeout_ms),
	  "Non-topic request timeout in milliseconds. "
	  "This is for metadata requests, etc.", /* FIXME: OffsetReq? */
	  10, 900*1000, 60*1000},
	{ _RK_GLOBAL, "topic.metadata.refresh.interval.ms", _RK_C_INT,
	  _RK(metadata_refresh_interval_ms),
	  "Topic metadata refresh interval in milliseconds. "
	  "The metadata is automatically refreshed on error and connect. "
	  "Use -1 to disable the intervalled refresh.",
	  -1, 3600*1000, 10*1000 },
	{ _RK_GLOBAL, "topic.metadata.refresh.fast.cnt", _RK_C_INT,
	  _RK(metadata_refresh_fast_cnt),
	  "When a topic looses its leader this number of metadata requests "
	  "are sent with `topic.metadata.refresh.fast.interval.ms` interval "
	  "disregarding the `topic.metadata.refresh.interval.ms` value. "
	  "This is used to recover quickly from transitioning leader brokers.",
	  0, 1000, 10 },
	{ _RK_GLOBAL, "topic.metadata.refresh.fast.interval.ms", _RK_C_INT,
	  _RK(metadata_refresh_fast_interval_ms),
	  "See `topic.metadata.refresh.fast.cnt` description",
	  1, 60*1000, 250 },
	{ _RK_GLOBAL, "debug", _RK_C_S2F, _RK(debug),
	  "A comma-separated list of debug contexts to enable: "
	  RD_KAFKA_DEBUG_CONTEXTS,
	  .s2i = {
			{ RD_KAFKA_DBG_GENERIC,  "generic" },
			{ RD_KAFKA_DBG_BROKER,   "broker" },
			{ RD_KAFKA_DBG_TOPIC,    "topic" },
			{ RD_KAFKA_DBG_METADATA, "metadata" },
			{ RD_KAFKA_DBG_PRODUCER, "producer" },
			{ RD_KAFKA_DBG_QUEUE,    "queue" },
			{ RD_KAFKA_DBG_MSG,      "msg" },
			{ RD_KAFKA_DBG_ALL,      "all" },
		} },
	{ _RK_GLOBAL, "socket.timeout.ms", _RK_C_INT, _RK(socket_timeout_ms),
	  "Timeout for network requests.",
	  10, 300*1000, 60*1000 },
	{ _RK_GLOBAL, "socket.send.buffer.bytes", _RK_C_INT,
	  _RK(socket_sndbuf_size),
	  "Broker socket send buffer size. System default is used if 0.",
	  0, 100000000, 0 },
	{ _RK_GLOBAL, "socket.receive.buffer.bytes", _RK_C_INT,
	  _RK(socket_rcvbuf_size),
	  "Broker socket receive buffer size. System default is used if 0.",
	  0, 100000000, 0 },
	{ _RK_GLOBAL, "broker.address.ttl", _RK_C_INT,
	  _RK(broker_addr_ttl),
	  "How long to cache the broker address resolving results.",
	  0, 86400*1000, 300*1000 },
	{ _RK_GLOBAL, "statistics.interval.ms", _RK_C_INT,
	  _RK(stats_interval_ms),
	  "librdkafka statistics emit interval. The application also needs to "
	  "register a stats callback using `rd_kafka_conf_set_stats_cb()`. "
	  "The granularity is 1000ms. A value of 0 disables statistics.",
	  0, 86400*1000, 0 },
	{ _RK_GLOBAL, "error_cb", _RK_C_PTR,
	  _RK(error_cb),
	  "Error callback (set with rd_kafka_conf_set_error_cb())" },
	{ _RK_GLOBAL, "stats_cb", _RK_C_PTR,
	  _RK(stats_cb),
	  "Statistics callback (set with rd_kafka_conf_set_stats_cb())" },
	{ _RK_GLOBAL, "opaque", _RK_C_PTR,
	  _RK(opaque),
	  "Application opaque (set with rd_kafka_conf_set_opaque())" },

	/* Global consumer properties */
	{ _RK_GLOBAL|_RK_CONSUMER, "queued.min.messages", _RK_C_INT,
	  _RK(queued_min_msgs),
	  "Minimum number of messages that should to be available "
	  "for consumption by application.",
	  1, 10000000, 100000 },
	{ _RK_GLOBAL|_RK_CONSUMER, "fetch.wait.max.ms", _RK_C_INT,
	  _RK(fetch_wait_max_ms),
	  "Maximum time the broker may wait to fill the response "
	  "with fetch.min.bytes.",
	  0, 300*1000, 100 },
        { _RK_GLOBAL|_RK_CONSUMER, "fetch.message.max.bytes", _RK_C_INT,
          _RK(fetch_msg_max_bytes),
          "Maximum number of bytes per topic+partition to request when "
          "fetching messages from the broker.",
          1, 1000000000, 1024*1024 },
	{ _RK_GLOBAL|_RK_CONSUMER, "fetch.min.bytes", _RK_C_INT,
	  _RK(fetch_min_bytes),
	  "Minimum number of bytes the broker responds with. "
	  "If fetch.wait.max.ms expires the accumulated data will "
	  "be sent to the client regardless of this setting.",
	  1, 100000000, 1 },
	{ _RK_GLOBAL|_RK_CONSUMER, "fetch.error.backoff.ms", _RK_C_INT,
	  _RK(fetch_error_backoff_ms),
	  "How long to postpone the next fetch request for a "
	  "topic+partition in case of a fetch error.",
	  1, 300*1000, 500 },


	/* Global producer properties */
	{ _RK_GLOBAL|_RK_PRODUCER, "queue.buffering.max.messages", _RK_C_INT,
	  _RK(queue_buffering_max_msgs),
	  "Maximum number of messages allowed on the producer queue.",
	  1, 10000000, 100000 },
	{ _RK_GLOBAL|_RK_PRODUCER, "queue.buffering.max.ms", _RK_C_INT,
	  _RK(buffering_max_ms),
	  "Maximum time, in milliseconds, for buffering data "
	  "on the producer queue.",
	  1, 900*1000, 1000 },
	{ _RK_GLOBAL|_RK_PRODUCER, "message.send.max.retries", _RK_C_INT,
	  _RK(max_retries),
	  "How many times to retry sending a failing MessageSet. "
	  "**Note:** retrying may cause reordering.",
	  0, 100, 2 },
	{ _RK_GLOBAL|_RK_PRODUCER, "retry.backoff.ms", _RK_C_INT,
	  _RK(retry_backoff_ms),
	  "The backoff time in milliseconds before retrying a message send.",
	  1, 300*1000, 100 },
	{ _RK_GLOBAL|_RK_PRODUCER, "compression.codec", _RK_C_S2I,
	  _RK(compression_codec),
	  "Compression codec to use for compressing message sets: "
	  "none, gzip or snappy",
	  .vdef = RD_KAFKA_COMPRESSION_NONE,
	  .s2i = {
			{ RD_KAFKA_COMPRESSION_NONE,   "none" },
			{ RD_KAFKA_COMPRESSION_GZIP,   "gzip" },
			{ RD_KAFKA_COMPRESSION_SNAPPY, "snappy" },
		} },
	{ _RK_GLOBAL|_RK_PRODUCER, "batch.num.messages", _RK_C_INT,
	  _RK(batch_num_messages),
	  "Maximum number of messages batched in one MessageSet.",
	  1, 1000000, 1000 },
	{ _RK_GLOBAL|_RK_PRODUCER, "dr_cb", _RK_C_PTR,
	  _RK(dr_cb),
	  "Delivery report callback (set with rd_kafka_conf_set_dr_cb())" },
	
	  

	/* Topic properties */
	{ _RK_TOPIC|_RK_PRODUCER, "request.required.acks", _RK_C_INT,
	  _RKT(required_acks),
	  "This field indicates how many acknowledgements the leader broker "
	  "must receive from ISR brokers before responding to the request: "
	  "*0*=broker does not send any response, "
	  "*1*=broker will wait until the data is written to local "
	  "log before sending a response, "
	  "*-1*=broker will block until message is committed by all "
	  "in sync replicas (ISRs) before sending response. "
	  "*>1*=for any number > 1 the broker will block waiting for this "
	  "number of acknowledgements to be received (but the broker "
	  "will never wait for more acknowledgements than there are ISRs).",
	  -1, 1000, 1 },

	{ _RK_TOPIC|_RK_PRODUCER, "request.timeout.ms", _RK_C_INT,
	  _RKT(request_timeout_ms),
	  "The ack timeout of the producer request in milliseconds. "
	  "This value is only enforced by the broker and relies "
	  "on `request.required.acks` being > 0.",
	  1, 900*1000, 5*1000 },
	{ _RK_TOPIC|_RK_PRODUCER, "message.timeout.ms", _RK_C_INT,
	  _RKT(message_timeout_ms),
	  "Local message timeout. "
	  "This value is only enforced locally and limits the time a "
	  "produced message waits for successful delivery.",
	  1, 900*1000, 300*1000 },
	{ _RK_TOPIC|_RK_PRODUCER, "partitioner", _RK_C_PTR,
	  _RKT(partitioner),
	  "Partitioner callback "
	  "(set with rd_kafka_topic_conf_set_partitioner_cb())" },
	{ _RK_TOPIC, "opaque", _RK_C_PTR,
	  _RKT(opaque),
	  "Application opaque (set with rd_kafka_topic_conf_set_opaque())" },


	{ _RK_TOPIC|_RK_CONSUMER, "auto.commit.enable", _RK_C_BOOL,
	  _RKT(auto_commit),
	  "If true, periodically commit offset of the last message handed "
	  "to the application. This commited offset will be used when the "
	  "process restarts to pick up where it left off. "
	  "If false, the application will have to call "
	  "`rd_kafka_offset_store()` to store an offset (optional). "
	  "**NOTE:** There is currently no zookeeper integration, offsets "
	  "will be written to local file according to offset.store.path.",
	  0, 1, 1 },
	{ _RK_TOPIC|_RK_CONSUMER, "auto.commit.interval.ms", _RK_C_INT,
	  _RKT(auto_commit_interval_ms),
	  "The frequency in milliseconds that the consumer offsets "
	  "are commited (written) to offset storage.",
	  10, 86400*1000, 60*1000 },
	{ _RK_TOPIC|_RK_CONSUMER, "auto.offset.reset", _RK_C_S2I,
	  _RKT(auto_offset_reset),
	  "Action to take when there is no initial offset in offset store "
	  "or the desired offset is out of range: "
	  "'smallest' - automatically reset the offset to the smallest offset, "
	  "'largest' - automatically reset the offset to the largest offset, "
	  "'error' - trigger an error which is retrieved by consuming messages "
	  "and checking 'message->err'.",
	  .vdef = RD_KAFKA_OFFSET_END,
	  .s2i = {
			{ RD_KAFKA_OFFSET_BEGINNING, "smallest" },
			{ RD_KAFKA_OFFSET_END, "largest" },
			{ RD_KAFKA_OFFSET_ERROR, "error" },
		}
	},
	{ _RK_TOPIC|_RK_CONSUMER, "offset.store.path", _RK_C_STR,
	  _RKT(offset_store_path),
	  "Path to local file for storing offsets. If the path is a directory "
	  "a filename will be automatically generated in that directory based "
	  "on the topic and partition.",
	  .sdef = "." },

	{ _RK_TOPIC|_RK_CONSUMER, "offset.store.sync.interval.ms", _RK_C_INT,
	  _RKT(offset_store_sync_interval_ms),
	  "fsync() interval for the offset file, in milliseconds. "
	  "Use -1 to disable syncing, and 0 for immediate sync after "
	  "each write.",
	  -1, 86400*1000, -1 },

	{ /* End */ }
};



static rd_kafka_conf_res_t
rd_kafka_anyconf_set_prop0 (int scope, void *conf,
			    const struct rd_kafka_property *prop,
			    const char *istr, int ival) {
#define _RK_PTR(TYPE,BASE,OFFSET)  (TYPE)(((char *)(BASE))+(OFFSET))
	switch (prop->type)
	{
	case _RK_C_STR:
	{
		char **str = _RK_PTR(char **, conf, prop->offset);
		if (*str)
			free(*str);
		if (istr)
			*str = strdup(istr);
		else
			*str = NULL;
		return RD_KAFKA_CONF_OK;
	}
	case _RK_C_PTR:
		*_RK_PTR(const void **, conf, prop->offset) = istr;
		return RD_KAFKA_CONF_OK;
	case _RK_C_BOOL:
	case _RK_C_INT:
	case _RK_C_S2I:
	case _RK_C_S2F:
	{
		int *val = _RK_PTR(int *, conf, prop->offset);

		if (prop->type == _RK_C_S2F) {
			/* Flags: OR it in */
			*val |= ival;
		} else {
			/* Single assignment */
			*val = ival;

		}
		
		return RD_KAFKA_CONF_OK;
	}
	default:
		assert(!*"unknown conf type");
	}

	/* unreachable */
	return RD_KAFKA_CONF_INVALID;
}

static rd_kafka_conf_res_t
rd_kafka_anyconf_set_prop (int scope, void *conf,
			   const struct rd_kafka_property *prop,
			   const char *value,
			   char *errstr, size_t errstr_size) {
	int ival;

	switch (prop->type)
	{
	case _RK_C_STR:
		rd_kafka_anyconf_set_prop0(scope, conf, prop, value, 0);
		return RD_KAFKA_CONF_OK;

	case _RK_C_PTR:
		snprintf(errstr, errstr_size,
			 "Property \"%s\" must be set through dedicated "
			 ".._set_..() function", prop->name);
		return RD_KAFKA_CONF_INVALID;

	case _RK_C_BOOL:
		if (!value) {
			snprintf(errstr, errstr_size,
				 "Bool configuration property \"%s\" cannot "
				 "be set to empty value", prop->name);
			return RD_KAFKA_CONF_INVALID;
		}

		if (!strcasecmp(value, "true") ||
		    !strcasecmp(value, "t") ||
		    !strcmp(value, "1"))
			ival = 1;
		else if (!strcasecmp(value, "false") ||
			 !strcasecmp(value, "f") ||
			 !strcmp(value, "0"))
			ival = 0;
		else {
			snprintf(errstr, errstr_size,
				 "Expected bool value for \"%s\": "
				 "true or false", prop->name);
			return RD_KAFKA_CONF_INVALID;
		}

		rd_kafka_anyconf_set_prop0(scope, conf, prop, NULL, ival);
		return RD_KAFKA_CONF_OK;

	case _RK_C_INT:
		if (!value) {
			snprintf(errstr, errstr_size,
				 "Integer configuration "
				 "property \"%s\" cannot be set "
				 "to empty value", prop->name);
			return RD_KAFKA_CONF_INVALID;
		}

		ival = atoi(value);
		if (ival < prop->vmin ||
		    ival > prop->vmax) {
			snprintf(errstr, errstr_size,
				 "Configuration property \"%s\" value "
				 "%i is outside allowed range %i..%i\n",
				 prop->name, ival,
				 prop->vmin,
				 prop->vmax);
			return RD_KAFKA_CONF_INVALID;
		}

		rd_kafka_anyconf_set_prop0(scope, conf, prop, NULL, ival);
		return RD_KAFKA_CONF_OK;

	case _RK_C_S2I:
	case _RK_C_S2F:
	{
		int j;
		const char *next;

		if (!value) {
			snprintf(errstr, errstr_size,
				 "Configuration "
				 "property \"%s\" cannot be set "
				 "to empty value", prop->name);
			return RD_KAFKA_CONF_INVALID;
		}
			

		next = value;
		while (next && *next) {
			const char *s, *t;

			s = next;

			if (prop->type == _RK_C_S2F &&
			    (t = strchr(s, ','))) {
				/* CSV flag field */
				next = t+1;
			} else {
				/* Single string */
				t = s+strlen(s);
				next = NULL;
			}


			/* Left trim */
			while (s < t && isspace((int)*s))
				s++;

			/* Right trim */
			while (t > s && isspace((int)*t))
				t--;

			/* Empty string? */
			if (s == t)
				continue;

			/* Match string to s2i table entry */
			for (j = 0 ; j < RD_ARRAYSIZE(prop->s2i); j++) {
				if (!prop->s2i[j].str ||
				    strlen(prop->s2i[j].str) != (int)(t-s) ||
				    strncmp(prop->s2i[j].str, s, (int)(t-s)))
					continue;

				rd_kafka_anyconf_set_prop0(scope, conf, prop,
							   NULL,
							   prop->s2i[j].val);

				if (prop->type == _RK_C_S2F) {
					/* Flags: OR it in: do next */
					break;
				} else {
					/* Single assignment */
					return RD_KAFKA_CONF_OK;
				}
			}
				
			/* S2F: Good match: continue with next */
			if (j < RD_ARRAYSIZE(prop->s2i))
				continue;

			/* No match */
			snprintf(errstr, errstr_size,
				 "Invalid value for "
				 "configuration property \"%s\"", prop->name);
			return RD_KAFKA_CONF_INVALID;

		}
		return RD_KAFKA_CONF_OK;
	}

	default:
		assert(!*"unknown conf type");
	}

	/* not reachable */
	return RD_KAFKA_CONF_INVALID;
}



static void rd_kafka_defaultconf_set (int scope, void *conf) {
	const struct rd_kafka_property *prop;

	for (prop = rd_kafka_properties ; prop->name ; prop++) {
		if (!(prop->scope & scope))
			continue;

		if (prop->sdef || prop->vdef)
			rd_kafka_anyconf_set_prop0(scope, conf, prop,
						   prop->sdef, prop->vdef);
	}
}

rd_kafka_conf_t *rd_kafka_conf_new (void) {
	rd_kafka_conf_t *conf = calloc(1, sizeof(*conf));
	rd_kafka_defaultconf_set(_RK_GLOBAL, conf);
	return conf;
}

rd_kafka_topic_conf_t *rd_kafka_topic_conf_new (void) {
	rd_kafka_topic_conf_t *tconf = calloc(1, sizeof(*tconf));
	rd_kafka_defaultconf_set(_RK_TOPIC, tconf);
	return tconf;
}



static int rd_kafka_anyconf_set (int scope, void *conf,
				 const char *name, const char *value,
				 char *errstr, size_t errstr_size) {
	char estmp[1];
	const struct rd_kafka_property *prop;

	if (!errstr) {
		errstr = estmp;
		errstr_size = 0;
	}

	if (value && !*value)
		value = NULL;

	for (prop = rd_kafka_properties ; prop->name ; prop++) {

		if (!(prop->scope & scope))
			continue;

		if (strcmp(prop->name, name))
			continue;

		return rd_kafka_anyconf_set_prop(scope, conf, prop, value,
						 errstr, errstr_size);
	}

	snprintf(errstr, errstr_size,
		 "No such configuration property: \"%s\"", name);

	return RD_KAFKA_CONF_UNKNOWN;
}


rd_kafka_conf_res_t rd_kafka_conf_set (rd_kafka_conf_t *conf,
				       const char *name,
				       const char *value,
				       char *errstr, size_t errstr_size) {
	return rd_kafka_anyconf_set(_RK_GLOBAL, conf, name, value,
				    errstr, errstr_size);
}


rd_kafka_conf_res_t rd_kafka_topic_conf_set (rd_kafka_topic_conf_t *conf,
					     const char *name,
					     const char *value,
					     char *errstr, size_t errstr_size) {
	if (!strncmp(name, "topic.", strlen("topic.")))
		name += strlen("topic.");

	return rd_kafka_anyconf_set(_RK_TOPIC, conf, name, value,
				    errstr, errstr_size);
}


static void rd_kafka_anyconf_clear (void *conf,
				    const struct rd_kafka_property *prop) {
	switch (prop->type)
	{
	case _RK_C_STR:
	{
		char **str = _RK_PTR(char **, conf, prop->offset);
		if (*str) {
			free(*str);
			*str = NULL;
		}
	}
	break;

	default:
		break;
	}

}

void rd_kafka_anyconf_destroy (int scope, void *conf) {
	const struct rd_kafka_property *prop;

	for (prop = rd_kafka_properties; prop->name ; prop++) {
		if (!(prop->scope & scope))
			continue;

		rd_kafka_anyconf_clear(conf, prop);
	}
}


void rd_kafka_conf_destroy (rd_kafka_conf_t *conf) {
	rd_kafka_anyconf_destroy(_RK_GLOBAL, conf);
	free(conf);
}

	
void rd_kafka_topic_conf_destroy (rd_kafka_topic_conf_t *topic_conf) {
	rd_kafka_anyconf_destroy(_RK_TOPIC, topic_conf);
	free(topic_conf);
}



static void rd_kafka_anyconf_copy (int scope, void *dst, const void *src) {
	const struct rd_kafka_property *prop;

	for (prop = rd_kafka_properties ; prop->name ; prop++) {
		const char *val = NULL;
		int ival = 0;

		if (!(prop->scope & scope))
			continue;

		switch (prop->type)
		{
		case _RK_C_STR:
		case _RK_C_PTR:
			val = *_RK_PTR(const char **, src, prop->offset);
			break;

		case _RK_C_BOOL:
		case _RK_C_INT:
		case _RK_C_S2I:
		case _RK_C_S2F:
			ival = *_RK_PTR(const int *, src, prop->offset);
			break;

		default:
			continue;
		}

		rd_kafka_anyconf_set_prop0(scope, dst, prop, val, ival);
	}
}


rd_kafka_conf_t *rd_kafka_conf_dup (const rd_kafka_conf_t *conf) {
	rd_kafka_conf_t *new = rd_kafka_conf_new();

	rd_kafka_anyconf_copy(_RK_GLOBAL, new, conf);

	return new;
}


rd_kafka_topic_conf_t *rd_kafka_topic_conf_dup (const rd_kafka_topic_conf_t
						*conf) {
	rd_kafka_topic_conf_t *new = rd_kafka_topic_conf_new();

	rd_kafka_anyconf_copy(_RK_TOPIC, new, conf);

	return new;
}


void rd_kafka_conf_set_dr_cb (rd_kafka_conf_t *conf,
			      void (*dr_cb) (rd_kafka_t *rk,
					     void *payload, size_t len,
					     rd_kafka_resp_err_t err,
					     void *opaque, void *msg_opaque)) {
	conf->dr_cb = dr_cb;
}


void rd_kafka_conf_set_error_cb (rd_kafka_conf_t *conf,
				 void  (*error_cb) (rd_kafka_t *rk, int err,
						    const char *reason,
						    void *opaque)) {
	conf->error_cb = error_cb;
}


void rd_kafka_conf_set_stats_cb (rd_kafka_conf_t *conf,
				 int (*stats_cb) (rd_kafka_t *rk,
						  char *json,
						  size_t json_len,
						  void *opaque)) {
	conf->stats_cb = stats_cb;
}


void rd_kafka_conf_set_opaque (rd_kafka_conf_t *conf, void *opaque) {
	conf->opaque = opaque;
}



void
rd_kafka_topic_conf_set_partitioner_cb (rd_kafka_topic_conf_t *topic_conf,
					int32_t (*partitioner) (
						const rd_kafka_topic_t *rkt,
						const void *keydata,
						size_t keylen,
						int32_t partition_cnt,
						void *rkt_opaque,
						void *msg_opaque)) {
	topic_conf->partitioner = partitioner;
}

void rd_kafka_topic_conf_set_opaque (rd_kafka_topic_conf_t *topic_conf,
				     void *opaque) {
	topic_conf->opaque = opaque;
}


static const char **rd_kafka_anyconf_dump (int scope, void *conf,
					   size_t *cntp) {
	const struct rd_kafka_property *prop;
	char **arr;
	int cnt = 0;

	arr = calloc(sizeof(char *), RD_ARRAYSIZE(rd_kafka_properties)*2);

	for (prop = rd_kafka_properties; prop->name ; prop++) {
		char tmp[22];
		const char *val = NULL;
		int j;

		if (!(prop->scope & scope))
			continue;


		switch (prop->type)
		{
		case _RK_C_STR:
			val = *_RK_PTR(const char **, conf, prop->offset);
			break;

		case _RK_C_PTR:
			val = *_RK_PTR(const void **, conf, prop->offset);
			if (val) {
				snprintf(tmp, sizeof(tmp), "%p", (void *)val);
				val = tmp;
			}
			break;

		case _RK_C_BOOL:
			val = (*_RK_PTR(int *, conf, prop->offset) ?
			       "true":"false");
			break;
		case _RK_C_INT:
			snprintf(tmp, sizeof(tmp), "%i",
				 *_RK_PTR(int *, conf, prop->offset));
			val = tmp;
			break;
		case _RK_C_S2I:
			for (j = 0 ; j < RD_ARRAYSIZE(prop->s2i); j++)
				if (prop->s2i[j].val ==
				    *_RK_PTR(int *, conf, prop->offset))
					val = prop->s2i[j].str;
			break;
		case _RK_C_S2F:
			/* FIXME: ignore for now, just used with "debug" */
		default:
			break;
		}

		if (val) {
			arr[cnt++] = strdup(prop->name);
			arr[cnt++] = strdup(val);
		}
	}

	*cntp = cnt;

	return (const char **)arr;
}


const char **rd_kafka_conf_dump (rd_kafka_conf_t *conf, size_t *cntp) {
	return rd_kafka_anyconf_dump(_RK_GLOBAL, conf, cntp);
}

const char **rd_kafka_topic_conf_dump (rd_kafka_topic_conf_t *conf,
				       size_t *cntp) {
	return rd_kafka_anyconf_dump(_RK_TOPIC, conf, cntp);
}

void rd_kafka_conf_dump_free (const char **arr, size_t cnt) {
	char **_arr = (char **)arr;
	int i;

	for (i = 0 ; i < cnt ; i++)
		if (_arr[i])
			free(_arr[i]);

	free(_arr);
}

void rd_kafka_conf_properties_show (FILE *fp) {
	const struct rd_kafka_property *prop;
	int last = 0;
	int j;
	const char *dash80 = "----------------------------------------"
		"----------------------------------------";

	for (prop = rd_kafka_properties; prop->name ; prop++) {
		
		if (!(prop->scope & last)) {
			fprintf(fp,
				"%s## %s configuration properties\n\n",
				last ? "\n\n":"",
				prop->scope == _RK_GLOBAL ? "Global": "Topic");

			fprintf(fp,
				"%-40s | %13s | %-25s\n"
				"%.*s-|-%.*s:|-%.*s\n",
				"Property", "Default", "Description",
				40, dash80, 13, dash80, 25, dash80);

			last = prop->scope & (_RK_GLOBAL|_RK_TOPIC);

		}

		fprintf(fp, "%-40s | ", prop->name);

		switch (prop->type)
		{
		case _RK_C_STR:
			fprintf(fp, "%13s", prop->sdef ? : "");
			break;
		case _RK_C_BOOL:
			fprintf(fp, "%13s", prop->vdef ? "true" : "false");
			break;
		case _RK_C_INT:
			fprintf(fp, "%13i", prop->vdef);
			break;
		case _RK_C_S2I:
			for (j = 0 ; j < RD_ARRAYSIZE(prop->s2i); j++) {
				if (prop->s2i[j].val == prop->vdef) {
					fprintf(fp, "%13s", prop->s2i[j].str);
					break;
				}
			}
			if (j == RD_ARRAYSIZE(prop->s2i))
				fprintf(fp, "%13s", " ");
			break;
		case _RK_C_S2F:
		default:
			/* FIXME when needed */
			fprintf(fp, "%-13s", " ");
			break;
		}

		fprintf(fp, " | %s\n", prop->desc);
	}
	fprintf(fp, "\n");

}
