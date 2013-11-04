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
	enum {
		_RK_GLOBAL = 0x1,
		_RK_PRODUCER = 0x2,
		_RK_CONSUMER = 0x4,
		_RK_TOPIC = 0x8
	} scope;
	const char *name;
	enum {
		_RK_C_STR,
		_RK_C_INT,
		_RK_C_S2I,  /* String to Integer mapping */
		_RK_C_S2F,  /* CSV String to Integer flag mapping (OR:ed) */
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
	  sdef: "rdkafka" },
	{ _RK_GLOBAL, "metadata.broker.list", _RK_C_STR, _RK(brokerlist),
	  "Initial list of brokers. "
	  "The application may also use `rd_kafka_brokers_add()` to add "
	  "brokers during runtime." },
	{ _RK_GLOBAL, "message.max.bytes", _RK_C_INT, _RK(max_msg_size),
	  "Maximum receive message size. "
	  "This is a safety precaution to avoid memory exhaustion in case of "
	  "protocol hickups.",
	  1000, 1000000000, 4000000 },
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
	  s2i: {
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
	{ _RK_GLOBAL, "broker.address.ttl", _RK_C_INT,
	  _RK(broker_addr_ttl),
	  "How long to cache the broker address resolving results.",
	  0, 86400*1000, 300*1000 },
	{ _RK_GLOBAL, "statistics.interval.ms", _RK_C_INT,
	  _RK(stats_interval_ms),
	  "librdkafka statistics emit interval. The application also needs to "
	  "register a stats callback using `rd_kafka_conf_set_stats_cb()`. "
	  "The granularity is 1000ms.",
	  0, 86400*1000, 0 },

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
	  _RK(msg_send_max_retries),
	  "How many times to retry sending a failing MessageSet. "
	  "**Note:** retrying may cause reordering.",
	  0, 100, 2 },
	{ _RK_GLOBAL|_RK_PRODUCER, "retry.backoff.ms", _RK_C_INT,
	  _RK(retry_backoff_ms),
	  "The backoff time in milliseconds before retrying a message send.",
	  1, 300*1000, 100 },
	{ _RK_GLOBAL|_RK_PRODUCER, "compression.codec", _RK_C_S2I,
	  _RK(compression_codec),
	  "Compression codec to use for compressing message sets.",
	  vdef: RD_KAFKA_COMPRESSION_NONE,
	  s2i: {
			{ RD_KAFKA_COMPRESSION_NONE,   "none" },
			{ RD_KAFKA_COMPRESSION_GZIP,   "gzip" },
			{ RD_KAFKA_COMPRESSION_SNAPPY, "snappy" },
		} },
	{ _RK_GLOBAL|_RK_PRODUCER, "batch.num.messages", _RK_C_INT,
	  _RK(batch_num_messages),
	  "Maximum number of messages batched in one MessageSet.",
	  1, 1000000, 1000 },
	
	  

	/**
	 * Topic properties */
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
	

	{ /* End */ }
};



static rd_kafka_conf_res_t
rd_kafka_anyconf_set_prop0 (int scope, void *conf,
			    const struct rd_kafka_property *prop,
			    const char *istr, int ival) {
#define _PTR(TYPE,BASE,OFFSET)  (TYPE)(((char *)(BASE))+(OFFSET))
	switch (prop->type)
	{
	case _RK_C_STR:
	{
		char **str = _PTR(char **, conf, prop->offset);
		if (*str)
			free(*str);
		*str = strdup(istr);
		return RD_KAFKA_CONF_OK;
	}
	case _RK_C_INT:
	case _RK_C_S2I:
	case _RK_C_S2F:
	{
		int *val = _PTR(int *, conf, prop->offset);

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

#undef PTR

	/* unreachable */
	return RD_KAFKA_CONF_INVALID;
}

static rd_kafka_conf_res_t
rd_kafka_anyconf_set_prop (int scope, void *conf,
			   const struct rd_kafka_property *prop,
			   const char *value,
			   char *errstr, size_t errstr_size) {

	switch (prop->type)
	{
	case _RK_C_STR:
		rd_kafka_anyconf_set_prop0(scope, conf, prop, value, 0);
		return RD_KAFKA_CONF_OK;

	case _RK_C_INT:
	{
		int ival;

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
	}

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
			while (s < t && isspace(*s))
				s++;

			/* Right trim */
			while (t > s && isspace(*t))
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

#define _PTR(TYPE,BASE,OFFSET)  (TYPE)(((char *)(BASE))+(OFFSET))
	switch (prop->type)
	{
	case _RK_C_STR:
	{
		char **str = _PTR(char **, conf, prop->offset);
		if (*str) {
			free(*str);
			*str = NULL;
		}
	}
	break;

	default:
		break;
	}
#undef _PTR

}

static void rd_kafka_anyconf_destroy (int scope, void *conf) {
	const struct rd_kafka_property *prop;

	for (prop = rd_kafka_properties; prop->name ; prop++) {
		if (!(prop->scope & scope))
			continue;

		rd_kafka_anyconf_clear(conf, prop);
	}
}


/**
 * Destroys a conf object.
 * The 'conf' pointer itself is not freed.
 */
void rd_kafka_conf_destroy (rd_kafka_conf_t *conf) {
	rd_kafka_anyconf_destroy(_RK_GLOBAL, conf);
}

	
/**
 * Destroys a topic conf object.
 * The 'topic_conf' pointer itself is not freed.
 */
void rd_kafka_topic_conf_destroy (rd_kafka_topic_conf_t *topic_conf) {
	rd_kafka_anyconf_destroy(_RK_TOPIC, topic_conf);
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



void rd_kafka_conf_properties_show (FILE *fp) {
	const struct rd_kafka_property *prop;
	int last = 0;
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
		case _RK_C_INT:
			fprintf(fp, "%13i", prop->vdef);
			break;
		case _RK_C_S2I:
			fprintf(fp, "%13s", prop->s2i[prop->vdef].str);
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
