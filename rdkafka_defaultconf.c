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

#include "rdkafka.h"
#include "rdkafka_int.h"
#include "rd.h"

/**
 * rd_kafka_conf_t default configuration.
 * Use a copy of this struct as the base of your own configuration.
 * See rdkafka.h for more information.
 */
static const rd_kafka_conf_t rd_kafka_defaultconf = {
	/* common settings */
	max_msg_size: 4000000,
	metadata_request_timeout_ms: 1000 * 60,  /* 1 minute */
	metadata_refresh_interval_ms: 10000,     /* 10s */
	metadata_refresh_fast_cnt: 10,
	metadata_refresh_fast_interval_ms: 250,  /* 250ms */

	/* consumer settings */
	consumer: {
	poll_interval: 1000 /* 1s */,
	replyq_low_thres: 1,
	max_size: 500000,
	},

	/* producer settings */
	producer: {
	max_messages: 1000000,
	buffering_max_ms: 1000,
	max_retries: 0,
	retry_backoff_ms: 100,
	batch_num_messages: 1000,
	},
	
};


static const rd_kafka_topic_conf_t rd_kafka_topic_defaultconf = {
required_acks: 1,
/* partitioner: defaults to random partitioner */
request_timeout_ms: 1500,          /* 1.5 seconds */
message_timeout_ms: 1000 * 60 * 5, /* 5 minutes */
};


void rd_kafka_defaultconf_set (rd_kafka_conf_t *conf) {
	*conf = rd_kafka_defaultconf;
}

void rd_kafka_topic_defaultconf_set (rd_kafka_topic_conf_t *topic_conf) {
	*topic_conf = rd_kafka_topic_defaultconf;
}



struct rd_kafka_property {
	const char *name;
	enum {
		_RK_C_STR,
		_RK_C_INT,
		_RK_C_S2I,  /* String to Integer mapping */
		_RK_C_S2F,  /* CSV String to Integer flag mapping (OR:ed) */
	} type;
	void *ptr;
	int   vmin;
	int   vmax;
	struct {
		int val;
		const char *str;
	} s2i[10];  /* _RK_C_S2I and _RK_C_S2F */
};


static int rd_kafka_anyconf_set (struct rd_kafka_property *properties,
				 const char *name, const char *value,
				 char *errstr, size_t errstr_size) {
	int i;

	if (value && !*value)
		value = NULL;

	for (i = 0 ; properties[i].name ; i++) {
		if (strcmp(properties[i].name, name))
			continue;

		switch (properties[i].type)
		{
		case _RK_C_STR:
			*(char **)properties[i].ptr = strdup(value);
			return RD_KAFKA_CONF_OK;

		case _RK_C_INT:
		{
			int ival;

			if (!value) {
				snprintf(errstr, errstr_size,
					 "Integer configuration "
					 "property \"%s\" cannot be set "
					 "to empty value", name);
				return RD_KAFKA_CONF_INVALID;
			}

			ival = atoi(value);
			if (ival < properties[i].vmin ||
			    ival > properties[i].vmax) {
				snprintf(errstr, errstr_size,
					 "Configuration property \"%s\" value "
					 "%i is outside allowed range %i..%i\n",
					 name, ival,
					 properties[i].vmin,
					 properties[i].vmax);
				return RD_KAFKA_CONF_INVALID;
			}


			*(int *)properties[i].ptr = ival;
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
					 "to empty value", name);
				return RD_KAFKA_CONF_INVALID;
			}
			

			next = value;
			while (next && *next) {
				const char *s, *t;

				s = next;

				if (properties[i].type == _RK_C_S2F &&
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
				for (j = 0 ;
				     j < RD_ARRAYSIZE(properties[i].s2i) ;
				     j++) {
					if (!properties[i].s2i[j].str ||
					    strlen(properties[i].s2i[j].str) !=
					    (int)(t-s) ||
					    strncmp(properties[i].s2i[j].str,
						    s, (int)(t-s)))
						continue;

					if (properties[i].type == _RK_C_S2F) {
						/* Flags: OR it in */
						*(int *)properties[i].ptr |=
							properties[i].s2i[j].
							val;
						break;
					} else {
						/* Single assignment */
						*(int *)properties[i].ptr =
							properties[i].s2i[j].
							val;
						return RD_KAFKA_CONF_OK;
					}
				}
				
				/* S2F: Good match: continue with next */
				if (j < RD_ARRAYSIZE(properties[i].s2i))
					continue;

				/* No match */
				snprintf(errstr, errstr_size,
					 "Invalid value for "
					 "configuration property \"%s\"", name);
				return RD_KAFKA_CONF_INVALID;

			}
			return RD_KAFKA_CONF_OK;
		}
		
		}

	}

	snprintf(errstr, errstr_size,
		 "No such configuration property: \"%s\"", name);

	return RD_KAFKA_CONF_UNKNOWN;
}


rd_kafka_conf_res_t rd_kafka_conf_set (rd_kafka_conf_t *conf,
				       const char *name,
				       const char *value,
				       char *errstr, size_t errstr_size) {
	struct rd_kafka_property properties[] = {
		{ "client.id", _RK_C_STR, &conf->clientid },
		{ "metadata.broker.list", _RK_C_STR, &conf->brokerlist },
		{ "message.max.bytes", _RK_C_INT,
		  &conf->max_msg_size, 1000, 100000000 },
		{ "metadata.request.timeout.ms", _RK_C_INT,
		  &conf->metadata_request_timeout_ms, 10, 900*1000 },
		{ "queue.buffering.max.messages", _RK_C_INT,
		  &conf->producer.max_messages, 1, 1001000000 },
		{ "queue.buffering.max.ms", _RK_C_INT,
		  &conf->producer.buffering_max_ms, 1, 900*1000 },
		{ "topic.metadata.refresh.interval.ms", _RK_C_INT,
		  &conf->metadata_refresh_interval_ms,
		  1000, 3600*1000 },
		{ "topic.metadata.refresh.fast.cnt", _RK_C_INT,
		  &conf->metadata_refresh_fast_cnt, 0, 1000 },
		{ "topic.metadata.refresh.fast.interval.ms", _RK_C_INT,
		  &conf->metadata_refresh_fast_interval_ms, 1, 60000 },
		{ "message.send.max.retries", _RK_C_INT,
		  &conf->producer.max_retries, 0, 100 },
		{ "retry.backoff.ms", _RK_C_INT,
		  &conf->producer.retry_backoff_ms, 1, 900*1000 },
		{ "compression.codec", _RK_C_S2I,
		  &conf->producer.compression_codec,
		  s2i: {
				{ RD_KAFKA_COMPRESSION_NONE,   "none" },
				{ RD_KAFKA_COMPRESSION_GZIP,   "gzip" },
				{ RD_KAFKA_COMPRESSION_SNAPPY, "snappy" },
			} },
		{ "queue.enqueue.timeout.ms", _RK_C_INT,
		  &conf->producer.enqueue_timeout_ms, -1, 60*1000 },
		{ "batch.num.messages", _RK_C_INT,
		  &conf->producer.batch_num_messages, 1, 1000000 },
		{ "debug", _RK_C_S2F,
		  &conf->debug,
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
		{ },
	};

	return rd_kafka_anyconf_set(properties, name, value,
				    errstr, errstr_size);
}


rd_kafka_conf_res_t rd_kafka_topic_conf_set (rd_kafka_topic_conf_t *conf,
					     const char *name,
					     const char *value,
					     char *errstr, size_t errstr_size) {
	struct rd_kafka_property properties[] = {
		{ "request.required.acks", _RK_C_INT,
		  &conf->required_acks, -1, 100 },
		{ "request.timeout.ms", _RK_C_INT,
		  &conf->request_timeout_ms, 1, 900*1000 },
		{ "message.timeout.ms", _RK_C_INT,
		  &conf->message_timeout_ms, 1, 900*1000 },
		{ }
	};

	if (!strncmp(name, "topic.", strlen("topic.")))
		name += strlen("topic.");

	return rd_kafka_anyconf_set(properties, name, value,
				    errstr, errstr_size);
}



/**
 * Destroys a conf object.
 * The 'conf' pointer itself is not freed.
 */
void rd_kafka_conf_destroy (rd_kafka_conf_t *conf) {
#define IF_FREE(p) if (p) free(p)
	IF_FREE(conf->clientid);
	IF_FREE(conf->brokerlist);
}


/**
 * Destroys a topic conf object.
 * The 'topic_conf' pointer itself is not freed.
 */
void rd_kafka_topic_conf_destroy (rd_kafka_topic_conf_t *topic_conf) {
}
