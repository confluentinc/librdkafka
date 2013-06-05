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

#include "rdkafka.h"

/**
 * rd_kafka_conf_t default configuration.
 * Use a copy of this struct as the base of your own configuration.
 * See rdkafka.h for more information.
 */
static const rd_kafka_conf_t rd_kafka_defaultconf = {
	/* common settings */
	max_msg_size: 4000000,
	request_timeout_ms: 1000 * 60,  /* 1 minute */

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
	metadata_refresh_interval_ms: 10000,
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
