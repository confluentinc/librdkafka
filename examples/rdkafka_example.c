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

/**
 * Apache Kafka consumer & producer example programs
 * using the Kafka driver from librdkafka
 * (https://github.com/edenhill/librdkafka)
 */

#include <ctype.h>
#include <signal.h>
#include <string.h>
#include <unistd.h>
#include <stdlib.h>

/* Typical include path would be <librdkafka/rdkafka.h>, but this program
 * is builtin from within the librdkafka source tree and thus differs. */
#include "rdkafka.h"  /* for Kafka driver */


static int run = 1;

static void stop (int sig) {
	run = 0;
}


#if 0
static void hexdump (FILE *fp, const char *name, const void *ptr, size_t len) {
	const char *p = (const char *)ptr;
	int of = 0;


	if (name)
		fprintf(fp, "%s hexdump (%zd bytes):\n", name, len);

	for (of = 0 ; of < len ; of += 16) {
		char hexen[16*3+1];
		char charen[16+1];
		int hof = 0;

		int cof = 0;
		int i;

		for (i = of ; i < of + 16 && i < len ; i++) {
			hof += sprintf(hexen+hof, "%02x ", p[i] & 0xff);
			cof += sprintf(charen+cof, "%c",
				      isprint(p[i]) ? p[i] : '.');
		}
		fprintf(fp, "%08x: %-48s %-16s\n",
			of, hexen, charen);
	}
}
#endif


/**
 * Message delivery report callback.
 * Called once for each message.
 * See rdkafka.h for more information.
 */
static void msg_delivered (rd_kafka_t *rk,
			   void *payload, size_t len,
			   int error_code,
			   void *opaque, void *msg_opaque) {

	if (error_code)
		printf("%% Message delivery failed: %s\n",
		       rd_kafka_err2str(rk, error_code));
	else
		printf("%% Message delivered (%zd bytes)\n", len);
}


int main (int argc, char **argv) {
	rd_kafka_t *rk;
	rd_kafka_topic_t *rkt;
	char *brokers = "localhost:9092";
	char mode = 'C';
	char *topic = NULL;
	int partition = RD_KAFKA_PARTITION_UA;
	int opt;
	rd_kafka_conf_t conf;
	rd_kafka_topic_conf_t topic_conf;
	char errstr[512];

	/* Kafka configuration
	 * Base configuration on the default config. */
	rd_kafka_defaultconf_set(&conf);
	/* Set up a message delivery report callback.
	 * It will be called once for each message, either on succesful
	 * delivery to broker, or upon failure to deliver to broker. */
	conf.producer.dr_cb = msg_delivered;

	/* Topic configuration
	 * Base topic configuration on the default topic config. */
	rd_kafka_topic_defaultconf_set(&topic_conf);


	while ((opt = getopt(argc, argv, "PCt:p:b:")) != -1) {
		switch (opt) {
		case 'P':
		case 'C':
			mode = opt;
			break;
		case 't':
			topic = optarg;
			break;
		case 'p':
			partition = atoi(optarg);
			break;
		case 'b':
			brokers = optarg;
			break;
		default:
			goto usage;
		}
	}

	if (!topic || optind != argc) {
	usage:
		fprintf(stderr,
			"Usage: %s [-C|-P] -t <topic> "
			"[-p <partition>] [-b <host1:port1,host2:port2,..>]\n"
			"\n"
			" Options:\n"
			"  -C | -P         Consumer or Producer mode\n"
			"  -t <topic>      Topic to fetch / produce\n"
			"  -p <num>        Partition (random partitioner)\n"
			"  -b <brokers>    Broker address (localhost:9092)\n"
			"\n"
			" In Consumer mode:\n"
			"  writes fetched messages to stdout\n"
			" In Producer mode:\n"
			"  reads messages from stdin and sends to broker\n"
			"\n",
			argv[0]);
		exit(1);
	}


	signal(SIGINT, stop);

	/* Socket hangups are gracefully handled in librdkafka on socket error
	 * without the use of signals, so SIGPIPE should be ignored by
	 * the calling program. */
	signal(SIGPIPE, SIG_IGN);

	if (mode == 'P') {
		/*
		 * Producer
		 */
		char buf[2048];
		int sendcnt = 0;

		/* Create Kafka handle */
		if (!(rk = rd_kafka_new(RD_KAFKA_PRODUCER, &conf,
					errstr, sizeof(errstr)))) {
			fprintf(stderr,
				"%% Failed to create new producer: %s\n",
				errstr);
			exit(1);
		}

		if (rd_kafka_brokers_add(rk, brokers) == 0) {
			fprintf(stderr, "%% No valid brokers specified\n");
			exit(1);
		}

		/* Create topic */
		rkt = rd_kafka_topic_new(rk, topic, &topic_conf);

		fprintf(stderr, "%% Type stuff and hit enter to send\n");
		while (run && (fgets(buf, sizeof(buf), stdin))) {
			size_t len = strlen(buf);

			/* Send/Produce message. */
			rd_kafka_produce(rkt, partition,
					 RD_KAFKA_MSG_F_COPY,
					 /* Payload and length */
					 buf, len,
					 /* Optional key and its length */
					 NULL, 0,
					 /* Message opaque, provided in
					  * delivery report callback as
					  * msg_opaque. */
					 NULL);
			fprintf(stderr, "%% Sent %zd bytes to topic "
				"%s partition %i\n",
				len, rd_kafka_topic_name(rkt), partition);
			sendcnt++;
			/* Poll to handle delivery reports */
			rd_kafka_poll(rk, 10);
		}

		/* Poll to handle delivery reports */
		rd_kafka_poll(rk, 0);

		/* Wait for messages to be delivered */
		while (run && rd_kafka_poll(rk, 1000) != -1)
			continue;
			
		/* Destroy the handle */
		rd_kafka_destroy(rk);

	} else if (mode == 'C') {
		/*
		 * Consumer
		 */
#if 0 /* FIXME: Not implemented */
		rd_kafka_op_t *rko;
		/* Base our configuration on the default config. */
		rd_kafka_conf_t conf = rd_kafka_defaultconf;


		/* The offset storage file is optional but its presence
		 * avoids starting all over from offset 0 again when
		 * the program restarts.
		 * ZooKeeper functionality will be implemented in future
		 * versions and then the offset will be stored there instead. */
		conf.consumer.offset_file = "."; /* current directory */

		/* Indicate to rdkafka that the application is responsible
		 * for storing the offset. This allows the application to
		 * succesfully handle a message before storing the offset.
		 * If this flag is not set rdkafka will store the offset
		 * just prior to returning the message from rd_kafka_consume().
		 */
		conf.flags |= RD_KAFKA_CONF_F_APP_OFFSET_STORE;



		/* Use the consumer convenience function
		 * to create a Kafka handle. */
		if (!(rk = rd_kafka_new_consumer(broker, topic,
						 (uint32_t)partition,
						 0, &conf))) {
			perror("kafka_new_consumer");
			exit(1);
		}

		while (run) {
			/* Fetch an "op" which is one of:
			 *  - a kafka message (if rko_len>0 && rko_err==0)
			 *  - an error (if rko_err)
			 */
			if (!(rko = rd_kafka_consume(rk, 1000/*timeout ms*/)))
				continue;
			
			if (rko->rko_err)
				fprintf(stderr, "%% Error: %.*s\n",
					rko->rko_len, rko->rko_payload);
			else if (rko->rko_len) {
				fprintf(stderr, "%% Message with "
					"next-offset %"PRIu64" is %i bytes\n",
					rko->rko_offset, rko->rko_len);
				hexdump(stdout, "Message",
					rko->rko_payload, rko->rko_len);
			}

			/* rko_offset contains the offset of the _next_
			 * message. We store it when we're done processing
			 * the current message. */
			if (rko->rko_offset)
				rd_kafka_offset_store(rk, rko->rko_offset);

			/* Destroy the op */
			rd_kafka_op_destroy(rk, rko);
		}

		/* Destroy the handle */
		rd_kafka_destroy(rk);
#endif
	}

	return 0;
}
