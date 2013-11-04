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
#include <syslog.h>
#include <sys/time.h>
#include <errno.h>

/* Typical include path would be <librdkafka/rdkafka.h>, but this program
 * is builtin from within the librdkafka source tree and thus differs. */
#include "rdkafka.h"  /* for Kafka driver */


static int run = 1;
static rd_kafka_t *rk;
static int exit_eof = 0;

static void stop (int sig) {
	run = 0;
	fclose(stdin); /* abort fgets() */
}


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

/**
 * Kafka logger callback (optional)
 */
static void logger (const rd_kafka_t *rk, int level,
		    const char *fac, const char *buf) {
	struct timeval tv;
	gettimeofday(&tv, NULL);
	fprintf(stderr, "%u.%03u RDKAFKA-%i-%s: %s: %s\n",
		(int)tv.tv_sec, (int)(tv.tv_usec / 1000),
		level, fac, rd_kafka_name(rk), buf);
}

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
		       rd_kafka_err2str(error_code));
	else
		printf("%% Message delivered (%zd bytes)\n", len);
}


static void msg_consume (rd_kafka_message_t *rkmessage,
			 void *opaque) {
	if (rkmessage->err) {
		if (rkmessage->err == RD_KAFKA_RESP_ERR__PARTITION_EOF) {
			printf("%% Consumer reached end of %s [%"PRId32"] "
			       "message queue at offset %"PRId64"\n",
			       rd_kafka_topic_name(rkmessage->rkt),
			       rkmessage->partition, rkmessage->offset);

			if (exit_eof)
				run = 0;

			return;
		}

		printf("%% Consume error for topic \"%s\" [%"PRId32"] "
		       "offset %"PRId64": %s\n",
		       rd_kafka_topic_name(rkmessage->rkt),
		       rkmessage->partition,
		       rkmessage->offset,
		       rd_kafka_message_errstr(rkmessage));
		return;
	}

	printf("%% Message (offset %"PRId64", %zd bytes):\n",
	       rkmessage->offset, rkmessage->len);

	if (rkmessage->key_len)
		hexdump(stdout, "Message Key",
			rkmessage->key, rkmessage->key_len);

	hexdump(stdout, "Message Payload", rkmessage->payload, rkmessage->len);
}


static void sig_usr1 (int sig) {
	rd_kafka_dump(stdout, rk);
}

int main (int argc, char **argv) {
	rd_kafka_topic_t *rkt;
	char *brokers = "localhost:9092";
	char mode = 'C';
	char *topic = NULL;
	int partition = RD_KAFKA_PARTITION_UA;
	int opt;
	rd_kafka_conf_t *conf;
	rd_kafka_topic_conf_t *topic_conf;
	char errstr[512];
	const char *debug = NULL;
	int64_t start_offset = 0;

	/* Kafka configuration */
	conf = rd_kafka_conf_new();

	/* Topic configuration */
	topic_conf = rd_kafka_topic_conf_new();

	while ((opt = getopt(argc, argv, "PCt:p:b:z:d:o:e")) != -1) {
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
		case 'z':
			if (rd_kafka_conf_set(conf, "compression.codec",
					      optarg,
					      errstr, sizeof(errstr)) !=
			    RD_KAFKA_CONF_OK) {
				fprintf(stderr, "%% %s\n", errstr);
				exit(1);
			}
			break;
		case 'o':
			start_offset = strtoll(optarg, NULL, 10);
			break;
		case 'e':
			exit_eof = 1;
			break;
		case 'd':
			debug = optarg;
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
			"  -z <codec>      Enable compression:\n"
			"                  none|gzip|snappy\n"
			"  -o <offset>     Start offset (consumer)\n"
			"  -e              Exit consumer when last message\n"
			"                  in partition has been received.\n"
			"  -d [facs..]     Enable debugging contexts:\n"
			"                  %s\n"
			"\n"
			" In Consumer mode:\n"
			"  writes fetched messages to stdout\n"
			" In Producer mode:\n"
			"  reads messages from stdin and sends to broker\n"
			"\n",
			argv[0],
			RD_KAFKA_DEBUG_CONTEXTS);
		exit(1);
	}


	signal(SIGINT, stop);
	signal(SIGUSR1, sig_usr1);

	/* Socket hangups are gracefully handled in librdkafka on socket error
	 * without the use of signals, so SIGPIPE should be ignored by
	 * the calling program. */
	signal(SIGPIPE, SIG_IGN);

	if (debug &&
	    rd_kafka_conf_set(conf, "debug", debug, errstr, sizeof(errstr)) !=
	    RD_KAFKA_CONF_OK) {
		printf("%% Debug configuration failed: %s: %s\n",
		       errstr, debug);
		exit(1);
	}

	if (mode == 'P') {
		/*
		 * Producer
		 */
		char buf[2048];
		int sendcnt = 0;

		/* Set up a message delivery report callback.
		 * It will be called once for each message, either on successful
		 * delivery to broker, or upon failure to deliver to broker. */
		rd_kafka_conf_set_dr_cb(conf, msg_delivered);

		/* Create Kafka handle */
		if (!(rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf,
					errstr, sizeof(errstr)))) {
			fprintf(stderr,
				"%% Failed to create new producer: %s\n",
				errstr);
			exit(1);
		}

		/* Set logger */
		rd_kafka_set_logger(rk, logger);
		rd_kafka_set_log_level(rk, LOG_DEBUG);

		/* Add brokers */
		if (rd_kafka_brokers_add(rk, brokers) == 0) {
			fprintf(stderr, "%% No valid brokers specified\n");
			exit(1);
		}

		/* Create topic */
		rkt = rd_kafka_topic_new(rk, topic, topic_conf);

		fprintf(stderr, "%% Type stuff and hit enter to send\n");
		while (run && fgets(buf, sizeof(buf), stdin)) {
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

		/* Create Kafka handle */
		if (!(rk = rd_kafka_new(RD_KAFKA_CONSUMER, conf,
					errstr, sizeof(errstr)))) {
			fprintf(stderr,
				"%% Failed to create new producer: %s\n",
				errstr);
			exit(1);
		}

		/* Set logger */
		rd_kafka_set_logger(rk, logger);
		rd_kafka_set_log_level(rk, LOG_DEBUG);

		/* Add brokers */
		if (rd_kafka_brokers_add(rk, brokers) == 0) {
			fprintf(stderr, "%% No valid brokers specified\n");
			exit(1);
		}

		/* Create topic */
		rkt = rd_kafka_topic_new(rk, topic, topic_conf);

		/* Start consuming */
		if (rd_kafka_consume_start(rkt, partition, start_offset) == -1){
			fprintf(stderr, "%% Failed to start consuming: %s\n",
				strerror(errno));
			exit(1);
		}

		while (run) {
			rd_kafka_message_t *rkmessage;

			/* Consume single message.
			 * See rdkafka_performance.c for high speed
			 * consuming of messages. */
			rkmessage = rd_kafka_consume(rkt, partition, 1000);
			if (!rkmessage) /* timeout */
				continue;

			msg_consume(rkmessage, NULL);

			/* Return message to rdkafka */
			rd_kafka_message_destroy(rkmessage);
		}

		/* Stop consuming */
		rd_kafka_consume_stop(rkt, partition);

		rd_kafka_topic_destroy(rkt);

		rd_kafka_destroy(rk);
	}

	/* Let background threads clean up and terminate cleanly. */
	rd_kafka_wait_destroyed(2000);

	return 0;
}
