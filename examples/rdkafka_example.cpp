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
 * Apache Kafka C++ consumer & producer example programs
 * using the C++ Kafka driver wrapper from librdkafka
 * (https://github.com/edenhill/librdkafka)
 */

#include <ctype.h>
#include <signal.h>
#include <string.h>

/* Typical include path would be <librdkafka/rdkafkah>, but this program
 * is builtin from within the librdkafka source tree and thus differs. */
#include "../c++/rdkafka.h"  /* for Kafka driver */
using rdkafka::Kafka;


static int run = 1;

static void stop (int sig) {
	run = 0;
}


int main (int argc, char **argv) {
	Kafka kafka;
	char *broker = NULL;
	char mode = 'C';
	char *topic = NULL;
	int partition = 0;
	int opt;


	while ((opt = getopt(argc, argv, "Pt:p:b:")) != -1) {
		switch (opt) {
		case 'P':
			mode = opt;
			break;
		case 't':
			topic = optarg;
			break;
		case 'p':
			partition = atoi(optarg);
			break;
		case 'b':
			broker = optarg;
			break;
		default:
			goto usage;
		}
	}

	if (!topic || optind != argc) {
	usage:
		fprintf(stderr,
			"Usage: %s "/*[-C|*/"-P] -t <topic> "
			"[-p <partition>] [-b <broker>]\n"
			"\n"
			" Options:\n"
			//"  -C | -P      Consumer or Producer mode\n"
			"  -P           Producer mode\n"
			"  -t <topic>   Topic to fetch / produce\n"
			"  -p <num>     Partition (defaults to 0)\n"
			"  -b <broker>  Broker address (localhost:9092)\n"
			"\n"
			//" In Consumer mode:\n"
			//"  writes fetched messages to stdout\n"
			" In Producer mode:\n"
			"  reads messages from stdin and sends to broker\n"
			"\n",
			argv[0]);
		exit(1);
	}


	signal(SIGINT, stop);

	/* Socket hangups are gracefully handled in librdkafka on socket error
	 * without the use of signals, so SIGPIPE should be ignored by the calling
	 * program. */
	signal(SIGPIPE, SIG_IGN);

	if (mode == 'P') {
		/*
		 * Producer
		 */
		char buf[2048];
		int sendcnt = 0;

		/* Create Kafka handle */
		if (!(kafka.setHandle(RD_KAFKA_PRODUCER, broker, NULL))) {
			perror("kafka_new producer");
			exit(1);
		}

		fprintf(stderr, "%% Type stuff and hit enter to send\n");
		while (run && (fgets(buf, sizeof(buf), stdin))) {
			int len = strlen(buf);
			char * opbuf = (char *)malloc(len + 1);
			strncpy(opbuf, buf, len + 1);

			/* Send/Produce message. */
			if (kafka.produce(topic, partition,
				RD_KAFKA_OP_F_FREE, opbuf, len) == -1){
				free(opbuf);
				continue;
			}
			fprintf(stderr, "%% Sent %i bytes to topic "
				"%s partition %i\n", len, topic, partition);
			sendcnt++;
		}

	} 
	#if 0 // Still not implemented
	else if (mode == 'C') {
		/*
		 * Consumer
		 */
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
	}
	#endif

	return 0;
}
