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
 * Apache Kafka consumer & producer performance tester
 * using the Kafka driver from librdkafka
 * (https://github.com/edenhill/librdkafka)
 */

#include <ctype.h>
#include <signal.h>
#include <string.h>

/* Typical include path would be <librdkafka/rdkafkah>, but this program
 * is builtin from within the librdkafka source tree and thus differs. */
#include "rdkafka.h"  /* for Kafka driver */
#include "rdtime.h"

static int run = 1;

static void stop (int sig) {
	run = 0;
}



int main (int argc, char **argv) {
	rd_kafka_t *rk;
	char *broker = NULL;
	char mode = 'C';
	char *topic = NULL;
	int partition = 0;
	int opt;
	int msgsize = 1024;
	int msgcnt = -1;
	int sendflags = 0;
	int dispintvl = 1000;
	struct {
		rd_ts_t  t_start;
		rd_ts_t  t_end;
		rd_ts_t  t_end_send;
		uint64_t msgs;
		uint64_t bytes;
		rd_ts_t  t_latency;
		rd_ts_t  t_last;
		rd_ts_t  t_total;
	} cnt = {};
	rd_ts_t now;
	char *dirstr = "";

	while ((opt = getopt(argc, argv, "PCt:p:b:s:c:fi:D")) != -1) {
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
			broker = optarg;
			break;
		case 's':
			msgsize = atoi(optarg);
			break;
		case 'c':
			msgcnt = atoi(optarg);
			break;
		case 'D':
			sendflags |= RD_KAFKA_OP_F_FREE;
			break;
		case 'i':
			dispintvl = atoi(optarg);
			break;
		default:
			goto usage;
		}
	}

	if (!topic || optind != argc) {
	usage:
		fprintf(stderr,
			"Usage: %s [-C|-P] -t <topic> "
			"[-p <partition>] [-b <broker>] [options..]\n"
			"\n"
			" Options:\n"
			"  -C | -P      Consumer or Producer mode\n"
			"  -t <topic>   Topic to fetch / produce\n"
			"  -p <num>     Partition (defaults to 0)\n"
			"  -b <broker>  Broker address (localhost:9092)\n"
			"  -s <size>    Message size (producer)\n"
			"  -c <cnt>     Messages to transmit/receive\n"
			"  -D           Copy/Duplicate data buffer (producer)\n"
			"  -i <ms>      Display interval\n"
			"\n"
			" In Consumer mode:\n"
			"  consumes messages and prints thruput\n"
			" In Producer mode:\n"
			"  writes messages of size -s <..> and prints thruput\n"
			"\n",
			argv[0]);
		exit(1);
	}

	dispintvl *= 1000; /* us */

	signal(SIGINT, stop);

	/* Socket hangups are gracefully handled in librdkafka on socket error
	 * without the use of signals, so SIGPIPE should be ignored by the
	 * calling program. */
	signal(SIGPIPE, SIG_IGN);

	if (mode == 'P') {
		/*
		 * Producer
		 */
		char *sbuf = malloc(msgsize);
		int endwait;
		int outq;
		int i;

		memset(sbuf, 'R', msgsize);

		if (msgcnt == -1)
			printf("%% Sending messages of size %i bytes\n",
			       msgsize);
		else
			printf("%% Sending %i messages of size %i bytes\n",
			       msgcnt ,msgsize);

		/* Create Kafka handle */
		if (!(rk = rd_kafka_new(RD_KAFKA_PRODUCER, broker, NULL))) {
			perror("kafka_new producer");
			exit(1);
		}

		cnt.t_start = rd_clock();

		while (run && (msgcnt == -1 || cnt.msgs < msgcnt)) {
			char *pbuf = sbuf;
			/* Send/Produce message. */

			if (sendflags & RD_KAFKA_OP_F_FREE) {
				/* Duplicate memory */
				pbuf = malloc(msgsize);
				memcpy(pbuf, sbuf, msgsize);
			}

			rd_kafka_produce(rk, topic, partition,
					 sendflags, pbuf, msgsize);
			cnt.msgs++;
			cnt.bytes += msgsize;
			
			now = rd_clock();
			if (cnt.t_last + dispintvl <= now) {
				printf("%% %"PRIu64" messages and %"PRIu64 
				       "bytes: %"PRIu64" msgs/s and "
				       "%.2f Mb/s\n",
				       cnt.msgs, cnt.bytes,
				       (cnt.msgs / (now - cnt.t_start)) *
				       1000000,
				       (float)(cnt.bytes /
					       (now - cnt.t_start)));
				cnt.t_last = now;
			}

		}



		/* Wait for messaging to finish. */
		i = 0;
		while (run && rd_kafka_outq_len(rk) > 0) {
			if (!(i++ % (dispintvl/1000)))
				printf("%% Waiting for %i messages in outq "
				       "to be sent. Abort with Ctrl-c\n",
				       rd_kafka_outq_len(rk));
			usleep(1000);
		}

		cnt.t_end_send = rd_clock();

		outq = rd_kafka_outq_len(rk);
		cnt.msgs -= outq;
		cnt.bytes -= msgsize * outq;

		cnt.t_end = rd_clock();

		/* Since there is no ack for produce messages in 0.7 
		 * we wait some more for any packets in the socket buffers
		 * to be sent.
		 * This is fixed in protocol version 0.8 */
		endwait = cnt.msgs * 10;
		printf("%% Test timers stopped, but waiting %ims more "
		       "for the %"PRIu64 " messages to be transmitted from "
		       "socket buffers.\n"
		       "%% End with Ctrl-c\n",
		       endwait / 1000,
		       cnt.msgs);
		run = 1;
		while (run && endwait > 0) {
			usleep(10000);
			endwait -= 10000;
		}

		/* Destroy the handle */
		rd_kafka_destroy(rk);

		dirstr = "sent";

	} else if (mode == 'C') {
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


		/* Tell rdkafka to (try to) maintain 10000 messages
		 * in its internal receive buffers. This is to avoid
		 * application -> rdkafka -> broker  per-message ping-pong
		 * latency. */
		conf.consumer.replyq_low_thres = 100000;

		/* Use the consumer convenience function
		 * to create a Kafka handle. */
		if (!(rk = rd_kafka_new_consumer(broker, topic,
						 (uint32_t)partition,
						 0, &conf))) {
			perror("kafka_new_consumer");
			exit(1);
		}
		
		cnt.t_start = rd_clock();
		while (run && (msgcnt == -1 || msgcnt > cnt.msgs)) {
			/* Fetch an "op" which is one of:
			 *  - a kafka message (if rko_len>0 && rko_err==0)
			 *  - an error (if rko_err)
			 */
			uint64_t latency;

			latency = rd_clock();
			if (!(rko = rd_kafka_consume(rk, 1000/*timeout ms*/)))
				continue;
			cnt.t_latency += rd_clock() - latency;
			
			if (rko->rko_err)
				fprintf(stderr, "%% Error: %.*s\n",
					rko->rko_len, rko->rko_payload);
			else if (rko->rko_len) {
				cnt.msgs++;
				cnt.bytes += rko->rko_len;
			}

			/* rko_offset contains the offset of the _next_
			 * message. We store it when we're done processing
			 * the current message. */
			if (rko->rko_offset)
				rd_kafka_offset_store(rk, rko->rko_offset);

			/* Destroy the op */
			rd_kafka_op_destroy(rk, rko);

			now = rd_clock();
			if (cnt.t_last + dispintvl <= now &&
				cnt.t_start + 1000000 < now) {
				printf("%% %"PRIu64" messages and %"PRIu64 
				       " bytes: %"PRIu64" msgs/s and "
				       "%.2f Mb/s\n",
				       cnt.msgs, cnt.bytes,
				       (cnt.msgs / ((now - cnt.t_start)/1000))
				       * 1000,
				       (float)(cnt.bytes /
					       ((now - cnt.t_start) / 1000)));
				cnt.t_last = now;
			}

		}
		cnt.t_end = rd_clock();

		/* Destroy the handle */
		rd_kafka_destroy(rk);

		dirstr = "received";
	}

	if (cnt.t_end_send)
		cnt.t_total = cnt.t_end_send - cnt.t_start;
	else
		cnt.t_total = cnt.t_end - cnt.t_start;

	printf("%% %"PRIu64" messages and %"PRIu64" bytes "
	       "%s in %"PRIu64"ms: %"PRIu64" msgs/s and %.02f Mb/s\n",
	       cnt.msgs, cnt.bytes,
	       dirstr,
	       cnt.t_total / 1000,
	       (cnt.msgs / (cnt.t_total / 1000)) * 1000,
	       (float)(cnt.bytes / (cnt.t_total / 1000)));

	if (cnt.t_latency)
		printf("%% Average application fetch latency: %"PRIu64"us\n",
		       cnt.t_latency / cnt.msgs);


	return 0;
}
