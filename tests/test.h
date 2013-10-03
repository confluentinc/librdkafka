#pragma once

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <assert.h>

#include "rdkafka.h"


#define TEST_FAIL(reason...) do {					\
		fprintf(stderr, "### Test failed at %s:%i:%s(): ###\n", \
			__FILE__,__LINE__,__FUNCTION__);		\
		fprintf(stderr, reason);				\
		fprintf(stderr, "\n");					\
		exit(1);						\
	} while (0)


#define TEST_PERROR(call) do {						\
		if (!(call))						\
			TEST_FAIL(#call " failed: %s", strerror(errno)); \
	} while (0)

#define TEST_SAY(what...) fprintf(stderr, what)

void test_conf_init (rd_kafka_conf_t **conf, rd_kafka_topic_conf_t **topic_conf,
		     int timeout);


void test_wait_exit (int timeout);
