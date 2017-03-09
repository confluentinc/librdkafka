/*
 * librdkafka - Apache Kafka C library
 *
 * Copyright (c) 2012-2013, Magnus Edenhill
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
 * Tests various config related things
 */


#include "test.h"

/* Typical include path would be <librdkafka/rdkafka.h>, but this program
 * is built from within the librdkafka source tree and thus differs. */
#include "rdkafka.h"  /* for Kafka driver */



static void dr_cb (rd_kafka_t *rk, void *payload, size_t len,
		   rd_kafka_resp_err_t err, void *opaque, void *msg_opaque) {
}

static void error_cb (rd_kafka_t *rk, int err, const char *reason,
		      void *opaque) {

}


static int32_t partitioner (const rd_kafka_topic_t *rkt,
			    const void *keydata,
			    size_t keylen,
			    int32_t partition_cnt,
			    void *rkt_opaque,
			    void *msg_opaque) {
	return 0;
}


static void conf_verify (int line,
			 const char **arr, size_t cnt, const char **confs) {
	int i, j;


	for (i = 0 ; confs[i] ; i += 2) {
		for (j = 0 ; j < (int)cnt ; j += 2) {
			if (!strcmp(confs[i], arr[j])) {
				if (strcmp(confs[i+1], arr[j+1]))
					TEST_FAIL("%i: Property %s mismatch: "
						  "expected %s != retrieved %s",
						  line,
						  confs[i],
						  confs[i+1], arr[j+1]);
			}
			if (j == (int)cnt)
				TEST_FAIL("%i: "
					  "Property %s not found in config\n",
					  line,
					  confs[i]);
		}
	}
}


static void conf_cmp (const char *desc,
		      const char **a, size_t acnt,
		      const char **b, size_t bcnt) {
	int i;

	if (acnt != bcnt)
		TEST_FAIL("%s config compare: count %zd != %zd mismatch",
			  desc, acnt, bcnt);

	for (i = 0 ; i < (int)acnt ; i += 2) {
		if (strcmp(a[i], b[i]))
			TEST_FAIL("%s conf mismatch: %s != %s",
				  desc, a[i], b[i]);
		else if (strcmp(a[i+1], b[i+1])) {
                        /* The default_topic_conf will be auto-created
                         * when global->topic fallthru is used, so its
                         * value will not match here. */
                        if (!strcmp(a[i], "default_topic_conf"))
                                continue;
                        TEST_FAIL("%s conf value mismatch for %s: %s != %s",
                                  desc, a[i], a[i+1], b[i+1]);
                }
	}
}


int main_0004_conf (int argc, char **argv) {
	rd_kafka_t *rk;
	rd_kafka_topic_t *rkt;
	rd_kafka_conf_t *ignore_conf, *conf, *conf2;
	rd_kafka_topic_conf_t *ignore_topic_conf, *tconf, *tconf2;
	char errstr[512];
	const char **arr_orig, **arr_dup;
	size_t cnt_orig, cnt_dup;
	int i;
        const char *topic;
	static const char *gconfs[] = {
		"message.max.bytes", "12345", /* int property */
		"client.id", "my id", /* string property */
		"debug", "topic,metadata", /* S2F property */
		"topic.blacklist", "__.*", /* #778 */
                "auto.offset.reset", "earliest", /* Global->Topic fallthru */
#if WITH_ZLIB
		"compression.codec", "gzip", /* S2I property */
#endif
		NULL
	};
	static const char *tconfs[] = {
		"request.required.acks", "-1", /* int */
		"auto.commit.enable", "false", /* bool */
		"auto.offset.reset", "error",  /* S2I */
		"offset.store.path", "my/path", /* string */
		NULL
	};

	test_conf_init(&ignore_conf, &ignore_topic_conf, 10);
	rd_kafka_conf_destroy(ignore_conf);
	rd_kafka_topic_conf_destroy(ignore_topic_conf);

        topic = test_mk_topic_name("0004", 0);

	/* Set up a global config object */
	conf = rd_kafka_conf_new();

	rd_kafka_conf_set_dr_cb(conf, dr_cb);
	rd_kafka_conf_set_error_cb(conf, error_cb);

	for (i = 0 ; gconfs[i] ; i += 2) {
		if (rd_kafka_conf_set(conf, gconfs[i], gconfs[i+1],
				      errstr, sizeof(errstr)) !=
		    RD_KAFKA_CONF_OK)
			TEST_FAIL("%s\n", errstr);
	}

	/* Set up a topic config object */
	tconf = rd_kafka_topic_conf_new();

	rd_kafka_topic_conf_set_partitioner_cb(tconf, partitioner);
	rd_kafka_topic_conf_set_opaque(tconf, (void *)0xbeef);

	for (i = 0 ; tconfs[i] ; i += 2) {
		if (rd_kafka_topic_conf_set(tconf, tconfs[i], tconfs[i+1],
				      errstr, sizeof(errstr)) !=
		    RD_KAFKA_CONF_OK)
			TEST_FAIL("%s\n", errstr);
	}


	/* Verify global config */
	arr_orig = rd_kafka_conf_dump(conf, &cnt_orig);
	conf_verify(__LINE__, arr_orig, cnt_orig, gconfs);

	/* Verify copied global config */
	conf2 = rd_kafka_conf_dup(conf);
	arr_dup = rd_kafka_conf_dump(conf2, &cnt_dup);
	conf_verify(__LINE__, arr_dup, cnt_dup, gconfs);
	conf_cmp("global", arr_orig, cnt_orig, arr_dup, cnt_dup);
	rd_kafka_conf_dump_free(arr_orig, cnt_orig);
	rd_kafka_conf_dump_free(arr_dup, cnt_dup);

	/* Verify topic config */
	arr_orig = rd_kafka_topic_conf_dump(tconf, &cnt_orig);
	conf_verify(__LINE__, arr_orig, cnt_orig, tconfs);

	/* Verify copied topic config */
	tconf2 = rd_kafka_topic_conf_dup(tconf);
	arr_dup = rd_kafka_topic_conf_dump(tconf2, &cnt_dup);
	conf_verify(__LINE__, arr_dup, cnt_dup, tconfs);
	conf_cmp("topic", arr_orig, cnt_orig, arr_dup, cnt_dup);
	rd_kafka_conf_dump_free(arr_orig, cnt_orig);
	rd_kafka_conf_dump_free(arr_dup, cnt_dup);


	/*
	 * Create kafka instances using original and copied confs
	 */

	/* original */
	rk = test_create_handle(RD_KAFKA_PRODUCER, conf);

	rkt = rd_kafka_topic_new(rk, topic, tconf);
	if (!rkt)
		TEST_FAIL("Failed to create topic: %s\n",
			  rd_strerror(errno));

	rd_kafka_topic_destroy(rkt);
	rd_kafka_destroy(rk);

	/* copied */
	rk = test_create_handle(RD_KAFKA_PRODUCER, conf2);

	rkt = rd_kafka_topic_new(rk, topic, tconf2);
	if (!rkt)
		TEST_FAIL("Failed to create topic: %s\n",
			  rd_strerror(errno));

	rd_kafka_topic_destroy(rkt);
	rd_kafka_destroy(rk);


	/* Incremental S2F property.
	 * NOTE: The order of fields returned in get() is hardcoded here. */
	{
		static const char *s2fs[] = {
			"generic,broker,queue,cgrp",
			"generic,broker,queue,cgrp",

			"-broker,+queue,topic",
			"generic,topic,queue,cgrp",

			"-all,security,-fetch,+metadata",
			"metadata,security",

			NULL
		};

		TEST_SAY("Incremental S2F tests\n");
		conf = rd_kafka_conf_new();

		for (i = 0 ; s2fs[i] ; i += 2) {
			const char *val;

			TEST_SAY("  Set: %s\n", s2fs[i]);
			test_conf_set(conf, "debug", s2fs[i]);
			val = test_conf_get(conf, "debug");
			TEST_SAY("  Now: %s\n", val);

			if (strcmp(val, s2fs[i+1]))
				TEST_FAIL_LATER("\n"
						"Expected: %s\n"
						"     Got: %s",
						s2fs[i+1], val);
		}
		rd_kafka_conf_destroy(conf);
	}

	/* Canonical int values, aliases, s2i-verified strings */
	{
		static const struct {
			const char *prop;
			const char *val;
			const char *exp;
			int is_global;
		} props[] = {
			{ "request.required.acks", "0", "0" },
			{ "request.required.acks", "-1", "-1" },
			{ "request.required.acks", "1", "1" },
			{ "acks", "3", "3" }, /* alias test */
			{ "request.required.acks", "393", "393" },
			{ "request.required.acks", "bad", NULL },
			{ "request.required.acks", "all", "-1" },
                        { "request.required.acks", "all", "-1", 1/*fallthru*/ },
			{ "acks", "0", "0" }, /* alias test */
#if WITH_SASL
			{ "sasl.mechanisms", "GSSAPI", "GSSAPI", 1 },
			{ "sasl.mechanisms", "PLAIN", "PLAIN", 1  },
			{ "sasl.mechanisms", "GSSAPI,PLAIN", NULL, 1  },
			{ "sasl.mechanisms", "", NULL, 1  },
#endif
			{ NULL }
		};

		TEST_SAY("Canonical tests\n");
		tconf = rd_kafka_topic_conf_new();
		conf = rd_kafka_conf_new();

		for (i = 0 ; props[i].prop ; i++) {
			char dest[64];
			size_t destsz;
			rd_kafka_conf_res_t res;

			TEST_SAY("  Set: %s=%s expect %s (%s)\n",
				 props[i].prop, props[i].val, props[i].exp,
                                 props[i].is_global ? "global":"topic");


			/* Set value */
			if (props[i].is_global)
				res = rd_kafka_conf_set(conf,
						      props[i].prop,
						      props[i].val,
						      errstr, sizeof(errstr));
			else
				res = rd_kafka_topic_conf_set(tconf,
							      props[i].prop,
							      props[i].val,
							      errstr,
							      sizeof(errstr));
			if ((res == RD_KAFKA_CONF_OK ? 1:0) !=
			    (props[i].exp ? 1:0))
				TEST_FAIL("Expected %s, got %s",
					  props[i].exp ? "success" : "failure",
					  (res == RD_KAFKA_CONF_OK ? "OK" :
					   (res == RD_KAFKA_CONF_INVALID ? "INVALID" :
					    "UNKNOWN")));

			if (!props[i].exp)
				continue;

			/* Get value and compare to expected result */
			destsz = sizeof(dest);
			if (props[i].is_global)
				res = rd_kafka_conf_get(conf,
							props[i].prop,
							dest, &destsz);
			else
				res = rd_kafka_topic_conf_get(tconf,
							      props[i].prop,
							      dest, &destsz);
			TEST_ASSERT(res == RD_KAFKA_CONF_OK,
				    ".._conf_get(%s) returned %d",
                                    props[i].prop, res);

			TEST_ASSERT(!strcmp(props[i].exp, dest),
				    "Expected \"%s\", got \"%s\"",
				    props[i].exp, dest);
		}
		rd_kafka_topic_conf_destroy(tconf);
		rd_kafka_conf_destroy(conf);
	}

	return 0;
}
