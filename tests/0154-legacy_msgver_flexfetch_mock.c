/*
 * librdkafka - Apache Kafka C library
 *
 * Copyright (c) 2026, Confluent Inc.
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

#include "test.h"

/**
 * @name Read legacy magic v0/v1 message sets over a modern flexible Fetch.
 *
 * When a topic is stored on disk in the legacy message format (magic byte 0
 * or 1, i.e. log.message.format.version <= 0.10.x), a broker speaking the
 * modern protocol serves those records as-is inside a flexible (Fetch v12+)
 * response. The Records/MessageSet payload is encoded in the fixed Kafka
 * record format and is independent of the FetchResponse's flexible framing;
 * in particular legacy magic v0/v1 Message Key/Value use plain int32 length
 * prefixes, not compact (uvarint) ones.
 *
 * A regression (2.5.0..) caused the message-set reader to decode those
 * lengths as compact bytes whenever the response buffer's flexible flag was
 * set, misaligning the parse and silently dropping/corrupting records (and
 * spinning forever on a bogus parsed offset).
 *
 * This test injects a hand-crafted magic v1 message set into a mock partition
 * (served verbatim over the modern flexible Fetch a default consumer
 * negotiates) and verifies every record is delivered with byte-exact
 * key/value and no errors.
 */

static void buf_push(uint8_t *buf, size_t *of, const void *data, size_t len) {
        memcpy(buf + *of, data, len);
        *of += len;
}

static void buf_push_i8(uint8_t *buf, size_t *of, int8_t v) {
        buf[(*of)++] = (uint8_t)v;
}

static void buf_push_i32(uint8_t *buf, size_t *of, int32_t v) {
        buf[(*of)++] = (uint8_t)((v >> 24) & 0xff);
        buf[(*of)++] = (uint8_t)((v >> 16) & 0xff);
        buf[(*of)++] = (uint8_t)((v >> 8) & 0xff);
        buf[(*of)++] = (uint8_t)(v & 0xff);
}

static void buf_push_i64(uint8_t *buf, size_t *of, int64_t v) {
        int i;
        for (i = 7; i >= 0; i--)
                buf[(*of)++] = (uint8_t)((v >> (i * 8)) & 0xff);
}

/**
 * @brief Append one legacy magic v1 (uncompressed) Message to \p buf, framed
 *        in a MessageSet entry with the given absolute \p offset.
 *
 * Layout:
 *   Offset:int64, MessageSize:int32,
 *   [ Crc:int32, MagicByte:int8(=1), Attributes:int8(=0), Timestamp:int64,
 *     KeyLength:int32, Key, ValueLength:int32, Value ]
 *
 * The Crc is left as 0: the consumer only verifies it when check.crcs is
 * enabled (default off), and this test targets the length-prefix alignment,
 * not CRC validation.
 */
static void build_v1_message(uint8_t *buf,
                             size_t *of,
                             int64_t offset,
                             const void *key,
                             int32_t keylen,
                             const void *value,
                             int32_t vallen) {
        size_t msgsize_of, msg_start;
        int32_t msgsize;

        buf_push_i64(buf, of, offset); /* Offset */
        msgsize_of = *of;
        buf_push_i32(buf, of, 0); /* MessageSize (backpatched below) */
        msg_start = *of;

        buf_push_i32(buf, of, 0);             /* Crc (unchecked) */
        buf_push_i8(buf, of, 1);              /* MagicByte = 1 (v1) */
        buf_push_i8(buf, of, 0);              /* Attributes (no compression) */
        buf_push_i64(buf, of, 1234567890123LL); /* Timestamp (v1) */
        buf_push_i32(buf, of, keylen);        /* KeyLength */
        if (keylen > 0)
                buf_push(buf, of, key, (size_t)keylen);
        buf_push_i32(buf, of, vallen);        /* ValueLength */
        if (vallen > 0)
                buf_push(buf, of, value, (size_t)vallen);

        /* Backpatch MessageSize (bytes following the MessageSize field). */
        msgsize             = (int32_t)(*of - msg_start);
        buf[msgsize_of + 0] = (uint8_t)((msgsize >> 24) & 0xff);
        buf[msgsize_of + 1] = (uint8_t)((msgsize >> 16) & 0xff);
        buf[msgsize_of + 2] = (uint8_t)((msgsize >> 8) & 0xff);
        buf[msgsize_of + 3] = (uint8_t)(msgsize & 0xff);
}


static void do_test_legacy_msgver_flexfetch(void) {
        const char *topic       = "0154-magicv1";
        const int32_t partition = 0;
        const int msgcnt        = 50;
        rd_kafka_mock_cluster_t *mcluster;
        const char *bootstraps;
        rd_kafka_t *c;
        rd_kafka_conf_t *conf;
        rd_kafka_topic_partition_list_t *parts;
        uint8_t *buf;
        size_t of = 0;
        struct {
                char key[64];
                int32_t keylen;
                char val[512];
                int32_t vallen;
        } *exp;
        int i;
        int delivered = 0;
        int errors    = 0;
        rd_ts_t deadline;

        SUB_TEST_QUICK();

        mcluster = test_mock_cluster_new(1, &bootstraps);
        rd_kafka_mock_topic_create(mcluster, topic, 1, 1);

        /* Build a magic v1 (legacy) uncompressed message set by hand, with
         * absolute offsets 0..msgcnt-1 and varied key/value sizes so any
         * misalignment of the length prefixes is detectable. */
        exp = rd_calloc(msgcnt, sizeof(*exp));
        buf = rd_malloc(1024 * 1024);
        for (i = 0; i < msgcnt; i++) {
                int j;
                exp[i].keylen = (int32_t)rd_snprintf(exp[i].key,
                                                     sizeof(exp[i].key),
                                                     "key-%d", i);
                exp[i].vallen = (int32_t)((i * 7) % 300); /* 0..299 */
                for (j = 0; j < exp[i].vallen; j++)
                        exp[i].val[j] = (char)('A' + ((i + j) % 26));
                build_v1_message(buf, &of, i, exp[i].key, exp[i].keylen,
                                 exp[i].val, exp[i].vallen);
        }

        TEST_SAY("Pushing %d magic v1 message(s) (%" PRIusz
                 " bytes) to %s [%" PRId32 "]\n",
                 msgcnt, of, topic, partition);
        TEST_CALL_ERR__(rd_kafka_mock_partition_push_msgset_raw(
            mcluster, topic, partition, buf, of, msgcnt));
        rd_free(buf);

        /* Default consumer config => modern flexible Fetch (v12+). */
        test_conf_init(&conf, NULL, 30);
        test_conf_set(conf, "bootstrap.servers", bootstraps);
        test_conf_set(conf, "enable.auto.commit", "false");
        test_conf_set(conf, "enable.partition.eof", "true");
        /* Surface any (corruption-induced) offset reset as an error rather
         * than spinning. */
        test_conf_set(conf, "auto.offset.reset", "error");

        c = test_create_consumer("0154-grp", NULL, conf, NULL);

        parts = rd_kafka_topic_partition_list_new(1);
        rd_kafka_topic_partition_list_add(parts, topic, partition)->offset = 0;
        test_consumer_assign("ASSIGN", c, parts);
        rd_kafka_topic_partition_list_destroy(parts);

        deadline = test_clock() + 20 * 1000000;
        while (delivered < msgcnt && errors == 0 && test_clock() < deadline) {
                rd_kafka_message_t *rkm = rd_kafka_consumer_poll(c, 1000);

                if (!rkm)
                        continue;

                if (rkm->err == RD_KAFKA_RESP_ERR__PARTITION_EOF) {
                        rd_kafka_message_destroy(rkm);
                        break;
                } else if (rkm->err) {
                        errors++;
                        TEST_WARN("Consume error at offset %" PRId64 ": %s\n",
                                  rkm->offset, rd_kafka_message_errstr(rkm));
                        rd_kafka_message_destroy(rkm);
                        continue;
                }

                i = (int)rkm->offset;
                TEST_ASSERT(i >= 0 && i < msgcnt,
                            "Consumed out-of-range offset %" PRId64,
                            rkm->offset);

                TEST_ASSERT(
                    (int32_t)rkm->key_len == exp[i].keylen &&
                        (exp[i].keylen == 0 ||
                         !memcmp(rkm->key, exp[i].key, (size_t)exp[i].keylen)),
                    "offset %d: key mismatch (got len %" PRIusz
                    ", expected len %" PRId32 ")",
                    i, rkm->key_len, exp[i].keylen);

                TEST_ASSERT(
                    (int32_t)rkm->len == exp[i].vallen &&
                        (exp[i].vallen == 0 ||
                         !memcmp(rkm->payload, exp[i].val,
                                 (size_t)exp[i].vallen)),
                    "offset %d: value mismatch (got len %" PRIusz
                    ", expected len %" PRId32 ")",
                    i, rkm->len, exp[i].vallen);

                delivered++;
                rd_kafka_message_destroy(rkm);
        }

        TEST_ASSERT(errors == 0, "Expected no consume errors, got %d", errors);
        TEST_ASSERT(delivered == msgcnt,
                    "Expected %d message(s), got %d "
                    "(legacy magic v1 records misparsed over flexible Fetch?)",
                    msgcnt, delivered);

        rd_kafka_consumer_close(c);
        rd_kafka_destroy(c);
        rd_free(exp);
        test_mock_cluster_destroy(mcluster);

        SUB_TEST_PASS();
}


int main_0154_legacy_msgver_flexfetch_mock(int argc, char **argv) {
        do_test_legacy_msgver_flexfetch();
        return 0;
}
