#include "rd.h"
#include "rdunittest.h"
#include "rdkafka_int.h"
#include "rdkafka_queue.h"
#include "rdkafka_fetcher.h"
#include "rdkafka_partition.h"

static rd_kafka_t *ut_create_rk(void) {
        rd_kafka_conf_t *conf = rd_kafka_conf_new();
        char errstr[128];

        if (rd_kafka_conf_set(conf, "group.id", "ut-share-filter", errstr,
                              sizeof(errstr)) != RD_KAFKA_CONF_OK) {
                rd_kafka_conf_destroy(conf);
                return NULL;
        }

        rd_kafka_t *rk =
            rd_kafka_new(RD_KAFKA_CONSUMER, conf, errstr, sizeof(errstr));
        if (!rk)
                rd_kafka_conf_destroy(conf);

        return rk;
}

static rd_kafka_toppar_t *ut_create_toppar(int32_t partition) {
        rd_kafka_toppar_t *rktp = rd_calloc(1, sizeof(*rktp));
        if (!rktp)
                return NULL;

        rktp->rktp_partition                  = partition;
        rktp->rktp_share_acknowledgement_list = NULL;
        rktp->rktp_share_acknowledge_count    = 0;

        return rktp;
}

static void ut_destroy_toppar(rd_kafka_toppar_t *rktp) {
        if (!rktp)
                return;

        if (rktp->rktp_share_acknowledgement_list) {
                rd_list_destroy(rktp->rktp_share_acknowledgement_list);
                rktp->rktp_share_acknowledgement_list = NULL;
        }

        rd_free(rktp);
}

static rd_kafka_broker_t *ut_create_broker(rd_kafka_t *rk) {
        rd_kafka_broker_t *rkb = rd_calloc(1, sizeof(*rkb));
        if (rkb)
                rkb->rkb_rk = rk;
        return rkb;
}

static rd_kafka_op_t *
ut_make_fetch_op(rd_kafka_t *rk RD_UNUSED, rd_kafka_toppar_t *rktp,
                 int64_t offset) {
        rd_kafka_op_t *rko = rd_kafka_op_new(RD_KAFKA_OP_FETCH);
        rko->rko_flags |= RD_KAFKA_OP_F_FREE;
        rko->rko_u.fetch.rkm.rkm_rkmessage.rkt       = NULL;
        rko->rko_u.fetch.rkm.rkm_rkmessage.partition = rktp->rktp_partition;
        rko->rko_u.fetch.rkm.rkm_rkmessage.offset    = offset;
        rko->rko_u.fetch.rkm.rkm_rkmessage.err       = RD_KAFKA_RESP_ERR_NO_ERROR;
        return rko;
}

static int ut_expect_appq_offsets(rd_kafka_q_t *q,
                                  const int64_t *expected,
                                  size_t exp_cnt) {
        RD_UT_ASSERT(rd_kafka_q_len(q) == (int)exp_cnt,
                     "appq len %d != %zu", rd_kafka_q_len(q), exp_cnt);

        for (size_t i = 0; i < exp_cnt; i++) {
                rd_kafka_op_t *rko =
                    rd_kafka_q_pop(q, RD_POLL_NOWAIT, 0);
                RD_UT_ASSERT(rko, "appq pop NULL at idx %zu", i);

                RD_UT_ASSERT(rd_kafka_op_get_offset(rko) == expected[i],
                             "offset[%zu] %" PRId64 " != %" PRId64, i,
                             rd_kafka_op_get_offset(rko), expected[i]);

                rd_kafka_op_destroy(rko);
        }

        return 0;
}

static int ut_expect_ack_entry(struct rd_kafka_toppar_share_ack_entry *e,
                               rd_kafka_share_acknowledgement_type type,
                               int64_t first,
                               int64_t last,
                               int16_t dcnt) {
        RD_UT_ASSERT(e, "ack entry NULL");
        RD_UT_ASSERT(e->type == type, "ack type %d != %d", e->type, type);
        RD_UT_ASSERT(e->first_offset == first, "first %" PRId64 " != %" PRId64,
                     e->first_offset, first);
        RD_UT_ASSERT(e->last_offset == last, "last %" PRId64 " != %" PRId64,
                     e->last_offset, last);

        if (type == RD_KAFKA_SHARE_ACK_ACCEPT)
                RD_UT_ASSERT(e->delivery_count == dcnt,
                             "dcnt %d != %d", e->delivery_count, dcnt);
        else
                RD_UT_ASSERT(e->delivery_count == 0,
                             "gap dcnt %d != 0", e->delivery_count);

        return 0;
}

static int ut_case_full_accept(rd_kafka_t *rk) {
        rd_kafka_broker_t *rkb = ut_create_broker(rk);
        RD_UT_ASSERT(rkb != NULL, "broker alloc failed");

        rd_kafka_toppar_t *rktp = ut_create_toppar(0);
        RD_UT_ASSERT(rktp != NULL, "toppar alloc failed");

        rd_kafka_q_t *temp_fetchq = rd_kafka_q_new(rk);
        rd_kafka_q_t *temp_appq   = rd_kafka_q_new(rk);

        for (int64_t off = 0; off <= 4; off++)
                rd_kafka_q_enq(temp_fetchq, ut_make_fetch_op(rk, rktp, off));

        int32_t cnt     = 1;
        int64_t first[] = {0};
        int64_t last[]  = {4};
        int16_t dcnt[]  = {1};

        rd_kafka_share_filter_forward(rkb, rktp, temp_fetchq, temp_appq, cnt,
                                      first, last, dcnt);

        RD_UT_ASSERT(
            rd_list_cnt(rktp->rktp_share_acknowledgement_list) == 1,
            "ack cnt %d != 1",
            rd_list_cnt(rktp->rktp_share_acknowledgement_list));

        if (ut_expect_ack_entry(
                rd_list_elem(rktp->rktp_share_acknowledgement_list, 0),
                RD_KAFKA_SHARE_ACK_ACCEPT, 0, 4, 1))
                return 1;

        int64_t expected[] = {0, 1, 2, 3, 4};
        if (ut_expect_appq_offsets(temp_appq, expected,
                                   RD_ARRAYSIZE(expected)))
                return 1;

        rd_kafka_q_destroy_owner(temp_appq);
        ut_destroy_toppar(rktp);
        rd_free(rkb);

        return 0;
}

static int ut_case_accept_and_gaps(rd_kafka_t *rk) {
        rd_kafka_broker_t *rkb = ut_create_broker(rk);
        RD_UT_ASSERT(rkb != NULL, "broker alloc failed");

        rd_kafka_toppar_t *rktp = ut_create_toppar(0);
        RD_UT_ASSERT(rktp != NULL, "toppar alloc failed");

        rd_kafka_q_t *temp_fetchq = rd_kafka_q_new(rk);
        rd_kafka_q_t *temp_appq   = rd_kafka_q_new(rk);
        int64_t present[]         = {0, 1, 3, 4, 6};

        for (size_t i = 0; i < RD_ARRAYSIZE(present); i++)
                rd_kafka_q_enq(temp_fetchq,
                               ut_make_fetch_op(rk, rktp, present[i]));

        int32_t cnt     = 1;
        int64_t first[] = {0};
        int64_t last[]  = {6};
        int16_t dcnt[]  = {2};

        rd_kafka_share_filter_forward(rkb, rktp, temp_fetchq, temp_appq, cnt,
                                      first, last, dcnt);

        RD_UT_ASSERT(
            rd_list_cnt(rktp->rktp_share_acknowledgement_list) == 5,
            "ack cnt %d != 5",
            rd_list_cnt(rktp->rktp_share_acknowledgement_list));

        if (ut_expect_ack_entry(
                rd_list_elem(rktp->rktp_share_acknowledgement_list, 0),
                RD_KAFKA_SHARE_ACK_ACCEPT, 0, 1, 2) ||
            ut_expect_ack_entry(
                rd_list_elem(rktp->rktp_share_acknowledgement_list, 1),
                RD_KAFKA_SHARE_ACK_GAP, 2, 2, 0) ||
            ut_expect_ack_entry(
                rd_list_elem(rktp->rktp_share_acknowledgement_list, 2),
                RD_KAFKA_SHARE_ACK_ACCEPT, 3, 4, 2) ||
            ut_expect_ack_entry(
                rd_list_elem(rktp->rktp_share_acknowledgement_list, 3),
                RD_KAFKA_SHARE_ACK_GAP, 5, 5, 0) ||
            ut_expect_ack_entry(
                rd_list_elem(rktp->rktp_share_acknowledgement_list, 4),
                RD_KAFKA_SHARE_ACK_ACCEPT, 6, 6, 2))
                return 1;

        if (ut_expect_appq_offsets(temp_appq, present,
                                   RD_ARRAYSIZE(present)))
                return 1;

        rd_kafka_q_destroy_owner(temp_appq);
        ut_destroy_toppar(rktp);
        rd_free(rkb);

        return 0;
}

static int ut_case_all_gap(rd_kafka_t *rk) {
        rd_kafka_broker_t *rkb = ut_create_broker(rk);
        RD_UT_ASSERT(rkb != NULL, "broker alloc failed");

        rd_kafka_toppar_t *rktp = ut_create_toppar(0);
        RD_UT_ASSERT(rktp != NULL, "toppar alloc failed");

        rd_kafka_q_t *temp_fetchq = rd_kafka_q_new(rk);
        rd_kafka_q_t *temp_appq   = rd_kafka_q_new(rk);

        int32_t cnt     = 1;
        int64_t first[] = {10};
        int64_t last[]  = {12};
        int16_t dcnt[]  = {3};

        rd_kafka_share_filter_forward(rkb, rktp, temp_fetchq, temp_appq, cnt,
                                      first, last, dcnt);

        RD_UT_ASSERT(
            rd_list_cnt(rktp->rktp_share_acknowledgement_list) == 1,
            "ack cnt %d != 1",
            rd_list_cnt(rktp->rktp_share_acknowledgement_list));

        if (ut_expect_ack_entry(
                rd_list_elem(rktp->rktp_share_acknowledgement_list, 0),
                RD_KAFKA_SHARE_ACK_GAP, 10, 12, 0))
                return 1;

        RD_UT_ASSERT(rd_kafka_q_len(temp_appq) == 0,
                     "appq len %d != 0", rd_kafka_q_len(temp_appq));

        rd_kafka_q_destroy_owner(temp_appq);
        ut_destroy_toppar(rktp);
        rd_free(rkb);

        return 0;
}

int unittest_fetcher_share_filter_forward(void) {
        rd_kafka_t *rk = ut_create_rk();
        RD_UT_ASSERT(rk != NULL, "rd_kafka_new failed");

        if (ut_case_full_accept(rk) ||
            ut_case_accept_and_gaps(rk) ||
            ut_case_all_gap(rk)) {
                rd_kafka_destroy(rk);
                return 1;
        }

        rd_kafka_destroy(rk);
        RD_UT_PASS();
}