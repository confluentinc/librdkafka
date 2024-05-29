/*
 * librdkafka - Apache Kafka C library
 *
 * Copyright (c) 2023, Confluent Inc.
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

#include "rd.h"
#include "rdrand.h"
#include "rdkafka_int.h"
#include "rdkafka_telemetry.h"
#include "rdkafka_telemetry_encode.h"
#include "rdkafka_request.h"
#include "nanopb/pb.h"
#include "rdkafka_lz4.h"
#include "snappy.h"

#if WITH_ZSTD
#include "rdkafka_zstd.h"
#endif


#define RD_KAFKA_TELEMETRY_PUSH_JITTER 20

/**
 * @brief Filters broker by availability of GetTelemetrySubscription.
 *
 * @return 0 if GetTelemetrySubscription is supported, 1 otherwise.
 *
 * @locks rd_kafka_broker_lock()
 */
static int
rd_kafka_filter_broker_by_GetTelemetrySubscription(rd_kafka_broker_t *rkb,
                                                   void *opaque) {
        int features;
        if (rd_kafka_broker_ApiVersion_supported0(
                rkb, RD_KAFKAP_GetTelemetrySubscriptions, 0, 0, &features) !=
            -1)
                return 0;
        return 1;
}

/**
 * @brief Cleans up the rk.rk_telemetry struct and frees any allocations.
 *
 * @param clear_control_flow_fields This determines if the control flow fields
 *                                  need to be cleared. This should only be set
 *                                  to true if the rk is terminating.
 * @locality main thread
 * @locks none
 * @locks_acquired rk_telemetry.lock
 */
void rd_kafka_telemetry_clear(rd_kafka_t *rk,
                              rd_bool_t clear_control_flow_fields) {
        rd_kafka_broker_t *rkb;
        if (clear_control_flow_fields) {
                mtx_lock(&rk->rk_telemetry.lock);
                if (rk->rk_telemetry.preferred_broker) {
                        rd_kafka_broker_destroy(
                            rk->rk_telemetry.preferred_broker);
                        rk->rk_telemetry.preferred_broker = NULL;
                }
                mtx_unlock(&rk->rk_telemetry.lock);
                mtx_destroy(&rk->rk_telemetry.lock);
                cnd_destroy(&rk->rk_telemetry.termination_cnd);
        }

        if (rk->rk_telemetry.accepted_compression_types_cnt) {
                rd_free(rk->rk_telemetry.accepted_compression_types);
                rk->rk_telemetry.accepted_compression_types     = NULL;
                rk->rk_telemetry.accepted_compression_types_cnt = 0;
        }

        if (rk->rk_telemetry.requested_metrics_cnt) {
                size_t i;
                for (i = 0; i < rk->rk_telemetry.requested_metrics_cnt; i++)
                        rd_free(rk->rk_telemetry.requested_metrics[i]);
                rd_free(rk->rk_telemetry.requested_metrics);
                rd_free(rk->rk_telemetry.matched_metrics);
                rk->rk_telemetry.requested_metrics     = NULL;
                rk->rk_telemetry.requested_metrics_cnt = 0;
                rk->rk_telemetry.matched_metrics       = NULL;
                rk->rk_telemetry.matched_metrics_cnt   = 0;
        }
        TAILQ_FOREACH(rkb, &rk->rk_brokers, rkb_link) {
                rd_atomic32_set(&rkb->rkb_avg_rtt.ra_v.maxv_reset, 1);
                rd_atomic32_set(&rkb->rkb_avg_outbuf_latency.ra_v.maxv_reset,
                                1);
                rd_atomic32_set(&rkb->rkb_avg_throttle.ra_v.maxv_reset, 1);
                rkb->rkb_c_historic.rkb_avg_outbuf_latency.ra_v.start =
                    rd_clock();
                rkb->rkb_c_historic.rkb_avg_rtt.ra_v.start      = rd_clock();
                rkb->rkb_c_historic.rkb_avg_throttle.ra_v.start = rd_clock();
                rkb->rkb_c_historic.assigned_partitions         = 0;
                rkb->rkb_c_historic.connects                    = 0;
                rkb->rkb_c_historic.ts_last  = rd_uclock() * 1000;
                rkb->rkb_c_historic.ts_start = rd_uclock() * 1000;
        }
        rk->rk_telemetry.telemetry_max_bytes = 0;
}

/**
 * @brief Sets the telemetry state to TERMINATED and signals the conditional
 * variable
 *
 * @locality main thread
 * @locks none
 * @locks_acquired rk_telemetry.lock
 */
static void rd_kafka_telemetry_set_terminated(rd_kafka_t *rk) {
        rd_dassert(thrd_is_current(rk->rk_thread));

        rd_kafka_dbg(rk, TELEMETRY, "TELTERM",
                     "Setting state to TERMINATED and signalling");

        rk->rk_telemetry.state = RD_KAFKA_TELEMETRY_TERMINATED;
        mtx_lock(&rk->rk_telemetry.lock);
        cnd_signal(&rk->rk_telemetry.termination_cnd);
        mtx_unlock(&rk->rk_telemetry.lock);
}

static void update_matched_metrics(rd_kafka_t *rk, size_t j) {
        rk->rk_telemetry.matched_metrics_cnt++;
        rk->rk_telemetry.matched_metrics =
            rd_realloc(rk->rk_telemetry.matched_metrics,
                       sizeof(int) * rk->rk_telemetry.matched_metrics_cnt);
        rk->rk_telemetry
            .matched_metrics[rk->rk_telemetry.matched_metrics_cnt - 1] = j;
}

static void rd_kafka_match_requested_metrics(rd_kafka_t *rk) {
        size_t metrics_cnt = RD_KAFKA_TELEMETRY_METRIC_CNT(rk), i;
        const rd_kafka_telemetry_metric_info_t *info =
            RD_KAFKA_TELEMETRY_METRIC_INFO(rk);

        if (rk->rk_telemetry.requested_metrics_cnt == 1 &&
            !strcmp(rk->rk_telemetry.requested_metrics[0],
                    RD_KAFKA_TELEMETRY_METRICS_ALL_METRICS_SUBSCRIPTION)) {
                size_t j;
                rd_kafka_dbg(rk, TELEMETRY, "RD_KAFKA_TELEMETRY_METRICS_INFO",
                             "All metrics subscribed");

                for (j = 0; j < metrics_cnt; j++)
                        update_matched_metrics(rk, j);
                return;
        }

        for (i = 0; i < rk->rk_telemetry.requested_metrics_cnt; i++) {
                size_t name_len = strlen(rk->rk_telemetry.requested_metrics[i]),
                       j;

                for (j = 0; j < metrics_cnt; j++) {
                        /* Prefix matching the requested metrics with the
                         * available metrics. */
                        char full_metric_name
                            [RD_KAFKA_TELEMETRY_METRIC_NAME_MAX_LEN];
                        rd_snprintf(full_metric_name, sizeof(full_metric_name),
                                    "%s%s", RD_KAFKA_TELEMETRY_METRIC_PREFIX,
                                    info[j].name);
                        bool name_matches =
                            strncmp(full_metric_name,
                                    rk->rk_telemetry.requested_metrics[i],
                                    name_len) == 0;

                        if (name_matches)
                                update_matched_metrics(rk, j);
                }
        }

        rd_kafka_dbg(rk, TELEMETRY, "RD_KAFKA_TELEMETRY_METRICS_INFO",
                     "Matched metrics: %" PRIdsz,
                     rk->rk_telemetry.matched_metrics_cnt);
}

/**
 * @brief Enqueues a GetTelemetrySubscriptionsRequest.
 *
 * @locks none
 * @locks_acquired none
 * @locality main thread
 */
static void rd_kafka_send_get_telemetry_subscriptions(rd_kafka_t *rk,
                                                      rd_kafka_broker_t *rkb) {
        /* Clear out the telemetry struct, free anything that is malloc'd. */
        rd_kafka_telemetry_clear(rk, rd_false /* clear_control_flow_fields */);

        /* Enqueue on broker transmit queue.
         * The preferred broker might change in the meanwhile but let it fail.
         */
        rd_kafka_dbg(rk, TELEMETRY, "GETSENT", "Sending GetTelemetryRequest");
        rd_kafka_GetTelemetrySubscriptionsRequest(
            rkb, NULL, 0, RD_KAFKA_REPLYQ(rk->rk_ops, 0),
            rd_kafka_handle_GetTelemetrySubscriptions, NULL);

        /* Change state */
        rk->rk_telemetry.state = RD_KAFKA_TELEMETRY_GET_SUBSCRIPTIONS_SENT;
}

/**
 * @brief Handles parsed GetTelemetrySubscriptions response.
 *
 * @locks none
 * @locks_acquired none
 * @locality main thread
 */
void rd_kafka_handle_get_telemetry_subscriptions(rd_kafka_t *rk,
                                                 rd_kafka_resp_err_t err) {
        rd_ts_t next_scheduled;
        double jitter_multiplier =
            rd_jitter(100 - RD_KAFKA_TELEMETRY_PUSH_JITTER,
                      100 + RD_KAFKA_TELEMETRY_PUSH_JITTER) /
            100.0;

        if (err != RD_KAFKA_RESP_ERR_NO_ERROR) {
                rd_kafka_dbg(rk, TELEMETRY, "GETERR",
                             "GetTelemetrySubscriptionsRequest failed: %s",
                             rd_kafka_err2str(err));
                if (rk->rk_telemetry.push_interval_ms == 0) {
                        rk->rk_telemetry.push_interval_ms =
                            30000; /* Default: 5min */
                }
        }

        if (err == RD_KAFKA_RESP_ERR_NO_ERROR &&
            rk->rk_telemetry.requested_metrics_cnt) {
                rd_kafka_match_requested_metrics(rk);

                /* Some metrics are requested. Start the timer accordingly */
                next_scheduled = (int)(jitter_multiplier * 1000 *
                                       rk->rk_telemetry.push_interval_ms);

                rk->rk_telemetry.state = RD_KAFKA_TELEMETRY_PUSH_SCHEDULED;
        } else {
                /* No metrics requested, or we're in error. */
                next_scheduled = rk->rk_telemetry.push_interval_ms * 1000;
                rk->rk_telemetry.state =
                    RD_KAFKA_TELEMETRY_GET_SUBSCRIPTIONS_SCHEDULED;
        }

        rd_kafka_dbg(
            rk, TELEMETRY, "GETHANDLE",
            "Handled GetTelemetrySubscriptions, scheduling FSM after "
            "%ld microseconds, state = %s, err = %s, metrics = %" PRIdsz,
            next_scheduled,
            rd_kafka_telemetry_state2str(rk->rk_telemetry.state),
            rd_kafka_err2str(err), rk->rk_telemetry.requested_metrics_cnt);

        rd_kafka_timer_start_oneshot(
            &rk->rk_timers, &rk->rk_telemetry.request_timer, rd_false,
            next_scheduled, rd_kafka_telemetry_fsm_tmr_cb, rk);
}

#if WITH_ZLIB

static int rd_kafka_compress_gzip(rd_kafka_broker_t *rkb,
                                  void *payload,
                                  size_t payload_len,
                                  void **outbuf,
                                  size_t *outlenp) {
        z_stream strm;
        int r;
        // TODO: Using the default compression level for now.
        int comp_level = Z_DEFAULT_COMPRESSION;

        memset(&strm, 0, sizeof(strm));

        r = deflateInit2(&strm, comp_level, Z_DEFLATED, 15 + 16, 8,
                         Z_DEFAULT_STRATEGY);
        if (r != Z_OK) {
                rd_rkb_log(rkb, LOG_ERR, "GZIP",
                           "Failed to initialize gzip for "
                           "compressing %" PRIusz
                           " bytes: %s (%i): "
                           "sending uncompressed",
                           payload_len, strm.msg ? strm.msg : "", r);
                return -1;
        }

        *outlenp = deflateBound(&strm, (uLong)payload_len);
        *outbuf  = rd_malloc(*outlenp);

        strm.next_in   = payload;
        strm.avail_in  = (uInt)payload_len;
        strm.next_out  = *outbuf;
        strm.avail_out = (uInt)*outlenp;

        if ((r = deflate(&strm, Z_FINISH)) != Z_STREAM_END) {
                rd_rkb_log(rkb, LOG_ERR, "GZIP",
                           "Failed to finish gzip compression "
                           " of %" PRIusz
                           " bytes: "
                           "%s (%i): "
                           "sending uncompressed",
                           payload_len, strm.msg ? strm.msg : "", r);
                deflateEnd(&strm);
                rd_free(*outbuf);
                *outbuf  = NULL;
                *outlenp = 0;
                return -1;
        }

        *outlenp = strm.total_out;

        deflateEnd(&strm);

        return 0;
}

#endif

#if WITH_SNAPPY

static int rd_kafka_compress_snappy(rd_kafka_broker_t *rkb,
                                    void *payload,
                                    size_t payload_len,
                                    void **outbuf,
                                    size_t *outlenp) {
        struct snappy_env env;
        rd_kafka_snappy_init_env_sg(&env, 1);
        int err;

        struct iovec *inpiov = NULL, *ciov = NULL;
        inpiov = rd_alloca(sizeof(*inpiov) * 1);
        ciov   = rd_alloca(sizeof(*ciov) * 1);

        inpiov[0].iov_base = payload;
        inpiov[0].iov_len  = payload_len;

        ciov[0].iov_len  = rd_kafka_snappy_max_compressed_length(payload_len);
        ciov[0].iov_base = rd_malloc(ciov[0].iov_len);


        err = rd_kafka_snappy_compress_iov(&env, inpiov, 1, payload_len, ciov);
        if (err) {
                printf("compression failed: %d\n", err);
                return -1;
        }

        rd_kafka_snappy_free_env(&env);
        *outbuf  = ciov[0].iov_base;
        *outlenp = ciov[0].iov_len;
        return 0;
}

#endif

static rd_kafka_compression_t
rd_kafka_push_telemetry_payload_compress(rd_kafka_t *rk,
                                         rd_kafka_broker_t *rkb,
                                         void *payload,
                                         size_t payload_len,
                                         void **compressed_payload,
                                         size_t *compressed_payload_size) {
        rd_kafka_compression_t compression_used = RD_KAFKA_COMPRESSION_NONE;
        size_t i;
        int r = -1;

        for (i = 0; i < rk->rk_telemetry.accepted_compression_types_cnt; i++) {
                rd_kafka_compression_t compression_type =
                    rk->rk_telemetry.accepted_compression_types[i];

                switch (compression_type) {
#if WITH_ZLIB
                case RD_KAFKA_COMPRESSION_GZIP:
                        r = rd_kafka_compress_gzip(rkb, payload, payload_len,
                                                   compressed_payload,
                                                   compressed_payload_size);
                        compression_used = RD_KAFKA_COMPRESSION_GZIP;
                        break;
#endif
                case RD_KAFKA_COMPRESSION_LZ4:
                        // TODO: Using 0 for compression level for now.
                        r = rd_kafka_lz4_compress_direct(
                            rkb, 0, payload, payload_len, compressed_payload,
                            compressed_payload_size);
                        compression_used = RD_KAFKA_COMPRESSION_LZ4;
                        break;
#if WITH_ZSTD
                case RD_KAFKA_COMPRESSION_ZSTD:
                        // TODO: Using 0 for compression level for now.
                        r = rd_kafka_zstd_compress_direct(
                            rkb, 0, payload, payload_len, compressed_payload,
                            compressed_payload_size);
                        compression_used = RD_KAFKA_COMPRESSION_ZSTD;
                        break;
#endif
#if WITH_SNAPPY
                case RD_KAFKA_COMPRESSION_SNAPPY:
                        r = rd_kafka_compress_snappy(rkb, payload, payload_len,
                                                     compressed_payload,
                                                     compressed_payload_size);
                        compression_used = RD_KAFKA_COMPRESSION_SNAPPY;
                        break;
#endif
                default:
                        break;
                }
                if (compression_used != RD_KAFKA_COMPRESSION_NONE && r == 0) {
                        rd_kafka_dbg(
                            rk, TELEMETRY, "PUSHCOMP",
                            "Compressed payload of size %" PRIusz " to %" PRIusz
                            " using compression type "
                            "%s",
                            payload_len, *compressed_payload_size,
                            rd_kafka_compression2str(compression_used));
                        rd_free(payload);
                        return compression_used;
                }
        }
        if (compression_used != RD_KAFKA_COMPRESSION_NONE && r == -1) {
                rd_kafka_dbg(rk, TELEMETRY, "PUSHCOMPERR",
                             "Failed to compress payload with available "
                             "compression types");
        }
        rd_kafka_dbg(rk, TELEMETRY, "PUSHCOMP", "Sending uncompressed payload");

        *compressed_payload      = payload;
        *compressed_payload_size = payload_len;
        return RD_KAFKA_COMPRESSION_NONE;
}


static void rd_kafka_send_push_telemetry(rd_kafka_t *rk,
                                         rd_kafka_broker_t *rkb,
                                         rd_bool_t terminating) {

        size_t metrics_payload_size = 0, compressed_metrics_payload_size = 0;
        void *metrics_payload =
                 rd_kafka_telemetry_encode_metrics(rk, &metrics_payload_size),
             *compressed_metrics_payload        = NULL;
        rd_kafka_compression_t compression_used = RD_KAFKA_COMPRESSION_NONE;

        if (rk->rk_telemetry.accepted_compression_types_cnt != 0) {
                compression_used = rd_kafka_push_telemetry_payload_compress(
                    rk, rkb, metrics_payload, metrics_payload_size,
                    &compressed_metrics_payload,
                    &compressed_metrics_payload_size);
        } else {
                rd_kafka_dbg(rk, TELEMETRY, "PUSHSENT",
                             "No compression types accepted, sending "
                             "uncompressed payload");
                compressed_metrics_payload      = metrics_payload;
                metrics_payload                 = NULL;
                compressed_metrics_payload_size = metrics_payload_size;
        }

        if (metrics_payload_size >
            (size_t)rk->rk_telemetry.telemetry_max_bytes) {
                rd_kafka_log(rk, LOG_WARNING, "TELEMETRY",
                             "Metrics payload size %" PRIdsz
                             " exceeds telemetry_max_bytes %" PRId32
                             "specified by the broker.",
                             metrics_payload_size,
                             rk->rk_telemetry.telemetry_max_bytes);
        }

        rd_kafka_dbg(rk, TELEMETRY, "PUSHSENT",
                     "Sending PushTelemetryRequest with terminating = %d",
                     terminating);
        rd_kafka_PushTelemetryRequest(
            rkb, &rk->rk_telemetry.client_instance_id,
            rk->rk_telemetry.subscription_id, terminating, compression_used,
            compressed_metrics_payload, compressed_metrics_payload_size, NULL,
            0, RD_KAFKA_REPLYQ(rk->rk_ops, 0), rd_kafka_handle_PushTelemetry,
            NULL);

        rd_free(compressed_metrics_payload);

        rk->rk_telemetry.state = terminating
                                     ? RD_KAFKA_TELEMETRY_TERMINATING_PUSH_SENT
                                     : RD_KAFKA_TELEMETRY_PUSH_SENT;
}


void rd_kafka_handle_push_telemetry(rd_kafka_t *rk, rd_kafka_resp_err_t err) {

        /* We only make a best-effort attempt to push telemetry while
         * terminating, and don't care about any errors. */
        if (rk->rk_telemetry.state ==
            RD_KAFKA_TELEMETRY_TERMINATING_PUSH_SENT) {
                rd_kafka_telemetry_set_terminated(rk);
                return;
        }

        /* There's a possiblity that we sent a PushTelemetryRequest, and
         * scheduled a termination before getting the response. In that case, we
         * will enter this method in the TERMINATED state when/if we get a
         * response, and we should not take any action. */
        if (rk->rk_telemetry.state != RD_KAFKA_TELEMETRY_PUSH_SENT)
                return;

        if (err == RD_KAFKA_RESP_ERR_NO_ERROR) {
                rd_kafka_dbg(rk, TELEMETRY, "PUSHOK",
                             "PushTelemetryRequest succeeded");
                rk->rk_telemetry.state = RD_KAFKA_TELEMETRY_PUSH_SCHEDULED;
                rd_kafka_timer_start_oneshot(
                    &rk->rk_timers, &rk->rk_telemetry.request_timer, rd_false,
                    rk->rk_telemetry.push_interval_ms * 1000,
                    rd_kafka_telemetry_fsm_tmr_cb, (void *)rk);
        } else { /* error */
                rd_kafka_dbg(rk, TELEMETRY, "PUSHERR",
                             "PushTelemetryRequest failed: %s",
                             rd_kafka_err2str(err));
                // Non-retriable errors
                if (err == RD_KAFKA_RESP_ERR_INVALID_REQUEST ||
                    err == RD_KAFKA_RESP_ERR_INVALID_RECORD) {
                        rd_kafka_log(
                            rk, LOG_WARNING, "TELEMETRY",
                            "PushTelemetryRequest failed with non-retriable "
                            "error: %s. Stopping telemetry.",
                            rd_kafka_err2str(err));
                        rd_kafka_telemetry_set_terminated(rk);
                        return;
                }

                if (err == RD_KAFKA_RESP_ERR_TELEMETRY_TOO_LARGE) {
                        rd_kafka_log(
                            rk, LOG_WARNING, "TELEMETRY",
                            "PushTelemetryRequest failed because of payload "
                            "size too large: %s. Continuing telemetry.",
                            rd_kafka_err2str(err));
                        rk->rk_telemetry.state =
                            RD_KAFKA_TELEMETRY_PUSH_SCHEDULED;
                        rd_kafka_timer_start_oneshot(
                            &rk->rk_timers, &rk->rk_telemetry.request_timer,
                            rd_false, rk->rk_telemetry.push_interval_ms * 1000,
                            rd_kafka_telemetry_fsm_tmr_cb, (void *)rk);
                        return;
                }

                rd_ts_t next_scheduled =
                    err == RD_KAFKA_RESP_ERR_UNKNOWN_SUBSCRIPTION_ID
                        ? 0
                        : rk->rk_telemetry.push_interval_ms * 1000;

                rk->rk_telemetry.state =
                    RD_KAFKA_TELEMETRY_GET_SUBSCRIPTIONS_SCHEDULED;
                rd_kafka_timer_start_oneshot(
                    &rk->rk_timers, &rk->rk_telemetry.request_timer, rd_false,
                    next_scheduled, rd_kafka_telemetry_fsm_tmr_cb, (void *)rk);
        }
}

/**
 * @brief This method starts the termination for telemetry and awaits
 * completion.
 *
 * @locks none
 * @locks_acquired rk_telemetry.lock
 * @locality app thread (normal case) or the main thread (when terminated
 *           during creation).
 */
void rd_kafka_telemetry_await_termination(rd_kafka_t *rk) {
        rd_kafka_op_t *rko;

        /* In the case where we have a termination during creation, we can't
         * send any telemetry. */
        if (thrd_is_current(rk->rk_thread) ||
            !rk->rk_conf.enable_metrics_push) {
                /* We can change state since we're on the main thread. */
                rk->rk_telemetry.state = RD_KAFKA_TELEMETRY_TERMINATED;
                return;
        }

        rko         = rd_kafka_op_new(RD_KAFKA_OP_TERMINATE_TELEMETRY);
        rko->rko_rk = rk;
        rd_kafka_q_enq(rk->rk_ops, rko);

        /* Await termination sequence completion. */
        rd_kafka_dbg(rk, TELEMETRY, "TELTERM",
                     "Awaiting termination of telemetry.");
        mtx_lock(&rk->rk_telemetry.lock);
        cnd_timedwait_ms(&rk->rk_telemetry.termination_cnd,
                         &rk->rk_telemetry.lock,
                         /* TODO(milind): Evaluate this timeout after completion
                            of all metrics push, is it too much, or too less if
                            we include serialization? */
                         1000 /* timeout for waiting */);
        mtx_unlock(&rk->rk_telemetry.lock);
        rd_kafka_dbg(rk, TELEMETRY, "TELTERM",
                     "Ended waiting for termination of telemetry.");
}

/**
 * @brief Send a final push request before terminating.
 *
 * @locks none
 * @locks_acquired none
 * @locality main thread
 * @note This method is on a best-effort basis.
 */
void rd_kafka_telemetry_schedule_termination(rd_kafka_t *rk) {
        rd_kafka_dbg(
            rk, TELEMETRY, "TELTERM",
            "Starting rd_kafka_telemetry_schedule_termination in state %s",
            rd_kafka_telemetry_state2str(rk->rk_telemetry.state));

        if (rk->rk_telemetry.state != RD_KAFKA_TELEMETRY_PUSH_SCHEDULED) {
                rd_kafka_telemetry_set_terminated(rk);
                return;
        }

        rk->rk_telemetry.state = RD_KAFKA_TELEMETRY_TERMINATING_PUSH_SCHEDULED;

        rd_kafka_dbg(rk, TELEMETRY, "TELTERM",
                     "Sending final request for Push");
        rd_kafka_timer_override_once(
            &rk->rk_timers, &rk->rk_telemetry.request_timer, 0 /* immediate */);
}


/**
 * @brief Sets telemetry broker if we are in AWAIT_BROKER state.
 *
 * @locks none
 * @locks_acquired rk_telemetry.lock
 * @locality main thread
 */
void rd_kafka_set_telemetry_broker_maybe(rd_kafka_t *rk,
                                         rd_kafka_broker_t *rkb) {
        rd_dassert(thrd_is_current(rk->rk_thread));

        /* The op triggering this method is scheduled by brokers without knowing
         * if a preferred broker is already set. If it is set, this method is a
         * no-op. */
        if (rk->rk_telemetry.state != RD_KAFKA_TELEMETRY_AWAIT_BROKER)
                return;

        mtx_lock(&rk->rk_telemetry.lock);

        if (rk->rk_telemetry.preferred_broker) {
                mtx_unlock(&rk->rk_telemetry.lock);
                return;
        }

        rd_kafka_broker_keep(rkb);
        rk->rk_telemetry.preferred_broker = rkb;

        mtx_unlock(&rk->rk_telemetry.lock);

        rd_kafka_dbg(rk, TELEMETRY, "TELBRKSET",
                     "Setting telemetry broker to %s\n", rkb->rkb_name);

        rk->rk_telemetry.state = RD_KAFKA_TELEMETRY_GET_SUBSCRIPTIONS_SCHEDULED;

        rd_kafka_timer_start_oneshot(
            &rk->rk_timers, &rk->rk_telemetry.request_timer, rd_false,
            0 /* immediate */, rd_kafka_telemetry_fsm_tmr_cb, (void *)rk);
}

/**
 * @brief Returns the preferred metrics broker or NULL if unavailable.
 *
 * @locks none
 * @locks_acquired rk_telemetry.lock, rd_kafka_wrlock()
 * @locality main thread
 */
static rd_kafka_broker_t *rd_kafka_get_preferred_broker(rd_kafka_t *rk) {
        rd_kafka_broker_t *rkb = NULL;

        mtx_lock(&rk->rk_telemetry.lock);
        if (rk->rk_telemetry.preferred_broker)
                rkb = rk->rk_telemetry.preferred_broker;
        else {
                /* If there is no preferred broker, that means that our previous
                 * one failed. Iterate through all available brokers to find
                 * one. */
                rd_kafka_wrlock(rk);
                rkb = rd_kafka_broker_random_up(
                    rk, rd_kafka_filter_broker_by_GetTelemetrySubscription,
                    NULL);
                rd_kafka_wrunlock(rk);

                /* No need to increase refcnt as broker_random_up does it
                 * already. */
                rk->rk_telemetry.preferred_broker = rkb;

                rd_kafka_dbg(rk, TELEMETRY, "TELBRKSET",
                             "Lost preferred broker, switching to new "
                             "preferred broker %d\n",
                             rkb ? rd_kafka_broker_id(rkb) : -1);
        }
        mtx_unlock(&rk->rk_telemetry.lock);

        return rkb;
}

/**
 * @brief Progress the telemetry state machine.
 *
 * @locks none
 * @locks_acquired none
 * @locality main thread
 */
static void rd_kafka_telemetry_fsm(rd_kafka_t *rk) {
        rd_kafka_broker_t *preferred_broker = NULL;

        rd_dassert(rk);
        rd_dassert(thrd_is_current(rk->rk_thread));

        switch (rk->rk_telemetry.state) {
        case RD_KAFKA_TELEMETRY_AWAIT_BROKER:
                rd_dassert(!*"Should never be awaiting a broker when the telemetry fsm is called.");
                break;

        case RD_KAFKA_TELEMETRY_GET_SUBSCRIPTIONS_SCHEDULED:
                preferred_broker = rd_kafka_get_preferred_broker(rk);
                if (!preferred_broker) {
                        rk->rk_telemetry.state =
                            RD_KAFKA_TELEMETRY_AWAIT_BROKER;
                        break;
                }
                rd_kafka_send_get_telemetry_subscriptions(rk, preferred_broker);
                break;

        case RD_KAFKA_TELEMETRY_PUSH_SCHEDULED:
                preferred_broker = rd_kafka_get_preferred_broker(rk);
                if (!preferred_broker) {
                        rk->rk_telemetry.state =
                            RD_KAFKA_TELEMETRY_AWAIT_BROKER;
                        break;
                }
                rd_kafka_send_push_telemetry(rk, preferred_broker, rd_false);
                break;

        case RD_KAFKA_TELEMETRY_PUSH_SENT:
        case RD_KAFKA_TELEMETRY_GET_SUBSCRIPTIONS_SENT:
        case RD_KAFKA_TELEMETRY_TERMINATING_PUSH_SENT:
                rd_dassert(!*"Should never be awaiting response when the telemetry fsm is called.");
                break;

        case RD_KAFKA_TELEMETRY_TERMINATING_PUSH_SCHEDULED:
                preferred_broker = rd_kafka_get_preferred_broker(rk);
                if (!preferred_broker) {
                        /* If there's no preferred broker, set state to
                         * terminated immediately to stop the app thread from
                         * waiting indefinitely. */
                        rd_kafka_telemetry_set_terminated(rk);
                        break;
                }
                rd_kafka_send_push_telemetry(rk, preferred_broker, rd_true);
                break;

        case RD_KAFKA_TELEMETRY_TERMINATED:
                rd_dassert(!*"Should not be terminated when the telemetry fsm is called.");
                break;

        default:
                rd_assert(!*"Unknown state");
        }
}

/**
 * @brief Callback for FSM timer.
 *
 * @locks none
 * @locks_acquired none
 * @locality main thread
 */
void rd_kafka_telemetry_fsm_tmr_cb(rd_kafka_timers_t *rkts, void *rk) {
        rd_kafka_telemetry_fsm(rk);
}
