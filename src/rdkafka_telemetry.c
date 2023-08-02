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
#include "rdkafka_request.h"

/**
 * @brief Returns the preferred metrics broker or NULL if unavailable.
 *
 * @locks_acquired rk_telemetry.lock
 */
static rd_kafka_broker_t *rd_kafka_get_preferred_broker(rd_kafka_t *rk) {
        rd_kafka_broker_t *rkb = NULL;

        mtx_lock(&rk->rk_telemetry.lock);
        if (rk->rk_telemetry.preferred_broker) {
                rkb = rk->rk_telemetry.preferred_broker;
        }
        /* TODO: handle recalculation of preferred broker in case broker goes
         * down. For now just return. */
        mtx_unlock(&rk->rk_telemetry.lock);
        return rkb;
}

/**
 * @brief Cleans up the rk.rk_telemetry struct and frees any allocations.
 *
 * @param clear_control_flow_fields This determines if the control flow fields
 *                                  need to be cleared. This should only be set
 *                                  to true if the rk is terminating.
 */
void rd_kafka_telemetry_clear(rd_kafka_t *rk,
                              rd_bool_t clear_control_flow_fields) {
        if (clear_control_flow_fields) {
                mtx_lock(&rk->rk_telemetry.lock);
                if (rk->rk_telemetry.preferred_broker) {
                        rd_kafka_broker_destroy(
                            rk->rk_telemetry.preferred_broker);
                        rk->rk_telemetry.preferred_broker = NULL;
                }
                mtx_unlock(&rk->rk_telemetry.lock);
                mtx_destroy(&rk->rk_telemetry.lock);
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
                rk->rk_telemetry.requested_metrics     = NULL;
                rk->rk_telemetry.requested_metrics_cnt = 0;
        }
}

/**
 * @brief Enqueues a GetTelemetrySubscriptionsRequest.
 *
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


void rd_kafka_handle_get_telemetry_subscriptions(rd_kafka_t *rk,
                                                 rd_kafka_resp_err_t err) {
        rd_ts_t next_scheduled;

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
                /* Some metrics are requested. Start the timer accordingly */
                next_scheduled = rd_jitter(0.8, 1.2) * 1000 *
                                 rk->rk_telemetry.push_interval_ms;

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

static void rd_kafka_send_push_telemetry(rd_kafka_t *rk,
                                         rd_kafka_broker_t *rkb,
                                         rd_bool_t terminating) {

        // TODO: Need to cycle through requested_metrics.
        // TODO: Metrics processing. Update historic, calculate deltas, and
        // serialize.

        // TODO: Update dummy values
        void *metrics_payload  = NULL;
        char *compression_type = "gzip";

        rd_kafka_dbg(rk, TELEMETRY, "PUSHSENT", "Sending PushTelemetryRequest");
        rd_kafka_PushTelemetryRequest(
            rkb, &rk->rk_telemetry.client_instance_id,
            rk->rk_telemetry.subscription_id, terminating, compression_type,
            metrics_payload, 0, NULL, 0, RD_KAFKA_REPLYQ(rk->rk_ops, 0),
            rd_kafka_handle_PushTelemetry, NULL);
        rk->rk_telemetry.state = RD_KAFKA_TELEMETRY_PUSH_SENT;
}

void rd_kafka_handle_push_telemetry(rd_kafka_t *rk, rd_kafka_resp_err_t err) {
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
                rk->rk_telemetry.state =
                    RD_KAFKA_TELEMETRY_GET_SUBSCRIPTIONS_SCHEDULED;
                rd_kafka_timer_start_oneshot(
                    &rk->rk_timers, &rk->rk_telemetry.request_timer, rd_false,
                    rk->rk_telemetry.push_interval_ms * 1000,
                    rd_kafka_telemetry_fsm_tmr_cb, (void *)rk);
        }
}

/**
 * @brief Progress the telemetry state machine.
 *
 * @locality main thread
 */
static void rd_kafka_telemetry_fsm(rd_kafka_t *rk) {
        rd_kafka_telemetry_state_t state;
        rd_kafka_broker_t *preferred_broker;

        rd_dassert(rk);

        /* We don't require a lock here, as the only way we can reach this
         * function is if we've already set the state from the broker thread,
         * and further state transitions happen only on the main thread. */
        mtx_lock(&rk->rk_telemetry.lock);
        state = rk->rk_telemetry.state;
        mtx_unlock(&rk->rk_telemetry.lock);
        switch (state) {
        case RD_KAFKA_TELEMETRY_AWAIT_BROKER:
                rd_dassert(!*"Should never be awaiting a broker when the telemetry fsm is called.");
                break;

        case RD_KAFKA_TELEMETRY_GET_SUBSCRIPTIONS_SCHEDULED:
                preferred_broker = rd_kafka_get_preferred_broker(rk);
                if (!preferred_broker) {
                        state = RD_KAFKA_TELEMETRY_AWAIT_BROKER;
                        break;
                }
                rd_kafka_send_get_telemetry_subscriptions(rk, preferred_broker);
                break;

        case RD_KAFKA_TELEMETRY_PUSH_SCHEDULED:
                preferred_broker = rd_kafka_get_preferred_broker(rk);
                if (!preferred_broker) {
                        state = RD_KAFKA_TELEMETRY_AWAIT_BROKER;
                        break;
                }
                rd_kafka_send_push_telemetry(rk, preferred_broker, rd_false);
                break;

        case RD_KAFKA_TELEMETRY_PUSH_SENT:
        case RD_KAFKA_TELEMETRY_GET_SUBSCRIPTIONS_SENT:
                rd_dassert(!*"Should never be awaiting response when the telemetry fsm is called.");
                break;

        case RD_KAFKA_TELEMETRY_TERMINATING:
                preferred_broker = rd_kafka_get_preferred_broker(rk);
                if (!preferred_broker) {
                        state = RD_KAFKA_TELEMETRY_AWAIT_BROKER;
                        break;
                }
                rd_kafka_send_push_telemetry(rk, preferred_broker, rd_true);
                break;

        default:
                rd_assert(!*"Unknown state");
        }
}

void rd_kafka_telemetry_fsm_tmr_cb(rd_kafka_timers_t *rkts, void *rk) {
        rd_kafka_telemetry_fsm(rk);
}
