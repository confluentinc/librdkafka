/*
 * librdkafka - Apache Kafka C library
 *
 * Copyright (c) 2018, Magnus Edenhill
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
 * @name Thin abstraction on top of sockem to provide scheduled delays,
 *       e.g.; set delay to 500ms in 2000ms
 */

#include "test.h"
#include "sockem.h"
#include "sockem_ctrl.h"

static int sockem_ctrl_thrd_main (void *arg) {
        sockem_ctrl_t *ctrl = (sockem_ctrl_t *)arg;

        mtx_lock(&ctrl->lock);

        test_curr = ctrl->test;

        while (!ctrl->term) {
                int64_t now;

                cnd_timedwait_ms(&ctrl->cnd, &ctrl->lock, 10);

                if (ctrl->cmd.ts_at) {
                        ctrl->next.ts_at = ctrl->cmd.ts_at;
                        ctrl->next.delay = ctrl->cmd.delay;
                        ctrl->cmd.ts_at = 0;
                        ctrl->cmd.ack = 1;
                        printf(_C_CYA "## %s: sockem: "
                               "received command to set delay "
                               "to %d in %dms\n" _C_CLR,
                               __FILE__,
                               ctrl->next.delay,
                               (int)(ctrl->next.ts_at - test_clock()) / 1000);

                }

                now = test_clock();
                if (ctrl->next.ts_at && now > ctrl->next.ts_at) {
                        printf(_C_CYA "## %s: "
                               "sockem: setting socket delay to %d\n" _C_CLR,
                               __FILE__, ctrl->next.delay);
                        test_socket_sockem_set_all("delay", ctrl->next.delay);
                        ctrl->next.ts_at = 0;
                        cnd_signal(&ctrl->cnd); /* signal back to caller */
                }
        }
        mtx_unlock(&ctrl->lock);

        return 0;
}



/**
 * @brief Set socket delay to kick in after \p after ms
 *
 * @remark Must not be used concurrently.
 */
void sockem_ctrl_set_delay (sockem_ctrl_t *ctrl, int after, int delay) {
        TEST_SAY("Set delay to %dms (after %dms)\n", delay, after);

        mtx_lock(&ctrl->lock);
        ctrl->cmd.ts_at = test_clock() + (after*1000);
        ctrl->cmd.delay = delay;
        ctrl->cmd.ack = 0;
        cnd_broadcast(&ctrl->cnd);

        /* Wait for ack from sockem thread */
        while (!ctrl->cmd.ack) {
                TEST_SAY("Waiting for sockem control ack\n");
                cnd_timedwait_ms(&ctrl->cnd, &ctrl->lock, 1000);
        }
        mtx_unlock(&ctrl->lock);
}


void sockem_ctrl_init (sockem_ctrl_t *ctrl) {
        memset(ctrl, 0, sizeof(*ctrl));
        mtx_init(&ctrl->lock, mtx_plain);
        cnd_init(&ctrl->cnd);
        ctrl->test = test_curr;

        mtx_lock(&ctrl->lock);
        if (thrd_create(&ctrl->thrd, sockem_ctrl_thrd_main,
                        ctrl) != thrd_success)
                TEST_FAIL("Failed to create sockem ctrl thread");
        mtx_unlock(&ctrl->lock);
}

void sockem_ctrl_term (sockem_ctrl_t *ctrl) {
        /* Join controller thread */
        mtx_lock(&ctrl->lock);
        ctrl->term = 1;
        mtx_unlock(&ctrl->lock);
        thrd_join(ctrl->thrd, NULL);

        cnd_destroy(&ctrl->cnd);
        mtx_destroy(&ctrl->lock);
}
