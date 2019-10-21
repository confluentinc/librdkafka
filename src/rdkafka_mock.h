/*
 * librdkafka - Apache Kafka C library
 *
 * Copyright (c) 2019 Magnus Edenhill
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

#ifndef _RDKAFKA_MOCK_H_
#define _RDKAFKA_MOCK_H_


/**
 * @name Mock cluster
 *
 * Provides a mock Kafka cluster with a configurable number of brokers
 * that support a reasonable subset of Kafka protocol operations,
 * error injection, etc.
 *
 * There are two ways to use the mock clusters, the most simple approach
 * is to configure `test.mock.num.brokers` (to e.g. 3) on the rd_kafka_t
 * in an existing application, which will replace the configured
 * `bootstrap.servers` with the mock cluster brokers.
 * This approach is convenient to easily test existing applications.
 *
 * The second approach is to explicitly create a mock cluster on an
 * rd_kafka_t instance by using rd_kafka_mock_cluster_new().
 *
 * Mock clusters provide localhost listeners that can be used as the bootstrap
 * servers by multiple rd_kafka_t instances.
 *
 * Currently supported functionality:
 *  - Producer
 *  - Idempotent Producer
 *  - Transactional Producer
 *  - Low-level consumer with offset commits (no consumer groups)
 *  - Topic Metadata and auto creation
 *
 * @remark High-level consumers making use of the balanced consumer groups
 *         are not supported.
 *
 * @remark This is an experimental public API that is NOT covered by the
 *         librdkafka API or ABI stability guarantees.
 *
 *
 * @warning THIS IS AN EXPERIMENTAL API, SUBJECT TO CHANGE OR REMOVAL.
 *
 * @{
 */

typedef struct rd_kafka_mock_cluster_s rd_kafka_mock_cluster_t;


/**
 * @brief Create new mock cluster with \p broker_cnt brokers.
 *
 * The broker ids will start at 1 up to and including \p broker_cnt.
 *
 * The \p rk instance is required for internal book keeping but continues
 * to operate as usual.
 */
rd_kafka_mock_cluster_t *rd_kafka_mock_cluster_new (rd_kafka_t *rk,
                                                    int broker_cnt);


/**
 * @brief Destroy mock cluster.
 */
void rd_kafka_mock_cluster_destroy (rd_kafka_mock_cluster_t *mcluster);



/**
 * @returns the rd_kafka_t instance for a cluster as passed to
 *          rd_kafka_mock_cluster_new().
 */
rd_kafka_t *
rd_kafka_mock_cluster_handle (const rd_kafka_mock_cluster_t *mcluster);



/**
 * @returns the mock cluster's bootstrap.servers list
 */
const char *
rd_kafka_mock_cluster_bootstraps (const rd_kafka_mock_cluster_t *mcluster);


/**
 * @brief Push \p cnt errors in the \p ... va-arg list onto the cluster's
 *        error stack for the given \p ApiKey.
 *
 * \p ApiKey is the Kafka protocol request type, e.g., ProduceRequest (0).
 *
 * The following \p cnt protocol requests matching \p ApiKey will fail with the
 * provided error code and removed from the stack, starting with
 * the first error code, then the second, etc.
 */
void rd_kafka_mock_push_request_errors (rd_kafka_mock_cluster_t *mcluster,
                                        int16_t ApiKey, size_t cnt, ...);

/**
 * @brief Set the topic error to return in protocol requests.
 *
 * Currently only used for TopicMetadataRequest and AddPartitionsToTxnRequest.
 */
void rd_kafka_mock_topic_set_error (rd_kafka_mock_cluster_t *mcluster,
                                    const char *topic,
                                    rd_kafka_resp_err_t err);

/**
 * @brief Sets the partition's preferred replica / follower.
 *
 * \p broker_id does not need to point to an existing broker.
 */
void rd_kafka_mock_partition_set_follower (rd_kafka_mock_cluster_t *mcluster,
                                           const char *topic, int32_t partition,
                                           int32_t broker_id);

/**
 * @brief Set's the broker's rack as reported in Metadata to the client.
 */
void rd_kafka_mock_broker_set_rack (rd_kafka_mock_cluster_t *mcluster,
                                    int32_t broker_id, const char *rack);

/**@}*/

#endif /* _RDKAFKA_MOCK_H_ */
