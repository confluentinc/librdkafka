/*
 * librdkafka - The Apache Kafka C/C++ library
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

#ifndef _RDKAFKA_SASL_OAUTHBEARER_H_
#define _RDKAFKA_SASL_OAUTHBEARER_H_

#include "rdkafka.h"

/**
 * @brief Default SASL/OAUTHBEARER token refresh callback that generates
 * unsecured JWTs as per https://tools.ietf.org/html/rfc7515#appendix-A.5.
 *
 * This method interprets \c sasl.oauthbearer.config as space-separated
 * name=value pairs with valid names including principalClaimName,
 * principal, scopeClaimName, scope, and lifeSeconds. The default
 * value for principalClaimName is sub.  The principal must be specified.
 * The default value for scopeClaimName is scope, and the default value
 * for lifeSeconds is 3600.  The scope value is csv format with the
 * default value being no/empty scope. For example:
 * "principalClaimName=azp principal=admin scopeClaimName=roles
 * scope=role1,role2 lifeSeconds=600".  SASL extensions can be
 * communicated to the broker via extension_<extensionname>=value. For
 * example: "principal=admin extension_traceId=123". Unrecognized names
 * are ignored.
 */
void rd_kafka_oauthbearer_unsecured_token(rd_kafka_t *rk, void *opaque);

/**
 * @brief Enqueue a token refresh.
 * 
 * A write lock must be acquired prior to calling this method via
 * \c rwlock_wrlock(&rk->rk_oauthbearer->refresh_lock) so that the
 * caller can be certain that nobody else is enqueuing a token refresh
 * operation -- otherwise multiple operations may be enqueued.  The caller
 * remains responsible for releasing the lock (this method does not release
 * it).
 */
void rd_kafka_oauthbearer_enqueue_token_refresh(rd_kafka_t *rk);

/**
 * @brief Enqueue a token refresh if necessary.
 * 
 * The method \c rd_kafka_oauthbearer_enqueue_token_refresh() is invoked
 * if necessary; all necessary locks are acquired and released.
 */
void rd_kafka_oauthbearer_enqueue_token_refresh_if_necessary(rd_kafka_t *rk);


#endif /* _RDKAFKA_SASL_OAUTHBEARER_H_ */
