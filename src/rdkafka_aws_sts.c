/*
 * librdkafka - The Apache Kafka C/C++ library
 *
 * Copyright (c) 2025, Confluent Inc.
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
 * @brief AWS STS GetWebIdentityToken integration.
 *
 * This file gates compilation on WITH_AWS_STS and will house the
 * SASL OAUTHBEARER token refresh callback wiring once the full
 * integration is complete.
 *
 * The actual AWS SDK call lives in rdkafka_aws_sts_impl.cpp — the only
 * .cpp file in src/ and the only file that includes AWS SDK headers.
 * This pure C file exists so that the rest of the library can call
 * into the AWS STS subsystem without touching any C++ constructs.
 */

#include "rdkafka_int.h"

#if WITH_AWS_STS

#include "rdkafka_aws_sts.h"

/* Token refresh callback wiring to rdkafka_sasl_oauthbearer.c
 * will be added here in a follow-on step. */

#endif /* WITH_AWS_STS */
