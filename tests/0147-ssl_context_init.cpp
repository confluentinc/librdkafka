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

#include <iostream>
#include <cstring>
#include <cstdlib>
#include <fstream>
#include <streambuf>
#include <openssl/ssl.h>
#include "testcpp.h"
#include "tinycthread.h"

/**
 * @name SslContextInitializationCb verification.
 *
 * Requires security.protocol=*SSL
 */
class TestContextInitializationCb : public RdKafka::SslContextInitializationCb {
 public:
  RdKafka::Conf *conf;
  bool init_ok;
  int cnt;  //< Verify callback triggered.
  mtx_t lock;

  TestContextInitializationCb(RdKafka::Conf *conf,
                              bool init_ok) : conf(conf),
                                              init_ok(init_ok),
                                              cnt(0) {
    mtx_init(&lock, mtx_plain);
  }

  ~TestContextInitializationCb() {
    mtx_destroy(&lock);
  }

  bool ssl_ctx_init_cb(void *ssl_ctx, std::string &errstr) {
    mtx_lock(&lock);

    Test::Say(tostr() << "ssl_ctx_init_cb #" << cnt << "\n");

    cnt++;
    mtx_unlock(&lock);

    std::string val;
    SSL_CTX *ctx = static_cast<SSL_CTX*>(ssl_ctx);

    if (!init_ok)
      goto error;

    // actually initialize the SSL context so that the connection attempt succeeds
    
    if (conf->get("ssl.certificate.location", val) != RdKafka::Conf::CONF_OK) {
      Test::Say("Failed to get ssl.certificate.location\n");
      goto error;
    }
    if (!SSL_CTX_use_certificate_chain_file(ctx, val.c_str())) {
      Test::Say("SSL_CTX_use_certificate_chain_file failed\n");
      goto error;
    }

    if (conf->get("ssl.key.location", val) != RdKafka::Conf::CONF_OK) {
      Test::Say("Failed to get ssl.key.location\n");
      goto error;
    }
    if (!SSL_CTX_use_PrivateKey_file(ctx, val.c_str(), SSL_FILETYPE_PEM)) {
      Test::Say("SSL_CTX_use_PrivateKey_file failed\n");
      goto error;
    }
    if ( !SSL_CTX_check_private_key(ctx) ) {
      Test::Say("SSL_CTX_check_private_key failed\n");
      goto error;
    }

    if (conf->get("ssl.ca.location", val) != RdKafka::Conf::CONF_OK) {
      Test::Say("Failed to get ssl.ca.location\n");
      goto error;
    }
    if (!SSL_CTX_load_verify_locations(ctx, val.c_str(), nullptr)) {
      Test::Say("SSL_CTX_load_verify_locations failed\n");
      goto error;
    }

    return true;

  error:
    errstr = "This test triggered an initialization failure";
    return false;
  }
};

static void do_test_context_init(const int line, bool init_ok) {
  std::string teststr = tostr() << line << ": "
                                << "SSL context init: init_ok=" << init_ok;

  Test::Say(_C_BLU "[ " + teststr + " ]\n" _C_CLR);

  RdKafka::Conf *conf;
  Test::conf_init(&conf, NULL, 10);

  std::string val;
  if (conf->get("ssl.key.location", val) != RdKafka::Conf::CONF_OK ||
      val.empty()) {
    Test::Skip("Test requires SSL to be configured\n");
    delete conf;
    return;
  }

  std::string errstr;
  conf->set("debug", "security", errstr);

  TestContextInitializationCb ctxInitCb(conf, init_ok);
  if (conf->set("ssl_ctx_init_cb", &ctxInitCb, errstr) != RdKafka::Conf::CONF_OK)
    Test::Fail("Failed to set ctxInitCb: " + errstr);

  if (conf->get("ssl_ctx_init_cb", val) != RdKafka::Conf::CONF_INVALID)
    Test::Fail("Unexpectedly got ctxInitCb as string");

  RdKafka::SslContextInitializationCb *ctxInitCbPtr;
  if (conf->get(ctxInitCbPtr) != RdKafka::Conf::CONF_OK)
    Test::Fail("Failed to get ctxInitCb");
  if (ctxInitCbPtr != &ctxInitCb)
    Test::Fail("Failed to get same ctxInitCb back");

  RdKafka::Producer *p = RdKafka::Producer::create(conf, errstr);
  mtx_lock(&ctxInitCb.lock);
  if (!ctxInitCb.cnt)
    Test::Fail("Expected at least one ctxInitCb invocation");
  mtx_unlock(&ctxInitCb.lock);

  if (init_ok) {
    if (!p)
      Test::Fail("Failed to create producer: " + errstr);
  } else {
    if (p)
      Test::Fail("Unexpectedly created producer");
  }
  delete conf;

  if (p) {
    /* Retrieving the clusterid allows us to easily check if a
     * connection could be made. */
    std::string cluster = p->clusterid(1000);
    if (cluster.empty())
      Test::Fail("Expected connection to succeed, but got clusterid '" +
                 cluster + "'");
    delete p;
  }

  Test::Say(_C_GRN "[ PASSED: " + teststr + " ]\n" _C_CLR);
}

extern "C"
int main_0147_ssl_context_init(int argc, char **argv) {
  if (!test_check_builtin("ssl")) {
    Test::Skip("Test requires SSL support\n");
    return 0;
  }

  do_test_context_init(__LINE__, true);
  do_test_context_init(__LINE__, false);

  return 0;
}
