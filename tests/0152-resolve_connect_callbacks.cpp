/*
 * librdkafka - Apache Kafka C library
 *
 * Copyright (c) 2019-2022, Magnus Edenhill
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
#include "testcpp.h"


class testResolveCb : public RdKafka::ResolveCb {
 public:
  int resolve_cb(const char *node,
                 const char *service,
                 const struct addrinfo *hints,
                 struct addrinfo **res) {
    Test::Say("Invoked testResolveCb callback");

    // If node and service are null, free the resources pointed to by res.
    if ((!node && !service) && *res) {
      delete (*res);
      return 0;
    }

    TEST_ASSERT(strcmp("not.a.real.dns.name", node) == 0,
                "resolver received an unexpected hostname: %s", node);

    TEST_ASSERT(strcmp("9092", service) == 0,
                "resolver received an unexpected port: %s", service);

    // Construct a valid resolver response pointing to a bogus endpoint.
    sockaddr_in *addr     = new sockaddr_in;
    addr->sin_family      = AF_INET;
    addr->sin_port        = htons(1234);
    addr->sin_addr.s_addr = htonl(0x7f010203);  // 127.1.2.3

    addrinfo *endpoint    = new addrinfo;
    endpoint->ai_family   = AF_INET;
    endpoint->ai_socktype = SOCK_STREAM;
    endpoint->ai_protocol = IPPROTO_TCP;
    endpoint->ai_addrlen  = sizeof(addr);
    endpoint->ai_next     = nullptr;
    endpoint->ai_addr     = reinterpret_cast<sockaddr *>(addr);

    *res = endpoint;

    return 0;
  }
};


class testConnectCb : public RdKafka::ConnectCb {
 public:
  int connect_cb(int sockfd,
                 const struct sockaddr *addr,
                 int addrlen,
                 const char *id) {
    Test::Say("Invoked testConnectCb callback");

    const struct sockaddr_in *addr_in = reinterpret_cast<const sockaddr_in *>(
        reinterpret_cast<const void *>(addr));

    // These assertions ensure we've received the expected resolver response.
    TEST_ASSERT(addr_in->sin_addr.s_addr == htonl(0x7f010203),
                "address has unexpected host: 0x%x",
                ntohl(addr_in->sin_addr.s_addr));

    TEST_ASSERT(addr_in->sin_port == htons(1234),
                "address has unexpected port: %d", ntohs(addr_in->sin_port));

    // Indicate a failure to connect, as we've already passed at this point.
    return -1;
  }
};


extern "C" {
int main_0152_resolve_connect_callbacks(int argc, char **argv) {
  RdKafka::Conf *conf;
  std::string errstr;

  Test::conf_init(&conf, NULL, 20);
  /* Pass in an invalid broken name, as it will be unused */
  Test::conf_set(conf, "bootstrap.servers", "not.a.real.dns.name");

  testResolveCb pResolve = testResolveCb();
  testConnectCb pConnect = testConnectCb();

  if (conf->set("resolve_cb", &pResolve, errstr) != RdKafka::Conf::CONF_OK)
    Test::Fail(errstr);
  if (conf->set("connect_cb", &pConnect, errstr) != RdKafka::Conf::CONF_OK)
    Test::Fail(errstr);

  Test::Say("Test Producer Connection\n");

  RdKafka::Producer *p = RdKafka::Producer::create(conf, errstr);
  if (!p)
    Test::Fail("Failed to create Producer: " + errstr);

  delete p;

  return 0;
}
}
