/*
 * librdkafka - Apache Kafka C library
 *
 * Copyright (c) 2019, Magnus Edenhill
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
  * Example of utilizing the Windows Certificate store with SSL.
  */

#include <iostream>
#include <string>
#include <cstdlib>
#include <cstdio>
#include <csignal>
#include <cstring>
#include <sstream>
#include <chrono>

#include "../win32/wingetopt.h"
#include <windows.h>
#include <wincrypt.h>

  /*
   * Typically include path in a real application would be
   * #include <librdkafka/rdkafkacpp.h>
   */
#include "rdkafkacpp.h"

class ExampleDeliveryReportCb : public RdKafka::DeliveryReportCb {
public:
    void dr_cb(RdKafka::Message& message) {
        std::string status_name;
        switch (message.status())
        {
        case RdKafka::Message::MSG_STATUS_NOT_PERSISTED:
            status_name = "NotPersisted";
            break;
        case RdKafka::Message::MSG_STATUS_POSSIBLY_PERSISTED:
            status_name = "PossiblyPersisted";
            break;
        case RdKafka::Message::MSG_STATUS_PERSISTED:
            status_name = "Persisted";
            break;
        default:
            status_name = "Unknown?";
            break;
        }
        std::cout << "Message delivery for (" << message.len() << " bytes): " <<
            status_name << ": " << message.errstr() << std::endl;
        if (message.key())
            std::cout << "Key: " << *(message.key()) << ";" << std::endl;
    }
};

class PrintingSSLVerifyCb : public RdKafka::SslCertificateVerifyCb {
    /* This SSL cert verification callback simply prints the certificates
     * in the certificate chain.
     * It provides no validation, everything is ok. */
public:
    bool ssl_cert_verify_cb(const std::string& broker_name,
        int32_t broker_id,
        int* x509_error,
        int depth,
        const char* buf, size_t size,
        std::string& errstr) {
        PCCERT_CONTEXT ctx = CertCreateCertificateContext(
            X509_ASN_ENCODING | PKCS_7_ASN_ENCODING,
            (const uint8_t*)buf, static_cast<unsigned long>(size));

        if (!ctx)
            std::cerr << "Failed to parse certificate" << std::endl;

        char subject[256] = "n/a";
        char issuer[256] = "n/a";

        CertGetNameStringA(ctx, CERT_NAME_FRIENDLY_DISPLAY_TYPE,
            0, NULL,
            subject, sizeof(subject));

        CertGetNameStringA(ctx, CERT_NAME_FRIENDLY_DISPLAY_TYPE,
            CERT_NAME_ISSUER_FLAG, NULL,
            issuer, sizeof(issuer));

        std::cerr << "Broker " << broker_name <<
            " (" << broker_id << "): " <<
            "certificate depth " << depth <<
            ", X509 error " << *x509_error <<
            ", subject " << subject <<
            ", issuer " << issuer << std::endl;

        if (ctx)
            CertFreeCertificateContext(ctx);

        return true;
    }
};


/**
* @brief Print the brokers in the cluster.
*/
static void print_brokers(RdKafka::Handle* handle,
    const RdKafka::Metadata* md) {
    std::cout << md->brokers()->size() << " broker(s) in cluster " <<
        handle->clusterid(0) << std::endl;

    /* Iterate brokers */
    RdKafka::Metadata::BrokerMetadataIterator ib;
    for (ib = md->brokers()->begin(); ib != md->brokers()->end(); ++ib)
        std::cout << "  broker " << (*ib)->id() << " at "
        << (*ib)->host() << ":" << (*ib)->port() << std::endl;

}

void msg_consume(RdKafka::Message* message, void* opaque) {
    const RdKafka::Headers* headers;

    switch (message->err()) {
    case RdKafka::ERR__TIMED_OUT:
        break;

    case RdKafka::ERR_NO_ERROR:
        /* Real message */
        std::cout << "Read msg at offset " << message->offset() << std::endl;
        if (message->key()) {
            std::cout << "Key: " << *message->key() << std::endl;
        }
        headers = message->headers();
        if (headers) {
            std::vector<RdKafka::Headers::Header> hdrs = headers->get_all();
            for (size_t i = 0; i < hdrs.size(); i++) {
                const RdKafka::Headers::Header hdr = hdrs[i];

                if (hdr.value() != NULL)
                    printf(" Header: %s = \"%.*s\"\n",
                        hdr.key().c_str(),
                        (int)hdr.value_size(), (const char*)hdr.value());
                else
                    printf(" Header:  %s = NULL\n", hdr.key().c_str());
            }
        }
        printf("%.*s\n",
            static_cast<int>(message->len()),
            static_cast<const char*>(message->payload()));
        break;

    case RdKafka::ERR__PARTITION_EOF:
        /* Last message */
        std::cerr << "Consume received ERR__PARTITION_EOF: " << message->errstr() << std::endl;
        break;

    case RdKafka::ERR__UNKNOWN_TOPIC:
    case RdKafka::ERR__UNKNOWN_PARTITION:
        std::cerr << "Consume failed: " << message->errstr() << std::endl;
        break;

    default:
        /* Errors */
        std::cerr << "Consume failed: " << message->errstr() << std::endl;
    }
}

int main(int argc, char** argv) {
    std::string brokers;
    std::string errstr;
    std::string enginePath;
    std::string topic_str;
    std::string mode;

    /*
     * Create configuration objects
     */
    RdKafka::Conf* conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
    RdKafka::Conf* tconf = RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC);

    int opt;
    while ((opt = getopt(argc, argv, "b:p:t:m:d:X:")) != -1) {
        switch (opt) {
        case 'b':
            brokers = optarg;
            break;
        case 'p':
            enginePath = optarg;
            break;
        case 't':
            topic_str = optarg;
            break;
        case 'm':
        {
            if (strcmp(optarg, "P") != 0 && strcmp(optarg, "C") != 0) {
                std::cerr << "Supported values for 'm' are P or C , not " << optarg << std::endl;
                exit(1);
            }
            mode = optarg;
            break;
        }
        case 'd':
            if (conf->set("debug", optarg, errstr) != RdKafka::Conf::CONF_OK) {
                std::cerr << errstr << std::endl;
                exit(1);
            }
            break;
        case 'X':
        {
            char* name, * val;

            name = optarg;
            if (!(val = strchr(name, '='))) {
                std::cerr << "%% Expected -X property=value, not " <<
                    name << std::endl;
                exit(1);
            }

            *val = '\0';
            val++;

            if (conf->set(name, val, errstr) != RdKafka::Conf::CONF_OK) {
                std::cerr << errstr << std::endl;
                exit(1);
            }
        }
        break;

        default:
            goto usage;
        }
    }

    if (brokers.empty() || enginePath.empty() || mode.empty() || topic_str.empty() || optind != argc) {
    usage:
        std::string features;
        conf->get("builtin.features", features);
        fprintf(stderr,
            "Usage: %s [options] -b <brokers> -s <cert-subject> -p <priv-key-password>\n"
            "\n"
            "Windows Certificate Store integration example.\n"
            "Use certlm.msc or mmc to view your certificates.\n"
            "\n"
            "librdkafka version %s (0x%08x, builtin.features \"%s\")\n"
            "\n"
            " Options:\n"
            "  -b <brokers>     Broker address\n"
            "  -p <engine path> Path to openssl engine. Capi Openssl engine can be used to try this code"
            "  -t <topic_name>  Topic to fetch / produce\n"
            "  -m C|P           Consumer or Producer mode\n"
            "  -d [facs..]      Enable debugging contexts: %s\n"
            "  -X <prop=name>   Set arbitrary librdkafka configuration property\n"
            "\n",
            argv[0],
            RdKafka::version_str().c_str(), RdKafka::version(),
            features.c_str(),
            RdKafka::get_debug_contexts().c_str());
        exit(1);
    }

    conf->set("bootstrap.servers", brokers, errstr);
    conf->set("openssl.engine.location", enginePath, errstr);

    /* We use the Certificiate verification callback to print the
     * certificate chains being used. */
    PrintingSSLVerifyCb ssl_verify_cb;

    if (conf->set("ssl_cert_verify_cb", &ssl_verify_cb, errstr) != RdKafka::Conf::CONF_OK) {
        std::cerr << errstr << std::endl;
        exit(1);
    }

    if (mode == "P") {

        ExampleDeliveryReportCb ex_dr_cb;

        /* Set delivery report callback */
        conf->set("dr_cb", &ex_dr_cb, errstr);

        conf->set("default_topic_conf", tconf, errstr);

        /*
         * Create producer using accumulated global configuration.
         */
        RdKafka::Producer* producer = RdKafka::Producer::create(conf, errstr);
        if (!producer) {
            std::cerr << "Failed to create producer: " << errstr << std::endl;
            exit(1);
        }

        std::cout << "% Created producer " << producer->name() << std::endl;


        /*
         * Read messages from stdin and produce to broker.
         */
        for (std::string line; std::getline(std::cin, line);) {
            if (line.empty()) {
                producer->poll(0);
                continue;
            }
            else if (line.compare("exit") == 0)
                break;

            RdKafka::Headers* headers = RdKafka::Headers::create();
            headers->add("my header", "header value");
            headers->add("other header", "yes");

            /*
             * Produce message
             */
            RdKafka::ErrorCode resp =
                producer->produce(topic_str, 0,
                    RdKafka::Producer::RK_MSG_COPY /* Copy payload */,
                    /* Value */
                    const_cast<char*>(line.c_str()), line.size(),
                    /* Key */
                    NULL, 0,
                    /* Timestamp (defaults to now) */
                    0,
                    /* Message headers, if any */
                    headers,
                    /* Per-message opaque value passed to
                     * delivery report */
                    NULL);
            if (resp != RdKafka::ERR_NO_ERROR) {
                std::cerr << "% Produce failed: " <<
                    RdKafka::err2str(resp) << std::endl;
                delete headers; /* Headers are automatically deleted on produce()
                                 * success. */
            }
            else {
                std::cerr << "% Produced message (" << line.size() << " bytes)" <<
                    std::endl;
            }

            producer->poll(0);
        }

        while (producer->outq_len() > 0) {
            std::cerr << "Waiting for " << producer->outq_len() << std::endl;
            producer->poll(1000);
        }

        delete producer;


    }
    else if (mode == "C") {
        /*
         * Consumer mode
         */

        conf->set("enable.partition.eof", "true", errstr);

        if (topic_str.empty())
            goto usage;

        /*
         * Create consumer using accumulated global configuration.
         */
        RdKafka::Consumer* consumer = RdKafka::Consumer::create(conf, errstr);
        if (!consumer) {
            std::cerr << "Failed to create consumer: " << errstr << std::endl;
            exit(1);
        }

        std::cout << "% Created consumer " << consumer->name() << std::endl;

        /*
         * Create topic handle.
         */
        RdKafka::Topic* topic = RdKafka::Topic::create(consumer, topic_str,
            tconf, errstr);
        if (!topic) {
            std::cerr << "Failed to create topic: " << errstr << std::endl;
            exit(1);
        }

        /*
         * Start consumer for topic+partition at start offset
         */
        RdKafka::ErrorCode resp = consumer->start(topic, 0, RdKafka::Topic::OFFSET_BEGINNING);
        if (resp != RdKafka::ERR_NO_ERROR) {
            std::cerr << "Failed to start consumer: " <<
                RdKafka::err2str(resp) << std::endl;
            exit(1);
        }

        auto start = std::chrono::high_resolution_clock::now();
        auto maxSecondsWait = std::chrono::seconds(100);
        /*
         * Consume messages
         */
        while (std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - start) < maxSecondsWait) {
            RdKafka::Message* msg = consumer->consume(topic, 0, 1000);
            msg_consume(msg, NULL);
            delete msg;
            consumer->poll(0);
        }

        /*
         * Stop consumer
         */
        consumer->stop(topic, 0);

        consumer->poll(1000);

        delete topic;
        delete consumer;
    }

    delete conf;
    delete tconf;

    /*
     * Wait for RdKafka to decommission.
     * This is not strictly needed (when check outq_len() above), but
     * allows RdKafka to clean up all its resources before the application
     * exits so that memory profilers such as valgrind wont complain about
     * memory leaks.
     */
    RdKafka::wait_destroyed(5000);

    return 0;
}
