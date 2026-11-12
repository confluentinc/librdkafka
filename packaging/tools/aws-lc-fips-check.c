/*
 * Verify that the dependency used by the AWS-LC FIPS CI build is AWS-LC,
 * that its FIPS module passes its runtime checks, and that librdkafka can
 * initialize SSL with it.
 */

#include <openssl/base.h>
#include <openssl/crypto.h>

#include "rdkafka.h"

#ifndef OPENSSL_IS_AWSLC
#error "The configured crypto library is not AWS-LC"
#endif

int main(void) {
        rd_kafka_conf_t *conf;
        rd_kafka_t *rk;
        char errstr[512];

        if (!FIPS_mode())
                return 1;

        if (!BORINGSSL_integrity_test())
                return 1;

        conf = rd_kafka_conf_new();
        if (rd_kafka_conf_set(conf, "security.protocol", "ssl", errstr,
                              sizeof(errstr)) != RD_KAFKA_CONF_OK) {
                rd_kafka_conf_destroy(conf);
                return 1;
        }

        rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
        if (!rk)
                return 1;

        rd_kafka_destroy(rk);

        return 0;
}
