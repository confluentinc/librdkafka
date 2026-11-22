/* Public certificate setter ownership and immediate-validation regression. */
#include "rdkafka.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int load(rd_kafka_conf_t *conf, rd_kafka_cert_type_t type,
                rd_kafka_cert_enc_t encoding, const char *path) {
        FILE *fp = fopen(path, "rb");
        long size;
        char *buf;
        char err[512];
        rd_kafka_conf_res_t r;
        if (!fp || fseek(fp, 0, SEEK_END) || (size = ftell(fp)) <= 0 ||
            fseek(fp, 0, SEEK_SET)) {
                fprintf(stderr, "Unable to read certificate fixture\n");
                return 1;
        }
        buf = malloc((size_t)size);
        if (!buf || fread(buf, 1, (size_t)size, fp) != (size_t)size)
                return 1;
        fclose(fp);
        r = rd_kafka_conf_set_ssl_cert(conf, type, encoding, buf,
                                       (size_t)size, err, sizeof(err));
        /* The setter promises ownership independent of this buffer. */
        memset(buf, 0, (size_t)size);
        free(buf);
        if (r != RD_KAFKA_CONF_OK) {
                fprintf(stderr, "Certificate setter rejected fixture: %s\n", err);
                return 1;
        }
        return 0;
}

int main(int argc, char **argv) {
        rd_kafka_conf_t *conf, *copy;
        char err[512];
        int i;
        const rd_kafka_cert_type_t types[] = {
            RD_KAFKA_CERT_CA, RD_KAFKA_CERT_PUBLIC_KEY,
            RD_KAFKA_CERT_PRIVATE_KEY};
        if (argc != 7) {
                fprintf(stderr, "Usage: %s CA.pem client.pem key.pem CA.der client.der key-pkcs8.der\n", argv[0]);
                return 2;
        }
        conf = rd_kafka_conf_new();
        for (i = 0; i < 3; i++) {
                if (load(conf, types[i], RD_KAFKA_CERT_ENC_PEM, argv[i + 1]))
                        return 1;
                /* Invalid replacement must fail synchronously. */
                if (rd_kafka_conf_set_ssl_cert(conf, types[i],
                         RD_KAFKA_CERT_ENC_PEM, "not a certificate", 17,
                         err, sizeof(err)) != RD_KAFKA_CONF_INVALID)
                        return 1;
        }
        copy = rd_kafka_conf_dup(conf);
        /* Clear the original while the duplicate retains all material. */
        for (i = 0; i < 3; i++) {
                if (rd_kafka_conf_set_ssl_cert(conf, types[i],
                         RD_KAFKA_CERT_ENC_PEM, NULL, 0, err,
                         sizeof(err)) != RD_KAFKA_CONF_OK)
                        return 1;
        }
        rd_kafka_conf_destroy(conf);
        /* Replace duplicate entries with DER and release shared originals. */
        for (i = 0; i < 3; i++) {
                if (load(copy, types[i], RD_KAFKA_CERT_ENC_DER, argv[i + 4]))
                        return 1;
        }
        rd_kafka_conf_destroy(copy);
        puts("certificate buffers: PEM/DER, immediate rejection, duplicate/clear passed");
        return 0;
}
