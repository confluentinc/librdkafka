#include <rdkafka.h>
#include <openssl/ssl.h>
#include <openssl/provider.h>
#include <openssl/evp.h>
#include <openssl/rand.h>
#include <dlfcn.h>
#include <stdio.h>
int main(int argc, char **argv) {
 OSSL_PROVIDER *provider = OSSL_PROVIDER_load(NULL,"default");
 SSL_CTX *ssl = SSL_CTX_new(TLS_client_method());
 rd_kafka_conf_t *conf = rd_kafka_conf_new();
 rd_kafka_t *rk;
 unsigned char random[32];
 char error[1024];
 if (argc != 2 || !provider || !ssl || RAND_bytes(random,sizeof(random)) != 1) return 1;
 if (OpenSSL_version_num() < 0x30000000L ||
     EVP_default_properties_is_fips_enabled(NULL)) {
   fputs("Host application OpenSSL symbols resolved to an unexpected crypto implementation\n", stderr);
   return 2;
 }
 if (rd_kafka_conf_set(conf,"security.protocol","ssl",error,sizeof(error)) ||
     rd_kafka_conf_set(conf,"ssl.fips.mode","required",error,sizeof(error)) ||
     rd_kafka_conf_set(conf,"ssl.ca.location",argv[1],error,sizeof(error))) return 3;
 rk = rd_kafka_new(RD_KAFKA_PRODUCER,conf,error,sizeof(error));
 if (!rk) { fprintf(stderr,"%s\n",error); return 4; }
 if (EVP_default_properties_is_fips_enabled(NULL) ||
     RAND_bytes(random,sizeof(random)) != 1 ||
     dlsym(RTLD_DEFAULT,"awslc_version_string")) return 5;
 rd_kafka_destroy(rk);
 SSL_CTX_free(ssl);
 OSSL_PROVIDER_unload(provider);
 puts("PASS: OpenSSL default context and private rustls/AWS-LC strict context coexist");
 return 0;
}
