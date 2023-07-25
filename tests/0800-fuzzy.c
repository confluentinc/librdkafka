
#include "test.h"

#include "rdkafka.h"
#include "testshared.h"
#include <pthread.h>
#include <unistd.h>

#define MAX_INDEX 100000

const char* k_topic_name = "kudu_profile_record_stream";

int producer_bitset[MAX_INDEX] = {
    0,
};

int timeout = 300;

void produce_dr_cb(rd_kafka_t *rk, void *payload, size_t len,
                   rd_kafka_resp_err_t err, void *opaque, void *msg_opaque) {
  char buf[128];
  rd_snprintf(buf, sizeof(buf), "%.*s", (int)len, (char *)payload);
  if (err) {
    TEST_SAY("Message delivery error: %s, producer produce_dr_cb check value %s\n", rd_kafka_err2str(err), buf);
  } else {
    TEST_SAY("Message delivery ok, producer produce_dr_cb check value ok: %s\n", buf);
  }
}

void produce_dr_msg_cb(rd_kafka_t *rk, const rd_kafka_message_t *rkm,
                       void *opaque) {
  char buf[128];
  rd_snprintf(buf, sizeof(buf), "%.*s", (int)rkm->len, (char *)rkm->payload);
  if (rkm->err) {
    TEST_SAY("Message delivery error: %s, producer produce_dr_msg_cb check value %s\n", rd_kafka_err2str(rkm->err), buf);
  } else {
    TEST_SAY("Message delivery ok, producer produce_dr_msg_cb check value ok: %s\n", buf);
  }
}

void* Producer(void* arg) {
    const char* prefix = (const char* ) arg;
    const char* topic_name  = NULL;

    rd_kafka_t* rk;
    rd_kafka_topic_t* rkt;
    rd_kafka_conf_t* conf;
    rd_kafka_topic_conf_t* topic_conf;

    test_conf_init(&conf, &topic_conf, timeout);
    rd_kafka_conf_set_error_cb(conf, NULL);
    // rd_kafka_conf_set_dr_cb(conf, produce_dr_cb);
    rd_kafka_conf_set_dr_msg_cb(conf, produce_dr_msg_cb);
    // rd_kafka_conf_set_events(conf, RD_KAFKA_EVENT_DR);
    char errstr[512];
    rd_kafka_topic_conf_set(topic_conf, "request.required.acks", "-1", errstr,
                            sizeof(errstr));
    if (!topic_name) {
        topic_name = test_mk_topic_name(k_topic_name, 0);
    }
    TEST_SAY("topic_name: %s\n", topic_name);
    rk = test_create_handle(RD_KAFKA_PRODUCER, conf);
    // rk->rk_drmode = RD_KAFKA_DR_MODE_EVENT;
    rkt = rd_kafka_topic_new(rk, topic_name, topic_conf);
    if (!rkt) {
        TEST_SAY("produce thread error\n");
        return NULL;
    }

    int i = 1;
    for (; i <= MAX_INDEX; i++) {
      char msg[128];
      rd_snprintf(msg, sizeof(msg), "%s %d", prefix, i);

      rd_kafka_resp_err_t error_code = rd_kafka_produce(
          rkt, 0, RD_KAFKA_MSG_F_COPY, msg, strlen(msg), NULL, 0, NULL);

      if (error_code == RD_KAFKA_RESP_ERR_NO_ERROR) {
        TEST_SAY("produce prepare ok: %d\n", i);
      } else {
        TEST_SAY("produce prepare error: %d, error_code: %s\n", i, rd_kafka_err2str(error_code));
        i--;
        continue;
      }

      error_code = rd_kafka_flush(rk, 5000);
      if (error_code == RD_KAFKA_RESP_ERR_NO_ERROR) {
        TEST_SAY("produce commit ok: %s\n", msg);
      } else {
        TEST_SAY("produce commit error: %s, err_msg: %s\n", msg, rd_kafka_err2str(error_code));
        i--;
      }
    }
    /* Destroy topic */
    rd_kafka_topic_destroy(rkt);

    /* Destroy rdkafka instance */
    TEST_SAY("Destroying kafka instance %s\n", rd_kafka_name(rk));
    rd_kafka_destroy(rk);
    return NULL;
}

void* Consumer(void* arg) {
    const char* topic_name  = NULL;

    rd_kafka_t* rk;
    rd_kafka_topic_t* rkt;
    rd_kafka_conf_t* conf;
    rd_kafka_topic_conf_t* topic_conf;

    test_conf_init(&conf, &topic_conf, 2 * timeout);
    rd_kafka_conf_set_error_cb(conf, NULL);
    if (!topic_name) {
        topic_name = test_mk_topic_name(k_topic_name, 0);
    }
    rk = test_create_handle(RD_KAFKA_CONSUMER, conf);

    rkt = rd_kafka_topic_new(rk, topic_name, topic_conf);
    if (!rkt) {
        TEST_SAY("consume thread error\n");
        return NULL;
    }

    rd_kafka_consume_start(rkt, 0, RD_KAFKA_OFFSET_BEGINNING);
    int retry_index = 0;
    while (1) {
      rd_kafka_message_t *rkm = rd_kafka_consume(rkt, 0, 1000);
      if (!rkm) {
        if (retry_index++ > 180) {
          break;
        }
        TEST_SAY("retry consume: %d\n", retry_index);
        continue; /* Timeout: no message within 1000ms, retry again. */
      }
      retry_index = 0;
      if (rkm->err) {
        /* Consumer errors are generally to be considered
         * informational as the consumer will automatically
         * try to recover from all types of errors. */
        TEST_SAY("%% consume error: %s\n", rd_kafka_message_errstr(rkm));
        rd_kafka_message_destroy(rkm);
        continue;
      }
      if (rkm->payload) {
        char buf[128];
        rd_snprintf(buf, sizeof(buf), "%.*s", (int)rkm->len,
                    (char *)rkm->payload);
        TEST_SAY("consume value ok: %s\n", buf);
      }
      rd_kafka_message_destroy(rkm);
    }
    rd_kafka_consume_stop(rkt, 0);

    /* Destroy topic */
    rd_kafka_topic_destroy(rkt);

    /* Destroy rdkafka instance */
    TEST_SAY("Destroying kafka instance %s\n", rd_kafka_name(rk));
    rd_kafka_destroy(rk);

    return NULL;
}

void* Fuzzy(void* arg) {
    TEST_SAY("Fuzzy thread start");
    sleep(5);
    const char* stop_cmd = "sh single_kafka_controller.sh stop_only 0";
    const char* start_cmd = "sh single_kafka_controller.sh start_only 0";
    int index = 0;
    while (++index < 100) {
        TEST_SAY("stop kafka, retry: %d\n", index);
        system(stop_cmd);
        sleep(3);
        TEST_SAY("start kafka, retry: %d\n", index);
        system(start_cmd);
        sleep(3);
    }
    TEST_SAY("Last start kafka, make sure kafka is running\n");
    system(start_cmd);
    TEST_SAY("Fuzzy thread finish");
    return NULL;
}

int main_0800_fuzzy(int argc, char** argv) {
    TEST_SAY("create and run fuzzy test");
    const char* uninstall_kafka_cmd = "sh single_kafka_controller.sh stop_only 0";
    // system(uninstall_kafka_cmd);
    const char* install_kafka_cmd = "sh single_kafka_controller.sh start_only 0";
    system(install_kafka_cmd);

    pthread_t producer_thread_1;
    pthread_create(&producer_thread_1, NULL, Producer, "producer1");
    pthread_t producer_thread_2;
    pthread_create(&producer_thread_2, NULL, Producer, "producer2");
    pthread_t producer_thread_3;
    pthread_create(&producer_thread_3, NULL, Producer, "producer3");
    pthread_t producer_thread_4;
    pthread_create(&producer_thread_4, NULL, Producer, "producer4");
    pthread_t producer_thread_5;
    pthread_create(&producer_thread_5, NULL, Producer, "producer5");
    pthread_t producer_thread_6;
    pthread_create(&producer_thread_6, NULL, Producer, "producer6");

    pthread_t consumer_thread;
    pthread_create(&consumer_thread, NULL, Consumer,  NULL);
    pthread_t fuzzy_thread;
    pthread_create(&fuzzy_thread, NULL, Fuzzy, NULL);

    pthread_join(producer_thread_1, NULL);
    pthread_join(producer_thread_2, NULL);
    pthread_join(producer_thread_3, NULL);
    pthread_join(producer_thread_4, NULL);
    pthread_join(producer_thread_5, NULL);
    pthread_join(producer_thread_6, NULL);

    pthread_join(consumer_thread, NULL);
    pthread_join(fuzzy_thread, NULL);

    system(uninstall_kafka_cmd);
    return 0;
}
