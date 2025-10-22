/*
 * Topic cleanup utility for librdkafka tests
 * Reads test.conf and deletes topics with the configured prefix
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include "rdkafka.h"

#define MAX_TOPICS         1000
#define MAX_TOPIC_NAME_LEN 256
#define TIMEOUT_MS         30000

/**
 * @brief Parse test.conf and configure rdkafka
 */
static int
parse_test_conf(rd_kafka_conf_t *conf, char *topic_prefix, size_t prefix_size) {
        FILE *fp;
        char line[512];
        char *key, *val, *ptr;
        int found_prefix = 0;
        char errstr[256];

        fp = fopen("test.conf", "r");
        if (!fp) {
                return -1;  // No config file
        }

        while (fgets(line, sizeof(line), fp)) {
                /* Remove trailing newline */
                if ((ptr = strchr(line, '\n')))
                        *ptr = '\0';

                /* Skip empty lines and comments */
                if (line[0] == '\0' || line[0] == '#')
                        continue;

                /* Split key=value */
                if (!(ptr = strchr(line, '=')))
                        continue;

                *ptr = '\0';
                key  = line;
                val  = ptr + 1;

                /* Remove leading/trailing spaces */
                while (*key == ' ' || *key == '\t')
                        key++;
                while (*val == ' ' || *val == '\t')
                        val++;

                if (strcmp(key, "test.topic.prefix") == 0) {
                        strncpy(topic_prefix, val, prefix_size - 1);
                        topic_prefix[prefix_size - 1] = '\0';
                        found_prefix                  = 1;
                } else if (strncmp(key, "test.", 5) == 0) {
                        /* Skip test-specific configuration properties */
                        continue;
                } else {
                        /* Apply all other Kafka configuration */
                        rd_kafka_conf_set(conf, key, val, errstr,
                                          sizeof(errstr));
                }
        }

        fclose(fp);
        return found_prefix ? 0 : -1;
}

/**
 * @brief Get topics matching prefix and delete them
 */
static int cleanup_topics(rd_kafka_conf_t *conf, const char *topic_prefix) {
        rd_kafka_t *rk;
        const rd_kafka_metadata_t *metadata;
        rd_kafka_DeleteTopic_t **del_topics = NULL;
        rd_kafka_AdminOptions_t *options    = NULL;
        rd_kafka_queue_t *queue             = NULL;
        rd_kafka_event_t *event;
        char errstr[256];
        int topic_count   = 0;
        int deleted_count = 0;
        int i;
        size_t prefix_len = strlen(topic_prefix);

        rd_kafka_conf_set(conf, "log_level", "3", errstr, sizeof(errstr));

        rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
        if (!rk) {
                fprintf(stderr, "Failed to create Kafka producer: %s\n",
                        errstr);
                return -1;
        }

        printf("Searching for topics with prefix '%s'\n", topic_prefix);

        if (rd_kafka_metadata(rk, 0, NULL, &metadata, TIMEOUT_MS) !=
            RD_KAFKA_RESP_ERR_NO_ERROR) {
                fprintf(stderr, "Failed to get metadata\n");
                rd_kafka_destroy(rk);
                return -1;
        }

        for (i = 0; i < metadata->topic_cnt; i++) {
                if (strncmp(metadata->topics[i].topic, topic_prefix,
                            prefix_len) == 0) {
                        topic_count++;
                }
        }

        if (topic_count == 0) {
                printf("Found 0 topics\n");
                rd_kafka_metadata_destroy(metadata);
                rd_kafka_destroy(rk);
                return 0;
        }

        printf("Found %d topic%s\n", topic_count, topic_count == 1 ? "" : "s");

        del_topics = malloc(sizeof(*del_topics) * topic_count);
        if (!del_topics) {
                rd_kafka_metadata_destroy(metadata);
                rd_kafka_destroy(rk);
                return -1;
        }

        /* Create delete topic objects */
        int idx = 0;
        for (i = 0; i < metadata->topic_cnt && idx < topic_count; i++) {
                if (strncmp(metadata->topics[i].topic, topic_prefix,
                            prefix_len) == 0) {
                        del_topics[idx] =
                            rd_kafka_DeleteTopic_new(metadata->topics[i].topic);
                        idx++;
                }
        }

        rd_kafka_metadata_destroy(metadata);
        options = rd_kafka_AdminOptions_new(rk, RD_KAFKA_ADMIN_OP_DELETETOPICS);
        rd_kafka_AdminOptions_set_operation_timeout(options, TIMEOUT_MS, errstr,
                                                    sizeof(errstr));
        queue = rd_kafka_queue_new(rk);

        rd_kafka_DeleteTopics(rk, del_topics, topic_count, options, queue);

        event = rd_kafka_queue_poll(queue, TIMEOUT_MS + 5000);
        if (event) {
                const rd_kafka_DeleteTopics_result_t *result =
                    rd_kafka_event_DeleteTopics_result(event);
                if (result) {
                        const rd_kafka_topic_result_t **topic_results;
                        size_t result_count;
                        topic_results = rd_kafka_DeleteTopics_result_topics(
                            result, &result_count);

                        for (i = 0; i < (int)result_count; i++) {
                                rd_kafka_resp_err_t err =
                                    rd_kafka_topic_result_error(
                                        topic_results[i]);
                                const char *topic_name =
                                    rd_kafka_topic_result_name(
                                        topic_results[i]);

                                if (err == RD_KAFKA_RESP_ERR_NO_ERROR ||
                                    err ==
                                        RD_KAFKA_RESP_ERR_UNKNOWN_TOPIC_OR_PART) {
                                        printf("Deleted %s\n", topic_name);
                                        deleted_count++;
                                } else {
                                        printf("Failed to delete %s: %s\n",
                                               topic_name,
                                               rd_kafka_err2str(err));
                                }
                        }
                }
                rd_kafka_event_destroy(event);
        }

        printf("\n%d topic%s deleted\n", deleted_count,
               deleted_count == 1 ? "" : "s");
        printf("\nTopic cleanup completed\n");

        rd_kafka_DeleteTopic_destroy_array(del_topics, topic_count);
        free(del_topics);
        rd_kafka_AdminOptions_destroy(options);
        rd_kafka_queue_destroy(queue);
        rd_kafka_destroy(rk);

        return 0;
}

int main() {
        char topic_prefix[128] = "";
        rd_kafka_conf_t *conf;

        conf = rd_kafka_conf_new();

        if (parse_test_conf(conf, topic_prefix, sizeof(topic_prefix)) < 0) {
                if (access("test.conf", R_OK) != 0) {
                        printf(
                            "No config file found - skipping topic cleanup\n");
                } else {
                        printf(
                            "No topic prefix configured - skipping topic "
                            "cleanup\n");
                }
                rd_kafka_conf_destroy(conf);
                return 0;
        }

        cleanup_topics(conf, topic_prefix);

        return 0;
}