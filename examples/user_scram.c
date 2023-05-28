/*
 * librdkafka - Apache Kafka C library
 *
 * Copyright (c) 2023, Adhitya Mahajan
 *  rights reserved.
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
 * ARE DISCLAIMED. IN NO EVENT SH THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

/**
 * Example utility that shows how to use SCRAM APIs (AdminAPI)
 * DescribeUserScramCredentials -> Describe the scram mechanism for each user 
 * AlterUserScramCredentials -> Changes the scram mechanism for the user
 */

#include <stdio.h>
#include <signal.h>
#include <string.h>
#include <stdlib.h>


/* Typical include path would be <librdkafka/rdkafka.h>, but this program
 * is builtin from within the librdkafka source tree and thus differs. */
#include "rdkafka.h"


static rd_kafka_queue_t *queue; /** Admin result queue.
                                 *  This is a global so we can
                                 *  yield in stop() */
static volatile sig_atomic_t run = 1;

/**
 * @brief Signal termination of program
 */
static void stop(int sig) {
        if (!run) {
                fprintf(stderr, "%% Forced termination\n");
                exit(2);
        }
        run = 0;
        rd_kafka_queue_yield(queue);
}


/**
 * @brief Parse an integer or fail.
 */
int64_t parse_int(const char *what, const char *str) {
        char *end;
        unsigned long n = strtoull(str, &end, 0);

        if (end != str + strlen(str)) {
                fprintf(stderr, "%% Invalid input for %s: %s: not an integer\n",
                        what, str);
                exit(1);
        }

        return (int64_t)n;
}

static void Describe(rd_kafka_t *rk,char **users,size_t user_cnt){
        rd_kafka_event_t *event;
        char errstr[512];      /* librdkafka API error reporting buffer */
        rd_kafka_resp_err_t api_error;
        
        rd_kafka_AdminOptions_t *options = rd_kafka_AdminOptions_new(rk, RD_KAFKA_ADMIN_OP_DESCRIBEUSERSCRAMCREDENTIALS);  

        if (rd_kafka_AdminOptions_set_request_timeout(
                options, 30 * 1000 /* 30s */, errstr, sizeof(errstr))) {
                fprintf(stderr, "%% Failed to set timeout: %s\n", errstr);
                return ;
        }
        
        /* Null Argument gives us  the users*/
        api_error = rd_kafka_DescribeUserScramCredentials(rk,users,user_cnt,options,queue);
        rd_kafka_AdminOptions_destroy(options);
        if(api_error){
                printf("API Entry point error : %s\n\n",rd_kafka_err2str(api_error));
                return;
        }
        /* Wait for results */
        event = rd_kafka_queue_poll(queue, -1 /*indefinitely*/);
        if (!event) {
                /* User hit Ctrl-C */
                fprintf(stderr, "%% Cancelled by user\n");

        } else if (rd_kafka_event_error(event)) {
                /* Request failed */
                fprintf(stderr, "%% DescribeUserScramCredentials failed: %s\n",
                        rd_kafka_event_error_string(event));

        } else {
                const rd_kafka_DescribeUserScramCredentials_result_t *result;
                size_t num_results;
                size_t i;
                result  = rd_kafka_event_DescribeUserScramCredentials_result(event);
                num_results = rd_kafka_DescribeUserScramCredentials_result_get_count(result);
                rd_kafka_resp_err_t request_errorcode = rd_kafka_DescribeUserScramCredentials_result_get_errorcode(result);
                if(request_errorcode){
                        char *errormsg = rd_kafka_DescribeUserScramCredentials_result_get_errormessage(result);
                        printf("Request Level Error Message : %s \n",errormsg);
                }
                printf("DescribeUserScramCredentials results[%d] [Error Code : %d]:\n",num_results,request_errorcode);
                for (i = 0; i < num_results; i++){
                        rd_kafka_UserScramCredentialsDescription_t *description;
                        description = rd_kafka_DescribeUserScramCredentials_result_get_description(result,i);
                        char *username;
                        rd_kafka_error_t *error;
                        username = rd_kafka_UserScramCredentialsDescription_get_user(description);
                        error = rd_kafka_UserScramCredentialsDescription_get_error(description);
                        rd_kafka_resp_err_t errorcode = rd_kafka_error_code(error);
                        printf("        Username : %s Error-code : %d\n",username,errorcode);
                        if(errorcode){
                                char *errstr = rd_kafka_error_string(error);
                                printf("                ErrorMessage : %s\n",errstr);
                        }
                        size_t num_credentials = rd_kafka_UserScramCredentialsDescription_get_scramcredentialinfo_cnt(description);
                        size_t itr;
                        for(itr=0;itr<num_credentials;itr++){
                                rd_kafka_ScramCredentialInfo_t *scram_credential = rd_kafka_UserScramCredentialsDescription_get_scramcredentialinfo(description,itr);
                                rd_kafka_ScramMechanism_t mechanism;
                                int32_t iterations;
                                mechanism = rd_kafka_ScramCredentialInfo_get_mechanism(scram_credential);
                                iterations = rd_kafka_ScramCredentialInfo_get_iterations(scram_credential);
                                switch (mechanism)
                                {
                                case RD_KAFKA_SCRAM_MECHANISM_UNKNOWN:
                                        printf("                Mechanism is UNKNOWN\n");
                                        break;
                                case RD_KAFKA_SCRAM_MECHANISM_SHA_256:
                                        printf("                Mechanism is SCRAM-SHA-256\n");
                                        break;
                                case RD_KAFKA_SCRAM_MECHANISM_SHA_512:
                                        printf("                Mechanism is SCRAM-SHA-512\n");
                                        break;
                                }
                                printf("                Iterations are %d\n",iterations);
                        }

                }
                printf("DescribeUserScramCredentials result END\n");
        }
        rd_kafka_event_destroy(event);
}

static void Alter(rd_kafka_t *rk,rd_kafka_UserScramCredentialAlteration_t **alterations,size_t alteration_cnt){
        rd_kafka_event_t *event;
        char errstr[512];      /* librdkafka API error reporting buffer */
        int api_error = 0;
        /* Set timeout (optional) */
        rd_kafka_AdminOptions_t *options =
            rd_kafka_AdminOptions_new(rk, RD_KAFKA_ADMIN_OP_ALTERUSERSCRAMCREDENTIALS);

        if (rd_kafka_AdminOptions_set_request_timeout(
                options, 30 * 1000 /* 30s */, errstr, sizeof(errstr))) {
                fprintf(stderr, "%% Failed to set timeout: %s\n", errstr);
                return;
        }

        /* C the AlterUserScramCredentials Function*/
        api_error = rd_kafka_AlterUserScramCredentials(rk,alterations,alteration_cnt,options,queue);
         if(api_error){
                printf("API Entry point error : %s\n\n",rd_kafka_err2str(api_error));
                return;
        }
        rd_kafka_AdminOptions_destroy(options);
        
        /* Wait for results */
        event = rd_kafka_queue_poll(queue, -1 /*indefinitely*/);
        if (!event) {
                /* User hit Ctrl-C */
                fprintf(stderr, "%% Cancelled by user\n");

        } else if (rd_kafka_event_error(event)) {
                /* Request failed */
                fprintf(stderr, "%% AlterUserScramCredentials failed: %s\n",
                        rd_kafka_event_error_string(event));

        } else {
                const rd_kafka_AlterUserScramCredentials_result_t *result = rd_kafka_event_AlterUserScramCredentials_result(event);
                size_t num_results = rd_kafka_AlterUserScramCredentials_result_get_count(result); 
                size_t i;
                printf("AlterUserScramCredentials results [%d]:\n",num_results);
                for (i = 0; i < num_results; i++){
                        rd_kafka_UserScramCredentialAlterationResultElement_t *element = rd_kafka_AlterUserScramCredentials_result_get_element(result,i);
                        char *username;
                        rd_kafka_error_t *error;
                        username = rd_kafka_UserScramCredentialAlterationResultElement_get_user(element);
                        error = rd_kafka_UserScramCredentialAlterationResultElement_get_error(element);
                        rd_kafka_resp_err_t errorcode = rd_kafka_error_code(error);
                        if(errorcode){
                                char *errstr = rd_kafka_error_string(error);
                                printf("        Username : %s , errorcode : %d , error-message : %s\n",username,errorcode,errstr);
                        }else{
                                printf("        Username : %s Success!!\n",username);
                        }

                }
                printf("AlterUserScramCredentials result END\n");
        }
        rd_kafka_event_destroy(event);
       
}
int main(int argc, char **argv) {
        rd_kafka_conf_t *conf; /* Temporary configuration object */
        char errstr[512];      /* librdkafka API error reporting buffer */
        const char *brokers = "localhost:9092";   /* Argument: broker list */
        rd_kafka_t *rk;        /* Admin client instance */
        /*
         * Create Kafka client configuration place-holder
         */
        conf = rd_kafka_conf_new();

        /* Set bootstrap broker(s) as a comma-separated list of
         * host or host:port (default port 9092).
         * librdkafka will use the bootstrap brokers to acquire the full
         * set of brokers from the cluster. */
        if (rd_kafka_conf_set(conf, "bootstrap.servers", brokers, errstr,
                              sizeof(errstr)) != RD_KAFKA_CONF_OK) {
                fprintf(stderr, "%s\n", errstr);
                return 1;
        }
        rd_kafka_conf_set(conf, "debug", "", NULL, 0);

        /*
         * Create an admin client, it can be created using any client type,
         * so we choose producer since it requires no extra configuration
         * and is more light-weight than the consumer.
         *
         * NOTE: rd_kafka_new() takes ownership of the conf object
         *       and the application must not reference it again after
         *       this c.
         */
        rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
        if (!rk) {
                fprintf(stderr, "%% Failed to create new producer: %s\n",
                        errstr);
                return 1;
        }

        /* The Admin API is completely asynchronous, results are emitted
         * on the result queue that is passed to DeleteRecords() */
        queue = rd_kafka_queue_new(rk);

        /* Signal handler for clean shutdown */
        signal(SIGINT, stop);
        char *users[1];
        users[0] = "broker";
        size_t user_cnt = 1;

        /* Describe  the users */
        Describe(rk,users,user_cnt);

        /* First Upsert a mechanism*/
        const char *username = "broker";
        rd_kafka_ScramMechanism_t mechanism = RD_KAFKA_SCRAM_MECHANISM_SHA_256;
        int32_t iterations = 10000;
        const char *salt = "salt";
        const char *password = "password";
        
        rd_kafka_UserScramCredentialAlteration_t *alterations[1];

        alterations[0] = rd_kafka_UserScramCredentialUpsertion_new(username,salt,password,mechanism,iterations);
        
        Alter(rk,alterations,1);
        
        
        rd_kafka_UserScramCredentialAlteration_destroy(alterations[0]);
        /* Describe the mechanisms */
        Describe(rk,users,user_cnt);

        /* Delete the upserted mechanism*/
        alterations[0] = rd_kafka_UserScramCredentialDeletion_new(username,mechanism);

        Alter(rk,alterations,1);
        rd_kafka_UserScramCredentialAlteration_destroy(alterations[0]);
        /* Describe the mechanisms */
        Describe(rk,users,user_cnt);


        signal(SIGINT, SIG_DFL);
        /* Destroy queue */
        rd_kafka_queue_destroy(queue);


        
        /* Destroy the producer instance */
        rd_kafka_destroy(rk);

        return 0;
}
