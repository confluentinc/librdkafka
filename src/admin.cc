/*
 * confluent-kafka-javascript - Node.js wrapper  for RdKafka C/C++ library
 *
 * Copyright (c) 2016-2023 Blizzard Entertainment
 *           (c) 2023 Confluent, Inc.
 *
 * This software may be modified and distributed under the terms
 * of the MIT license.  See the LICENSE.txt file for details.
 */

#include "src/admin.h"

#include <math.h>
#include <string>
#include <vector>

#include "src/workers.h"

using Nan::FunctionCallbackInfo;

namespace NodeKafka {

/**
 * @brief AdminClient v8 wrapped object.
 *
 * Specializes the connection to wrap a producer object through compositional
 * inheritence. Establishes its prototype in node through `Init`
 *
 * @sa RdKafka::Handle
 * @sa NodeKafka::Client
 */

AdminClient::AdminClient(Conf *gconfig) : Connection(gconfig, NULL) {}

AdminClient::AdminClient(Connection *connection) : Connection(connection) {}

AdminClient::~AdminClient() {
  Disconnect();
}

Baton AdminClient::Connect() {
  if (IsConnected()) {
    return Baton(RdKafka::ERR_NO_ERROR);
  }

  /* We should never fail the IsConnected check when we have an underlying
   * client, as it should always be connected. */
  if (m_has_underlying) {
    return Baton(RdKafka::ERR__STATE,
                 "Existing client is not connected, and dependent client "
                 "cannot initiate connection.");
  }

  Baton baton = setupSaslOAuthBearerConfig();
  if (baton.err() != RdKafka::ERR_NO_ERROR) {
    return baton;
  }

  std::string errstr;
  {
    scoped_shared_write_lock lock(m_connection_lock);
    m_client = RdKafka::Producer::create(m_gconfig, errstr);
  }

  if (!m_client || !errstr.empty()) {
    DeactivateDispatchers();
    return Baton(RdKafka::ERR__STATE, errstr);
  }

  /* Set the client name at the first possible opportunity for logging. */
  m_event_cb.dispatcher.SetClientName(m_client->name());

  baton = setupSaslOAuthBearerBackgroundQueue();
  if (baton.err() != RdKafka::ERR_NO_ERROR) {
    DeactivateDispatchers();
  }

  std::string stats_interval_ms;
  if (this->m_gconfig->get("statistics.interval.ms", stats_interval_ms) ==
      RdKafka::Conf::CONF_OK) {
    Connection::SetPollInBackground(true);
  }

  return baton;
}

Baton AdminClient::Disconnect() {
  /* Dependent AdminClients don't need to do anything. We block the call to
   * disconnect in JavaScript, but the destructor of AdminClient might trigger
   * this call. */
  if (m_has_underlying) {
    return Baton(RdKafka::ERR_NO_ERROR);
  }

  if (IsConnected()) {
    scoped_shared_write_lock lock(m_connection_lock);

    DeactivateDispatchers();

    delete m_client;
    m_client = NULL;
  }

  return Baton(RdKafka::ERR_NO_ERROR);
}

Nan::Persistent<v8::Function> AdminClient::constructor;

void AdminClient::Init(v8::Local<v8::Object> exports) {
  Nan::HandleScope scope;

  v8::Local<v8::FunctionTemplate> tpl = Nan::New<v8::FunctionTemplate>(New);
  tpl->SetClassName(Nan::New("AdminClient").ToLocalChecked());
  tpl->InstanceTemplate()->SetInternalFieldCount(1);

  // Inherited from NodeKafka::Connection
  Nan::SetPrototypeMethod(tpl, "configureCallbacks", NodeConfigureCallbacks);
  Nan::SetPrototypeMethod(tpl, "name", NodeName);

  // Admin client operations
  Nan::SetPrototypeMethod(tpl, "createTopic", NodeCreateTopic);
  Nan::SetPrototypeMethod(tpl, "deleteTopic", NodeDeleteTopic);
  Nan::SetPrototypeMethod(tpl, "createPartitions", NodeCreatePartitions);
  Nan::SetPrototypeMethod(tpl, "deleteRecords", NodeDeleteRecords);
  Nan::SetPrototypeMethod(tpl, "describeTopics", NodeDescribeTopics);
  Nan::SetPrototypeMethod(tpl, "listOffsets", NodeListOffsets);

  // Consumer group related operations
  Nan::SetPrototypeMethod(tpl, "listGroups", NodeListGroups);
  Nan::SetPrototypeMethod(tpl, "describeGroups", NodeDescribeGroups);
  Nan::SetPrototypeMethod(tpl, "deleteGroups", NodeDeleteGroups);
  Nan::SetPrototypeMethod(tpl, "listConsumerGroupOffsets",
                          NodeListConsumerGroupOffsets);

  Nan::SetPrototypeMethod(tpl, "connect", NodeConnect);
  Nan::SetPrototypeMethod(tpl, "disconnect", NodeDisconnect);
  Nan::SetPrototypeMethod(tpl, "setSaslCredentials", NodeSetSaslCredentials);
  Nan::SetPrototypeMethod(tpl, "getMetadata", NodeGetMetadata);
  Nan::SetPrototypeMethod(tpl, "setOAuthBearerToken", NodeSetOAuthBearerToken);
  Nan::SetPrototypeMethod(tpl, "setOAuthBearerTokenFailure",
                          NodeSetOAuthBearerTokenFailure);

  constructor.Reset(
    (tpl->GetFunction(Nan::GetCurrentContext())).ToLocalChecked());
  Nan::Set(exports, Nan::New("AdminClient").ToLocalChecked(),
    tpl->GetFunction(Nan::GetCurrentContext()).ToLocalChecked());
}

void AdminClient::New(const Nan::FunctionCallbackInfo<v8::Value>& info) {
  if (!info.IsConstructCall()) {
    return Nan::ThrowError("non-constructor invocation not supported");
  }

  if (info.Length() < 1) {
    return Nan::ThrowError("You must supply a global configuration or a preexisting client"); // NOLINT
  }

  Connection *connection = NULL;
  Conf *gconfig = NULL;
  AdminClient *client = NULL;

  if (info.Length() >= 3 && !info[2]->IsNull() && !info[2]->IsUndefined()) {
    if (!info[2]->IsObject()) {
      return Nan::ThrowError("Third argument, if provided, must be a client object"); // NOLINT
    }
    // We check whether this is a wrapped object within the calling JavaScript
    // code, so it's safe to unwrap it here. We Unwrap it directly into a
    // Connection object, since it's OK to unwrap into the parent class.
    connection = ObjectWrap::Unwrap<Connection>(
        info[2]->ToObject(Nan::GetCurrentContext()).ToLocalChecked());
    client = new AdminClient(connection);
  } else {
    if (!info[0]->IsObject()) {
      return Nan::ThrowError("Global configuration data must be specified");
    }

    std::string errstr;
    gconfig = Conf::create(
        RdKafka::Conf::CONF_GLOBAL,
        (info[0]->ToObject(Nan::GetCurrentContext())).ToLocalChecked(), errstr);

    if (!gconfig) {
      return Nan::ThrowError(errstr.c_str());
    }
    client = new AdminClient(gconfig);
  }

  // Wrap it
  client->Wrap(info.This());

  // Then there is some weird initialization that happens
  // basically it sets the configuration data
  // we don't need to do that because we lazy load it

  info.GetReturnValue().Set(info.This());
}

v8::Local<v8::Object> AdminClient::NewInstance(v8::Local<v8::Value> arg) {
  Nan::EscapableHandleScope scope;

  const unsigned argc = 1;

  v8::Local<v8::Value> argv[argc] = { arg };
  v8::Local<v8::Function> cons = Nan::New<v8::Function>(constructor);
  v8::Local<v8::Object> instance =
    Nan::NewInstance(cons, argc, argv).ToLocalChecked();

  return scope.Escape(instance);
}

/**
 * Poll for a particular event on a queue.
 *
 * This will keep polling until it gets an event of that type,
 * given the number of tries and a timeout
 */
rd_kafka_event_t* PollForEvent(
  rd_kafka_queue_t * topic_rkqu,
  rd_kafka_event_type_t event_type,
  int timeout_ms) {
  // Initiate exponential timeout
  int attempts = 1;
  int exp_timeout_ms = timeout_ms;
  if (timeout_ms > 2000) {
    // measure optimal number of attempts
    attempts = log10(timeout_ms / 1000) / log10(2) + 1;
    // measure initial exponential timeout based on attempts
    exp_timeout_ms = timeout_ms / (pow(2, attempts) - 1);
  }

  rd_kafka_event_t * event_response = nullptr;

  // Poll the event queue until we get it
  do {
    // free previously fetched event
    rd_kafka_event_destroy(event_response);
    // poll and update attempts and exponential timeout
    event_response = rd_kafka_queue_poll(topic_rkqu, exp_timeout_ms);
    attempts = attempts - 1;
    exp_timeout_ms = 2 * exp_timeout_ms;
  } while (
    rd_kafka_event_type(event_response) != event_type &&
    attempts > 0);

  // TODO: change this function so a type mismatch leads to an INVALID_TYPE
  // error rather than a null event. A null event is treated as a timeout, which
  // isn't true all the time.
  // If this isn't the type of response we want, or if we do not have a response
  // type, bail out with a null
  if (event_response == NULL ||
    rd_kafka_event_type(event_response) != event_type) {
    rd_kafka_event_destroy(event_response);
    return NULL;
  }

  return event_response;
}

Baton AdminClient::CreateTopic(rd_kafka_NewTopic_t* topic, int timeout_ms) {
  if (!IsConnected()) {
    return Baton(RdKafka::ERR__STATE);
  }

  {
    scoped_shared_write_lock lock(m_connection_lock);
    if (!IsConnected()) {
      return Baton(RdKafka::ERR__STATE);
    }

    // Make admin options to establish that we are creating topics
    rd_kafka_AdminOptions_t *options = rd_kafka_AdminOptions_new(
      m_client->c_ptr(), RD_KAFKA_ADMIN_OP_CREATETOPICS);

    // Create queue just for this operation
    rd_kafka_queue_t * topic_rkqu = rd_kafka_queue_new(m_client->c_ptr());

    rd_kafka_CreateTopics(m_client->c_ptr(), &topic, 1, options, topic_rkqu);

    // Poll for an event by type in that queue
    rd_kafka_event_t * event_response = PollForEvent(
      topic_rkqu,
      RD_KAFKA_EVENT_CREATETOPICS_RESULT,
      timeout_ms);

    // Destroy the queue since we are done with it.
    rd_kafka_queue_destroy(topic_rkqu);

    // Destroy the options we just made because we polled already
    rd_kafka_AdminOptions_destroy(options);

    // If we got no response from that operation, this is a failure
    // likely due to time out
    if (event_response == NULL) {
      return Baton(RdKafka::ERR__TIMED_OUT);
    }

    // Now we can get the error code from the event
    if (rd_kafka_event_error(event_response)) {
      // If we had a special error code, get out of here with it
      const rd_kafka_resp_err_t errcode = rd_kafka_event_error(event_response);
      rd_kafka_event_destroy(event_response);
      return Baton(static_cast<RdKafka::ErrorCode>(errcode));
    }

    // get the created results
    const rd_kafka_CreateTopics_result_t * create_topic_results =
      rd_kafka_event_CreateTopics_result(event_response);

    size_t created_topic_count;
    const rd_kafka_topic_result_t **restopics = rd_kafka_CreateTopics_result_topics(  // NOLINT
      create_topic_results,
      &created_topic_count);

    for (int i = 0 ; i < static_cast<int>(created_topic_count) ; i++) {
      const rd_kafka_topic_result_t *terr = restopics[i];
      const rd_kafka_resp_err_t errcode = rd_kafka_topic_result_error(terr);
      const char *errmsg = rd_kafka_topic_result_error_string(terr);

      if (errcode != RD_KAFKA_RESP_ERR_NO_ERROR) {
        if (errmsg) {
          const std::string errormsg = std::string(errmsg);
          rd_kafka_event_destroy(event_response);
          return Baton(static_cast<RdKafka::ErrorCode>(errcode), errormsg); // NOLINT
        } else {
          rd_kafka_event_destroy(event_response);
          return Baton(static_cast<RdKafka::ErrorCode>(errcode));
        }
      }
    }

    rd_kafka_event_destroy(event_response);
    return Baton(RdKafka::ERR_NO_ERROR);
  }
}

Baton AdminClient::DeleteTopic(rd_kafka_DeleteTopic_t* topic, int timeout_ms) {
  if (!IsConnected()) {
    return Baton(RdKafka::ERR__STATE);
  }

  {
    scoped_shared_write_lock lock(m_connection_lock);
    if (!IsConnected()) {
      return Baton(RdKafka::ERR__STATE);
    }

    // Make admin options to establish that we are deleting topics
    rd_kafka_AdminOptions_t *options = rd_kafka_AdminOptions_new(
      m_client->c_ptr(), RD_KAFKA_ADMIN_OP_DELETETOPICS);

    // Create queue just for this operation.
    // May be worth making a "scoped queue" class or something like a lock
    // for RAII
    rd_kafka_queue_t * topic_rkqu = rd_kafka_queue_new(m_client->c_ptr());

    rd_kafka_DeleteTopics(m_client->c_ptr(), &topic, 1, options, topic_rkqu);

    // Poll for an event by type in that queue
    rd_kafka_event_t * event_response = PollForEvent(
      topic_rkqu,
      RD_KAFKA_EVENT_DELETETOPICS_RESULT,
      timeout_ms);

    // Destroy the queue since we are done with it.
    rd_kafka_queue_destroy(topic_rkqu);

    // Destroy the options we just made because we polled already
    rd_kafka_AdminOptions_destroy(options);

    // If we got no response from that operation, this is a failure
    // likely due to time out
    if (event_response == NULL) {
      return Baton(RdKafka::ERR__TIMED_OUT);
    }

    // Now we can get the error code from the event
    if (rd_kafka_event_error(event_response)) {
      // If we had a special error code, get out of here with it
      const rd_kafka_resp_err_t errcode = rd_kafka_event_error(event_response);
      rd_kafka_event_destroy(event_response);
      return Baton(static_cast<RdKafka::ErrorCode>(errcode));
    }

    // get the created results
    const rd_kafka_DeleteTopics_result_t * delete_topic_results =
      rd_kafka_event_DeleteTopics_result(event_response);

    size_t deleted_topic_count;
    const rd_kafka_topic_result_t **restopics = rd_kafka_DeleteTopics_result_topics(  // NOLINT
      delete_topic_results,
      &deleted_topic_count);

    for (int i = 0 ; i < static_cast<int>(deleted_topic_count) ; i++) {
      const rd_kafka_topic_result_t *terr = restopics[i];
      const rd_kafka_resp_err_t errcode = rd_kafka_topic_result_error(terr);

      if (errcode != RD_KAFKA_RESP_ERR_NO_ERROR) {
        rd_kafka_event_destroy(event_response);
        return Baton(static_cast<RdKafka::ErrorCode>(errcode));
      }
    }

    rd_kafka_event_destroy(event_response);
    return Baton(RdKafka::ERR_NO_ERROR);
  }
}

Baton AdminClient::CreatePartitions(
  rd_kafka_NewPartitions_t* partitions,
  int timeout_ms) {
  if (!IsConnected()) {
    return Baton(RdKafka::ERR__STATE);
  }

  {
    scoped_shared_write_lock lock(m_connection_lock);
    if (!IsConnected()) {
      return Baton(RdKafka::ERR__STATE);
    }

    // Make admin options to establish that we are deleting topics
    rd_kafka_AdminOptions_t *options = rd_kafka_AdminOptions_new(
      m_client->c_ptr(), RD_KAFKA_ADMIN_OP_CREATEPARTITIONS);

    // Create queue just for this operation.
    // May be worth making a "scoped queue" class or something like a lock
    // for RAII
    rd_kafka_queue_t * topic_rkqu = rd_kafka_queue_new(m_client->c_ptr());

    rd_kafka_CreatePartitions(m_client->c_ptr(),
      &partitions, 1, options, topic_rkqu);

    // Poll for an event by type in that queue
    rd_kafka_event_t * event_response = PollForEvent(
      topic_rkqu,
      RD_KAFKA_EVENT_CREATEPARTITIONS_RESULT,
      timeout_ms);

    // Destroy the queue since we are done with it.
    rd_kafka_queue_destroy(topic_rkqu);

    // Destroy the options we just made because we polled already
    rd_kafka_AdminOptions_destroy(options);

    // If we got no response from that operation, this is a failure
    // likely due to time out
    if (event_response == NULL) {
      return Baton(RdKafka::ERR__TIMED_OUT);
    }

    // Now we can get the error code from the event
    if (rd_kafka_event_error(event_response)) {
      // If we had a special error code, get out of here with it
      const rd_kafka_resp_err_t errcode = rd_kafka_event_error(event_response);
      rd_kafka_event_destroy(event_response);
      return Baton(static_cast<RdKafka::ErrorCode>(errcode));
    }

    // get the created results
    const rd_kafka_CreatePartitions_result_t * create_partitions_results =
      rd_kafka_event_CreatePartitions_result(event_response);

    size_t created_partitions_topic_count;
    const rd_kafka_topic_result_t **restopics = rd_kafka_CreatePartitions_result_topics(  // NOLINT
      create_partitions_results,
      &created_partitions_topic_count);

    for (int i = 0 ; i < static_cast<int>(created_partitions_topic_count) ; i++) {  // NOLINT
      const rd_kafka_topic_result_t *terr = restopics[i];
      const rd_kafka_resp_err_t errcode = rd_kafka_topic_result_error(terr);
      const char *errmsg = rd_kafka_topic_result_error_string(terr);

      if (errcode != RD_KAFKA_RESP_ERR_NO_ERROR) {
        if (errmsg) {
          const std::string errormsg = std::string(errmsg);
          rd_kafka_event_destroy(event_response);
          return Baton(static_cast<RdKafka::ErrorCode>(errcode), errormsg); // NOLINT
        } else {
          rd_kafka_event_destroy(event_response);
          return Baton(static_cast<RdKafka::ErrorCode>(errcode));
        }
      }
    }

    rd_kafka_event_destroy(event_response);
    return Baton(RdKafka::ERR_NO_ERROR);
  }
}

Baton AdminClient::ListGroups(
    bool is_match_states_set,
    std::vector<rd_kafka_consumer_group_state_t> &match_states,
    bool is_match_types_set,
    std::vector<rd_kafka_consumer_group_type_t> &match_types, int timeout_ms,
    /* out */ rd_kafka_event_t **event_response) {
  if (!IsConnected()) {
    return Baton(RdKafka::ERR__STATE);
  }

  {
    scoped_shared_write_lock lock(m_connection_lock);
    if (!IsConnected()) {
      return Baton(RdKafka::ERR__STATE);
    }

    // Make admin options to establish that we are listing groups
    rd_kafka_AdminOptions_t *options = rd_kafka_AdminOptions_new(
        m_client->c_ptr(), RD_KAFKA_ADMIN_OP_LISTCONSUMERGROUPS);

    char errstr[512];
    rd_kafka_resp_err_t err = rd_kafka_AdminOptions_set_request_timeout(
        options, timeout_ms, errstr, sizeof(errstr));
    if (err != RD_KAFKA_RESP_ERR_NO_ERROR) {
      return Baton(static_cast<RdKafka::ErrorCode>(err), errstr);
    }

    if (is_match_states_set) {
      rd_kafka_error_t *error =
          rd_kafka_AdminOptions_set_match_consumer_group_states(
              options, &match_states[0], match_states.size());
      if (error) {
        return Baton::BatonFromErrorAndDestroy(error);
      }
    }

    if (is_match_types_set) {
      rd_kafka_error_t *error =
          rd_kafka_AdminOptions_set_match_consumer_group_types(
              options, &match_types[0], match_types.size());
      if (error) {
        return Baton::BatonFromErrorAndDestroy(error);
      }
    }

    // Create queue just for this operation.
    rd_kafka_queue_t *rkqu = rd_kafka_queue_new(m_client->c_ptr());

    rd_kafka_ListConsumerGroups(m_client->c_ptr(), options, rkqu);

    // Poll for an event by type in that queue
    // DON'T destroy the event. It is the out parameter, and ownership is
    // the caller's.
    *event_response = PollForEvent(
        rkqu, RD_KAFKA_EVENT_LISTCONSUMERGROUPS_RESULT, timeout_ms);

    // Destroy the queue since we are done with it.
    rd_kafka_queue_destroy(rkqu);

    // Destroy the options we just made because we polled already
    rd_kafka_AdminOptions_destroy(options);

    // If we got no response from that operation, this is a failure
    // likely due to time out
    if (*event_response == NULL) {
      return Baton(RdKafka::ERR__TIMED_OUT);
    }

    // Now we can get the error code from the event
    if (rd_kafka_event_error(*event_response)) {
      // If we had a special error code, get out of here with it
      const rd_kafka_resp_err_t errcode = rd_kafka_event_error(*event_response);
      return Baton(static_cast<RdKafka::ErrorCode>(errcode));
    }

    // At this point, event_response contains the result, which needs
    // to be parsed/converted by the caller.
    return Baton(RdKafka::ERR_NO_ERROR);
  }
}

Baton AdminClient::DescribeGroups(std::vector<std::string> &groups,
                                  bool include_authorized_operations,
                                  int timeout_ms,
                                  /* out */ rd_kafka_event_t **event_response) {
  if (!IsConnected()) {
    return Baton(RdKafka::ERR__STATE);
  }

  {
    scoped_shared_write_lock lock(m_connection_lock);
    if (!IsConnected()) {
      return Baton(RdKafka::ERR__STATE);
    }

    // Make admin options to establish that we are describing groups
    rd_kafka_AdminOptions_t *options = rd_kafka_AdminOptions_new(
        m_client->c_ptr(), RD_KAFKA_ADMIN_OP_DESCRIBECONSUMERGROUPS);

    char errstr[512];
    rd_kafka_resp_err_t err = rd_kafka_AdminOptions_set_request_timeout(
        options, timeout_ms, errstr, sizeof(errstr));
    if (err != RD_KAFKA_RESP_ERR_NO_ERROR) {
      return Baton(static_cast<RdKafka::ErrorCode>(err), errstr);
    }

    if (include_authorized_operations) {
      rd_kafka_error_t *error =
          rd_kafka_AdminOptions_set_include_authorized_operations(
              options, include_authorized_operations);
      if (error) {
        return Baton::BatonFromErrorAndDestroy(error);
      }
    }

    // Create queue just for this operation.
    rd_kafka_queue_t *rkqu = rd_kafka_queue_new(m_client->c_ptr());

    // Construct a char** to pass to librdkafka. Avoid too many allocations.
    std::vector<const char *> c_groups(groups.size());
    for (size_t i = 0; i < groups.size(); i++) {
      c_groups[i] = groups[i].c_str();
    }

    rd_kafka_DescribeConsumerGroups(m_client->c_ptr(), &c_groups[0],
                                    groups.size(), options, rkqu);

    // Poll for an event by type in that queue
    // DON'T destroy the event. It is the out parameter, and ownership is
    // the caller's.
    *event_response = PollForEvent(
        rkqu, RD_KAFKA_EVENT_DESCRIBECONSUMERGROUPS_RESULT, timeout_ms);

    // Destroy the queue since we are done with it.
    rd_kafka_queue_destroy(rkqu);

    // Destroy the options we just made because we polled already
    rd_kafka_AdminOptions_destroy(options);

    // If we got no response from that operation, this is a failure
    // likely due to time out
    if (*event_response == NULL) {
      return Baton(RdKafka::ERR__TIMED_OUT);
    }

    // Now we can get the error code from the event
    if (rd_kafka_event_error(*event_response)) {
      // If we had a special error code, get out of here with it
      const rd_kafka_resp_err_t errcode = rd_kafka_event_error(*event_response);
      return Baton(static_cast<RdKafka::ErrorCode>(errcode));
    }

    // At this point, event_response contains the result, which needs
    // to be parsed/converted by the caller.
    return Baton(RdKafka::ERR_NO_ERROR);
  }
}

Baton AdminClient::DeleteGroups(rd_kafka_DeleteGroup_t **group_list,
                                size_t group_cnt, int timeout_ms,
                                /* out */ rd_kafka_event_t **event_response) {
  if (!IsConnected()) {
    return Baton(RdKafka::ERR__STATE);
  }

  {
    scoped_shared_write_lock lock(m_connection_lock);
    if (!IsConnected()) {
      return Baton(RdKafka::ERR__STATE);
    }

    // Make admin options to establish that we are deleting groups
    rd_kafka_AdminOptions_t *options = rd_kafka_AdminOptions_new(
        m_client->c_ptr(), RD_KAFKA_ADMIN_OP_DELETEGROUPS);

    char errstr[512];
    rd_kafka_resp_err_t err = rd_kafka_AdminOptions_set_request_timeout(
        options, timeout_ms, errstr, sizeof(errstr));
    if (err != RD_KAFKA_RESP_ERR_NO_ERROR) {
      return Baton(static_cast<RdKafka::ErrorCode>(err), errstr);
    }

    // Create queue just for this operation.
    rd_kafka_queue_t *rkqu = rd_kafka_queue_new(m_client->c_ptr());

    rd_kafka_DeleteGroups(m_client->c_ptr(), group_list, group_cnt, options,
                          rkqu);

    // Poll for an event by type in that queue
    // DON'T destroy the event. It is the out parameter, and ownership is
    // the caller's.
    *event_response =
        PollForEvent(rkqu, RD_KAFKA_EVENT_DELETEGROUPS_RESULT, timeout_ms);

    // Destroy the queue since we are done with it.
    rd_kafka_queue_destroy(rkqu);

    // Destroy the options we just made because we polled already
    rd_kafka_AdminOptions_destroy(options);

    // If we got no response from that operation, this is a failure
    // likely due to time out
    if (*event_response == NULL) {
      return Baton(RdKafka::ERR__TIMED_OUT);
    }

    // Now we can get the error code from the event
    if (rd_kafka_event_error(*event_response)) {
      // If we had a special error code, get out of here with it
      const rd_kafka_resp_err_t errcode = rd_kafka_event_error(*event_response);
      return Baton(static_cast<RdKafka::ErrorCode>(errcode));
    }

    // At this point, event_response contains the result, which needs
    // to be parsed/converted by the caller.
    return Baton(RdKafka::ERR_NO_ERROR);
  }
}

Baton AdminClient::ListConsumerGroupOffsets(
    rd_kafka_ListConsumerGroupOffsets_t **req, size_t req_cnt,
    bool require_stable_offsets, int timeout_ms,
    rd_kafka_event_t **event_response) {
  if (!IsConnected()) {
    return Baton(RdKafka::ERR__STATE);
  }

  {
    scoped_shared_write_lock lock(m_connection_lock);
    if (!IsConnected()) {
      return Baton(RdKafka::ERR__STATE);
    }

    // Make admin options to establish that we are fetching offsets
    rd_kafka_AdminOptions_t *options = rd_kafka_AdminOptions_new(
        m_client->c_ptr(), RD_KAFKA_ADMIN_OP_LISTCONSUMERGROUPOFFSETS);

    char errstr[512];
    rd_kafka_resp_err_t err = rd_kafka_AdminOptions_set_request_timeout(
        options, timeout_ms, errstr, sizeof(errstr));
    if (err != RD_KAFKA_RESP_ERR_NO_ERROR) {
      return Baton(static_cast<RdKafka::ErrorCode>(err), errstr);
    }

    if (require_stable_offsets) {
      rd_kafka_error_t *error =
          rd_kafka_AdminOptions_set_require_stable_offsets(
              options, require_stable_offsets);
      if (error) {
        return Baton::BatonFromErrorAndDestroy(error);
      }
    }

    // Create queue just for this operation.
    rd_kafka_queue_t *rkqu = rd_kafka_queue_new(m_client->c_ptr());

    rd_kafka_ListConsumerGroupOffsets(m_client->c_ptr(), req, req_cnt, options,
                                      rkqu);

    // Poll for an event by type in that queue
    // DON'T destroy the event. It is the out parameter, and ownership is
    // the caller's.
    *event_response = PollForEvent(
        rkqu, RD_KAFKA_EVENT_LISTCONSUMERGROUPOFFSETS_RESULT, timeout_ms);

    // Destroy the queue since we are done with it.
    rd_kafka_queue_destroy(rkqu);

    // Destroy the options we just made because we polled already
    rd_kafka_AdminOptions_destroy(options);

    // If we got no response from that operation, this is a failure
    // likely due to time out
    if (*event_response == NULL) {
      return Baton(RdKafka::ERR__TIMED_OUT);
    }

    // Now we can get the error code from the event
    if (rd_kafka_event_error(*event_response)) {
      // If we had a special error code, get out of here with it
      const rd_kafka_resp_err_t errcode = rd_kafka_event_error(*event_response);
      return Baton(static_cast<RdKafka::ErrorCode>(errcode));
    }

    // At this point, event_response contains the result, which needs
    // to be parsed/converted by the caller.
    return Baton(RdKafka::ERR_NO_ERROR);
  }
}

Baton AdminClient::DeleteRecords(rd_kafka_DeleteRecords_t **del_records,
                                 size_t del_records_cnt,
                                 int operation_timeout_ms, int timeout_ms,
                                 rd_kafka_event_t **event_response) {
  if (!IsConnected()) {
    return Baton(RdKafka::ERR__STATE);
  }

  {
    scoped_shared_write_lock lock(m_connection_lock);
    if (!IsConnected()) {
      return Baton(RdKafka::ERR__STATE);
    }

    // Make admin options to establish that we are deleting records
    rd_kafka_AdminOptions_t *options = rd_kafka_AdminOptions_new(
        m_client->c_ptr(), RD_KAFKA_ADMIN_OP_DELETERECORDS);

    char errstr[512];
    rd_kafka_resp_err_t err = rd_kafka_AdminOptions_set_request_timeout(
        options, timeout_ms, errstr, sizeof(errstr));
    if (err != RD_KAFKA_RESP_ERR_NO_ERROR) {
      return Baton(static_cast<RdKafka::ErrorCode>(err), errstr);
    }

    err = rd_kafka_AdminOptions_set_operation_timeout(
        options, operation_timeout_ms, errstr, sizeof(errstr));
    if (err != RD_KAFKA_RESP_ERR_NO_ERROR) {
      return Baton(static_cast<RdKafka::ErrorCode>(err), errstr);
    }

    // Create queue just for this operation.
    rd_kafka_queue_t *rkqu = rd_kafka_queue_new(m_client->c_ptr());

    rd_kafka_DeleteRecords(m_client->c_ptr(), del_records,
                           del_records_cnt, options, rkqu);

    // Poll for an event by type in that queue
    // DON'T destroy the event. It is the out parameter, and ownership is
    // the caller's.
    *event_response =
        PollForEvent(rkqu, RD_KAFKA_EVENT_DELETERECORDS_RESULT, timeout_ms);

    // Destroy the queue since we are done with it.
    rd_kafka_queue_destroy(rkqu);

    // Destroy the options we just made because we polled already
    rd_kafka_AdminOptions_destroy(options);

    // If we got no response from that operation, this is a failure
    // likely due to time out
    if (*event_response == NULL) {
      return Baton(RdKafka::ERR__TIMED_OUT);
    }

    // Now we can get the error code from the event
    if (rd_kafka_event_error(*event_response)) {
      // If we had a special error code, get out of here with it
      const rd_kafka_resp_err_t errcode = rd_kafka_event_error(*event_response);
      return Baton(static_cast<RdKafka::ErrorCode>(errcode));
    }

    // At this point, event_response contains the result, which needs
    // to be parsed/converted by the caller.
    return Baton(RdKafka::ERR_NO_ERROR);
  }
}

Baton AdminClient::DescribeTopics(rd_kafka_TopicCollection_t *topics,
                                  bool include_authorized_operations,
                                  int timeout_ms,
                                  rd_kafka_event_t **event_response) {
  if (!IsConnected()) {
    return Baton(RdKafka::ERR__STATE);
  }

  {
    scoped_shared_write_lock lock(m_connection_lock);
    if (!IsConnected()) {
      return Baton(RdKafka::ERR__STATE);
    }

    // Make admin options to establish that we are describing topics
    rd_kafka_AdminOptions_t *options = rd_kafka_AdminOptions_new(
        m_client->c_ptr(), RD_KAFKA_ADMIN_OP_DESCRIBETOPICS);

    if (include_authorized_operations) {
      rd_kafka_error_t *error =
          rd_kafka_AdminOptions_set_include_authorized_operations(
              options, include_authorized_operations);
      if (error) {
        return Baton::BatonFromErrorAndDestroy(error);
      }
    }

    char errstr[512];
    rd_kafka_resp_err_t err = rd_kafka_AdminOptions_set_request_timeout(
        options, timeout_ms, errstr, sizeof(errstr));
    if (err != RD_KAFKA_RESP_ERR_NO_ERROR) {
      return Baton(static_cast<RdKafka::ErrorCode>(err), errstr);
    }

    // Create queue just for this operation.
    rd_kafka_queue_t *rkqu = rd_kafka_queue_new(m_client->c_ptr());

    rd_kafka_DescribeTopics(m_client->c_ptr(), topics, options, rkqu);

    // Poll for an event by type in that queue
    // DON'T destroy the event. It is the out parameter, and ownership is
    // the caller's.
    *event_response =
        PollForEvent(rkqu, RD_KAFKA_EVENT_DESCRIBETOPICS_RESULT, timeout_ms);

    // Destroy the queue since we are done with it.
    rd_kafka_queue_destroy(rkqu);

    // Destroy the options we just made because we polled already
    rd_kafka_AdminOptions_destroy(options);

    // If we got no response from that operation, this is a failure
    // likely due to time out
    if (*event_response == NULL) {
      return Baton(RdKafka::ERR__TIMED_OUT);
    }

    // Now we can get the error code from the event
    if (rd_kafka_event_error(*event_response)) {
      // If we had a special error code, get out of here with it
      const rd_kafka_resp_err_t errcode = rd_kafka_event_error(*event_response);
      return Baton(static_cast<RdKafka::ErrorCode>(errcode));
    }

    // At this point, event_response contains the result, which needs
    // to be parsed/converted by the caller.
    return Baton(RdKafka::ERR_NO_ERROR);
  }
}


Baton AdminClient::ListOffsets(rd_kafka_topic_partition_list_t *partitions,
                               int timeout_ms,
                               rd_kafka_IsolationLevel_t isolation_level,
                               rd_kafka_event_t **event_response) {
  if (!IsConnected()) {
    return Baton(RdKafka::ERR__STATE);
  }

  {
    scoped_shared_write_lock lock(m_connection_lock);
    if (!IsConnected()) {
      return Baton(RdKafka::ERR__STATE);
    }

    // Make admin options to establish that we are fetching offsets
    rd_kafka_AdminOptions_t *options = rd_kafka_AdminOptions_new(
        m_client->c_ptr(), RD_KAFKA_ADMIN_OP_LISTOFFSETS);

    char errstr[512];
    rd_kafka_resp_err_t err = rd_kafka_AdminOptions_set_request_timeout(
        options, timeout_ms, errstr, sizeof(errstr));
    if (err != RD_KAFKA_RESP_ERR_NO_ERROR) {
      return Baton(static_cast<RdKafka::ErrorCode>(err), errstr);
    }

    rd_kafka_error_t *error =
        rd_kafka_AdminOptions_set_isolation_level(options, isolation_level);
    if (error) {
      return Baton::BatonFromErrorAndDestroy(error);
    }

    // Create queue just for this operation.
    rd_kafka_queue_t *rkqu = rd_kafka_queue_new(m_client->c_ptr());

    rd_kafka_ListOffsets(m_client->c_ptr(), partitions, options, rkqu);

    // Poll for an event by type in that queue
    // DON'T destroy the event. It is the out parameter, and ownership is
    // the caller's.
    *event_response =
        PollForEvent(rkqu, RD_KAFKA_EVENT_LISTOFFSETS_RESULT, timeout_ms);

    // Destroy the queue since we are done with it.
    rd_kafka_queue_destroy(rkqu);

    // Destroy the options we just made because we polled already
    rd_kafka_AdminOptions_destroy(options);

    // If we got no response from that operation, this is a failure
    // likely due to time out
    if (*event_response == NULL) {
      return Baton(RdKafka::ERR__TIMED_OUT);
    }

    // Now we can get the error code from the event
    if (rd_kafka_event_error(*event_response)) {
      // If we had a special error code, get out of here with it
      const rd_kafka_resp_err_t errcode = rd_kafka_event_error(*event_response);
      return Baton(static_cast<RdKafka::ErrorCode>(errcode));
    }

    // At this point, event_response contains the result, which needs
    // to be parsed/converted by the caller.
    return Baton(RdKafka::ERR_NO_ERROR);
  }
}

void AdminClient::ActivateDispatchers() {
  // Listen to global config
  m_gconfig->listen();

  // Listen to non global config
  // tconfig->listen();

  // This should be refactored to config based management
  m_event_cb.dispatcher.Activate();
}
void AdminClient::DeactivateDispatchers() {
  // Stop listening to the config dispatchers
  m_gconfig->stop();

  // Also this one
  m_event_cb.dispatcher.Deactivate();
}

/**
 * @section
 * C++ Exported prototype functions
 */

NAN_METHOD(AdminClient::NodeConnect) {
  Nan::HandleScope scope;

  AdminClient* client = ObjectWrap::Unwrap<AdminClient>(info.This());

  // Activate the dispatchers before the connection, as some callbacks may run
  // on the background thread.
  // We will deactivate them if the connection fails.
  // Because the Admin Client connect is synchronous, we can do this within
  // AdminClient::Connect as well, but we do it here to keep the code similiar
  // to the Producer and Consumer.
  client->ActivateDispatchers();

  Baton b = client->Connect();
  // Let the JS library throw if we need to so the error can be more rich
  int error_code = static_cast<int>(b.err());
  return info.GetReturnValue().Set(Nan::New<v8::Number>(error_code));
}

NAN_METHOD(AdminClient::NodeDisconnect) {
  Nan::HandleScope scope;

  AdminClient* client = ObjectWrap::Unwrap<AdminClient>(info.This());

  Baton b = client->Disconnect();
  // Let the JS library throw if we need to so the error can be more rich
  int error_code = static_cast<int>(b.err());
  return info.GetReturnValue().Set(Nan::New<v8::Number>(error_code));
}

/**
 * Create topic
 */
NAN_METHOD(AdminClient::NodeCreateTopic) {
  Nan::HandleScope scope;

  if (info.Length() < 3 || !info[2]->IsFunction()) {
    // Just throw an exception
    return Nan::ThrowError("Need to specify a callback");
  }

  if (!info[1]->IsNumber()) {
    return Nan::ThrowError("Must provide 'timeout'");
  }

  // Create the final callback object
  v8::Local<v8::Function> cb = info[2].As<v8::Function>();
  Nan::Callback *callback = new Nan::Callback(cb);
  AdminClient* client = ObjectWrap::Unwrap<AdminClient>(info.This());

  // Get the timeout
  int timeout = Nan::To<int32_t>(info[1]).FromJust();

  std::string errstr;
  // Get that topic we want to create
  rd_kafka_NewTopic_t* topic = Conversion::Admin::FromV8TopicObject(
    info[0].As<v8::Object>(), errstr);

  if (topic == NULL) {
    Nan::ThrowError(errstr.c_str());
    return;
  }

  // Queue up dat work
  Nan::AsyncQueueWorker(
    new Workers::AdminClientCreateTopic(callback, client, topic, timeout));

  return info.GetReturnValue().Set(Nan::Null());
}

/**
 * Delete topic
 */
NAN_METHOD(AdminClient::NodeDeleteTopic) {
  Nan::HandleScope scope;

  if (info.Length() < 3 || !info[2]->IsFunction()) {
    // Just throw an exception
    return Nan::ThrowError("Need to specify a callback");
  }

  if (!info[1]->IsNumber() || !info[0]->IsString()) {
    return Nan::ThrowError("Must provide 'timeout', and 'topicName'");
  }

  // Create the final callback object
  v8::Local<v8::Function> cb = info[2].As<v8::Function>();
  Nan::Callback *callback = new Nan::Callback(cb);
  AdminClient* client = ObjectWrap::Unwrap<AdminClient>(info.This());

  // Get the topic name from the string
  std::string topic_name = Util::FromV8String(
    Nan::To<v8::String>(info[0]).ToLocalChecked());

  // Get the timeout
  int timeout = Nan::To<int32_t>(info[1]).FromJust();

  // Get that topic we want to create
  rd_kafka_DeleteTopic_t* topic = rd_kafka_DeleteTopic_new(
    topic_name.c_str());

  // Queue up dat work
  Nan::AsyncQueueWorker(
    new Workers::AdminClientDeleteTopic(callback, client, topic, timeout));

  return info.GetReturnValue().Set(Nan::Null());
}

/**
 * Delete topic
 */
NAN_METHOD(AdminClient::NodeCreatePartitions) {
  Nan::HandleScope scope;

  if (info.Length() < 4) {
    // Just throw an exception
    return Nan::ThrowError("Need to specify a callback");
  }

  if (!info[3]->IsFunction()) {
    // Just throw an exception
    return Nan::ThrowError("Need to specify a callback 2");
  }

  if (!info[2]->IsNumber() || !info[1]->IsNumber() || !info[0]->IsString()) {
    return Nan::ThrowError(
      "Must provide 'totalPartitions', 'timeout', and 'topicName'");
  }

  // Create the final callback object
  v8::Local<v8::Function> cb = info[3].As<v8::Function>();
  Nan::Callback *callback = new Nan::Callback(cb);
  AdminClient* client = ObjectWrap::Unwrap<AdminClient>(info.This());

  // Get the timeout
  int timeout = Nan::To<int32_t>(info[2]).FromJust();

  // Get the total number of desired partitions
  int partition_total_count = Nan::To<int32_t>(info[1]).FromJust();

  // Get the topic name from the string
  std::string topic_name = Util::FromV8String(
    Nan::To<v8::String>(info[0]).ToLocalChecked());

  // Create an error buffer we can throw
  char* errbuf = reinterpret_cast<char*>(malloc(100));

  // Create the new partitions request
  rd_kafka_NewPartitions_t* new_partitions = rd_kafka_NewPartitions_new(
    topic_name.c_str(), partition_total_count, errbuf, 100);

  // If we got a failure on the create new partitions request,
  // fail here
  if (new_partitions == NULL) {
    return Nan::ThrowError(errbuf);
  }

  // Queue up dat work
  Nan::AsyncQueueWorker(new Workers::AdminClientCreatePartitions(
    callback, client, new_partitions, timeout));

  return info.GetReturnValue().Set(Nan::Null());
}

/**
 * List Consumer Groups.
 */
NAN_METHOD(AdminClient::NodeListGroups) {
  Nan::HandleScope scope;

  if (info.Length() < 2 || !info[1]->IsFunction()) {
    // Just throw an exception
    return Nan::ThrowError("Need to specify a callback");
  }

  if (!info[0]->IsObject()) {
    return Nan::ThrowError("Must provide options object");
  }

  v8::Local<v8::Object> config = info[0].As<v8::Object>();

  // Create the final callback object
  v8::Local<v8::Function> cb = info[1].As<v8::Function>();
  Nan::Callback *callback = new Nan::Callback(cb);
  AdminClient *client = ObjectWrap::Unwrap<AdminClient>(info.This());

  // Get the timeout - default 5000.
  int timeout_ms = GetParameter<int64_t>(config, "timeout", 5000);

  // Get the match states, or not if they are unset.
  std::vector<rd_kafka_consumer_group_state_t> match_states;
  v8::Local<v8::String> match_consumer_group_states_key =
      Nan::New("matchConsumerGroupStates").ToLocalChecked();
  bool is_match_states_set =
      Nan::Has(config, match_consumer_group_states_key).FromMaybe(false);
  v8::Local<v8::Array> match_states_array = Nan::New<v8::Array>();

  if (is_match_states_set) {
    match_states_array = GetParameter<v8::Local<v8::Array>>(
        config, "matchConsumerGroupStates", match_states_array);
    if (match_states_array->Length()) {
      match_states = Conversion::Admin::FromV8GroupStateArray(
        match_states_array);
    }
  }

  std::vector<rd_kafka_consumer_group_type_t> match_types;
  v8::Local<v8::String> match_consumer_group_types_key =
      Nan::New("matchConsumerGroupTypes").ToLocalChecked();
  bool is_match_types_set =
      Nan::Has(config, match_consumer_group_types_key).FromMaybe(false);
  v8::Local<v8::Array> match_types_array = Nan::New<v8::Array>();

  if (is_match_types_set) {
    match_types_array = GetParameter<v8::Local<v8::Array>>(
        config, "matchConsumerGroupTypes", match_types_array);
    if (match_types_array->Length()) {
      match_types = Conversion::Admin::FromV8GroupTypeArray(
          match_types_array);
    }
  }

  // Queue the work.
  Nan::AsyncQueueWorker(new Workers::AdminClientListGroups(
      callback, client, is_match_states_set, match_states, is_match_types_set,
      match_types, timeout_ms));
}

/**
 * Describe Consumer Groups.
 */
NAN_METHOD(AdminClient::NodeDescribeGroups) {
  Nan::HandleScope scope;

  if (info.Length() < 3 || !info[2]->IsFunction()) {
    // Just throw an exception
    return Nan::ThrowError("Need to specify a callback");
  }

  if (!info[0]->IsArray()) {
    return Nan::ThrowError("Must provide group name array");
  }

  if (!info[1]->IsObject()) {
    return Nan::ThrowError("Must provide options object");
  }

  // Get list of group names to describe.
  v8::Local<v8::Array> group_names = info[0].As<v8::Array>();
  if (group_names->Length() == 0) {
    return Nan::ThrowError("Must provide at least one group name");
  }
  std::vector<std::string> group_names_vector =
      v8ArrayToStringVector(group_names);

  v8::Local<v8::Object> config = info[1].As<v8::Object>();

  // Get the timeout - default 5000.
  int timeout_ms = GetParameter<int64_t>(config, "timeout", 5000);

  // Get whether to include authorized operations - default false.
  bool include_authorized_operations =
      GetParameter<bool>(config, "includeAuthorizedOperations", false);

  // Create the final callback object
  v8::Local<v8::Function> cb = info[2].As<v8::Function>();
  Nan::Callback *callback = new Nan::Callback(cb);
  AdminClient *client = ObjectWrap::Unwrap<AdminClient>(info.This());

  // Queue the work.
  Nan::AsyncQueueWorker(new Workers::AdminClientDescribeGroups(
      callback, client, group_names_vector, include_authorized_operations,
      timeout_ms));
}

/**
 * Delete Consumer Groups.
 */
NAN_METHOD(AdminClient::NodeDeleteGroups) {
  Nan::HandleScope scope;

  if (info.Length() < 3 || !info[2]->IsFunction()) {
    // Just throw an exception
    return Nan::ThrowError("Need to specify a callback");
  }

  if (!info[0]->IsArray()) {
    return Nan::ThrowError("Must provide group name array");
  }

  if (!info[1]->IsObject()) {
    return Nan::ThrowError("Must provide options object");
  }

  // Get list of group names to delete, and convert it into an
  // rd_kafka_DeleteGroup_t array.
  v8::Local<v8::Array> group_names = info[0].As<v8::Array>();
  if (group_names->Length() == 0) {
    return Nan::ThrowError("Must provide at least one group name");
  }
  std::vector<std::string> group_names_vector =
      v8ArrayToStringVector(group_names);

  // The ownership of this array is transferred to the worker.
  rd_kafka_DeleteGroup_t **group_list = static_cast<rd_kafka_DeleteGroup_t **>(
      malloc(sizeof(rd_kafka_DeleteGroup_t *) * group_names_vector.size()));
  for (size_t i = 0; i < group_names_vector.size(); i++) {
    group_list[i] = rd_kafka_DeleteGroup_new(group_names_vector[i].c_str());
  }

  v8::Local<v8::Object> config = info[1].As<v8::Object>();

  // Get the timeout - default 5000.
  int timeout_ms = GetParameter<int64_t>(config, "timeout", 5000);

  // Create the final callback object
  v8::Local<v8::Function> cb = info[2].As<v8::Function>();
  Nan::Callback *callback = new Nan::Callback(cb);
  AdminClient *client = ObjectWrap::Unwrap<AdminClient>(info.This());

  // Queue the work.
  Nan::AsyncQueueWorker(new Workers::AdminClientDeleteGroups(
      callback, client, group_list, group_names_vector.size(), timeout_ms));
}

/**
 * List Consumer Group Offsets.
 */
NAN_METHOD(AdminClient::NodeListConsumerGroupOffsets) {
  Nan::HandleScope scope;

  if (info.Length() < 3 || !info[2]->IsFunction()) {
    return Nan::ThrowError("Need to specify a callback");
  }

  if (!info[0]->IsArray()) {
    return Nan::ThrowError("Must provide an array of 'listGroupOffsets'");
  }

  v8::Local<v8::Array> listGroupOffsets = info[0].As<v8::Array>();

  if (listGroupOffsets->Length() == 0) {
    return Nan::ThrowError("'listGroupOffsets' cannot be empty");
  }

  /**
   * The ownership of this is taken by
   * Workers::AdminClientListConsumerGroupOffsets and freeing it is also handled
   * by that class.
   */
  rd_kafka_ListConsumerGroupOffsets_t **requests =
      static_cast<rd_kafka_ListConsumerGroupOffsets_t **>(
          malloc(sizeof(rd_kafka_ListConsumerGroupOffsets_t *) *
                 listGroupOffsets->Length()));

  for (uint32_t i = 0; i < listGroupOffsets->Length(); ++i) {
    v8::Local<v8::Value> listGroupOffsetValue =
        Nan::Get(listGroupOffsets, i).ToLocalChecked();
    if (!listGroupOffsetValue->IsObject()) {
      return Nan::ThrowError("Each entry must be an object");
    }
    v8::Local<v8::Object> listGroupOffsetObj =
        listGroupOffsetValue.As<v8::Object>();

    v8::Local<v8::Value> groupIdValue;
    if (!Nan::Get(listGroupOffsetObj, Nan::New("groupId").ToLocalChecked())
             .ToLocal(&groupIdValue)) {
      return Nan::ThrowError("Each entry must have 'groupId'");
    }

    Nan::MaybeLocal<v8::String> groupIdMaybe =
        Nan::To<v8::String>(groupIdValue);
    if (groupIdMaybe.IsEmpty()) {
      return Nan::ThrowError("'groupId' must be a string");
    }
    Nan::Utf8String groupIdUtf8(groupIdMaybe.ToLocalChecked());
    std::string groupIdStr = *groupIdUtf8;

    v8::Local<v8::Value> partitionsValue;
    rd_kafka_topic_partition_list_t *partitions = NULL;

    if (Nan::Get(listGroupOffsetObj, Nan::New("partitions").ToLocalChecked())
            .ToLocal(&partitionsValue) &&
        partitionsValue->IsArray()) {
      v8::Local<v8::Array> partitionsArray = partitionsValue.As<v8::Array>();

      if (partitionsArray->Length() > 0) {
        partitions = Conversion::TopicPartition::
            TopicPartitionv8ArrayToTopicPartitionList(partitionsArray, false);
        if (partitions == NULL) {
          return Nan::ThrowError(
              "Failed to convert partitions to list, provide proper object in "
              "partitions");
        }
      }
    }

    requests[i] =
        rd_kafka_ListConsumerGroupOffsets_new(groupIdStr.c_str(), partitions);

    if (partitions != NULL) {
      rd_kafka_topic_partition_list_destroy(partitions);
    }
  }

  // Now process the second argument: options (timeout and requireStableOffsets)
  v8::Local<v8::Object> options = info[1].As<v8::Object>();

  bool require_stable_offsets =
      GetParameter<bool>(options, "requireStableOffsets", false);
  int timeout_ms = GetParameter<int64_t>(options, "timeout", 5000);

  // Create the final callback object
  v8::Local<v8::Function> cb = info[2].As<v8::Function>();
  Nan::Callback *callback = new Nan::Callback(cb);
  AdminClient *client = ObjectWrap::Unwrap<AdminClient>(info.This());

  // Queue the worker to process the offset fetch request asynchronously
  Nan::AsyncQueueWorker(new Workers::AdminClientListConsumerGroupOffsets(
      callback, client, requests, listGroupOffsets->Length(),
      require_stable_offsets, timeout_ms));
}

/**
 * Delete Records.
 */
NAN_METHOD(AdminClient::NodeDeleteRecords) {
  Nan::HandleScope scope;

  if (info.Length() < 3 || !info[2]->IsFunction()) {
    return Nan::ThrowError("Need to specify a callback");
  }

  if (!info[0]->IsArray()) {
    return Nan::ThrowError(
        "Must provide array containg 'TopicPartitionOffset' objects");
  }

  if (!info[1]->IsObject()) {
    return Nan::ThrowError("Must provide 'options' object");
  }

  // Get list of TopicPartitions to delete records from
  // and convert it into rd_kafka_DeleteRecords_t array
  v8::Local<v8::Array> delete_records_list = info[0].As<v8::Array>();

  if (delete_records_list->Length() == 0) {
    return Nan::ThrowError("Must provide at least one TopicPartitionOffset");
  }

  /**
   * The ownership of this is taken by
   * Workers::AdminClientDeleteRecords and freeing it is also handled
   * by that class.
   */
  rd_kafka_DeleteRecords_t **delete_records =
      static_cast<rd_kafka_DeleteRecords_t **>(
          malloc(sizeof(rd_kafka_DeleteRecords_t *) * 1));

  rd_kafka_topic_partition_list_t *partitions =
      Conversion::TopicPartition::TopicPartitionv8ArrayToTopicPartitionList(
          delete_records_list, true);
  if (partitions == NULL) {
    return Nan::ThrowError(
        "Failed to convert objects in delete records list, provide proper "
        "TopicPartitionOffset objects");
  }
  delete_records[0] = rd_kafka_DeleteRecords_new(partitions);

  rd_kafka_topic_partition_list_destroy(partitions);

  // Now process the second argument: options (timeout and operation_timeout)
  v8::Local<v8::Object> options = info[1].As<v8::Object>();

  int operation_timeout_ms =
      GetParameter<int64_t>(options, "operation_timeout", 60000);
  int timeout_ms = GetParameter<int64_t>(options, "timeout", 5000);

  // Create the final callback object
  v8::Local<v8::Function> cb = info[2].As<v8::Function>();
  Nan::Callback *callback = new Nan::Callback(cb);
  AdminClient *client = ObjectWrap::Unwrap<AdminClient>(info.This());

  // Queue the worker to process the offset fetch request asynchronously
  Nan::AsyncQueueWorker(new Workers::AdminClientDeleteRecords(
      callback, client, delete_records, 1, operation_timeout_ms, timeout_ms));
}

/**
 * Describe Topics.
 */
NAN_METHOD(AdminClient::NodeDescribeTopics) {
  Nan::HandleScope scope;

  if (info.Length() < 3 || !info[2]->IsFunction()) {
    return Nan::ThrowError("Need to specify a callback");
  }

  if (!info[0]->IsArray()) {
    return Nan::ThrowError("Must provide an array of 'topicNames'");
  }

  v8::Local<v8::Array> topicNames = info[0].As<v8::Array>();

  if (topicNames->Length() == 0) {
    return Nan::ThrowError("'topicNames' cannot be empty");
  }

  std::vector<std::string> topicNamesVector = v8ArrayToStringVector(topicNames);

  const char **topics = static_cast<const char **>(
      malloc(sizeof(const char *) * topicNamesVector.size()));

  for (size_t i = 0; i < topicNamesVector.size(); i++) {
    topics[i] = topicNamesVector[i].c_str();
  }

  /**
   * The ownership of this is taken by
   * Workers::AdminClientDescribeTopics and freeing it is also handled
   * by that class.
   */
  rd_kafka_TopicCollection_t *topic_collection =
      rd_kafka_TopicCollection_of_topic_names(topics, topicNamesVector.size());

  free(topics);

  v8::Local<v8::Object> options = info[1].As<v8::Object>();

  bool include_authorised_operations =
      GetParameter<bool>(options, "includeAuthorizedOperations", false);

  int timeout_ms = GetParameter<int64_t>(options, "timeout", 5000);

  v8::Local<v8::Function> cb = info[2].As<v8::Function>();
  Nan::Callback *callback = new Nan::Callback(cb);
  AdminClient *client = ObjectWrap::Unwrap<AdminClient>(info.This());

  Nan::AsyncQueueWorker(new Workers::AdminClientDescribeTopics(
      callback, client, topic_collection,
      include_authorised_operations, timeout_ms));
}


/**
 * List Offsets.
 */
NAN_METHOD(AdminClient::NodeListOffsets) {
  Nan::HandleScope scope;

  if (info.Length() < 3 || !info[2]->IsFunction()) {
    return Nan::ThrowError("Need to specify a callback");
  }

  if (!info[0]->IsArray()) {
    return Nan::ThrowError("Must provide an array of 'TopicPartitionOffsets'");
  }

  v8::Local<v8::Array> listOffsets = info[0].As<v8::Array>();

  /**
   * The ownership of this is taken by
   * Workers::AdminClientListOffsets and freeing it is also handled
   * by that class.
   */
  rd_kafka_topic_partition_list_t *partitions = Conversion::TopicPartition::
      TopicPartitionOffsetSpecv8ArrayToTopicPartitionList(listOffsets);

  // Now process the second argument: options (timeout and isolationLevel)
  v8::Local<v8::Object> options = info[1].As<v8::Object>();

  rd_kafka_IsolationLevel_t isolation_level =
      static_cast<rd_kafka_IsolationLevel_t>(GetParameter<int32_t>(
          options, "isolationLevel",
          static_cast<int32_t>(RD_KAFKA_ISOLATION_LEVEL_READ_UNCOMMITTED)));

  int timeout_ms = GetParameter<int64_t>(options, "timeout", 5000);

  // Create the final callback object
  v8::Local<v8::Function> cb = info[2].As<v8::Function>();
  Nan::Callback *callback = new Nan::Callback(cb);
  AdminClient *client = ObjectWrap::Unwrap<AdminClient>(info.This());

  // Queue the worker to process the offset fetch request asynchronously
  Nan::AsyncQueueWorker(new Workers::AdminClientListOffsets(
      callback, client, partitions, timeout_ms, isolation_level));
}

}  // namespace NodeKafka
