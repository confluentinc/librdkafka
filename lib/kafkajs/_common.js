const error = require("./_error");
const LibrdKafkaError = require('../error');

/**
 * @function kafkaJSToRdKafkaConfig()
 * @param {object} config
 * @returns {{globalConfig: import("../../types/config").ConsumerGlobalConfig|import("../../types/config").ProducerTopicConfig, topicConfig: import("../../types/config").ConsumerTopicConfig|import("../../types/config").ProducerTopicConfig}}
 */
async function kafkaJSToRdKafkaConfig(config) {
  const globalConfig = {
    "allow.auto.create.topics": "false",
  };
  const topicConfig = {};
  globalConfig["bootstrap.servers"] = config["brokers"].join(",");

  let withSASL = false;

  if (config.sasl) {
    const sasl = config.sasl;
    if (
      sasl.mechanism === "plain" &&
      typeof sasl.username === "string" &&
      typeof sasl.password === "string"
    ) {
      globalConfig["sasl.mechanism"] = "PLAIN";
      globalConfig["sasl.username"] = sasl.username;
      globalConfig["sasl.password"] = sasl.password;
      withSASL = true;
    }
  }

  if (config.ssl === true && withSASL) {
    globalConfig["security.protocol"] = "sasl_ssl";
  } else if (withSASL) {
    globalConfig["security.protocol"] = "sasl_plaintext";
  }

  if (config.rdKafka) {
    if (config.rdKafka.constructor === Function) {
      await config.rdKafka(globalConfig, topicConfig);
    } else {
      Object.assign(globalConfig, config.rdKafka.globalConfig);
      Object.assign(topicConfig, config.rdKafka.topicConfig);
    }
  }

  return { globalConfig, topicConfig };
}

function topicPartitionOffsetToRdKafka(tpo) {
  return {
    topic: tpo.topic,
    partition: tpo.partition,
    offset: Number(tpo.offset),
  };
}

/**
 * Convert a librdkafka error from node-rdkafka into a KafkaJSError.
 * @param {LibrdKafkaError} librdKafkaError to convert from.
 * @returns KafkaJSError
 */
function createKafkaJsErrorFromLibRdKafkaError(librdKafkaError) {
  const properties = {
    retriable: librdKafkaError.retriable,
    fatal: librdKafkaError.fatal,
    abortable: librdKafkaError.abortable,
    stack: librdKafkaError.stack,
    code: librdKafkaError.code,
  };

  let err = null;

  if (properties.code === error.ErrorCodes.ERR_OFFSET_OUT_OF_RANGE) {
    err = new error.KafkaJSOffsetOutOfRange(e, properties);
  } else if (properties.code === error.ErrorCodes.ERR_REQUEST_TIMED_OUT) {
    err = new error.KafkaJSRequestTimeoutError(e, properties);
  } else if (properties.code === error.ErrorCodes.ERR__PARTIAL) {
    err = new error.KafkaJSPartialMessageError(e, properties);
  } else if (properties.code === error.ErrorCodes.ERR__AUTHENTICATION) {
    err = new error.KafkaJSSASLAuthenticationError(e, properties);
  } else if (properties.code === error.ErrorCodes.ERR_GROUP_COORDINATOR_NOT_AVAILABLE) {
    err = new error.KafkaJSGroupCoordinatorNotAvailableError(e, properties);
  } else if (properties.code === error.ErrorCodes.ERR__NOT_IMPLEMENTED) {
    err = new error.KafkaJSNotImplemented(e, properties);
  } else if (properties.code === error.ErrorCodes.ERR__TIMED_OUT) {
    err = new error.KafkaJSTimedOut(e, properties);
  } else if (properties.code === error.ErrorCodes.ERR__ALL_BROKERS_DOWN) {
    err = new error.KafkaJSNoBrokerAvailableError(e, properties);
  } else if (properties.code === error.ErrorCodes.ERR__TRANSPORT) {
    err = new error.KafkaJSConnectionError(e, properties);
  } else if (properties.code > 0) { /* Indicates a non-local error */
    err = new error.KafkaJSProtocolError(e, properties);
  } else {
    err = new error.KafkaJSError(e, properties);
  }

  return err;
}

module.exports = {
  kafkaJSToRdKafkaConfig,
  topicPartitionOffsetToRdKafka,
  createKafkaJsErrorFromLibRdKafkaError,
};
