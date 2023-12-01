const error = require("./_error");

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

/**
 * Converts a topicPartitionOffset from KafkaJS to a format that can be used by node-rdkafka.
 * @param {import("../../types/kafkajs").TopicPartitionOffset} tpo
 * @returns {{topic: string, partition: number, offset: number}}
 */
function topicPartitionOffsetToRdKafka(tpo) {
  // TODO: do we need some checks for negative offsets and stuff? Or 'named' offsets?
  return {
    topic: tpo.topic,
    partition: tpo.partition,
    offset: Number(tpo.offset),
  };
}

/**
 * Convert a librdkafka error from node-rdkafka into a KafkaJSError.
 * @param {import("../error")} librdKafkaError to convert from.
 * @returns {error.KafkaJSError} the converted error.
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
    err = new error.KafkaJSOffsetOutOfRange(librdKafkaError, properties);
  } else if (properties.code === error.ErrorCodes.ERR_REQUEST_TIMED_OUT) {
    err = new error.KafkaJSRequestTimeoutError(librdKafkaError, properties);
  } else if (properties.code === error.ErrorCodes.ERR__PARTIAL) {
    err = new error.KafkaJSPartialMessageError(librdKafkaError, properties);
  } else if (properties.code === error.ErrorCodes.ERR__AUTHENTICATION) {
    err = new error.KafkaJSSASLAuthenticationError(librdKafkaError, properties);
  } else if (properties.code === error.ErrorCodes.ERR_GROUP_COORDINATOR_NOT_AVAILABLE) {
    err = new error.KafkaJSGroupCoordinatorNotAvailableError(librdKafkaError, properties);
  } else if (properties.code === error.ErrorCodes.ERR__NOT_IMPLEMENTED) {
    err = new error.KafkaJSNotImplemented(librdKafkaError, properties);
  } else if (properties.code === error.ErrorCodes.ERR__TIMED_OUT) {
    err = new error.KafkaJSTimedOut(librdKafkaError, properties);
  } else if (properties.code === error.ErrorCodes.ERR__ALL_BROKERS_DOWN) {
    err = new error.KafkaJSNoBrokerAvailableError(librdKafkaError, properties);
  } else if (properties.code === error.ErrorCodes.ERR__TRANSPORT) {
    err = new error.KafkaJSConnectionError(librdKafkaError, properties);
  } else if (properties.code > 0) { /* Indicates a non-local error */
    err = new error.KafkaJSProtocolError(librdKafkaError, properties);
  } else {
    err = new error.KafkaJSError(librdKafkaError, properties);
  }

  console.log("Converted err = " + JSON.stringify(err, null, 2) + " librdkafka erro = " + JSON.stringify(librdKafkaError, null, 2));
  return err;
}

/**
 * Converts KafkaJS headers to a format that can be used by node-rdkafka.
 * @param {import("../../types/kafkajs").IHeaders|null} kafkaJSHeaders
 * @returns {import("../../").MessageHeader[]|null} the converted headers.
 */
function convertToRdKafkaHeaders(kafkaJSHeaders) {
  if (!kafkaJSHeaders) return null;

  const headers = [];
  for (const [key, value] of Object.entries(kafkaJSHeaders)) {
    if (value.constructor === Array) {
      for (const v of value) {
        headers.push({ key, value: v });
      }
    } else {
      headers.push({ key, value });
    }
  }
  return headers;
}


function notImplemented(msg = 'Not implemented') {
  throw new error.KafkaJSError(msg, { code: error.ErrorCodes.ERR__NOT_IMPLEMENTED });
}

module.exports = {
  kafkaJSToRdKafkaConfig,
  topicPartitionOffsetToRdKafka,
  createKafkaJsErrorFromLibRdKafkaError,
  convertToRdKafkaHeaders,
  notImplemented,
};
