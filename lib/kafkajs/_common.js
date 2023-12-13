const error = require("./_error");
const process = require("process");

const logLevel = Object.freeze({
  NOTHING: 0,
  ERROR: 1,
  WARN: 2,
  INFO: 3,
  DEBUG: 4,
});

const severityToLogLevel = Object.freeze({
  0: logLevel.NOTHING,
  1: logLevel.ERROR,
  2: logLevel.ERROR,
  3: logLevel.ERROR,
  4: logLevel.WARN,
  5: logLevel.WARN,
  6: logLevel.INFO,
  7: logLevel.DEBUG,
});

/**
 * Default logger implementation.
 * @type import("../../types/kafkajs").Logger
 */
class DefaultLogger {
  constructor() {
    this.logLevel = logLevel.INFO;
  }

  setLogLevel(logLevel) {
    this.logLevel = logLevel;
  }

  info(message, extra) {
    if (this.logLevel >= logLevel.INFO)
      console.info({ message, ...extra });
  }

  error(message, extra) {
    if (this.logLevel >= logLevel.ERROR)
      console.error({ message, ...extra });
  }

  warn(message, extra) {
    if (this.logLevel >= logLevel.WARN)
      console.warn({ message, ...extra });
  }

  debug(message, extra) {
    if (this.logLevel >= logLevel.DEBUG)
      console.log({ message, ...extra });
  }

  namespace() {
    return this;
  }
}

/**
 * Trampoline for user defined logger, if any.
 * @param {{severity: number, fac: string, message: string}} msg
 *
 */
function loggerTrampoline(msg, logger) {
  if (!logger) {
    return;
  }

  const level = severityToLogLevel[msg.severity];
  switch (level) {
    case logLevel.NOTHING:
      break;
    case logLevel.ERROR:
      logger.error(msg.message, { fac: msg.fac, timestamp: Date.now() });
      break;
    case logLevel.WARN:
      logger.warn(msg.message, { fac: msg.fac, timestamp: Date.now() });
      break;
    case logLevel.INFO:
      logger.info(msg.message, { fac: msg.fac, timestamp: Date.now() });
      break;
    case logLevel.DEBUG:
      logger.debug(msg.message, { fac: msg.fac, timestamp: Date.now() });
      break;
    default:
      throw new error.KafkaJSError("Invalid logLevel", {
        code: error.ErrorCodes.ERR__INVALID_ARG,
      });
  }

}

/**
 * Converts the common configuration from KafkaJS to a format that can be used by node-rdkafka.
 * @param {object} config
 * @returns {{globalConfig: import("../../types/config").ConsumerGlobalConfig|import("../../types/config").ProducerTopicConfig, topicConfig: import("../../types/config").ConsumerTopicConfig|import("../../types/config").ProducerTopicConfig}}
 * @throws {error.KafkaJSError} if the configuration is invalid.
 *                              The error code will be ERR__INVALID_ARG in case of invalid arguments or features that are not supported.
 *                              The error code will be ERR__NOT_IMPLEMENTED in case of features that are not yet implemented.
 */
async function kafkaJSToRdKafkaConfig(config) {
  const globalConfig = {};
  const topicConfig = {};

  if (!Array.isArray(config["brokers"])) {
    throw new error.KafkaJSError("brokers must be an list of strings", {
      code: error.ErrorCodes.ERR__INVALID_ARG,
    });
  }
  globalConfig["bootstrap.servers"] = config["brokers"].join(",");

  if (Object.hasOwn(config, "clientId")) {
    globalConfig["client.id"] = config.clientId;
  }

  let withSASL = false;

  if (Object.hasOwn(config, "sasl")) {
    const sasl = config.sasl;
    const mechanism = sasl.mechanism.toUpperCase();

    if (mechanism === 'OAUTHBEARER') {
      throw new error.KafkaJSError("OAUTHBEARER is not supported", {
        code: error.ErrorCodes.ERR__NOT_IMPLEMENTED,
      });
    }

    /* The mechanism must be PLAIN or SCRAM. */

    if (typeof sasl.username !== "string" || typeof sasl.password !== "string") {
      throw new error.KafkaJSError("username and password must be present and be strings", {
        code: error.ErrorCodes.ERR__INVALID_ARG,
      });
    }

    globalConfig["sasl.mechanism"] = mechanism;
    globalConfig["sasl.username"] = sasl.username;
    globalConfig["sasl.password"] = sasl.password;
    withSASL = true;
  }

  if (Object.hasOwn(config, "ssl") && withSASL) {
    globalConfig["security.protocol"] = "sasl_ssl";
  } else if (withSASL) {
    globalConfig["security.protocol"] = "sasl_plaintext";
  }

  if (Object.hasOwn(config, "requestTimeout")) {
    globalConfig["socket.timeout.ms"] = config.requestTimeout;
  }

  if (Object.hasOwn(config, "enforceRequestTimeout")) {
    globalConfig["socket.timeout.ms"] = 300000;
  }

  const connectionTimeout = config.connectionTimeout ?? 0;
  const authenticationTimeout = config.authenticationTimeout ?? 0;
  let totalConnectionTimeout = Number(connectionTimeout) + Number(authenticationTimeout);

  /* The minimum value for socket.connection.setup.timeout.ms is 1000. */
  if (totalConnectionTimeout) {
    totalConnectionTimeout = Math.max(totalConnectionTimeout, 1000);
    globalConfig["socket.connection.setup.timeout.ms"] = totalConnectionTimeout;
  }

  if (Object.hasOwn(config, "retry")) {
    const { maxRetryTime, initialRetryTime, factor, multiplier, retries } = config.retry;

    if (maxRetryTime) {
      globalConfig["retry.backoff.max.ms"] = maxRetryTime;
    }

    if (initialRetryTime) {
      globalConfig["retry.backoff.ms"] = initialRetryTime;
    }

    if (retries) {
      globalConfig["retries"] = retries;
    }

    if (factor || multiplier) {
      throw new error.KafkaJSError("retry.factor and retry.multiplier are not supported", {
        code: error.ErrorCodes.ERR__INVALID_ARG,
      });
    }
  }

  if (Object.hasOwn(config, "restartOnFailure") && !config.restartOnFailure) {
    throw new error.KafkaJSError("restartOnFailure cannot be false, it must be true or unset", {
      code: error.ErrorCodes.ERR__INVALID_ARG,
    });
  }

  if (Object.hasOwn(config, "socketFactory")) {
    throw new error.KafkaJSError("socketFactory is not supported", {
      code: error.ErrorCodes.ERR__INVALID_ARG,
    });
  }

  if (Object.hasOwn(config, "logLevel")) {
    let setLevel = config.logLevel;

    if (process.env.KAFKAJS_LOG_LEVEL) {
      setLevel = logLevel[process.env.KAFKAJS_LOG_LEVEL.toUpperCase()];
    }

    switch (setLevel) {
      case logLevel.NOTHING:
        globalConfig["log_level"] = 0; /* LOG_EMERG - we don't have a true log nothing yet */
        break;
      case logLevel.ERROR:
        globalConfig["log_level"] = 3 /* LOG_ERR */;
        break;
      case logLevel.WARN:
        globalConfig["log_level"] = 4 /* LOG_WARNING */;
        break;
      case logLevel.INFO:
        globalConfig["log_level"] = 6 /* LOG_INFO */;
        break;
      case logLevel.DEBUG:
        globalConfig["debug"] = "all" /* this will set librdkafka log_level to 7 */;
        break;
      default:
        throw new error.KafkaJSError("Invalid logLevel", {
          code: error.ErrorCodes.ERR__INVALID_ARG,
        });
    }
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
    err = new error.KafkaJSTimeout(librdKafkaError, properties);
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
  logLevel,
  loggerTrampoline,
  DefaultLogger,
};
