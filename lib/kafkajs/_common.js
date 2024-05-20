const error = require("./_error");
const process = require("process");

/* A list of kafkaJS compatible properties that we process.
 * All of these are not necessarily supported, and an error will be
 * thrown if they aren't. */
const kafkaJSProperties = {
  common: [
    "brokers",
    "clientId",
    "sasl",
    "ssl",
    "requestTimeout",
    "enforceRequestTimeout",
    "connectionTimeout",
    "authenticationTimeout",
    "retry",
    "socketFactory",
    "reauthenticationThreshold",
    "logLevel",
    'logger',
  ],
  producer: [
    'createPartitioner',
    'metadataMaxAge',
    'allowAutoTopicCreation',
    'transactionTimeout',
    'idempotent',
    'maxInFlightRequests',
    'transactionalId',
    'compression',
    'acks',
    'timeout',
  ],
  consumer: [
    'groupId',
    'partitionAssigners',
    'partitionAssignors',
    'sessionTimeout',
    'rebalanceTimeout',
    'heartbeatInterval',
    'metadataMaxAge',
    'allowAutoTopicCreation',
    'maxBytesPerPartition',
    'maxWaitTimeInMs',
    'minBytes',
    'maxBytes',
    'readUncommitted',
    'maxInFlightRequests',
    'rackId',
    'fromBeginning',
    'autoCommit',
    'autoCommitInterval',
    'autoCommitThreshold',
    'rebalanceListener',
  ],
  admin: [],
}

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

function createReplacementErrorMessage(cOrP, fnCall, property, propertyVal, replacementVal, isLK = false) {
  if (!isLK) {
    replacementVal = `kafkaJS: { ${replacementVal}, ... }`
  }
  return `'${property}' is not supported as a property to '${fnCall}', but must be passed to the ${cOrP} during creation.\n` +
    `Before: \n` +
    `\tconst ${cOrP} = kafka.${cOrP}({ ... });\n` +
    `\tawait ${cOrP}.connect();\n` +
    `\t${cOrP}.${fnCall}({ ${propertyVal}, ... });\n` +
    `After: \n` +
    `\tconst ${cOrP} = kafka.${cOrP}({ ${replacementVal}, ... });\n` +
    `\tawait ${cOrP}.connect();\n` +
    `\t${cOrP}.${fnCall}({ ... });\n` +
    (isLK ? `For more details on what can be used outside the kafkaJS block, see https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md\n` : '');
}

const CompatibilityErrorMessages = Object.freeze({
  /* Common */
  brokerString: () =>
    "The 'brokers' property must be an array of strings.\n" +
    "For example: ['kafka:9092', 'kafka2:9093']\n",
  saslUnsupportedMechanism: (mechanism) =>
    `SASL mechanism ${mechanism} is not supported.`,
  saslUsernamePasswordString: (mechanism) =>
    `The 'sasl.username' and 'sasl.password' properties must be strings and must be present for the mechanism ${mechanism}.`,
  saslOauthBearerProvider: () =>
    `The 'oauthBearerProvider' property must be a function.`,
  sslObject: () =>
    "The 'ssl' property must be a boolean. Any additional configuration must be provided outside the kafkaJS block.\n" +
    "Before: \n" +
    "\tconst kafka = new Kafka({ kafkaJS: { ssl: { rejectUnauthorized: false, ca: [ ... ], key: ..., cert: ... }, } }); \n" +
    "After: \n" +
    '\tconst kafka = new Kafka({ kafkaJS: { ssl: true, }, "enable.ssl.certificate.verification": false, "ssl.ca.location": ..., "ssl.certificate.pem": ... });\n' +
    `For more details on what can be used outside the kafkaJS block, see https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md\n`,
  retryFactorMultiplier: () =>
    +   "The 'retry.factor' and 'retry.multiplier' are not supported. They are always set to the default of 0.2 and 2 respectively.",
  retryRestartOnFailure: () =>
    "The restartOnFailure property is ignored. The client always retries on failure.",
  socketFactory: () =>
    "The socketFactory property is not supported.",
  logLevelName: (setLevel) =>
    "The log level must be one of: " + Object.keys(logLevel).join(", ") + ", was " + setLevel,
  reauthenticationThreshold: () =>
    "Reauthentication threshold cannot be set, and reauthentication is automated when 80% of connections.max.reauth.ms is reached.",
  unsupportedKey: (key) =>
    `The '${key}' property is not supported.`,
  kafkaJSCommonKey: (key) =>
    `The '${key}' property seems to be a KafkaJS property in the main config block.` +
    `It must be moved to the kafkaJS block.` +
    `\nBefore: \n` +
    `\tconst kafka = new Kafka({ ${key}: <value>, ... });\n` +
    `After: \n` +
    `\tconst kafka = new Kafka({ kafkaJS: { ${key}: <value>, ... }, ... });\n`,
  kafkaJSClientKey: (key, cOrP) =>
    `The '${key}' property seems to be a KafkaJS property in the main config block. ` +
    `It must be moved to the kafkaJS block.` +
    `\nBefore: \n` +
    `\tconst kafka = new Kafka({ ... });\n` +
    `\tconst ${cOrP} = kafka.${cOrP}({ ${key}: <value>, ... });\n` +
    `After: \n` +
    `\tconst kafka = new Kafka({ ... });\n` +
    `\tconst ${cOrP} = kafka.${cOrP}({ kafkaJS: { ${key}: <value>, ... }, ... });\n`,

  /* Producer */
  createPartitioner: () =>
    "The 'createPartitioner' property is not supported yet. The default partitioner is set to murmur2_random, compatible with the DefaultPartitioner and the Java partitioner.\n" +
    "A number of alternative partioning strategies are available through the 'rdKafka' property, for example: \n" +
    "\tconst kafka = new Kafka({ rdKafka: { 'partitioner':  'random|consistent_random|consistent|murmur2|murmur2_random|fnv1a|fnv1a_random' } });\n" +
    `For more details on what can be used inside the rdKafka block, see https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md\n`,
  sendOptionsMandatoryMissing: () =>
    "The argument passed to send must be an object, and must contain the 'topic' and 'messages' properties: {topic: string, messages: Message[]}\n",
  sendOptionsAcks: (fn) =>
    createReplacementErrorMessage('producer', fn, 'acks', 'acks: <number>', 'acks: <number>', false),
  sendOptionsCompression: (fn) =>
    createReplacementErrorMessage('producer', fn, 'compression', 'compression: <type>', 'compression: CompressionTypes.GZIP|SNAPPY|LZ4|ZSTD', false),
  sendOptionsTimeout: (fn) =>
    createReplacementErrorMessage('producer', fn, 'timeout', 'timeout: <number>', 'timeout: <number>', false),
  sendBatchMandatoryMissing: () =>
    "The argument passed to sendbatch must be an object, and must contain the 'topicMessages' property: { topicMessages: {topic: string, messages: Message[]}[] } \n",

  /* Consumer */
  partitionAssignors: () =>
    'partitionAssignors must be a list of strings from within `PartitionAssignors`.\n',
  subscribeOptionsFromBeginning: () =>
    createReplacementErrorMessage('consumer', 'subscribe', 'fromBeginning', 'fromBeginning: <boolean>', 'fromBeginning: <boolean>', false),
  subscribeOptionsMandatoryMissing: () =>
    "The argument passed to subscribe must be an object, and must contain the 'topics' or the 'topic' property: {topics: string[]} or {topic: string}\n",
  subscribeOptionsRegexFlag: () =>
    "If subscribing to topic by RegExp, no flags are allowed. /abcd/ is okay, but /abcd/i is not.\n",
  runOptionsAutoCommit: () =>
    createReplacementErrorMessage('consumer', 'run', 'autoCommit', 'autoCommit: <boolean>', 'autoCommit: <boolean>', false),
  runOptionsAutoCommitInterval: () =>
    createReplacementErrorMessage('consumer', 'run', 'autoCommitInterval', 'autoCommitInterval: <number>', 'autoCommitInterval: <number>', false),
  runOptionsAutoCommitThreshold: () =>
    "The property 'autoCommitThreshold' is not supported by run.\n",
  runOptionsRunConcurrently: () =>
    "The property 'partitionsConsumedConcurrently' is not currently supported by run\n",
});

/**
 * Converts the common configuration from KafkaJS to a format that can be used by node-rdkafka.
 * @param {object} config
 * @returns {import('../../types/config').ProducerGlobalConfig | import('../../types/config').ConsumerGlobalConfig} the converted configuration
 * @throws {error.KafkaJSError} if the configuration is invalid.
 *                              The error code will be ERR__INVALID_ARG in case of invalid arguments or features that are not supported.
 *                              The error code will be ERR__NOT_IMPLEMENTED in case of features that are not yet implemented.
 */
function kafkaJSToRdKafkaConfig(config) {
  /* Since the kafkaJS block is specified, we operate in
   * kafkaJS compatibility mode. That means we change the defaults
   * match the kafkaJS defaults. */
  const rdkafkaConfig = {};

  if (Object.hasOwn(config, "brokers")) {
    if (!Array.isArray(config["brokers"])) {
      throw new error.KafkaJSError(CompatibilityErrorMessages.brokerString(), {
        code: error.ErrorCodes.ERR__INVALID_ARG,
      });
    }
    rdkafkaConfig["bootstrap.servers"] = config["brokers"].join(",");
  }

  if (Object.hasOwn(config, "clientId")) {
    rdkafkaConfig["client.id"] = config.clientId;
  }

  let withSASL = false;

  if (Object.hasOwn(config, "sasl")) {
    const sasl = config.sasl;
    const mechanism = sasl.mechanism.toUpperCase();

    if (mechanism === 'OAUTHBEARER') {
      rdkafkaConfig["sasl.mechanism"] = mechanism;
      if (Object.hasOwn(sasl, "oauthBearerProvider")) {
        if (typeof sasl.oauthBearerProvider !== 'function') {
          throw new error.KafkaJSError(CompatibilityErrorMessages.saslOauthBearerProvider(), {
            code: error.ErrorCodes.ERR__INVALID_ARG,
          });
        }
        rdkafkaConfig['oauthbearer_token_refresh_cb'] = function (oauthbearer_config) {
          return sasl.oauthBearerProvider(oauthbearer_config)
            .then((token) => {
              if (!Object.hasOwn(token, 'value')) {
                throw new error.KafkaJSError('Token must have a value property.', {
                  code: error.ErrorCodes.ERR__INVALID_ARG,
                });
              } else if (!Object.hasOwn(token, 'principal')) {
                throw new error.KafkaJSError('Token must have a principal property.', {
                  code: error.ErrorCodes.ERR__INVALID_ARG,
                });
              } else if (!Object.hasOwn(token, 'lifetime')) {
                throw new error.KafkaJSError('Token must have a lifetime property.', {
                  code: error.ErrorCodes.ERR__INVALID_ARG,
                });
              }

              // Recast token into a value expected by node-rdkafka's callback.
              const setToken = {
                tokenValue: token.value,
                extensions: token.extensions,
                principal: token.principal,
                lifetime: token.lifetime,
              };
              return setToken;
            })
            .catch(err => {
              if (!(err instanceof Error)) {
                err = new Error(err);
              }
              throw err;
            });
        }
      }
    /* It's a valid case (unlike in KafkaJS) for oauthBearerProvider to be
    * null, because librdkafka provides an unsecured token provider for
    * non-prod usecases. So don't do anything in that case. */
    } else if (mechanism === 'PLAIN' || mechanism.startsWith('SCRAM')) {
      if (typeof sasl.username !== "string" || typeof sasl.password !== "string") {
        throw new error.KafkaJSError(CompatibilityErrorMessages.saslUsernamePasswordString(mechanism), {
          code: error.ErrorCodes.ERR__INVALID_ARG,
        });
      }
      rdkafkaConfig["sasl.mechanism"] = mechanism;
      rdkafkaConfig["sasl.username"] = sasl.username;
      rdkafkaConfig["sasl.password"] = sasl.password;
    } else {
      throw new error.KafkaJSError(CompatibilityErrorMessages.saslUnsupportedMechanism(mechanism), {
        code: error.ErrorCodes.ERR__INVALID_ARG,
      });
    }

    withSASL = true;
  }

  if (Object.hasOwn(config, "ssl") && config.ssl && withSASL) {
    rdkafkaConfig["security.protocol"] = "sasl_ssl";
  } else if (withSASL) {
    rdkafkaConfig["security.protocol"] = "sasl_plaintext";
  } else if (Object.hasOwn(config, "ssl") && config.ssl) {
    rdkafkaConfig["security.protocol"] = "ssl";
  }

  /* TODO: add best-effort support for ssl besides just true/false */
  if (Object.hasOwn(config, "ssl") && typeof config.ssl !== "boolean") {
    throw new error.KafkaJSError(CompatibilityErrorMessages.sslObject(), {
      code: error.ErrorCodes.ERR__INVALID_ARG,
    });
  }

  if (Object.hasOwn(config, "requestTimeout")) {
    rdkafkaConfig["socket.timeout.ms"] = config.requestTimeout;
  } else {
    /* KafkaJS default */
    rdkafkaConfig["socket.timeout.ms"] = 30000;
  }

  if (Object.hasOwn(config, "enforceRequestTimeout") && !config.enforceRequestTimeout) {
    rdkafkaConfig["socket.timeout.ms"] = 300000;
  }

  const connectionTimeout = config.connectionTimeout ?? 1000;
  const authenticationTimeout = config.authenticationTimeout ?? 10000;
  let totalConnectionTimeout = Number(connectionTimeout) + Number(authenticationTimeout);

  /* The minimum value for socket.connection.setup.timeout.ms is 1000. */
  totalConnectionTimeout = Math.max(totalConnectionTimeout, 1000);
  rdkafkaConfig["socket.connection.setup.timeout.ms"] = totalConnectionTimeout;

  const retry = config.retry ?? {};
  const { maxRetryTime, initialRetryTime, factor, multiplier, restartOnFailure } = retry;

  rdkafkaConfig["retry.backoff.max.ms"] = maxRetryTime ?? 30000;
  rdkafkaConfig["retry.backoff.ms"] = initialRetryTime ?? 300;

  if ((typeof factor === 'number') || (typeof multiplier === 'number')) {
    throw new error.KafkaJSError(CompatibilityErrorMessages.retryFactorMultiplier(), {
      code: error.ErrorCodes.ERR__INVALID_ARG,
    });
  }

  if (restartOnFailure) {
    throw new error.KafkaJSError(CompatibilityErrorMessages.retryRestartOnFailure(), {
      code: error.ErrorCodes.ERR__INVALID_ARG,
    });
  }

  if (Object.hasOwn(config, "socketFactory")) {
    throw new error.KafkaJSError(CompatibilityErrorMessages.socketFactory(), {
      code: error.ErrorCodes.ERR__INVALID_ARG,
    });
  }

  if (Object.hasOwn(config, "reauthenticationThreshold")) {
    throw new error.KafkaJSError(CompatibilityErrorMessages.reauthenticationThreshold(), {
      code: error.ErrorCodes.ERR__INVALID_ARG,
    });
  }

  rdkafkaConfig["log_level"] = 6 /* LOG_INFO - default in KafkaJS compatibility mode. */;
  if (Object.hasOwn(config, "logLevel")) {
    let setLevel = config.logLevel;

    if (process.env.KAFKAJS_LOG_LEVEL) {
      setLevel = logLevel[process.env.KAFKAJS_LOG_LEVEL.toUpperCase()];
    }
    switch (setLevel) {
      case logLevel.NOTHING:
        rdkafkaConfig["log_level"] = 0; /* LOG_EMERG - we don't have a true log nothing yet */
        break;
      case logLevel.ERROR:
        rdkafkaConfig["log_level"] = 3 /* LOG_ERR */;
        break;
      case logLevel.WARN:
        rdkafkaConfig["log_level"] = 4 /* LOG_WARNING */;
        break;
      case logLevel.INFO:
        rdkafkaConfig["log_level"] = 6 /* LOG_INFO */;
        break;
      case logLevel.DEBUG:
        rdkafkaConfig["debug"] = "all" /* Turn on debug logs for everything, otherwise this log level is not useful*/;
        rdkafkaConfig["log_level"] = 7 /* LOG_DEBUG */;
        break;
      default:
        throw new error.KafkaJSError(CompatibilityErrorMessages.logLevelName(setLevel), {
          code: error.ErrorCodes.ERR__INVALID_ARG,
        });
    }
  }

  return rdkafkaConfig;
}

/**
 * Checks if the config object contains any keys not allowed by KafkaJS.
 * @param {'producer'|'consumer'|'admin'} clientType
 * @param {any} config
 * @returns {string|null} the first unsupported key, or null if all keys are supported.
 */
function checkAllowedKeys(clientType, config) {
  const allowedKeysCommon = kafkaJSProperties.common;

  if (!Object.hasOwn(kafkaJSProperties, clientType)) {
    throw new error.KafkaJSError(`Unknown client type ${clientType}`, {
      code: error.ErrorCodes.ERR__INVALID_ARG,
    });
  }
  const allowedKeysSpecific = kafkaJSProperties[clientType];

  for (const key of Object.keys(config)) {
    if (!allowedKeysCommon.includes(key) && !allowedKeysSpecific.includes(key)) {
      return key;
    }
  }

  return null;
}

/**
 * Checks if the config object contains any keys specific to KafkaJS.
 * @param {'producer'|'consumer'|'admin'|'common'} propertyType
 * @param {any} config
 * @returns {string|null} the first KafkaJS specific key, or null if none is present.
 */
function checkIfKafkaJsKeysPresent(propertyType, config) {
  if (!Object.hasOwn(kafkaJSProperties, propertyType)) {
    throw new error.KafkaJSError(`Unknown config type for ${propertyType}`, {
      code: error.ErrorCodes.ERR__INVALID_ARG,
    });
  }
  const kjsKeys = kafkaJSProperties[propertyType];

  for (const key of Object.keys(config)) {
    /* We exclude 'acks' since it's common to both librdkafka and kafkaJS.
     * We don't intend to keep up with new properties, so we don't need to really worry about making it extensible. */
    if (kjsKeys.includes(key) && key !== 'acks') {
      return key;
    }
  }

  return null;
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
 * Converts a topicPartitionOffset from KafkaJS to a format that can be used by node-rdkafka.
 * Includes metadata.
 *
 * @param {import("../../types/kafkajs").TopicPartitionOffsetAndMetadata} tpo
 * @returns {import("../../types/rdkafka").TopicPartitionOffsetAndMetadata}
 */
function topicPartitionOffsetMetadataToRdKafka(tpo) {
  return {
    topic: tpo.topic,
    partition: tpo.partition,
    offset: tpo.offset ? Number(tpo.offset) : null,
    metadata: tpo.metadata,
  };
}

/**
 * Converts a topicPartitionOffset from node-rdkafka to a format that can be used by KafkaJS.
 * Includes metadata.
 *
 * @param {import("../../types/rdkafka").TopicPartitionOffsetAndMetadata} tpo
 * @returns {import("../../types/kafkajs").TopicPartitionOffsetAndMetadata}
 */
function topicPartitionOffsetMetadataToKafkaJS(tpo) {
  return {
    topic: tpo.topic,
    partition: tpo.partition,
    offset: tpo.offset ? tpo.offset.toString() : null,
    metadata: tpo.metadata,
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
    if (value && value.constructor === Array) {
      for (const v of value) {
        const header = {};
        header[key] = v;
        headers.push(header);
      }
    } else {
      const header = {};
      header[key] = value;
      headers.push(header);
    }
  }
  return headers;
}


function notImplemented(msg = 'Not implemented') {
  throw new error.KafkaJSError(msg, { code: error.ErrorCodes.ERR__NOT_IMPLEMENTED });
}

/* Code from the async lock is from github.com/tulios/kafkajs.
 * For more details, see LICENSE.kafkajs */
const LockStates = Object.freeze({
  LOCKED: 'locked',
  TIMEOUT: 'timeout',
  WAITING: 'waiting',
  TIMEOUT_ERROR_MESSAGE: 'timeoutErrorMessage',
});

class Lock {
  constructor({ timeout, description = null } = {}) {
    if (typeof timeout !== 'number') {
      throw new TypeError(`'timeout' is not a number, received '${typeof timeout}'`);
    }

    this[LockStates.LOCKED] = false;
    this[LockStates.TIMEOUT] = timeout;
    this[LockStates.WAITING] = new Set();
    this[LockStates.TIMEOUT_ERROR_MESSAGE] = () => {
      const timeoutMessage = `Timeout while acquiring lock (${this[LockStates.WAITING].size} waiting locks)`;
      return description ? `${timeoutMessage}: "${description}"` : timeoutMessage;
    }
  }

  async acquire() {
    return new Promise((resolve, reject) => {
      if (!this[LockStates.LOCKED]) {
        this[LockStates.LOCKED] = true;
        return resolve();
      }

      let timeoutId = null;
      const tryToAcquire = async () => {
        if (!this[LockStates.LOCKED]) {
          this[LockStates.LOCKED] = true;
          clearTimeout(timeoutId);
          this[LockStates.WAITING].delete(tryToAcquire);
          return resolve();
        }
      }

      this[LockStates.WAITING].add(tryToAcquire);
      timeoutId = setTimeout(() => {
        // The message should contain the number of waiters _including_ this one
        const e = new error.KafkaJSLockTimeout(this[LockStates.TIMEOUT_ERROR_MESSAGE]());
        this[LockStates.WAITING].delete(tryToAcquire);
        reject(e);
      }, this[LockStates.TIMEOUT]);
    })
  }

  async release() {
    this[LockStates.LOCKED] = false;
    const waitingLock = this[LockStates.WAITING].values().next().value;

    if (waitingLock) {
      return waitingLock();
    }
  }
}

/**
  * Acquires a lock, or logs an error if it fails.
  * @param {Lock} lock
  * @param {import("../../types/kafkajs").Logger} logger
  * @returns {boolean} true if the lock was acquired, false otherwise.
  */
async function acquireOrLog(lock, logger) {
  try {
    await lock.acquire();
    return true;
  } catch (e) {
    logger.error(`Failed to acquire lock: ${e.message}`);
  }
  return false;
}

module.exports = {
  kafkaJSToRdKafkaConfig,
  topicPartitionOffsetToRdKafka,
  topicPartitionOffsetMetadataToRdKafka,
  topicPartitionOffsetMetadataToKafkaJS,
  createKafkaJsErrorFromLibRdKafkaError,
  convertToRdKafkaHeaders,
  notImplemented,
  logLevel,
  loggerTrampoline,
  DefaultLogger,
  createReplacementErrorMessage,
  CompatibilityErrorMessages,
  severityToLogLevel,
  checkAllowedKeys,
  checkIfKafkaJsKeysPresent,
  Lock,
  acquireOrLog,
};
