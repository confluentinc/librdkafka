const RdKafka = require('../rdkafka');
const { kafkaJSToRdKafkaConfig,
  topicPartitionOffsetToRdKafka,
  createKafkaJsErrorFromLibRdKafkaError,
  convertToRdKafkaHeaders,
  createBindingMessageMetadata,
  DefaultLogger,
  loggerTrampoline,
  severityToLogLevel,
  checkAllowedKeys,
  CompatibilityErrorMessages,
  logLevel,
} = require('./_common');
const error = require('./_error');
const { Buffer } = require('buffer');

const ProducerState = Object.freeze({
  INIT: 0,
  CONNECTING: 1,
  INITIALIZING_TRANSACTIONS: 2,
  INITIALIZED_TRANSACTIONS: 3,
  CONNECTED: 4,
  DISCONNECTING: 5,
  DISCONNECTED: 6,
});

const CompressionTypes = Object.freeze({
  None: 'none',
  GZIP: 'gzip',
  SNAPPY: 'snappy',
  LZ4: 'lz4',
  ZSTD: 'zstd',
});

class Producer {
  /**
   * The config supplied by the user.
   * @type {import("../../types/kafkajs").ProducerConstructorConfig|null}
   */
  #userConfig = null;

  /**
   * The config realized after processing any compatibility options.
   * @type {import("../../types/config").ProducerGlobalConfig|null}
   */
  #internalConfig = null;

  /**
   * internalClient is the node-rdkafka client used by the API.
   * @type {import("../rdkafka").Producer|null}
   */
  #internalClient = null;

  /**
   * connectPromiseFunc is the set of promise functions used to resolve/reject the connect() promise.
   * @type {{resolve: Function, reject: Function}|{}}
   */
  #connectPromiseFunc = {};

  /**
   * state is the current state of the producer.
   * @type {ProducerState}
   */
  #state = ProducerState.INIT;

  /**
   * ongoingTransaction is true if there is an ongoing transaction.
   * @type {boolean}
   */
  #ongoingTransaction = false;

  /**
   * A logger for the producer.
   * @type {import("../../types/kafkajs").Logger}
   */
  #logger = new DefaultLogger();

  /**
   * @constructor
   * @param {import("../../types/kafkajs").ProducerConfig} kJSConfig
   */
  constructor(kJSConfig) {
    this.#userConfig = kJSConfig;
  }

  /**
   * The client name used by the producer for logging - determined by librdkafka
   * using a combination of clientId and an integer.
   * @type {string|undefined}
   */
  #clientName = undefined;

  /**
   * Convenience function to create the metadata object needed for logging.
   */
  #createProducerBindingMessageMetadata() {
    return createBindingMessageMetadata(this.#clientName);
  }

  #config() {
    if (!this.#internalConfig)
      this.#internalConfig = this.#finalizedConfig();
    return this.#internalConfig;
  }

  #kafkaJSToProducerConfig(kjsConfig) {
    if (!kjsConfig || Object.keys(kjsConfig).length === 0) {
      return {};
    }

    const disallowedKey = checkAllowedKeys('producer', kjsConfig);
    if (disallowedKey) {
      throw new error.KafkaJSError(CompatibilityErrorMessages.unsupportedKey(disallowedKey), { code: error.ErrorCodes.ERR__INVALID_ARG });
    }

    const rdKafkaConfig = kafkaJSToRdKafkaConfig(kjsConfig);

    /* Producer specific configuration. */
    if (Object.hasOwn(kjsConfig, 'createPartitioner')) {
      throw new error.KafkaJSError(CompatibilityErrorMessages.createPartitioner(), { code: error.ErrorCodes.ERR__NOT_IMPLEMENTED });
    }
    rdKafkaConfig['partitioner'] = 'murmur2_random';

    if (Object.hasOwn(kjsConfig, 'metadataMaxAge')) {
      rdKafkaConfig['topic.metadata.refresh.interval.ms'] = kjsConfig.metadataMaxAge;
    }

    if (Object.hasOwn(kjsConfig, 'allowAutoTopicCreation')) {
      rdKafkaConfig['allow.auto.create.topics'] = kjsConfig.allowAutoTopicCreation;
    }

    if (Object.hasOwn(kjsConfig, 'transactionTimeout')) {
      rdKafkaConfig['transaction.timeout.ms'] = kjsConfig.transactionTimeout;
    } else {
      rdKafkaConfig['transaction.timeout.ms'] = 60000;
    }

    // `socket.timeout.ms` must be set <= `transaction.timeout.ms` + 100
    if (rdKafkaConfig['socket.timeout.ms'] > rdKafkaConfig['transaction.timeout.ms'] + 100) {
      rdKafkaConfig['socket.timeout.ms'] = rdKafkaConfig['transaction.timeout.ms'] + 100;
    }

    if (Object.hasOwn(kjsConfig, 'idempotent')) {
      rdKafkaConfig['enable.idempotence'] = kjsConfig.idempotent;
    }

    if (Object.hasOwn(kjsConfig, 'maxInFlightRequests')) {
      rdKafkaConfig['max.in.flight'] = kjsConfig.maxInFlightRequests;
    }

    if (Object.hasOwn(kjsConfig, 'transactionalId')) {
      rdKafkaConfig['transactional.id'] = kjsConfig.transactionalId;
    }

    if (Object.hasOwn(kjsConfig, 'compression')) {
      rdKafkaConfig['compression.codec'] = kjsConfig.compression;
    }

    if (Object.hasOwn(kjsConfig, 'acks')) {
      rdKafkaConfig['acks'] = kjsConfig.acks;
    }

    if (Object.hasOwn(kjsConfig, 'timeout')) {
      rdKafkaConfig['request.timeout.ms'] = kjsConfig.timeout;
    }

    const retry = kjsConfig.retry ?? {};
    const { retries } = retry;
    rdKafkaConfig["retries"] = retries ?? 5;

    /* Set the logger */
    if (Object.hasOwn(kjsConfig, 'logger')) {
      this.#logger = kjsConfig.logger;
    }

    /* Set the log level - INFO for compatibility with kafkaJS, or DEBUG if that is turned
     * on using the logLevel property. rdKafkaConfig.log_level is guaranteed to be set if we're
     * here, and containing the correct value. */
    this.#logger.setLogLevel(severityToLogLevel[rdKafkaConfig.log_level]);

    return rdKafkaConfig;
  }

  #finalizedConfig() {
    /* Creates an rdkafka config based off the kafkaJS block. Switches to compatibility mode if the block exists. */
    let compatibleConfig = this.#kafkaJSToProducerConfig(this.#userConfig.kafkaJS);

    /* There can be multiple different and conflicting config directives for setting the log level:
     * 1. If there's a kafkaJS block:
     *   a. If there's a logLevel directive in the kafkaJS block, set the logger level accordingly.
     *   b. If there's no logLevel directive, set the logger level to INFO.
     *   (both these are already handled in the conversion method above).
     * 2. If there is a log_level or debug directive in the main config, set the logger level accordingly.
     *    !This overrides any different value provided in the kafkaJS block!
     *   a. If there's a log_level directive, set the logger level accordingly.
     *   b. If there's a debug directive, set the logger level to DEBUG regardless of anything else. This is because
     *      librdkafka ignores log_level if debug is set, and our behaviour should be identical.
     * 3. There's nothing at all. Take no action in this case, let the logger use its default log level.
     */
    if (Object.hasOwn(this.#userConfig, 'log_level')) {
      this.#logger.setLogLevel(severityToLogLevel[this.#userConfig.log_level]);
    }

    if (Object.hasOwn(this.#userConfig, 'debug')) {
      this.#logger.setLogLevel(logLevel.DEBUG);
    }

    let rdKafkaConfig = Object.assign(compatibleConfig, this.#userConfig);

    /* Delete properties which are already processed, or cannot be passed to node-rdkafka */
    delete rdKafkaConfig.kafkaJS;

    /* Certain properties that the user has set are overridden. There is
     * no longer a delivery report, rather, results are made available on
     * awaiting. */
    /* TODO: Add a warning if dr_cb is set? Or else, create a trampoline for it. */
    rdKafkaConfig.dr_cb = true;

    return rdKafkaConfig;
  }

  /**
   * Flattens a list of topics with partitions into a list of topic, partition, offset.
   * @param {import("../../types/kafkajs").TopicOffsets[]} topics
   * @returns {import("../../types/kafkajs").TopicPartitionOffset}
   */
  #flattenTopicPartitionOffsets(topics) {
    return topics.flatMap(topic => {
      return topic.partitions.map(partition => {
        return { partition: Number(partition.partition), offset: String(partition.offset), topic: String(topic.topic) };
      });
    });
  }

  #readyTransactions(err) {
    if (err) {
      this.#connectPromiseFunc["reject"](err);
      return;
    }

    if (this.#state !== ProducerState.INITIALIZING_TRANSACTIONS) {
      // FSM impossible state. We should add error handling for
      // this later.
      return;
    }

    this.#state = ProducerState.INITIALIZED_TRANSACTIONS;
    this.#readyCb();
  }

  /**
   * Processes a delivery report, converting it to the type that the promisified API uses.
   * @param {import('../..').LibrdKafkaError} err
   * @param {import('../..').DeliveryReport} report
   */
  #deliveryCallback(err, report) {
    const opaque = report.opaque;
    if (!opaque || (typeof opaque.resolve !== 'function' && typeof opaque.reject !== 'function')) {
      // not sure how to handle this.
      throw new error.KafkaJSError("Internal error: deliveryCallback called without opaque set properly", { code: error.ErrorCodes.ERR__STATE });
    }

    if (err) {
      opaque.reject(createKafkaJsErrorFromLibRdKafkaError(err));
      return;
    }

    delete report['opaque'];

    const recordMetadata = {
      topicName: report.topic,
      partition: report.partition,
      errorCode: 0,
      baseOffset: report.offset,
      logAppendTime: '-1',
      logStartOffset: '0',
    };

    opaque.resolve(recordMetadata);
  }

  async #readyCb() {
    if (this.#state !== ProducerState.CONNECTING && this.#state !== ProducerState.INITIALIZED_TRANSACTIONS) {
      /* The connectPromiseFunc might not be set, so we throw such an error. It's a state error that we can't recover from. Probably a bug. */
      throw new error.KafkaJSError(`Ready callback called in invalid state ${this.#state}`, { code: error.ErrorCodes.ERR__STATE });
    }

    const rdKafkaConfig = this.#config();
    this.#clientName = this.#internalClient.name;

    if (Object.hasOwn(rdKafkaConfig, 'transactional.id') && this.#state !== ProducerState.INITIALIZED_TRANSACTIONS) {
      this.#state = ProducerState.INITIALIZING_TRANSACTIONS;
      this.#logger.debug("Attempting to initialize transactions", this.#createProducerBindingMessageMetadata());
      this.#internalClient.initTransactions(5000 /* default: 5s */, this.#readyTransactions.bind(this));
      return;
    }

    this.#state = ProducerState.CONNECTED;
    this.#internalClient.setPollInBackground(true);
    this.#internalClient.on('delivery-report', this.#deliveryCallback.bind(this));
    this.#logger.info("Producer connected", this.#createProducerBindingMessageMetadata());

    // Resolve the promise.
    this.#connectPromiseFunc["resolve"]();
  }

  /**
   * Callback for the event.error event, either fails the initial connect(), or logs the error.
   * @param {Error} err
   */
  #errorCb(err) {
    if (this.#state === ProducerState.CONNECTING) {
      this.#connectPromiseFunc["reject"](err);
    } else {
      this.#logger.error(err, this.#createProducerBindingMessageMetadata());
    }
  }

  /**
   * Set up the client and connect to the bootstrap brokers.
   * @returns {Promise<void>} Resolves when connection is complete, rejects on error.
   */
  async connect() {
    if (this.#state !== ProducerState.INIT) {
      throw new error.KafkaJSError("Connect has already been called elsewhere.", { code: error.ErrorCodes.ERR__STATE });
    }

    this.#state = ProducerState.CONNECTING;

    const rdKafkaConfig = this.#config();

    this.#internalClient = new RdKafka.Producer(rdKafkaConfig);
    this.#internalClient.on('ready', this.#readyCb.bind(this));
    this.#internalClient.on('event.error', this.#errorCb.bind(this));
    this.#internalClient.on('error', this.#errorCb.bind(this));
    this.#internalClient.on('event.log', (msg) => loggerTrampoline(msg, this.#logger));

    return new Promise((resolve, reject) => {
      this.#connectPromiseFunc = { resolve, reject };
      this.#internalClient.connect(null, (err) => {
        if (err)
          reject(createKafkaJsErrorFromLibRdKafkaError(err));
      });
    });
  }

  /**
   * Disconnect from the brokers, clean-up and tear down the client.
   * @returns {Promise<void>} Resolves when disconnect is complete, rejects on error.
   */
  async disconnect() {
    /* Not yet connected - no error. */
    if (this.#state === ProducerState.INIT) {
      return;
    }

    /* TODO: We should handle a case where we are connecting, we should
     * await the connection and then schedule a disconnect. */

    /* Already disconnecting, or disconnected. */
    if (this.#state >= ProducerState.DISCONNECTING) {
      return;
    }

    this.#state = ProducerState.DISCONNECTING;
    await new Promise((resolve, reject) => {
      const cb = (err) => {
        if (err) {
          reject(createKafkaJsErrorFromLibRdKafkaError(err));
          return;
        }
        this.#state = ProducerState.DISCONNECTED;
        this.#logger.info("Producer disconnected", this.#createProducerBindingMessageMetadata());
        resolve();
      };
      this.#internalClient.disconnect(5000 /* default timeout, 5000ms */, cb);
    });
  }

  /**
   * Start a transaction - can only be used with a transactional producer.
   * @returns {Promise<Producer>} Resolves with the producer when the transaction is started.
   */
  async transaction() {
    if (this.#state !== ProducerState.CONNECTED) {
      throw new error.KafkaJSError("Cannot start transaction without awaiting connect()", { code: error.ErrorCodes.ERR__STATE });
    }

    if (this.#ongoingTransaction) {
      throw new error.KafkaJSError("Can only start one transaction at a time.", { code: error.ErrorCodes.ERR__STATE });
    }

    this.#logger.debug("Attempting to begin transaction", this.#createProducerBindingMessageMetadata());
    return new Promise((resolve, reject) => {
      this.#internalClient.beginTransaction((err) => {
        if (err) {
          reject(createKafkaJsErrorFromLibRdKafkaError(err));
          return;
        }
        this.#ongoingTransaction = true;

        // Resolve with 'this' because we don't need any specific transaction object.
        // Just using the producer works since we can only have one transaction
        // ongoing for one producer.
        resolve(this);
      });
    });
  }

  /**
   * Commit the current transaction.
   * @returns {Promise<void>} Resolves when the transaction is committed.
   */
  async commit() {
    if (this.#state !== ProducerState.CONNECTED) {
      throw new error.KafkaJSError("Cannot commit without awaiting connect()", { code: error.ErrorCodes.ERR__STATE });
    }

    if (!this.#ongoingTransaction) {
      throw new error.KafkaJSError("Cannot commit, no transaction ongoing.", { code: error.ErrorCodes.ERR__STATE });
    }

    this.#logger.debug("Attempting to commit transaction", this.#createProducerBindingMessageMetadata());
    return new Promise((resolve, reject) => {
      this.#internalClient.commitTransaction(5000 /* default: 5000ms */, err => {
        if (err) {
          // TODO: Do we reset ongoingTransaction here?
          reject(createKafkaJsErrorFromLibRdKafkaError(err));
          return;
        }
        this.#ongoingTransaction = false;
        resolve();
      });
    });
  }

  /**
   * Abort the current transaction.
   * @returns {Promise<void>} Resolves when the transaction is aborted.
   */
  async abort() {
    if (this.#state !== ProducerState.CONNECTED) {
      throw new error.KafkaJSError("Cannot abort without awaiting connect()", { code: error.ErrorCodes.ERR__STATE });
    }

    if (!this.#ongoingTransaction) {
      throw new error.KafkaJSError("Cannot abort, no transaction ongoing.", { code: error.ErrorCodes.ERR__STATE });
    }

    this.#logger.debug("Attempting to abort transaction", this.#createProducerBindingMessageMetadata());
    return new Promise((resolve, reject) => {
      this.#internalClient.abortTransaction(5000 /* default: 5000ms */, err => {
        if (err) {
          // TODO: Do we reset ongoingTransaction here?
          reject(createKafkaJsErrorFromLibRdKafkaError(err));
          return;
        }
        this.#ongoingTransaction = false;
        resolve();
      });
    });
  }

  /**
   * Send offsets for the transaction.
   * @param {object} arg - The arguments to sendOffsets
   * @param {Consumer} arg.consumer - The consumer to send offsets for.
   * @param {import("../../types/kafkajs").TopicOffsets[]} arg.topics - The topics, partitions and the offsets to send.
   *
   * @returns {Promise<void>} Resolves when the offsets are sent.
   */
  async sendOffsets(arg) {
    let { consumerGroupId, topics, consumer } = arg;

    /* If the user has not supplied a consumer, or supplied a consumerGroupId, throw immediately. */
    if (consumerGroupId || !consumer) {
      throw new error.KafkaJSError(CompatibilityErrorMessages.sendOffsetsMustProvideConsumer(), { code: error.ErrorCodes.ERR__INVALID_ARG });
    }

    if (!Array.isArray(topics) || topics.length === 0) {
      throw new error.KafkaJSError("sendOffsets arguments are invalid", { code: error.ErrorCodes.ERR__INVALID_ARG });
    }

    if (this.#state !== ProducerState.CONNECTED) {
      throw new error.KafkaJSError("Cannot sendOffsets without awaiting connect()", { code: error.ErrorCodes.ERR__STATE });
    }

    if (!this.#ongoingTransaction) {
      throw new error.KafkaJSError("Cannot sendOffsets, no transaction ongoing.", { code: error.ErrorCodes.ERR__STATE });
    }

    return new Promise((resolve, reject) => {
      this.#internalClient.sendOffsetsToTransaction(
        this.#flattenTopicPartitionOffsets(topics).map(topicPartitionOffsetToRdKafka),
        consumer._getInternalConsumer(),
        async err => {
          if (err)
            reject(createKafkaJsErrorFromLibRdKafkaError(err));
          else
            resolve();
        });
    });
  }

  /**
   * Check if there is an ongoing transaction.
   *
   * NOTE: Since Producer itself represents a transaction, and there is no distinct
   *       type for a transaction, this method exists on the producer.
   * @returns {boolean} true if there is an ongoing transaction, false otherwise.
   */
  isActive() {
    return this.#ongoingTransaction;
  }

  /**
   * Sends a record of messages to a specific topic.
   *
   * @param {import('../../types/kafkajs').ProducerRecord} sendOptions - The record to send. The keys `acks`, `timeout`, and `compression` are not used, and should not be set, rather, they should be set in the global config.
   * @returns {Promise<import("../../types/kafkajs").RecordMetadata[]>} Resolves with the record metadata for the messages.
   */
  async send(sendOptions) {
    if (this.#state !== ProducerState.CONNECTED) {
      throw new error.KafkaJSError("Cannot send without awaiting connect()", { code: error.ErrorCodes.ERR__STATE });
    }

    if (sendOptions === null || !(sendOptions instanceof Object)) {
      throw new error.KafkaJSError(CompatibilityErrorMessages.sendOptionsMandatoryMissing(), { code: error.ErrorCodes.ERR__INVALID_ARG });
    }

    if (Object.hasOwn(sendOptions, 'acks')) {
      throw new error.KafkaJSError(CompatibilityErrorMessages.sendOptionsAcks('send'), { code: error.ErrorCodes.ERR__INVALID_ARG });
    }
    if (Object.hasOwn(sendOptions, 'timeout')) {
      throw new error.KafkaJSError(CompatibilityErrorMessages.sendOptionsTimeout('send'), { code: error.ErrorCodes.ERR__INVALID_ARG });
    }
    if (Object.hasOwn(sendOptions, 'compression')) {
      throw new error.KafkaJSError(CompatibilityErrorMessages.sendOptionsCompression('send'), { code: error.ErrorCodes.ERR__INVALID_ARG });
    }

    const msgPromises = [];
    for (let i = 0; i < sendOptions.messages.length; i++) {
      const msg = sendOptions.messages[i];

      if (!Object.hasOwn(msg, "partition") || msg.partition === null) {
        msg.partition = -1;
      }

      if (typeof msg.value === 'string') {
        msg.value = Buffer.from(msg.value);
      }

      if (Object.hasOwn(msg, "timestamp") && msg.timestamp) {
        msg.timestamp = Number(msg.timestamp);
      } else {
        msg.timestamp = 0;
      }

      msg.headers = convertToRdKafkaHeaders(msg.headers);

      msgPromises.push(new Promise((resolve, reject) => {
        const opaque = { resolve, reject };
        try {
          this.#internalClient.produce(sendOptions.topic, msg.partition, msg.value, msg.key, msg.timestamp, opaque, msg.headers);
        } catch (err) {
          reject(err);
        }
      }));
    }

    /* The delivery report will be handled by the delivery-report event handler, and we can simply wait for it here. */

    const recordMetadataArr = await Promise.all(msgPromises);

    const topicPartitionRecordMetadata = new Map();
    for (const recordMetadata of recordMetadataArr) {
      const key = `${recordMetadata.topicName},${recordMetadata.partition}`;
      if (recordMetadata.baseOffset === null || !topicPartitionRecordMetadata.has(key)) {
        topicPartitionRecordMetadata.set(key, recordMetadata);
        continue;
      }

      const currentRecordMetadata = topicPartitionRecordMetadata.get(key);

      // Don't overwrite a null baseOffset
      if (currentRecordMetadata.baseOffset === null) {
        continue;
      }

      if (currentRecordMetadata.baseOffset > recordMetadata.baseOffset) {
        topicPartitionRecordMetadata.set(key, recordMetadata);
      }
    }

    const ret = [];
    for (const value of topicPartitionRecordMetadata.values()) {
      value.baseOffset = value.baseOffset?.toString();
      ret.push(value);
    }
    return ret;
  }

  /**
    * Sends a record of messages to various topics.
    *
    * NOTE: This method is identical to calling send() repeatedly and waiting on all the return values together.
    * @param {import('../../types/kafkajs').ProducerBatch} sendOptions - The record to send. The keys `acks`, `timeout`, and `compression` are not used, and should not be set, rather, they should be set in the global config.
    * @returns {Promise<import("../../types/kafkajs").RecordMetadata[]>} Resolves with the record metadata for the messages.
  */
  async sendBatch(sendOptions) {
    if (this.#state !== ProducerState.CONNECTED) {
      throw new error.KafkaJSError("Cannot sendBatch without awaiting connect()", { code: error.ErrorCodes.ERR__STATE });
    }

    if (sendOptions === null || !(sendOptions instanceof Object)) {
      throw new error.KafkaJSError(CompatibilityErrorMessages.sendBatchMandatoryMissing(), { code: error.ErrorCodes.ERR__INVALID_ARG });
    }

    if (Object.hasOwn(sendOptions, 'acks')) {
      throw new error.KafkaJSError(CompatibilityErrorMessages.sendOptionsAcks('sendBatch'), { code: error.ErrorCodes.ERR__INVALID_ARG });
    }
    if (Object.hasOwn(sendOptions, 'timeout')) {
      throw new error.KafkaJSError(CompatibilityErrorMessages.sendOptionsTimeout('timeout'), { code: error.ErrorCodes.ERR__INVALID_ARG });
    }
    if (Object.hasOwn(sendOptions, 'compression')) {
      throw new error.KafkaJSError(CompatibilityErrorMessages.sendOptionsCompression('compression'), { code: error.ErrorCodes.ERR__INVALID_ARG });
    }

    if (sendOptions.topicMessages !== null && !Array.isArray(sendOptions.topicMessages)) {
      throw new error.KafkaJSError(CompatibilityErrorMessages.sendBatchMandatoryMissing(), { code: error.ErrorCodes.ERR__INVALID_ARG });
    }

    if (!sendOptions.topicMessages || sendOptions.topicMessages.length === 0) {
      return Promise.resolve([]);
    }

    // Internally, we just use send() because the batching is handled by librdkafka.
    const sentPromises = [];

    for (const topicMessage of sendOptions.topicMessages) {
      sentPromises.push(this.send(topicMessage));
    }

    const records = await Promise.all(sentPromises);
    return records.flat();
  }

  /**
   * @returns {import("../../types/kafkajs").Logger} the logger associated to this producer.
   */
  logger() {
    return this.#logger;
  }

  /**
   * Change SASL credentials to be sent on the next authentication attempt.
   *
   * @param {string} args.username
   * @param {string} args.password
   * @note Only applicable if SASL authentication is being used.
   */
  setSaslCredentials(args = {}) {
    if (!Object.hasOwn(args, 'username')) {
      throw new error.KafkaJSError("username must be set for setSaslCredentials", { code: error.ErrorCodes.ERR__INVALID_ARG });
    }

    if (!Object.hasOwn(args, 'password')) {
      throw new error.KafkaJSError("password must be set for setSaslCredentials", { code: error.ErrorCodes.ERR__INVALID_ARG });
    }

    /**
     * In case we've not started connecting yet, just modify the configuration for
     * the first connection attempt.
     */
    if (this.#state < ProducerState.CONNECTING) {
      this.#userConfig['sasl.username'] = args.username;
      this.#userConfig['sasl.password'] = args.password;
      if (Object.hasOwn(this.#userConfig, 'kafkaJS') && Object.hasOwn(this.#userConfig.kafkaJS, 'sasl')) {
        this.#userConfig.kafkaJS.sasl.username = args.username;
        this.#userConfig.kafkaJS.sasl.password = args.password;
      }
      return;
    }

    this.#logger.info("Setting SASL credentials", this.#createProducerBindingMessageMetadata());
    this.#internalClient.setSaslCredentials(args.username, args.password);
  }

  /**
   * Flushes any pending messages.
   *
   * Messages are batched internally by librdkafka for performance reasons.
   * Continously sent messages are batched upto a timeout, or upto a maximum
   * size. Calling flush sends any pending messages immediately without
   * waiting for this size or timeout.
   *
   * @param {number} args.timeout Time to try flushing for in milliseconds.
   * @returns {Promise<void>} Resolves on successful flush.
   * @throws {KafkaJSTimeout} if the flush times out.
   *
   * @note This is only useful when using asynchronous sends.
   *       For example, the following code does not get any benefit from flushing,
   *       since `await`ing the send waits for the delivery report, and the message
   *       has already been sent by the time we start flushing:
   *         for (let i = 0; i < 100; i++) await send(...);
   *         await flush(...) // Not useful.
   *
   *       However, using the following code may put these 5 messages into a batch
   *       and then the subsequent `flush` will send the batch altogether (as long as
   *       batch size, etc. are conducive to batching):
   *         for (let i = 0; i < 5; i++) send(...);
   *         await flush({timeout: 5000});
   */
  async flush(args = { timeout: 500 }) {
    if (this.#state !== ProducerState.CONNECTED) {
      throw new error.KafkaJSError("Cannot flush without awaiting connect()", { code: error.ErrorCodes.ERR__STATE });
    }

    if (!Object.hasOwn(args, 'timeout')) {
      throw new error.KafkaJSError("timeout must be set for flushing", { code: error.ErrorCodes.ERR__INVALID_ARG });
    }

    this.#logger.debug(`Attempting to flush messages for ${args.timeout}ms`, this.#createProducerBindingMessageMetadata());
    return new Promise((resolve, reject) => {
      this.#internalClient.flush(args.timeout, (err) => {
        if (err) {
          const kjsErr = createKafkaJsErrorFromLibRdKafkaError(err);
          if (err.code === error.ErrorCodes.ERR__TIMED_OUT) {
            /* See reason below for yield. Same here - but for partially processed delivery reports. */
            setTimeout(() => reject(kjsErr), 0);
          } else {
            reject(kjsErr);
          }
          return;
        }
        /* Yielding here allows any 'then's and 'awaits' on associated sends to be scheduled
         * before flush completes, which means that the user doesn't have to yield themselves.
         * It's not necessary that all the 'then's and 'awaits' will be able to run, but
         * it's better than nothing. */
        setTimeout(resolve, 0);
      });
    });
  }
}

module.exports = { Producer, CompressionTypes };
