const RdKafka = require('../rdkafka');
const { kafkaJSToRdKafkaConfig,
  topicPartitionOffsetToRdKafka,
  createKafkaJsErrorFromLibRdKafkaError,
  convertToRdKafkaHeaders,
  DefaultLogger,
  loggerTrampoline,
  logLevel, } = require('./_common');
const { Consumer } = require('./_consumer');
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

class Producer {
  /**
   * kJSConfig is the kafkaJS config object.
   * @type {import("../../types/kafkajs").ProducerConfig|null}
   */
  #kJSConfig = null;

  /**
   * rdKafkaConfig contains the config objects that will be passed to node-rdkafka.
   * @type {{globalConfig: import("../../types/config").ProducerGlobalConfig, topicConfig: import("../../types/config").ProducerTopicConfig}|null}
   */
  #rdKafkaConfig = null;

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
    this.#kJSConfig = kJSConfig;
  }

  async #config() {
    if (!this.#rdKafkaConfig)
      this.#rdKafkaConfig = await this.#finalizedConfig();
    return this.#rdKafkaConfig;
  }

  async #finalizedConfig() {
    /* This sets the common configuration options for the client. */
    const { globalConfig, topicConfig } = await kafkaJSToRdKafkaConfig(this.#kJSConfig);

    /* Set the logger */
    if (Object.hasOwn(this.#kJSConfig, 'logger')) {
      this.#logger = this.#kJSConfig.logger;
    }
    this.#logger.setLogLevel(this.#kJSConfig.logLevel || logLevel.INFO);

    /* Create producer specific configuration. */
    globalConfig.dr_cb = 'true';

    if (Object.hasOwn(this.#kJSConfig, 'metadataMaxAge')) {
      globalConfig['topic.metadata.refresh.interval.ms'] = this.#kJSConfig.metadataMaxAge;
    }

    if (Object.hasOwn(this.#kJSConfig, 'allowAutoTopicCreation')) {
      globalConfig['allow.auto.create.topics'] = this.#kJSConfig.allowAutoTopicCreation;
    }

    if (Object.hasOwn(this.#kJSConfig, 'transactionTimeout')) {
      globalConfig['transaction.timeout.ms'] = this.#kJSConfig.transactionTimeout;
    }

    if (Object.hasOwn(this.#kJSConfig, 'idempotent')) {
      globalConfig['enable.idempotence'] = this.#kJSConfig.idempotent;
    }

    if (Object.hasOwn(this.#kJSConfig, 'maxInFlightRequests')) {
      globalConfig['max.in.flight'] = this.#kJSConfig.maxInFlightRequests;
    }

    if (Object.hasOwn(this.#kJSConfig, 'transactionalId')) {
      globalConfig['transactional.id'] = this.#kJSConfig.transactionalId;
    }

    if (Object.hasOwn(this.#kJSConfig, 'createPartitioner')) {
      throw new error.KafkaJSError("createPartitioner is not supported", { code: error.ErrorCodes.ERR__NOT_IMPLEMENTED });
    }

    return { globalConfig, topicConfig };
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
      })
    })
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

    const { globalConfig } = await this.#config();
    if (Object.hasOwn(globalConfig, 'transactional.id') && this.#state !== ProducerState.INITIALIZED_TRANSACTIONS) {
      this.#state = ProducerState.INITIALIZING_TRANSACTIONS;
      this.#internalClient.initTransactions(5000 /* default: 5s */, this.#readyTransactions.bind(this));
      return;
    }

    this.#state = ProducerState.CONNECTED;

    /* Start a loop to poll. the queues. */
    const pollInterval = setInterval(() => {
      if (this.#state >= ProducerState.DISCONNECTING) {
        clearInterval(pollInterval);
        return;
      }
      this.#internalClient.poll();
    }, 500);

    this.#internalClient.on('delivery-report', this.#deliveryCallback.bind(this));

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
      this.#logger.error(err);
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

    const { globalConfig, topicConfig } = await this.#config();
    this.#internalClient = new RdKafka.Producer(globalConfig, topicConfig);
    this.#internalClient.on('ready', this.#readyCb.bind(this));
    this.#internalClient.on('event.error', this.#errorCb.bind(this));
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
    if (this.#state == ProducerState.INIT) {
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
        resolve();
      }
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
   * @param {string} arg.consumerGroupId - The consumer group id to send offsets for.
   * @param {Consumer} arg.consumer - The consumer to send offsets for.
   * @param {import("../../types/kafkajs").TopicOffsets[]} arg.topics - The topics, partitions and the offsets to send.
   *
   * @note only one of consumerGroupId or consumer must be set. It is recommended to use `consumer`.
   * @returns {Promise<void>} Resolves when the offsets are sent.
   */
  async sendOffsets(arg) {
    let { consumerGroupId, topics, consumer } = arg;

    if ((!consumerGroupId && !consumer) || !Array.isArray(topics) || topics.length === 0) {
      throw new error.KafkaJSError("sendOffsets arguments are invalid", { code: error.ErrorCodes.ERR__INVALID_ARG });
    }

    if (this.#state !== ProducerState.CONNECTED) {
      throw new error.KafkaJSError("Cannot sendOffsets without awaiting connect()", { code: error.ErrorCodes.ERR__STATE });
    }

    if (!this.#ongoingTransaction) {
      throw new error.KafkaJSError("Cannot sendOffsets, no transaction ongoing.", { code: error.ErrorCodes.ERR__STATE });
    }

    // If we don't have a consumer, we must create a consumer at this point internally.
    // This isn't exactly efficient, but we expect people to use either a consumer,
    // or we will need to change the C/C++ code to facilitate using the consumerGroupId
    // directly.
    // TODO: Change the C/C++ code to facilitate this if we go to release with this.

    let consumerCreated = false;
    if (!consumer) {
      const config = Object.assign({ groupId: consumerGroupId }, this.#kJSConfig);
      consumer = new Consumer(config);
      consumerCreated = true;
      await consumer.connect();
    }

    return new Promise((resolve, reject) => {
      this.#internalClient.sendOffsetsToTransaction(
        this.#flattenTopicPartitionOffsets(topics).map(topicPartitionOffsetToRdKafka),
        consumer._getInternalConsumer(),
        async err => {
          if (consumerCreated)
            await consumer.disconnect();
          if (err)
            reject(createKafkaJsErrorFromLibRdKafkaError(err));
          else
            resolve();
        })
    });
  }

  /**
   *   send(record: ProducerRecord): Promise<RecordMetadata[]>

   * @param {import('../../types/kafkajs').ProducerRecord} sendOptions - The record to send. The keys `acks`, `timeout`, and `compression` are not used, and should not be set, rather, they should be set in the global config.
   * @returns {Promise<import("../../types/kafkajs").RecordMetadata[]>} Resolves with the record metadata for the messages.
   */
  async send(sendOptions) {
    if (this.#state !== ProducerState.CONNECTED) {
      throw new error.KafkaJSError("Cannot send without awaiting connect()", { code: error.ErrorCodes.ERR__STATE });
    }

    if (sendOptions === null || !(sendOptions instanceof Object)) {
      throw new error.KafkaJSError("sendOptions must be set correctly", { code: error.ErrorCodes.ERR__INVALID_ARG });
    }

    // Ignore all properties except topic and messages.
    // TODO: log a warning instead of ignoring.
    if (!Object.hasOwn(sendOptions, "topic") || !Object.hasOwn(sendOptions, "messages") || !Array.isArray(sendOptions["messages"])) {
      // TODO: add further validations.
      throw new error.KafkaJSError("sendOptions must be of the form {topic: string, messages: Message[]}", { code: error.ErrorCodes.ERR__INVALID_ARG });
    }

    if (Object.hasOwn(sendOptions, "acks") || Object.hasOwn(sendOptions, "timeout") || Object.hasOwn(sendOptions, "compression")) {
      throw new error.KafkaJSError("sendOptions must not contain acks, timeout, or compression", { code: error.ErrorCodes.ERR__INVALID_ARG });
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
      if (recordMetadata.baseOffset == null || !topicPartitionRecordMetadata.has(key)) {
        topicPartitionRecordMetadata.set(key, recordMetadata);
        continue;
      }

      const currentRecordMetadata = topicPartitionRecordMetadata.get(key);

      // Don't overwrite a null baseOffset
      if (currentRecordMetadata.baseOffset == null) {
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
    * sendBatch(batch: ProducerBatch): Promise<RecordMetadata[]>
    * @param {import('../../types/kafkajs').ProducerBatch} sendOptions - The record to send. The keys `acks`, `timeout`, and `compression` are not used, and should not be set, rather, they should be set in the global config.
    * @returns {Promise<import("../../types/kafkajs").RecordMetadata[]>} Resolves with the record metadata for the messages.
  */
  async sendBatch(sendOptions) {
    if (this.#state !== ProducerState.CONNECTED) {
      throw new error.KafkaJSError("Cannot send without awaiting connect()", { code: error.ErrorCodes.ERR__STATE });
    }

    if (sendOptions === null || !(sendOptions instanceof Object)) {
      throw new error.KafkaJSError("sendOptions must be set correctly", { code: error.ErrorCodes.ERR__INVALID_ARG });
    }

    // Ignore all properties except topic and messages.
    if (Object.hasOwn(sendOptions, "acks") || Object.hasOwn(sendOptions, "timeout") || Object.hasOwn(sendOptions, "compression")) {
      throw new error.KafkaJSError("sendOptions must not contain acks, timeout, or compression", { code: error.ErrorCodes.ERR__INVALID_ARG });
    }

    if (sendOptions.topicMessages !== null && !Array.isArray(sendOptions.topicMessages)) {
      throw new error.KafkaJSError("sendOptions.topicMessages must be an array if set", { code: error.ErrorCodes.ERR__INVALID_ARG });
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
}

module.exports = { Producer }
