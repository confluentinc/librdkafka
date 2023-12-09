const LibrdKafkaError = require('../error');
const error = require('./_error');
const RdKafka = require('../rdkafka');
const {
  kafkaJSToRdKafkaConfig,
  topicPartitionOffsetToRdKafka,
  createKafkaJsErrorFromLibRdKafkaError,
  notImplemented
} = require('./_common');
const { Buffer } = require('buffer');

const ConsumerState = Object.freeze({
  INIT: 0,
  CONNECTING: 1,
  CONNECTED: 2,
  DISCONNECTING: 3,
  DISCONNECTED: 4,
});

const PartitionAssigners = Object.freeze({
  roundRobin: 'roundrobin',
  range: 'range',
  cooperativeSticky: 'cooperative-sticky',
});

class Consumer {
  /**
   * kJSConfig is the kafkaJS config object.
   * @type {import("../../types/kafkajs").ConsumerConfig|null}
   */
  #kJSConfig = null;

  /**
   * rdKafkaConfig contains the config objects that will be passed to node-rdkafka.
   * @type {{globalConfig: import("../../types/config").ConsumerGlobalConfig, topicConfig: import("../../types/config").ConsumerTopicConfig}|null}
   */
  #rdKafkaConfig = null;

  /**
   * internalClient is the node-rdkafka client used by the API.
   * @type {import("../rdkafka").Consumer|null}
   */
  #internalClient = null;

  /**
   * connectPromiseFunc is the set of promise functions used to resolve/reject the connect() promise.
   * @type {{resolve: Function, reject: Function}|{}}
   */
  #connectPromiseFunc = {};

  /**
   * state is the current state of the consumer.
   * @type {ConsumerState}
   */
  #state = ConsumerState.INIT;

  /**
   * @constructor
   * @param {import("../../types/kafkajs").ConsumerConfig} kJSConfig
   */
  constructor(kJSConfig) {
    this.#kJSConfig = kJSConfig;
  }

  async #config() {
    if (!this.#rdKafkaConfig)
      this.#rdKafkaConfig = await this.#finalizedConfig();
    return this.#rdKafkaConfig;
  }

  /**
   * Used as a trampoline to the user's rebalance listener, if any.
   * @param {Error} err - error in rebalance
   * @param {import("../../types").TopicPartition[]} assignment
   */
  #rebalanceCallback(err, assignment) {
    // Create the librdkafka error
    err = LibrdKafkaError.create(err);

    let call;
    switch (err.code) {
      // TODO: is this the right way to handle this error?
      // We might just be able to throw, because the error is something the user has caused.
      case LibrdKafkaError.codes.ERR__ASSIGN_PARTITIONS:
        call = (this.#kJSConfig.rebalanceListener.onPartitionsAssigned ?
          this.#kJSConfig.rebalanceListener.onPartitionsAssigned(assignment) :
          Promise.resolve()).catch(console.error);
        break;
      case LibrdKafkaError.codes.ERR__REVOKE_PARTITIONS:
        call = (this.#kJSConfig.rebalanceListener.onPartitionsRevoked ?
          this.#kJSConfig.rebalanceListener.onPartitionsRevoked(assignment) :
          Promise.resolve()).catch(console.error);
        break;
      default:
        call = Promise.reject(`Unexpected rebalanceListener error code ${err.code}`).catch((e) => {
          console.error(e);
        });
        break;
    }

    call
      .finally(() => {
        // Emit the event
        this.#internalClient.emit('rebalance', err, assignment);

        try {
          if (err.code === LibrdKafkaError.codes.ERR__ASSIGN_PARTITIONS) {
            this.#internalClient.assign(assignment);
          } else {
            this.#internalClient.unassign();
          }
        } catch (e) {
          // Ignore exceptions if we are not connected
          if (this.#internalClient.isConnected()) {
            this.#internalClient.emit('rebalance.error', e);
          }
        }
      });
  }

  async #finalizedConfig() {
    /* This sets the common configuration options for the client. */
    const { globalConfig, topicConfig } = await kafkaJSToRdKafkaConfig(this.#kJSConfig);

    /* Consumer specific configuration */

    if (Object.hasOwn(this.#kJSConfig, 'groupId')) {
      globalConfig['group.id'] = this.#kJSConfig.groupId;
    }

    if (Object.hasOwn(this.#kJSConfig, 'partitionAssigners')) {
      this.#kJSConfig.partitionAssignors = this.#kJSConfig.partitionAssigners;
    }

    if (Object.hasOwn(this.#kJSConfig, 'partitionAssignors')) {
      this.#kJSConfig.partitionAssignors.forEach(assignor => {
        if (typeof assignor !== 'string')
          throw new error.KafkaJSError('partitionAssignors must be a list of strings from within `PartitionAssignors`', { code: error.ErrorCodes.ERR__INVALID_ARG });
      });

      globalConfig['partition.assignment.strategy'] = this.#kJSConfig.partitionAssignors.join(',');
    }

    if (Object.hasOwn(this.#kJSConfig, 'sessionTimeout')) {
      globalConfig['session.timeout.ms'] = this.#kJSConfig.sessionTimeout;
    }

    if (Object.hasOwn(this.#kJSConfig, 'rebalanceTimeout')) {
      /* In librdkafka, we use the max poll interval as the rebalance timeout as well. */
      globalConfig['max.poll.interval.ms'] = this.#kJSConfig.rebalanceTimeout;
    }

    if (Object.hasOwn(this.#kJSConfig, 'heartbeatInterval')) {
      globalConfig['heartbeat.interval.ms'] = this.#kJSConfig.heartbeatInterval;
    }

    if (Object.hasOwn(this.#kJSConfig, 'metadataMaxAge')) {
      globalConfig['topic.metadata.refresh.interval.ms'] = this.#kJSConfig.metadataMaxAge;
    }

    if (Object.hasOwn(this.#kJSConfig, 'allowAutoTopicCreation')) {
      globalConfig['allow.auto.create.topics'] = this.#kJSConfig.allowAutoTopicCreation;
    }

    if (Object.hasOwn(this.#kJSConfig, 'maxBytesPerPartition')) {
      globalConfig['max.partition.fetch.bytes'] = this.#kJSConfig.maxBytesPerPartition;
    }

    if (Object.hasOwn(this.#kJSConfig, 'maxWaitTimeInMs')) {
      globalConfig['fetch.wait.max.ms'] = this.#kJSConfig.maxWaitTimeInMs;
    }

    if (Object.hasOwn(this.#kJSConfig, 'minBytes')) {
      globalConfig['fetch.min.bytes'] = this.#kJSConfig.minBytes;
    }

    if (Object.hasOwn(this.#kJSConfig, 'maxBytes')) {
      globalConfig['fetch.message.max.bytes'] = this.#kJSConfig.maxBytes;
    }

    if (Object.hasOwn(this.#kJSConfig, 'readUncommitted')) {
      globalConfig['isolation.level'] = this.#kJSConfig.readUncommitted ? 'read_uncommitted' : 'read_committed';
    }

    if (Object.hasOwn(this.#kJSConfig, 'maxInFlightRequests')) {
      globalConfig['max.in.flight'] = this.#kJSConfig.maxInFlightRequests;
    }

    if (Object.hasOwn(this.#kJSConfig, 'rackId')) {
      globalConfig['client.rack'] = this.#kJSConfig.rackId;
    }

    globalConfig['offset_commit_cb'] = true;
    if (this.#kJSConfig.rebalanceListener) {
      globalConfig['rebalance_cb'] = this.#rebalanceCallback.bind(this);
    }
    return { globalConfig, topicConfig };
  }

  #readyCb() {
    if (this.#state !== ConsumerState.CONNECTING) {
      /* The connectPromiseFunc might not be set, so we throw such an error. It's a state error that we can't recover from. Probably a bug. */
      throw new error.KafkaJSError(`Ready callback called in invalid state ${this.#state}`, { code: error.ErrorCodes.ERR__STATE });
    }
    this.#state = ConsumerState.CONNECTED;

    // Resolve the promise.
    this.#connectPromiseFunc['resolve']();
  }

  /**
   * Callback for the event.error event, either fails the initial connect(), or logs the error.
   * @param {Error} err
   */
  #errorCb(err) {
    if (this.#state === ConsumerState.CONNECTING) {
      this.#connectPromiseFunc['reject'](err);
    } else {
      /* TODO: we should log the error returned here, depending on the log level.
       * Right now, we're just using console.err, but we should allow for a custom
       * logger, or at least make a function in _common.js that handles consumer
       * and producer. */
      console.error(err);
    }
  }

  /**
   * Converts a message returned by node-rdkafka into a message that can be used by the eachMessage callback.
   * @param {import("../..").Message} message
   * @returns {import("../../types/kafkajs").EachMessagePayload}
   */
  #createPayload(message) {
    let key = message.key;
    if (typeof key === 'string') {
      key = Buffer.from(key);
    }

    let timestamp = message.timestamp ? String(message.timestamp) : '';

    let headers;
    if (message.headers) {
      headers = {}
      for (const [key, value] of Object.entries(message.headers)) {
        if (!Object.hasOwn(headers, key)) {
          headers[key] = value;
        } else if (headers[key].constructor === Array) {
          headers[key].push(value);
        } else {
          headers[key] = [headers[key], value];
        }
      }
    }

    return {
      topic: message.topic,
      partition: message.partition,
      message: {
        key,
        value: message.value,
        timestamp,
        attributes: 0,
        offset: String(message.offset),
        size: message.size,
        headers
      },
      heartbeat: async () => { /* no op */ },
      pause: () => this.pause([{ topic: message.topic, partitions: [message.partition] }]),
    };
  }

  /**
   * Consumes a single message from the internal consumer.
   * @returns {Promise<import("../..").Message>} a promise that resolves to a single message.
   */
  async #consumeSingle() {
    return new Promise((resolve, reject) => {
      this.#internalClient.consume(1, function (err, messages) {
        if (err) {
          reject(createKafkaJsErrorFromLibRdKafkaError(err));
          return;
        }
        const message = messages[0];
        resolve(message);
      });
    });
  }

  /**
   * Flattens a list of topics with partitions into a list of topic, partition.
   * @param {({topic: string, partitions: number[]}|{topic: string, partition: number})[]} topics
   * @returns {import("../../types").TopicPartition[]} a list of (topic, partition).
   */
  #flattenTopicPartitions(topics) {
    const ret = [];
    for (const topic of topics) {
      if (typeof topic.partition === 'number')
        ret.push({
          topic: topic.topic,
          partition: topic.partition
        });
      else {
        for (const partition of topic.partitions) {
          ret.push({ topic: topic.topic, partition });
        }
      }
    }
    return ret;
  }

  /**
   * @returns {import("../rdkafka").Consumer} the internal node-rdkafka client.
   */
  _getInternalConsumer() {
    return this.#internalClient;
  }

  /**
   * Set up the client and connect to the bootstrap brokers.
   * @returns {Promise<void>} a promise that resolves when the consumer is connected.
   */
  async connect() {
    if (this.#state !== ConsumerState.INIT) {
      throw new error.KafkaJSError('Connect has already been called elsewhere.', { code: error.ErrorCodes.ERR__STATE });
    }

    const { globalConfig, topicConfig } = await this.#config();

    this.#state = ConsumerState.CONNECTING;
    this.#internalClient = new RdKafka.KafkaConsumer(globalConfig, topicConfig);
    this.#internalClient.on('ready', this.#readyCb.bind(this));
    this.#internalClient.on('event.error', this.#errorCb.bind(this));
    this.#internalClient.on('event.log', console.log);

    return new Promise((resolve, reject) => {
      this.#connectPromiseFunc = { resolve, reject };
      this.#internalClient.connect(null, (err) => {
        if (err)
          reject(createKafkaJsErrorFromLibRdKafkaError(err));
      });
    });
  }

  /**
   * Subscribes the consumer to the given topics.
   * @param {import("../../types/kafkajs").ConsumerSubscribeTopics | import("../../types/kafkajs").ConsumerSubscribeTopic} subscription
   */
  async subscribe(subscription) {
    if (this.#state !== ConsumerState.CONNECTED) {
      throw new error.KafkaJSError('Subscribe can only be called while connected.', { code: error.ErrorCodes.ERR__STATE });
    }

    if (typeof subscription.fromBeginning === 'boolean') {
      throw new error.KafkaJSError(
        'fromBeginning is not supported by subscribe(), but must be passed as an rdKafka property to the consumer.',
        { code: error.ErrorCodes.ERR__NOT_IMPLEMENTED });
    }

    if (!Object.hasOwn(subscription, 'topics') && !Object.hasOwn(subscription, 'topic')) {
      throw new error.KafkaJSError('Either topics or topic must be specified.', { code: error.ErrorCodes.ERR__INVALID_ARG });
    }

    let topics = [];
    if (subscription.topic) {
      topics.push(subscription.topic);
    } else if (Array.isArray(subscription.topics)) {
      topics = subscription.topics;
    } else {
      throw new error.KafkaJSError('topics must be an object of the type ConsumerSubscribeTopics.', { code: error.ErrorCodes.ERR__INVALID_ARG });
    }

    topics = topics.map(topic => {
      if (typeof topic === 'string') {
        return topic;
      } else if (topic instanceof RegExp) {
        // Flags are not supported, and librdkafka only considers a regex match if the first character of the regex is ^.
        const regexSource = topic.source;
        if (regexSource.charAt(0) !== '^')
          return '^' + regexSource;
        else
          return regexSource;
      } else {
        throw new error.KafkaJSError('Invalid topic ' + topic + ' (' + typeof topic + '), the topic name has to be a String or a RegExp', { code: error.ErrorCodes.ERR__INVALID_ARG });
      }
    });

    this.#internalClient.subscribe(topics);
  }

  async stop() {
    notImplemented();
  }

  /**
   * Starts consumer polling.
   * @param {import("../../types/kafkajs").ConsumerRunConfig} config
   */
  async run(config) {
    if (this.#state !== ConsumerState.CONNECTED) {
      throw new error.KafkaJSError('Run must be called after a successful connect().', { code: error.ErrorCodes.ERR__STATE });
    }

    if (typeof config.autoCommit == 'boolean' || typeof config.autoCommitInterval == 'number' || typeof config.autoCommitThreshold == 'number') {
      throw new error.KafkaJSError(
        'autoCommit related properties are not supported by run(), but must be passed as rdKafka properties to the consumer.',
        { code: error.ErrorCodes.ERR__NOT_IMPLEMENTED });
    }

    while (this.#state === ConsumerState.CONNECTED) {
      const m = await this.#consumeSingle();
      if (m) {
        /* TODO: add partitionsConsumedConcurrently-based concurrency here.
         * If we maintain a map of topic partitions to promises, and a counter,
         * we can probably achieve it with the correct guarantees of ordering
         * though to maximize performance, we need to consume only from partitions for which
         * an eachMessage call is not already going.
         * It's risky to consume, and then store the message in something like an
         * array/list until it can be processed, because librdkafka marks it as
         * 'stored'... but anyway - we can implement something like this.
         */
        await config.eachMessage(
          this.#createPayload(m)
        )
        /* TODO: another check we need to do here is to see how kafkaJS is handling
         * commits. Are they commmitting after a message is _processed_?
         * In that case we need to turn off librdkafka's auto-commit, and commit
         * inside this function.
         */
      }
    }
  }

  /**
   * Commit offsets for the given topic partitions. If topic partitions are not specified, commits all offsets.
   * @param {import("../../types/kafkajs").TopicPartitionOffsetAndMetadata[]?} topicPartitions
   * @returns {Promise<void>} a promise that resolves when the offsets have been committed.
   */
  async commitOffsets(topicPartitions = null) {
    if (this.#state !== ConsumerState.CONNECTED) {
      throw new error.KafkaJSError('Commit can only be called while connected.', { code: error.ErrorCodes.ERR__STATE });
    }

    try {
      if (topicPartitions === null) {
        this.#internalClient.commitSync();
      } else {
        const topicPartitions = topicPartitions.map(
          topicPartitionOffsetToRdKafka);
        this.#internalClient.commitSync(topicPartitions);
      }
    } catch (e) {
      if (!e.code || e.code !== error.ErrorCodes.ERR__NO_OFFSET) {
        throw createKafkaJsErrorFromLibRdKafkaError(e);
      }
    }
  }

  /**
   * Seek to the given offset for the topic partition.
   * @param {import("../../types/kafkajs").TopicPartitionOffset} topicPartitionOffset
   * @returns {Promise<void>} a promise that resolves when the consumer has seeked.
   */
  seek(topicPartitionOffset) {
    if (this.#state !== ConsumerState.CONNECTED) {
      throw new error.KafkaJSError('Seek can only be called while connected.', { code: error.ErrorCodes.ERR__STATE });
    }

    return new Promise((resolve, reject) => {
      const rdKafkaTopicPartitionOffset =
        topicPartitionOffsetToRdKafka(topicPartitionOffset);
      this.#internalClient.seek(rdKafkaTopicPartitionOffset, 0, (err) => {
        if (err) {
          reject(createKafkaJsErrorFromLibRdKafkaError(err));
        } else {
          resolve();
        }
      });
    });
  }

  async describeGroup() {
    notImplemented();
  }

  /**
   * Find the assigned topic partitions for the consumer.
   * @returns {import("../../types").TopicPartition[]} the current assignment.
   */
  assignment() {
    if (this.#state !== ConsumerState.CONNECTED) {
      throw new error.KafkaJSError('Assignment can only be called while connected.', { code: error.ErrorCodes.ERR__STATE });
    }

    return this.#flattenTopicPartitions(this.#internalClient.assignments());
  }

  /**
   * Fetches all partitions of topic that are assigned to this consumer.
   * @param {string} topic
   * @returns {number[]} a list of partitions.
   */
  #getAllAssignedPartition(topic) {
    return this.#internalClient.assignments()
      .filter((partition) => partition.topic === topic)
      .map((tpo) => tpo.partition);
  }

  /**
   * Pauses the given topic partitions. If partitions are not specified, pauses
   * all partitions for the given topic. If topic partition(s) are already paused
   * this method has no effect.
   * @param {{topic: string, partitions?: number[]}[]} topics
   */
  pause(topics) {
    if (this.#state !== ConsumerState.CONNECTED) {
      throw new error.KafkaJSError('Pause can only be called while connected.', { code: error.ErrorCodes.ERR__STATE });
    }

    for (let topic of topics) {
      if (!topic.partitions) {
        topic.partitions = this.#getAllAssignedPartition(topic.topic);
      }
    }

    topics = this.#flattenTopicPartitions(topics);
    if (topics.length === 0) {
      return;
    }

    this.#internalClient.pause(topics);
  }

  paused() {
    notImplemented();
  }


  /**
   * Resumes the given topic partitions. If partitions are not specified, resumes
   * all partitions for the given topic. If topic partition(s) are already resumed
   * this method has no effect.
   * @param {{topic: string, partitions?: number[]}[]} topics
   */
  resume(topics) {
    if (this.#state !== ConsumerState.CONNECTED) {
      throw new error.KafkaJSError('Resume can only be called while connected.', { code: error.ErrorCodes.ERR__STATE });
    }

    for (let topic of topics) {
      if (!topic.partitions) {
        topic.partitions = this.#getAllAssignedPartition(topic.topic);
      }
    }

    topics = this.#flattenTopicPartitions(topics);
    this.#internalClient.resume(topics);
  }

  on(/* eventName, listener */) {
    notImplemented();
  }

  logger() {
    notImplemented();
  }

  get events() {
    notImplemented();
    return null;
  }

  /**
   * Disconnects and cleans up the consumer.
   * @returns {Promise<void>} a promise that resolves when the consumer has disconnected.
   */
  async disconnect() {
    /* Not yet connected - no error. */
    if (this.#state == ConsumerState.INIT) {
      return;
    }

    /* TODO: We should handle a case where we are connecting, we should
     * await the connection and then schedule a disconnect. */

    /* Already disconnecting, or disconnected. */
    if (this.#state >= ConsumerState.DISCONNECTING) {
      return;
    }
    if (this.#state >= ConsumerState.DISCONNECTING) {
      return;
    }

    this.#state = ConsumerState.DISCONNECTING;
    await new Promise((resolve, reject) => {
      const cb = (err) => {
        if (err) {
          reject(createKafkaJsErrorFromLibRdKafkaError(err));
          return;
        }
        this.#state = ConsumerState.DISCONNECTED;
        resolve();
      }
      this.#internalClient.disconnect(cb);
    });
  }
}

module.exports = { Consumer, PartitionAssigners, }
