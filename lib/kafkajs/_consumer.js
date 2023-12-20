const LibrdKafkaError = require('../error');
const error = require('./_error');
const RdKafka = require('../rdkafka');
const {
  kafkaJSToRdKafkaConfig,
  topicPartitionOffsetToRdKafka,
  createKafkaJsErrorFromLibRdKafkaError,
  notImplemented,
  loggerTrampoline,
  DefaultLogger,
  CompatibilityErrorMessages,
  severityToLogLevel,
  checkAllowedKeys,
  logLevel
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
   * The config supplied by the user.
   * @type {import("../../types/kafkajs").ConsumerConstructorConfig|null}
   */
  #userConfig = null;

  /**
   * The config realized after processing any compatibility options.
   * @type {import("../../types/config").ConsumerGlobalConfig|null}
   */
  #internalConfig = null;

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
   * Denotes if there are any new pending seeks we need to check.
   * @type {boolean}
   */
  #checkPendingSeeks = false;

  /**
   * Contains a mapping of topic+partition to an offset that the user wants to seek to.
   * The keys are of the type "<topic>|<partition>".
   * @type {Map<string, number>}
   */
  #pendingSeeks = new Map();

  /**
   * Stores the list of paused partitions, as a set of JSON.stringify'd TopicPartition objects.
   * @type {Set<string>}
   */
  #pausedPartitions = new Set();

  /**
   * Contains a list of stored topics/regexes that the user has subscribed to.
   * @type {(string|RegExp)[]}
   */
  #storedSubscriptions = [];

  /**
   * A logger for the consumer.
   * @type {import("../../types/kafkajs").Logger}
   */
  #logger = new DefaultLogger();

  /**
   * @constructor
   * @param {import("../../types/kafkajs").ConsumerConfig} kJSConfig
   */
  constructor(kJSConfig) {
    this.#userConfig = kJSConfig;
  }

  #config() {
    if (!this.#internalConfig)
      this.#internalConfig = this.#finalizedConfig();
    return this.#internalConfig;
  }

  /**
   * Used as a trampoline to the user's rebalance listener, if any.
   * @param {Error} err - error in rebalance
   * @param {import("../../types").TopicPartition[]} assignment
   */
  #rebalanceCallback(err, assignment) {
    // Create the librdkafka error
    err = LibrdKafkaError.create(err);
    const userSpecifiedRebalanceCb = this.#userConfig['rebalance_cb'];

    let call;
    if (typeof userSpecifiedRebalanceCb === 'function') {
      call = new Promise((resolve, reject) => setImmediate(() => {
        try {
          userSpecifiedRebalanceCb(err, assignment);
          resolve();
        } catch (e) {
          reject(e);
        }
      }));
    } else {
      switch (err.code) {
        // TODO: is this the right way to handle this error?
        // We might just be able to throw, because the error is something the user has caused.
        case LibrdKafkaError.codes.ERR__ASSIGN_PARTITIONS:
          call = (this.#userConfig.rebalanceListener.onPartitionsAssigned ?
            this.#userConfig.rebalanceListener.onPartitionsAssigned(assignment) :
            Promise.resolve()).catch(this.#logger.error);
          break;
        case LibrdKafkaError.codes.ERR__REVOKE_PARTITIONS:
          call = (this.#userConfig.rebalanceListener.onPartitionsRevoked ?
            this.#userConfig.rebalanceListener.onPartitionsRevoked(assignment) :
            Promise.resolve()).catch(this.#logger.error);
          break;
        default:
          call = Promise.reject(`Unexpected rebalanceListener error code ${err.code}`).catch((e) => {
            this.#logger.error(e);
          });
          break;
      }
    }

    call
      .finally(async () => {
        // Emit the event
        this.#internalClient.emit('rebalance', err, assignment);

        try {
          if (err.code === LibrdKafkaError.codes.ERR__ASSIGN_PARTITIONS) {

            if (this.#checkPendingSeeks)
              assignment = this.#assignAsPerSeekedOffsets(assignment);

            this.#internalClient.assign(assignment);

            if (this.#checkPendingSeeks) {
              const offsetsToCommit = assignment
                .filter((topicPartition) => topicPartition.offset !== undefined)
                .map((topicPartition) => ({
                  topic: topicPartition.topic,
                  partition: topicPartition.partition,
                  offset: String(topicPartition.offset),
                }));

              if (offsetsToCommit.length !== 0 && this.#internalConfig['enable.auto.commit']) {
                await this.#commitOffsetsUntilNoStateErr(offsetsToCommit);
              }
            }

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

  #kafkaJSToConsumerConfig(kjsConfig) {
    if (!kjsConfig || Object.keys(kjsConfig).length === 0) {
      return {};
    }

    const allowedKeysSpecificToConsumer = [
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
    ];

    checkAllowedKeys(allowedKeysSpecificToConsumer, kjsConfig);
    const rdKafkaConfig = kafkaJSToRdKafkaConfig(kjsConfig);

    /* Consumer specific configuration */
    if (Object.hasOwn(kjsConfig, 'groupId')) {
      rdKafkaConfig['group.id'] = kjsConfig.groupId;
    }

    if (Object.hasOwn(kjsConfig, 'partitionAssigners')) {
      kjsConfig.partitionAssignors = kjsConfig.partitionAssigners;
    }

    if (Object.hasOwn(kjsConfig, 'partitionAssignors')) {
      if (!Array.isArray(kjsConfig.partitionAssignors)) {
        throw new error.KafkaJSError(CompatibilityErrorMessages.partitionAssignors(), { code: error.ErrorCodes.ERR__INVALID_ARG });
      }

      kjsConfig.partitionAssignors.forEach(assignor => {
        if (typeof assignor !== 'string')
          throw new error.KafkaJSError(CompatibilityErrorMessages.partitionAssignors(), { code: error.ErrorCodes.ERR__INVALID_ARG });
        if (assignor === PartitionAssigners.cooperativeSticky)
          throw new error.KafkaJSError(CompatibilityErrorMessages.partitionAssignors(), { code: error.ErrorCodes.ERR__NOT_IMPLEMENTED });
      });

      rdKafkaConfig['partition.assignment.strategy'] = kjsConfig.partitionAssignors.join(',');
    } else {
      rdKafkaConfig['partition.assignment.strategy'] = PartitionAssigners.roundRobin;
    }

    if (Object.hasOwn(kjsConfig, 'sessionTimeout')) {
      rdKafkaConfig['session.timeout.ms'] = kjsConfig.sessionTimeout;
    } else {
      rdKafkaConfig['session.timeout.ms'] = 30000;
    }

    if (Object.hasOwn(kjsConfig, 'rebalanceTimeout')) {
      /* In librdkafka, we use the max poll interval as the rebalance timeout as well. */
      rdKafkaConfig['max.poll.interval.ms'] = kjsConfig.rebalanceTimeout;
    }

    if (Object.hasOwn(kjsConfig, 'heartbeatInterval')) {
      rdKafkaConfig['heartbeat.interval.ms'] = kjsConfig.heartbeatInterval;
    }

    if (Object.hasOwn(kjsConfig, 'metadataMaxAge')) {
      rdKafkaConfig['topic.metadata.refresh.interval.ms'] = kjsConfig.metadataMaxAge;
    }

    if (Object.hasOwn(kjsConfig, 'allowAutoTopicCreation')) {
      rdKafkaConfig['allow.auto.create.topics'] = kjsConfig.allowAutoTopicCreation;
    } else {
      rdKafkaConfig['allow.auto.create.topics'] = true;
    }

    if (Object.hasOwn(kjsConfig, 'maxBytesPerPartition')) {
      rdKafkaConfig['max.partition.fetch.bytes'] = kjsConfig.maxBytesPerPartition;
    } else {
      rdKafkaConfig['max.partition.fetch.bytes'] = 1048576;
    }

    if (Object.hasOwn(kjsConfig, 'maxWaitTimeInMs')) {
      rdKafkaConfig['fetch.wait.max.ms'] = kjsConfig.maxWaitTimeInMs;
    } else {
      rdKafkaConfig['fetch.wait.max.ms'] = 5000;
    }

    if (Object.hasOwn(kjsConfig, 'minBytes')) {
      rdKafkaConfig['fetch.min.bytes'] = kjsConfig.minBytes;
    }

    if (Object.hasOwn(kjsConfig, 'maxBytes')) {
      rdKafkaConfig['fetch.message.max.bytes'] = kjsConfig.maxBytes;
    } else {
      rdKafkaConfig['fetch.message.max.bytes'] = 10485760;
    }

    if (Object.hasOwn(kjsConfig, 'readUncommitted')) {
      rdKafkaConfig['isolation.level'] = kjsConfig.readUncommitted ? 'read_uncommitted' : 'read_committed';
    }

    if (Object.hasOwn(kjsConfig, 'maxInFlightRequests')) {
      rdKafkaConfig['max.in.flight'] = kjsConfig.maxInFlightRequests;
    }

    if (Object.hasOwn(kjsConfig, 'rackId')) {
      rdKafkaConfig['client.rack'] = kjsConfig.rackId;
    }

    if (Object.hasOwn(kjsConfig, 'fromBeginning')) {
      rdKafkaConfig['auto.offset.reset'] = kjsConfig.fromBeginning ? 'earliest' : 'latest';
    }

    if (Object.hasOwn(kjsConfig, 'autoCommit')) {
      rdKafkaConfig['enable.auto.commit'] = kjsConfig.autoCommit;
    }

    if (Object.hasOwn(kjsConfig, 'autoCommitInterval')) {
      rdKafkaConfig['auto.commit.interval.ms'] = kjsConfig.autoCommitInterval;
    }

    if (Object.hasOwn(kjsConfig, 'autoCommitThreshold')) {
      throw new error.KafkaJSError(CompatibilityErrorMessages.runOptionsAutoCommitThreshold(), { code: error.ErrorCodes.ERR__NOT_IMPLEMENTED });
    }

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
    let compatibleConfig = this.#kafkaJSToConsumerConfig(this.#userConfig.kafkaJs);

    /* Set the logger's level in case we're not in compatibility mode - just set it to DEBUG, the broadest
     * log level, as librdkafka will control the granularity. */
    if (!compatibleConfig || Object.keys(compatibleConfig).length === 0) {
      this.#logger.setLogLevel(logLevel.DEBUG);
    }

    let rdKafkaConfig = Object.assign(compatibleConfig, this.#userConfig);

    /* Delete properties which are already processed, or cannot be passed to node-rdkafka */
    delete rdKafkaConfig.kafkaJs;
    delete rdKafkaConfig.rebalanceListener;

    /* Certain properties that the user has set are overridden. We use trampolines to accommodate the user's callbacks.
     * TODO: add trampoline method for offset commit callback. */
    rdKafkaConfig['offset_commit_cb'] = true;

    if (!Object.hasOwn(this.#userConfig, 'rebalanceListener')) {
      /* We might want to do certain things to maintain internal state in rebalance listener, so we need to set it to an empty object. */
      this.#userConfig.rebalanceListener = {};
    }
    rdKafkaConfig['rebalance_cb'] = this.#rebalanceCallback.bind(this);

    return rdKafkaConfig;
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
      this.#logger.error(err);
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
      pause: this.pause.bind(this, [{ topic: message.topic, partitions: [message.partition] }]),
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

    const rdKafkaConfig = this.#config();

    this.#state = ConsumerState.CONNECTING;
    this.#internalClient = new RdKafka.KafkaConsumer(rdKafkaConfig);
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
   * Subscribes the consumer to the given topics.
   * @param {import("../../types/kafkajs").ConsumerSubscribeTopics | import("../../types/kafkajs").ConsumerSubscribeTopic} subscription
   */
  async subscribe(subscription) {
    if (this.#state !== ConsumerState.CONNECTED) {
      throw new error.KafkaJSError('Subscribe can only be called while connected.', { code: error.ErrorCodes.ERR__STATE });
    }

    if (typeof subscription.fromBeginning === 'boolean') {
      throw new error.KafkaJSError(
        CompatibilityErrorMessages.subscribeOptionsFromBeginning(),
        { code: error.ErrorCodes.ERR__INVALID_ARG });
    }

    if (!Object.hasOwn(subscription, 'topics') && !Object.hasOwn(subscription, 'topic')) {
      throw new error.KafkaJSError(CompatibilityErrorMessages.subscribeOptionsMandatoryMissing(), { code: error.ErrorCodes.ERR__INVALID_ARG });
    }

    let topics = [];
    if (subscription.topic) {
      topics.push(subscription.topic);
    } else if (Array.isArray(subscription.topics)) {
      topics = subscription.topics;
    } else {
      throw new error.KafkaJSError(CompatibilityErrorMessages.subscribeOptionsMandatoryMissing(), { code: error.ErrorCodes.ERR__INVALID_ARG });
    }

    topics = topics.map(topic => {
      if (typeof topic === 'string') {
        return topic;
      } else if (topic instanceof RegExp) {
        // Flags are not supported, and librdkafka only considers a regex match if the first character of the regex is ^.
        if (topic.flags) {
          throw new error.KafkaJSError(CompatibilityErrorMessages.subscribeOptionsRegexFlag(), { code: error.ErrorCodes.ERR__INVALID_ARG });
        }
        const regexSource = topic.source;
        if (regexSource.charAt(0) !== '^')
          return '^' + regexSource;
        else
          return regexSource;
      } else {
        throw new error.KafkaJSError('Invalid topic ' + topic + ' (' + typeof topic + '), the topic name has to be a String or a RegExp', { code: error.ErrorCodes.ERR__INVALID_ARG });
      }
    });

    this.#storedSubscriptions = subscription.replace ? topics : this.#storedSubscriptions.concat(topics);
    this.#internalClient.subscribe(this.#storedSubscriptions);
  }

  async stop() {
    notImplemented();
  }

  /**
   * Starts consumer polling. This method returns immediately.
   * @param {import("../../types/kafkajs").ConsumerRunConfig} config
   */
  async run(config) {
    if (this.#state !== ConsumerState.CONNECTED) {
      throw new error.KafkaJSError('Run must be called after a successful connect().', { code: error.ErrorCodes.ERR__STATE });
    }

    if (Object.hasOwn(config, 'autoCommit')) {
      throw new error.KafkaJSError(CompatibilityErrorMessages.runOptionsAutoCommit(), { code: error.ErrorCodes.ERR__INVALID_ARG });
    }

    if (Object.hasOwn(config, 'autoCommitInterval')) {
      throw new error.KafkaJSError(CompatibilityErrorMessages.runOptionsAutoCommitInterval(), { code: error.ErrorCodes.ERR__INVALID_ARG });
    }

    if (Object.hasOwn(config, 'autoCommitThreshold')) {
      throw new error.KafkaJSError(CompatibilityErrorMessages.runOptionsAutoCommitThreshold(), { code: error.ErrorCodes.ERR__NOT_IMPLEMENTED });
    }

    if (Object.hasOwn(config, 'eachBatch')) {
      throw new error.KafkaJSError(CompatibilityErrorMessages.runOptionsEachBatch(), { code: error.ErrorCodes.ERR__NOT_IMPLEMENTED });
    }

    if (Object.hasOwn(config, 'partitionsConsumedConcurrently')) {
      throw new error.KafkaJSError(CompatibilityErrorMessages.runOptionsPartitionsConsumedConcurrently(), { code: error.ErrorCodes.ERR__NOT_IMPLEMENTED });
    }

    /* We deliberately don't await this. */
    this.#runInternal(config);
  }

  /* Internal polling loop. It accepts the same config object that `run` accepts. */
  async #runInternal(config) {
    while (this.#state === ConsumerState.CONNECTED) {
      const m = await this.#consumeSingle().catch(e => {
        /* Since this error cannot be exposed to the user in the current situation, just log and retry.
         * This is due to restartOnFailure being set to always true. */
        if (this.#logger)
          this.#logger.error(`Consumer encountered error while consuming. Retrying. Error details: ${JSON.stringify(e)}`);
      });

      /* It's possible for the state to change as we await something.
       * Unfortunately, we have no alternative but to break and drop the message, if it exists.
       * TODO: fix this, possibly with a flag in disconnect that waits until the run loop winds down.
       */
      if (this.#state !== ConsumerState.CONNECTED)
        break;

      if (!m) {
        continue;
      }

      /* TODO: add partitionsConsumedConcurrently-based concurrency here.
        * If we maintain a map of topic partitions to promises, and a counter,
        * we can probably achieve it with the correct guarantees of ordering
        * though to maximize performance, we need to consume only from partitions for which
        * an eachMessage call is not already going.
        * It's risky to consume, and then store the message in something like an
        * array/list until it can be processed, because librdkafka marks it as
        * 'stored'... but anyway - we can implement something like this.
        */

      /* Make pending seeks 'concrete'. */
      if (this.#checkPendingSeeks) {
        const invalidateMessage = await this.#seekInternal({ topic: m.topic, partition: m.partition });
        if (invalidateMessage) {
          /* Don't pass this message on to the user if this topic partition was seeked to. */
          continue;
        }

        /* It's possible for the state to change as we await something.
         * Unfortunately, we have no alternative but to break and drop the message.
         * TODO: fix this, possibly with a flag in disconnect that waits until the run loop winds down.
         */
        if (this.#state !== ConsumerState.CONNECTED)
          break;
      }

      try {
        await config.eachMessage(
          this.#createPayload(m)
        )

        /* It's possible for the state to change as we await something.
         * Unfortunately, we have no alternative but to break without taking any action that the user might need.
         */
        if (this.#state !== ConsumerState.CONNECTED)
          break;
      } catch (e) {
        /* It's not only possible, but expected that an error will be thrown by eachMessage.
         * This is especially true since the pattern of pause() followed by throwing an error
         * is encouraged. To meet the API contract, we seek one offset backward at this point (which
         * means seeking to the message offset). */
        if (this.#state === ConsumerState.CONNECTED)
          this.seek({
            topic: m.topic,
            partition: m.partition,
            offset: m.offset,
          });
      }

      /* Force a immediate seek here. It's possible that there are no more messages to be passed to the user,
       * but the user seeked in the call to eachMessage, or else we encountered the error catch block.
       * In that case, the results of that seek will never be reflected unless we do this. */
      if (this.#checkPendingSeeks && this.#state === ConsumerState.CONNECTED)
        await this.#seekInternal();

      /* TODO: another check we need to do here is to see how kafkaJS is handling
       * commits. Are they commmitting after a message is _processed_?
       * In that case we need to turn off librdkafka's auto-commit, and commit
       * inside this function.
       */

      /* Yield for a bit to allow other scheduled tasks on the event loop to run.
       * For instance, if disconnect() is called during/after we await eachMessage, and
       * we don't await anything else after that, this loop will run despite needing to
       * disconnect.
       * It's better than any pending tasks be processed here, while we've processed one
       * message completely, rather than between message processing. */
      await new Promise((resolve) => setTimeout(resolve, 0));
    }
  }

  async #commitOffsetsUntilNoStateErr(offsetsToCommit) {
    let err = { code: error.ErrorCodes.ERR_NO_ERROR };
    do {
      try {
        await this.commitOffsets(offsetsToCommit);
      } catch (e) {
        err = e;
      }
    } while (err.code && err.code === error.ErrorCodes.ERR__STATE);
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
        const topicPartitionsRdKafka = topicPartitions.map(
          topicPartitionOffsetToRdKafka);
        this.#internalClient.commitSync(topicPartitionsRdKafka);
      }
    } catch (e) {
      if (!e.code || e.code !== error.ErrorCodes.ERR__NO_OFFSET) {
        throw createKafkaJsErrorFromLibRdKafkaError(e);
      }
    }
  }

  /**
   * Apply pending seeks to topic partitions we have just obtained as a result of a rebalance.
   * @param {{topic: string, partition: number}[]} assignment The list of topic partitions to check for pending seeks.
   * @returns {{topic: string, partition: number, offset: number}[]} the new assignment with the offsets seeked to, which can be passed to assign().
   */
  #assignAsPerSeekedOffsets(assignment) {
    const offsetsToCommit = [];

    for (let i = 0; i < assignment.length; i++) {
      const topicPartition = assignment[i];
      const key = `${topicPartition.topic}|${topicPartition.partition}`;
      if (!this.#pendingSeeks.has(key))
        continue;

      const offset = this.#pendingSeeks.get(key);
      this.#pendingSeeks.delete(key);

      assignment[i].offset = offset;

      offsetsToCommit.push({
        topic: topicPartition.topic,
        partition: topicPartition.partition,
        offset: String(offset),
      });
    }
    return assignment;
  }

  /**
   * This method processes any pending seeks on partitions that are assigned to this consumer.
   * @param {{topic: string, partition: number}} messageTopicPartition If this method was triggered by a message, pass the topic partition of the message, else it's optional.
   * @returns whether the message that triggered this should be invalidated (if any).
   */
  async #seekInternal(messageTopicPartition) {
    this.#checkPendingSeeks = false;
    const assignment = this.assignment();
    const offsetsToCommit = [];
    let invalidateMessage = false;

    for (const topicPartition of assignment) {
      const key = `${topicPartition.topic}|${topicPartition.partition}`;
      if (!this.#pendingSeeks.has(key))
        continue;

      const offset = this.#pendingSeeks.get(key);
      this.#pendingSeeks.delete(key);

      const topicPartitionOffset = {
        topic: topicPartition.topic,
        partition: topicPartition.partition,
        offset
      };

      /* It's assumed that topicPartition is already assigned, and thus can be seeked to and committed to.
       * Errors are logged to detect bugs in the internal code. */
      this.#internalClient.seek(topicPartitionOffset, 0, err => err ? this.#logger.error(err) : null);
      offsetsToCommit.push({
        topic: topicPartition.topic,
        partition: topicPartition.partition,
        offset: String(offset),
      });

      /* If we're seeking the same topic partition as in the message that triggers it, invalidate
       * the message. */
      if (messageTopicPartition && topicPartition.topic === messageTopicPartition.topic && topicPartition.partition === messageTopicPartition.partition) {
        invalidateMessage = true;
      }
    }

    if (offsetsToCommit.length !== 0 && this.#internalConfig['enable.auto.commit']) {
      await this.#commitOffsetsUntilNoStateErr(offsetsToCommit);
    }

    return invalidateMessage;
  }

  /**
   * Seek to the given offset for the topic partition.
   * This method is completely asynchronous, and does not wait for the seek to complete.
   * In case any partitions that are seeked to, are not a part of the current assignment, they are stored internally.
   * If at any time, the consumer is assigned the partition, the seek will be performed.
   * Depending on the value of the librdkafka property 'enable.auto.commit', the consumer will commit the offset seeked to.
   * @param {import("../../types/kafkajs").TopicPartitionOffset} topicPartitionOffset
   */
  seek(topicPartitionOffset) {
    if (this.#state !== ConsumerState.CONNECTED) {
      throw new error.KafkaJSError('Seek can only be called while connected.', { code: error.ErrorCodes.ERR__STATE });
    }

    const rdKafkaTopicPartitionOffset =
      topicPartitionOffsetToRdKafka(topicPartitionOffset);

    if (typeof rdKafkaTopicPartitionOffset.topic !== 'string') {
      throw new error.KafkaJSError('Topic must be a string.', { code: error.ErrorCodes.ERR__INVALID_ARG });
    }

    if (isNaN(rdKafkaTopicPartitionOffset.offset) || (rdKafkaTopicPartitionOffset.offset < 0 && rdKafkaTopicPartitionOffset.offset !== -2 && rdKafkaTopicPartitionOffset.offset !== -3)) {
      throw new error.KafkaJSError('Offset must be >= 0, or a special value.', { code: error.ErrorCodes.ERR__INVALID_ARG });
    }

    this.#checkPendingSeeks = true;
    this.#pendingSeeks.set(`${rdKafkaTopicPartitionOffset.topic}|${rdKafkaTopicPartitionOffset.partition}`, rdKafkaTopicPartitionOffset.offset);
  }

  async describeGroup() {
    notImplemented();
  }

  /**
   * Find the assigned topic partitions for the consumer.
   * @returns {import("../../types/kafkajs").TopicPartition[]} the current assignment.
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
   * @returns {Function} a function that can be called to resume the given topic partitions.
   */
  pause(topics) {
    if (this.#state !== ConsumerState.CONNECTED) {
      throw new error.KafkaJSError('Pause can only be called while connected.', { code: error.ErrorCodes.ERR__STATE });
    }

    for (let topic of topics) {
      if (typeof topic.topic !== 'string') {
        throw new error.KafkaJSError('Topic must be a string.', { code: error.ErrorCodes.ERR__INVALID_ARG });
      }

      if (!topic.partitions) {
        topic.partitions = this.#getAllAssignedPartition(topic.topic);
      }
    }

    topics = this.#flattenTopicPartitions(topics);
    if (topics.length === 0) {
      return;
    }
    this.#internalClient.pause(topics);

    topics.map(JSON.stringify).forEach(topicPartition => this.#pausedPartitions.add(topicPartition));

    return () => this.resume(topics);
  }

  /**
   * Returns the list of paused topic partitions.
   * @returns {{topic: string, partitions: number[]}[]} a list of paused topic partitions.
   */
  paused() {
    const topicToPartitions = Array
      .from(this.#pausedPartitions.values())
      .map(JSON.parse)
      .reduce(
        (acc, { topic, partition }) => {
          if (!acc[topic]) {
            acc[topic] = [];
          }
          acc[topic].push(partition);
          return acc;
        },
        {});
    return Array.from(Object.entries(topicToPartitions), ([topic, partitions]) => ({ topic, partitions }));
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
      if (typeof topic.topic !== 'string') {
        throw new error.KafkaJSError('Topic must be a string.', { code: error.ErrorCodes.ERR__INVALID_ARG });
      }

      if (!topic.partitions) {
        topic.partitions = this.#getAllAssignedPartition(topic.topic);
      }
    }

    topics = this.#flattenTopicPartitions(topics);
    if (topics.length === 0) {
      return;
    }
    this.#internalClient.resume(topics);

    topics.map(JSON.stringify).forEach(topicPartition => this.#pausedPartitions.delete(topicPartition));
  }

  on(/* eventName, listener */) {
    notImplemented();
  }

  /**
   * @returns {import("../../types/kafkajs").Logger} the logger associated to this consumer.
   */
  logger() {
    return this.#logger;
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
