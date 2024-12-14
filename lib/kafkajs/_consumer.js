const LibrdKafkaError = require('../error');
const { Admin } = require('./_admin');
const error = require('./_error');
const RdKafka = require('../rdkafka');
const {
  kafkaJSToRdKafkaConfig,
  topicPartitionOffsetToRdKafka,
  topicPartitionOffsetMetadataToRdKafka,
  topicPartitionOffsetMetadataToKafkaJS,
  createBindingMessageMetadata,
  createKafkaJsErrorFromLibRdKafkaError,
  notImplemented,
  loggerTrampoline,
  DefaultLogger,
  CompatibilityErrorMessages,
  severityToLogLevel,
  checkAllowedKeys,
  logLevel,
  Lock,
  partitionKey,
  DeferredPromise,
  Timer
} = require('./_common');
const { Buffer } = require('buffer');
const MessageCache = require('./_consumer_cache');
const { hrtime } = require('process');
const { LinkedList } = require('./_linked-list');

const ConsumerState = Object.freeze({
  INIT: 0,
  CONNECTING: 1,
  CONNECTED: 2,
  DISCONNECTING: 3,
  DISCONNECTED: 4,
});

/**
 * A list of supported partition assignor types.
 * @enum {string}
 * @readonly
 * @memberof KafkaJS
 */
const PartitionAssigners = {
  roundRobin: 'roundrobin',
  range: 'range',
  cooperativeSticky: 'cooperative-sticky',
};

/**
 * Consumer for reading messages from Kafka (promise-based, async API).
 *
 * The consumer allows reading messages from the Kafka cluster, and provides
 * methods to configure and control various aspects of that. This class should
 * not be instantiated directly, and rather, an instance of
 * [Kafka]{@link KafkaJS.Kafka} should be used.
 *
 * @example
 * const { Kafka } = require('@confluentinc/kafka-javascript');
 * const kafka = new Kafka({ 'bootstrap.servers': 'localhost:9092' });
 * const consumer = kafka.consumer({ 'group.id': 'test-group' });
 * await consumer.connect();
 * await consumer.subscribe({ topics: ["test-topic"] });
 * consumer.run({
 *   eachMessage: async ({ topic, partition, message }) => { console.log({topic, partition, message}); }
 * });
 * @memberof KafkaJS
 * @see [Consumer example]{@link https://github.com/confluentinc/confluent-kafka-javascript/blob/master/examples/consumer.js}
 */
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
   * Stores the first error encountered while connecting (if any). This is what we
   * want to reject with.
   * @type {Error|null}
   */
  #connectionError = null;

  /**
   * state is the current state of the consumer.
   * @type {ConsumerState}
   */
  #state = ConsumerState.INIT;

  /**
   * Contains a mapping of topic+partition to an offset that the user wants to seek to.
   * The keys are of the type "<topic>|<partition>".
   * @type {Map<string, number>}
   */
  #pendingSeeks = new Map();

  /**
   * Stores the map of paused partitions keys to TopicPartition objects.
   * @type {Map<string, TopicPartition>}
   */
  #pausedPartitions = new Map();

  /**
   * Contains a list of stored topics/regexes that the user has subscribed to.
   * @type {Array<string|RegExp>}
   */
  #storedSubscriptions = [];

  /**
   * A logger for the consumer.
   * @type {import("../../types/kafkajs").Logger}
   */
  #logger = new DefaultLogger();

  /**
   * A map of topic+partition to the offset that was last consumed.
   * The keys are of the type "<topic>|<partition>".
   * @type {Map<string, number>}
   */
  #lastConsumedOffsets = new Map();

  /**
   * A lock for consuming and disconnecting.
   * This lock should be held whenever we want to change the state from CONNECTED to any state other than CONNECTED.
   * In practical terms, this lock is held whenever we're consuming a message, or disconnecting.
   * @type {Lock}
   */
  #lock = new Lock();

  /**
   * Whether the consumer is running.
   * @type {boolean}
   */
  #running = false;

  /**
   * The message cache for KafkaJS compatibility mode.
   * @type {MessageCache|null}
   */
  #messageCache = null;

  /**
   * The maximum size of the message cache.
   * Will be adjusted dynamically.
   */
  #messageCacheMaxSize = 1;

  /**
   * Number of times we tried to increase the cache.
   */
  #increaseCount = 0;

  /**
   * Whether the user has enabled manual offset management (commits).
   */
  #autoCommit = false;

  /**
   * Signals an intent to disconnect the consumer.
   */
  #disconnectStarted = false;

  /**
   * Number of partitions owned by the consumer.
   * @note This value may or may not be completely accurate, it's more so a hint for spawning concurrent workers.
   */
  #partitionCount = 0;

  /**
   * Whether worker termination has been scheduled.
   */
  #workerTerminationScheduled = new DeferredPromise();

  /**
   * The worker functions currently running in the consumer.
   */
  #workers = [];

  /**
   * The number of partitions to consume concurrently as set by the user, or 1.
   */
  #concurrency = 1;

  /**
   * Promise that resolves together with last in progress fetch.
   * It's set to null when no fetch is in progress.
   */
  #fetchInProgress;

  /**
   * List of DeferredPromises waiting on consumer queue to be non-empty.
   */
  #queueWaiters = new LinkedList();

  /**
   * Whether any rebalance callback is in progress.
   * That can last more than the fetch itself given it's not awaited.
   * So we await it after fetch is done.
   */
  #rebalanceCbInProgress;

  /**
   * Promise that is resolved on fetch to restart max poll interval timer.
   */
  #maxPollIntervalRestart = new DeferredPromise();

  /**
   * Initial default value for max poll interval.
   */
  #maxPollIntervalMs = 300000;
  /**
   * Maximum interval between poll calls from workers,
   * if exceeded, the cache is cleared so a new poll can be made
   * before reaching the max poll interval.
   * It's set to max poll interval value.
   */
  #cacheExpirationTimeoutMs = 300000;

  /**
   * Last fetch real time clock in nanoseconds.
   */
  #lastFetchClockNs = 0;

  /**
   * List of pending operations to be executed after
   * all workers reach the end of their current processing.
   */
  #pendingOperations = [];

  /**
   * Maps topic-partition key to the batch payload for marking staleness.
   *
   * Only used with eachBatch.
   * NOTE: given that size of this map will never exceed #concurrency, a
   * linear search might actually be faster over what will generally be <10 elems.
   * But a map makes conceptual sense. Revise at a later point if needed.
   */
  #topicPartitionToBatchPayload = new Map();

  /**
   * The client name used by the consumer for logging - determined by librdkafka
   * using a combination of clientId and an integer.
   * @type {string|undefined}
   */
  #clientName = undefined;

  // Convenience function to create the metadata object needed for logging.
  #createConsumerBindingMessageMetadata() {
    return createBindingMessageMetadata(this.#clientName);
  }

  /**
   * This method should not be used directly. See {@link KafkaJS.Consumer}.
   * @constructor
   * @param {import("../../types/kafkajs").ConsumerConfig} kJSConfig
   */
  constructor(kJSConfig) {
    this.#userConfig = kJSConfig;
  }

  /**
   * @returns {import("../rdkafka").Consumer | null} the internal node-rdkafka client.
   * @note only for internal use and subject to API changes.
   * @private
   */
  _getInternalClient() {
    return this.#internalClient;
  }

  /**
   * Create a new admin client using the underlying connections of the consumer.
   *
   * The consumer must be connected before connecting the resulting admin client.
   * The usage of the admin client is limited to the lifetime of the consumer.
   * The consumer's logger is shared with the admin client.
   * @returns {KafkaJS.Admin}
   */
  dependentAdmin() {
    return new Admin(null, this);
  }

  #config() {
    if (!this.#internalConfig)
      this.#internalConfig = this.#finalizedConfig();
    return this.#internalConfig;
  }

  /**
   * Clear the message cache, and reset to stored positions.
   *
   * @param {Array<{topic: string, partition: number}>|null} topicPartitions to clear the cache for, if null, then clear all assigned.
   * @private
   */
  async #clearCacheAndResetPositions() {
    /* Seek to stored offset for each topic partition. It's possible that we've
     * consumed messages upto N from the internalClient, but the user has stale'd the cache
     * after consuming just k (< N) messages. We seek back to last consumed offset + 1. */
    this.#messageCache.clear();
    this.#messageCacheMaxSize = 1;
    this.#increaseCount = 0;
    const clearPartitions = this.assignment();
    const seeks = [];
    for (const topicPartition of clearPartitions) {
      const key = partitionKey(topicPartition);
      if (!this.#lastConsumedOffsets.has(key))
        continue;

      const lastConsumedOffsets = this.#lastConsumedOffsets.get(key);
      const topicPartitionOffsets = [
        {
          topic: topicPartition.topic,
          partition: topicPartition.partition,
          offset: lastConsumedOffsets.offset,
          leaderEpoch: lastConsumedOffsets.leaderEpoch,
        }
      ];
      seeks.push(this.#seekInternal(topicPartitionOffsets));
    }

    await Promise.allSettled(seeks);
    try {
      await Promise.all(seeks);
    } catch (err) {
      /* TODO: we should cry more about this and render the consumer unusable. */
      this.#logger.error(`Seek error. This is effectively a fatal error: ${err.stack}`);
    }
  }

  #unassign(assignment) {
    if (this.#internalClient.rebalanceProtocol() === "EAGER") {
      this.#internalClient.unassign();
      this.#messageCache.clear();
      this.#partitionCount = 0;
    } else {
      this.#internalClient.incrementalUnassign(assignment);
      this.#messageCache.markStale(assignment);
      this.#partitionCount -= assignment.length;
    }
  }

  /**
   * Used as a trampoline to the user's rebalance listener, if any.
   * @param {Error} err - error in rebalance
   * @param {import("../../types").TopicPartition[]} assignment
   * @private
   */
  async #rebalanceCallback(err, assignment) {
    const isLost = this.#internalClient.assignmentLost();
    this.#rebalanceCbInProgress = new DeferredPromise();
    let assignmentFnCalled = false;
    this.#logger.info(
      `Received rebalance event with message: '${err.message}' and ${assignment.length} partition(s), isLost: ${isLost}`,
      this.#createConsumerBindingMessageMetadata());
    /* We allow the user to modify the assignment by returning it. If a truthy
     * value is returned, we use that and do not apply any pending seeks to it either.
     * The user can alternatively use the assignmentFns argument.
     * Precedence is given to the calling of functions within assignmentFns. */
    let assignmentModified = false;

    const assignmentFn = (userAssignment) => {
      if (assignmentFnCalled)
        return;
      assignmentFnCalled = true;

      if (this.#internalClient.rebalanceProtocol() === "EAGER") {
        this.#internalClient.assign(userAssignment);
        this.#partitionCount = userAssignment.length;
      } else {
        this.#internalClient.incrementalAssign(userAssignment);
        this.#partitionCount += userAssignment.length;
      }
    };

    const unassignmentFn = (userAssignment) => {
      if (assignmentFnCalled)
        return;

      assignmentFnCalled = true;
      if (this.#disconnectStarted)
        this.#unassign(userAssignment);
      else
        this.#addPendingOperation(() => this.#unassign(userAssignment));
    };

    try {
      err = LibrdKafkaError.create(err);
      const userSpecifiedRebalanceCb = this.#userConfig['rebalance_cb'];

      if (typeof userSpecifiedRebalanceCb === 'function') {
        const assignmentFns = {
          assign: assignmentFn,
          unassign: unassignmentFn,
          assignmentLost: () => isLost,
        };

        let alternateAssignment = null;
        try {
          alternateAssignment = await userSpecifiedRebalanceCb(err, assignment, assignmentFns);
        } catch (e) {
          this.#logger.error(`Error from user's rebalance callback: ${e.stack}, `+
                             'continuing with the default rebalance behavior.');
        }

        if (alternateAssignment) {
          assignment = alternateAssignment;
          assignmentModified = true;
        }
      } else if (err.code !== LibrdKafkaError.codes.ERR__ASSIGN_PARTITIONS && err.code !== LibrdKafkaError.codes.ERR__REVOKE_PARTITIONS) {
        throw new Error(`Unexpected rebalance_cb error code ${err.code}`);
      }

    } finally {
      /* Emit the event */
      this.#internalClient.emit('rebalance', err, assignment);

      /**
       * We never need to clear the cache in case of a rebalance.
       * This is because rebalances are triggered ONLY when we call the consume()
       * method of the internalClient.
       * In case consume() is being called, we've already either consumed all the messages
       * in the cache, or timed out (this.#messageCache.cachedTime is going to exceed max.poll.interval)
       * and marked the cache stale. This means that the cache is always expired when a rebalance
       * is triggered.
       * This is applicable both for incremental and non-incremental rebalances.
       * Multiple consume()s cannot be called together, too, because we make sure that only
       * one worker is calling into the internal consumer at a time.
       */
      try {

        if (err.code === LibrdKafkaError.codes.ERR__ASSIGN_PARTITIONS) {

          const checkPendingSeeks = this.#pendingSeeks.size !== 0;
          if (checkPendingSeeks && !assignmentModified && !assignmentFnCalled)
            assignment = this.#assignAsPerSeekedOffsets(assignment);

          assignmentFn(assignment);

        } else {
          unassignmentFn(assignment);
        }
      } catch (e) {
        // Ignore exceptions if we are not connected
        if (this.#internalClient.isConnected()) {
          this.#internalClient.emit('rebalance.error', e);
        }
      }

      /**
       * Schedule worker termination here, in case the number of workers is not equal to the target concurrency.
       * We need to do this so we will respawn workers with the correct concurrency count.
       */
      const workersToSpawn = Math.max(1, Math.min(this.#concurrency, this.#partitionCount));
      if (workersToSpawn !== this.#workers.length) {
        this.#workerTerminationScheduled.resolve();
        /* We don't need to await the workers here. We are OK if the termination and respawning
          * occurs later, since even if we have a few more or few less workers for a while, it's
          * not a big deal. */
      }
      this.#rebalanceCbInProgress.resolve();
    }
  }

  #kafkaJSToConsumerConfig(kjsConfig) {
    if (!kjsConfig || Object.keys(kjsConfig).length === 0) {
      return {};
    }

    const disallowedKey = checkAllowedKeys('consumer', kjsConfig);
    if (disallowedKey !== null) {
      throw new error.KafkaJSError(CompatibilityErrorMessages.unsupportedKey(disallowedKey),
        { code: error.ErrorCodes.ERR__INVALID_ARG });
    }
    const rdKafkaConfig = kafkaJSToRdKafkaConfig(kjsConfig);

    this.#logger = new DefaultLogger();

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
      rdKafkaConfig['max.poll.interval.ms'] = +kjsConfig.rebalanceTimeout;
    } else if (!rdKafkaConfig['max.poll.interval.ms']) {
      rdKafkaConfig['max.poll.interval.ms'] = 300000; /* librdkafka default */
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
    } else {
      rdKafkaConfig['enable.auto.commit'] = true;
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
    let compatibleConfig = this.#kafkaJSToConsumerConfig(this.#userConfig.kafkaJS);

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

    /* Certain properties that the user has set are overridden. We use trampolines to accommodate the user's callbacks.
     * TODO: add trampoline method for offset commit callback. */
    rdKafkaConfig['offset_commit_cb'] = true;
    rdKafkaConfig['rebalance_cb'] = (err, assignment) => this.#rebalanceCallback(err, assignment).catch(e =>
      {
        if (this.#logger)
          this.#logger.error(`Error from rebalance callback: ${e.stack}`);
      });

    /* We handle offset storage within the promisified API by ourselves. Thus we don't allow the user to change this
     * setting and set it to false. */
    if (Object.hasOwn(this.#userConfig, 'enable.auto.offset.store')) {
      throw new error.KafkaJSError(
        "Changing 'enable.auto.offset.store' is unsupported while using the promisified API.",
        { code: error.ErrorCodes.ERR__INVALID_ARG });
    }
    rdKafkaConfig['enable.auto.offset.store'] = false;

    if (!Object.hasOwn(rdKafkaConfig, 'enable.auto.commit')) {
      this.#autoCommit = true; /* librdkafka default. */
    } else {
      this.#autoCommit = rdKafkaConfig['enable.auto.commit'];
    }

     /**
     * Actual max poll interval is twice the configured max poll interval,
     * because we want to ensure that when we ask for worker termination,
     * and there is one last message to be processed, we can process it in
     * the configured max poll interval time.
     * This will cause the rebalance callback timeout to be double
     * the value of the configured max poll interval.
     * But it's expected otherwise we cannot have a cache and need to consider
     * max poll interval reached on processing the very first message.
     */
    this.#maxPollIntervalMs = rdKafkaConfig['max.poll.interval.ms'] ?? 300000;
    this.#cacheExpirationTimeoutMs = this.#maxPollIntervalMs;
    rdKafkaConfig['max.poll.interval.ms'] = this.#maxPollIntervalMs * 2;

    return rdKafkaConfig;
  }

  #readyCb() {
    if (this.#state !== ConsumerState.CONNECTING) {
      /* The connectPromiseFunc might not be set, so we throw such an error. It's a state error that we can't recover from. Probably a bug. */
      throw new error.KafkaJSError(`Ready callback called in invalid state ${this.#state}`, { code: error.ErrorCodes.ERR__STATE });
    }
    this.#state = ConsumerState.CONNECTED;

    /* Slight optimization for cases where the size of messages in our subscription is less than the cache size. */
    this.#internalClient.setDefaultIsTimeoutOnlyForFirstMessage(true);

    // We will fetch only those messages which are already on the queue. Since we will be
    // woken up by #queueNonEmptyCb, we don't need to set a wait timeout.
    this.#internalClient.setDefaultConsumeTimeout(0);

    this.#clientName = this.#internalClient.name;
    this.#logger.info('Consumer connected', this.#createConsumerBindingMessageMetadata());

    // Resolve the promise.
    this.#connectPromiseFunc['resolve']();
  }

  /**
   * Callback for the event.error event, either fails the initial connect(), or logs the error.
   * @param {Error} err
   * @private
   */
  #errorCb(err) {
    /* If we get an error in the middle of connecting, reject the promise later with this error. */
    if (this.#state < ConsumerState.CONNECTED) {
      if (!this.#connectionError)
        this.#connectionError = err;
    } else {
      this.#logger.error(err, this.#createConsumerBindingMessageMetadata());
    }
  }

  /**
   * Converts headers returned by node-rdkafka into a format that can be used by the eachMessage/eachBatch callback.
   * @param {import("../..").MessageHeader[] | undefined} messageHeaders
   * @returns {import("../../types/kafkajs").IHeaders}
   * @private
   */
  #createHeaders(messageHeaders) {
    let headers;
    if (messageHeaders) {
      headers = {};
      for (const header of messageHeaders) {
        for (const [key, value] of Object.entries(header)) {
          if (!Object.hasOwn(headers, key)) {
            headers[key] = value;
          } else if (headers[key].constructor === Array) {
            headers[key].push(value);
          } else {
            headers[key] = [headers[key], value];
          }
        }
      }
    }
    return headers;
  }

  /**
   * Converts a message returned by node-rdkafka into a message that can be used by the eachMessage callback.
   * @param {import("../..").Message} message
   * @returns {import("../../types/kafkajs").EachMessagePayload}
   * @private
   */
  #createPayload(message) {
    let key = message.key;
    if (typeof key === 'string') {
      key = Buffer.from(key);
    }

    let timestamp = message.timestamp ? String(message.timestamp) : '';
    const headers = this.#createHeaders(message.headers);

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
        leaderEpoch: message.leaderEpoch,
        headers
      },
      heartbeat: async () => { /* no op */ },
      pause: this.pause.bind(this, [{ topic: message.topic, partitions: [message.partition] }]),
    };
  }

  /**
   * Method used by #createBatchPayload to resolve offsets.
   * Resolution stores the offset into librdkafka if needed, and into the lastConsumedOffsets map
   * that we use for seeking to the last consumed offset when forced to clear cache.
   *
   * @param {*} payload The payload we're creating. This is a method attached to said object.
   * @param {*} offsetToResolve The offset to resolve.
   * @param {*} leaderEpoch The leader epoch of the message (optional). We expect users to provide it, but for API-compatibility reasons, it's optional.
   * @private
   */
  #eachBatchPayload_resolveOffsets(payload, offsetToResolve, leaderEpoch = -1) {
    const offset = +offsetToResolve;

    if (isNaN(offset)) {
      /* Not much we can do but throw and log an error. */
      const e = new error.KafkaJSError(`Invalid offset to resolve: ${offsetToResolve}`, { code: error.ErrorCodes.ERR__INVALID_ARG });
      throw e;
    }

    /* The user might resolve offset N (< M) after resolving offset M. Given that in librdkafka we can only
     * store one offset, store the last possible one. */
    if (offset <= payload._lastResolvedOffset.offset)
      return;

    const topic = payload.batch.topic;
    const partition = payload.batch.partition;

    payload._lastResolvedOffset = { offset, leaderEpoch };

    try {
      this.#internalClient._offsetsStoreSingle(
        topic,
        partition,
        offset + 1,
        leaderEpoch);
    } catch (e) {
      /* Not much we can do, except log the error. */
      this.#logger.error(`Consumer encountered error while storing offset. Error details: ${e}:${e.stack}`, this.#createConsumerBindingMessageMetadata());
    }
  }

  /**
   * Method used by #createBatchPayload to commit offsets.
   * @private
   */
  async #eachBatchPayload_commitOffsetsIfNecessary() {
    if (this.#autoCommit) {
      /* librdkafka internally handles committing of whatever we store.
       * We don't worry about it here. */
      return;
    }
    /* If the offsets are being resolved by the user, they've already called resolveOffset() at this point
     * We just need to commit the offsets that we've stored. */
    await this.commitOffsets();
  }

  /**
   * Request a size increase.
   * It increases the size by 2x, but only if the size is less than 1024,
   * only if the size has been requested to be increased twice in a row.
   * @private
   */
   #increaseMaxSize() {
     if (this.#messageCacheMaxSize === 1024)
         return;
     this.#increaseCount++;
     if (this.#increaseCount <= 1)
         return;
     this.#messageCacheMaxSize = Math.min(this.#messageCacheMaxSize << 1, 1024);
     this.#increaseCount = 0;
  }

  /**
   * Request a size decrease.
   * It decreases the size to 80% of the last received size, with a minimum of 1.
   * @param {number} recvdSize - the number of messages received in the last poll.
   * @private
   */
  #decreaseMaxSize(recvdSize) {
      this.#messageCacheMaxSize = Math.max(Math.floor((recvdSize * 8) / 10), 1);
      this.#increaseCount = 0;
  }

  /**
   * Converts a list of messages returned by node-rdkafka into a message that can be used by the eachBatch callback.
   * @param {import("../..").Message[]} messages - must not be empty. Must contain messages from the same topic and partition.
   * @returns {import("../../types/kafkajs").EachBatchPayload}
   * @private
   */
  #createBatchPayload(messages) {
    const topic = messages[0].topic;
    const partition = messages[0].partition;

    const messagesConverted = [];
    for (let i = 0; i < messages.length; i++) {
      const message = messages[i];
      let key = message.key;
      if (typeof key === 'string') {
        key = Buffer.from(key);
      }

      let timestamp = message.timestamp ? String(message.timestamp) : '';
      const headers = this.#createHeaders(message.headers);

      const messageConverted = {
        key,
        value: message.value,
        timestamp,
        attributes: 0,
        offset: String(message.offset),
        size: message.size,
        leaderEpoch: message.leaderEpoch,
        headers
      };

      messagesConverted.push(messageConverted);
    }

    const batch = {
      topic,
      partition,
      highWatermark: '-1001', /* We don't fetch it yet. We can call committed() to fetch it but that might incur network calls. */
      messages: messagesConverted,
      isEmpty: () => false,
      firstOffset: () => (messagesConverted[0].offset).toString(),
      lastOffset: () => (messagesConverted[messagesConverted.length - 1].offset).toString(),
      offsetLag: () => notImplemented(),
      offsetLagLow: () => notImplemented(),
    };

    const returnPayload = {
      batch,
      _stale: false,
      _lastResolvedOffset: { offset: -1, leaderEpoch: -1 },
      heartbeat: async () => { /* no op */ },
      pause: this.pause.bind(this, [{ topic, partitions: [partition] }]),
      commitOffsetsIfNecessary: this.#eachBatchPayload_commitOffsetsIfNecessary.bind(this),
      isRunning: () => this.#running,
      isStale: () => returnPayload._stale,
      /* NOTE: Probably never to be implemented. Not sure exactly how we'd compute this
       * inexpensively. */
      uncommittedOffsets: () => notImplemented(),
    };

    returnPayload.resolveOffset = this.#eachBatchPayload_resolveOffsets.bind(this, returnPayload);

    return returnPayload;
  }

  async #fetchAndResolveWith(takeFromCache, size) {
    if (this.#fetchInProgress) {
      return null;
    }

    try {
      this.#fetchInProgress = new DeferredPromise();
      const fetchResult = new DeferredPromise();
      this.#logger.debug(`Attempting to fetch ${size} messages to the message cache`,
        this.#createConsumerBindingMessageMetadata());
      this.#internalClient.consume(size, (err, messages) =>
        fetchResult.resolve([err, messages]));

      let [err, messages] = await fetchResult;
      if (this.#rebalanceCbInProgress) {
        await this.#rebalanceCbInProgress;
        this.#rebalanceCbInProgress = null;
      }

      if (err) {
        throw createKafkaJsErrorFromLibRdKafkaError(err);
      }

      this.#messageCache.addMessages(messages);
      const res = takeFromCache();
      this.#lastFetchClockNs = hrtime.bigint();
      this.#maxPollIntervalRestart.resolve();
      if (messages.length === this.#messageCacheMaxSize) {
        this.#increaseMaxSize();
      } else {
        this.#decreaseMaxSize(messages.length);
      }
      return res;
    } finally {
      this.#fetchInProgress.resolve();
      this.#fetchInProgress = null;
    }
  }

  /**
   * Consumes a single message from the internal consumer.
   * @param {PerPartitionCache} ppc Per partition cache to use or null|undefined .
   * @returns {Promise<import("../..").Message | null>} a promise that resolves to a single message or null.
   * @note this method caches messages as well, but returns only a single message.
   * @private
   */
  async #consumeSingleCached(ppc) {
    const msg = this.#messageCache.next(ppc);
    if (msg) {
      return msg;
    }

    /* It's possible that we get msg = null, but that's because partitionConcurrency
     * exceeds the number of partitions containing messages. So in this case,
     * we should not call for new fetches, rather, try to focus on what we have left.
     */
    if (!msg && this.#messageCache.assignedSize !== 0) {
      return null;
    }

    return this.#fetchAndResolveWith(() => this.#messageCache.next(),
      this.#messageCacheMaxSize);
  }

  /**
   * Consumes a single message from the internal consumer.
   * @param {number} savedIndex - the index of the message in the cache to return.
   * @param {number} size - the number of messages to fetch.
   * @returns {Promise<import("../..").Message[] | null>} a promise that resolves to a list of messages or null.
   * @note this method caches messages as well.
   * @sa #consumeSingleCached
   * @private
   */
  async #consumeCachedN(ppc, size) {
    const msgs = this.#messageCache.nextN(ppc, size);
    if (msgs) {
      return msgs;
    }

    /* It's possible that we get msgs = null, but that's because partitionConcurrency
     * exceeds the number of partitions containing messages. So in this case,
     * we should not call for new fetches, rather, try to focus on what we have left.
     */
    if (!msgs && this.#messageCache.assignedSize !== 0) {
      return null;
    }

    return this.#fetchAndResolveWith(() =>
        this.#messageCache.nextN(null, size),
      this.#messageCacheMaxSize);
  }

  /**
   * Consumes n messages from the internal consumer.
   * @returns {Promise<import("../..").Message[]>} A promise that resolves to a list of messages. The size of this list is guaranteed to be less than or equal to n.
   * @note this method cannot be used in conjunction with #consumeSingleCached.
   * @private
   */
  async #consumeN(n) {
    return new Promise((resolve, reject) => {
      this.#internalClient.consume(n, (err, messages) => {
        if (err) {
          reject(createKafkaJsErrorFromLibRdKafkaError(err));
          return;
        }
        resolve(messages);
      });
    });
  }

  /**
   * Flattens a list of topics with partitions into a list of topic, partition.
   * @param {Array<({topic: string, partitions: Array<number>}|{topic: string, partition: number})>} topics
   * @returns {import("../../types/rdkafka").TopicPartition[]} a list of (topic, partition).
   * @private
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
   * Set up the client and connect to the bootstrap brokers.
   *
   * This method can be called only once for a consumer instance, and must be
   * called before doing any other operations.
   *
   * @returns {Promise<void>} a promise that resolves when the consumer is connected.
   */
  async connect() {
    if (this.#state !== ConsumerState.INIT) {
      throw new error.KafkaJSError('Connect has already been called elsewhere.', { code: error.ErrorCodes.ERR__STATE });
    }

    const rdKafkaConfig = this.#config();
    this.#state = ConsumerState.CONNECTING;
    rdKafkaConfig.queue_non_empty_cb = this.#queueNonEmptyCb.bind(this);
    this.#internalClient = new RdKafka.KafkaConsumer(rdKafkaConfig);
    this.#internalClient.on('ready', this.#readyCb.bind(this));
    this.#internalClient.on('error', this.#errorCb.bind(this));
    this.#internalClient.on('event.error', this.#errorCb.bind(this));
    this.#internalClient.on('event.log', (msg) => loggerTrampoline(msg, this.#logger));

    return new Promise((resolve, reject) => {
      this.#connectPromiseFunc = { resolve, reject };
      this.#internalClient.connect(null, (err) => {
        if (err) {
          this.#state = ConsumerState.DISCONNECTED;
          const rejectionError = this.#connectionError ? this.#connectionError : err;
          reject(createKafkaJsErrorFromLibRdKafkaError(rejectionError));
        }
      });
    });
  }

  /**
   * Subscribes the consumer to the given topics.
   * @param {object} subscription - An object containing the topic(s) to subscribe to - one of `topic` or `topics` must be present.
   * @param {string?} subscription.topic - The topic to subscribe to.
   * @param {Array<string>?} subscription.topics - The topics to subscribe to.
   * @param {boolean?} subscription.replace - Whether to replace the existing subscription, or to add to it. Adds by default.
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
          throw new error.KafkaJSError(CompatibilityErrorMessages.subscribeOptionsRegexStart(), { code: error.ErrorCodes.ERR__INVALID_ARG });

        return regexSource;
      } else {
        throw new error.KafkaJSError('Invalid topic ' + topic + ' (' + typeof topic + '), the topic name has to be a String or a RegExp', { code: error.ErrorCodes.ERR__INVALID_ARG });
      }
    });

    this.#storedSubscriptions = subscription.replace ? topics : this.#storedSubscriptions.concat(topics);
    this.#logger.debug(`${subscription.replace ? 'Replacing' : 'Adding'} topics [${topics.join(', ')}] to subscription`, this.#createConsumerBindingMessageMetadata());
    this.#internalClient.subscribe(this.#storedSubscriptions);
  }

  async stop() {
    notImplemented();
  }

  /**
   * Starts consumer polling. This method returns immediately.
   * @param {object} config - The configuration for running the consumer.
   * @param {function?} config.eachMessage - The function to call for processing each message.
   * @param {function?} config.eachBatch - The function to call for processing each batch of messages - can only be set if eachMessage is not set.
   * @param {boolean?} config.eachBatchAutoResolve - Whether to automatically resolve offsets for each batch (only applicable if eachBatch is set, true by default).
   * @param {number?} config.partitionsConsumedConcurrently - The limit to the number of partitions consumed concurrently (1 by default).
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

    if (this.#running) {
      throw new error.KafkaJSError('Consumer is already running.', { code: error.ErrorCodes.ERR__STATE });
    }
    this.#running = true;

    /* We're going to add keys to the configuration, so make a copy */
    const configCopy = Object.assign({}, config);

    /* Batches are auto resolved by default. */
    if (!Object.hasOwn(config, 'eachBatchAutoResolve')) {
      configCopy.eachBatchAutoResolve = true;
    }

    if (!Object.hasOwn(config, 'partitionsConsumedConcurrently')) {
      configCopy.partitionsConsumedConcurrently = 1;
    }

    this.#messageCache = new MessageCache(this.#logger);
    /* We deliberately don't await this because we want to return from this method immediately. */
    this.#runInternal(configCopy);
  }

  /**
   * Processes a single message.
   *
   * @param m Message as obtained from #consumeSingleCached.
   * @param config Config as passed to run().
   * @returns {Promise<number>} The cache index of the message that was processed.
   * @private
   */
  async #messageProcessor(m, config) {
    let ppc;
    [m, ppc] = m;
    let key = partitionKey(m);
    let eachMessageProcessed = false;
    const payload = this.#createPayload(m);

    try {
      this.#lastConsumedOffsets.set(key, m);
      await config.eachMessage(payload);
      eachMessageProcessed = true;
    } catch (e) {
      /* It's not only possible, but expected that an error will be thrown by eachMessage.
       * This is especially true since the pattern of pause() followed by throwing an error
       * is encouraged. To meet the API contract, we seek one offset backward (which
       * means seeking to the message offset).
       * However, we don't do this inside the catch, but just outside it. This is because throwing an
       * error is not the only case where we might want to seek back.
       *
       * So - do nothing but a log, but at this point eachMessageProcessed is false.
       * TODO: log error only if error type is not KafkaJSError and if no pause() has been called, else log debug.
       */
      this.#logger.error(
        `Consumer encountered error while processing message. Error details: ${e}: ${e.stack}. The same message may be reprocessed.`,
        this.#createConsumerBindingMessageMetadata());
    }

    /* If the message is unprocessed, due to an error, or because the user has not resolved it, we seek back. */
    if (!eachMessageProcessed) {
      this.seek({
        topic: m.topic,
        partition: m.partition,
        offset: m.offset,
        leaderEpoch: m.leaderEpoch,
      });
    }

    /* Store the offsets we need to store, or at least record them for cache invalidation reasons. */
    if (eachMessageProcessed) {
      try {
        this.#internalClient._offsetsStoreSingle(m.topic, m.partition, Number(m.offset) + 1, m.leaderEpoch);
      } catch (e) {
        /* Not much we can do, except log the error. */
        this.#logger.error(`Consumer encountered error while storing offset. Error details: ${JSON.stringify(e)}`, this.#createConsumerBindingMessageMetadata());
      }
    }


    return ppc;
  }

  /**
   * Processes a batch of messages.
   *
   * @param {object} ms Messages as obtained from #consumeCachedN (ms.length !== 0).
   *                    This is of the form [Message[], PerPartitionCache].
   * @param config Config as passed to run().
   * @returns {Promise<PerPartitionCache>} the PPC corresponding to
   *                                       the passed batch.
   * @private
   */
  async #batchProcessor(ms, config) {
    let ppc;
    [ms, ppc] = ms;
    const key = partitionKey(ms[0]);
    const payload = this.#createBatchPayload(ms);

    this.#topicPartitionToBatchPayload.set(key, payload);

    let lastOffsetProcessed = { offset: -1, leaderEpoch: -1 };
    const firstMessage = ms[0];
    const lastMessage = ms[ms.length - 1];
    const lastOffset = +(lastMessage.offset);
    const lastLeaderEpoch = lastMessage.leaderEpoch;
    try {
      await config.eachBatch(payload);

      /* If the user isn't resolving offsets, we resolve them here. It's significant here to call this method
       * because besides updating `payload._lastResolvedOffset`, this method is also storing the offsets to
       * librdkafka, and accounting for any cache invalidations.
       * Don't bother resolving offsets if payload became stale at some point. We can't know when the payload
       * became stale, so either the user has been nice enough to keep resolving messages, or we must seek to
       * the first offset to ensure no message loss. */
      if (config.eachBatchAutoResolve && !payload._stale) {
        payload.resolveOffset(lastOffset, lastLeaderEpoch);
      }

      lastOffsetProcessed = payload._lastResolvedOffset;
    } catch (e) {
      /* It's not only possible, but expected that an error will be thrown by eachBatch.
       * This is especially true since the pattern of pause() followed by throwing an error
       * is encouraged. To meet the API contract, we seek one offset backward (which
       * means seeking to the message offset).
       * However, we don't do this inside the catch, but just outside it. This is because throwing an
       * error is not the only case where we might want to seek back. We might want to seek back
       * if the user has not called `resolveOffset` manually in case of using eachBatch without
       * eachBatchAutoResolve being set.
       *
       * So - do nothing but a log, but at this point eachMessageProcessed needs to be false unless
       * the user has explicitly marked it as true.
       * TODO: log error only if error type is not KafkaJSError and if no pause() has been called, else log debug.
       */
      this.#logger.error(
        `Consumer encountered error while processing message. Error details: ${e}: ${e.stack}. The same message may be reprocessed.`,
        this.#createConsumerBindingMessageMetadata());

      /* The value of eachBatchAutoResolve is not important. The only place where a message is marked processed
       * despite an error is if the user says so, and the user can use resolveOffset for both the possible
       * values eachBatchAutoResolve can take. */
      lastOffsetProcessed = payload._lastResolvedOffset;
    }

    this.#topicPartitionToBatchPayload.delete(key);

    /* If any message is unprocessed, either due to an error or due to the user not marking it processed, we must seek
     * back to get it so it can be reprocessed. */
    if (lastOffsetProcessed.offset !== lastOffset) {
      const offsetToSeekTo = lastOffsetProcessed.offset === -1 ? firstMessage.offset : (lastOffsetProcessed.offset + 1);
      const leaderEpoch = lastOffsetProcessed.offset === -1 ? firstMessage.leaderEpoch : lastOffsetProcessed.leaderEpoch;
      this.seek({
        topic: firstMessage.topic,
        partition: firstMessage.partition,
        offset: offsetToSeekTo,
        leaderEpoch: leaderEpoch,
      });
    }

    return ppc;
  }

  #discardMessages(ms, ppc) {
    if (ms) {
      let m = ms[0];
      if (m.constructor === Array) {
        m = m[0];
      }
      ppc = ms[1];
      if (m && !this.#lastConsumedOffsets.has(ppc.key)) {
        this.#lastConsumedOffsets.set(ppc.key, {
          topic: m.topic,
          partition: m.partition,
          offset: m.offset - 1,
        });
      }
    }
    return ppc;
  }

  #queueNonEmptyCb() {
    for (const waiter of this.#queueWaiters) {
      waiter.resolve();
    }
  }

  async #nextFetchRetry() {
    if (this.#fetchInProgress) {
      await this.#fetchInProgress;
    } else {
      /* Backoff a little. If m is null, we might be without messages
       * or in available partition starvation, and calling consumeSingleCached
       * in a tight loop will help no one.
       * In case there is any message in the queue, we'll be woken up before the
       * timer expires.
       * We have a per-worker promise, otherwise we end up awakening
       * other workers when they've already looped and just restarted awaiting.
       * The `Promise` passed to `Timer.withTimeout` cannot be reused
       * in next call to this method, to avoid memory leaks caused
       * by `Promise.race`. */
      const waiter = new DeferredPromise();
      const waiterNode = this.#queueWaiters.addLast(waiter);
      await Timer.withTimeout(1000, waiter);

      /* Resolves the "extra" promise that has been spawned when creating the timer. */
      waiter.resolve();
      this.#queueWaiters.remove(waiterNode);
    }
  }

  /**
   * Starts a worker to fetch messages/batches from the internal consumer and process them.
   *
   * A worker runs until it's told to stop.
   * Conditions where the worker is told to stop:
   *  1. Cache globally stale
   *  2. Disconnected initiated
   *  3. Rebalance
   *  4. Some other worker has started terminating.
   *
   * Worker termination acts as a async barrier.
   * @private
   */
  async #worker(config, perMessageProcessor, fetcher) {
    let ppc = null;

    while (!this.#workerTerminationScheduled.resolved) {

      const ms = await fetcher(ppc).catch(e => {
        /* Since this error cannot be exposed to the user in the current situation, just log and retry.
          * This is due to restartOnFailure being set to always true. */
        if (this.#logger)
          this.#logger.error(`Consumer encountered error while consuming. Retrying. Error details: ${e} : ${e.stack}`, this.#createConsumerBindingMessageMetadata());
      });

      if (this.#pendingOperations.length) {
        ppc  = this.#discardMessages(ms, ppc);
        break;
      }

      if (!ms) {
        await this.#nextFetchRetry();
        continue;
      }

      ppc = await perMessageProcessor(ms, config);
    }

    if (ppc)
      this.#messageCache.return(ppc);
  }

  async #checkMaxPollIntervalNotExceeded(now) {
    const maxPollExpiration = this.#lastFetchClockNs +
      BigInt((this.#cacheExpirationTimeoutMs + this.#maxPollIntervalMs)
              * 1e6);

    let interval = Number(maxPollExpiration - now) / 1e6;
    if (interval < 1)
      interval = 1;
    await Timer.withTimeout(interval,
      this.#maxPollIntervalRestart);
    now = hrtime.bigint();

    if (now > (maxPollExpiration - 1000000n)) {
      this.#markBatchPayloadsStale(this.assignment());
    }
  }

  /**
   * Clears the cache and resets the positions when
   * the internal client hasn't been polled for more than
   * max poll interval since the last fetch.
   * After that it waits until barrier is reached or
   * max poll interval is reached. In the latter case it
   * marks the batch payloads as stale.
   * @private
   */
  async #cacheExpirationLoop() {
    while (!this.#workerTerminationScheduled.resolved) {
      let now = hrtime.bigint();
      const cacheExpiration = this.#lastFetchClockNs +
        BigInt(this.#cacheExpirationTimeoutMs * 1e6);

      if (now > cacheExpiration) {
        this.#addPendingOperation(() =>
          this.#clearCacheAndResetPositions());
        await this.#checkMaxPollIntervalNotExceeded(now);
        break;
      }

      let interval = Number(cacheExpiration - now) / 1e6;
      if (interval < 100)
        interval = 100;
      await Timer.withTimeout(interval, this.#maxPollIntervalRestart);
      if (this.#maxPollIntervalRestart.resolved)
        this.#maxPollIntervalRestart = new DeferredPromise();
    }
    if (this.#maxPollIntervalRestart.resolved)
      this.#maxPollIntervalRestart = new DeferredPromise();
  }

  /**
   * Executes all pending operations and clears the list.
   * @private
   */
  async #executePendingOperations() {
    for (const op of this.#pendingOperations) {
      await op();
    }
    this.#pendingOperations = [];
  }

  /**
   * Internal polling loop.
   * Spawns and awaits workers until disconnect is initiated.
   * @private
   */
  async #runInternal(config) {
    this.#concurrency = config.partitionsConsumedConcurrently;
    const perMessageProcessor = config.eachMessage ? this.#messageProcessor : this.#batchProcessor;
    /* TODO: make this dynamic, based on max batch size / size of last message seen. */
    const maxBatchSize = 32;
    const fetcher = config.eachMessage
      ? (savedIdx) => this.#consumeSingleCached(savedIdx)
      : (savedIdx) => this.#consumeCachedN(savedIdx, maxBatchSize);
    this.#workers = [];

    await this.#lock.write(async () => {

      while (!this.#disconnectStarted) {
        if (this.#maxPollIntervalRestart.resolved)
          this.#maxPollIntervalRestart = new DeferredPromise();

        this.#workerTerminationScheduled = new DeferredPromise();
        this.#lastFetchClockNs = hrtime.bigint();
        if (this.#pendingOperations.length === 0) {
          const workersToSpawn = Math.max(1, Math.min(this.#concurrency, this.#partitionCount));
          const cacheExpirationLoop = this.#cacheExpirationLoop();
          this.#logger.debug(`Spawning ${workersToSpawn} workers`, this.#createConsumerBindingMessageMetadata());
          this.#workers =
            Array(workersToSpawn)
              .fill()
              .map((_, i) =>
                this.#worker(config, perMessageProcessor.bind(this), fetcher.bind(this))
                  .catch(e => {
                    if (this.#logger)
                      this.#logger.error(`Worker ${i} encountered an error: ${e}:${e.stack}`);
                  }));

          /* Best we can do is log errors on worker issues - handled by the catch block above. */
          await Promise.allSettled(this.#workers);
          this.#maxPollIntervalRestart.resolve();
          await cacheExpirationLoop;
        }

        await this.#executePendingOperations();
      }

    });
    this.#maxPollIntervalRestart.resolve();
  }

  /**
   * Consumes a single message from the consumer within the given timeout.
   * THIS METHOD IS NOT IMPLEMENTED.
   * @note This method cannot be used with run(). Either that, or this must be used.
   *
   * @param {any} args
   * @param {number} args.timeout - the timeout in milliseconds, defaults to 1000.
   * @returns {import("../..").Message|null} a message, or null if the timeout was reached.
   * @private
   */
  async consume({ timeout } = { timeout: 1000 }) {
    if (this.#state !== ConsumerState.CONNECTED) {
      throw new error.KafkaJSError('consume can only be called while connected.', { code: error.ErrorCodes.ERR__STATE });
    }

    if (this.#running) {
      throw new error.KafkaJSError('consume() and run() cannot be used together.', { code: error.ErrorCodes.ERR__CONFLICT });
    }

    this.#internalClient.setDefaultConsumeTimeout(timeout);
    let m = null;

    try {
      const ms = await this.#consumeN(1);
      m = ms[0];
    } finally {
      this.#internalClient.setDefaultConsumeTimeout(undefined);
    }

    throw new error.KafkaJSError('consume() is not implemented.' + m, { code: error.ErrorCodes.ERR__NOT_IMPLEMENTED });
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
   * @param {Array<{topic: string, partition: number, offset: string, leaderEpoch: number|null, metadata: string|null}>?} topicPartitions
   * @returns {Promise<void>} A promise that resolves when the offsets have been committed.
   */
  async commitOffsets(topicPartitions = null) {
    if (this.#state !== ConsumerState.CONNECTED) {
      throw new error.KafkaJSError('Commit can only be called while connected.', { code: error.ErrorCodes.ERR__STATE });
    }

    return new Promise((resolve, reject) => {
      try {
        let cb = (e) => {
          if (e)
            reject(createKafkaJsErrorFromLibRdKafkaError(e));
          else
            resolve();
        };

        if (topicPartitions)
          topicPartitions = topicPartitions.map(topicPartitionOffsetMetadataToRdKafka);
        else
          topicPartitions = null;
        this.#internalClient.commitCb(topicPartitions, cb);
      } catch (e) {
        if (!e.code || e.code !== error.ErrorCodes.ERR__NO_OFFSET)
          reject(createKafkaJsErrorFromLibRdKafkaError(e));
        else
          resolve();
      }
    });
  }

  /**
   * Fetch committed offsets for the given topic partitions.
   *
   * @param {Array<{topic: string, partition: number}>?} topicPartitions -
   *        The topic partitions to check for committed offsets. Defaults to all assigned partitions.
   * @param {number} timeout - Timeout in ms. Defaults to infinite (-1).
   * @returns {Promise<Array<{topic: string, partition: number, offset: string, leaderEpoch: number|null, metadata: string|null}>>} A promise that resolves to the committed offsets.
   */
  async committed(topicPartitions = null, timeout = -1) {
    if (this.#state !== ConsumerState.CONNECTED) {
      throw new error.KafkaJSError('Committed can only be called while connected.', { code: error.ErrorCodes.ERR__STATE });
    }

    if (!topicPartitions) {
      topicPartitions = this.assignment();
    }

    const topicPartitionsRdKafka = topicPartitions.map(
      topicPartitionOffsetToRdKafka);

    return new Promise((resolve, reject) => {
      this.#internalClient.committed(topicPartitionsRdKafka, timeout, (err, offsets) => {
        if (err) {
          reject(createKafkaJsErrorFromLibRdKafkaError(err));
          return;
        }
        resolve(offsets.map(topicPartitionOffsetMetadataToKafkaJS));
      });
    });
  }

  /**
   * Apply pending seeks to topic partitions we have just obtained as a result of a rebalance.
   * @param {Array<{topic: string, partition: number}>} assignment The list of topic partitions to check for pending seeks.
   * @returns {Array<{topic: string, partition: number, offset: number}>} The new assignment with the offsets seeked to, which can be passed to assign().
   * @private
   */
  #assignAsPerSeekedOffsets(assignment) {
    for (let i = 0; i < assignment.length; i++) {
      const topicPartition = assignment[i];
      const key = partitionKey(topicPartition);
      if (!this.#pendingSeeks.has(key))
        continue;

      const tpo = this.#pendingSeeks.get(key);
      this.#pendingSeeks.delete(key);

      assignment[i].offset = tpo.offset;
      assignment[i].leaderEpoch = tpo.leaderEpoch;
    }
    return assignment;
  }

  #addPendingOperation(fun) {
    if (this.#pendingOperations.length === 0) {
      this.#workerTerminationScheduled.resolve();
    }
    this.#pendingOperations.push(fun);
  }

  async #seekInternal(topicPartitionOffsets) {
    if (topicPartitionOffsets.length === 0) {
      return;
    }

    // Uncomment to test an additional delay in seek
    // await Timer.withTimeout(1000);

    const seekedPartitions = [];
    const pendingSeeks = new Map();
    const assignmentSet = new Set();
    for (const topicPartitionOffset of topicPartitionOffsets) {
      const key = partitionKey(topicPartitionOffset);
      pendingSeeks.set(key, topicPartitionOffset);
    }

    const assignment = this.assignment();
    for (const topicPartition of assignment) {
      const key = partitionKey(topicPartition);
      assignmentSet.add(key);
      if (!pendingSeeks.has(key))
        continue;
      seekedPartitions.push([key, pendingSeeks.get(key)]);
    }

    for (const topicPartitionOffset of topicPartitionOffsets) {
      const key = partitionKey(topicPartitionOffset);
      if (!assignmentSet.has(key))
        this.#pendingSeeks.set(key, topicPartitionOffset);
    }

    const offsetsToCommit = [];
    const librdkafkaSeekPromises = [];
    for (const [key, topicPartitionOffset] of seekedPartitions) {
      this.#lastConsumedOffsets.delete(key);
      this.#messageCache.markStale([topicPartitionOffset]);
      offsetsToCommit.push(topicPartitionOffset);

      const librdkafkaSeekPromise = new DeferredPromise();
      this.#internalClient.seek(topicPartitionOffset, 1000,
        (err) => {
          if (err)
            this.#logger.error(`Error while calling seek from within seekInternal: ${err}`, this.#createConsumerBindingMessageMetadata());
          librdkafkaSeekPromise.resolve();
        });
      librdkafkaSeekPromises.push(librdkafkaSeekPromise);
    }
    await Promise.allSettled(librdkafkaSeekPromises);
    await Promise.all(librdkafkaSeekPromises);

    for (const [key, ] of seekedPartitions) {
      this.#pendingSeeks.delete(key);
    }

    /* Offsets are committed on seek only when in compatibility mode. */
    if (offsetsToCommit.length !== 0 && this.#internalConfig['enable.auto.commit']) {
      await this.#commitOffsetsUntilNoStateErr(offsetsToCommit);
    }
  }

  #markBatchPayloadsStale(topicPartitions) {
    for (const topicPartition of topicPartitions) {
      const key = partitionKey(topicPartition);
      if (this.#topicPartitionToBatchPayload.has(key))
        this.#topicPartitionToBatchPayload.get(key)._stale = true;
    }
  }

  async #pauseInternal(topicPartitions) {
    // Uncomment to test future async pause
    // await Timer.withTimeout(1000);

    this.#messageCache.markStale(topicPartitions);
    this.#internalClient.pause(topicPartitions);

    const seekOffsets = [];
    for (let topicPartition of topicPartitions) {
      const key = partitionKey(topicPartition);
      if (this.#lastConsumedOffsets.has(key)) {
        const seekOffset = this.#lastConsumedOffsets.get(key);
        const topicPartitionOffset = {
          topic: topicPartition.topic,
          partition: topicPartition.partition,
          offset: seekOffset.offset + 1,
          leaderEpoch: seekOffset.leaderEpoch,
        };
        seekOffsets.push(topicPartitionOffset);
      }
    }
    if (seekOffsets.length) {
      await this.#seekInternal(seekOffsets, false);
    }
  }

  async #resumeInternal(topicPartitions) {
    // Uncomment to test future async resume
    // await Timer.withTimeout(1000);
    this.#internalClient.resume(topicPartitions);
  }

  /**
   * Seek to the given offset for a topic partition.
   *
   * This method is completely asynchronous, and does not wait for the seek to complete.
   * In case any partitions that are seeked to, are not a part of the current assignment, they are stored internally.
   *
   * If at any later time, the consumer is assigned the partition, as a part of a rebalance,
   * the pending seek will be performed.
   *
   * Additionally, if the librdkafka property 'enable.auto.commit' or kafkaJS.autoCommit is true,
   * the consumer will commit the offset seeked.
   *
   * @param {object} topicPartitionOffset
   * @param {string} topicPartitionOffset.topic
   * @param {number} topicPartitionOffset.partition
   * @param {string} topicPartitionOffset.offset - The offset to seek to.
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

    /* If anyone's using eachBatch, mark the batch as stale. */
    this.#markBatchPayloadsStale([rdKafkaTopicPartitionOffset]);

    this.#addPendingOperation(() =>
        this.#seekInternal([rdKafkaTopicPartitionOffset]));
  }

  async describeGroup() {
    notImplemented();
  }

  /**
   * Find the assigned topic partitions for the consumer.
   * @returns {Array<{topic: string, partitions: Array<number>}>} the current assignment.
   */
  assignment() {
    if (this.#state !== ConsumerState.CONNECTED) {
      throw new error.KafkaJSError('Assignment can only be called while connected.', { code: error.ErrorCodes.ERR__STATE });
    }

    return this.#flattenTopicPartitions(this.#internalClient.assignments());
  }

  /**
   * Get the type of rebalance protocol used in the consumer group.
   *
   * @returns {string} "NONE" (if not in a group yet), "COOPERATIVE" or "EAGER".
   */
  rebalanceProtocol() {
    if (this.#state !== ConsumerState.CONNECTED) {
      return "NONE";
    }
    return this.#internalClient.rebalanceProtocol();
  }

  /**
   * Fetches all partitions of topic that are assigned to this consumer.
   * @param {string} topic
   * @returns {Array<number>} A list of partitions.
   * @private
   */
  #getAllAssignedPartition(topic) {
    return this.#internalClient.assignments()
      .filter((partition) => partition.topic === topic)
      .map((tpo) => tpo.partition);
  }

  /**
   * Pauses the given topic partitions. If partitions are not specified, pauses
   * all partitions for the given topic.
   *
   * If topic partition(s) are already paused this method has no effect.
   *
   * @param {Array<{topic: string, partitions: Array<number>|null}>} topics - Topics or topic partitions to pause.
   * @returns {function} A function that can be called to resume the topic partitions paused by this call.
   */
  pause(topics) {
    if (this.#state !== ConsumerState.CONNECTED) {
      throw new error.KafkaJSError('Pause can only be called while connected.', { code: error.ErrorCodes.ERR__STATE });
    }

    this.#logger.debug(`Pausing ${topics.length} topics`, this.#createConsumerBindingMessageMetadata());

    const toppars = [];
    for (let topic of topics) {
      if (typeof topic.topic !== 'string') {
        throw new error.KafkaJSError('Topic must be a string.', { code: error.ErrorCodes.ERR__INVALID_ARG });
      }

      const toppar = { topic: topic.topic };

      if (!topic.partitions) {
        toppar.partitions = this.#getAllAssignedPartition(topic.topic);
      } else {
        /* TODO: add a check here to make sure we own each partition */
        toppar.partitions = [...topic.partitions];
      }

      toppars.push(toppar);
    }

    const flattenedToppars = this.#flattenTopicPartitions(toppars);
    if (flattenedToppars.length === 0) {
      return;
    }

    /* If anyone's using eachBatch, mark the batch as stale. */
    this.#markBatchPayloadsStale(flattenedToppars);

    flattenedToppars.forEach(
      topicPartition => this.#pausedPartitions.set(
        partitionKey(topicPartition),
        topicPartition));

    this.#addPendingOperation(() =>
        this.#pauseInternal(flattenedToppars));

    /* Note: we don't use flattenedToppars here because resume flattens them again. */
    return () => this.resume(toppars);
  }

  /**
   * Returns the list of paused topic partitions.
   * @returns {Array<{topic: string, partitions: Array<number>}>} A list of paused topic partitions.
   */
  paused() {
    const topicToPartitions = Array
      .from(this.#pausedPartitions.values())
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
   * all partitions for the given topic.
   *
   * If topic partition(s) are already resumed this method has no effect.
   * @param {Array<{topic: string, partitions: Array<number>|null}>} topics - Topics or topic partitions to resume.
   */
  resume(topics) {
    if (this.#state !== ConsumerState.CONNECTED) {
      throw new error.KafkaJSError('Resume can only be called while connected.', { code: error.ErrorCodes.ERR__STATE });
    }

    this.#logger.debug(`Resuming ${topics.length} topics`, this.#createConsumerBindingMessageMetadata());

    const toppars = [];
    for (let topic of topics) {
      if (typeof topic.topic !== 'string') {
        throw new error.KafkaJSError('Topic must be a string.', { code: error.ErrorCodes.ERR__INVALID_ARG });
      }
      const toppar = { topic: topic.topic };

      if (!topic.partitions) {
        toppar.partitions = this.#getAllAssignedPartition(topic.topic);
      } else {
        toppar.partitions = [...topic.partitions];
      }

      toppars.push(toppar);
    }

    const flattenedToppars = this.#flattenTopicPartitions(toppars);
    if (flattenedToppars.length === 0) {
      return;
    }
    flattenedToppars.map(partitionKey).
      forEach(key => this.#pausedPartitions.delete(key));

    this.#addPendingOperation(() =>
      this.#resumeInternal(flattenedToppars));
  }

  on(/* eventName, listener */) {
    notImplemented();
  }

  /**
   * Get the logger associated to this consumer instance.
   *
   * @example
   * const logger = consumer.logger();
   * logger.info('Hello world');
   * logger.setLogLevel(logLevel.ERROR);
   * @see {@link KafkaJS.logLevel} for available log levels
   * @returns {Object} The logger instance.
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
   *
   * Warning: This cannot be called from within `eachMessage` or `eachBatch` callback of
   * [Consumer.run]{@link KafkaJS.Consumer#run}.
   * @returns {Promise<void>} A promise that resolves when the consumer has disconnected.
   */
  async disconnect() {
    /* Not yet connected - no error. */
    if (this.#state === ConsumerState.INIT) {
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

    this.#disconnectStarted = true;
    this.#workerTerminationScheduled.resolve();
    this.#logger.debug("Signalling disconnection attempt to workers", this.#createConsumerBindingMessageMetadata());
    await this.#lock.write(async () => {

      this.#state = ConsumerState.DISCONNECTING;

    });

    await new Promise((resolve, reject) => {
      const cb = (err) => {
        if (err) {
          reject(createKafkaJsErrorFromLibRdKafkaError(err));
          return;
        }
        this.#state = ConsumerState.DISCONNECTED;
        this.#logger.info("Consumer disconnected", this.#createConsumerBindingMessageMetadata());
        resolve();
      };
      this.#internalClient.unsubscribe();
      this.#internalClient.disconnect(cb);
    });
  }
}

module.exports = { Consumer, PartitionAssigners: Object.freeze(PartitionAssigners), };
