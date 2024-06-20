const LibrdKafkaError = require('../error');
const error = require('./_error');
const RdKafka = require('../rdkafka');
const {
  kafkaJSToRdKafkaConfig,
  topicPartitionOffsetToRdKafka,
  topicPartitionOffsetMetadataToRdKafka,
  topicPartitionOffsetMetadataToKafkaJS,
  createKafkaJsErrorFromLibRdKafkaError,
  notImplemented,
  loggerTrampoline,
  DefaultLogger,
  CompatibilityErrorMessages,
  severityToLogLevel,
  checkAllowedKeys,
  logLevel,
  Lock,
  acquireOrLog,
  partitionKey,
} = require('./_common');
const { Buffer } = require('buffer');
const MessageCache = require('./_consumer_cache');

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
   * A map of topic+partition to the offset that was last consumed.
   * The keys are of the type "<topic>|<partition>".
   * @type {Map<string, number>}
   */
  #lastConsumedOffsets = new Map();

  /**
   * A lock for consuming and disconnecting.
   * This lock should be held whenever we want to change the state from CONNECTED to any state other than CONNECTED.
   * In practical terms, this lock is held whenever we're consuming a message, or disconnecting.
   * We set the timeout to 5 seconds, after which we log an error, but keep trying to acquire the lock.
   * @type {Lock}
   */
  #lock = new Lock({ timeout: 5000 });

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
   * Whether the user has enabled manual offset management (stores).
   */
  #userManagedStores = false;

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
  #workerTerminationScheduled = false;

  /**
   * The worker functions currently running in the consumer.
   */
  #workers = [];

  /**
   * The number of partitions to consume concurrently as set by the user, or 1.
   */
  #concurrency = 1;

  /**
   * Whether any call to the internalClient's consume() method is in progress.
   */
  #fetchInProgress = false;

  /**
   * TODO: remove this or make it a bit more reliable.
   * This is a debug property for this branch.
   */
  clientId = null;

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
   * Clear the message cache, and reset to stored positions.
   *
   * @param {Array<{topic: string, partition: number}>|null} topicPartitions to clear the cache for, if null, then clear all assigned.
   */
  async #clearCacheAndResetPositions(topicPartitions = null) {
    /* Seek to stored offset for each topic partition. It's possible that we've
     * consumed messages upto N from the internalClient, but the user has stale'd the cache
     * after consuming just k (< N) messages. We seek to k+1. */

    const clearPartitions = topicPartitions ? topicPartitions : this.assignment();
    const seekPromises = [];
    for (const topicPartitionOffset of clearPartitions) {
      const key = partitionKey(topicPartitionOffset);
      if (!this.#lastConsumedOffsets.has(key))
        continue;

      /* Fire off a seek */
      const seekPromise = new Promise((resolve, reject) => {
        this.#internalClient.seek({
          topic: topicPartitionOffset.topic,
          partition: topicPartitionOffset.partition,
          offset: +this.#lastConsumedOffsets.get(key)
        }, 10000, err => {
          if (err) {
            reject(err);
          } else {
            resolve();
          }
        });

        this.#lastConsumedOffsets.delete(key);
      });
      seekPromises.push(seekPromise);
    }

    /* TODO: we should cry more about this and render the consumer unusable. */
    await Promise.all(seekPromises).catch(err => this.#logger.error("Seek error. This is effectively a fatal error:" + err));


    /* Clear the cache and stored offsets.
     * We need to do this only if topicPartitions = null (global cache expiry).
     * This is because in case of a local cache expiry, MessageCache handles
     * skipping that (and clearing that later before getting new messages). */
    if (!topicPartitions) {
      this.#messageCache.clear();
    }
  }

  /**
   * Used as a trampoline to the user's rebalance listener, if any.
   * @param {Error} err - error in rebalance
   * @param {import("../../types").TopicPartition[]} assignment
   */
  #rebalanceCallback(err, assignment) {
    err = LibrdKafkaError.create(err);
    const userSpecifiedRebalanceCb = this.#userConfig['rebalance_cb'];

    let call;

    /* Since we don't expose assign() or incremental_assign() methods, we allow the user
     * to modify the assignment by returning it. If a truthy value is returned, we use that
     * and do not apply any pending seeks to it either. */
    let assignmentModified = false;
    if (typeof userSpecifiedRebalanceCb === 'function') {
      call = new Promise((resolve, reject) => {
        try {
          const alternateAssignment = userSpecifiedRebalanceCb(err, assignment);
          if (alternateAssignment) {
            assignment = alternateAssignment;
            assignmentModified = true;
          }
          resolve();
        } catch (e) {
          reject(e);
        }
      });
    } else {
      switch (err.code) {
        // TODO: is this the right way to handle this error?
        // We might just be able to throw, because the error is something the user has caused.
        case LibrdKafkaError.codes.ERR__ASSIGN_PARTITIONS:
          call = (this.#userConfig.rebalanceListener.onPartitionsAssigned ?
            this.#userConfig.rebalanceListener.onPartitionsAssigned(assignment) :
            Promise.resolve()).catch(e => this.#logger.error(e));
          break;
        case LibrdKafkaError.codes.ERR__REVOKE_PARTITIONS:
          call = (this.#userConfig.rebalanceListener.onPartitionsRevoked ?
            this.#userConfig.rebalanceListener.onPartitionsRevoked(assignment) :
            Promise.resolve()).catch(e => this.#logger.error(e));
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
            if (checkPendingSeeks && !assignmentModified)
              assignment = this.#assignAsPerSeekedOffsets(assignment);

            if (this.#internalClient.rebalanceProtocol() === "EAGER") {
              this.#internalClient.assign(assignment);
              this.#partitionCount = assignment.length;
            } else {
              this.#internalClient.incrementalAssign(assignment);
              this.#partitionCount += assignment.length;
            }

            if (checkPendingSeeks) {
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

            // Populate per-partion caches.
            // For cooperative sticky, just add the newly recieved partitions.
            // If it's eager, it's already empty, so we can add all the partitions.
            this.#messageCache.addTopicPartitions(assignment);

          } else {
            if (this.#internalClient.rebalanceProtocol() === "EAGER") {
              this.#internalClient.unassign();
              this.#messageCache.removeTopicPartitions();
              this.#partitionCount = 0;
            } else {
              this.#internalClient.incrementalUnassign(assignment);
              this.#messageCache.removeTopicPartitions(assignment);
              this.#partitionCount -= assignment.length;
            }
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
          this.#workerTerminationScheduled = true;
          /* We don't need to await the workers here. We are OK if the termination and respawning
           * occurs later, since even if we have a few more or few less workers for a while, it's
           * not a big deal. */
        }
      });
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
    this.clientId = rdKafkaConfig['client.id'];
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
    } else {
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

    /* Set the logger's level in case we're not in compatibility mode - just set it to DEBUG, the broadest
     * log level, as librdkafka will control the granularity. */
    if (!compatibleConfig || Object.keys(compatibleConfig).length === 0) {
      this.#logger.setLogLevel(logLevel.DEBUG);
    }

    /* Even if we are in compability mode, setting a 'debug' in the main config must override the logger's level. */
    if (Object.hasOwn(this.#userConfig, 'debug')) {
      this.#logger.setLogLevel(logLevel.DEBUG);
    }

    let rdKafkaConfig = Object.assign(compatibleConfig, this.#userConfig);

    /* Delete properties which are already processed, or cannot be passed to node-rdkafka */
    delete rdKafkaConfig.kafkaJS;
    delete rdKafkaConfig.rebalanceListener;

    /* Certain properties that the user has set are overridden. We use trampolines to accommodate the user's callbacks.
     * TODO: add trampoline method for offset commit callback. */
    rdKafkaConfig['offset_commit_cb'] = true;

    if (!Object.hasOwn(this.#userConfig, 'rebalanceListener')) {
      /* We might want to do certain things to maintain internal state in rebalance listener, so we need to set it to an empty object. */
      this.#userConfig.rebalanceListener = {};
    }
    rdKafkaConfig['rebalance_cb'] = this.#rebalanceCallback.bind(this);

    /* Offset management is different from case to case.
     * Case 1: User has changed value of enable.auto.offset.store. In this case, we respect that.
     * Case 2: automatic committing is on. In this case, we turn off auto.offset.store and store offsets manually.
     *         this is necessary for cache invalidation and management, as we want to put things into the store
     *         after eachMessage is called, and not on consume itself.
     * Case 3: automatic committing is off. In this case, we turn off auto.offset.store too. Since the user might
     *         call an empty commit() and expect things to work properly (ie. the right offsets be stored).
     * All this works out a singular, simple condition.
     */
    if (!Object.hasOwn(this.#userConfig, 'enable.auto.offset.store')) {
      rdKafkaConfig['enable.auto.offset.store'] = false;
    } else {
      this.#userManagedStores = !rdKafkaConfig['enable.auto.offset.store'];
    }

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
      for (const header of message.headers) {
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
   * Converts a message returned by node-rdkafka into a message that can be used by the eachBatch callback.
   * @param {import("../..").Message} message
   * @returns {import("../../types/kafkajs").EachBatchPayload}
   * @note Unlike the KafkaJS consumer, a batch here is for API compatibility only. It is always a single message.
   */
  #createBatchPayload(message) {
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

    const batch = {
      topic: message.topic,
      partition: message.partition,
      highWatermark: '-1001', // Invalid - we don't fetch it
      messages: [messageConverted],
      isEmpty: () => false,
      firstOffset: () => messageConverted.offset,
      lastOffset: () => messageConverted.offset,
      offsetLag: () => notImplemented(),
      offsetLagLow: () => notImplemented(),
    };

    const returnPayload = {
      batch,
      _messageResolved: false,
      resolveOffset: () => { returnPayload._messageResolved = true; },
      heartbeat: async () => { /* no op */ },
      pause: this.pause.bind(this, [{ topic: message.topic, partitions: [message.partition] }]),
      commitOffsetsIfNecessary: async () => { /* no op */ },
      uncommittedOffsets: () => notImplemented(),
      isRunning: () => this.#running,
      isStale: () => false,
    };

    return returnPayload;
  }

  /**
   * Consumes a single message from the internal consumer.
   * @param {number} savedIndex - the index of the message in the cache to return.
   * @returns {Promise<import("../..").Message>} a promise that resolves to a single message.
   * @note this method caches messages as well, but returns only a single message.
   */
  async #consumeSingleCached(savedIndex) {
    const msg = this.#messageCache.next(savedIndex);
    if (msg) {
      return msg;
    }

    /* It's possible that we get msg = null, but that's because partitionConcurrency
     * exceeds the number of partitions containing messages. So in this case,
     * we should not call for new fetches, rather, try to focus on what we have left.
     */
    if (!msg && this.#messageCache.pendingSize() !== 0) {
      return null;
    }

    if (this.#fetchInProgress) {
      return null;
    }

    this.#fetchInProgress = true;
    return new Promise((resolve, reject) => {
      this.#internalClient.consume(this.#messageCache.maxSize, (err, messages) => {
        this.#fetchInProgress = false;
        if (err) {
          reject(createKafkaJsErrorFromLibRdKafkaError(err));
          return;
        }
        this.#messageCache.addMessages(messages);
        const message = this.#messageCache.next();
        if (messages.length === this.#messageCache.maxSize) {
          this.#messageCache.increaseMaxSize();
        } else {
          this.#messageCache.decreaseMaxSize(messages.length);
        }
        resolve(message);
      });
    });
  }

  /**
   * Consumes n messages from the internal consumer.
   * @returns {Promise<import("../..").Message[]>} a promise that resolves to a list of messages.
   *                                               The size of this list is guaranteed to be less
   *                                               than or equal to n.
   * @note this method cannot be used in conjunction with #consumeSingleCached.
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
   * @param {({topic: string, partitions: number[]}|{topic: string, partition: number})[]} topics
   * @returns {import("../../types/rdkafka").TopicPartition[]} a list of (topic, partition).
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
    this.#internalClient.on('error', this.#errorCb.bind(this));
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

    if (this.#running) {
      throw new error.KafkaJSError('Consumer is already running.', { code: error.ErrorCodes.ERR__STATE });
    }
    this.#running = true;

    /* Batches are auto resolved by default. */
    if (!Object.hasOwn(config, 'eachBatchAutoResolve')) {
      config.eachBatchAutoResolve = true;
    }

    if (!Object.hasOwn(config, 'partitionsConsumedConcurrently')) {
      config.partitionsConsumedConcurrently = 1;
    }

    const rdKafkaConfig = this.#config();
    const maxPollInterval = rdKafkaConfig['max.poll.interval.ms'] ?? 300000;
    this.#messageCache = new MessageCache(Math.floor(maxPollInterval * 0.8), config.partitionsConsumedConcurrently, this.#logger);

    /* We deliberately don't await this because we want to return from this method immediately. */
    this.#runInternal(config);
  }

  /**
   * Processes a single message.
   *
   * @param m Message as obtained from #consumeSingleCached.
   * @param config Config as passed to run().
   * @returns {Promise<number>} the cache index of the message that was processed.
   */
  async #messageProcessor(m, config) {
    let eachMessageProcessed = false;
    const payload = this.#createPayload(m);

    try {
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
       * So - do nothing but a debug log, but at this point eachMessageProcessed is false.
       */
      this.#logger.debug(`Consumer encountered error while processing message. Error details: ${e}: ${e.stack}. The same message may be reprocessed.`);

      /* TODO: log error if error type is not KafkaJSError and if no pause() has been called */
      this.#logger.error(`Consumer encountered error while processing message. Error details: ${e}: ${e.stack}. The same message may be reprocessed.`);
    }

    /* If the message is unprocessed, due to an error, or because the user has not resolved it, we seek back. */
    if (!eachMessageProcessed) {
      await this.seek({
        topic: m.topic,
        partition: m.partition,
        offset: m.offset,
      });
    }

    /* Store the offsets we need to store, or at least record them for cache invalidation reasons. */
    if (eachMessageProcessed) {
      try {
        if (!this.#userManagedStores) {
          this.#internalClient.offsetsStore([{
            topic: m.topic, partition: m.partition, offset: Number(m.offset) + 1, leaderEpoch: m.leaderEpoch
          }]);
        }
        this.#lastConsumedOffsets.set(partitionKey(m), Number(m.offset) + 1);
      } catch (e) {
        /* Not much we can do, except log the error. */
        if (this.#logger)
          this.#logger.error(`Consumer encountered error while storing offset. Error details: ${JSON.stringify(e)}`);
      }
    }


    /* Force a immediate seek here. It's possible that there are no more messages to be passed to the user,
     * but the user seeked in the call to eachMessage, or else we encountered the error catch block.
     * In that case, the results of that seek will never be reflected unless we do this.
     * TOOD: this block can probably be common and not per message. */
    if (this.#checkPendingSeeks)
      await this.#seekInternal();

    return m.index;
  }

  /**
   * Processes a batch message (a single message as of now).
   *
   * @param m Message as obtained from #consumeSingleCached.
   * @param config Config as passed to run().
   * @returns {Promise<number>} the cache index of the message that was processed.
   */
  async #batchProcessor(m, config) {
    let eachMessageProcessed = false;
    const payload = this.#createBatchPayload(m);
    try {
      await config.eachBatch(payload);
      if (config.eachBatchAutoResolve) {
        eachMessageProcessed = true;
      } else {
        eachMessageProcessed = payload._messageResolved;
      }
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
       * So - do nothing but a debug log, but at this point eachMessageProcessed needs to be false unless
       * the user has explicitly marked it as true.
       */
      this.#logger.debug(`Consumer encountered error while processing message. Error details: ${e}: ${e.stack}. The same message may be reprocessed.`);

      /* TODO: log error if error type is not KafkaJSError and if no pause() has been called */
      this.#logger.error(`Consumer encountered error while processing message. Error details: ${e}: ${e.stack}. The same message may be reprocessed.`);

      /* The value of eachBatchAutoResolve is not important. The only place where a message is marked processed
       * despite an error is if the user says so, and the user can use resolveOffsets for both the possible
       * values eachBatchAutoResolve can take. */
      eachMessageProcessed = payload._messageResolved;
    }

    /* If the message is unprocessed, due to an error, or because the user has not resolved it, we seek back. */
    if (!eachMessageProcessed) {
      await this.seek({
        topic: m.topic,
        partition: m.partition,
        offset: m.offset,
      });
    }

    /* Store the offsets we need to store, or at least record them for cache invalidation reasons. */
    if (eachMessageProcessed) {
      try {
        if (!this.#userManagedStores) {
          this.#internalClient.offsetsStore([{
            topic: m.topic, partition: m.partition, offset: Number(m.offset) + 1, leaderEpoch: m.leaderEpoch
          }]);
        }
        this.#lastConsumedOffsets.set(partitionKey(m), Number(m.offset) + 1);
      } catch (e) {
        /* Not much we can do, except log the error. */
        if (this.#logger)
          this.#logger.error(`Consumer encountered error while storing offset. Error details: ${JSON.stringify(e)}`);
      }
    }

    /* Force a immediate seek here. It's possible that there are no more messages to be passed to the user,
     * but the user seeked in the call to eachMessage, or else we encountered the error catch block.
     * In that case, the results of that seek will never be reflected unless we do this. */
    if (this.#checkPendingSeeks)
      await this.#seekInternal();

    return m.index;
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
   */
  async #worker(config, perMessageProcessor, id) {
    let nextIdx = -1;
    while (!this.#workerTerminationScheduled) {
      /* Invalidate the message cache if needed */
      const locallyStale = this.#messageCache.popLocallyStale();
      if (this.#messageCache.isStale()) { /* global staleness */
        this.#workerTerminationScheduled = true;
        break;
      } else if (locallyStale.length !== 0) { /* local staleness */
        // TODO: is it correct to await some concurrent promises for eachMessage here?
        // to be safe we can do it, but I don't think we really need to do that for
        // any correctness reason.
        await this.#clearCacheAndResetPositions(locallyStale);
        continue;
      }

      const m = await this.#consumeSingleCached(nextIdx).catch(e => {
        /* Since this error cannot be exposed to the user in the current situation, just log and retry.
         * This is due to restartOnFailure being set to always true. */
        if (this.#logger)
          this.#logger.error(`Consumer encountered error while consuming. Retrying. Error details: ${e} : ${e.stack}`);
      });

      nextIdx = -1;

      if (!m) {
        /* Backoff a little. If m is null, we might be fetching from the internal consumer (fetch in progress),
         * and calling consumeSingleCached in a tight loop will help no one. */
        await new Promise((resolve) => setTimeout(resolve, 1));
        continue;
      }

      nextIdx = await perMessageProcessor(m, config);
    }

    if (nextIdx !== -1) {
      this.#messageCache.return(nextIdx);
    }
  }

  /**
   * Internal polling loop.
   * Spawns and awaits workers until disconnect is initiated.
   */
  async #runInternal(config) {
    this.#concurrency = config.partitionsConsumedConcurrently;
    const perMessageProcessor = config.eachMessage ? this.#messageProcessor : this.#batchProcessor;
    this.#workers = [];
    while (!(await acquireOrLog(this.#lock, this.#logger)));

    while (!this.#disconnectStarted) {
      this.#workerTerminationScheduled = false;
      const workersToSpawn = Math.max(1, Math.min(this.#concurrency, this.#partitionCount));
      this.#workers = Array(workersToSpawn).fill().map((_, i) => this.#worker(config, perMessageProcessor.bind(this), i));
      await Promise.all(this.#workers);

      /* One of the possible reasons for the workers to end is that the cache is globally stale.
       * We need to take care of expiring it. */
      if (this.#messageCache.isStale()) {
        await this.#clearCacheAndResetPositions();
      }
    }

    this.#lock.release();
  }

  /**
   * Consumes a single message from the consumer within the given timeout.
   * THIS METHOD IS NOT IMPLEMENTED.
   * @note This method cannot be used with run(). Either that, or this must be used.
   *
   * @param {any} args
   * @param {number} args.timeout - the timeout in milliseconds, defaults to 1000.
   * @returns {import("../..").Message|null} a message, or null if the timeout was reached.
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
    // return m ?? null;
  }

  /**
   * Store offsets for the given topic partitions.
   *
   * Stored offsets will be commited automatically at a later point if autoCommit is enabled.
   * Otherwise, they will be committed when commitOffsets is called without arguments.
   *
   * enable.auto.offset.store must be set to false to use this API.
   * @param {import("../../types/kafkajs").TopicPartitionOffset[]?} topicPartitions
   */
  storeOffsets(topicPartitions) {
    if (this.#state !== ConsumerState.CONNECTED) {
      throw new error.KafkaJSError('Store can only be called while connected.', { code: error.ErrorCodes.ERR__STATE });
    }

    if (!this.#userManagedStores) {
      throw new error.KafkaJSError(
        'Store can only be called when enable.auto.offset.store is explicitly set to false.', { code: error.ErrorCodes.ERR__INVALID_ARG });
    }

    const topicPartitionsRdKafka = topicPartitions.map(
      topicPartitionOffsetMetadataToRdKafka);
    this.#internalClient.offsetsStore(topicPartitionsRdKafka);
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
   * @param {import("../../types/kafkajs").TopicPartitionOffset[]?} topicPartitions
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
          topicPartitionOffsetMetadataToRdKafka);
        this.#internalClient.commitSync(topicPartitionsRdKafka);
      }
    } catch (e) {
      if (!e.code || e.code !== error.ErrorCodes.ERR__NO_OFFSET) {
        throw createKafkaJsErrorFromLibRdKafkaError(e);
      }
    }
  }

  /**
   * Fetch committed offsets for the given topic partitions.
   *
   * @param {import("../../types/kafkajs").TopicPartitionOffsetAndMetadata[]} topicPartitions -
   *        the topic partitions to check for committed offsets. Defaults to all assigned partitions.
   * @param {number} timeout - timeout in ms. Defaults to infinite (-1).
   * @returns {Promise<import("../../types/kafkajs").TopicPartitionOffsetAndMetadata[]>} a promise that resolves to the committed offsets.
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
   * @param {{topic: string, partition: number}[]} assignment The list of topic partitions to check for pending seeks.
   * @returns {{topic: string, partition: number, offset: number}[]} the new assignment with the offsets seeked to, which can be passed to assign().
   */
  #assignAsPerSeekedOffsets(assignment) {
    const offsetsToCommit = [];

    for (let i = 0; i < assignment.length; i++) {
      const topicPartition = assignment[i];
      const key = partitionKey(topicPartition);
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
      const key = partitionKey(topicPartition);
      if (!this.#pendingSeeks.has(key))
        continue;

      const offset = this.#pendingSeeks.get(key);
      this.#pendingSeeks.delete(key);

      const topicPartitionOffset = {
        topic: topicPartition.topic,
        partition: topicPartition.partition,
        offset
      };

      /* The ideal sequence of events here is to:
       * 1. Mark the cache as stale so we don't consume from it any further.
       * 2. Call clearCacheAndResetPositions() for the topic partition, which is supposed
       *    to be called after each cache invalidation.
       *
       * However, what (2) does is to pop lastConsumedOffsets[topic partition], and seeks to
       * the said popped value. Seeking is redundant since we seek here anyway. So, we can skip
       * the seek by just clearing the lastConsumedOffsets[topic partition].
       */
      this.#messageCache.markStale([topicPartition]);
      this.#lastConsumedOffsets.delete(key);

      /* It's assumed that topicPartition is already assigned, and thus can be seeked to and committed to.
       * Errors are logged to detect bugs in the internal code. */
      /* TODO: is it worth awaiting seeks to finish? */
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

    /* Offsets are committed on seek only when in compatibility mode. */
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
   * @returns {Promise<void>|null} a promise that resolves when the seek has been performed.
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
    this.#pendingSeeks.set(partitionKey(rdKafkaTopicPartitionOffset), rdKafkaTopicPartitionOffset.offset);
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
   * Get the type of rebalance protocol used in the consumer group.
   *
   * @returns "NONE" (if not in a group yet), "COOPERATIVE" or "EAGER".
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

    /* Mark the messages in the cache as stale, runInternal* will deal with
     * making it unusable. */
    this.#messageCache.markStale(topics);

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
   * @note This cannot be called from within `eachMessage` callback of `Consumer.run`.
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

    this.#disconnectStarted = true;
    this.#workerTerminationScheduled = true;
    while (!(await acquireOrLog(this.#lock, this.#logger))); /* Just retry... */

    this.#state = ConsumerState.DISCONNECTING;

    /* Since there are state-checks before everything, we are safe to proceed without the lock. */
    await this.#lock.release();

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
