const { OffsetSpec } = require('../admin');
const RdKafka = require('../rdkafka');
const { kafkaJSToRdKafkaConfig,
  createKafkaJsErrorFromLibRdKafkaError,
  DefaultLogger,
  CompatibilityErrorMessages,
  createBindingMessageMetadata,
  logLevel,
  checkAllowedKeys,
  loggerTrampoline,
  severityToLogLevel,
} = require('./_common');
const error = require('./_error');
const { hrtime } = require('process');

const AdminState = Object.freeze({
  INIT: 0,
  CONNECTING: 1,
  CONNECTED: 4,
  DISCONNECTING: 5,
  DISCONNECTED: 6,
});

/**
 * Admin client for administering Kafka clusters (promise-based, async API).
 *
 * This client is the way you can interface with the Kafka Admin APIs.
 * This class should not be instantiated directly, and rather, an instance of
 * [Kafka]{@link KafkaJS.Kafka} should be used to create it, or an existing
 * Producer or Consumer's `dependentAdmin` method may be used.
 *
 * @example
 * const { Kafka } = require('@confluentinc/kafka-javascript');
 * // From a Kafka object.
 * const kafka = new Kafka({ 'bootstrap.servers': 'localhost:9092' });
 * const admin = kafka.admin();
 * await admin.connect();
 * await admin.createTopics({ });
 *
 * // From a producer/consumer
 * const admin = preExistingProducer.dependentAdmin();
 * await admin.connect();
 * await admin.createTopics({ });
 *
 * @memberof KafkaJS
 * @see [Admin client examples]{@link https://github.com/confluentinc/confluent-kafka-javascript/blob/master/examples/kafkajs/admin}
 */
class Admin {
  /**
   * The config supplied by the user.
   * @type {import("../../types/kafkajs").AdminConstructorConfig|null}
   */
  #userConfig = null;

  /**
   * The config realized after processing any compatibility options.
   * @type {import("../../types/config").GlobalConfig|null}
   */
  #internalConfig = null;

  /**
   * internalClient is the node-rdkafka client used by the API.
   * @type {import("../rdkafka").AdminClient|null}
   */
  #internalClient = null;
  /**
   * state is the current state of the admin client.
   * @type {AdminState}
   */
  #state = AdminState.INIT;

  /**
   * A logger for the admin client.
   * @type {import("../../types/kafkajs").Logger}
   */
  #logger = new DefaultLogger();

  /**
   * connectPromiseFunc is the set of promise functions used to resolve/reject the connect() promise.
   * @type {{resolve: Function, reject: Function}|{}}
   */
  #connectPromiseFunc = null;

  /**
   * Stores the first error encountered while connecting (if any). This is what we
   * want to reject with.
   * @type {Error|null}
   */
  #connectionError = null;

  /**
   * The client name used by the admin client for logging - determined by librdkafka
   * using a combination of clientId and an integer.
   * @type {string|undefined}
   */
  #clientName = undefined;

  /**
   * The existing client to use as basis for this admin client, if any is provided.
   */
  #existingClient = null;

  // Convenience function to create the metadata object needed for logging.
  #createAdminBindingMessageMetadata() {
    return createBindingMessageMetadata(this.#clientName);
  }

  /**
   * @constructor
   * @param {import("../../types/kafkajs").AdminConstructorConfig?} config
   * @param {import("../../types/kafkajs").Client?} existingClient
   */
  constructor(config, existingClient) {
    this.#userConfig = config;
    this.#existingClient = existingClient;
  }

  #config() {
    if (!this.#internalConfig)
      this.#internalConfig = this.#finalizedConfig();
    return this.#internalConfig;
  }

  #kafkaJSToAdminConfig(kjsConfig) {
    if (!kjsConfig || Object.keys(kjsConfig).length === 0) {
      return {};
    }

    const disallowedKey = checkAllowedKeys('admin', kjsConfig);
    if (disallowedKey) {
      throw new error.KafkaJSError(CompatibilityErrorMessages.unsupportedKey(disallowedKey), { code: error.ErrorCodes.ERR__INVALID_ARG });
    }

    const rdKafkaConfig = kafkaJSToRdKafkaConfig(kjsConfig);

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
    let compatibleConfig = this.#kafkaJSToAdminConfig(this.#userConfig.kafkaJS);

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

    if (Object.hasOwn(rdKafkaConfig, 'stats_cb')) {
      if (typeof rdKafkaConfig['stats_cb'] === 'function')
        this.#statsCb = rdKafkaConfig['stats_cb'];
      delete rdKafkaConfig['stats_cb'];
    }

    return rdKafkaConfig;
  }

  #readyCb() {
    if (this.#state !== AdminState.CONNECTING) {
      /* The connectPromiseFunc might not be set, so we throw such an error. It's a state error that we can't recover from. Probably a bug. */
      throw new error.KafkaJSError(`Ready callback called in invalid state ${this.#state}`, { code: error.ErrorCodes.ERR__STATE });
    }
    this.#state = AdminState.CONNECTED;

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
    if (this.#state < AdminState.CONNECTED) {
      if (!this.#connectionError)
        this.#connectionError = err;
    } else {
      this.#logger.error(`Error: ${err.message}`, this.#createAdminBindingMessageMetadata());
    }
  }

  /**
   * Callback for the event.stats event, if defined.
   * @private
   */
  #statsCb = null;

  /**
   * Set up the client and connect to the bootstrap brokers.
   * @returns {Promise<void>} Resolves when connection is complete, rejects on error.
   */
  async connect() {
    if (this.#state !== AdminState.INIT) {
      throw new error.KafkaJSError("Connect has already been called elsewhere.", { code: error.ErrorCodes.ERR__STATE });
    }

    this.#state = AdminState.CONNECTING;

    return new Promise((resolve, reject) => {
      try {
        /* AdminClient creation is a synchronous operation for node-rdkafka */
        this.#connectPromiseFunc = { resolve, reject };
        if (!this.#existingClient) {
          const config = this.#config();
          this.#internalClient = RdKafka.AdminClient.create(config, {
            'error': this.#errorCb.bind(this),
            'ready': this.#readyCb.bind(this),
            'event.log': (msg) => loggerTrampoline(msg, this.#logger),
          });
          if (this.#statsCb) {
               this.#internalClient.on('event.stats', this.#statsCb.bind(this));
          }
        } else {
          const underlyingClient = this.#existingClient._getInternalClient();
          if (!underlyingClient) {
            throw new error.KafkaJSError("Underlying client is not connected.", { code: error.ErrorCodes.ERR__STATE });
          }
          this.#logger = this.#existingClient.logger();
          this.#internalClient = RdKafka.AdminClient.createFrom(underlyingClient, {
            'ready': this.#readyCb.bind(this),
          });
        }

        this.#clientName = this.#internalClient.name;
        this.#logger.info("Admin client connected", this.#createAdminBindingMessageMetadata());
      } catch (err) {
        this.#state = AdminState.DISCONNECTED;
        const rejectionError = this.#connectionError ? this.#connectionError : err;
        reject(createKafkaJsErrorFromLibRdKafkaError(rejectionError));
      }
    });
  }

  /**
   * Disconnect from the brokers, clean-up and tear down the client.
   * @returns {Promise<void>} Resolves when disconnect is complete, rejects on error.
   */
  async disconnect() {
    /* Not yet connected - no error. */
    if (this.#state === AdminState.INIT) {
      return;
    }

    /* Already disconnecting, or disconnected. */
    if (this.#state >= AdminState.DISCONNECTING) {
      return;
    }

    this.#state = AdminState.DISCONNECTING;
    return new Promise((resolve, reject) => {
      try {
        /* AdminClient disconnect for node-rdkakfa is synchronous. */
        this.#internalClient.disconnect();
        this.#state = AdminState.DISCONNECTED;
        this.#logger.info("Admin client disconnected", this.#createAdminBindingMessageMetadata());
        resolve();
      } catch (err) {
        reject(createKafkaJsErrorFromLibRdKafkaError(err));
      }
    });
  }


  /**
   * Converts a topic configuration object from kafkaJS to a format suitable for node-rdkafka.
   * @param {import("../../types/kafkajs").ITopicConfig} topic
   * @returns {import("../../index").NewTopic}
   * @private
   */
  #topicConfigToRdKafka(topic) {
    let topicConfig = { topic: topic.topic };
    topicConfig.topic = topic.topic;
    topicConfig.num_partitions = topic.numPartitions ?? -1;
    topicConfig.replication_factor = topic.replicationFactor ?? -1;

    if (Object.hasOwn(topic, "replicaAssignment")) {
      throw new error.KafkaJSError("replicaAssignment is not yet implemented.", { code: error.ErrorCodes.ERR__NOT_IMPLEMENTED });
    }

    topicConfig.config = {};
    topic.configEntries = topic.configEntries ?? [];
    for (const configEntry of topic.configEntries) {
      topicConfig.config[configEntry.name] = configEntry.value;
    }

    return topicConfig;
  }

  /**
   * Create topics with the given configuration.
   * @param {object} options
   * @param {number?} options.timeout - The request timeout in milliseconds (default: 5000).
   * @param {Array<{topic: string, numPartitions: number | null, replicationFactor: number | null, configEntries: Array<{name: string, value: string}> | null}>} options.topics
   * The topics to create and optionally, the configuration for each topic.
   * @returns {Promise<boolean>} Resolves true when the topics are created, false if topic exists already, rejects on error.
   *                             In case even one topic already exists, this will return false.
   */
  async createTopics(options) {
    if (this.#state !== AdminState.CONNECTED) {
      throw new error.KafkaJSError("Admin client is not connected.", { code: error.ErrorCodes.ERR__STATE });
    }

    if (Object.hasOwn(options, "validateOnly")) {
      throw new error.KafkaJSError("validateOnly is not yet implemented.", { code: error.ErrorCodes.ERR__NOT_IMPLEMENTED });
    }

    if (Object.hasOwn(options, "waitForLeaders")) {
      throw new error.KafkaJSError("waitForLeaders is not yet implemented.", { code: error.ErrorCodes.ERR__NOT_IMPLEMENTED });
    }

    /* Convert each topic to a format suitable for node-rdkafka, and dispatch the call. */
    let allTopicsCreated = true;
    const errors = [];
    const ret =
      options.topics
        .map(this.#topicConfigToRdKafka)
        .map(topicConfig => new Promise(resolve => {
          this.#internalClient.createTopic(topicConfig, options.timeout ?? 5000, (err) => {
            if (err) {
              if (err.code === error.ErrorCodes.ERR_TOPIC_ALREADY_EXISTS) {
                allTopicsCreated = false;
                resolve();
                return;
              }
              const e = createKafkaJsErrorFromLibRdKafkaError(err);
              const createTopicError = new error.KafkaJSCreateTopicError(e, topicConfig.topic, e /* includes the properties */);
              errors.push(createTopicError);
              resolve(); // Don't reject this promise, instead, look at the errors array later.
            } else {
              resolve();
            }
          });
        }));

    await Promise.allSettled(ret);
    if (errors.length > 0) {
      throw new error.KafkaJSAggregateError("Topic creation errors", errors);
    }
    return allTopicsCreated;
  }

  /**
   * Deletes given topics.
   * @param {object} options
   * @param {Array<string>} options.topics - The topics to delete.
   * @param {number?} options.timeout - The request timeout in milliseconds (default: 5000).
   * @returns {Promise<void>} Resolves when the topics are deleted, rejects on error.
   */
  async deleteTopics(options) {
    if (this.#state !== AdminState.CONNECTED) {
      throw new error.KafkaJSError("Admin client is not connected.", { code: error.ErrorCodes.ERR__STATE });
    }

    return Promise.all(
      options.topics.map(topic => new Promise((resolve, reject) => {
        this.#internalClient.deleteTopic(topic, options.timeout ?? 5000, err => {
          if (err) {
            reject(createKafkaJsErrorFromLibRdKafkaError(err));
          } else {
            resolve();
          }
        });
      }))
    );
  }

  /**
   * List consumer groups.
   *
   * @param {object?} options
   * @param {number?} options.timeout - The request timeout in milliseconds.
   *                                    May be unset (default: 5000).
   * @param {Array<KafkaJS.ConsumerGroupStates>?} options.matchConsumerGroupStates -
   *        A list of consumer group states to match. May be unset, fetches all states (default: unset).
   * @param {Array<KafkaJS.ConsumerGroupTypes>?} options.matchConsumerGroupTypes -
   *       A list of consumer group types to match. May be unset, fetches all types (default: unset).
   * @returns {Promise<{ groups: Array<{groupId: string, protocolType: string, isSimpleConsumerGroup: boolean, state: KafkaJS.ConsumerGroupStates, type: KafkaJS.ConsumerGroupTypes}>, errors: Array<RdKafka.LibrdKafkaError> }>}
   *          Resolves with the list of consumer groups, rejects on error.
   */
  async listGroups(options = {}) {
    if (this.#state !== AdminState.CONNECTED) {
      throw new error.KafkaJSError("Admin client is not connected.", { code: error.ErrorCodes.ERR__STATE });
    }

    return new Promise((resolve, reject) => {
      this.#internalClient.listGroups(options, (err, groups) => {
        if (err) {
          reject(createKafkaJsErrorFromLibRdKafkaError(err));
        } else {
          resolve(groups);
        }
      });
    });
  }

  /**
   * Describe consumer groups.
   *
   * @param {Array<string>} groups - The names of the groups to describe.
   * @param {object?} options
   * @param {number?} options.timeout - The request timeout in milliseconds.
   *                                    May be unset (default: 5000)
   * @param {boolean?} options.includeAuthorizedOperations - If true, include operations allowed on the group by the calling client (default: false).
   * @returns {Promise<{groups: Array<object>}>} The descriptions of the requested groups.
   */
  async describeGroups(groups, options = {}) {
    if (this.#state !== AdminState.CONNECTED) {
      throw new error.KafkaJSError("Admin client is not connected.", { code: error.ErrorCodes.ERR__STATE });
    }

    return new Promise((resolve, reject) => {
      this.#internalClient.describeGroups(groups, options, (err, descriptions) => {
        if (err) {
          reject(createKafkaJsErrorFromLibRdKafkaError(err));
        } else {
          resolve(descriptions);
        }
      });
    });
  }

  /**
   * Delete consumer groups.
   * @param {Array<string>} groups - The names of the groups to delete.
   * @param {object?} options
   * @param {number?} options.timeout - The request timeout in milliseconds.
   *                                    May be unset (default: 5000)
   * @returns {Promise<Array<{groupId: string, errorCode: number | null, error: RdKafka.LibrdKafkaError | null}>>}
   *          Resolves with the list of deletion reports (including per-group errors).
   */
  async deleteGroups(groups, options = {}) {
    if (this.#state !== AdminState.CONNECTED) {
      throw new error.KafkaJSError("Admin client is not connected.", { code: error.ErrorCodes.ERR__STATE });
    }

    return new Promise((resolve, reject) => {
      this.#internalClient.deleteGroups(groups, options, (err, reports) => {
        if (err) {
          reject(createKafkaJsErrorFromLibRdKafkaError(err));
          return;
        }

        /* Convert group-level errors to KafkaJS errors if required. */
        let errorsPresent = false;
        reports = reports.map(groupReport => {
          if (groupReport.error) {
            errorsPresent = true;
            groupReport.error = createKafkaJsErrorFromLibRdKafkaError(groupReport.error);
          }
          return groupReport;
        });

        if (errorsPresent) {
          reject(new error.KafkaJSDeleteGroupsError('Error in DeleteGroups', reports));
          return;
        }
        resolve(reports);
      });
    });
  }

  /**
   * List topics.
   *
   * @param {object?} options
   * @param {number?} options.timeout - The request timeout in milliseconds.
   *                                    May be unset (default: 5000).
   * @returns {Promise<Array<string>>} The list of all topics.
   */
  async listTopics(options = {}) {
    if (this.#state !== AdminState.CONNECTED) {
      throw new error.KafkaJSError("Admin client is not connected.", { code: error.ErrorCodes.ERR__STATE });
    }

    return new Promise((resolve, reject) => {
      this.#internalClient.listTopics(options, (err, topics) => {
        if (err) {
          reject(createKafkaJsErrorFromLibRdKafkaError(err));
        } else {
          resolve(topics);
        }
      });
    });
  }

  /**
   * Fetch the offsets for topic partition(s) for consumer group(s).
   *
   * @param {object?} options
   * @param {string} options.groupId - The group ID to fetch offsets for.
   * @param {Array<string> | Array<{topic: string, partitions: Array<number>}>} options.topics
   *        The topics to fetch offsets for. Can be specified as a list of topics (in case offsets for all topics are fetched),
   *        or as a list of objects, each containing a topic and a list of partitions.
   * @param {number?} options.timeout - The request timeout in milliseconds.
   *                                    May be unset (default: 5000).
   * @param {boolean?} options.requireStableOffsets - Whether broker should return stable offsets
   *                                                  (transaction-committed). (default: false).
   *
   * @returns {Promise<Array<{topic: string, partitions: Array<object>}>>}
   *          The list of requested offsets.
   */
  async fetchOffsets(options = {}) {
    if (this.#state !== AdminState.CONNECTED) {
      throw new error.KafkaJSError("Admin client is not connected.", { code: error.ErrorCodes.ERR__STATE });
    }

    if (Object.hasOwn(options, "resolveOffsets")) {
      throw new error.KafkaJSError("resolveOffsets is not yet implemented.", { code: error.ErrorCodes.ERR__NOT_IMPLEMENTED });
    }

    const { groupId, topics } = options;

    if (!groupId) {
      throw new error.KafkaJSError("groupId is required.", { code: error.ErrorCodes.ERR__INVALID_ARG });
    }

    let partitions = null;
    let originalTopics = null;

    /*
      If the input is a list of topic string, the user expects us to
      fetch offsets for all all partitions of all the input topics. In
      librdkafka, we can only fetch offsets by topic partitions, or else,
      we can fetch all of them. Thus, we must fetch offsets for all topic
      partitions (by settings partitions to null) and filter by the topic strings later.
    */
    if (topics && Array.isArray(topics)) {
      if (typeof topics[0] === 'string') {
        originalTopics = topics;
        partitions = null;
      } else if (typeof topics[0] === 'object' && Array.isArray(topics[0].partitions)) {
        partitions = topics.flatMap(topic => topic.partitions.map(partition => ({
          topic: topic.topic,
          partition
        })));
      } else {
        throw new error.KafkaJSError("Invalid topics format.", { code: error.ErrorCodes.ERR__INVALID_ARG });
      }
    }

    const listGroupOffsets = [{
      groupId,
      partitions
    }];


    return new Promise((resolve, reject) => {
      this.#internalClient.listConsumerGroupOffsets(listGroupOffsets, options, (err, offsets) => {
        if (err) {
          reject(createKafkaJsErrorFromLibRdKafkaError(err));
        } else {

          /**
           * Offsets is an array of group results, each containing a group id,
           * an error and an array of partitions.
           * We need to convert it to the required format of an array of topics, each
           * containing an array of partitions.
           */
          const topicPartitionMap = new Map();

          if (offsets.length !== 1) {
            reject(new error.KafkaJSError("Unexpected number of group results."));
            return;
          }

          const groupResult = offsets[0];

          if (groupResult.error) {
            reject(createKafkaJsErrorFromLibRdKafkaError(groupResult.error));
            return;
          }

          // Traverse the partitions and group them by topic
          groupResult.partitions.forEach(partitionObj => {
            const { topic, partition, offset, leaderEpoch, metadata, error } = partitionObj;
            const fetchOffsetsPartition = {
              partition: partition,
              offset: String(offset),
              metadata: metadata || null,
              leaderEpoch: leaderEpoch || null,
              error: error || null
            };

            // Group partitions by topic
            if (!topicPartitionMap.has(topic)) {
              topicPartitionMap.set(topic, []);
            }
            topicPartitionMap.get(topic).push(fetchOffsetsPartition);
          });

          // Convert the map back to the desired array format
          let convertedOffsets = Array.from(topicPartitionMap, ([topic, partitions]) => ({
            topic,
            partitions
          }));

          if (originalTopics !== null) {
            convertedOffsets = convertedOffsets.filter(convertedOffset => originalTopics.includes(convertedOffset.topic));
          }
          resolve(convertedOffsets);
        }
      });
    });
  }

  /**
   * Deletes records (messages) in topic partitions older than the offsets provided.
   *
   * Provide -1 as offset to delete all records in the partition.
   *
   * @param {object} options
   * @param {string} options.topic - The topic to delete offsets for.
   * @param {Array<{partition: number, offset: string}>} options.partitions
   *        The partitions and associated offsets to delete up until.
   * @param {number?} options.operationTimeout - The operation timeout in milliseconds.
   *                                             May be unset (default: 60000).
   * @param {number?} options.timeout - The request timeout in milliseconds.
   *                                    May be unset (default: 5000).
   *
   * @returns {Promise<Array<{topic: string, partition: number, lowWatermark: number, error: RdKafka.LibrdKafkaError | null}>>}
   *          A list of results for each partition.
   */
  async deleteTopicRecords(options = {}) {
    if (this.#state !== AdminState.CONNECTED) {
      throw new error.KafkaJSError("Admin client is not connected.", { code: error.ErrorCodes.ERR__STATE });
    }

    if (!Object.hasOwn(options, 'topic') || !Object.hasOwn(options, 'partitions') || !Array.isArray(options.partitions)) {
      throw new error.KafkaJSError("Options must include 'topic' and 'partitions', and 'partitions' must be an array.", { code: error.ErrorCodes.ERR__INVALID_ARG });
    }

    const { topic, partitions } = options;

    // Create an array of TopicPartitionOffset objects
    const topicPartitionOffsets = [];

    for (const partition of partitions) {
      if (partition.offset === null || partition.offset === undefined) {
        throw new error.KafkaJSError("Each partition must have a valid offset.", { code: error.ErrorCodes.ERR__INVALID_ARG });
      }

      const offset = +partition.offset;
      if (isNaN(offset)) {
        throw new error.KafkaJSError("Offset must be a valid number.", { code: error.ErrorCodes.ERR__INVALID_ARG });
      }

      topicPartitionOffsets.push({
        topic,
        partition: partition.partition,
        offset: offset,
      });
    }

    return new Promise((resolve, reject) => {
      this.#internalClient.deleteRecords(topicPartitionOffsets, options, (err, results) => {
        if (err) {
          reject(createKafkaJsErrorFromLibRdKafkaError(err));
          return;
        }

        let errorsPresent = false;
        results = results.map(result => {
          if (result.error) {
            errorsPresent = true;
            result.error = createKafkaJsErrorFromLibRdKafkaError(result.error);
          }
          return result;
        });

        if (errorsPresent) {
          const partitionsWithError =
          {
            /* Note that, for API compatibility, we must filter out partitions
             * without errors, even though it is more useful to return all of
             * them so the user can check offsets. */
            partitions:
              results.filter(result => result.error).map(result => ({
                partition: result.partition,
                offset: String(result.lowWatermark),
                error: result.error,
              }))
          };
          reject(new error.KafkaJSDeleteTopicRecordsError(partitionsWithError));
          return;
        }
        resolve(results);
      });
    });
  }

  /**
   * Describe topics.
   *
   * @param {object?} options
   * @param {Array<string>} options.topics - The topics to describe.
   *                                    If unset, all topics will be described.
   * @param {number?} options.timeout - The request timeout in milliseconds.
   *                                    May be unset (default: 5000).
   * @param {boolean?} options.includeAuthorizedOperations - If true, include operations allowed on the topic
   *                                                         by the calling client (default: false).
   *
   * @returns {Promise<{ topics: Array<object> }>}
   */
  async fetchTopicMetadata(options = {}) {
    if (this.#state !== AdminState.CONNECTED) {
      throw new error.KafkaJSError("Admin client is not connected.", { code: error.ErrorCodes.ERR__STATE });
    }

    let topics = options.topics;
    if (!Object.hasOwn(options, 'topics')) {
      try {
        topics = await this.listTopics();
      } catch (err) {
        throw createKafkaJsErrorFromLibRdKafkaError(err);
      }
    }

    return new Promise((resolve, reject) => {
      this.#internalClient.describeTopics(topics, options, (err, metadata) => {
        if (err) {
          reject(createKafkaJsErrorFromLibRdKafkaError(err));
        } else {

          let errs = metadata.filter(topic => topic.error);
          if (errs.length > 0) {
            reject(createKafkaJsErrorFromLibRdKafkaError(errs[0].error));
            return;
          }
          const convertedMetadata = metadata.map(topic => ({
            name: topic.name,
            topicId: topic.topicId,
            isInternal: topic.isInternal,
            partitions: topic.partitions.map(partition => ({
              partitionErrorCode: error.ErrorCodes.ERR_NO_ERROR,
              partitionId: partition.partition,
              leader: partition.leader.id,
              leaderNode: partition.leader,
              replicas: partition.replicas.map(replica => replica.id),
              replicaNodes: partition.replicas,
              isr: partition.isr.map(isrNode => isrNode.id),
              isrNodes: partition.isr
            })),
            authorizedOperations: topic.authorizedOperations
          }));

          resolve(convertedMetadata);
        }
      });
    });
  }

  /**
   * List offsets for the specified topic partition(s).
   *
   * @param {string} topic - The topic to fetch offsets for.
   * @param {object?} options
   * @param {number?} options.timeout - The request timeout in milliseconds.
   *                                    May be unset (default: 5000)
   * @param {KafkaJS.IsolationLevel?} options.isolationLevel - The isolation level for reading the offsets.
   *                                                           (default: READ_UNCOMMITTED)
   *
   * @returns {Promise<Array<{partition: number, offset: string, high: string, low: string}>>}
   */
  async fetchTopicOffsets(topic, options = {}) {
    if (this.#state !== AdminState.CONNECTED) {
      throw new error.KafkaJSError("Admin client is not connected.", { code: error.ErrorCodes.ERR__STATE });
    }

    if (!Object.hasOwn(options, 'timeout')) {
      options.timeout = 5000;
    }

    let topicData;
    let startTime, endTime, timeTaken;

    try {
      // Measure time taken for fetchTopicMetadata
      startTime = hrtime.bigint();
      topicData = await this.fetchTopicMetadata({ topics: [topic], timeout: options.timeout });
      endTime = hrtime.bigint();
      timeTaken = Number(endTime - startTime) / 1e6; // Convert nanoseconds to milliseconds

      // Adjust timeout for the next request
      options.timeout -= timeTaken;
      if (options.timeout <= 0) {
        throw new error.KafkaJSError("Timeout exceeded while fetching topic metadata.", { code: error.ErrorCodes.ERR__TIMED_OUT });
      }
    } catch (err) {
      throw new createKafkaJsErrorFromLibRdKafkaError(err);
    }

    const partitionIds = topicData.flatMap(topic =>
      topic.partitions.map(partition => partition.partitionId)
    );

    const topicPartitionOffsetsLatest = partitionIds.map(partitionId => ({
      topic,
      partition: partitionId,
      offset: OffsetSpec.LATEST
    }));

    const topicPartitionOffsetsEarliest = partitionIds.map(partitionId => ({
      topic,
      partition: partitionId,
      offset: OffsetSpec.EARLIEST
    }));

    try {
      // Measure time taken for listOffsets (latest)
      startTime = hrtime.bigint();
      const latestOffsets = await this.#listOffsets(topicPartitionOffsetsLatest, options);
      endTime = hrtime.bigint();
      timeTaken = Number(endTime - startTime) / 1e6; // Convert nanoseconds to milliseconds

      // Adjust timeout for the next request
      options.timeout -= timeTaken;
      if (options.timeout <= 0) {
        throw new error.KafkaJSError("Timeout exceeded while fetching latest offsets.", { code: error.ErrorCodes.ERR__TIMED_OUT });
      }

      // Measure time taken for listOffsets (earliest)
      startTime = hrtime.bigint();
      const earliestOffsets = await this.#listOffsets(topicPartitionOffsetsEarliest, options);
      endTime = hrtime.bigint();
      timeTaken = Number(endTime - startTime) / 1e6; // Convert nanoseconds to milliseconds

      // Adjust timeout for the next request
      options.timeout -= timeTaken;
      if (options.timeout <= 0) {
        throw new error.KafkaJSError("Timeout exceeded while fetching earliest offsets.", { code: error.ErrorCodes.ERR__TIMED_OUT });
      }

      const combinedResults = partitionIds.map(partitionId => {
        const latest = latestOffsets.find(offset => offset.partition === partitionId);
        const earliest = earliestOffsets.find(offset => offset.partition === partitionId);

        return {
          partition: partitionId,
          offset: latest.offset.toString(),
          high: latest.offset.toString(),
          low: earliest.offset.toString()
        };
      });

      return combinedResults;
    } catch (err) {
      throw createKafkaJsErrorFromLibRdKafkaError(err);
    }
  }

  #listOffsets(partitionOffsets, options) {
    return new Promise((resolve, reject) => {
      this.#internalClient.listOffsets(partitionOffsets, options, (err, offsets) => {
        if (err) {
          reject(createKafkaJsErrorFromLibRdKafkaError(err));
        } else {
          resolve(offsets);
        }
      });
    });
  }

  /**
   * List offsets for the topic partition(s) by timestamp.
   *
   * @param {string} topic - The topic to fetch offsets for.
   * @param {number?} timestamp - The timestamp to fetch offsets for.
   * @param {object?} options
   * @param {number?} options.timeout - The request timeout in milliseconds.
   *                                    May be unset (default: 5000)
   * @param {KafkaJS.IsolationLevel?} options.isolationLevel - The isolation level for reading the offsets.
   *                                                           (default: READ_UNCOMMITTED)
   *
   * The returned topic partitions contain the earliest offset whose timestamp is greater than or equal to
   * the given timestamp. If there is no such offset, or if the timestamp is unset, the latest offset is returned instead.
   *
   * @returns {Promise<Array<{partition: number, offset: string}>>}
   */
  async fetchTopicOffsetsByTimestamp(topic, timestamp, options = {}) {
    if (this.#state !== AdminState.CONNECTED) {
      throw new error.KafkaJSError("Admin client is not connected.", { code: error.ErrorCodes.ERR__STATE });
    }

    if (!Object.hasOwn(options, 'timeout')) {
      options.timeout = 5000;
    }

    let topicData;
    let startTime, endTime, timeTaken;

    try {
      // Measure time taken for fetchTopicMetadata
      startTime = hrtime.bigint();
      topicData = await this.fetchTopicMetadata({ topics: [topic], timeout: options.timeout });
      endTime = hrtime.bigint();
      timeTaken = Number(endTime - startTime) / 1e6; // Convert nanoseconds to milliseconds

      // Adjust timeout for the next request
      options.timeout -= timeTaken;
      if (options.timeout <= 0) {
        throw new error.KafkaJSError("Timeout exceeded while fetching topic metadata.", { code: error.ErrorCodes.ERR__TIMED_OUT });
      }
    } catch (err) {
      throw new createKafkaJsErrorFromLibRdKafkaError(err);
    }

    const partitionIds = topicData.flatMap(topic =>
      topic.partitions.map(partition => partition.partitionId)
    );
    let topicPartitionOffset = [];
    if (typeof timestamp === 'undefined') {
      topicPartitionOffset = partitionIds.map(partitionId => ({
        topic,
        partition: partitionId,
        offset: OffsetSpec.LATEST
      }));
    }
    else {
      topicPartitionOffset = partitionIds.map(partitionId => ({
        topic,
        partition: partitionId,
        offset: new OffsetSpec(timestamp)
      }));
    }

    const topicPartitionOffsetsLatest = partitionIds.map(partitionId => ({
      topic,
      partition: partitionId,
      offset: OffsetSpec.LATEST
    }));

    try {
      // Measure time taken for listOffsets (by timestamp)
      startTime = hrtime.bigint();
      const offsetsByTimeStamp = await this.#listOffsets(topicPartitionOffset, options);
      endTime = hrtime.bigint();
      timeTaken = Number(endTime - startTime) / 1e6; // Convert nanoseconds to milliseconds

      // Adjust timeout for the next request
      options.timeout -= timeTaken;
      if (options.timeout <= 0) {
        throw new error.KafkaJSError("Timeout exceeded while fetching offsets.", { code: error.ErrorCodes.ERR__TIMED_OUT });
      }

      if (typeof timestamp === 'undefined') {
        // Return result from offsetsByTimestamp if timestamp is undefined
        return offsetsByTimeStamp.map(offset => ({
          partition: offset.partition,
          offset: offset.offset.toString(),
        }));
      } else {
        // Measure time taken for listOffsets(latest)
        startTime = hrtime.bigint();
        const latestOffsets = await this.#listOffsets(topicPartitionOffsetsLatest, options);
        endTime = hrtime.bigint();
        timeTaken = Number(endTime - startTime) / 1e6; // Convert nanoseconds to milliseconds

        // Adjust timeout for the next request
        options.timeout -= timeTaken;
        if (options.timeout <= 0) {
          throw new error.KafkaJSError("Timeout exceeded while fetching latest offsets.", { code: error.ErrorCodes.ERR__TIMED_OUT });
        }

        const combinedResults = partitionIds.map(partitionId => {
          const latest = latestOffsets.find(offset => offset.partition === partitionId);
          const timestampOffset = offsetsByTimeStamp.find(offset => offset.partition === partitionId);

          if (timestampOffset.offset === -1) {
            return {
              partition: partitionId,
              offset: latest.offset.toString(),
            };
          } else {
            return {
              partition: partitionId,
              offset: timestampOffset.offset.toString(),
            };
          }
        });

        return combinedResults;
      }
    } catch (err) {
      throw createKafkaJsErrorFromLibRdKafkaError(err);
    }
  }
}

module.exports = {
  Admin,
  /**
   * A list of consumer group states.
   * @enum {number}
   * @readonly
   * @memberof KafkaJS
   * @see RdKafka.ConsumerGroupStates
   */
  ConsumerGroupStates: RdKafka.AdminClient.ConsumerGroupStates,
  /**
   * A list of consumer group types.
   * @enum {number}
   * @readonly
   * @memberof KafkaJS
   * @see RdKafka.ConsumerGroupTypes
   */
  ConsumerGroupTypes: RdKafka.AdminClient.ConsumerGroupTypes,
  /**
   * A list of ACL operation types.
   * @enum {number}
   * @readonly
   * @memberof KafkaJS
   * @see RdKafka.AclOperationTypes
   */
  AclOperationTypes: RdKafka.AdminClient.AclOperationTypes,
  /**
   * A list of isolation levels.
   * @enum {number}
   * @readonly
   * @memberof KafkaJS
   * @see RdKafka.IsolationLevel
   */
  IsolationLevel: RdKafka.AdminClient.IsolationLevel
};
