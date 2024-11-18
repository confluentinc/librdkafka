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

/**
 * NOTE: The Admin client is currently in an experimental state with many
 *       features missing or incomplete, and the API is subject to change.
 */

const AdminState = Object.freeze({
  INIT: 0,
  CONNECTING: 1,
  CONNECTED: 4,
  DISCONNECTING: 5,
  DISCONNECTED: 6,
});

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
   * The client name used by the admin client for logging - determined by librdkafka
   * using a combination of clientId and an integer.
   * @type {string|undefined}
   */
  #clientName = undefined;

  /**
   * Convenience function to create the metadata object needed for logging.
   */
  #createAdminBindingMessageMetadata() {
    return createBindingMessageMetadata(this.#clientName);
  }

  /**
   * @constructor
   * @param {import("../../types/kafkajs").AdminConstructorConfig} config
   */
  constructor(config) {
    this.#userConfig = config;
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
   */
  #errorCb(err) {
    if (this.#state === AdminState.CONNECTING) {
      this.#connectPromiseFunc['reject'](err);
    } else {
      this.#logger.error(`Error: ${err.message}`, this.#createAdminBindingMessageMetadata());
    }
  }

  /**
   * Set up the client and connect to the bootstrap brokers.
   * @returns {Promise<void>} Resolves when connection is complete, rejects on error.
   */
  async connect() {
    if (this.#state !== AdminState.INIT) {
      throw new error.KafkaJSError("Connect has already been called elsewhere.", { code: error.ErrorCodes.ERR__STATE });
    }

    this.#state = AdminState.CONNECTING;

    const config = this.#config();

    return new Promise((resolve, reject) => {
      try {
        /* AdminClient creation is a synchronous operation for node-rdkafka */
        this.#connectPromiseFunc = { resolve, reject };
        this.#internalClient = RdKafka.AdminClient.create(config, {
          'error': this.#errorCb.bind(this),
          'ready': this.#readyCb.bind(this),
          'event.log': (msg) => loggerTrampoline(msg, this.#logger),
        });

        this.#clientName = this.#internalClient.name;
        this.#logger.info("Admin client connected", this.#createAdminBindingMessageMetadata());
      } catch (err) {
        reject(createKafkaJsErrorFromLibRdKafkaError(err));
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
   * @param {{ validateOnly?: boolean, waitForLeaders?: boolean, timeout?: number, topics: import("../../types/kafkajs").ITopicConfig[] }} options
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
    const ret =
      options.topics
        .map(this.#topicConfigToRdKafka)
        .map(topicConfig => new Promise((resolve, reject) => {
          this.#internalClient.createTopic(topicConfig, options.timeout ?? 5000, (err) => {
            if (err) {
              if (err.code === error.ErrorCodes.ERR_TOPIC_ALREADY_EXISTS) {
                allTopicsCreated = false;
                resolve();
                return;
              }
              reject(createKafkaJsErrorFromLibRdKafkaError(err));
            } else {
              resolve();
            }
          });
        }));

    return Promise.all(ret).then(() => allTopicsCreated);
  }

  /**
   * Deletes given topics.
   * @param {{topics: string[], timeout?: number}} options
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
   *                                    May be unset (default: 5000)
   * @param {import("../../types/kafkajs").ConsumerGroupStates[]?} options.matchConsumerGroupStates -
   *        A list of consumer group states to match. May be unset, fetches all states (default: unset).
   * @returns {Promise<{ groups: import("../../types/kafkajs").GroupOverview[], errors: import("../../types/kafkajs").LibrdKafkaError[] }>}
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
   * @param {string[]} groups - The names of the groups to describe.
   * @param {object?} options
   * @param {number?} options.timeout - The request timeout in milliseconds.
   *                                    May be unset (default: 5000)
   * @param {boolean?} options.includeAuthorizedOperations - If true, include operations allowed on the group by the calling client (default: false).
   * @returns {Promise<import("../../types/kafkajs").GroupDescriptions>}
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
   * @param {string[]} groups - The names of the groups to delete.
   * @param {any?} options
   * @param {number?} options.timeout - The request timeout in milliseconds.
   *                                    May be unset (default: 5000)
   * @returns {Promise<import("../../types/kafkajs").DeleteGroupsResult[]>}
   */
  async deleteGroups(groups, options = {}) {
    if (this.#state !== AdminState.CONNECTED) {
      throw new error.KafkaJSError("Admin client is not connected.", { code: error.ErrorCodes.ERR__STATE });
    }

    return new Promise((resolve, reject) => {
      this.#internalClient.deleteGroups(groups, options, (err, reports) => {
        if (err) {
          reject(createKafkaJsErrorFromLibRdKafkaError(err));
        } else {
          resolve(reports);
        }
      });
    });
  }

  /**
   * List topics.
   *
   * @param {any?} options
   * @param {number?} options.timeout - The request timeout in milliseconds.
   *                                    May be unset (default: 5000)
   * @returns {Promise<string[]>}
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
   * @param {string} options.groupId - The group ID to fetch offsets for.
   * @param {import("../../types/kafkajs").TopicInput} options.topics - The topics to fetch offsets for.
   * @param {boolean} options.resolveOffsets - not yet implemented
   * @param {number?} options.timeout - The request timeout in milliseconds.
   *                                    May be unset (default: 5000)
   * @param {boolean?} options.requireStableOffsets - Whether broker should return stable offsets
   *                                                  (transaction-committed). (default: false)
   *
   * @returns {Promise<Array<topic: string, partitions: import('../../types/kafkajs').FetchOffsetsPartition>>}
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
   * Provide -1 as offset to delete all records.
   *
   * @param {string} options.topic - The topic to delete offsets for.
   * @param {import("../../types/kafkajs").SeekEntry[]} options.partitons - The partitions to delete offsets for.
   * @param {number?} options.operationTimeout - The operation timeout in milliseconds.
   *                                             May be unset (default: 60000)
   * @param {number?} options.timeout - The request timeout in milliseconds.
   *                                    May be unset (default: 5000)
   *
   * @returns {Promise<import('../../types/kafkajs').DeleteRecordsResult[]>>}
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
        } else {
          resolve(results);
        }
      });
    });
  }

  /**
   * Describe topics.
   *
   * @param {string[]} options.topics - The topics to describe.
   *                                    If unset, all topics will be described.
   * @param {number?} options.timeout - The request timeout in milliseconds.
   *                                    May be unset (default: 5000)
   * @param {boolean?} options.includeAuthorizedOperations - If true, include operations allowed on the topic
   *                                                         by the calling client (default: false).
   *
   * @returns {Promise<{ topics: Array<import('../../types/kafkajs').ITopicMetadata> }>}
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
}

module.exports = {
  Admin,
  ConsumerGroupStates: RdKafka.AdminClient.ConsumerGroupStates,
  AclOperationTypes: RdKafka.AdminClient.AclOperationTypes
};
