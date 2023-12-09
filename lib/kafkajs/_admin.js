const RdKafka = require('../rdkafka');
const { kafkaJSToRdKafkaConfig, createKafkaJsErrorFromLibRdKafkaError } = require('./_common');
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
   * kJSConfig is the merged kafkaJS config object.
   * @type {import("../../types/kafkajs").AdminConfig  & import("../../types/kafkajs").KafkaConfig}
   */
  #kJSConfig = null;

  /**
   * rdKafkaConfig contains the config objects that will be passed to node-rdkafka.
   * @type {{globalConfig: import("../../types/config").GlobalConfig}|null}
   */
  #rdKafkaConfig = null;

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
    const { globalConfig } = await kafkaJSToRdKafkaConfig(this.#kJSConfig);

    return { globalConfig };
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

    const { globalConfig } = await this.#config();

    return new Promise((resolve, reject) => {
      try {
        /* AdminClient creation is a synchronous operation for node-rdkafka */
        this.#internalClient = RdKafka.AdminClient.create(globalConfig);
        this.#state = AdminState.CONNECTED;
        resolve();
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
    if (this.#state == AdminState.INIT) {
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
   * @returns {Promise<void>} Resolves when the topics are created, rejects on error.
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
    const ret =
      options.topics
        .map(this.#topicConfigToRdKafka)
        .map(topicConfig => new Promise((resolve, reject) => {
          this.#internalClient.createTopic(topicConfig, options.timeout ?? 5000, (err) => {
            if (err) {
              reject(createKafkaJsErrorFromLibRdKafkaError(err));
            } else {
              resolve();
            }
          });
        }));

    return Promise.all(ret);
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

}

module.exports = { Admin }
