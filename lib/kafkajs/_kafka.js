const { Producer, CompressionTypes } = require('./_producer');
const { Consumer, PartitionAssigners } = require('./_consumer');
const { Admin } = require('./_admin');
const error = require('./_error');
const { logLevel } = require('./_common');

class Kafka {
  /* @type{import("../../types/kafkajs").CommonConstructorConfig} */
  #commonClientConfig = {};

  /**
   *
   * @param {import("../../types/kafkajs").CommonConstructorConfig} config
   */
  constructor(config) {
    this.#commonClientConfig = config ?? {};
  }

  /**
   * Merge the producer/consumer specific configuration with the common configuration.
   * @param {import("../../types/kafkajs").ProducerConstructorConfig|import("../../types/kafkajs").ConsumerConstructorConfig} config
   * @returns {(import("../../types/kafkajs").ProducerConstructorConfig & import("../../types/kafkajs").CommonConstructorConfig) | (import("../../types/kafkajs").ConsumerConstructorConfig & import("../../types/kafkajs").CommonConstructorConfig)}
   */
  #mergeConfiguration(config) {
    config = config ?? {};
    const mergedConfig = Object.assign({}, this.#commonClientConfig);

    mergedConfig.kafkaJs = mergedConfig.kafkaJs ?? {};

    if (typeof config.kafkaJs === 'object') {
      mergedConfig.kafkaJs = Object.assign(mergedConfig.kafkaJs, config.kafkaJs);
      delete config.kafkaJs;
    }

    Object.assign(mergedConfig, config);

    return mergedConfig;
  }

  /**
   * Creates a new producer.
   * @param {import("../../types/kafkajs").ProducerConstructorConfig} config
   * @returns {Producer}
   */
  producer(config) {
    return new Producer(this.#mergeConfiguration(config));
  }

  /**
   * Creates a new consumer.
   * @param {import("../../types/kafkajs").ConsumerConstructorConfig} config
   * @returns {Consumer}
   */
  consumer(config) {
    return new Consumer(this.#mergeConfiguration(config));
  }

  admin(config) {
    return new Admin(this.#mergeConfiguration(config));
  }
}

module.exports = { Kafka, ...error, logLevel, PartitionAssigners, PartitionAssignors: PartitionAssigners, CompressionTypes };
