const { Producer, CompressionTypes } = require('./_producer');
const { Consumer, PartitionAssigners } = require('./_consumer');
const { Admin, ConsumerGroupStates, AclOperationTypes } = require('./_admin');
const error = require('./_error');
const { logLevel, checkIfKafkaJsKeysPresent, CompatibilityErrorMessages } = require('./_common');

class Kafka {
  /* @type{import("../../types/kafkajs").CommonConstructorConfig} */
  #commonClientConfig = {};

  /**
   *
   * @param {import("../../types/kafkajs").CommonConstructorConfig} config
   */
  constructor(config) {
    this.#commonClientConfig = config ?? {};

    const disallowedKey = checkIfKafkaJsKeysPresent('common', this.#commonClientConfig);
    if (disallowedKey !== null) {
      throw new error.KafkaJSError(CompatibilityErrorMessages.kafkaJSCommonKey(disallowedKey));
    }
  }

  /**
   * Merge the producer/consumer specific configuration with the common configuration.
   * @param {import("../../types/kafkajs").ProducerConstructorConfig|import("../../types/kafkajs").ConsumerConstructorConfig} config
   * @returns {(import("../../types/kafkajs").ProducerConstructorConfig & import("../../types/kafkajs").CommonConstructorConfig) | (import("../../types/kafkajs").ConsumerConstructorConfig & import("../../types/kafkajs").CommonConstructorConfig)}
   */
  #mergeConfiguration(config) {
    config = Object.assign({}, config) ?? {};
    const mergedConfig = Object.assign({}, this.#commonClientConfig);

    mergedConfig.kafkaJS = Object.assign({}, mergedConfig.kafkaJS) ?? {};

    if (typeof config.kafkaJS === 'object') {
      mergedConfig.kafkaJS = Object.assign(mergedConfig.kafkaJS, config.kafkaJS);
      delete config.kafkaJS;
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
    const disallowedKey = checkIfKafkaJsKeysPresent('producer', config ?? {});
    if (disallowedKey !== null) {
      throw new error.KafkaJSError(CompatibilityErrorMessages.kafkaJSClientKey(disallowedKey, 'producer'));
    }

    return new Producer(this.#mergeConfiguration(config));
  }

  /**
   * Creates a new consumer.
   * @param {import("../../types/kafkajs").ConsumerConstructorConfig} config
   * @returns {Consumer}
   */
  consumer(config) {
    const disallowedKey = checkIfKafkaJsKeysPresent('consumer', config ?? {});
    if (disallowedKey !== null) {
      throw new error.KafkaJSError(CompatibilityErrorMessages.kafkaJSClientKey(disallowedKey, 'consumer'));
    }

    return new Consumer(this.#mergeConfiguration(config));
  }

  admin(config) {
    const disallowedKey = checkIfKafkaJsKeysPresent('admin', config ?? {});
    if (disallowedKey !== null) {
      throw new error.KafkaJSError(CompatibilityErrorMessages.kafkaJSClientKey(disallowedKey, 'admin'));
    }

    return new Admin(this.#mergeConfiguration(config));
  }
}

module.exports = {
  Kafka,
  ...error, logLevel,
  PartitionAssigners,
  PartitionAssignors: PartitionAssigners,
  CompressionTypes,
  ConsumerGroupStates,
  AclOperationTypes };
