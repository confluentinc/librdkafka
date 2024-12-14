const { Producer, CompressionTypes } = require('./_producer');
const { Consumer, PartitionAssigners } = require('./_consumer');
const { Admin, ConsumerGroupStates, AclOperationTypes, IsolationLevel } = require('./_admin');
const error = require('./_error');
const { logLevel, checkIfKafkaJsKeysPresent, CompatibilityErrorMessages } = require('./_common');

/**
 * This class holds common configuration for clients, and an instance can be
 * used to create producers, consumers, or admin clients.
 * @memberof KafkaJS
 */
class Kafka {
  /** @type{import("../../types/kafkajs").CommonConstructorConfig} */
  #commonClientConfig = {};

  /**
   * The configuration provided will be shared across all clients created using
   * this Kafka object.
   *
   * Properties listed within [CONFIGURATION.md]{@link https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md}
   * can be used as keys for this object.
   *
   * A number of KafkaJS-compatible configuration options can be provided as an object
   * with the `kafkaJS` key as illustrated in [MIGRATION.md]{@link https://github.com/confluentinc/confluent-kafka-javascript/blob/dev_early_access_development_branch/MIGRATION.md}.
   * @param {object} config - The common configuration for all clients created using this Kafka object.
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
   * @private
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
   *
   * An object containing the configuration for the producer to be created. This
   * will be merged with the common configuration provided when creating the
   * {@link RdKafka.Kafka} object, and the same set of keys can be used.
   *
   * @param {object} config - The configuration for the producer to be created.
   * @returns {KafkaJS.Producer}
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
   *
   * An object containing the configuration for the consumer to be created. This
   * will be merged with the common configuration provided when creating the
   * {@link RdKafka.Kafka} object, and the same set of keys can be used.
   *
   * @param {object} config - The configuration for the consumer to be created.
   * @returns {KafkaJS.Consumer}
   */
  consumer(config) {
    const disallowedKey = checkIfKafkaJsKeysPresent('consumer', config ?? {});
    if (disallowedKey !== null) {
      throw new error.KafkaJSError(CompatibilityErrorMessages.kafkaJSClientKey(disallowedKey, 'consumer'));
    }

    return new Consumer(this.#mergeConfiguration(config));
  }

  /**
   * Creates a new admin client.
   *
   * An object containing the configuration for the admin client to be created. This
   * will be merged with the common configuration provided when creating the
   * {@link RdKafka.Kafka} object, and the same set of keys can be used.
   *
   * @param {object} config - The configuration for the admin client to be created.
   * @returns {KafkaJS.Admin}
   */
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
  AclOperationTypes,
  IsolationLevel};
