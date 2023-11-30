const { Producer } = require('./_producer');
const { Consumer } = require('./_consumer');
const error = require('./_error');

class Kafka {
  #commonClientConfig = {};

  /**
   *
   * @param {import("../../types/kafkajs").KafkaConfig} config
   */
  constructor(config) {
    this.#commonClientConfig = config;
  }

  /**
   * Merge the producer/consumer specific configuration with the common configuration.
   * @param {import("../../types/kafkajs").ProducerConfig|import("../../types/kafkajs").ConsumerConfig} config
   * @returns
   */
  #mergeConfiguration(config) {
    let baseConfig = Object.assign({}, this.#commonClientConfig);
    config = Object.assign({}, config);

    // TODO: there's some confusion around this, as we currently allow
    // rdKafka to be a function, but here, we don't seem to treat it as such.
    // Correct this, so that only objects are allowed for `rdKafka`.
    let rdKafka = baseConfig.rdKafka;
    Object.assign(baseConfig, config);
    if (typeof rdKafka === 'object' && typeof config.rdKafka === 'object') {
      baseConfig.rdKafka = {
        ...rdKafka,
        ...config.rdKafka,
      };
    }
    return baseConfig;
  }

  /**
   * Creates a new producer.
   * @param {import("../../types/kafkajs").ProducerConfig} config
   * @returns {Producer}
   */
  producer(config) {
    return new Producer(this.#mergeConfiguration(config));
  }

  /**
   * Creates a new consumer.
   * @param {import("../../types/kafkajs").Consumer} config
   * @returns {Consumer}
   */
  consumer(config) {
    return new Consumer(this.#mergeConfiguration(config));
  }
}

module.exports = { Kafka, ...error };
