const { Producer } = require('./_producer');
const { Consumer } = require('./_consumer');


class Kafka {
    #commonClientConfig = {};

    constructor(config) {
        this.#commonClientConfig = config;
    }

    #mergeConfiguration(config) {
      let baseConfig = Object.assign({}, this.#commonClientConfig);
      config = Object.assign({}, config);

      let rdKafka = baseConfig.rdKafka;
      Object.assign(baseConfig, config);
      if (rdKafka && config.rdKafka) {
        baseConfig.rdKafka = {
          ...rdkafka,
          ...config.rdKafka
        }
      }
      return baseConfig;
    }

    producer(config) {
        return new Producer(this.#mergeConfiguration(config));
    }

    consumer(config) {
      return new Consumer(this.#mergeConfiguration(config));
    }
}

module.exports = { Kafka }
