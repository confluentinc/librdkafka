const { Producer } = require('./_producer');
const { Consumer } = require('./_consumer');


class Kafka {
    #commonClientConfig = {};

    constructor(config) {
        this.#commonClientConfig = config;
    }

    producer(config) {
        if (config === null || !(config instanceof Object)) {
            config = {};
        }

        config = Object.assign(config, this.#commonClientConfig);
        return new Producer(config);
    }

    consumer(config) {
      if (config === null || !(config instanceof Object)) {
        config = {};
    }

      config = Object.assign(config, this.#commonClientConfig);
      return new Consumer(config);
    }
}

module.exports = { Kafka }
