const LibrdKafkaError = require('../error');
const RdKafka = require('../rdkafka');
const { kafkaJSToRdKafkaConfig } = require('./_common');

const ConsumerState = Object.freeze({
  INIT:   0,
  CONNECTING:  1,
  CONNECTED: 2,
  DISCONNECTING: 3,
  DISCONNECTED: 4,
});

class Consumer {
  #kJSConfig = null
  #rdKafkaConfig = null;
  #internalClient = null;
  #connectPromiseFunc = {};
  #state = ConsumerState.INIT;

  constructor(kJSConfig) {
    this.#kJSConfig = kJSConfig;
  }

  #config() {
    if (!this.#rdKafkaConfig)
      this.#rdKafkaConfig = this.#finalizedConfig();
    return this.#rdKafkaConfig;
  }

  async #finalizedConfig() {
    const config = await kafkaJSToRdKafkaConfig(this.#kJSConfig);
    if (this.#kJSConfig.groupId) {
      config['group.id'] = this.#kJSConfig.groupId;
    }
    config['offset_commit_cb'] = true;
    if (this.#kJSConfig.rebalanceListener) {
      config['rebalance_cb'] = (err, assignment) => {
        // Create the librdkafka error
        err = LibrdKafkaError.create(err);

        let call;
        switch(err.code) {
          case LibrdKafkaError.codes.ERR__ASSIGN_PARTITIONS:
            call = (this.#kJSConfig.rebalanceListener.onPartitionsAssigned ?
                   this.#kJSConfig.rebalanceListener.onPartitionsAssigned(assignment) :
                   Promise.resolve()).catch(console.error);
            break;
          case LibrdKafkaError.codes.ERR__REVOKE_PARTITIONS:
            call = (this.#kJSConfig.rebalanceListener.onPartitionsRevoked ?
                    this.#kJSConfig.rebalanceListener.onPartitionsRevoked(assignment) :
                    Promise.resolve()).catch(console.error);
            break;
          default:
            call = Promise.reject().catch(() => {
              console.error(`Unexpected rebalanceListener error code ${err.code}`);
            });
            break;
        }

        call
        .finally(() => {
          // Emit the event
          this.#internalClient.emit('rebalance', err, assignment);

          try {
            if (err.code === LibrdKafkaError.codes.ERR__ASSIGN_PARTITIONS) {
              this.#internalClient.assign(assignment);
            } else {
              this.#internalClient.unassign();
            }
          } catch (e) {
            // Ignore exceptions if we are not connected
            if (this.#internalClient.isConnected()) {
              this.#internalClient.emit('rebalance.error', e);
            }
          }
        });
      };
    }
    return config;
  }

  #readyCb(arg) {
      if (this.#state !== ConsumerState.CONNECTING) {
          // I really don't know how to handle this now.
          return;
      }
      this.#state = ConsumerState.CONNECTED;

      // Resolve the promise.
      this.#connectPromiseFunc['resolve']();
  }

  #errorCb(args) {
      console.log('error', args);
      if (this.#state === ConsumerState.CONNECTING) {
          this.#connectPromiseFunc['reject'](args);
      } else {
          // do nothing for now.
      }
  }

  #notImplemented() {
    throw new Error('Not implemented');
  }

  #createPayload(message) {
    var key = message.key == null ? null : message.key;
    if (typeof key === 'string') {
      key = Buffer.from(key);
    }

    let timestamp = message.timestamp ? new Date(message.timestamp).toISOString()
                    : '';

    var headers = undefined;
    if (message.headers) {
      headers = {}
      for (const [key, value] of Object.entries(message.headers)) {
        if (!headers[key]) {
          headers[key] = value;
        } else if (headers[key].constructor === Array) {
          headers[key].push(value);
        } else {
          headers[key] = [headers[key], value];
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
        offset: message.offset,
        size: message.size,
        headers
      },
      heartbeat: async () => {},
      pause: () => {}
    }
  }

  async #consumeSingle() {
    return new Promise((resolve, reject) => {
      this.#internalClient.consume(1, function(err, messages) {
        if (err) {
          reject(`Consume error code ${err.code}`);
          return;
        }

        const message = messages[0];
        resolve(message);
      });
    });
  }

  #topicPartitionOffsetToRdKafka(tpo) {
    return {
      topic: tpo.topic,
      partition: tpo.partition,
      offset: Number(tpo.offset),
    }
  }

  #flattenTopicPartitions(topics) {
    const ret = [];
    for (let topic of topics) {
      if (topic.partition != null)
        ret.push({
          topic: topic.topic,
          partition: topic.partition
        });
      else {
        for (let partition of topic.partitions) {
          ret.push({topic: topic.topic, partition});
        }
      }
    }
    return ret;
  }

  async connect() {
      if (this.#state !== ConsumerState.INIT) {
          return Promise.reject('Connect has already been called elsewhere.');
      }

      this.#state = ConsumerState.CONNECTING;
      this.#internalClient = new RdKafka.KafkaConsumer(await this.#config());
      this.#internalClient.on('ready', this.#readyCb.bind(this));
      this.#internalClient.on('event.error', this.#errorCb.bind(this));
      this.#internalClient.on('event.log', console.log);

      return new Promise((resolve, reject) => {
          this.#connectPromiseFunc = {resolve, reject};
          console.log('Connecting....');
          this.#internalClient.connect();
          console.log('connect() called');
      });
  }

  async subscribe(subscription) {
    this.#internalClient.subscribe(subscription.topics);
  }

  async stop() {
    this.#notImplemented();
  }

  async run(config) {
    if (this.#state !== ConsumerState.CONNECTED) {
        throw new Error('Run must be called in state CONNECTED.');
    }

    while (this.#state === ConsumerState.CONNECTED) {
      let m = await this.#consumeSingle();
      if (m) {
        await config.eachMessage(
          this.#createPayload(m)
        )
      }
    }
  }

  async commitOffsets(topicPartitions = null) {
    try {
      if (topicPartitions == null) {
        this.#internalClient.commitSync();
      } else {
        const topicPartitions = topicPartitions.map(
          this.#topicPartitionOffsetToRdKafka);
        this.#internalClient.commitSync(topicPartitions);
      }
    } catch (e) {
      if (!e.code || e.code != LibrdKafkaError.codes.ERR__NO_OFFSET) {
        throw e;
      }
    }
  }

  seek(topicPartitionOffset) {
    return new Promise((resolve, reject) => {
      const rdKafkaTopicPartitionOffset =
      this.#topicPartitionOffsetToRdKafka(topicPartitionOffset);
      this.#internalClient.seek(rdKafkaTopicPartitionOffset, 0, (err) => {
        if (err) {
          reject(new Error(`Seek error code ${err.code}`));
        } else {
          resolve();
        }
      });
    }).catch(console.error); // Default handler
  }

  async describeGroup() {
    this.#notImplemented();
  }

  pause(topics) {
    topics = this.#flattenTopicPartitions(topics);
    this.#internalClient.pause(topics);
  }

  paused() {
    this.#notImplemented();
  }

  assignment() {
    return this.#flattenTopicPartitions(this.#internalClient.assignments());
  }

  resume(topics) {
    topics = this.#flattenTopicPartitions(topics);
    this.#internalClient.resume(topics);
  }

  on(eventName, listener) {
    this.#notImplemented();
  }

  logger() {
    this.#notImplemented();
  }

  get events() {
    this.#notImplemented();
  }

  async disconnect() {
    if (this.#state >= ConsumerState.DISCONNECTING) {
      return;
    }
    this.#state = ConsumerState.DISCONNECTING;
    await new Promise((resolve, reject) => {
      const cb = (err) => {
        err ? reject(err) : resolve();
        this.#state = ConsumerState.DISCONNECTED;
      }
      this.#internalClient.disconnect(cb);
    });
  }
}

module.exports = { Consumer }
