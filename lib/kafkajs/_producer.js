const RdKafka = require('../rdkafka');
const { kafkaJSToRdKafkaConfig, topicPartitionOffsetToRdKafka } = require('./_common');
const { Consumer } = require('./_consumer');

const ProducerState = Object.freeze({
  INIT:   0,
  CONNECTING:  1,
  INITIALIZING_TRANSACTIONS: 2,
  INITIALIZED_TRANSACTIONS: 3,
  CONNECTED: 4,
  DISCONNECTING: 5,
  DISCONNECTED: 6,
});

class Producer {
  #kJSConfig = null
  #rdKafkaConfig = null;
  #internalClient = null;
  #connectPromiseFunc = {};
  #state = ProducerState.INIT;
  #ongoingTransaction = false;

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
    config.dr_cb = 'true';

    if (this.#kJSConfig.hasOwnProperty('transactionalId')) {
      config['transactional.id'] = this.#kJSConfig.transactionalId;
    }

    return config;
  }

  #flattenTopicPartitionOffsets(topics) {
    return topics.flatMap(topic => {
      return topic.partitions.map(partition => {
        return { partition: partition.partition, offset: partition.offset, topic: topic.topic };
      })
    })
  }

  #readyTransactions(err) {
      if (err) {
         this.#connectPromiseFunc["reject"](err);
         return;
      }

      if (this.#state !== ProducerState.INITIALIZING_TRANSACTIONS) {
          // FSM impossible state. We should add error handling for
          // this later.
          return;
      }

      this.#state = ProducerState.INITIALIZED_TRANSACTIONS;
      this.#readyCb(null);
  }

  async #readyCb(arg) {
      if (this.#state !== ProducerState.CONNECTING && this.#state !== ProducerState.INITIALIZED_TRANSACTIONS) {
          // I really don't know how to handle this now.
          return;
      }

      let config = await this.#config();
      if (config.hasOwnProperty('transactional.id') && this.#state !== ProducerState.INITIALIZED_TRANSACTIONS) {
          this.#state = ProducerState.INITIALIZING_TRANSACTIONS;
          this.#internalClient.initTransactions(5000 /* default: 5s */, this.#readyTransactions.bind(this));
          return;
      }

      this.#state = ProducerState.CONNECTED;

      // Start a loop to poll.
      let pollInterval = setInterval(() => {
          if (this.#state >= ProducerState.DISCONNECTING) {
              clearInterval(pollInterval);
              return;
          }
          this.#internalClient.poll();
      }, 500);

      this.#internalClient.on('delivery-report', function(err, report) {
          //console.log('got delivery report', report, err);
          const opaque = report.opaque;
          if (!opaque) {
              // not sure how to handle this.
              return;
          }
          if (err) {
              opaque.reject('err out');
              return;
          }
          //console.log('delivery-report: ' + JSON.stringify(report));
          delete report['opaque'];

          const recordMetadata = {
            topicName: report.topic,
            partition: report.partition,
            errorCode: 0,
            baseOffset: report.offset,
            logAppendTime: null,
            logStartOffset: null,
          }

          opaque.resolve(recordMetadata);
      });

      // Resolve the promise.
      this.#connectPromiseFunc["resolve"]();
  }

  #errorCb(args) {
      console.log('error', args);
      if (this.#state === ProducerState.CONNECTING) {
          this.#connectPromiseFunc["reject"](args);
      } else {
          // do nothing for now.
      }
  }

  async connect() {
      if (this.#state !== ProducerState.INIT) {
          return Promise.reject("Connect has already been called elsewhere.");
      }

      this.#state = ProducerState.CONNECTING;
      this.#internalClient = new RdKafka.Producer(await this.#config());
      this.#internalClient.on('ready', this.#readyCb.bind(this));
      this.#internalClient.on('event.error', this.#errorCb.bind(this));
      this.#internalClient.on('event.log', console.log);

      return new Promise((resolve, reject) => {
          this.#connectPromiseFunc = {resolve, reject};
          console.log("Connecting....");
          this.#internalClient.connect();
          console.log("connect() called");
      });
  }

  async disconnect() {
    if (this.#state >= ProducerState.DISCONNECTING) {
      return;
    }
    this.#state = ProducerState.DISCONNECTING;
    await new Promise((resolve, reject) => {
      const cb = (err) => {
        err ? reject(err) : resolve();
        this.#state = ProducerState.DISCONNECTED;
      }
      this.#internalClient.disconnect(5000, cb);
    });
  }

  async transaction() {
    if (this.#state !== ProducerState.CONNECTED) {
      return Promise.reject("Cannot start transaction without awaiting connect()");
    }

    if (this.#ongoingTransaction) {
      return Promise.reject("Can only start one transaction at a time.");
    }

    return new Promise((resolve, reject) => {
      this.#internalClient.beginTransaction((err) => {
        if (err) {
          reject(err);
          return;
        }
        this.#ongoingTransaction = true;

        // Resolve with 'this' because we don't need any specific transaction object.
        // Just using the producer works since we can only have one transaction
        // ongoing for one producer.
        resolve(this);
      });
    });
  }

  async commit() {
    if (this.#state !== ProducerState.CONNECTED) {
      return Promise.reject("Cannot commit without awaiting connect()");
    }

    if (!this.#ongoingTransaction) {
      return Promise.reject("Cannot commit, no transaction ongoing.");
    }

    return new Promise((resolve, reject) => {
      this.#internalClient.commitTransaction(5000 /* default: 5000ms */, err => {
        if (err) {
          // TODO: Do we reset ongoingTransaction here?
          reject(err);
          return;
        }
        this.#ongoingTransaction = false;
        resolve();
      });
    });
  }


  async abort() {
    if (this.#state !== ProducerState.CONNECTED) {
      return Promise.reject("Cannot abort without awaiting connect()");
    }

    if (!this.#ongoingTransaction) {
      return Promise.reject("Cannot abort, no transaction ongoing.");
    }

    return new Promise((resolve, reject) => {
      this.#internalClient.abortTransaction(5000 /* default: 5000ms */, err => {
        if (err) {
          // TODO: Do we reset ongoingTransaction here?
          reject(err);
          return;
        }
        this.#ongoingTransaction = false;
        resolve();
      });
    });
  }

  async sendOffsets(arg) {
    let { consumerGroupId, topics, consumer } = arg;

    if ((!consumerGroupId && !consumer) || !Array.isArray(topics) || topics.length === 0) {
      return Promise.reject("sendOffsets must have the arguments {consumerGroupId: string or consumer: Consumer, topics: non-empty array");
    }

    if (this.#state !== ProducerState.CONNECTED) {
      return Promise.reject("Cannot sendOffsets without awaiting connect()");
    }

    if (!this.#ongoingTransaction) {
      return Promise.reject("Cannot sendOffsets, no transaction ongoing.");
    }

    // If we don't have a consumer, we must create a consumer at this point internally.
    // This isn't exactly efficient, but we expect people to use either a consumer,
    // or we will need to change the C/C++ code to facilitate using the consumerGroupId
    // directly.
    // TODO: Change the C/C++ code to facilitate this if we go to release with this.

    let consumerCreated = false;
    if (!consumer) {
      const config = Object.assign({ groupId: consumerGroupId }, this.#kJSConfig);
      consumer = new Consumer(config);
      consumerCreated = true;
      await consumer.connect();
    }

    return new Promise((resolve, reject) => {
      this.#internalClient.sendOffsetsToTransaction(
        this.#flattenTopicPartitionOffsets(topics).map(topicPartitionOffsetToRdKafka),
        consumer._getInternalConsumer(),
        async err => {
          if (consumerCreated)
            await consumer.disconnect();
          if (err)
            reject(err);
          else
            resolve();
        })
    });
  }

  async send(sendOptions) {
      if (this.#state !== ProducerState.CONNECTED) {
          return Promise.reject("Cannot send message without awaiting connect()");
      }

      if (sendOptions === null || !(sendOptions instanceof Object)) {
          return Promise.reject("sendOptions must be set correctly");
      }

      // Ignore all properties except topic and messages.
      // TODO: log a warning instead of ignoring.
      if (!sendOptions.hasOwnProperty("topic") || !sendOptions.hasOwnProperty("messages") || !Array.isArray(sendOptions["messages"])) {
          // TODO: add further validations.
          return Promise.reject("sendOptions must be of the form {topic: string, messages: Message[]}");
      }

      const msgPromises = [];
      for (let i = 0; i < sendOptions.messages.length; i++) {
          const msg = sendOptions.messages[i];

          if (!msg.hasOwnProperty("partition") || msg.partition === null) {
              msg.partition = -1;
          }

          if (typeof msg.value === 'string') {
              msg.value = Buffer.from(msg.value);
          }

          msgPromises.push(new Promise((resolve, reject) => {
              const opaque = {resolve, reject};
              this.#internalClient.produce(sendOptions.topic, msg.partition, msg.value, msg.key, msg.timestamp ?? Date.now(), opaque, msg.headers);
          }));

      }
      const recordMetadataArr = await Promise.all(msgPromises);

      const topicPartitionRecordMetadata = new Map();
      for (const recordMetadata of recordMetadataArr) {
        const key = `${recordMetadata.topicName},${recordMetadata.partition}`;
        if (recordMetadata.baseOffset == null || !topicPartitionRecordMetadata.has(key)) {
          topicPartitionRecordMetadata.set(key, recordMetadata);
          continue;
        }

        const currentRecordMetadata = topicPartitionRecordMetadata.get(key);

        // Don't overwrite a null baseOffset
        if (currentRecordMetadata.baseOffset == null) {
          continue;
        }

        if (currentRecordMetadata.baseOffset > recordMetadata.baseOffset) {
          topicPartitionRecordMetadata.set(key, recordMetadata);
        }
      }

      const ret = [];
      for (const [key, value] of topicPartitionRecordMetadata.entries()) {
        value.baseOffset = value.baseOffset?.toString();
        ret.push(value);
      }
      return ret;
  }
}

module.exports = { Producer }
