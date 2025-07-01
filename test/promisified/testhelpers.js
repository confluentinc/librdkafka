const crypto = require('crypto');
const process = require('process');
const { Kafka } = require('../../lib').KafkaJS;
const { DeferredPromise } = require('../../lib/kafkajs/_common');

// TODO: pick this up from a file
const clusterInformation = {
    kafkaJS: {
        brokers: process.env.KAFKA_HOST ? process.env.KAFKA_HOST.split(',') : ['localhost:9092'],
    },
    librdkafka: {
        'bootstrap.servers': process.env.KAFKA_HOST ? process.env.KAFKA_HOST : 'localhost:9092',
    },
};

const debug = process.env.TEST_DEBUG;

function testConsumerGroupProtocol() {
    return process.env.TEST_CONSUMER_GROUP_PROTOCOL ?? null;
}

function testConsumerGroupProtocolClassic() {
    const protocol = testConsumerGroupProtocol();
    return protocol === null || protocol === "classic";
}

function makeConfig(config, common) {
    const kafkaJS =  Object.assign(config, clusterInformation.kafkaJS);
    if (debug) {
        common['debug'] = debug;
    } else { /* Turn off excessive logging unless specifically asked for, otherwise stdout gets very crowded. */
        common['log_level'] = 5;
    }

    return Object.assign(common, { kafkaJS });
}

function createConsumer(config, common = {}) {
    const protocol = testConsumerGroupProtocol();
    if (protocol !== null && !('group.protocol' in common)) {
        common['group.protocol'] = protocol;
    }
    if (!testConsumerGroupProtocolClassic()) {
        const forbiddenProperties = [
            "session.timeout.ms",
            "partition.assignment.strategy",
            "heartbeat.interval.ms",
            "group.protocol.type"
        ];
        const forbiddenPropertiesKafkaJS = [
            "sessionTimeout",
            "partitionAssignors",
            "partitionAssigners",
            "heartbeatInterval"
        ];
        for (const prop of forbiddenProperties) {
            if (prop in common) {
                delete common[prop];
            }
        }
        for (const prop of forbiddenPropertiesKafkaJS) {
            if (prop in config) {
                delete config[prop];
            }
        }
    }
    const kafka = new Kafka(makeConfig(config, common));
    return kafka.consumer();
}

function createProducer(config, common = {}) {
    const kafka = new Kafka(makeConfig(config, common));
    return kafka.producer();
}

function createAdmin(config, common = {}) {
    const kafka = new Kafka(makeConfig(config, common));
    return kafka.admin();
}

function secureRandom(length = 10) {
    return `${crypto.randomBytes(length).toString('hex')}-${process.pid}-${crypto.randomUUID()}`;
}

async function createTopic(args) {
    const { topic, partitions } = args;
    const admin = createAdmin({});
    await admin.connect();
    await admin.createTopics({
        topics: [
            { topic, numPartitions: partitions ?? 1 }
        ]
    });
    /* Wait for topic to propagate in the metadata. */
    await sleep(500);
    await admin.disconnect();
}

async function waitForConsumerToJoinGroup(/* consumer is passed as the first argument, and ignored */) {
    // We don't yet have a deterministic way to test this, so we just wait for a bit.
    // TODO: we can probably wait for consumer.assignment() to be not empty, but that only
    // works if the assignment exists.
    return new Promise(resolve => setTimeout(resolve, 2500));
}

async function waitFor(check, resolveValue, { delay = 50 } = {}) {
    return new Promise(resolve => {
        const interval = setInterval(() => {
            if (check()) {
                clearInterval(interval);
                resolve(resolveValue());
            }
        }, delay);
    });
}

async function waitForMessages(messagesConsumed, { number = 1, delay } = {}) {
    return waitFor(() => messagesConsumed.length >= number, () => messagesConsumed, { delay });
}

async function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

const generateMessages = options => {
    const { prefix, number = 100, partition } = options || {};
    const prefixOrEmpty = prefix ? `-${prefix}` : '';

    return Array(number)
        .fill()
        .map((v, i) => {
            const value = secureRandom();
            const message = {
                key: `key${prefixOrEmpty}-${i}-${value}`,
                value: `value${prefixOrEmpty}-${i}-${value}`,
            };
            if (partition !== undefined) {
                message.partition = partition;
            }
            return message;
        });
};

/**
 * Represents a list of promises that can be resolved in sequence or
 * in a different order and awaited multiple times.
 * Useful for testing particular ordering of async operations without
 * relying of timing.
 */
class SequentialPromises {
    #promises;
    #current = 0;

    constructor(num) {
        this.#promises = Array(num).fill().map(() => new DeferredPromise());
    }

    get(index) {
      return this.#promises[index];
    }

    resolveNext(value) {
      this.#promises[this.#current].resolve(value);
      this.#current++;
    }
}

module.exports = {
    testConsumerGroupProtocolClassic,
    createConsumer,
    createProducer,
    createAdmin,
    secureRandom,
    waitForMessages,
    createTopic,
    waitForConsumerToJoinGroup,
    waitFor,
    sleep,
    generateMessages,
    clusterInformation,
    SequentialPromises,
    DeferredPromise,
};
