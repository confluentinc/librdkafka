const crypto = require('crypto');
const process = require('process');
const { Kafka } = require('../../lib').KafkaJS;

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

function makeConfig(config, common) {
    const kafkaJS =  Object.assign(config, clusterInformation.kafkaJS);
    if (debug) {
        common['debug'] = debug;
    }

    return Object.assign(common, { kafkaJS });
}

function createConsumer(config, common = {}) {
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

module.exports = {
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
};
