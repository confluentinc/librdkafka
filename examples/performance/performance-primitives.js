const { Kafka, ErrorCodes, CompressionTypes } = require('../../').KafkaJS;
const { randomBytes } = require('crypto');
const { hrtime } = require('process');

module.exports = {
    runProducer,
    runConsumer,
    runConsumeTransformProduce,
    runCreateTopics,
};

async function runCreateTopics(brokers, topic, topic2) {
    const kafka = new Kafka({
        'client.id': 'kafka-test-performance',
        "metadata.broker.list": brokers,
    });

    const admin = kafka.admin();
    await admin.connect();

    for (let t of [topic, topic2]) {
        let topicCreated = await admin.createTopics({
            topics: [{ topic: t, numPartitions: 3 }],
        }).catch(console.error);
        if (topicCreated) {
            console.log(`Created topic ${t}`);
            continue;
        }

        console.log(`Topic ${t} already exists, deleting and recreating.`);
        await admin.deleteTopics({ topics: [t] }).catch(console.error);
        await new Promise(resolve => setTimeout(resolve, 1000)); /* Propagate. */
        await admin.createTopics({
            topics: [
                { topic: t, numPartitions: 3 },
            ],
        }).catch(console.error);
        console.log(`Created topic ${t}`);
        await new Promise(resolve => setTimeout(resolve, 1000)); /* Propagate. */
    }

    await admin.disconnect();
}

async function runProducer(brokers, topic, batchSize, warmupMessages, totalMessageCnt, msgSize, compression) {
    let totalMessagesSent = 0;
    let totalBytesSent = 0;

    const message = {
        value: randomBytes(msgSize),
    }

    const messages = Array(batchSize).fill(message);

    const kafka = new Kafka({
        'client.id': 'kafka-test-performance',
        'metadata.broker.list': brokers,
        'compression.codec': CompressionTypes[compression],
    });

    const producer = kafka.producer();
    await producer.connect();

    console.log('Sending ' + warmupMessages + ' warmup messages.');
    while (warmupMessages > 0) {
        await producer.send({
            topic,
            messages,
        });
        warmupMessages -= batchSize;
    }
    console.log('Sent warmup messages');

    // Now that warmup is done, start measuring...
    let startTime;
    let promises = [];
    startTime = hrtime();
    let messagesDispatched = 0;

    // The double while-loop allows us to send a bunch of messages and then
    // await them all at once. We need the second while loop to keep sending
    // in case of queue full errors, which surface only on awaiting.
    while (totalMessageCnt == -1 || messagesDispatched < totalMessageCnt) {
        while (totalMessageCnt == -1 || messagesDispatched < totalMessageCnt) {
            promises.push(producer.send({
                topic,
                messages,
            }).then(() => {
                totalMessagesSent += batchSize;
                totalBytesSent += batchSize * msgSize;
            }).catch((err) => {
                if (err.code === ErrorCodes.ERR__QUEUE_FULL) {
                    /* do nothing, just send them again */
                    messagesDispatched -= batchSize;
                } else {
                    console.error(err);
                    throw err;
                }
            }));
            messagesDispatched += batchSize;
        }
        await Promise.all(promises);
    }
    let elapsed = hrtime(startTime);
    let durationNanos = elapsed[0] * 1e9 + elapsed[1];
    let rate = (totalBytesSent / durationNanos) * 1e9 / (1024 * 1024); /* MB/s */
    console.log(`Sent ${totalMessagesSent} messages, ${totalBytesSent} bytes; rate is ${rate} MB/s`);

    await producer.disconnect();
    return rate;
}

async function runConsumer(brokers, topic, totalMessageCnt) {
    const kafka = new Kafka({
        'client.id': 'kafka-test-performance',
        'metadata.broker.list': brokers,
    });

    const consumer = kafka.consumer({
        'group.id': 'test-group' + Math.random(),
        'enable.auto.commit': false,
        'auto.offset.reset': 'earliest',
    });
    await consumer.connect();
    await consumer.subscribe({ topic });

    let messagesReceived = 0;
    let totalMessageSize = 0;
    let startTime;
    let rate;
    consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            messagesReceived++;
            totalMessageSize += message.value.length;
            if (messagesReceived === 1) {
                consumer.pause([{ topic }]);
            } else if (messagesReceived === 2) {
                startTime = hrtime();
            } else if (messagesReceived === totalMessageCnt) {
                let elapsed = hrtime(startTime);
                let durationNanos = elapsed[0] * 1e9 + elapsed[1];
                rate = (totalMessageSize / durationNanos) * 1e9 / (1024 * 1024); /* MB/s */
                console.log(`Recvd ${messagesReceived} messages, ${totalMessageSize} bytes; rate is ${rate} MB/s`);
                consumer.pause([{ topic }]);
                // } else if (messagesReceived % 100 == 0) {
                //     console.log(`Recvd ${messagesReceived} messages, ${totalMessageSize} bytes`);
            }
        }
    });

    // Wait until the first message is received
    await new Promise((resolve) => {
        let interval = setInterval(() => {
            if (messagesReceived > 0) {
                clearInterval(interval);
                resolve();
            }
        }, 100);
    });

    console.log("Starting consumer.")

    totalMessageSize = 0;
    consumer.resume([{ topic }]);
    await new Promise((resolve) => {
        let interval = setInterval(() => {
            if (messagesReceived >= totalMessageCnt) {
                clearInterval(interval);
                resolve();
            }
        }, 1000);
    });

    await consumer.disconnect();
    return rate;
}

async function runConsumeTransformProduce(brokers, consumeTopic, produceTopic, totalMessageCnt, messageProcessTimeMs, ctpConcurrency) {
    const kafka = new Kafka({
        'client.id': 'kafka-test-performance',
        'metadata.broker.list': brokers,
    });

    const producer = kafka.producer({
        /* We want things to be flushed immediately as we'll be awaiting this. */
        'linger.ms': 0
    });
    await producer.connect();

    const consumer = kafka.consumer({
        'group.id': 'test-group' + Math.random(),
        'enable.auto.commit': false,
        'auto.offset.reset': 'earliest',

        /* These fields are more-or-less required for cases where eachMessage includes
         * any async operatiosn, else `partitionsConsumedConcurrently` does not have
         * much effect. Reason for this is that, internally, librdkafka fetches
         * a large number of messages from one topic partition and that fills the
         * cache up, and we end up underutilizing concurrency.
         * TODO: remove or change these, discuss this issue and make changes in the code. */
        'message.max.bytes': 1000,
        'fetch.max.bytes': 1000,
    });
    await consumer.connect();
    await consumer.subscribe({ topic: consumeTopic });

    let messagesReceived = 0;
    let totalMessageSize = 0;
    let startTime;
    let rate;
    consumer.run({
        partitionsConsumedConcurrently: ctpConcurrency,
        eachMessage: async ({ topic, partition, message }) => {
            /* Simulate message processing for messageProcessTimeMs */
            if (messageProcessTimeMs > 0) {
                await new Promise((resolve) => setTimeout(resolve, messageProcessTimeMs));
            }
            await producer.send({
                topic: produceTopic,
                messages: [{ value: message.value }],
            })
            messagesReceived++;
            totalMessageSize += message.value.length;
            if (messagesReceived === 1) {
                consumer.pause([{ topic }]);
            } else if (messagesReceived === 2) {
                startTime = hrtime();
            } else if (messagesReceived === totalMessageCnt) {
                let elapsed = hrtime(startTime);
                let durationNanos = elapsed[0] * 1e9 + elapsed[1];
                rate = (totalMessageSize / durationNanos) * 1e9 / (1024 * 1024); /* MB/s */
                console.log(`Recvd, transformed and sent ${messagesReceived} messages, ${totalMessageSize} bytes; rate is ${rate} MB/s`);
                consumer.pause([{ topic }]);
                // } else if (messagesReceived % 1 == 0) {
                //     console.log(`Recvd ${messagesReceived} messages, ${totalMessageSize} bytes`);
            }
        }
    });

    // Wait until the first message is received
    await new Promise((resolve) => {
        let interval = setInterval(() => {
            if (messagesReceived > 0) {
                clearInterval(interval);
                resolve();
            }
        }, 100);
    });

    console.log("Starting consume-transform-produce.")

    totalMessageSize = 0;
    consumer.resume([{ topic: consumeTopic }]);
    await new Promise((resolve) => {
        let interval = setInterval(() => {
            if (messagesReceived >= totalMessageCnt) {
                clearInterval(interval);
                resolve();
            }
        }, 1000);
    });

    await consumer.disconnect();
    await producer.disconnect();
    return rate;
}
