const { Kafka, CompressionTypes } = require('kafkajs');
const { randomBytes } = require('crypto');
const { hrtime } = require('process');

module.exports = {
    runProducer,
    runConsumer,
    runConsumeTransformProduce,
    runCreateTopics,
    runProducerConsumerTogether,
};

async function runCreateTopics(brokers, topic, topic2) {
    const kafka = new Kafka({
        clientId: 'kafka-test-performance',
        brokers: brokers.split(','),
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
        clientId: 'kafka-test-performance',
        brokers: brokers.split(','),
    });

    const producer = kafka.producer();
    await producer.connect();

    console.log('Sending ' + warmupMessages + ' warmup messages.');
    while (warmupMessages > 0) {
        await producer.send({
            topic,
            messages,
            compression: CompressionTypes[compression],
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
                compression: CompressionTypes[compression],
            }).then(() => {
                totalMessagesSent += batchSize;
                totalBytesSent += batchSize * msgSize;
            }).catch((err) => {
                console.error(err);
                throw err;
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
        clientId: 'kafka-test-performance',
        brokers: brokers.split(','),
    });

    const consumer = kafka.consumer({
        groupId: 'test-group' + Math.random(),
    });
    await consumer.connect();
    await consumer.subscribe({ topic, fromBeginning: true });

    let messagesReceived = 0;
    let messagesMeasured = 0;
    let totalMessageSize = 0;
    let startTime;
    let rate;
    const skippedMessages = 100;

    console.log("Starting consumer.");

    consumer.run({
        autoCommit: false,
        eachMessage: async ({ topic, partition, message }) => {
            messagesReceived++;

            if (messagesReceived >= skippedMessages) {
                messagesMeasured++;
                totalMessageSize += message.value.length;

                if (messagesReceived === skippedMessages) {
                    startTime = hrtime();
                } else if (messagesMeasured === totalMessageCnt) {
                    let elapsed = hrtime(startTime);
                    let durationNanos = elapsed[0] * 1e9 + elapsed[1];
                    rate = (totalMessageSize / durationNanos) * 1e9 / (1024 * 1024); /* MB/s */
                    console.log(`Recvd ${messagesMeasured} messages, ${totalMessageSize} bytes; rate is ${rate} MB/s`);
                    consumer.pause([{ topic }]);
                }
            }
        }
    });

    totalMessageSize = 0;

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

async function runConsumeTransformProduce(brokers, consumeTopic, produceTopic, warmupMessages, totalMessageCnt, messageProcessTimeMs, ctpConcurrency) {
    const kafka = new Kafka({
        clientId: 'kafka-test-performance',
        brokers: brokers.split(','),
    });

    const producer = kafka.producer({});
    await producer.connect();

    const consumer = kafka.consumer({
        groupId: 'test-group' + Math.random(),
    });
    await consumer.connect();
    await consumer.subscribe({ topic: consumeTopic, fromBeginning: true });

    let messagesReceived = 0;
    let messagesMeasured = 0;
    let totalMessageSize = 0;
    let startTime;
    let rate;
    const skippedMessages = warmupMessages;

    console.log("Starting consume-transform-produce.");

    consumer.run({
        autoCommit: false,
        partitionsConsumedConcurrently: ctpConcurrency,
        eachMessage: async ({ topic, partition, message }) => {
            messagesReceived++;

            if (messagesReceived >= skippedMessages) {
                messagesMeasured++;
                totalMessageSize += message.value.length;

                if (messagesReceived === skippedMessages)
                    startTime = hrtime();

                /* Simulate message processing for messageProcessTimeMs */
                if (messageProcessTimeMs > 0) {
                    await new Promise((resolve) => setTimeout(resolve, messageProcessTimeMs));
                }
                await producer.send({
                    topic: produceTopic,
                    messages: [{ value: message.value }],
                })

                if (messagesMeasured === totalMessageCnt) {
                    let elapsed = hrtime(startTime);
                    let durationNanos = elapsed[0] * 1e9 + elapsed[1];
                    rate = (totalMessageSize / durationNanos) * 1e9 / (1024 * 1024); /* MB/s */
                    console.log(`Recvd, transformed and sent ${messagesMeasured} messages, ${totalMessageSize} bytes; rate is ${rate} MB/s`);
                    consumer.pause([{ topic }]);
                }
            } else {
                await producer.send({
                    topic: produceTopic,
                    messages: [{ value: message.value }],
                })
            }
        }
    });

    totalMessageSize = 0;
    await new Promise((resolve) => {
        let interval = setInterval(() => {
            if (messagesMeasured >= totalMessageCnt) {
                clearInterval(interval);
                resolve();
            }
        }, 1000);
    });

    await consumer.disconnect();
    await producer.disconnect();
    return rate;
}


async function runProducerConsumerTogether(brokers, topic, totalMessageCnt, msgSize, produceMessageProcessTimeMs, consumeMessageProcessTimeMs) {
    const kafka = new Kafka({
        clientId: 'kafka-test-performance',
        brokers: brokers.split(','),
    });

    const producer = kafka.producer({});
    await producer.connect();

    let consumerReady = false;
    let consumerFinished = false;
    const consumer = kafka.consumer({
        groupId: 'test-group' + Math.random(),
    });
    await consumer.connect();
    await consumer.subscribe({ topic: topic, fromBeginning: true });

    let startTime = null;
    let diffs = [];
    console.log("Starting consumer.");

    consumer.run({
        autoCommit: false,
        eachMessage: async ({ topic, partition, message }) => {
            if (!consumerReady) {
                consumerReady = true;
                return;
            }
            if (startTime === null)
                return;
            let endTime = hrtime.bigint();
            diffs.push(endTime - startTime);
            await new Promise(resolve => setTimeout(resolve, consumeMessageProcessTimeMs));
            if (diffs.length >= totalMessageCnt) {
                consumerFinished = true;
            }
        }
    });

    const message = {
        value: randomBytes(msgSize),
    }

    // Don't initialize startTime here, the first message includes the metadata
    // request and isn't representative of latency measurements.
    await producer.send({
        topic,
        messages: [message],
    });
    // We don't want this to show up at all for our measurements, so make sure the
    // consumer processes this and ignores it before proceeding.
    await new Promise(resolve => setTimeout(resolve, 1000));

    while(!consumerReady) {
        await new Promise(resolve => setTimeout(resolve, 1000));
    }


    console.log("Starting producer.");

    for (let i = 0; i < totalMessageCnt; i++) {
        startTime = hrtime.bigint();
        await producer.send({
            topic,
            messages: [message],
        });
        await new Promise(resolve => setTimeout(resolve, produceMessageProcessTimeMs));
    }

    while (!consumerFinished) {
        await new Promise(resolve => setTimeout(resolve, 1000));
    }

    console.log("Consumer finished.");

    await consumer.disconnect();
    await producer.disconnect();

    const nanoDiffs = diffs.map(d => parseInt(d));
    const sortedDiffs = nanoDiffs.sort((a, b) => a - b);
    const p50 = sortedDiffs[Math.floor(sortedDiffs.length / 2)] / 1e6;
    const p90 = sortedDiffs[Math.floor(sortedDiffs.length * 0.9)] / 1e6;
    const p95 = sortedDiffs[Math.floor(sortedDiffs.length * 0.95)] / 1e6;
    const mean = nanoDiffs.reduce((acc, d) => acc + d, 0) / nanoDiffs.length / 1e6;
    // Count outliers: elements 10x or more than the p50.
    const outliers = sortedDiffs.map(d => d/1e6).filter(d => (d) > (10 * p50));
    return { mean, p50, p90, p95, outliers };
}
