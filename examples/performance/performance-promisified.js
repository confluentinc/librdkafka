const { Kafka, CompressionTypes, ErrorCodes } = require('../../').KafkaJS;
const { randomBytes } = require('crypto');
const { hrtime } = require('process');

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
        'compression.codec': compression,
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
    console.log({messagesDispatched, totalMessageCnt})
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
    startTime = hrtime();
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

const brokers = process.env.KAFKA_BROKERS || 'localhost:9092';
const topic = process.env.KAFKA_TOPIC || 'test-topic';
const messageCount = process.env.MESSAGE_COUNT ? +process.env.MESSAGE_COUNT : 1000000;
const messageSize = process.env.MESSAGE_SIZE ? +process.env.MESSAGE_SIZE : 256;
const batchSize = process.env.BATCH_SIZE ? +process.env.BATCH_SIZE : 100;
const compression = process.env.COMPRESSION || CompressionTypes.NONE;
const warmupMessages = process.env.WARMUP_MESSAGES ? +process.env.WARMUP_MESSAGES : (batchSize * 10);

runProducer(brokers, topic, batchSize, warmupMessages, messageCount, messageSize, compression).then(async (producerRate) => {
    const consumerRate = await runConsumer(brokers, topic, messageCount);
    console.log(producerRate, consumerRate);
});
