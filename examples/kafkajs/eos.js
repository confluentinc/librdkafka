const { Kafka } = require('../..').KafkaJS
//const { Kafka } = require('kafkajs')

async function eosStart() {
    const kafka = new Kafka({
        kafkaJs: {
            brokers: ['<fill>'],
            ssl: true,
            sasl: {
                mechanism: 'plain',
                username: '<fill>',
                password: '<fill>',
            }
        }
    });

    const consumer = kafka.consumer({
        kafkaJs: {
            groupId: 'groupId',
            autoCommit: false,
        }
    });

    const producer = kafka.producer({
        kafkaJs: {
            transactionalId: 'txid'
        }
    });

    await consumer.connect();
    await producer.connect();

    await consumer.subscribe({
        topics: ["consumeTopic"]
    });

    // Similar to https://github.com/tulios/kafkajs/issues/1221
    // The run method acts like a consume-transform-produce loop.
    consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            const msgAckString = JSON.stringify({
                topic,
                partition,
                offset: message.offset,
                key: message.key?.toString(),
                value: message.value.toString()
            });

            console.log(msgAckString);

            try {
                const transaction = await producer.transaction();

                await transaction.send({
                    topic: 'produceTopic',
                    messages: [
                        { value: 'consumed a message: ' + msgAckString },
                    ]
                });

                await transaction.sendOffsets({
                    // Either a consumer can be used, which is typically used to consume
                    // in the EOS consume-transform-produce looop.
                    // Or use consumer group id (like KafkaJS - but it's recommended to use consumer).
                    consumer,
                    // consumerGroupId: 'groupdId',
                    topics: [
                        {
                            topic,
                            partitions: [
                                { partition, offset: message.offset },
                            ],
                        }
                    ],
                });

                await transaction.commit();

            } catch (e) {
                console.log({ e, s: "ERROR" });
                await transaction.abort();
            }
        },
    });

    const disconnect = async () => {
        process.off('SIGINT', disconnect);
        process.off('SIGTERM', disconnect);
        await consumer.disconnect();
        await producer.disconnect();
    }
    process.on('SIGINT', disconnect);
    process.on('SIGTERM', disconnect);
}

eosStart();
