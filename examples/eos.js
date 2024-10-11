const { Kafka } = require('@confluentinc/kafka-javascript').KafkaJS;

async function eosStart() {
    const consumer = new Kafka().consumer({
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'test-group4',
        'enable.auto.commit': false,
        'auto.offset.reset': 'earliest',
    });

    const producer = new Kafka().producer({
        'bootstrap.servers': 'localhost:9092',
        'transactional.id': 'txid',
    });

    await consumer.connect();
    await producer.connect();

    await consumer.subscribe({
        topics: ["consumeTopic"]
    });

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
                    consumer,
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
