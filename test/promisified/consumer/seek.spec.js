const {
    createConsumer,
    createProducer,
    secureRandom,
    createTopic,
    waitForMessages,
} = require('../testhelpers')

describe('Consumer', () => {
    let topicName, groupId, producer, consumer;

    beforeEach(async () => {
        topicName = `test-topic-${secureRandom()}`;
        groupId = `consumer-group-id-${secureRandom()}`;

        producer = createProducer({});

        consumer = createConsumer({
            groupId,
            rdKafka: {
                topicConfig: {
                    'auto.offset.reset': 'earliest',
                },
            }
        });
    });

    afterEach(async () => {
        consumer && (await consumer.disconnect());
        producer && (await producer.disconnect());
    });

    describe('when seek offset', () => {
        describe('with one partition', () => {
            beforeEach(async () => {
                await createTopic({ topic: topicName, partitions: 1 })
            });

            it('throws an error if the topic is invalid', async () => {
                await consumer.connect();
                expect(() => consumer.seek({ topic: null })).toThrow('must be a string');
            });

            it('throws an error if the partition is not a number', async () => {
                await consumer.connect();
                expect(() => consumer.seek({ topic: topicName, partition: 'ABC' })).toThrow('Offset must be');
            });

            it('throws an error if the offset is not a number', async () => {
                await consumer.connect();
                expect(() => consumer.seek({ topic: topicName, partition: 0, offset: 'ABC' })).toThrow('Offset must be');
            });

            it('throws an error if the offset is negative and not a special offset', async () => {
                await consumer.connect();
                expect(() => consumer.seek({ topic: topicName, partition: 0, offset: '-32' })).toThrow('Offset must be');
            });

            it('recovers from offset out of range', async () => {
                await consumer.connect();
                await producer.connect();

                const key1 = secureRandom();
                const message1 = { key: `key-${key1}`, value: `value-${key1}` };

                await producer.send({ topic: topicName, messages: [message1] });
                await consumer.subscribe({ topic: topicName, });

                const messagesConsumed = [];
                consumer.seek({ topic: topicName, partition: 0, offset: 100 });
                consumer.run({
                    eachMessage: async event => {
                        messagesConsumed.push(event);
                    }
                });

                await expect(waitForMessages(messagesConsumed, { number: 1 })).resolves.toEqual([
                    expect.objectContaining({
                        topic: topicName,
                        partition: 0,
                        message: expect.objectContaining({ offset: '0' }),
                    }),
                ]);
            }, 10000);


            describe('When "enable.auto.commit" is false', () => {
                beforeEach(() => {
                    consumer = createConsumer({
                        groupId,
                        rdKafka: {
                            globalConfig: {
                                'enable.auto.commit': false,
                            },
                            topicConfig: {
                                'auto.offset.reset': 'earliest',
                            },
                        }
                    });
                });

                it('should not commit the offset', async () => {
                    await producer.connect();
                    await consumer.connect();

                    await producer.send({
                        topic: topicName,
                        messages: [1, 2, 3].map(n => ({ key: `key-${n}`, value: `value-${n}`, partition: 0 })),
                    });
                    await consumer.subscribe({ topic: topicName });

                    let messagesConsumed = []
                    consumer.seek({ topic: topicName, partition: 0, offset: 2 })
                    consumer.run({
                        eachMessage: async event => messagesConsumed.push(event),
                    });

                    await expect(waitForMessages(messagesConsumed, { number: 1 })).resolves.toEqual([
                        expect.objectContaining({
                            topic: topicName,
                            partition: 0,
                            message: expect.objectContaining({ offset: '2' }),
                        }),
                    ]);

                    /* We disconnect this consumer, and create another one of the same consumer group.
                     * This new consumer should start from 0, despite the fact that we've sought to 2 */
                    await consumer.disconnect();

                    consumer = createConsumer({
                        groupId,
                        rdKafka: {
                            globalConfig: {
                                'enable.auto.commit': false,
                            },
                            topicConfig: {
                                'auto.offset.reset': 'earliest',
                            },
                        }
                    });
                    await consumer.connect();
                    await consumer.subscribe({ topic: topicName });

                    messagesConsumed = [];
                    consumer.run({
                        eachMessage: async event => messagesConsumed.push(event),
                    });

                    await expect(waitForMessages(messagesConsumed, { number: 3 })).resolves.toEqual([
                        expect.objectContaining({
                            topic: topicName,
                            partition: 0,
                            message: expect.objectContaining({ offset: '0' }),
                        }),
                        expect.objectContaining({
                            topic: topicName,
                            partition: 0,
                            message: expect.objectContaining({ offset: '1' }),
                        }),
                        expect.objectContaining({
                            topic: topicName,
                            partition: 0,
                            message: expect.objectContaining({ offset: '2' }),
                        }),
                    ]);
                }, 10000);
            });
        });

        describe('with two partitions', () => {
            beforeEach(async () => {
                await createTopic({ topic: topicName, partitions: 2 })
            });

            it('updates the partition offset to the given offset', async () => {
                await consumer.connect();
                await producer.connect();

                const value1 = secureRandom();
                const message1 = { key: `key-1`, value: `value-${value1}`, partition: 1, };
                const value2 = secureRandom();
                const message2 = { key: `key-1`, value: `value-${value2}`, partition: 1, };
                const value3 = secureRandom();
                const message3 = { key: `key-1`, value: `value-${value3}`, partition: 1, };
                const value4 = secureRandom();
                const message4 = { key: `key-0`, value: `value-${value4}`, partition: 0, };

                await producer.send({
                    topic: topicName,
                    messages: [message1, message2, message3, message4],
                });

                await consumer.subscribe({ topic: topicName });

                const messagesConsumed = []
                consumer.seek({ topic: topicName, partition: 1, offset: 1 });
                consumer.run({
                    eachMessage: async event => {
                        messagesConsumed.push(event);
                    }
                });

                let check = await expect(waitForMessages(messagesConsumed, { number: 3 })).resolves;

                await check.toEqual(
                    expect.arrayContaining([
                        expect.objectContaining({
                            topic: topicName,
                            partition: 0,
                            message: expect.objectContaining({ offset: '0' }),
                        }),
                        expect.objectContaining({
                            topic: topicName,
                            partition: 1,
                            message: expect.objectContaining({ offset: '1' }),
                        }),
                        expect.objectContaining({
                            topic: topicName,
                            partition: 1,
                            message: expect.objectContaining({ offset: '2' }),
                        }),
                    ])
                );

                await check.toEqual(
                    expect.not.arrayContaining([
                        expect.objectContaining({
                            topic: topicName,
                            partition: 1,
                            message: expect.objectContaining({ offset: '0' }),
                        }),
                    ])
                );

            }, 10000);

            it('works for both partitions', async () => {
                await consumer.connect();
                await producer.connect();

                const value1 = secureRandom();
                const message1 = { key: `key-1`, value: `value-${value1}`, partition: 1, };
                const value2 = secureRandom();
                const message2 = { key: `key-1`, value: `value-${value2}`, partition: 1, };
                const value3 = secureRandom();
                const message3 = { key: `key-0`, value: `value-${value3}`, partition: 0 };
                const value4 = secureRandom();
                const message4 = { key: `key-0`, value: `value-${value4}`, partition: 0, };
                const value5 = secureRandom();
                const message5 = { key: `key-0`, value: `value-${value5}`, partition: 0 };

                await producer.send({
                    topic: topicName,
                    messages: [message1, message2, message3, message4, message5],
                });
                await consumer.subscribe({ topic: topicName })

                const messagesConsumed = [];
                consumer.seek({ topic: topicName, partition: 0, offset: 2 });
                consumer.seek({ topic: topicName, partition: 1, offset: 1 });
                consumer.run({
                    eachMessage: async event => {
                        messagesConsumed.push(event);
                    }
                });

                let check = await expect(waitForMessages(messagesConsumed, { number: 2 })).resolves;

                await check.toEqual(
                    expect.arrayContaining([
                        expect.objectContaining({
                            topic: topicName,
                            partition: 0,
                            message: expect.objectContaining({ offset: '2' }),
                        }),
                        expect.objectContaining({
                            topic: topicName,
                            partition: 1,
                            message: expect.objectContaining({ offset: '1' }),
                        }),
                    ])
                );

                await check.toEqual(
                    expect.not.arrayContaining([
                        expect.objectContaining({
                            topic: topicName,
                            partition: 0,
                            message: expect.objectContaining({ offset: '0' }),
                        }),
                        expect.objectContaining({
                            topic: topicName,
                            partition: 0,
                            message: expect.objectContaining({ offset: '1' }),
                        }),
                        expect.objectContaining({
                            topic: topicName,
                            partition: 1,
                            message: expect.objectContaining({ offset: '0' }),
                        }),
                    ])
                );

            }, 10000);

            it('uses the last seek for a given topic/partition', async () => {
                await consumer.connect()
                await producer.connect()

                const value1 = secureRandom()
                const message1 = { key: `key-0`, value: `value-${value1}` }
                const value2 = secureRandom()
                const message2 = { key: `key-0`, value: `value-${value2}` }
                const value3 = secureRandom()
                const message3 = { key: `key-0`, value: `value-${value3}` }

                await producer.send({ topic: topicName, messages: [message1, message2, message3] })
                await consumer.subscribe({ topic: topicName, })

                const messagesConsumed = []
                consumer.seek({ topic: topicName, partition: 0, offset: 0 });
                consumer.seek({ topic: topicName, partition: 0, offset: 1 });
                consumer.seek({ topic: topicName, partition: 0, offset: 2 });
                consumer.run({
                    eachMessage: async event => {
                        messagesConsumed.push(event);
                    }
                });

                let check = await expect(waitForMessages(messagesConsumed, { number: 1 })).resolves;

                await check.toEqual(
                    expect.arrayContaining([
                        expect.objectContaining({
                            topic: topicName,
                            partition: 0,
                            message: expect.objectContaining({ offset: '2' }),
                        }),
                    ])
                );

                await check.toEqual(
                    expect.not.arrayContaining([
                        expect.objectContaining({
                            topic: topicName,
                            partition: 0,
                            message: expect.objectContaining({ offset: '0' }),
                        }),
                        expect.objectContaining({
                            topic: topicName,
                            partition: 0,
                            message: expect.objectContaining({ offset: '1' }),
                        }),
                    ])
                );
            }, 10000);
        });
    });
})
