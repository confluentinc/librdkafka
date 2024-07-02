jest.setTimeout(30000);

const {
    createConsumer,
    createProducer,
    secureRandom,
    createTopic,
    waitForMessages,
    waitFor,
    sleep,
} = require('../testhelpers')

describe('Consumer seek >', () => {
    let topicName, groupId, producer, consumer;

    beforeEach(async () => {
        console.log("Starting:", expect.getState().currentTestName);
        topicName = `test-topic-${secureRandom()}`;
        groupId = `consumer-group-id-${secureRandom()}`;

        producer = createProducer({});

        consumer = createConsumer({
            groupId,
            fromBeginning: true,
        });
    });

    afterEach(async () => {
        consumer && (await consumer.disconnect());
        producer && (await producer.disconnect());
        console.log("Ending:", expect.getState().currentTestName);
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
            });


            describe('when "enable.auto.commit" is false', () => {
                beforeEach(() => {
                    consumer = createConsumer({
                        groupId,
                        fromBeginning: true,
                        autoCommit: false,
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
                        fromBeginning: true,
                        autoCommit: false,
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
                });
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

            });

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

            });

            it('uses the last seek for a given topic/partition', async () => {
                await consumer.connect()
                await producer.connect()

                const value1 = secureRandom()
                const message1 = { key: `key-0`, value: `value-${value1}`, partition: 0 }
                const value2 = secureRandom()
                const message2 = { key: `key-0`, value: `value-${value2}`, partition: 0 }
                const value3 = secureRandom()
                const message3 = { key: `key-0`, value: `value-${value3}`, partition: 0 }

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
            });
        });
    });

    describe('batch staleness >', () => {
        it('stops consuming messages after staleness', async () => {
            consumer = createConsumer({
                groupId,
                maxWaitTimeInMs: 500,
                fromBeginning: true,
            });

            const messages = Array(10)
                .fill()
                .map(() => {
                    const value = secureRandom()
                    return { key: `key-${value}`, value: `value-${value}`, partition: 0 }
                });

            await consumer.connect();
            await producer.connect();
            await producer.send({ topic: topicName, messages });
            await consumer.subscribe({ topic: topicName });

            const offsetsConsumed = [];

            consumer.run({
                eachMessage: async ({ message }) => {
                    offsetsConsumed.push(message.offset)

                    if (offsetsConsumed.length === 1) {
                        consumer.seek({ topic: topicName, partition: 0, offset: message.offset });
                    }
                },
            })

            await waitFor(() => offsetsConsumed.length >= 2, () => { }, { delay: 50 })

            expect(offsetsConsumed[0]).toEqual(offsetsConsumed[1])
        });

        it('resolves a batch as stale when seek was called while processing it', async () => {
            consumer = createConsumer({
                groupId,
                // make sure we fetch a batch of messages
                minBytes: 1024,
                maxWaitTimeInMs: 500,
                fromBeginning: true,
                autoCommit: true,
            })

            const messages = Array(10)
                .fill()
                .map(() => {
                    const value = secureRandom()
                    return { key: `key-${value}`, value: `value-${value}` }
                });

            await consumer.connect();
            await producer.connect();
            await producer.send({ topic: topicName, messages });
            await consumer.subscribe({ topic: topicName });

            const offsetsConsumed = [];

            consumer.run({
                eachBatch: async ({ batch, isStale, resolveOffset }) => {
                    for (const message of batch.messages) {
                        if (isStale()) break;

                        offsetsConsumed.push(message.offset)

                        if (offsetsConsumed.length === 1) {
                            consumer.seek({ topic: topicName, partition: 0, offset: +message.offset })
                        }

                        resolveOffset(message.offset)
                    }
                },
            })

            await waitFor(() => offsetsConsumed.length >= 2, () => null, { delay: 50 })

            expect(offsetsConsumed[0]).toEqual(offsetsConsumed[1])
        });

        it('resolves a batch as stale when seek is called from outside eachBatch', async () => {
            consumer = createConsumer({
                groupId,
                maxWaitTimeInMs: 500,
                fromBeginning: true,
                autoCommit: true,
            })

            const messages = Array(10)
                .fill()
                .map(() => {
                    const value = secureRandom()
                    return { key: `key-${value}`, value: `value-${value}`, partition: 0 }
                });

            await consumer.connect();
            await producer.connect();
            await producer.send({ topic: topicName, messages });
            await consumer.subscribe({ topic: topicName });

            const offsetsConsumed = [];

            consumer.run({
                eachBatch: async ({ batch, isStale, resolveOffset }) => {
                    for (const message of batch.messages) {
                        if (isStale()) break;

                        offsetsConsumed.push(message.offset)

                        /* Slow things down so we can call seek predictably. */
                        await sleep(1000);

                        resolveOffset(message.offset)
                    }
                },
            })

            await waitFor(() => offsetsConsumed.length === 1, () => null, { delay: 50 })
            consumer.seek({ topic: topicName, partition: 0, offset: offsetsConsumed[0] });

            await waitFor(() => offsetsConsumed.length >= 2, () => null, { delay: 50 })

            expect(offsetsConsumed[0]).toEqual(offsetsConsumed[1])
        });

        it('resolves a batch as stale when pause was called while processing it', async () => {
            consumer = createConsumer({
                groupId,
                maxWaitTimeInMs: 500,
                fromBeginning: true,
                autoCommit: true,
            })

            const numMessages = 100;
            const messages = Array(numMessages)
                .fill()
                .map(() => {
                    const value = secureRandom()
                    return { key: `key-${value}`, value: `value-${value}` }
                });

            await consumer.connect();
            await producer.connect();
            await producer.send({ topic: topicName, messages });
            await consumer.subscribe({ topic: topicName });

            const offsetsConsumed = [];

            let resume;
            consumer.run({
                eachBatchAutoResolve: true,
                eachBatch: async ({ batch, isStale, resolveOffset, pause }) => {
                    for (const message of batch.messages) {
                        if (isStale()) break;

                        offsetsConsumed.push(message.offset)

                        if (offsetsConsumed.length === Math.floor(numMessages/2)) {
                            resume = pause();
                        }

                        resolveOffset(message.offset);
                    }
                },
            })

            /* Despite eachBatchAutoResolve being true, it shouldn't resolve offsets on its own.
             * However, manual resolution of offsets should still count. */
            await waitFor(() => offsetsConsumed.length >= numMessages/2, () => null, { delay: 50 });

            resume();

            /* Since we've properly resolved all offsets before pause, including the offset that we paused at,
             * there is no repeat. */
            await waitFor(() => offsetsConsumed.length >= numMessages, () => null, { delay: 50 })
            expect(offsetsConsumed.length).toBe(numMessages);

            expect(+offsetsConsumed[Math.floor(numMessages/2)]).toEqual(+offsetsConsumed[Math.floor(numMessages/2) + 1] - 1)
        });

        /* Skip as it uses consumer events */
        it.skip('skips messages fetched while seek was called', async () => {
            consumer = createConsumer({
                cluster: createCluster(),
                groupId,
                maxWaitTimeInMs: 1000,
                logger: newLogger(),
            })

            const messages = Array(10)
                .fill()
                .map(() => {
                    const value = secureRandom()
                    return { key: `key-${value}`, value: `value-${value}` }
                })
            await producer.connect()
            await producer.send({ topic: topicName, messages })

            await consumer.connect()

            await consumer.subscribe({ topic: topicName })

            const offsetsConsumed = []

            const eachBatch = async ({ batch, heartbeat }) => {
                for (const message of batch.messages) {
                    offsetsConsumed.push(message.offset)
                }

                await heartbeat()
            }

            consumer.run({
                eachBatch,
            })

            await waitForConsumerToJoinGroup(consumer)

            await waitFor(() => offsetsConsumed.length === messages.length, { delay: 50 })
            await waitForNextEvent(consumer, consumer.events.FETCH_START)

            const seekedOffset = offsetsConsumed[Math.floor(messages.length / 2)]
            consumer.seek({ topic: topicName, partition: 0, offset: seekedOffset })
            await producer.send({ topic: topicName, messages }) // trigger completion of fetch

            await waitFor(() => offsetsConsumed.length > messages.length, { delay: 50 })

            expect(offsetsConsumed[messages.length]).toEqual(seekedOffset)
        });
    });
})
