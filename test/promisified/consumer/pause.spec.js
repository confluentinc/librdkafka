jest.setTimeout(30000);

const {
    secureRandom,
    createTopic,
    waitForMessages,
    waitFor,
    waitForConsumerToJoinGroup,
    createConsumer,
    createProducer,
} = require('../testhelpers');

describe('Consumer', () => {
    let consumer;
    let groupId, producer, topics;

    beforeEach(async () => {
        console.log("Starting:", expect.getState().currentTestName);
        topics = [`test-topic1-${secureRandom()}`, `test-topic2-${secureRandom()}`]
        groupId = `consumer-group-id-${secureRandom()}`

        for (const topic of topics) {
            await createTopic({ topic, partitions: 2 })
        }

        producer = createProducer({
        });

        consumer = createConsumer({
            groupId,
            maxWaitTimeInMs: 1,
            maxBytesPerPartition: 180,
            fromBeginning: true,
        });
    })

    afterEach(async () => {
        consumer && (await consumer.disconnect())
        producer && (await producer.disconnect())
        console.log("Ending:", expect.getState().currentTestName);
    })

    describe('when pausing', () => {
        it('throws an error if the topic is invalid', async () => {
            await consumer.connect();
            expect(() => consumer.pause([{ topic: null, partitions: [0] }])).toThrow('Topic must be a string');
        });

        it('throws an error if Consumer#connect has not been called', () => {
            expect(() => consumer.pause([{ topic: 'foo', partitions: [0] }])).toThrow('Pause can only be called while connected');
        });

        it('pauses the appropriate topic/partition when pausing via the eachMessage callback', async () => {
            await consumer.connect();
            await producer.connect();

            /* Send 4 of the same messages to each topic, in order to partition 0, 0, 1, 0 of that topic. */
            const messages = [0, 0, 1, 0].map(partition => {
                const key = secureRandom()
                return { key: `key-${key}`, value: `value-${key}`, partition }
            })

            /* Send the first 2 messages to each topic. */
            for (const topic of topics) {
                await producer.send({ topic, messages: messages.slice(0, 2) });
                await consumer.subscribe({ topic });
            }

            let shouldPause = true;
            let pauseMessageRecvd = false;
            const messagesConsumed = [];
            const resumeCallbacks = [];
            consumer.run({
                eachMessage: async event => {
                    const { topic, message, pause } = event;

                    const whichTopic = topics.indexOf(topic)
                    const whichMessage = messages.findIndex(m => String(m.key) === String(message.key))

                    /* In case we're at the 2nd message (idx = 1) for the first topic, pause the partition.
                     * It should be the 0th partition which gets paused. */
                    if (shouldPause && whichTopic === 0 && whichMessage === 1) {
                        resumeCallbacks.push(pause());
                        pauseMessageRecvd = true;
                        /* We throw an error to indicate to the runner that this message should be
                         * considered 'unprocessed'. */
                        throw new Error('bailing out');
                    }
                    messagesConsumed.push({
                        topic: whichTopic,
                        message: whichMessage,
                    });
                },
            });

            await waitForMessages(messagesConsumed, { number: 3 });
            /* Librdkafka provides no guarantee about message ordering beyond per-partition.
             * Encountering 3 messages is no guarantee of that we did manage to pause. */
            await waitFor(() => pauseMessageRecvd, () => { }, { delay: 100 });
            const [pausedTopic] = topics;
            expect(consumer.paused()).toEqual([{ topic: pausedTopic, partitions: [0] }])

            for (const topic of topics) {
                await producer.send({ topic, messages: messages.slice(2) })
            }
            await waitForMessages(messagesConsumed, { number: 6, delay: 10 })

            expect(messagesConsumed).toHaveLength(6)
            expect(messagesConsumed).toContainEqual({ topic: 0, message: 0 }) // partition 0
            expect(messagesConsumed).toContainEqual({ topic: 0, message: 2 }) // partition 1

            expect(messagesConsumed).toContainEqual({ topic: 1, message: 0 }) // partition 0
            expect(messagesConsumed).toContainEqual({ topic: 1, message: 1 }) // partition 0
            expect(messagesConsumed).toContainEqual({ topic: 1, message: 2 }) // partition 1
            expect(messagesConsumed).toContainEqual({ topic: 1, message: 3 }) // partition 0

            shouldPause = false;
            resumeCallbacks.forEach(resume => resume());

            await waitForMessages(messagesConsumed, { number: 8 })

            // these messages have to wait until the consumer has resumed
            expect(messagesConsumed).toHaveLength(8)
            expect(messagesConsumed).toContainEqual({ topic: 0, message: 1 }) // partition 0
            expect(messagesConsumed).toContainEqual({ topic: 0, message: 3 }) // partition 0
        }, 10000);

        it('avoids calling eachMessage again for paused topics/partitions when paused via consumer.pause', async () => {
            await consumer.connect()
            await producer.connect()
            const messages = [0, 0, 1, 0].map(partition => {
                const key = secureRandom()
                return { key: `key-${key}`, value: `value-${key}`, partition }
            })

            for (const topic of topics) {
                await producer.send({ topic, messages: messages.slice(0, 2) })
            }
            await consumer.subscribe({ topics, replace: true });

            let shouldPause = true
            const messagesConsumed = []
            consumer.run({
                eachMessage: async event => {
                    const { topic, message, partition } = event;

                    const whichTopic = topics.indexOf(topic)
                    const whichMessage = messages.findIndex(m => String(m.key) === String(message.key))

                    messagesConsumed.push({
                        topic: whichTopic,
                        message: whichMessage,
                    })

                    // here, we pause after the first message (0) on the first topic (0)
                    if (shouldPause && whichTopic === 0 && whichMessage === 0) {
                        consumer.pause([{ topic, partitions: [partition] }])
                        // we don't throw an exception here to ensure the loop calling us breaks on its own and doesn't call us again
                    }
                },
            })

            await waitForMessages(messagesConsumed, { number: 3 })
            const [pausedTopic] = topics
            expect(consumer.paused()).toEqual([{ topic: pausedTopic, partitions: [0] }])

            for (const topic of topics) {
                await producer.send({ topic, messages: messages.slice(2) })
            }
            await waitForMessages(messagesConsumed, { number: 6, delay: 10 })

            expect(messagesConsumed).toHaveLength(6)
            expect(messagesConsumed).toContainEqual({ topic: 0, message: 0 }) // partition 0
            expect(messagesConsumed).toContainEqual({ topic: 0, message: 2 }) // partition 1

            expect(messagesConsumed).toContainEqual({ topic: 1, message: 0 }) // partition 0
            expect(messagesConsumed).toContainEqual({ topic: 1, message: 1 }) // partition 0
            expect(messagesConsumed).toContainEqual({ topic: 1, message: 2 }) // partition 1
            expect(messagesConsumed).toContainEqual({ topic: 1, message: 3 }) // partition 0

            shouldPause = false
            consumer.resume(consumer.paused())

            await waitForMessages(messagesConsumed, { number: 8 })

            // these messages have to wait until the consumer has resumed
            expect(messagesConsumed).toHaveLength(8)
            expect(messagesConsumed).toContainEqual({ topic: 0, message: 1 }) // partition 0
            expect(messagesConsumed).toContainEqual({ topic: 0, message: 3 }) // partition 0
        }, 15000);

        it('pauses when pausing via the eachBatch callback', async () => {
            await consumer.connect()
            await producer.connect()
            const originalMessages = [0, 0, 0, 1].map(partition => {
                const key = secureRandom()
                return { key: `key-${key}`, value: `value-${key}`, partition }
            })

            for (const topic of topics) {
                await producer.send({ topic, messages: originalMessages })
                await consumer.subscribe({ topic })
            }

            let shouldPause = true
            const messagesConsumed = []
            const resumeCallbacks = []
            consumer.run({
                eachBatch: async event => {
                    const {
                        batch: { topic, messages },
                        pause,
                        resolveOffset,
                        commitOffsetsIfNecessary,
                    } = event
                    messages.every(message => {
                        const whichTopic = topics.indexOf(topic)
                        const whichMessage = originalMessages.findIndex(
                            m => String(m.key) === String(message.key)
                        )

                        if (shouldPause && whichTopic === 0 && whichMessage === 1) {
                            resumeCallbacks.push(pause())
                            return false
                        } else if (shouldPause && whichTopic === 1 && whichMessage === 3) {
                            resumeCallbacks.push(pause())
                            return false
                        }
                        messagesConsumed.push({
                            topic: whichTopic,
                            message: whichMessage,
                        })
                        resolveOffset(message.offset)
                        return true
                    })
                    await commitOffsetsIfNecessary()
                },
                eachBatchAutoResolve: false,
            })
            await waitForConsumerToJoinGroup(consumer)
            await waitForMessages(messagesConsumed, { number: 5 })
            expect(messagesConsumed.length).toEqual(5);
            expect(consumer.paused()).toContainEqual({ topic: topics[0], partitions: [0] })
            expect(consumer.paused()).toContainEqual({ topic: topics[1], partitions: [1] })
            shouldPause = false
            resumeCallbacks.forEach(resume => resume())
            await waitForMessages(messagesConsumed, { number: 8 })
            expect(consumer.paused()).toEqual([])
            expect(messagesConsumed).toContainEqual({ topic: 0, message: 1 })
            expect(messagesConsumed).toContainEqual({ topic: 1, message: 3 })
        }, 10000);

        it('does not fetch messages for the paused topic', async () => {
            await consumer.connect();
            await producer.connect();

            const key1 = secureRandom();
            const message1 = { key: `key-${key1}`, value: `value-${key1}`, partition: 0 };
            const key2 = secureRandom();
            const message2 = { key: `key-${key2}`, value: `value-${key2}`, partition: 1 };

            for (const topic of topics) {
                await producer.send({ topic, messages: [message1] });
            }
            await consumer.subscribe({ topics });

            const messagesConsumed = [];
            consumer.run({ eachMessage: async event => messagesConsumed.push(event) });
            await waitForMessages(messagesConsumed, { number: 2 });

            expect(consumer.paused()).toEqual([]);
            const [pausedTopic, activeTopic] = topics;
            consumer.pause([{ topic: pausedTopic }]);

            for (const topic of topics) {
                await producer.send({ topic, messages: [message2] });
            }

            const consumedMessages = await waitForMessages(messagesConsumed, { number: 3 });

            expect(consumedMessages.filter(({ topic }) => topic === pausedTopic)).toEqual([
                expect.objectContaining({
                    topic: pausedTopic,
                    partition: expect.any(Number),
                    message: expect.objectContaining({ offset: '0' }),
                }),
            ]);

            const byPartition = (a, b) => a.partition - b.partition
            expect(
                consumedMessages.filter(({ topic }) => topic === activeTopic).sort(byPartition)
            ).toEqual([
                expect.objectContaining({
                    topic: activeTopic,
                    partition: 0,
                    message: expect.objectContaining({ offset: '0' }),
                }),
                expect.objectContaining({
                    topic: activeTopic,
                    partition: 1,
                    message: expect.objectContaining({ offset: '0' }),
                }),
            ]);

            expect(consumer.paused()).toEqual([
                {
                    topic: pausedTopic,
                    partitions: [0, 1],
                },
            ]);
        }, 10000);

        it('does not fetch messages for the paused partitions', async () => {
            await consumer.connect();
            await producer.connect();

            const [topic] = topics;
            const partitions = [0, 1];

            const messages = Array(1)
                .fill()
                .map(() => {
                    const value = secureRandom()
                    return { key: `key-${value}`, value: `value-${value}` }
                });
            const forPartition = partition => message => ({ ...message, partition });

            for (const partition of partitions) {
                await producer.send({ topic, messages: messages.map(forPartition(partition)) });
            }
            await consumer.subscribe({ topic });

            const messagesConsumed = []
            consumer.run({ eachMessage: async event => messagesConsumed.push(event) });

            await waitForMessages(messagesConsumed, { number: messages.length * partitions.length });

            expect(consumer.paused()).toEqual([]);
            const [pausedPartition, activePartition] = partitions;
            consumer.pause([{ topic, partitions: [pausedPartition] }]);

            for (const partition of partitions) {
                await producer.send({ topic, messages: messages.map(forPartition(partition)) });
            }

            const consumedMessages = await waitForMessages(messagesConsumed, {
                number: messages.length * 3,
            });

            expect(consumedMessages.filter(({ partition }) => partition === pausedPartition)).toEqual(
                messages.map((message, i) =>
                    expect.objectContaining({
                        topic,
                        partition: pausedPartition,
                        message: expect.objectContaining({ offset: `${i}` }),
                    })
                )
            );

            expect(consumedMessages.filter(({ partition }) => partition !== pausedPartition)).toEqual(
                messages.concat(messages).map((message, i) =>
                    expect.objectContaining({
                        topic,
                        partition: activePartition,
                        message: expect.objectContaining({ offset: `${i}` }),
                    })
                )
            );

            expect(consumer.paused()).toEqual([
                {
                    topic,
                    partitions: [pausedPartition],
                },
            ]);
        }, 10000);
    });

    describe('when pausing and breaking the consumption', () => {
        it('does not process messages when consumption from topic is paused', async () => {
            const [topic] = topics;
            const key1 = secureRandom();
            const message1 = { key: `key-${key1}`, value: `value-${key1}`, partition: 0 };
            const messagesConsumed = [];
            let shouldThrow = true;

            await consumer.connect();
            await producer.connect();

            await producer.send({ topic, messages: [message1] });
            await consumer.subscribe({ topic });

            consumer.run({
                eachMessage: async event => {
                    messagesConsumed.push(event)
                    if (shouldThrow) {
                        consumer.pause([{ topic }])
                        throw new Error('Should fail')
                    }
                },
            });

            const consumedMessagesTillError = [
                ...(await waitForMessages(messagesConsumed, { delay: 100 })),
            ];

            shouldThrow = false;
            consumer.resume([{ topic }]);

            const consumedMessages = await waitForMessages(messagesConsumed, { number: 2 })

            expect(consumedMessagesTillError).toHaveLength(1)
            expect(consumedMessagesTillError).toEqual([
                expect.objectContaining({
                    topic,
                    partition: expect.any(Number),
                    message: expect.objectContaining({ offset: '0' }),
                }),
            ])
            expect(consumedMessages).toHaveLength(2)
            expect(consumedMessages).toEqual([
                expect.objectContaining({
                    topic,
                    partition: expect.any(Number),
                    message: expect.objectContaining({ offset: '0' }),
                }),
                expect.objectContaining({
                    topic,
                    partition: expect.any(Number),
                    message: expect.objectContaining({ offset: '0' }),
                }),
            ])
        }, 10000);

        it('does not process messages when consumption from topic-partition is paused', async () => {
            const [topic] = topics;
            const pausedPartition = 0;
            const key1 = secureRandom();
            const message1 = { key: `key-${key1}`, value: `value-${key1}`, partition: 0 };
            const key2 = secureRandom();
            const message2 = { key: `key-${key2}`, value: `value-${key2}`, partition: 1 };
            const messagesConsumed = [];
            let shouldThrow = true;

            await consumer.connect();
            await producer.connect();

            await producer.send({ topic, messages: [message1, message2] })
            await consumer.subscribe({ topic })

            consumer.run({
                eachMessage: async event => {
                    messagesConsumed.push(event)
                    if (shouldThrow && event.partition === pausedPartition) {
                        consumer.pause([{ topic, partitions: [pausedPartition] }])
                        throw new Error('Should fail')
                    }
                },
            });

            const consumedMessagesTillError = [
                ...(await waitForMessages(messagesConsumed, { number: 2 })),
            ];

            shouldThrow = false;
            consumer.resume([{ topic, partitions: [pausedPartition] }]);

            const consumedMessages = await waitForMessages(messagesConsumed, { number: 3 });

            expect(consumedMessagesTillError).toHaveLength(2);
            expect(consumedMessagesTillError).toEqual(
                expect.arrayContaining([
                    expect.objectContaining({
                        topic,
                        partition: 0,
                        message: expect.objectContaining({ offset: '0' }),
                    }),
                    expect.objectContaining({
                        topic,
                        partition: 1,
                        message: expect.objectContaining({ offset: '0' }),
                    }),
                ])
            );
            expect(consumedMessages).toHaveLength(3);
            expect(consumedMessages).toEqual(
                expect.arrayContaining([
                    expect.objectContaining({
                        topic,
                        partition: 0,
                        message: expect.objectContaining({ offset: '0' }),
                    }),
                    expect.objectContaining({
                        topic,
                        partition: 0,
                        message: expect.objectContaining({ offset: '0' }),
                    }),
                    expect.objectContaining({
                        topic,
                        partition: 1,
                        message: expect.objectContaining({ offset: '0' }),
                    }),
                ])
            );
        }, 10000);
    });

    describe('when all topics are paused', () => {
        it('does not fetch messages', async () => {
            consumer = createConsumer({
                groupId,
                maxWaitTimeInMs: 100,
                maxBytesPerPartition: 180,
            });

            await producer.connect();
            await consumer.connect();

            const [topic1, topic2] = topics;
            await consumer.subscribe({ topics: [topic1, topic2] });

            const eachMessage = jest.fn();
            consumer.run({ eachMessage });

            await waitFor(() => consumer.assignment().length > 0, () => { }, { delay: 10 });
            consumer.pause([{ topic: topic1 }, { topic: topic2 }]);

            const key1 = secureRandom();
            const message1 = { key: `key-${key1}`, value: `value-${key1}`, partition: 0 };

            await producer.send({ topic: topic1, messages: [message1] });
            await producer.send({ topic: topic2, messages: [message1] });

            expect(eachMessage).not.toHaveBeenCalled();
        })
    });

    describe('when resuming', () => {
        it('throws an error if the topic is invalid', async () => {
            await consumer.connect();
            expect(() => consumer.pause([{ topic: null, partitions: [0] }])).toThrow('Topic must be a string');
        });

        it('throws an error if Consumer#connect has not been called', () => {
            expect(() => consumer.resume([{ topic: 'foo', partitions: [0] }])).toThrow(
                'Resume can only be called while connected'
            );
        });

        it('resumes fetching from the specified topic', async () => {
            await consumer.connect();
            await producer.connect();

            const key = secureRandom();
            const message = { key: `key-${key}`, value: `value-${key}`, partition: 0 };

            await consumer.subscribe({ topics });

            const messagesConsumed = [];
            consumer.run({
                eachMessage: async event => {
                    return messagesConsumed.push(event);
                }
            });
            await waitFor(() => consumer.assignment().length > 0, () => { }, { delay: 10 });
            const [pausedTopic, activeTopic] = topics;
            consumer.pause([{ topic: pausedTopic }]);

            for (const topic of topics) {
                await producer.send({ topic, messages: [message] });
            }

            await waitForMessages(messagesConsumed, { number: 1 });

            consumer.resume([{ topic: pausedTopic }]);

            await expect(waitForMessages(messagesConsumed, { number: 2 })).resolves.toEqual([
                expect.objectContaining({
                    topic: activeTopic,
                    partition: 0,
                    message: expect.objectContaining({ offset: '0' }),
                }),
                expect.objectContaining({
                    topic: pausedTopic,
                    partition: 0,
                    message: expect.objectContaining({ offset: '0' }),
                }),
            ]);

            expect(consumer.paused()).toEqual([]);
        });

        it('resumes fetching from earlier paused partitions', async () => {
            await consumer.connect();
            await producer.connect();

            const [topic] = topics;
            const partitions = [0, 1];

            const messages = Array(1)
                .fill()
                .map(() => {
                    const value = secureRandom()
                    return { key: `key-${value}`, value: `value-${value}` }
                });
            const forPartition = partition => message => ({ ...message, partition });

            for (const partition of partitions) {
                await producer.send({ topic, messages: messages.map(forPartition(partition)) });
            }
            await consumer.subscribe({ topic });

            const messagesConsumed = [];
            consumer.run({ eachMessage: async event => messagesConsumed.push(event) });

            await waitForMessages(messagesConsumed, { number: messages.length * partitions.length });

            const [pausedPartition, activePartition] = partitions;
            consumer.pause([{ topic, partitions: [pausedPartition] }]);

            for (const partition of partitions) {
                await producer.send({ topic, messages: messages.map(forPartition(partition)) });
            }

            await waitForMessages(messagesConsumed, {
                number: messages.length * 3,
            });

            consumer.resume([{ topic, partitions: [pausedPartition] }]);

            const consumedMessages = await waitForMessages(messagesConsumed, {
                number: messages.length * 4,
            });

            expect(consumedMessages.filter(({ partition }) => partition === pausedPartition)).toEqual(
                messages.concat(messages).map((message, i) =>
                    expect.objectContaining({
                        topic,
                        partition: pausedPartition,
                        message: expect.objectContaining({ offset: `${i}` }),
                    })
                )
            )

            expect(consumedMessages.filter(({ partition }) => partition !== pausedPartition)).toEqual(
                messages.concat(messages).map((message, i) =>
                    expect.objectContaining({
                        topic,
                        partition: activePartition,
                        message: expect.objectContaining({ offset: `${i}` }),
                    })
                )
            )

            expect(consumer.paused()).toEqual([])
        }, 10000);
    });
})
