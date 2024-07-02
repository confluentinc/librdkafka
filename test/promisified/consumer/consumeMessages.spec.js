jest.setTimeout(30000)

const { CompressionTypes } = require('../../../lib').KafkaJS;
const {
    secureRandom,
    createTopic,
    waitFor,
    createProducer,
    createConsumer,
    waitForMessages,
    waitForConsumerToJoinGroup,
    sleep,
} = require('../testhelpers');

/* All variations of partitionsConsumedConcurrently */
const cases = Array(3).fill().map((_, i) => [(i % 3) + 1]);

describe.each(cases)('Consumer', (partitionsConsumedConcurrently) => {
    let topicName, groupId, producer, consumer;
    const partitions = 3;

    beforeEach(async () => {
        console.log("Starting:", expect.getState().currentTestName, "| partitionsConsumedConcurrently =", partitionsConsumedConcurrently);
        topicName = `test-topic-${secureRandom()}`
        groupId = `consumer-group-id-${secureRandom()}`

        await createTopic({ topic: topicName, partitions })
        producer = createProducer({});

        consumer = createConsumer({
            groupId,
            maxWaitTimeInMs: 100,
            fromBeginning: true,
            autoCommit: true,
        });
    });

    afterEach(async () => {
        consumer && (await consumer.disconnect())
        producer && (await producer.disconnect())
        console.log("Ending:", expect.getState().currentTestName, "| partitionsConsumedConcurrently =", partitionsConsumedConcurrently);
    });

    it('consume messages', async () => {
        await consumer.connect();
        await producer.connect();
        await consumer.subscribe({ topic: topicName })

        const messagesConsumed = [];
        consumer.run({
            partitionsConsumedConcurrently,
            eachMessage: async event => messagesConsumed.push(event)
        });

        const messages = Array(10)
            .fill()
            .map(() => {
                const value = secureRandom()
                return { key: `key-${value}`, value: `value-${value}`, partition: 0 }
            })

        await producer.send({ topic: topicName, messages })
        await waitForMessages(messagesConsumed, { number: messages.length })

        expect(messagesConsumed[0]).toEqual(
            expect.objectContaining({
                topic: topicName,
                partition: 0,
                message: expect.objectContaining({
                    key: Buffer.from(messages[0].key),
                    value: Buffer.from(messages[0].value),
                    offset: '0',
                }),
            })
        )

        expect(messagesConsumed[messagesConsumed.length - 1]).toEqual(
            expect.objectContaining({
                topic: topicName,
                partition: 0,
                message: expect.objectContaining({
                    key: Buffer.from(messages[messages.length - 1].key),
                    value: Buffer.from(messages[messages.length - 1].value),
                    offset: '' + (messagesConsumed.length - 1),
                }),
            })
        )

        // check if all offsets are present
        expect(messagesConsumed.map(m => m.message.offset)).toEqual(messages.map((_, i) => `${i}`))
    });

    it('consume messages with headers', async () => {
        await consumer.connect();
        await producer.connect();
        await consumer.subscribe({ topic: topicName })

        const messagesConsumed = [];
        consumer.run({
            partitionsConsumedConcurrently,
            eachMessage: async event => messagesConsumed.push(event)
        });

        const messages = [{
            value: `value-${secureRandom}`,
            headers: {
                'header-1': 'value-1',
                'header-2': 'value-2',
                'header-3': ['value-3-1', 'value-3-2', Buffer.from([1, 0, 1, 0, 1])],
                'header-4': Buffer.from([1, 0, 1, 0, 1]),
            },
            partition: 0,
        }]

        await producer.send({ topic: topicName, messages })
        await waitForMessages(messagesConsumed, { number: messages.length })

        expect(messagesConsumed[0]).toEqual(
            expect.objectContaining({
                topic: topicName,
                partition: 0,
                message: expect.objectContaining({
                    value: Buffer.from(messages[0].value),
                    offset: '0',
                    headers: {
                        // Headers are always returned as Buffers from the broker.
                        'header-1': Buffer.from('value-1'),
                        'header-2': Buffer.from('value-2'),
                        'header-3': [Buffer.from('value-3-1'), Buffer.from('value-3-2'), Buffer.from([1, 0, 1, 0, 1])],
                        'header-4': Buffer.from([1, 0, 1, 0, 1]),
                    }
                }),
            })
        )
    });

    it.each([[true], [false]])('consumes messages using eachBatch', async (isAutoResolve) => {
        await consumer.connect();
        await producer.connect();
        await consumer.subscribe({ topic: topicName })

        const messagesConsumed = [];
        consumer.run({
            partitionsConsumedConcurrently,
            eachBatchAutoResolve: isAutoResolve,
            eachBatch: async event => {
                // Match the message format to be checked easily later.
                event.batch.messages = event.batch.messages.map(msg => ({
                    message: msg,
                    topic: event.batch.topic,
                    partition: event.batch.partition,
                }));
                messagesConsumed.push(...event.batch.messages);

                // If we're not auto-resolving, we need to resolve the offsets manually.
                if (!isAutoResolve)
                    event.resolveOffset(event.batch.messages[event.batch.messages.length - 1].message.offset);
            }
        });

        const messages = Array(100 * partitions)
            .fill()
            .map((_, i) => {
                const value = secureRandom()
                return { key: `key-${value}`, value: `value-${value}`, partition: i % partitions }
            })

        await producer.send({ topic: topicName, messages })
        await waitForMessages(messagesConsumed, { number: messages.length })

        for (let p = 0; p < partitions; p++) {
            const specificPartitionMessages = messagesConsumed.filter(m => m.partition === p);
            const specificExpectedMessages = messages.filter(m => m.partition === p);
            expect(specificPartitionMessages[0]).toEqual(
                expect.objectContaining({
                    topic: topicName,
                    partition: p,
                    message: expect.objectContaining({
                        key: Buffer.from(specificExpectedMessages[0].key),
                        value: Buffer.from(specificExpectedMessages[0].value),
                        offset: String(0),
                    }),
                })
            );

            expect(specificPartitionMessages[specificPartitionMessages.length - 1]).toEqual(
                expect.objectContaining({
                    topic: topicName,
                    partition: p,
                    message: expect.objectContaining({
                        key: Buffer.from(specificExpectedMessages[specificExpectedMessages.length - 1].key),
                        value: Buffer.from(specificExpectedMessages[specificExpectedMessages.length - 1].value),
                        offset: String(specificExpectedMessages.length - 1),
                    }),
                })
            );

            // check if all offsets are present
            expect(specificPartitionMessages.map(m => m.message.offset)).toEqual(specificExpectedMessages.map((_, i) => `${i}`))
        }

    });

    it('partially resolving offsets in eachBatch leads to reconsumption', async () => {
        await consumer.connect();
        await producer.connect();
        await consumer.subscribe({ topic: topicName });

        const messagesConsumed = [];
        const skippedResolutionForPartition = Array(partitions).fill(false);
        const messagesPerPartition = 100;
        consumer.run({
            partitionsConsumedConcurrently,
            eachBatchAutoResolve: false,
            eachBatch: async (event) => {
                const partition = event.batch.partition;
                let maxOffset = -1;
                for (const message of event.batch.messages) {
                    const offset = +message.offset;
                    maxOffset = offset;
                    messagesConsumed.push(message);
                    /* If we get a message greater than the halfway point, don't resolve it the first time around
                     * Only resolve it when we see it the second time. */
                    if (offset < Math.floor(messagesPerPartition/2) || skippedResolutionForPartition[partition]) {
                        event.resolveOffset(offset);
                    }
                }
                /* If we've completed the first half of messages, then we are now allowed to resolve
                 * the second half. */
                if (maxOffset >= Math.floor(messagesPerPartition/2))
                    skippedResolutionForPartition[partition] = true;
            }
        });

        const messages = Array(messagesPerPartition * partitions)
            .fill()
            .map((_, i) => {
                const value = secureRandom()
                return { key: `key-${value}`, value: `value-${value}`, partition: i % partitions }
            })

        await producer.send({ topic: topicName, messages });

        /* It's not possible to actually know the exact number of messages without knowing the
         * cache growth characteristics, which may change in the future. So just check if there
         * is at least 1 message more than we sent. */
        await waitForMessages(messagesConsumed, { number: messages.length + 1 });
    });

    it('is able to reconsume messages after not resolving it', async () => {
        await consumer.connect();
        await producer.connect();
        await consumer.subscribe({ topic: topicName })

        let messageSeen = false;
        const messagesConsumed = [];
        consumer.run({
            partitionsConsumedConcurrently,
            eachBatchAutoResolve: false,
            eachBatch: async event => {
                expect(event.batch.messages.length).toEqual(1);
                expect(event.batch.messages[0].offset).toEqual('0');
                expect(event.batch.topic).toEqual(topicName);
                expect(event.batch.partition).toEqual(0);

                if (!messageSeen) {
                    messageSeen = true;
                    return;
                }
                messagesConsumed.push(...event.batch.messages);

                // Since we're not auto-resolving, we need to resolve the offsets manually.
                event.resolveOffset(event.batch.messages[event.batch.messages.length - 1].offset);
            }
        });

        const messages = Array(1)
            .fill()
            .map(() => {
                const value = secureRandom()
                return { key: `key-${value}`, value: `value-${value}`, partition: 0 }
            })

        await producer.send({ topic: topicName, messages });
        await waitFor(() => consumer.assignment().length > 0, () => { }, 100);
        await waitForMessages(messagesConsumed, { number: messages.length });
    });

    it.each([[true], [false]])('is able to reconsume messages when an error is thrown', async (isAutoResolve) => {
        await consumer.connect();
        await producer.connect();
        await consumer.subscribe({ topic: topicName })

        let messageSeen = false;
        const messagesConsumed = [];
        consumer.run({
            partitionsConsumedConcurrently,
            eachBatchAutoResolve: isAutoResolve,
            eachBatch: async event => {
                expect(event.batch.messages.length).toEqual(1);
                expect(event.batch.messages[0].offset).toEqual('0');
                expect(event.batch.topic).toEqual(topicName);
                expect(event.batch.partition).toEqual(0);

                if (!messageSeen) {
                    messageSeen = true;
                    throw new Error('a new error.');
                }
                messagesConsumed.push(...event.batch.messages);
            }
        });

        const messages = Array(1)
            .fill()
            .map(() => {
                const value = secureRandom()
                return { key: `key-${value}`, value: `value-${value}`, partition: 0 };
            })

        await producer.send({ topic: topicName, messages });
        await waitForMessages(messagesConsumed, { number: messages.length });
    });

    it.each([[true], [false]])('does not reconsume resolved messages even on error', async (isAutoResolve) => {
        await consumer.connect();
        await producer.connect();
        await consumer.subscribe({ topic: topicName })

        const messagesConsumed = [];
        consumer.run({
            partitionsConsumedConcurrently,
            eachBatchAutoResolve: isAutoResolve,
            eachBatch: async event => {
                messagesConsumed.push(...event.batch.messages);
                // Resolve offsets irrespective of the value of eachBatchAutoResolve.
                event.resolveOffset(event.batch.messages[event.batch.messages.length - 1].offset);
                throw new Error('a new error.');
            }
        });

        const messages = Array(2)
            .fill()
            .map(() => {
                const value = secureRandom()
                return { key: `key-${value}`, value: `value-${value}`, partition: 0 }
            })

        await producer.send({ topic: topicName, messages })
        await waitForMessages(messagesConsumed, { number: messages.length });

        expect(messagesConsumed[0].key.toString()).toBe(messages[0].key);
        expect(messagesConsumed[1].key.toString()).toBe(messages[1].key);
    });

    it('consumes messages concurrently where partitionsConsumedConcurrently - partitions = diffConcurrencyPartitions', async () => {
        const partitions = 3;
        /* We want partitionsConsumedConcurrently to be 2, 3, and 4 rather than 1, 2, and 3 that is tested by the test. */
        const partitionsConsumedConcurrentlyDiff = partitionsConsumedConcurrently + 1;
        topicName = `test-topic-${secureRandom()}`
        await createTopic({
            topic: topicName,
            partitions: partitions,
        })
        await consumer.connect()
        await producer.connect()
        await consumer.subscribe({ topic: topicName })

        let inProgress = 0;
        let inProgressMaxValue = 0;
        const messagesConsumed = [];
        consumer.run({
            partitionsConsumedConcurrently: partitionsConsumedConcurrentlyDiff,
            eachMessage: async event => {
                inProgress++;
                await sleep(1);
                messagesConsumed.push(event);
                inProgressMaxValue = Math.max(inProgress, inProgressMaxValue)
                inProgress--;
            },
        })

        await waitFor(() => consumer.assignment().length > 0, () => { }, 100);

        const messages = Array(1024*9)
            .fill()
            .map((_, i) => {
                const value = secureRandom(512)
                return { key: `key-${value}`, value: `value-${value}`, partition: i % partitions }
            });

        await producer.send({ topic: topicName, messages });
        await waitForMessages(messagesConsumed, { number: messages.length });
        expect(inProgressMaxValue).toBe(Math.min(partitionsConsumedConcurrentlyDiff, partitions));
    });

    it('consume GZIP messages', async () => {
        /* Discard and recreate producer with the compression set */
        producer = createProducer({
            compression: CompressionTypes.GZIP,
        });

        await consumer.connect();
        await producer.connect();
        await consumer.subscribe({ topic: topicName });

        const messagesConsumed = [];
        consumer.run({ eachMessage: async event => messagesConsumed.push(event) });

        const key1 = secureRandom();
        const message1 = { key: `key-${key1}`, value: `value-${key1}`, partition: 0 };
        const key2 = secureRandom();
        const message2 = { key: `key-${key2}`, value: `value-${key2}`, partition: 0 };

        await producer.send({
            topic: topicName,
            messages: [message1, message2],
        });

        await expect(waitForMessages(messagesConsumed, { number: 2 })).resolves.toEqual([
            expect.objectContaining({
                topic: topicName,
                partition: 0,
                message: expect.objectContaining({
                    key: Buffer.from(message1.key),
                    value: Buffer.from(message1.value),
                    offset: '0',
                }),
            }),
            expect.objectContaining({
                topic: topicName,
                partition: 0,
                message: expect.objectContaining({
                    key: Buffer.from(message2.key),
                    value: Buffer.from(message2.value),
                    offset: '1',
                }),
            }),
        ])
    });

    /* Skip as it uses consumer events. */
    it.skip('commits the last offsets processed before stopping', async () => {
        jest.spyOn(cluster, 'refreshMetadataIfNecessary')

        await Promise.all([admin.connect(), consumer.connect(), producer.connect()])
        await consumer.subscribe({ topic: topicName })

        const messagesConsumed = []
        consumer.run({ eachMessage: async event => messagesConsumed.push(event) })
        await waitForConsumerToJoinGroup(consumer)

        // stop the consumer right after processing the batch, the offsets should be
        // committed in the end
        consumer.on(consumer.events.END_BATCH_PROCESS, async () => {
            await consumer.stop()
        })

        const messages = Array(100)
            .fill()
            .map(() => {
                const value = secureRandom()
                return { key: `key-${value}`, value: `value-${value}` }
            })

        await producer.send({ topic: topicName, messages })
        await waitForMessages(messagesConsumed, { number: messages.length })

        expect(cluster.refreshMetadataIfNecessary).toHaveBeenCalled()

        expect(messagesConsumed[0]).toEqual(
            expect.objectContaining({
                topic: topicName,
                partition: 0,
                message: expect.objectContaining({
                    key: Buffer.from(messages[0].key),
                    value: Buffer.from(messages[0].value),
                    offset: '0',
                }),
            })
        )

        expect(messagesConsumed[messagesConsumed.length - 1]).toEqual(
            expect.objectContaining({
                topic: topicName,
                partition: 0,
                message: expect.objectContaining({
                    key: Buffer.from(messages[messages.length - 1].key),
                    value: Buffer.from(messages[messages.length - 1].value),
                    offset: '99',
                }),
            })
        )

        // check if all offsets are present
        expect(messagesConsumed.map(m => m.message.offset)).toEqual(messages.map((_, i) => `${i}`))
        const response = await admin.fetchOffsets({ groupId, topics: [topicName] })
        const { partitions } = response.find(({ topic }) => topic === topicName)
        const partition = partitions.find(({ partition }) => partition === 0)
        expect(partition.offset).toEqual('100') // check if offsets were committed
    });

    it('stops consuming messages when running = false', async () => {
        await consumer.connect();
        await producer.connect();
        await consumer.subscribe({ topic: topicName });

        let calls = 0;

        consumer.run({
            eachMessage: async event => {
                calls++;
                await sleep(100);
            },
        });

        const key1 = secureRandom();
        const message1 = { key: `key-${key1}`, value: `value-${key1}` };
        const key2 = secureRandom();
        const message2 = { key: `key-${key2}`, value: `value-${key2}` };

        await producer.send({ topic: topicName, messages: [message1, message2] });
        await waitFor(() => calls > 0, () => { }, 10);
        await consumer.disconnect(); // don't give the consumer the chance to consume the 2nd message

        expect(calls).toEqual(1);
    });

    /* Skip as it uses consumer events */
    it.skip('discards messages received when pausing while fetch is in-flight', async () => {
        consumer = createConsumer({
            cluster: createCluster(),
            groupId,
            maxWaitTimeInMs: 200,
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

        consumer.pause([{ topic: topicName }])
        await producer.send({ topic: topicName, messages }) // trigger completion of fetch

        await waitForNextEvent(consumer, consumer.events.FETCH)

        expect(offsetsConsumed.length).toEqual(messages.length)
    });

    it('does not disconnect in the middle of message processing', async () => {
        await producer.connect();
        await consumer.connect();
        await consumer.subscribe({ topic: topicName });

        let calls = 0;
        let failedSeek = false;
        consumer.run({
            eachMessage: async ({ message }) => {
                /* Take a long time to process the message. */
                await sleep(7000);
                try {
                    consumer.seek({ topic: topicName, partition: 0, offset: message.offset });
                } catch (e) {
                    failedSeek = true;
                }
                calls++;
            }
        });

        await producer.send({
            topic: topicName,
            messages: [{ key: '1', value: '1' }],
        });

        /* Waiting for assignment and then a bit more means that the first eachMessage starts running. */
        await waitFor(() => consumer.assignment().length > 0, () => { }, { delay: 50 });
        await sleep(200);
        await consumer.disconnect();

        /* Even without explicitly waiting for it, a pending call to eachMessage must complete before disconnect does. */
        expect(calls).toEqual(1);
        expect(failedSeek).toEqual(false);

        await producer.disconnect();
    });
});
