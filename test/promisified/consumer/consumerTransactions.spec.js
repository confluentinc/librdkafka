jest.setTimeout(10000);

const { ErrorCodes } = require('../../../lib').KafkaJS;
const {
    secureRandom,
    createTopic,
    createProducer,
    createConsumer,
    waitForMessages,
    generateMessages,
} = require('../testhelpers');

describe('Consumer transactions', () => {
    let topicName, groupId, producer, consumer;

    beforeEach(async () => {
        topicName = `test-topic-${secureRandom()}`;
        groupId = `consumer-group-id-${secureRandom()}`;

        await createTopic({ topic: topicName });
        producer = createProducer({
            idempotent: true,
            maxInFlightRequests: 1,
        });

        consumer = createConsumer({
            groupId,
            maxWaitTimeInMs: 100,
            fromBeginning: true,
            autoCommit: true,
        });
    });

    afterEach(async () => {
        consumer && (await consumer.disconnect());
        producer && (await producer.disconnect());
    });

    it('accepts messages from an idempotent producer', async () => {
        await consumer.connect();
        await producer.connect();
        await consumer.subscribe({ topic: topicName });

        const messagesConsumed = [];
        const idempotentMessages = generateMessages({ prefix: 'idempotent', partition: 0 });

        consumer.run({
            eachMessage: async event => messagesConsumed.push(event),
        });

        await producer.sendBatch({
            topicMessages: [{ topic: topicName, messages: idempotentMessages }],
        });

        const number = idempotentMessages.length;
        await waitForMessages(messagesConsumed, {
            number,
        });

        expect(messagesConsumed).toHaveLength(idempotentMessages.length);
        expect(messagesConsumed[0].message.value.toString()).toMatch(/value-idempotent-0/);
        expect(messagesConsumed[99].message.value.toString()).toMatch(/value-idempotent-99/);
    });

    it('accepts messages from committed transactions', async () => {
        producer = createProducer({
            transactionalId: `transactional-id-${secureRandom()}`,
            maxInFlightRequests: 1,
        });

        await consumer.connect();
        await producer.connect();
        await consumer.subscribe({ topic: topicName });

        const messagesConsumed = [];

        const messages1 = generateMessages({ prefix: 'txn1', partition: 0 });
        const messages2 = generateMessages({ prefix: 'txn2', partition: 0 });
        const nontransactionalMessages1 = generateMessages({ prefix: 'nontransactional1', number: 1, partition: 0 });

        consumer.run({
            eachMessage: async event => messagesConsumed.push(event),
        });

        // We cannot send non-transaction messages.
        await expect(producer.sendBatch({
            topicMessages: [{ topic: topicName, messages: nontransactionalMessages1 }],
        })).rejects.toHaveProperty('code', ErrorCodes.ERR__STATE);

        // We can run a transaction
        const txn1 = await producer.transaction();
        await txn1.sendBatch({
            topicMessages: [{ topic: topicName, messages: messages1 }],
        });
        await txn1.commit();

        // We can immediately run another transaction
        const txn2 = await producer.transaction();
        await txn2.sendBatch({
            topicMessages: [{ topic: topicName, messages: messages2 }],
        });
        await txn2.commit();

        const numMessages =
            messages1.length + messages2.length;

        await waitForMessages(messagesConsumed, {
            number: numMessages,
        });

        expect(messagesConsumed[0].message.value.toString()).toMatch(/value-txn1-0/);
        expect(messagesConsumed[numMessages - 1].message.value.toString()).toMatch(/value-txn2-99/);
    });

    it('does not receive aborted messages', async () => {
        producer = createProducer({
            transactionalId: `transactional-id-${secureRandom()}`,
            maxInFlightRequests: 1,
        });

        await consumer.connect();
        await producer.connect();
        await consumer.subscribe({ topic: topicName });

        const messagesConsumed = [];

        const abortedMessages1 = generateMessages({ prefix: 'aborted-txn-1', partition: 0 });
        const abortedMessages2 = generateMessages({ prefix: 'aborted-txn-2', partition: 0 });
        const committedMessages = generateMessages({ prefix: 'committed-txn', number: 10, partition: 0 });

        consumer.run({
            eachMessage: async event => messagesConsumed.push(event),
        });

        const abortedTxn1 = await producer.transaction();
        await abortedTxn1.sendBatch({
            topicMessages: [{ topic: topicName, messages: abortedMessages1 }],
        });
        await abortedTxn1.abort();

        const abortedTxn2 = await producer.transaction();
        await abortedTxn2.sendBatch({
            topicMessages: [{ topic: topicName, messages: abortedMessages2 }],
        });
        await abortedTxn2.abort();

        const committedTxn = await producer.transaction();
        await committedTxn.sendBatch({
            topicMessages: [{ topic: topicName, messages: committedMessages }],
        });
        await committedTxn.commit();

        const number = committedMessages.length;
        await waitForMessages(messagesConsumed, {
            number,
        });

        expect(messagesConsumed).toHaveLength(number);
        expect(messagesConsumed[0].message.value.toString()).toMatch(/value-committed-txn-0/);
        expect(messagesConsumed[number - 1].message.value.toString()).toMatch(/value-committed-txn-9/);
    });

    it(
        'receives aborted messages for an isolation level of READ_UNCOMMITTED',
        async () => {
            producer = createProducer({
                transactionalId: `transactional-id-${secureRandom()}`,
                maxInFlightRequests: 1,
            });

            consumer = createConsumer({
                groupId,
                maxWaitTimeInMs: 100,
                readUncommitted: true,
                fromBeginning: true,
                autoCommit: true,
            });

            await consumer.connect();
            await producer.connect();
            await consumer.subscribe({ topic: topicName });

            const messagesConsumed = [];

            const abortedMessages = generateMessages({ prefix: 'aborted-txn1', partition: 0 });

            consumer.run({
                eachMessage: async event => messagesConsumed.push(event),
            });

            const abortedTxn1 = await producer.transaction();
            await abortedTxn1.sendBatch({
                topicMessages: [{ topic: topicName, messages: abortedMessages }],
            });
            await abortedTxn1.abort();

            const number = abortedMessages.length;
            await waitForMessages(messagesConsumed, {
                number,
            });

            expect(messagesConsumed).toHaveLength(abortedMessages.length);
            expect(messagesConsumed[0].message.value.toString()).toMatch(/value-aborted-txn1-0/);
            expect(messagesConsumed[messagesConsumed.length - 1].message.value.toString()).toMatch(
                /value-aborted-txn1-99/
            );
        }
    );

    it(
        'respects offsets sent by a committed transaction ("consume-transform-produce" flow)',
        async () => {
            // Seed the topic with some messages. We don't need a tx producer for this.
            await producer.connect();
            const partition = 0;
            const messages = generateMessages().map(message => ({
                ...message,
                partition,
            }));

            await producer.send({
                topic: topicName,
                messages,
            });

            await producer.disconnect();

            producer = createProducer({
                transactionalId: `transactional-id-${secureRandom()}`,
                maxInFlightRequests: 1,
            });

            consumer = createConsumer({
                groupId,
                maxWaitTimeInMs: 100,
                fromBeginning: true,
                autoCommit: false,
            });

            await consumer.connect();
            await producer.connect();
            await consumer.subscribe({ topic: topicName });

            // 1. Run consumer with "autoCommit=false"

            let messagesConsumed = [];
            // This stores the latest offsets consumed for each partition, when we received the ith message.
            let uncommittedOffsetsPerMessage = [];
            let latestOffsetsPerPartition = {};

            const eachMessage = async ({ partition, message }) => {
                messagesConsumed.push(message);
                /* The message.offset indicates current offset, so we need to add 1 to it, since committed offset denotes
                 * the next offset to consume. */
                latestOffsetsPerPartition[partition] = Number(message.offset) + 1;
                uncommittedOffsetsPerMessage.push(Object.assign({}, latestOffsetsPerPartition));
            };

            consumer.run({
                eachMessage,
            });

            // 2. Consume pre-produced messages.

            const number = messages.length;
            await waitForMessages(messagesConsumed, {
                number,
            });

            expect(messagesConsumed[0].value.toString()).toMatch(/value-0/);
            expect(messagesConsumed[99].value.toString()).toMatch(/value-99/);
            expect(uncommittedOffsetsPerMessage).toHaveLength(messagesConsumed.length);

            // 3. Send offsets in a transaction and commit
            const txnToCommit = await producer.transaction();
            let offsetsToCommit = uncommittedOffsetsPerMessage[97];
            let topicPartitionOffsets = { topic: topicName, partitions: [] };
            for (const partition in offsetsToCommit) {
                topicPartitionOffsets.partitions.push({ partition, offset: offsetsToCommit[partition] });
            }

            await txnToCommit.sendOffsets({
                consumer,
                topics: [topicPartitionOffsets],
            });
            await txnToCommit.commit();

            // Restart consumer - we cannot stop it, so we recreate it.
            await consumer.disconnect();

            consumer = createConsumer({
                groupId,
                maxWaitTimeInMs: 100,
                fromBeginning: true,
                autoCommit: false,
            });

            await consumer.connect();
            await consumer.subscribe({ topic: topicName });

            messagesConsumed = [];
            uncommittedOffsetsPerMessage = [];

            consumer.run({ eachMessage });

            // Assert we only consume the messages that were after the sent offset
            await waitForMessages(messagesConsumed, {
                number: 2,
            });

            expect(messagesConsumed).toHaveLength(2);
            expect(messagesConsumed[0].value.toString()).toMatch(/value-98/);
            expect(messagesConsumed[1].value.toString()).toMatch(/value-99/);
        }
    );

    it(
        'does not respect offsets sent by an aborted transaction ("consume-transform-produce" flow)',
        async () => {
            // Seed the topic with some messages. We don't need a tx producer for this.
            await producer.connect();

            const partition = 0;
            const messages = generateMessages().map(message => ({
                ...message,
                partition,
            }));

            await producer.send({
                topic: topicName,
                messages,
            });

            await producer.disconnect();

            producer = createProducer({
                transactionalId: `transactional-id-${secureRandom()}`,
                maxInFlightRequests: 1,
            });

            consumer = createConsumer({
                groupId,
                maxWaitTimeInMs: 100,
                fromBeginning: true,
                autoCommit: false,
            });

            await consumer.connect();
            await producer.connect();
            await consumer.subscribe({ topic: topicName });

            // 1. Run consumer with "autoCommit=false"

            let messagesConsumed = [];
            // This stores the latest offsets consumed for each partition, when we received the ith message.
            let uncommittedOffsetsPerMessage = [];
            let latestOffsetsPerPartition = {};

            const eachMessage = async ({ partition, message }) => {
                messagesConsumed.push(message);
                /* The message.offset indicates current offset, so we need to add 1 to it, since committed offset denotes
                 * the next offset to consume. */
                latestOffsetsPerPartition[partition] = Number(message.offset) + 1;
                uncommittedOffsetsPerMessage.push(Object.assign({}, latestOffsetsPerPartition));
            };

            consumer.run({
                eachMessage,
            });

            // Consume produced messages.
            await waitForMessages(messagesConsumed, { number: messages.length });

            expect(messagesConsumed[0].value.toString()).toMatch(/value-0/);
            expect(messagesConsumed[99].value.toString()).toMatch(/value-99/);
            expect(uncommittedOffsetsPerMessage).toHaveLength(messagesConsumed.length);

            // 3. Send offsets in a transaction and commit
            const txnToAbort = await producer.transaction();
            let offsetsToCommit = uncommittedOffsetsPerMessage[97];
            let topicPartitionOffsets = { topic: topicName, partitions: [] };
            for (const partition in offsetsToCommit) {
                topicPartitionOffsets.partitions.push({ partition, offset: offsetsToCommit[partition] });
            }

            await txnToAbort.sendOffsets({
                consumer,
                topics: [topicPartitionOffsets],
            });
            await txnToAbort.abort();

            /* Restart consumer - we cannot stop it, so we recreate it. */
            messagesConsumed = [];
            uncommittedOffsetsPerMessage = [];

            await consumer.disconnect();

            consumer = createConsumer({
                groupId,
                maxWaitTimeInMs: 100,
                fromBeginning: true,
                autoCommit: false,
            });

            await consumer.connect();
            await consumer.subscribe({ topic: topicName });

            consumer.run({
                eachMessage,
            });

            await waitForMessages(messagesConsumed, { number: 1 });
            expect(messagesConsumed[0].value.toString()).toMatch(/value-0/);
            await waitForMessages(messagesConsumed, { number: messages.length });
            expect(messagesConsumed[messagesConsumed.length - 1].value.toString()).toMatch(/value-99/);
        }
    );
});