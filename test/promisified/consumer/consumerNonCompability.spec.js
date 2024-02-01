jest.setTimeout(30000)

const { Kafka } = require('../../../lib/kafkajs');
const {
    secureRandom,
    createTopic,
    waitFor,
    createProducer,
    createConsumer,
    waitForMessages,
    clusterInformation,
    sleep,
} = require('../testhelpers');

describe('Consumer (non-compability)', () => {
    let topicName, groupId, producer, consumer;

    beforeEach(async () => {
        topicName = `test-topic-${secureRandom()}`
        groupId = `consumer-group-id-${secureRandom()}`

        await createTopic({ topic: topicName, partitions: 3 })

        producer = new Kafka().producer({
            'bootstrap.servers': clusterInformation.librdkafka['bootstrap.servers'],
        });

        consumer = new Kafka().consumer({
            'bootstrap.servers': clusterInformation.librdkafka['bootstrap.servers'],
            'group.id': groupId,
            'auto.offset.reset': 'earliest',
        });
    });

    afterEach(async () => {
        consumer && (await consumer.disconnect())
        producer && (await producer.disconnect())
    });

    it('is able to use consume()', async () => {
        await consumer.connect();
        await producer.connect();
        await consumer.subscribe({ topic: topicName })

        /* Produce 10 messages */
        let i = 0;
        const messages = Array(9)
            .fill()
            .map(() => {
                const value = secureRandom()
                return { value: `value-${value}`, partition: ((i++) % 3) }
            })
        await producer.send({ topic: topicName, messages })

        const messagesConsumed = [];
        while (messagesConsumed.length < 9) {
            const msg = await consumer.consume();
            if (!msg)
                continue;
            messagesConsumed.push(msg);
        }

        // check if all offsets are present
        // partition 0
        expect(messagesConsumed.filter(m => m.partition === 0).map(m => m.offset)).toEqual(Array(3).fill().map((_, i) => i));
        // partition 1
        expect(messagesConsumed.filter(m => m.partition === 1).map(m => m.offset)).toEqual(Array(3).fill().map((_, i) => i));
        // partition 2
        expect(messagesConsumed.filter(m => m.partition === 2).map(m => m.offset)).toEqual(Array(3).fill().map((_, i) => i));
    });

    it('cannot use consume() in compability mode', async () => {
        consumer = createConsumer({
            groupId,
        });
        await consumer.connect();
        await consumer.subscribe({ topic: topicName });

        await expect(consumer.consume()).rejects.toThrow(/cannot be used in KafkaJS compatibility mode/);
    });

    it('is able to use consume() and pause()', async () => {
        await consumer.connect();
        await producer.connect();
        await consumer.subscribe({ topic: topicName })

        /* Produce 10 messages */
        let i = 0;
        const messages = Array(9)
            .fill()
            .map(() => {
                const value = secureRandom()
                return { value: `value-${value}`, partition: ((i++) % 3) }
            })
        await producer.send({ topic: topicName, messages });

        const messagesConsumed = [];
        while (messagesConsumed.length < 5) {
            const msg = await consumer.consume();
            if (!msg)
                continue;
            messagesConsumed.push(msg);
        }

        await consumer.pause([{ topic: topicName }]);

        let msg = await consumer.consume();
        expect(msg).toBeNull();

        await consumer.resume([{ topic: topicName }]);

        while (messagesConsumed.length < 9) {
            const msg = await consumer.consume();
            if (!msg)
                continue;
            messagesConsumed.push(msg);
        }

        // check if all offsets are present
        // partition 0
        expect(messagesConsumed.filter(m => m.partition === 0).map(m => m.offset)).toEqual(Array(3).fill().map((_, i) => i));
        // partition 1
        expect(messagesConsumed.filter(m => m.partition === 1).map(m => m.offset)).toEqual(Array(3).fill().map((_, i) => i));
        // partition 2
        expect(messagesConsumed.filter(m => m.partition === 2).map(m => m.offset)).toEqual(Array(3).fill().map((_, i) => i));
    });

    it('is able to use consume() and seek()', async () => {
        await consumer.connect();
        await producer.connect();
        await consumer.subscribe({ topic: topicName })

        /* Produce 10 messages */
        let i = 0;
        const messages = Array(9)
            .fill()
            .map(() => {
                const value = secureRandom()
                return { value: `value-${value}`, partition: ((i++) % 3) }
            })
        await producer.send({ topic: topicName, messages });

        let messagesConsumed = [];
        while (messagesConsumed.length < 9) {
            const msg = await consumer.consume();
            if (!msg)
                continue;
            messagesConsumed.push(msg);
        }

        await consumer.seek({ topic: topicName, partition: 0, offset: 0 });

        while (messagesConsumed.length < 12) {
            const msg = await consumer.consume();
            if (!msg)
                continue;
            messagesConsumed.push(msg);
        }

        // check if all offsets are present
        // partition 0 - repeated completely because of seek
        expect(messagesConsumed.filter(m => m.partition === 0).map(m => m.offset)).toEqual(Array(6).fill().map((_, i) => i % 3));
        // partition 1
        expect(messagesConsumed.filter(m => m.partition === 1).map(m => m.offset)).toEqual(Array(3).fill().map((_, i) => i));
        // partition 2
        expect(messagesConsumed.filter(m => m.partition === 2).map(m => m.offset)).toEqual(Array(3).fill().map((_, i) => i));
    });

    it('is able to commit offsets (using auto commit)', async () => {
        await consumer.connect();
        await producer.connect();
        await consumer.subscribe({ topic: topicName })

        /* Produce 10 messages */
        let i = 0;
        const messages = Array(9)
            .fill()
            .map(() => {
                const value = secureRandom()
                return { value: `value-${value}`, partition: ((i++) % 3) }
            })
        await producer.send({ topic: topicName, messages })

        const messagesConsumed = [];
        while (messagesConsumed.length < 6) {
            const msg = await consumer.consume();
            if (!msg)
                continue;
            messagesConsumed.push(msg);
        }
        await consumer.disconnect();

        /* Create new consumer and append to that array. We should get the last 3 messages */
        consumer = new Kafka().consumer({
            'bootstrap.servers': clusterInformation.librdkafka['bootstrap.servers'],
            'group.id': groupId,
            'auto.offset.reset': 'earliest',
        });
        await consumer.connect();
        await consumer.subscribe({ topic: topicName })

        while (messagesConsumed.length < 9) {
            const msg = await consumer.consume();
            if (!msg)
                continue;
            messagesConsumed.push(msg);
        }
        /* No more messages when the total adds up. */
        await expect(consumer.consume()).resolves.toBeNull();

        // check if all offsets are present
        // partition 0
        expect(messagesConsumed.filter(m => m.partition === 0).map(m => m.offset)).toEqual(Array(3).fill().map((_, i) => i));
        // partition 1
        expect(messagesConsumed.filter(m => m.partition === 1).map(m => m.offset)).toEqual(Array(3).fill().map((_, i) => i));
        // partition 2
        expect(messagesConsumed.filter(m => m.partition === 2).map(m => m.offset)).toEqual(Array(3).fill().map((_, i) => i));

    });
});
