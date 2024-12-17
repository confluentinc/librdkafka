jest.setTimeout(30000);

const { ErrorCodes } = require("../../../lib").KafkaJS;
const {
    secureRandom,
    createTopic,
    createProducer,
    createAdmin,
} = require("../testhelpers");

describe("fetchTopicOffsetsByTimestamp function", () => {
    let topicName, admin, producer;

    beforeEach(async () => {
        admin = createAdmin({});
        producer = createProducer({
            clientId: "test-producer-id",
        });

        await admin.connect();
        await producer.connect();

        topicName = `test-topic-${secureRandom()}`;
    });

    afterEach(async () => {
        await admin.deleteTopics({
            topics: [topicName],
        });
        await admin.disconnect();
        producer && (await producer.disconnect());
    });

    it("should timeout when fetching topic offsets by timestamp", async () => {
        await createTopic({ topic: topicName, partitions: 1 });

        await expect(
            admin.fetchTopicOffsetsByTimestamp(topicName, Date.now(), { timeout: 0 })
        ).rejects.toHaveProperty("code", ErrorCodes.ERR__TIMED_OUT);
    });

    it("should fetch offsets for specific timestamps (t1, t2, t3)", async () => {
        await createTopic({ topic: topicName, partitions: 1 });

        // Messages with specific timestamps
        const now = 10000000;
        const t1 = now + 100;
        const t2 = now + 250;
        const t3 = now + 400;

        const messages = [
            { value: 'message1', timestamp: t1.toString() },
            { value: 'message2', timestamp: t2.toString() },
            { value: 'message3', timestamp: t3.toString() },
        ];

        await producer.send({ topic: topicName, messages });

        const offsetsAtSpecificTimestamp1 = await admin.fetchTopicOffsetsByTimestamp(topicName, now + 50);
        expect(offsetsAtSpecificTimestamp1).toEqual([
            {
                partition: 0,
                offset: "0",
            },
        ]);

        const offsetsAtSpecificTimestamp2 = await admin.fetchTopicOffsetsByTimestamp(topicName, now + 250);
        expect(offsetsAtSpecificTimestamp2).toEqual([
            {
                partition: 0,
                offset: "1",
            },
        ]);

        const offsetsAtSpecificTimestamp3 = await admin.fetchTopicOffsetsByTimestamp(topicName, now + 500);
        expect(offsetsAtSpecificTimestamp3).toEqual([
            {
                partition: 0,
                offset: "3",
            },
        ]);
    });

    it("should return result for a topic with a single partition and no timestamp", async () => {
        await createTopic({ topic: topicName, partitions: 1 });

        // Send some messages to reach specific offsets
        const messages = Array.from({ length: 5 }, (_, i) => ({
            value: `message${i}`,
        }));
        await producer.send({ topic: topicName, messages: messages });

        // Fetch offsets without providing timestamp
        const offsets = await admin.fetchTopicOffsetsByTimestamp(topicName);

        expect(offsets).toEqual([
            {
                partition: 0,
                offset: "5", // As per the test case, no timestamp should return the last committed offset '5'
            },
        ]);
    });

    it("should fetch offsets for specific timestamps for a topic with multiple partitions", async () => {
        await createTopic({ topic: topicName, partitions: 2 });

        // Messages with specific timestamps for each partition
        const now = 10000000;
        const t1 = now + 100;
        const t2 = now + 250;
        const t3 = now + 400;

        const messagesPartition0 = [
            { value: "message0-partition0-t1", timestamp: t1.toString(), partition: 0 },
            { value: "message1-partition0-t2", timestamp: t2.toString(), partition: 0 },
            { value: "message2-partition0-t3", timestamp: t3.toString(), partition: 0 },
        ];

        const messagesPartition1 = [
            { value: "message0-partition1-t1", timestamp: t1.toString(), partition: 1 },
            { value: "message1-partition1-t2", timestamp: t2.toString(), partition: 1 },
            { value: "message2-partition1-t3", timestamp: t3.toString(), partition: 1 },
        ];

        await producer.send({ topic: topicName, messages: messagesPartition0 });
        await producer.send({ topic: topicName, messages: messagesPartition1 });

        const offsetsBeforeT1 = await admin.fetchTopicOffsetsByTimestamp(topicName, now + 50);
        expect(offsetsBeforeT1).toEqual([
            { partition: 0, offset: "0" }, // Offset before any message in partition 0
            { partition: 1, offset: "0" }, // Offset before any message in partition 1
        ]);

        const offsetsBetweenT1AndT2 = await admin.fetchTopicOffsetsByTimestamp(topicName, now + 250);
        expect(offsetsBetweenT1AndT2).toEqual([
            { partition: 0, offset: "1" },
            { partition: 1, offset: "1" },
        ]);

        // Fetch latest offsets
        const offsetsAfterT3 = await admin.fetchTopicOffsetsByTimestamp(topicName, now + 500);
        expect(offsetsAfterT3).toEqual([
            { partition: 0, offset: "3" }, // Latest offset in partition 0
            { partition: 1, offset: "3" }, // Latest offset in partition 1
        ]);
    });


    it("should return result for a topic with multiple partitions and no timestamp", async () => {
        await createTopic({ topic: topicName, partitions: 2 });

        const messagesPartition0 = Array.from({ length: 5 }, (_, i) => ({
            value: `message${i}`,
            partition: 0,
        }));
        const messagesPartition1 = Array.from({ length: 5 }, (_, i) => ({
            value: `message${i}`,
            partition: 1,
        }));

        await producer.send({ topic: topicName, messages: messagesPartition0 });
        await producer.send({ topic: topicName, messages: messagesPartition1 });

        // Fetch offsets without providing timestamp
        const offsets = await admin.fetchTopicOffsetsByTimestamp(topicName);

        expect(offsets).toEqual([
            {
                partition: 0,
                offset: "5", // As per the test case, no timestamp should return the last committed offset '5'
            },
            {
                partition: 1,
                offset: "5", // As per the test case, no timestamp should return the last committed offset '5'
            },
        ]);
    });
});
