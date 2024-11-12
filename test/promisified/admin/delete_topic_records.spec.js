jest.setTimeout(30000);

const { ErrorCodes } = require("../../../lib").KafkaJS;
const {
    secureRandom,
    createTopic,
    createProducer,
    createAdmin,
} = require("../testhelpers");

describe("deleteTopicRecords function", () => {
    let topicName, producer, admin;

    beforeEach(async () => {

        producer = createProducer({
            clientId: "test-producer-id",
        });

        admin = createAdmin({});

        await producer.connect();

        await admin.connect();

        topicName = `test-topic-${secureRandom()}`;
    });

    afterEach(async () => {
        await admin.deleteTopics({
            topics: [topicName],
        });
        await admin.disconnect();
        producer && (await producer.disconnect());
    });

    it("should timeout when deleting records", async () => {

        await createTopic({ topic: topicName, partitions: 1 });

        const messages = Array.from({ length: 5 }, (_, i) => ({
            value: `message${i}`,
        }));
        await producer.send({ topic: topicName, messages: messages });

        await expect(
            admin.deleteTopicRecords({ topic: topicName, partitions: [{ partition: 0, offset: "4" }], timeout: 0 })
        ).rejects.toHaveProperty("code", ErrorCodes.ERR__TIMED_OUT);
    });

    it("should return correct offset and success error code after deleting records", async () => {

        await createTopic({ topic: topicName, partitions: 1 });

        const messages = Array.from({ length: 5 }, (_, i) => ({
            value: `message${i}`,
        }));
        await producer.send({ topic: topicName, messages: messages });

        const records = await admin.deleteTopicRecords({
            topic: topicName,
            partitions: [{ partition: 0, offset: "5" }],
        });

        expect(records).toEqual([
            {
                topic: topicName,
                partition: 0,
                lowWatermark: 5
            }
        ]);
    });

    it("should delete all records in partition when offset set to -1", async () => {

        await createTopic({ topic: topicName, partitions: 1 });

        const messages = Array.from({ length: 5 }, (_, i) => ({
            value: `message${i}`,
        }));
        await producer.send({ topic: topicName, messages: messages });

        const records = await admin.deleteTopicRecords({
            topic: topicName,
            partitions: [{ partition: 0, offset: "-1" }],
        });

        expect(records).toEqual([
            {
                topic: topicName,
                partition: 0,
                lowWatermark: 5
            }
        ]);
    });

    it("should return correct offset and success error code after deleting records from multiple partitions", async () => {

        // Create a topic with 2 partitions
        await createTopic({ topic: topicName, partitions: 2 });

        // Send messages to partition 0
        const messagesPartition0 = Array.from({ length: 6 }, (_, i) => ({
            value: `message${i}`,
            partition: 0,
        }));

        // Send messages to partition 1
        const messagesPartition1 = Array.from({ length: 11 }, (_, i) => ({
            value: `message${i}`,
            partition: 1,
        }));

        await producer.send({ topic: topicName, messages: messagesPartition0 });
        await producer.send({ topic: topicName, messages: messagesPartition1 });

        // Call deleteTopicRecords to delete the records
        const records = await admin.deleteTopicRecords({
            topic: topicName,
            partitions: [
                { partition: 0, offset: "5" },
                { partition: 1, offset: "10" },
            ],
        });

        // Check the returned offsets and error codes for both partitions
        expect(records).toEqual([
            {
                topic: topicName,
                partition: 0,
                lowWatermark: 5
            },
            {
                topic: topicName,
                partition: 1,
                lowWatermark: 10
            }
        ]);
    });
});
