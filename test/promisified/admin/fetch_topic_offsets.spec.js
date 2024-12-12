jest.setTimeout(30000);

const { ErrorCodes, IsolationLevel } = require("../../../lib").KafkaJS;
const {
  secureRandom,
  createTopic,
  createProducer,
  createAdmin,
} = require("../testhelpers");

describe("fetchTopicOffsets function", () => {
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

  it("should timeout when fetching topic offsets", async () => {
    await createTopic({ topic: topicName, partitions: 1 });

    await expect(
      admin.fetchTopicOffsets(topicName, { timeout: 0 })
    ).rejects.toHaveProperty("code", ErrorCodes.ERR__TIMED_OUT);
  });

  it("should return result for a topic with a single partition with isolation level READ_UNCOMMITTED", async () => {
    await createTopic({ topic: topicName, partitions: 1 });

    // Send some messages to reach specific offsets
    const messages = Array.from({ length: 5 }, (_, i) => ({
      value: `message${i}`,
    }));
    await producer.send({ topic: topicName, messages: messages });

    // Fetch offsets with isolation level READ_UNCOMMITTED
    const offsets = await admin.fetchTopicOffsets(topicName, {
      isolationLevel: IsolationLevel.READ_UNCOMMITTED,
    });

    expect(offsets).toEqual([
      {
        partition: 0,
        offset: "5",
        low: "0",
        high: "5",
      },
    ]);
  });

  it("should return result for a topic with a single partition with isolation level READ_COMMITTED", async () => {
    await createTopic({ topic: topicName, partitions: 1 });

    // Send some messages to reach specific offsets
    const messages = Array.from({ length: 5 }, (_, i) => ({
      value: `message${i}`,
    }));
    await producer.send({ topic: topicName, messages: messages });

    // Fetch offsets with isolation level READ_COMMITTED
    const offsets = await admin.fetchTopicOffsets(topicName, {
      isolationLevel: IsolationLevel.READ_COMMITTED,
    });

    expect(offsets).toEqual([
      {
        partition: 0,
        offset: "5",
        low: "0",
        high: "5",
      },
    ]);
  });

  it("should return result for a topic with multiple partitions", async () => {
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

    // Fetch offsets with isolation level READ_UNCOMMITTED
    const offsets = await admin.fetchTopicOffsets(topicName, {
      isolationLevel: IsolationLevel.READ_UNCOMMITTED,
    });

    expect(offsets).toEqual([
      { partition: 0, offset: "5", low: "0", high: "5" },
      { partition: 1, offset: "5", low: "0", high: "5" },
    ]);
  });
});
