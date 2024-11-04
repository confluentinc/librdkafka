jest.setTimeout(30000);

const { ErrorCodes } = require("../../../lib").KafkaJS;
const {
  secureRandom,
  createTopic,
  createProducer,
  createConsumer,
  waitForMessages,
  createAdmin,
} = require("../testhelpers");

describe("fetchOffset function", () => {
  let topicName, topicName2, groupId, producer, consumer, admin;
  let topicsToDelete = [];

  beforeEach(async () => {
    groupId = `consumer-group-id-${secureRandom()}`;

    producer = createProducer({
      clientId: "test-producer-id",
    });

    consumer = createConsumer({
      groupId,
      fromBeginning: true,
      clientId: "test-consumer-id",
      autoCommit: false,
    });

    admin = createAdmin({});

    await producer.connect();
    await consumer.connect();

    await admin.connect();

    topicName = `test-topic-${secureRandom()}`;
    topicName2 = `test-topic-${secureRandom()}`;

    topicsToDelete = [];
  });

  afterEach(async () => {
    await admin.deleteTopics({
      topics: topicsToDelete,
    });
    await admin.disconnect();
    producer && (await producer.disconnect());
    consumer && (await consumer.disconnect());
  });

  it("should timeout when fetching offsets", async () => {

    await createTopic({ topic: topicName, partitions: 1 });
    topicsToDelete.push(topicName);

    await expect(
      admin.fetchOffsets({ groupId, topic: topicName, timeout: 0 })
    ).rejects.toHaveProperty("code", ErrorCodes.ERR__TIMED_OUT);
  });

  it("should return correct offset after consuming messages", async () => {

    await createTopic({ topic: topicName, partitions: 1 });
    topicsToDelete.push(topicName);

    const messages = Array.from({ length: 5 }, (_, i) => ({
      value: `message${i}`,
    }));
    await producer.send({ topic: topicName, messages: messages });

    await consumer.subscribe({ topic: topicName });

    let messagesConsumed = []; // Define messagesConsumed

    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {

        messagesConsumed.push(message); // Populate messagesConsumed
        if (messagesConsumed.length === 5) {
          await consumer.commitOffsets([
            {
              topic,
              partition,
              offset: (parseInt(message.offset, 10) + 1).toString(),
            },
          ]);
        }
      },
    });

    await waitForMessages(messagesConsumed, { number: 5 });

    // Fetch offsets after all messages have been consumed
    const offsets = await admin.fetchOffsets({
      groupId: groupId,
      topics: [topicName],
    });
    expect(messagesConsumed.length).toEqual(5);

    const resultWithPartitionAndOffset = offsets.map(({ partitions, ...rest }) => {
      const newPartitions = partitions.map(({ partition, offset }) => ({
        partition,
        offset,
      }));
      return { ...rest, partitions: newPartitions };
    });

    expect(resultWithPartitionAndOffset).toEqual([
      {
        topic: topicName,
        partitions: [{ partition: 0, offset: "5" }],
      },
    ]);
  });

  it("should return correct offset after consuming messages with specific partitions", async () => {

    await createTopic({ topic: topicName, partitions: 1 });
    topicsToDelete.push(topicName);

    const messages = Array.from({ length: 5 }, (_, i) => ({
      value: `message${i}`,
    }));
    await producer.send({ topic: topicName, messages: messages });

    await consumer.subscribe({ topic: topicName });

    let messagesConsumed = []; // Define messagesConsumed

    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {

        messagesConsumed.push(message); // Populate messagesConsumed
        if (messagesConsumed.length === 5) {
          await consumer.commitOffsets([
            {
              topic,
              partition,
              offset: (parseInt(message.offset, 10) + 1).toString(),
            },
          ]);
        }
      },
    });

    await waitForMessages(messagesConsumed, { number: 5 });

    // Fetch offsets after all messages have been consumed
    const offsets = await admin.fetchOffsets({
      groupId,
      topics: [{ topic: topicName, partitions: [0] }],
    });

    const resultWithPartitionAndOffset = offsets.map(({ partitions, ...rest }) => {
      const newPartitions = partitions.map(({ partition, offset }) => ({
        partition,
        offset,
      }));
      return { ...rest, partitions: newPartitions };
    });

    expect(messagesConsumed.length).toEqual(5);
    expect(resultWithPartitionAndOffset).toEqual([
      {
        topic: topicName,
        partitions: [{ partition: 0, offset: "5" }],
      },
    ]);
  });

  it("should handle unset or null topics", async () => {

    await createTopic({ topic: topicName, partitions: 1 });
    topicsToDelete.push(topicName);

    const messages = Array.from({ length: 5 }, (_, i) => ({
      value: `message${i}`,
    }));
    await producer.send({ topic: topicName, messages: messages });

    await consumer.subscribe({ topic: topicName });

    let messagesConsumed = []; // Define messagesConsumed

    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        messagesConsumed.push(message); // Populate messagesConsumed
        if (messagesConsumed.length === 5) {
          await consumer.commitOffsets([
            {
              topic,
              partition,
              offset: (parseInt(message.offset, 10) + 1).toString(),
            },
          ]);
        }
      },
    });

    await waitForMessages(messagesConsumed, { number: 5 });

    // Fetch offsets after all messages have been consumed
    const offsets = await admin.fetchOffsets({
      groupId,
    });

    const resultWithPartitionAndOffset = offsets.map(({ partitions, ...rest }) => {
      const newPartitions = partitions.map(({ partition, offset }) => ({
        partition,
        offset,
      }));
      return { ...rest, partitions: newPartitions };
    });
    expect(messagesConsumed.length).toEqual(5);
    expect(resultWithPartitionAndOffset).toEqual([
      {
        topic: topicName,
        partitions: [{ partition: 0, offset: "5" }],
      },
    ]);

    const offsets2 = await admin.fetchOffsets({
      groupId,
      topics: null,
    });

    const resultWithPartitionAndOffset2 = offsets2.map(({ partitions, ...rest }) => {
      const newPartitions = partitions.map(({ partition, offset }) => ({
        partition,
        offset,
      }));
      return { ...rest, partitions: newPartitions };
    });
    expect(resultWithPartitionAndOffset2).toEqual([
      {
        topic: topicName,
        partitions: [{ partition: 0, offset: "5" }],
      },
    ]);
  });

  it("should handle multiple topics each with more than 1 partition", async () => {

    await createTopic({ topic: topicName, partitions: 2 });
    await createTopic({ topic: topicName2, partitions: 2 });
    topicsToDelete.push(topicName, topicName2);

    await consumer.subscribe({
      topics: [topicName, topicName2],
    });

    const messages = Array.from({ length: 10 }, (_, i) => ({
      value: `message${i}`,
      partition: i % 2, // alternates between 0 and 1 for even and odd i
    }));

    await producer.send({ topic: topicName, messages });
    await producer.send({ topic: topicName2, messages });

    let messagesConsumed = []; // Define messagesConsumed

    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {

        messagesConsumed.push(message); // Populate messagesConsumed

        // Check the offset of the message and commit only if the offset is 4
        if (parseInt(message.offset, 10) === 4) {
          await consumer.commitOffsets([
            {
              topic,
              partition,
              offset: (parseInt(message.offset, 10) + 1).toString(),
            },
          ]);
        }
      },
    });

    await waitForMessages(messagesConsumed, { number: 20 });

    // Fetch offsets with multiple topics each with more than 1 partition
    const offsets = await admin.fetchOffsets({
      groupId,
    });

    // Sort the actual offsets array
    const sortedOffsets = offsets.sort((a, b) =>
      a.topic.localeCompare(b.topic)
    );

    // remove leaderEpoch from the partitions
    const resultWithPartitionAndOffset = sortedOffsets.map(({ partitions, ...rest }) => {
      const newPartitions = partitions.map(({ partition, offset }) => ({
        partition,
        offset,
      }));
      return { ...rest, partitions: newPartitions };
    });

    expect(resultWithPartitionAndOffset.length).toEqual(2);

    resultWithPartitionAndOffset.forEach((item) => {
      expect(item.partitions.length).toEqual(2);
    });

    expect(resultWithPartitionAndOffset).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          topic: topicName,
          partitions: expect.arrayContaining([
            expect.objectContaining({
              partition: 0,
              offset: "5",
            }),
            expect.objectContaining({
              partition: 1,
              offset: "5",
            }),
          ]),
        }),
        expect.objectContaining({
          topic: topicName2,
          partitions: expect.arrayContaining([
            expect.objectContaining({
              partition: 0,
              offset: "5",
            }),
            expect.objectContaining({
              partition: 1,
              offset: "5",
            }),
          ]),
        }),
      ])
    );
  });
});
