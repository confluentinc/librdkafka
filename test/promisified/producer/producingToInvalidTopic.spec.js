const { createTopic, createProducer, secureRandom } = require('../testhelpers');
const { ErrorCodes } = require('../../../lib').KafkaJS;

describe('Producer > Producing to invalid topics', () => {
  let producer, topicName;

  beforeEach(async () => {
    topicName = `test-topic-${secureRandom()}`;

    producer = createProducer({
    });
    await producer.connect();
    await createTopic({ topic: topicName });
  });

  afterEach(async () => {
    producer && (await producer.disconnect());
  });

  it('rejects when producing to an invalid topic name, but is able to subsequently produce to a valid topic', async () => {
    const message = { key: `key-${secureRandom()}`, value: `value-${secureRandom()}` };
    const invalidTopicName = `${topicName}-abc)(*&^%`;
    await expect(producer.send({ topic: invalidTopicName, messages: [message] })).rejects.toHaveProperty(
        'code',
        ErrorCodes.ERR_TOPIC_EXCEPTION,
    );

    await expect(producer.send({ topic: topicName, messages: [message] })).resolves.toBeTruthy();
  });
});
