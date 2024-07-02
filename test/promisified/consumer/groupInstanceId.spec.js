jest.setTimeout(30000);

const { waitFor,
    secureRandom,
    createTopic,
    createConsumer,
    sleep, } = require("../testhelpers");
const { ErrorCodes } = require('../../../lib').KafkaJS;

describe('Consumer with static membership', () => {
    let consumer;
    let groupId, topicName;

    let consumerConfig;

    beforeEach(async () => {
        topicName = `test-topic1-${secureRandom()}`;
        groupId = `consumer-group-id-${secureRandom()}`
        consumerConfig = {
            groupId,
        };
        consumer = null;
        await createTopic({ topic: topicName, partitions: 2 });
    });

    afterEach(async () => {
        consumer && (await consumer.disconnect())
    });

    it('does not rebalance after disconnect', async () => {
        let assigns = 0;
        let revokes = 0;
        const rebalanceCallback = function (err, assignment) {
            if (err.code === ErrorCodes.ERR__ASSIGN_PARTITIONS) {
                assigns++;
            } else if (err.code === ErrorCodes.ERR__REVOKE_PARTITIONS) {
                revokes++;
            } else {
                // It's either assign or revoke and nothing else.
                jest.fail('Unexpected error code');
            }
        }

        // Create and start two consumers.
        consumer = createConsumer(consumerConfig, {
            'group.instance.id': 'instance-1',
        });
        await consumer.connect();
        await consumer.subscribe({ topic: topicName });
        consumer.run({ eachMessage: async () => { } });

        await waitFor(() => consumer.assignment().length > 0, () => null, 1000);

        const consumer2 = createConsumer(consumerConfig, {
            'rebalance_cb': rebalanceCallback,
            'group.instance.id': 'instance-2',
        });
        await consumer2.connect();
        await consumer2.subscribe({ topic: topicName });
        consumer2.run({ eachMessage: async () => { } });

        await waitFor(() => consumer2.assignment().length > 0, () => null, 1000);
        expect(assigns).toBe(1);

        // Disconnect one consumer and reconnect it. It should not cause a rebalance in the other.
        await consumer.disconnect();

        consumer = createConsumer(consumerConfig, {
            'group.instance.id': 'instance-1',
        });
        await consumer.connect();
        await consumer.subscribe({ topic: topicName });
        consumer.run({ eachMessage: async () => { } });

        await waitFor(() => consumer.assignment().length > 0, () => null, 1000);
        expect(assigns).toBe(1);
        expect(revokes).toBe(0);

        await consumer.disconnect();
        await consumer2.disconnect();
        consumer = null;
    });

    it('does rebalance after session timeout', async () => {
        let assigns = 0;
        let revokes = 0;
        const rebalanceCallback = function (err, assignment) {
            if (err.code === ErrorCodes.ERR__ASSIGN_PARTITIONS) {
                assigns++;
            } else if (err.code === ErrorCodes.ERR__REVOKE_PARTITIONS) {
                revokes++;
            } else {
                // It's either assign or revoke and nothing else.
                jest.fail('Unexpected error code');
            }
        }

        // Create and start two consumers.
        consumer = createConsumer(consumerConfig, {
            'group.instance.id': 'instance-1',
            'session.timeout.ms': '10000',
        });
        await consumer.connect();
        await consumer.subscribe({ topic: topicName });
        consumer.run({ eachMessage: async () => { } });

        await waitFor(() => consumer.assignment().length > 0, () => null, 1000);

        const consumer2 = createConsumer(consumerConfig, {
            'rebalance_cb': rebalanceCallback,
            'group.instance.id': 'instance-2',
            'session.timeout.ms': '10000',
        });
        await consumer2.connect();
        await consumer2.subscribe({ topic: topicName });
        consumer2.run({ eachMessage: async () => { } });

        await waitFor(() => consumer2.assignment().length > 0, () => null, 1000);
        expect(assigns).toBe(1);

        // Disconnect one consumer and reconnect it. It should cause a rebalance after session timeout.
        await consumer.disconnect();
        consumer = null;

        await sleep(8000);

        // Session timeout (10s) hasn't kicked in yet - we have slept for just 8s.
        expect(consumer2.assignment().length).toBe(1);

        await waitFor(() => consumer2.assignment().length === 2, () => null, 1000);
        expect(consumer2.assignment().length).toBe(2);
        expect(assigns).toBe(2);
        expect(revokes).toBe(1);

        await consumer2.disconnect();
    });
});