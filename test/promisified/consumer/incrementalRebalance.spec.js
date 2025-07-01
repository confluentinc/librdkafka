jest.setTimeout(30000);

const { waitFor,
    secureRandom,
    createTopic,
    createConsumer,
    testConsumerGroupProtocolClassic } = require("../testhelpers");
const { PartitionAssigners, ErrorCodes } = require('../../../lib').KafkaJS;

describe('Consumer > incremental rebalance', () => {
    let consumer;
    let groupId, topicName;

    const consumerConfig = { groupId };

    if (testConsumerGroupProtocolClassic()) {
        consumerConfig.partitionAssigners = [PartitionAssigners.cooperativeSticky];
    }

    beforeEach(async () => {
        topicName = `test-topic1-${secureRandom()}`;
        groupId = `consumer-group-id-${secureRandom()}`;
        consumer = null;
        await createTopic({ topic: topicName, partitions: 2 });
    });

    afterEach(async () => {
        consumer && (await consumer.disconnect());
    });

    it('returns protocol name', async () => {
        consumer = createConsumer(consumerConfig);
        await consumer.connect();
        await consumer.subscribe({ topic: topicName });
        consumer.run({ eachMessage: async () => { } });

        await waitFor(() => consumer.assignment().length > 0, () => null, 1000);

        expect(consumer.rebalanceProtocol()).toEqual('COOPERATIVE');
    });

    it('calls rebalance callback', async () => {
        let assigns = 0;
        let revokes = 0;
        const rebalanceCallback = function (err, assignment) {
            if (err.code === ErrorCodes.ERR__ASSIGN_PARTITIONS) {
                assigns++;
                expect(assignment.length).toBe(2);
            } else if (err.code === ErrorCodes.ERR__REVOKE_PARTITIONS) {
                revokes++;
                expect(assignment.length).toBe(2);
            } else {
                // It's either assign or revoke and nothing else.
                jest.fail('Unexpected error code');
            }
        };


        consumer = createConsumer(consumerConfig, {
            'rebalance_cb': rebalanceCallback,
        });
        await consumer.connect();
        await consumer.subscribe({ topic: topicName });
        consumer.run({ eachMessage: async () => { } });

        await waitFor(() => consumer.assignment().length > 0, () => null, 1000);
        expect(assigns).toBe(1);
        expect(consumer.assignment().length).toBe(2);

        await consumer.disconnect();
        consumer = null;
        expect(revokes).toBe(1);
        expect(assigns).toBe(1);
    });

    it('allows changing the assignment', async () => {
        let assigns = 0;
        const rebalanceCallback = function (err, assignment) {
            if (err.code === ErrorCodes.ERR__ASSIGN_PARTITIONS) {
                assigns++;
                expect(assignment.length).toBe(2);
                assignment = [assignment[0]];
                return assignment;
            } else {
                // It's either assign or revoke and nothing else.
                expect(err.code).toBe(ErrorCodes.ERR__REVOKE_PARTITIONS);
            }
        };


        consumer = createConsumer(consumerConfig, {
            'rebalance_cb': rebalanceCallback,
        });
        await consumer.connect();
        await consumer.subscribe({ topic: topicName });
        consumer.run({ eachMessage: async () => { } });

        await waitFor(() => consumer.assignment().length > 0, () => null, 1000);
        expect(assigns).toBe(1);
        expect(consumer.assignment().length).toBe(1);
    });

    it('is actually incremental', async () => {
        let expectedAssignmentCount = 0;
        const rebalanceCallback = (err, assignment) => {
            /* Empty assignments are ignored, they're a rebalance for the synchronization barrier. */
            if (assignment.length === 0)
                return;
            if (err.code === ErrorCodes.ERR__ASSIGN_PARTITIONS) {
                expect(assignment.length).toBe(expectedAssignmentCount);
            } else if (err.code === ErrorCodes.ERR__REVOKE_PARTITIONS) {
                expect(assignment.length).toBe(expectedAssignmentCount);
            } else {
                // It's either assign or revoke and nothing else.
                jest.fail('Unexpected error code');
            }
        };

        /* First consumer joins and gets all partitions. */
        expectedAssignmentCount = 2;
        consumer = createConsumer(consumerConfig, {
            'rebalance_cb': rebalanceCallback,
        });

        await consumer.connect();
        await consumer.subscribe({ topic: topicName });
        consumer.run({ eachMessage: async () => { } });

        await waitFor(() => consumer.assignment().length > 0, () => null, 1000);
        expect(consumer.assignment().length).toBe(2);

        /* Second consumer joins and gets one partition. */
        expectedAssignmentCount = 1;
        const consumer2 = createConsumer(consumerConfig, {
            'rebalance_cb': rebalanceCallback,
        });

        await consumer2.connect();
        await consumer2.subscribe({ topic: topicName });
        consumer2.run({ eachMessage: async () => { } });
        await waitFor(() => consumer2.assignment().length > 0, () => null, 1000);
        expect(consumer.assignment().length).toBe(1);
        expect(consumer2.assignment().length).toBe(1);

        await consumer2.disconnect();
    });

    it('works with promisified handler', async () => {
        let assigns = 0;
        let revokes = 0;

        consumer = createConsumer(consumerConfig, {
            rebalance_cb: async (err, assignment) => {
                if (err.code === ErrorCodes.ERR__ASSIGN_PARTITIONS) {
                    assigns++;
                    expect(assignment.length).toBe(2);
                } else if (err.code === ErrorCodes.ERR__REVOKE_PARTITIONS) {
                    revokes++;
                    expect(assignment.length).toBe(2);
                }
            },
        });
        await consumer.connect();
        await consumer.subscribe({ topic: topicName });
        consumer.run({ eachMessage: async () => { } });

        await waitFor(() => consumer.assignment().length > 0, () => null, 1000);
        expect(assigns).toBe(1);
        expect(consumer.assignment().length).toBe(2);

        await consumer.disconnect();
        consumer = null;
        expect(revokes).toBe(1);
        expect(assigns).toBe(1);
    });
});