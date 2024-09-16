jest.setTimeout(30000);

const {
    createConsumer,
    secureRandom,
    createTopic,
    waitFor,
    createAdmin,
} = require('../testhelpers');
const { ConsumerGroupStates, ErrorCodes } = require('../../../lib').KafkaJS;

describe('Admin > listGroups', () => {
    let topicName, groupId, consumer, admin;

    beforeEach(async () => {
        topicName = `test-topic-${secureRandom()}`;
        groupId = `consumer-group-id-${secureRandom()}`;

        consumer = createConsumer({
            groupId,
            fromBeginning: true,
        });

        await createTopic({ topic: topicName, partitions: 2 });

        admin = createAdmin({});
    });

    afterEach(async () => {
        consumer && (await consumer.disconnect());
        admin && (await admin.disconnect());
    });

    it('should timeout', async () => {
        await admin.connect();

        await expect(admin.listGroups({ timeout: 0 })).rejects.toHaveProperty(
            'code',
            ErrorCodes.ERR__TIMED_OUT
        );
    });

    it('should list consumer groups', async () => {
        await consumer.connect();
        await consumer.subscribe({ topic: topicName });
        await consumer.run({ eachMessage: async () => {} });

        await waitFor(() => consumer.assignment().length > 0, () => null, 1000);

        await admin.connect();
        let listGroupsResult = await admin.listGroups({
            matchConsumerGroupStates: undefined,
        });
        expect(listGroupsResult.errors).toEqual([]);
        expect(listGroupsResult.groups).toEqual(
            expect.arrayContaining([
                expect.objectContaining({
                    groupId,
                    isSimpleConsumerGroup: false,
                    protocolType: 'consumer',
                    state: ConsumerGroupStates.STABLE,
                }),
            ])
        );

        // Disconnect the consumer to make the group EMPTY.
        await consumer.disconnect();
        consumer = null;

        listGroupsResult = await admin.listGroups();
        expect(listGroupsResult.errors).toEqual([]);
        expect(listGroupsResult.groups).toEqual(
            expect.arrayContaining([
                expect.objectContaining({
                    groupId,
                    isSimpleConsumerGroup: false,
                    protocolType: 'consumer',
                    state: ConsumerGroupStates.EMPTY,
                }),
            ])
        );

        // Consumer group should not show up if filtering by non-empty groups
        // using state matching.
        listGroupsResult = await admin.listGroups({
            matchConsumerGroupStates: [ ConsumerGroupStates.STABLE,
                                        ConsumerGroupStates.PREPARING_REBALANCE,
                                        ConsumerGroupStates.COMPLETING_REBALANCE, ] });
        expect(listGroupsResult.errors).toEqual([]);
        expect(listGroupsResult.groups.map(group => group.groupId)).not.toContain(groupId);
    });
});

