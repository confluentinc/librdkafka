jest.setTimeout(30000);

const {
    testConsumerGroupProtocolClassic,
    createConsumer,
    createProducer,
    secureRandom,
    createTopic,
    waitFor,
    createAdmin,
    sleep,
} = require('../testhelpers');
const { ConsumerGroupStates, ConsumerGroupTypes, ErrorCodes, AclOperationTypes } = require('../../../lib').KafkaJS;

describe('Admin > describeGroups', () => {
    let topicName, groupId, consumer, admin, groupInstanceId, producer;

    beforeEach(async () => {
        topicName = `test-topic-${secureRandom()}`;
        groupId = `consumer-group-id-${secureRandom()}`;
        groupInstanceId = `consumer-group-instance-id-${secureRandom()}`;

        producer = createProducer({});

        consumer = createConsumer({
            groupId,
            fromBeginning: true,
            clientId: 'test-client-id',
        }, {
            'group.instance.id': groupInstanceId,
            'session.timeout.ms': 10000,
            'partition.assignment.strategy': 'roundrobin',
        });

        await createTopic({ topic: topicName, partitions: 2 });

        admin = createAdmin({});
    });

    afterEach(async () => {
        producer && (await producer.disconnect());
        consumer && (await consumer.disconnect());
        admin && (await admin.disconnect());
    });

    it('should timeout', async () => {
        await admin.connect();

        await expect(admin.describeGroups(['not-a-real-group'], { timeout: 0 })).rejects.toHaveProperty(
            'code',
            ErrorCodes.ERR__TIMED_OUT
        );
    });

    it('should not accept empty or null groups array', async () => {
        await admin.connect();

        await expect(admin.describeGroups([])).rejects.toHaveProperty(
            'message',
            'Must provide at least one group name'
        );

        await expect(admin.describeGroups(null)).rejects.toHaveProperty(
            'message',
            'Must provide group name array'
        );
    });

    it('should describe consumer groups', async () => {
        let messagesConsumed = 0;

        await consumer.connect();
        await consumer.subscribe({ topic: topicName });
        await consumer.run({ eachMessage: async () => { messagesConsumed++; } });

        await waitFor(() => consumer.assignment().length > 0, () => null, 1000);

        await admin.connect();
        let describeGroupsResult = await admin.describeGroups(
            [groupId], { includeAuthorizedOperations: true });

        expect(describeGroupsResult.groups.length).toEqual(1);
        expect(describeGroupsResult.groups[0]).toEqual(
            expect.objectContaining({
                groupId,
                protocol: testConsumerGroupProtocolClassic() ? 'roundrobin' : 'uniform',
                partitionAssignor: testConsumerGroupProtocolClassic() ? 'roundrobin' : 'uniform',
                isSimpleConsumerGroup: false,
                protocolType: 'consumer',
                state: ConsumerGroupStates.STABLE,
                type: testConsumerGroupProtocolClassic() ? ConsumerGroupTypes.CLASSIC : ConsumerGroupTypes.CONSUMER,
                coordinator: expect.objectContaining({
                    id: expect.any(Number),
                    host: expect.any(String),
                    port: expect.any(Number),
                }),
                authorizedOperations: expect.arrayContaining([AclOperationTypes.READ, AclOperationTypes.DESCRIBE]),
                members: expect.arrayContaining([
                    expect.objectContaining({
                        clientHost: expect.any(String),
                        clientId: 'test-client-id',
                        memberId: expect.any(String),
                        memberAssignment: null,
                        memberMetadata: null,
                        groupInstanceId: groupInstanceId,
                        assignment: {
                            topicPartitions:[
                                expect.objectContaining({ topic: topicName, partition: 0 }),
                                expect.objectContaining({ topic: topicName, partition: 1 }),
                            ],
                        }
                    }),
                ]),
            })
        );

        // Produce some messages so that the consumer can commit them, and hence
        // the group doesn't become DEAD.
        await producer.connect();
        await producer.send({
            topic: topicName,
            messages: [{ key: 'key', value: 'value' }],
        });

        await waitFor(() => messagesConsumed > 0, () => null, 1000);

        // Disconnect the consumer to make the group EMPTY and commit offsets.
        await consumer.disconnect();
        consumer = null;

        // Wait so that session.timeout.ms expires and the group becomes EMPTY.
        await sleep(10500);

        // Don't include authorized operations this time.
        describeGroupsResult = await admin.describeGroups([groupId]);
        expect(describeGroupsResult.groups.length).toEqual(1);
        expect(describeGroupsResult.groups[0]).toEqual(
            expect.objectContaining({
                groupId,
                protocol: testConsumerGroupProtocolClassic() ? '' : 'uniform',
                partitionAssignor: testConsumerGroupProtocolClassic() ? '' : 'uniform',
                state: ConsumerGroupStates.EMPTY,
                type: testConsumerGroupProtocolClassic() ? ConsumerGroupTypes.CLASSIC : ConsumerGroupTypes.CONSUMER,
                protocolType: 'consumer',
                isSimpleConsumerGroup: false,
                coordinator: expect.objectContaining({
                    id: expect.any(Number),
                    host: expect.any(String),
                    port: expect.any(Number),
                }),
                members: [],
            })
        );

        expect(describeGroupsResult.groups[0].authorizedOperations).toBeUndefined();
    });
});

