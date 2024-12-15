jest.setTimeout(30000);

const {
    createConsumer,
    secureRandom,
    createTopic,
    waitFor,
    createAdmin,
} = require('../testhelpers');
const { KafkaJSDeleteGroupsError, ErrorCodes } = require('../../../lib').KafkaJS;

describe('Admin > deleteGroups', () => {
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

        await expect(admin.deleteGroups(['invalid-group'], { timeout: 0 })).rejects.toHaveProperty(
            'code',
            ErrorCodes.ERR__TIMED_OUT
        );
    });

    it('should delete only empty consumer groups', async () => {
        await consumer.connect();
        await consumer.subscribe({ topic: topicName });
        await consumer.run({ eachMessage: async () => {} });

        await waitFor(() => consumer.assignment().length > 0, () => null, 1000);

        await admin.connect();
        let listGroupsResult = await admin.listGroups();
        expect(listGroupsResult.errors).toEqual([]);
        expect(listGroupsResult.groups.map(group => group.groupId)).toContain(groupId);

        // Delete when the group is not empty - it should fail.
        let deleteErr;
        await expect(
            admin.deleteGroups([groupId]).catch(e => {
                deleteErr = e;
                throw e;
            })
        ).rejects.toThrow(KafkaJSDeleteGroupsError);

        expect(deleteErr.groups).toHaveLength(1);
        expect(deleteErr.groups[0]).toEqual(
            expect.objectContaining({
                groupId,
                error: expect.objectContaining({
                    code: ErrorCodes.ERR_NON_EMPTY_GROUP,
                }),
                errorCode: ErrorCodes.ERR_NON_EMPTY_GROUP,
            })
        );

        // Disconnect the consumer to make the group EMPTY.
        await consumer.disconnect();
        consumer = null;

        listGroupsResult = await admin.listGroups();
        expect(listGroupsResult.errors).toEqual([]);
        expect(listGroupsResult.groups.map(group => group.groupId)).toContain(groupId);

        // Delete the empty consumer group.
        const deleteResult = await admin.deleteGroups([groupId]);
        expect(deleteResult).toEqual([
            expect.objectContaining({
                groupId,
                errorCode: ErrorCodes.ERR_NO_ERROR,
            }),
        ]);

        // Cross-verify the deletion.
        listGroupsResult = await admin.listGroups();
        expect(listGroupsResult.errors).toEqual([]);
        expect(listGroupsResult.groups.map(group => group.groupId)).not.toContain(groupId);

        // Deleting the group again should fail.
        deleteErr = [];
        await expect(
            admin.deleteGroups([groupId]).catch(e => {
                deleteErr = e;
                throw e;
            })
        ).rejects.toThrow(KafkaJSDeleteGroupsError);

        expect(deleteErr.groups).toHaveLength(1);
        expect(deleteErr.groups[0]).toEqual(
            expect.objectContaining({
                groupId,
                error: expect.objectContaining({
                    code: ErrorCodes.ERR_GROUP_ID_NOT_FOUND,
                }),
                errorCode: ErrorCodes.ERR_GROUP_ID_NOT_FOUND,
            })
        );
    });
});

