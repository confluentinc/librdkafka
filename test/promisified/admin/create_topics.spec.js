jest.setTimeout(30000);

const {
    secureRandom,
    createAdmin,
    sleep,
} = require('../testhelpers');
const { KafkaJSAggregateError, KafkaJSCreateTopicError, ErrorCodes } = require('../../../lib').KafkaJS;

describe('Admin > createTopics', () => {
    let topicNames, admin;

    beforeEach(async () => {
        topicNames = [`test-topic-${secureRandom()}`, `test-topic-${secureRandom()}`];
        admin = createAdmin({});
    });

    afterEach(async () => {
        admin && (await admin.disconnect());
    });

    it('should create topics', async () => {
        await admin.connect();
        await expect(admin.createTopics({
            topics: [{
                topic: topicNames[0]
            }, {
                topic: topicNames[1],
                replicationFactor: 1,
            }]
        })).resolves.toEqual(true);
        await sleep(1000); /* wait for metadata propagation */

        const listTopicsResult = await admin.listTopics();
        expect(listTopicsResult).toEqual(
            expect.arrayContaining(topicNames)
        );
    });

    it('should indicate if topics were already created', async () => {
        await admin.connect();
        await expect(admin.createTopics({
            topics: [{
                topic: topicNames[0]
            }]
        })).resolves.toEqual(true);
        await sleep(1000); /* wait for metadata propagation */

        await expect(admin.createTopics({
            topics: [{
                topic: topicNames[0]
            }]
        })).resolves.toEqual(false); /* topic already exists */
        await sleep(1000); /* wait for metadata propagation */


        await expect(admin.createTopics({
            topics: [{
                topic: topicNames[0]
            }, {
                topic: topicNames[1]
            }]
        })).resolves.toEqual(false); /* Even of one topic already exists */
    });

    it('should throw topic errors', async () => {
        await admin.connect();

        let storedErr;
        await expect(admin.createTopics({
            topics: [{
                topic: topicNames[0] + '-invalid',
                replicationFactor: 9090, /* unlikely that anyone has this many brokers in test env */
            },
            {
                topic: topicNames[1] + '-invalid',
                numPartitions: 0, /* 0 partitions is invalid */
            }, {
                topic: topicNames[0]
            }]
        }).catch(err => {
            /* Store the error for checking contents later. */
            storedErr = err;
            throw err;
        })).rejects.toThrow(KafkaJSAggregateError);
        await sleep(1000); /* wait for metadata propagation */

        expect(storedErr.message).toMatch(/Topic creation errors/);
        expect(storedErr.errors).toHaveLength(2);

        const replicationErr = storedErr.errors.find(e => e.topic === topicNames[0] + '-invalid');
        expect(replicationErr).toBeInstanceOf(KafkaJSCreateTopicError);
        expect(replicationErr.code).toEqual(ErrorCodes.ERR_INVALID_REPLICATION_FACTOR);

        const partitionsErr = storedErr.errors.find(e => e.topic === topicNames[1]+ '-invalid');
        expect(partitionsErr).toBeInstanceOf(KafkaJSCreateTopicError);
        expect(partitionsErr.code).toEqual(ErrorCodes.ERR_INVALID_PARTITIONS);

        /* Despite errors the valid topic should still be created. */
        await expect(admin.listTopics()).resolves.toEqual(expect.arrayContaining([topicNames[0]]));
    });
});

