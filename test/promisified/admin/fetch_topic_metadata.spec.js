jest.setTimeout(30000);

const {
    createAdmin,
    createTopic,
    secureRandom,
} = require('../testhelpers');
const { ErrorCodes, AclOperationTypes } = require('../../../lib').KafkaJS;

describe('Admin > fetchTopicMetadata', () => {
    let topicNameSinglePartition, topicNameMultiplePartitions, admin;
    let topicsToDelete = [];

    beforeEach(async () => {
        admin = createAdmin({});

        topicNameSinglePartition = `test-topic-single-${secureRandom()}`;
        topicNameMultiplePartitions = `test-topic-multiple-${secureRandom()}`;

        await createTopic({ topic: topicNameSinglePartition, partitions: 1 });
        await createTopic({ topic: topicNameMultiplePartitions, partitions: 3 });

        topicsToDelete.push(topicNameSinglePartition, topicNameMultiplePartitions);
    });

    afterEach(async () => {
        await admin.deleteTopics({
            topics: topicsToDelete,
        });
        topicsToDelete = [];
        await admin.disconnect();
    });

    it('should timeout when fetching topic metadata', async () => {
        await admin.connect();

        await expect(admin.fetchTopicMetadata({
            topics: [topicNameSinglePartition],
            timeout: 0
        })).rejects.toHaveProperty(
            'code',
            ErrorCodes.ERR__TIMED_OUT
        );
    });

    it('should fetch metadata for a topic with a single partition, includeAuthorizedOperations = false', async () => {
        await admin.connect();

        const metadata = await admin.fetchTopicMetadata({
            topics: [topicNameSinglePartition],
            includeAuthorizedOperations: false,
        });

        expect(metadata.length).toEqual(1);
        const topicMetadata = metadata[0];

        expect(topicMetadata).toEqual(
            expect.objectContaining({
                name: topicNameSinglePartition,
                topicId: expect.objectContaining({
                    mostSignificantBits: expect.any(BigInt),
                    leastSignificantBits: expect.any(BigInt),
                    base64: expect.any(String),
                }),
                isInternal: false,
                partitions: [
                    expect.objectContaining({
                        partitionErrorCode: expect.any(Number),
                        partitionId: 0,
                        leader: expect.any(Number),
                        leaderNode: expect.objectContaining({
                            id: expect.any(Number),
                            host: expect.any(String),
                            port: expect.any(Number)
                        }),
                        replicas: expect.arrayContaining([expect.any(Number)]),
                        replicaNodes: expect.arrayContaining([
                            expect.objectContaining({
                                id: expect.any(Number),
                                host: expect.any(String),
                                port: expect.any(Number)
                            }),
                        ]),
                        isr: expect.arrayContaining([expect.any(Number)]),
                        isrNodes: expect.arrayContaining([
                            expect.objectContaining({
                                id: expect.any(Number),
                                host: expect.any(String),
                                port: expect.any(Number)
                            }),
                        ]),
                    }),
                ],
                authorizedOperations: undefined
            })
        );
    });

    it('should fetch metadata for a topic with a single partition, includeAuthorizedOperations = true', async () => {
        await admin.connect();

        const metadata = await admin.fetchTopicMetadata({
            topics: [topicNameSinglePartition],
            includeAuthorizedOperations: true,
        });

        expect(metadata.length).toEqual(1);
        const topicMetadata = metadata[0];

        expect(topicMetadata).toEqual(
            expect.objectContaining({
                name: topicNameSinglePartition,
                topicId: expect.objectContaining({
                    mostSignificantBits: expect.any(BigInt),
                    leastSignificantBits: expect.any(BigInt),
                    base64: expect.any(String),
                }),
                isInternal: false,
                partitions: [
                    expect.objectContaining({
                        partitionErrorCode: expect.any(Number),
                        partitionId: 0,
                        leader: expect.any(Number),
                        leaderNode: expect.objectContaining({
                            id: expect.any(Number),
                            host: expect.any(String),
                            port: expect.any(Number)
                        }),
                        replicas: expect.arrayContaining([expect.any(Number)]),
                        replicaNodes: expect.arrayContaining([
                            expect.objectContaining({
                                id: expect.any(Number),
                                host: expect.any(String),
                                port: expect.any(Number)
                            }),
                        ]),
                        isr: expect.arrayContaining([expect.any(Number)]),
                        isrNodes: expect.arrayContaining([
                            expect.objectContaining({
                                id: expect.any(Number),
                                host: expect.any(String),
                                port: expect.any(Number)
                            }),
                        ]),
                    }),
                ],
                authorizedOperations: expect.arrayContaining([AclOperationTypes.READ, AclOperationTypes.DESCRIBE]),
            })
        );
    });

    it('should fetch metadata for multiple topics', async () => {
        await admin.connect();

        const metadata = await admin.fetchTopicMetadata({
            topics: [topicNameSinglePartition, topicNameMultiplePartitions],
            includeAuthorizedOperations: false,
        });

        expect(metadata.length).toEqual(2);

        const singlePartitionTopic = metadata.find(
            (topic) => topic.name === topicNameSinglePartition
        );
        const multiplePartitionsTopic = metadata.find(
            (topic) => topic.name === topicNameMultiplePartitions
        );

        // Check single partition topic structure
        expect(singlePartitionTopic).toEqual(
            expect.objectContaining({
                name: topicNameSinglePartition,
                topicId: expect.objectContaining({
                    mostSignificantBits: expect.any(BigInt),
                    leastSignificantBits: expect.any(BigInt),
                    base64: expect.any(String),
                }),
                isInternal: false,
                partitions: [
                    expect.objectContaining({
                        partitionErrorCode: 0,
                        partitionId: 0,
                        leader: expect.any(Number),
                        leaderNode: expect.objectContaining({
                            id: expect.any(Number),
                            host: expect.any(String),
                            port: expect.any(Number)
                        }),
                        replicas: expect.arrayContaining([expect.any(Number)]),
                        replicaNodes: expect.arrayContaining([
                            expect.objectContaining({
                                id: expect.any(Number),
                                host: expect.any(String),
                                port: expect.any(Number)
                            }),
                        ]),
                        isr: expect.arrayContaining([expect.any(Number)]),
                        isrNodes: expect.arrayContaining([
                            expect.objectContaining({
                                id: expect.any(Number),
                                host: expect.any(String),
                                port: expect.any(Number)
                            }),
                        ]),
                    }),
                ],
                authorizedOperations: undefined
            })
        );

        // Check multiple partitions topic structure
        expect(multiplePartitionsTopic).toEqual(
            expect.objectContaining({
                name: topicNameMultiplePartitions,
                topicId: expect.objectContaining({
                    mostSignificantBits: expect.any(BigInt),
                    leastSignificantBits: expect.any(BigInt),
                    base64: expect.any(String),
                }),
                isInternal: false,
                partitions: expect.arrayContaining([
                    expect.objectContaining({
                        partitionErrorCode: 0,
                        partitionId: 0,
                        leader: expect.any(Number),
                    }),
                    expect.objectContaining({
                        partitionErrorCode: 0,
                        partitionId: 1,
                        leader: expect.any(Number),
                    }),
                    expect.objectContaining({
                        partitionErrorCode: 0,
                        partitionId: 2,
                        leader: expect.any(Number),
                    }),
                ]),
                authorizedOperations: undefined
            })
        );
    });

    it('should throw an error when fetching metadata for a non-existent topic', async () => {
        await admin.connect();

        const nonExistentTopic = `non-existent-topic-${secureRandom()}`;

        await expect(admin.fetchTopicMetadata({
            topics: [nonExistentTopic],
        })).rejects.toHaveProperty(
            'code',
            ErrorCodes.ERR_UNKNOWN_TOPIC_OR_PART
        );
    });

    it('should throw an error when fetching metadata for a list with one valid topic and one non-existent topic', async () => {
        await admin.connect();

        const nonExistentTopic = `non-existent-topic-${secureRandom()}`;

        await expect(admin.fetchTopicMetadata({
            topics: [topicNameSinglePartition, nonExistentTopic],
        })).rejects.toHaveProperty(
            'code',
            ErrorCodes.ERR_UNKNOWN_TOPIC_OR_PART
        );
    });
});
