jest.setTimeout(10000);

const {
    secureRandom,
    createProducer,
    createTopic,
} = require('../testhelpers');

describe('Producer', () => {
    let producer, topicName, message;
    const partitions = 3;

    beforeEach(async () => {
        producer = createProducer({
        }, {
            'linger.ms': 0,
        });

        topicName = `test-topic-${secureRandom()}`;

        await createTopic({ topic: topicName, partitions: 3 });
    });

    afterEach(async () => {
        producer && (await producer.disconnect());
    });


    it('can send messages concurrently',
        async () => {
            await producer.connect();
            const sender = async (p) => {
                message = { partition: p, value: `value-${secureRandom()}` };
                const report = await producer.send({ topic: topicName, messages: [message] });
                return report;
            };
            const reports = await Promise.all(Array(partitions).fill().map((_, i) => sender(i)));
            expect(reports.length).toBe(partitions);
            for (let i = 0; i < partitions; i++) {
                expect(reports[i].length).toBe(1);
                expect(reports[i][0].partition).toBe(i);
            }
        }
    );
});
