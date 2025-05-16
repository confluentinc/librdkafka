jest.setTimeout(10000);

const {
    secureRandom,
    createProducer,
    createTopic,
} = require('../testhelpers');
const { Kafka } = require('../../../lib').KafkaJS;
const process = require('process');

describe('Producer > Flush', () => {
    let producer, topicName, message;

    beforeEach(async () => {
        producer = createProducer({
        }, {
            'linger.ms': 5000, /* large linger ms to test flush */
            'queue.buffering.max.kbytes': 2147483647, /* effectively unbounded */
        });

        topicName = `test-topic-${secureRandom()}`;
        message = { key: `key-${secureRandom()}`, value: `value-${secureRandom()}` };

        await createTopic({ topic: topicName });
    });

    afterEach(async () => {
        producer && (await producer.disconnect());
    });


    it('does not wait for linger.ms',
        async () => {
            await producer.connect();
            let messageSent = false;
            const startTime = process.hrtime();
            let diffTime;

            producer.send({ topic: topicName, messages: [message] }).then(() => {
                messageSent = true;
                diffTime = process.hrtime(startTime);
            });

            await producer.flush({ timeout: 5000 });
            expect(messageSent).toBe(true);

            const diffTimeSeconds = diffTime[0] + diffTime[1] / 1e9;
            expect(diffTimeSeconds).toBeLessThan(5);
        }
    );

    it('does not matter when awaiting sends',
        async () => {
            await producer.connect();
            let messageSent = false;
            const startTime = process.hrtime();
            let diffTime;

            await producer.send({ topic: topicName, messages: [message] }).then(() => {
                messageSent = true;
                diffTime = process.hrtime(startTime);
            });

            await producer.flush({ timeout: 1000 });
            expect(messageSent).toBe(true);

            const diffTimeSeconds = diffTime[0] + diffTime[1] / 1e9;
            expect(diffTimeSeconds).toBeGreaterThan(5);
        }
    );

    it('times out if messages are pending',
        async () => {
            await producer.connect();
            let messageSent = false;

            /* Larger number of messages */
            producer.send({ topic: topicName, messages: Array(1000).fill(message) }).then(() => {
                messageSent = true;
            });

            /* Small timeout */
            await expect(producer.flush({ timeout: 1 })).rejects.toThrow(Kafka.KafkaJSTimeout);
            expect(messageSent).toBe(false);
        }
    );

});
