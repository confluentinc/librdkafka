jest.setTimeout(10000);

const {
    createConsumer,
} = require('../testhelpers');

describe('Consumer', () => {
    it('can receive stats callbacks',
        async () => {
            let numCalls = 0;
            let consumer = createConsumer({}, {
                'group.id': `group-${Date.now()}`,
                'statistics.interval.ms': '100',
                'stats_cb': function (event) {
                    expect(event).toHaveProperty('message');
                    expect(event.message).toContain('"type":');
                    numCalls++;
                }
            });
            await consumer.connect();
            await new Promise((resolve) => setTimeout(resolve, 400));
            await consumer.disconnect();
            expect(numCalls).toBeGreaterThanOrEqual(3);
        }
    );
});
