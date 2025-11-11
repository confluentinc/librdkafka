jest.setTimeout(10000);

const {
    createProducer,
} = require('../testhelpers');

describe('Producer', () => {
    it('can receive stats callbacks',
        async () => {
            let numCalls = 0;
            let producer = createProducer({}, {
                'statistics.interval.ms': '100',
                'stats_cb': function (event) {
                    expect(event).toHaveProperty('message');
                    expect(event.message).toContain('"type":');
                    numCalls++;
                }
            });
            await producer.connect();
            await new Promise((resolve) => setTimeout(resolve, 400));
            await producer.disconnect();
            expect(numCalls).toBeGreaterThanOrEqual(3);
        }
    );
});
