jest.setTimeout(10000);

const {
    createAdmin,
} = require('../testhelpers');

describe('Admin', () => {
    it('can receive stats callbacks',
        async () => {
            let numCalls = 0;
            let admin = createAdmin({}, {
                'statistics.interval.ms': '100',
                'stats_cb': function (event) {
                    expect(event).toHaveProperty('message');
                    expect(event.message).toContain('"type":');
                    numCalls++;
                }
            });
            await admin.connect();
            await new Promise((resolve) => setTimeout(resolve, 400));
            await admin.disconnect();
            expect(numCalls).toBeGreaterThanOrEqual(3);
        }
    );
});
