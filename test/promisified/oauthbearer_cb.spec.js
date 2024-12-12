// minimum 30s are needed for the connect timeouts of consumer/producer
jest.setTimeout(35000);

const {
    createProducer,
    sleep,
    createConsumer,
    createAdmin,
} = require('./testhelpers');

describe('Client > oauthbearer callback', () => {
    let oauthbearer_cb_called = 0;
    const oauthbearer_config = 'key=value';
    const providerCb = async (config) => {
        expect(config).toEqual(oauthbearer_config);
        oauthbearer_cb_called++;
        throw new Error('oauthbearer_cb error');
    };

    beforeEach(async () => {
        oauthbearer_cb_called = 0;
    });

    it('works for producer',
        async () => {
            const client = createProducer({
                sasl: {
                    mechanism: 'OAUTHBEARER',
                    oauthBearerProvider: providerCb,
                }
            }, {
                'sasl.oauthbearer.config': oauthbearer_config,
            });

            await expect(client.connect()).rejects.toThrow('oauthbearer_cb error');
            expect(oauthbearer_cb_called).toBeGreaterThanOrEqual(1);
            await client.disconnect();
        }
    );

    it('works for consumer',
        async () => {
            const client = createConsumer({
                groupId: 'gid',
                sasl: {
                    mechanism: 'OAUTHBEARER',
                    oauthBearerProvider: providerCb,
                }
            }, {
                'sasl.oauthbearer.config': oauthbearer_config,
            });

            await expect(client.connect()).rejects.toThrow('oauthbearer_cb error');
            expect(oauthbearer_cb_called).toBeGreaterThanOrEqual(1);
            await client.disconnect();
        }
    );

    it('works for admin',
        async () => {
            const client = createAdmin({
                sasl: {
                    mechanism: 'OAUTHBEARER',
                    oauthBearerProvider: providerCb,
                }
            }, {
                'sasl.oauthbearer.config': oauthbearer_config,
            });

            // Unlike others, there is no actual connection establishment
            // within the admin client, so we can't test for the error here.
            await expect(client.connect()).resolves.toBeUndefined();

            await sleep(2000); // Wait for the callback to be called
            expect(oauthbearer_cb_called).toBeGreaterThanOrEqual(1);
            await client.disconnect();
        }
    );

});
