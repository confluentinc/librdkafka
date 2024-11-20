jest.setTimeout(30000);

const {
    secureRandom,
    createProducer,
    createConsumer,
} = require('../testhelpers');

describe.each(["producer", "consumer"])('Dependent admin client (%s)', (dependentOn) => {
    let admin, underlyingClient;

    beforeEach(async () => {
        if (dependentOn === "producer") {
            underlyingClient = createProducer({});
        } else {
            underlyingClient = createConsumer({ groupId: `test-group-${secureRandom()}` });
        }
        admin = underlyingClient.dependentAdmin();
    });

    afterEach(async () => {
        admin && (await admin.disconnect());
        underlyingClient && (await underlyingClient.disconnect());
    });

    it('should connect and work for connected underlying client', async () => {
        await underlyingClient.connect();
        await admin.connect();

        const listTopicsResult = await admin.listTopics();
        expect(listTopicsResult).toBeInstanceOf(Array);
    });

    it('should not connect for unconnected underlying client', async () => {
        await expect(admin.connect()).rejects.toHaveProperty('message', 'Underlying client is not connected.');

        underlyingClient = null; // prevents disconnect call
        admin = null; // prevents disconnect call
    });

    it('should not connect for disconnected underlying client', async () => {
        await underlyingClient.connect();
        await underlyingClient.disconnect();

        await expect(admin.connect()).rejects.toHaveProperty('message', 'Existing client must be connected before creating a new client from it');

        underlyingClient = null; // prevents disconnect call
        admin = null; // prevents disconnect call
    });
});

