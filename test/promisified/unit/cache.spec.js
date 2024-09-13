const MessageCache = require('../../../lib/kafkajs/_consumer_cache');

describe('MessageCache', () => {
    const messages =
        Array(5000)
            .fill()
            .map((_, i) => ({ topic: 'topic', partition: i % 3, number: i }));

    let cache;
    beforeEach(() => {
        cache = new MessageCache();
    });

    it('caches messages and retrieves them', () => {
        const msgs = messages.slice(0, 90);
        cache.addMessages(msgs);

        const receivedMessages = [];
        let ppc = null, next = null;
        for (let i = 0; i < 90; i++) {
            next = cache.next(ppc);           
            expect(next).not.toBeNull();
            [next, ppc] = next;
            expect(next).not.toBeNull();
            receivedMessages.push(next);
        }

        /* Results are on a per-partition basis and well-ordered */
        expect(receivedMessages.slice(1, 30).every((msg, i) => msg.partition === receivedMessages[0].partition && (msg.number - 3) ===  receivedMessages[i].number)).toBeTruthy();
        expect(receivedMessages.slice(31, 30).every((msg, i) => msg.partition === receivedMessages[30].partition && (msg.number - 3) ===  receivedMessages[i].number)).toBeTruthy();
        expect(receivedMessages.slice(61, 30).every((msg, i) => msg.partition === receivedMessages[60].partition && (msg.number - 3) ===  receivedMessages[i].number)).toBeTruthy();
    });

    it('caches messages and retrieves N of them', () => {
        const msgs = messages.slice(0, 90);
        cache.addMessages(msgs);

        const receivedMessages = [];
        let ppc = null, next = null;
        const expectedFetchedSizes = [11, 11, 8];
        for (let i = 0; i < (90/11); i++) {
            /* We choose to fetch 11 messages together rather than 10 so that we can test the case where
                * remaining messages > 0 but less than requested size. */
            next = cache.nextN(ppc, 11);
            expect(next).not.toBeNull();
            [next, ppc] = next;
            /* There are 30 messages per partition, the first fetch will get 11, the second 11, and the last one
                * 8, and then it repeats for each partition. */
            expect(next.length).toBe(expectedFetchedSizes[i % 3]);
            expect(next).not.toBeNull();
            receivedMessages.push(...next);
        }

        /* Results are on a per-partition basis and well-ordered */
        expect(receivedMessages.slice(1, 30).every((msg, i) => msg.partition === receivedMessages[0].partition && (msg.number - 3) ===  receivedMessages[i].number)).toBeTruthy();
        expect(receivedMessages.slice(31, 30).every((msg, i) => msg.partition === receivedMessages[30].partition && (msg.number - 3) ===  receivedMessages[i].number)).toBeTruthy();
        expect(receivedMessages.slice(61, 30).every((msg, i) => msg.partition === receivedMessages[60].partition && (msg.number - 3) ===  receivedMessages[i].number)).toBeTruthy();
    });

    it('stops fetching from stale partition', () => {
        const msgs = messages.slice(0, 90);
        cache.addMessages(msgs);

        const receivedMessages = [];
        let ppc = null, next = null;
        for (let i = 0; i < 3; i++) {
            next = cache.next(null);
            expect(next).not.toBeNull();
            [next, ppc] = next;
            expect(next).not.toBeNull();
            receivedMessages.push(next);
            cache.markStale([{topic: next.topic, partition: next.partition}]);
        }

        // We should not be able to get anything more.
        expect(cache.next(ppc)).toBeNull();
        // Nothing should be pending, we've returned everything.
        expect(cache.assignedSize).toBe(0);
        // The first 3 messages from different toppars are what we should get.
        expect(receivedMessages).toEqual(expect.arrayContaining(msgs.slice(0, 3)));
    });

    it('caches messages and retrieves 2-at-a-time', () => {
        const msgs = messages.slice(0, 90).filter(msg => msg.partition !== 3);
        cache.addMessages(msgs);

        const receivedMessages = [];
        let next = [null, null];
        let nextPpc = [null, null];
        for (let i = 0; i < 30; i++) {
            next[0] = cache.next(nextPpc[0]);
            next[1] = cache.next(nextPpc[1]);
            expect(next[0]).not.toBeNull();
            expect(next[1]).not.toBeNull();
            [next[0], nextPpc[0]] = next[0];
            [next[1], nextPpc[1]] = next[1];
            receivedMessages.push(next[0]);
            receivedMessages.push(next[1]);
        }

        expect(receivedMessages.length).toBe(60);
        expect(receivedMessages.filter(msg => msg.partition === 0).length).toBe(30);
        expect(receivedMessages.filter(msg => msg.partition === 1).length).toBe(30);
    });

    it('caches messages and retrieves N of them 2-at-a-time', () => {
        const msgs = messages.slice(0, 90).filter(msg => msg.partition !== 3);
        cache.addMessages(msgs);

        const receivedMessages = [];
        let next = [null, null];
        let nextPpc = [null, null];
        for (let i = 0; i < 30/11; i++) {
            next[0] = cache.nextN(nextPpc[0], 11);
            next[1] = cache.nextN(nextPpc[1], 11);
            expect(next[0]).not.toBeNull();
            expect(next[1]).not.toBeNull();
            [next[0], nextPpc[0]] = next[0];
            [next[1], nextPpc[1]] = next[1];
            receivedMessages.push(...next[0]);
            receivedMessages.push(...next[1]);
        }

        expect(receivedMessages.length).toBe(60);
        expect(receivedMessages.filter(msg => msg.partition === 0).length).toBe(30);
        expect(receivedMessages.filter(msg => msg.partition === 1).length).toBe(30);
    });

    it('does not allow fetching messages more than available partitions at a time', () => {
        const msgs = messages.slice(0, 90);
        cache.addMessages(msgs);

        let next = cache.next();
        let ppc = next[1];
        expect(next).not.toBeNull();
        next = cache.next();
        expect(next).not.toBeNull();
        next = cache.next();
        expect(next).not.toBeNull();
        next = cache.next();
        expect(next).toBeNull();
        expect(cache.assignedSize).toBe(3);

        // Fetch after returning ppc works.
        cache.return(ppc);
        next = cache.next();
        expect(next).not.toBeNull();
    });


    it('does not allow fetching message sets more than available partitions at a time', () => {
        const msgs = messages.slice(0, 90);
        cache.addMessages(msgs);

        let next = cache.nextN(null, 11);
        let ppc = next[1];
        expect(next).not.toBeNull();
        next = cache.nextN(null, 11);
        expect(next).not.toBeNull();
        next = cache.nextN(null, 11);
        expect(next).not.toBeNull();
        next = cache.nextN(null, 11);
        expect(next).toBeNull();
        expect(cache.assignedSize).toBe(3);

        // Fetch after returning ppc works.
        cache.return(ppc);
        next = cache.nextN(null, 11);
        expect(next).not.toBeNull();
    });

    it('stops fetching message sets from stale partition', () => {
        const msgs = messages.slice(0, 90);
        cache.addMessages(msgs);

        const receivedMessages = [];
        let next, ppc;
        for (let i = 0; i < 3; i++) {
            next = cache.nextN(null, 11);
            expect(next).not.toBeNull();
            [next, ppc] = next;
            receivedMessages.push(...next);
            cache.markStale([{topic: next[0].topic, partition: next[0].partition}]);
            cache.return(ppc);
        }

        // We should not be able to get anything more.
        expect(cache.nextN(null, 11)).toBeNull();
        // Nothing should be pending, we've returned everything.
        expect(cache.assignedSize).toBe(0);
        // The first [11, 11, 11] messages from different toppars.
        expect(receivedMessages.length).toBe(33);
        expect(receivedMessages).toEqual(expect.arrayContaining(msgs.slice(0, 33)));
    });

    it('one slow processing message should not slow down others', () => {
        const msgs = messages.slice(0, 90);
        cache.addMessages(msgs);

        const receivedMessages = [];
        let next, ppc;
        cache.next(ppc);
        for (let i = 0; i < 60; i++) { /* 60 - for non-partition 0 msgs */
            next = cache.next(ppc);
            expect(next).not.toBeNull();
            [next, ppc] = next;
            expect(next).not.toBeNull();
            receivedMessages.push(next);
        }

        // We should not be able to get anything more.
        expect(cache.next(ppc)).toBeNull();
        // The slowMsg should be pending.
        expect(cache.assignedSize).toBe(1);

        /* Messages should be partition-wise and well-ordered. */
        expect(receivedMessages.slice(1, 30).every((msg, i) => msg.partition === receivedMessages[0].partition && (msg.number - 3) ===  receivedMessages[i].number)).toBeTruthy();
        expect(receivedMessages.slice(31, 30).every((msg, i) => msg.partition === receivedMessages[30].partition && (msg.number - 3) ===  receivedMessages[i].number)).toBeTruthy();
    });

    it('one slow processing message set should not slow down others', () => {
        const msgs = messages.slice(0, 90);
        cache.addMessages(msgs);

        const receivedMessages = [];
        let next, ppc;
        cache.nextN(ppc, 11);
        for (let i = 0; i < 60/11; i++) { /* 60 - for non-partition 0 msgs */
            next = cache.nextN(ppc, 11);
            expect(next).not.toBeNull();
            [next, ppc] = next;
            receivedMessages.push(...next);
        }


        // We should not be able to get anything more.
        expect(cache.nextN(ppc, 11)).toBeNull();
        // The slowMsg should be pending.
        expect(cache.assignedSize).toBe(1);

        /* Messages should be partition-wise and well-ordered. */
        expect(receivedMessages.slice(1, 30).every((msg, i) => msg.partition === receivedMessages[0].partition && (msg.number - 3) ===  receivedMessages[i].number)).toBeTruthy();
        expect(receivedMessages.slice(31, 30).every((msg, i) => msg.partition === receivedMessages[30].partition && (msg.number - 3) ===  receivedMessages[i].number)).toBeTruthy();
    });

    it('should be able to handle cache-clearance in the middle of processing', () => {
        const msgs = messages.slice(0, 90);
        cache.addMessages(msgs);

        const receivedMessages = [];
        let next, ppc;
        cache.next();
        for (let i = 0; i < 60; i++) { /* 60 - for non-partition 0 msgs */
            next = cache.next(ppc);
            expect(next).not.toBeNull();
            [next, ppc] = next;
            expect(next).not.toBeNull();
            receivedMessages.push(next);
        }

        // We should not be able to get anything more.
        expect(cache.next(ppc)).toBeNull();

        // The slowMsg should be pending.
        expect(cache.assignedSize).toBe(1);

        expect(() => cache.clear()).not.toThrow();
    });

    it('should be able to handle message adds in the middle of processing', () => {
        const msgs = messages.slice(0, 90);
        cache.addMessages(msgs);

        const receivedMessages = [];
        let next, ppc;
        cache.next();
        for (let i = 0; i < 60; i++) { /* 60 - for non-partition 0 msgs */
            next = cache.next(ppc);
            expect(next).not.toBeNull();
            [next, ppc] = next;
            expect(next).not.toBeNull();
            receivedMessages.push(next);
        }

        // We should not be able to get anything more.
        expect(cache.next(ppc)).toBeNull();

        // The slowMsg should be pending.
        expect(cache.assignedSize).toBe(1);

        expect(() => cache.addMessages(msgs)).not.toThrow();
    });
});