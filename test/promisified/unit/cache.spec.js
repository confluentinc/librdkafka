const MessageCache = require('../../../lib/kafkajs/_consumer_cache');

describe('MessageCache', () => {
    const expiryTime = 300000; // Long time.
    const toppars = [{ topic: 'topic', partition: 0 }, { topic: 'topic', partition: 1 }, { topic: 'topic', partition: 2 }];
    const messages =
        Array(5000)
            .fill()
            .map((_, i) => ({ topic: 'topic', partition: i % 3, number: i }));

    describe("with concurrency", () => {
        let cache;
        beforeEach(() => {
            cache = new MessageCache(expiryTime, 1);
            cache.addTopicPartitions(toppars);
        });

        it('caches messages and retrieves them', () => {
            const msgs = messages.slice(0, 90);
            cache.addMessages(msgs);

            const receivedMessages = [];
            let nextIdx = -1;
            for (let i = 0; i < 90; i++) {
                const next = cache.next(nextIdx);
                expect(next).not.toBeNull();
                receivedMessages.push(next);
                nextIdx = next.index;
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
            let nextIdx = -1;
            const expectedFetchedSizes = [11, 11, 8];
            for (let i = 0; i < (90/11); i++) {
                /* We choose to fetch 11 messages together rather than 10 so that we can test the case where
                 * remaining messages > 0 but less than requested size. */
                const next = cache.nextN(nextIdx, 11);
                /* There are 30 messages per partition, the first fetch will get 11, the second 11, and the last one
                 * 8, and then it repeats for each partition. */
                expect(next.length).toBe(expectedFetchedSizes[i % 3]);
                expect(next).not.toBeNull();
                receivedMessages.push(...next);
                nextIdx = next.index;
            }

            /* Results are on a per-partition basis and well-ordered */
            expect(receivedMessages.slice(1, 30).every((msg, i) => msg.partition === receivedMessages[0].partition && (msg.number - 3) ===  receivedMessages[i].number)).toBeTruthy();
            expect(receivedMessages.slice(31, 30).every((msg, i) => msg.partition === receivedMessages[30].partition && (msg.number - 3) ===  receivedMessages[i].number)).toBeTruthy();
            expect(receivedMessages.slice(61, 30).every((msg, i) => msg.partition === receivedMessages[60].partition && (msg.number - 3) ===  receivedMessages[i].number)).toBeTruthy();
        });

        it('does not allow fetching more than 1 message at a time', () => {
            const msgs = messages.slice(0, 90);
            cache.addMessages(msgs);

            let next = cache.next(-1);
            let savedIndex = next.index;
            expect(next).not.toBeNull();
            next = cache.next(-1);
            expect(next).toBeNull();
            expect(cache.pendingSize()).toBeGreaterThan(0);

            // Fetch after returning index works.
            next = cache.next(savedIndex);
            expect(next).not.toBeNull();
        });

        it('stops fetching from stale partition', () => {
            const msgs = messages.slice(0, 90);
            cache.addMessages(msgs);

            const receivedMessages = [];
            let nextIdx = -1;
            for (let i = 0; i < 3; i++) {
                const next = cache.next(nextIdx);
                expect(next).not.toBeNull();
                receivedMessages.push(next);
                nextIdx = next.index;
                cache.markStale([{topic: next.topic, partition: next.partition}]);
            }

            // We should not be able to get anything more.
            expect(cache.next(nextIdx)).toBeNull();
            // Nothing should be pending, we've returned everything.
            expect(cache.pendingSize()).toBe(0);
            // The first 3 messages from different toppars are what we should get.
            expect(receivedMessages).toEqual(expect.arrayContaining(msgs.slice(0, 3)));
        });

    });

    describe("with concurrency = 2", () => {
        let cache;
        beforeEach(() => {
            cache = new MessageCache(expiryTime, 2);
            cache.addTopicPartitions(toppars);
        });

        it('caches messages and retrieves them', () => {
            const msgs = messages.slice(0, 90);
            cache.addMessages(msgs);

            const receivedMessages = [];
            let nextIdx = -1;
            for (let i = 0; i < 90; i++) {
                const next = cache.next(nextIdx);
                expect(next).not.toBeNull();
                receivedMessages.push(next);
                nextIdx = next.index;
            }

            /* Results are on a per-partition basis and well-ordered */
            expect(receivedMessages.slice(1, 30).every((msg, i) => msg.partition === receivedMessages[0].partition && (msg.number - 3) ===  receivedMessages[i].number)).toBeTruthy();
            expect(receivedMessages.slice(31, 30).every((msg, i) => msg.partition === receivedMessages[30].partition && (msg.number - 3) ===  receivedMessages[i].number)).toBeTruthy();
            expect(receivedMessages.slice(61, 30).every((msg, i) => msg.partition === receivedMessages[60].partition && (msg.number - 3) ===  receivedMessages[i].number)).toBeTruthy();
        });

        it('caches messages and retrieves 2-at-a-time', () => {
            const msgs = messages.slice(0, 90).filter(msg => msg.partition !== 3);
            cache.addMessages(msgs);

            const receivedMessages = [];
            let nextIdxs = [-1, -1];
            for (let i = 0; i < 30; i++) {
                const next0 = cache.next(nextIdxs[0]);
                const next1 = cache.next(nextIdxs[1]);
                expect(next0).not.toBeNull();
                expect(next1).not.toBeNull();
                receivedMessages.push(next0);
                receivedMessages.push(next1);
                nextIdxs = [next0.index, next1.index];
            }

            expect(receivedMessages.length).toBe(60);
            expect(receivedMessages.filter(msg => msg.partition === 0).length).toBe(30);
            expect(receivedMessages.filter(msg => msg.partition === 1).length).toBe(30);
        });

        it('caches messages and retrieves N of them 2-at-a-time', () => {
            const msgs = messages.slice(0, 90).filter(msg => msg.partition !== 3);
            cache.addMessages(msgs);

            const receivedMessages = [];
            let nextIdxs = [-1, -1];
            for (let i = 0; i < 30/11; i++) {
                const next0 = cache.nextN(nextIdxs[0], 11);
                const next1 = cache.nextN(nextIdxs[1], 11);
                expect(next0).not.toBeNull();
                expect(next1).not.toBeNull();
                receivedMessages.push(...next0);
                receivedMessages.push(...next1);
                nextIdxs = [next0.index, next1.index];
            }

            expect(receivedMessages.length).toBe(60);
            expect(receivedMessages.filter(msg => msg.partition === 0).length).toBe(30);
            expect(receivedMessages.filter(msg => msg.partition === 1).length).toBe(30);
        });

        it('does not allow fetching more than 2 message at a time', () => {
            const msgs = messages.slice(0, 90);
            cache.addMessages(msgs);

            let next = cache.next(-1);
            let savedIndex = next.index;
            expect(next).not.toBeNull();
            next = cache.next(-1);
            expect(next).not.toBeNull();
            next = cache.next(-1);
            expect(next).toBeNull();
            expect(cache.pendingSize()).toBe(2);

            // Fetch after returning index works.
            next = cache.next(savedIndex);
            expect(next).not.toBeNull();
        });


        it('does not allow fetching more than 2 message sets at a time', () => {
            const msgs = messages.slice(0, 90);
            cache.addMessages(msgs);

            let next = cache.nextN(-1, 11);
            let savedIndex = next.index;
            expect(next).not.toBeNull();
            next = cache.nextN(-1, 11);
            expect(next).not.toBeNull();
            next = cache.nextN(-1, 11);
            expect(next).toBeNull();
            expect(cache.pendingSize()).toBe(2);

            // Fetch after returning index works.
            next = cache.nextN(savedIndex, 11);
            expect(next).not.toBeNull();
        });

        it('stops fetching from stale partition', () => {
            const msgs = messages.slice(0, 90);
            cache.addMessages(msgs);

            const receivedMessages = [];
            let nextIdx = -1;
            for (let i = 0; i < 3; i++) {
                const next = cache.next(nextIdx);
                expect(next).not.toBeNull();
                receivedMessages.push(next);
                nextIdx = next.index;
                cache.markStale([{topic: next.topic, partition: next.partition}]);
            }

            // We should not be able to get anything more.
            expect(cache.next(nextIdx)).toBeNull();
            // Nothing should be pending, we've returned everything.
            expect(cache.pendingSize()).toBe(0);
            // The first 3 messages from different toppars are what we should get.
            expect(receivedMessages).toEqual(expect.arrayContaining(msgs.slice(0, 3)));
        });

        it('stops fetching message sets from stale partition', () => {
            const msgs = messages.slice(0, 90);
            cache.addMessages(msgs);

            const receivedMessages = [];
            let nextIdx = -1;
            for (let i = 0; i < 3; i++) {
                const next = cache.nextN(nextIdx, 11);
                expect(next).not.toBeNull();
                receivedMessages.push(...next);
                nextIdx = next.index;
                cache.markStale([{topic: next[0].topic, partition: next[0].partition}]);
            }

            // We should not be able to get anything more.
            expect(cache.nextN(nextIdx, 11)).toBeNull();
            // Nothing should be pending, we've returned everything.
            expect(cache.pendingSize()).toBe(0);
            // The first [11, 11, 11] messages from different toppars.
            expect(receivedMessages.length).toBe(33);
            expect(receivedMessages).toEqual(expect.arrayContaining(msgs.slice(0, 33)));
        });

        it('one slow processing message should not slow down others', () => {
            const msgs = messages.slice(0, 90);
            cache.addMessages(msgs);

            const receivedMessages = [];
            let nextIdx = -1;
            cache.next(nextIdx);
            for (let i = 0; i < 60; i++) { /* 60 - for non-partition 0 msgs */
                const next = cache.next(nextIdx);
                expect(next).not.toBeNull();
                receivedMessages.push(next);
                nextIdx = next.index;
            }


            // We should not be able to get anything more.
            expect(cache.next(nextIdx)).toBeNull();
            // The slowMsg should be pending.
            expect(cache.pendingSize()).toBe(1);

            /* Messages should be partition-wise and well-ordered. */
            expect(receivedMessages.slice(1, 30).every((msg, i) => msg.partition === receivedMessages[0].partition && (msg.number - 3) ===  receivedMessages[i].number)).toBeTruthy();
            expect(receivedMessages.slice(31, 30).every((msg, i) => msg.partition === receivedMessages[30].partition && (msg.number - 3) ===  receivedMessages[i].number)).toBeTruthy();
        });

        it('one slow processing message set should not slow down others', () => {
            const msgs = messages.slice(0, 90);
            cache.addMessages(msgs);

            const receivedMessages = [];
            let nextIdx = -1;
            cache.nextN(nextIdx, 11);
            for (let i = 0; i < 60/11; i++) { /* 60 - for non-partition 0 msgs */
                const next = cache.nextN(nextIdx, 11);
                expect(next).not.toBeNull();
                receivedMessages.push(...next);
                nextIdx = next.index;
            }


            // We should not be able to get anything more.
            expect(cache.nextN(nextIdx, 11)).toBeNull();
            // The slowMsg should be pending.
            expect(cache.pendingSize()).toBe(1);

            /* Messages should be partition-wise and well-ordered. */
            expect(receivedMessages.slice(1, 30).every((msg, i) => msg.partition === receivedMessages[0].partition && (msg.number - 3) ===  receivedMessages[i].number)).toBeTruthy();
            expect(receivedMessages.slice(31, 30).every((msg, i) => msg.partition === receivedMessages[30].partition && (msg.number - 3) ===  receivedMessages[i].number)).toBeTruthy();
        });

        it('should not be able to handle cache-clearance in the middle of processing', () => {
            const msgs = messages.slice(0, 90);
            cache.addMessages(msgs);

            const receivedMessages = [];
            let nextIdx = -1;
            cache.next(nextIdx);
            for (let i = 0; i < 60; i++) { /* 60 - for non-partition 0 msgs */
                const next = cache.next(nextIdx);
                expect(next).not.toBeNull();
                receivedMessages.push(next);
                nextIdx = next.index;
            }


            // We should not be able to get anything more.
            expect(cache.next(nextIdx)).toBeNull();

            // The slowMsg should be pending.
            expect(cache.pendingSize()).toBe(1);

            expect(() => cache.clear()).toThrow();
        });

        it('should not be able to handle message adds in the middle of processing', () => {
            const msgs = messages.slice(0, 90);
            cache.addMessages(msgs);

            const receivedMessages = [];
            let nextIdx = -1;
            cache.next(nextIdx);
            for (let i = 0; i < 60; i++) { /* 60 - for non-partition 0 msgs */
                const next = cache.next(nextIdx);
                expect(next).not.toBeNull();
                receivedMessages.push(next);
                nextIdx = next.index;
            }

            // We should not be able to get anything more.
            expect(cache.next(nextIdx)).toBeNull();

            // The slowMsg should be pending.
            expect(cache.pendingSize()).toBe(1);

            expect(() => cache.addMessages(msgs)).toThrow();
        });
    });
});