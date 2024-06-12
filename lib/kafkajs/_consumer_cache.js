const { hrtime } = require('process');
const {
    partitionKey,
} = require('./_common');

/**
 * A PerPartitionMessageCache is a cache for messages for a single partition.
 */
class PerPartitionMessageCache {
    /* The cache is a list of messages. */
    cache = [];
    /* Index of next element to be fetched in the cache. */
    currentIndex = 0;
    /* Whether the cache is stale. */
    stale = false;

    /**
     * Returns the number of total elements in the cache.
     */
    size() {
        return this.cache.length;
    }

    /**
     * Clears the cache.
     */
    clear() {
        this.cache = [];
        this.currentIndex = 0;
        this.stale = false;
    }

    /**
     * Adds a message to the cache.
     */
    add(message) {
        this.cache.push(message);
    }

    /**
     * Returns whether the cache is stale.
     */
    isStale() {
        return this.stale;
    }

    /**
     * @returns The next element in the cache or null if none exists.
     * @warning Does not check for staleness.
     */
    next() {
        return this.currentIndex < this.cache.length ? this.cache[this.currentIndex++] : null;
    }
}


/**
 * MessageCache defines a dynamically sized cache for messages.
 * Internally, it uses PerPartitionMessageCache to store messages for each partition.
 * The capacity is increased or decreased according to whether the last fetch of messages
 * was less than the current capacity or saturated the current capacity.
 */
class MessageCache {

    constructor(expiryDurationMs) {
        /* Per partition cache list containing non-empty PPCs */
        this.ppcList = [];
        /* Map of topic+partition to PerPartitionMessageCache. */
        this.tpToPpc = new Map();
        /* Index of the current PPC in the ppcList. */
        this.currentPpc = 0;
        /* Maximum size of the cache. (Capacity) */
        this.maxSize = 1;
        /* Number of times the size has been increased in a row, used for accounting for maxSize. */
        this.increaseCount = 0;
        /* Last cached time */
        this.cachedTime = hrtime();
        /* Whether the cache is stale. */
        this.stale = false;
        /* Expiry duration for this cache */
        this.expiryDurationMs = expiryDurationMs;
        /* A list of caches which have been marked stale since the last call to popLocallyStale or addMessages. */
        this.locallyStaleCaches = [];
    }

    /**
     * Add a set of topic partitions to the cache (empty PPCs).
     * Pre-conditions: ppcList must be empty (cache is inactive)
     */
    addTopicPartitions(topicPartitions) {
        if (this.ppcList.length !== 0) {
            throw new Error('Cannot add topic partitions to a non-empty cache.');
        }
        for (const topicPartition of topicPartitions) {
            const key = partitionKey(topicPartition);
            this.tpToPpc.set(key, new PerPartitionMessageCache());
        }
    }

    /**
     * Remove a set of topic partitions from the cache.
     * If topicPartitions is null, removes everything.
     * Pre-conditions: ppcList must be empty (cache is inactive)
     */
    removeTopicPartitions(topicPartitions = null) {
        if (this.ppcList.length !== 0) {
            throw new Error('Cannot remove topic partitions from a non-empty cache.');
        }

        if (topicPartitions === null) {
            this.tpToPpc.clear();
            return;
        }
        for (const topicPartition of topicPartitions) {
            const key = partitionKey(topicPartition);
            this.tpToPpc.delete(key);
        }
    }

    /**
     * Returns whether the cache is globally stale.
     */
    isStale() {
        if (this.stale)
            return true;

        const cacheTime = hrtime(this.cachedTime);
        const cacheTimeMs = Math.floor(cacheTime[0] * 1000 + cacheTime[1] / 1000000);
        this.stale = cacheTimeMs > this.expiryDurationMs;

        return this.stale;
    }

    /**
     * If there are any locally stale caches, return them, and clear
     * the list of locally stale caches.
     */
    popLocallyStale() {
        if (this.locallyStaleCaches.length > 0) {
            const locallyStale = this.locallyStaleCaches;
            this.locallyStaleCaches = [];
            return locallyStale;
        }
        return [];
    }

    /**
     * Mark a set of topic partitions 'stale'.
     * If no topic partitions are provided, marks the entire cache as stale globally.
     *
     * Pre-conditions: toppars must be in tpToPpc, may or may not be in ppcList.
     * Post-conditions: PPCs marked stale, locally stale caches updated to contain said toppars.
     */
    markStale(topicPartitions = null) {
        if (!topicPartitions) {
            this.stale = true;
            return;
        }

        for (const topicPartition of topicPartitions) {
            const key = partitionKey(topicPartition);
            const cache = this.tpToPpc.get(key);
            if (!cache)
                continue;

            if (!cache.stale) {
                /* Newly stale cache, so add it into list of such caches. */
                this.locallyStaleCaches.push(topicPartition);
            }
            cache.stale = true;
        }
    }

    /**
     * Request a size increase.
     * It increases the size by 2x, but only if the size is less than 1024,
     * only if the size has been requested to be increased twice in a row.
     */
    increaseMaxSize() {
        if (this.maxSize === 1024)
            return;

        this.increaseCount++;
        if (this.increaseCount <= 1)
            return;

        this.maxSize = Math.min(this.maxSize << 1, 1024);
        this.increaseCount = 0;
    }

    /**
     * Request a size decrease.
     * It decreases the size to 80% of the last received size, with a minimum of 1.
     * @param {number} recvdSize - the number of messages received in the last poll.
     */
    decreaseMaxSize(recvdSize) {
        this.maxSize = Math.max(Math.floor((recvdSize * 8) / 10), 1);
        this.increaseCount = 0;
    }

    /**
     * Add a single message to a PPC.
     * Pre-conditions: PPC does not have stale messages.
     * Post-conditions: PPC is unstale, ppcList contains all caches with messages in them.
     */
    #add(message) {
        const key = partitionKey(message)
        const cache = this.tpToPpc.get(key);
        cache.add(message);
        if (cache.size() === 1) {
            this.ppcList.push(cache);
            /* Just in case this cache was marked stale by pause or seek, we unstale it now
             * that there are fresh messages in here. It is possible because markStale() can
             * mark toppar caches as stale without checking if they're in ppcList. */
            cache.stale = false;
        }
    }

    /**
     * Adds many messages into the cache, partitioning them as per their toppar.
     * Pre-conditions: no locally stale caches with messages in them.
     * Post-conditions: all caches are unstale, (todo: ppcList is sorted by timestamp).
     */
    addMessages(messages) {
        /* There will be caches in the ppcList which are either stale, or have
         * run out of messages. We need to clear them, else #add() will not add
         * them back to the ppcList since they're not empty. */
        this.ppcList.forEach(cache => cache.clear());
        this.currentPpc = 0;
        this.ppcList = [];

        if (this.locallyStaleCaches.length !== 0 && this.locallyStaleCaches.some(tp => {
            const key = partitionKey(tp);
            return this.tpToPpc.get(key).size() !== 0;
        })) {
            console.error('Locally stale caches should have been cleared before adding messages: ', this.locallyStaleCaches);
            throw new Error('Locally stale caches should have been cleared before adding messages.');
        }

        this.stale = false;
        this.cachedTime = hrtime();

        for (const message of messages)
            this.#add(message);

        // TODO: add ppcList sort step.
        // Rationale: ideally it's best to consume in the ascending order of timestamps.
    }

    /**
     * Returns the next element in the cache, or null if none exists.
     *
     * If the current PPC is exhausted, it moves to the next PPC.
     * If all PPCs are exhausted, it returns null.
     * @warning Does not check for global staleness. That is left up to the user.
     *          Skips locally stale messages.
     */
    next() {
        if (this.currentPpc >= this.ppcList.length) {
            return null;
        }

        let next = null;
        while (next === null && this.currentPpc < this.ppcList.length) {
            if (this.ppcList[this.currentPpc].isStale()) {
                this.currentPpc++;
                continue;
            }

            next = this.ppcList[this.currentPpc].next();
            if (next !== null)
                break;
            this.currentPpc++;
        }
        return next; // Caller is responsible for triggering fetch logic here if next == null.
    }

    /**
     * Clears the cache completely.
     * This resets it to a base state, and reduces the capacity of the cache back to 1.
     * Pre-conditions: none
     * Post-conditions: maxSize = 1, all caches are unstale, ppcList is empty, locallyStaleCaches is empty.
     */
    clear() {
        for (const cache of this.ppcList) {
            cache.clear();
        }
        this.ppcList = [];
        this.currentPpc = 0;
        this.maxSize = 1;
        this.increaseCount = 0;
        this.stale = false;
        this.cachedTime = hrtime();
        this.locallyStaleCaches = [];
    }
}

module.exports = MessageCache;
