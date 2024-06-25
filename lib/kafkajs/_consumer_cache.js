const { hrtime } = require('process');
const {
    partitionKey,
} = require('./_common');
const { Heap } = require('./_heap');

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

    /**
     * @returns Upto `n` next elements in the cache or an null if none available.
     * @warning Does not check for staleness.
     */
    nextN(n) {
        if (this.currentIndex >= this.cache.length) {
            return null;
        }

        if (this.currentIndex + n >= this.cache.length) {
            const res = this.cache.slice(this.currentIndex);
            this.currentIndex = this.cache.length;
            return res;
        }

        const res = this.cache.slice(this.currentIndex, this.currentIndex + n);
        this.currentIndex += n;
        return res;
    }
}


/**
 * MessageCache defines a dynamically sized cache for messages.
 * Internally, it uses PerPartitionMessageCache to store messages for each partition.
 * The capacity is increased or decreased according to whether the last fetch of messages
 * was less than the current capacity or saturated the current capacity.
 */
class MessageCache {

    constructor(expiryDurationMs, maxConcurrency, logger) {
        /* Per partition cache list containing non-empty PPCs */
        this.ppcList = [];
        /* Map of topic+partition to PerPartitionMessageCache. */
        this.tpToPpc = new Map();
        /* Index of the current PPC in the ppcList. */
        this.currentPpcTODO_remove_this = 0;
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
        /* Max allowed concurrency */
        this.maxConcurrency = maxConcurrency;
        /* Contains a list of indices of ppcList from which we are allowed to consume. */
        this.indices = new Heap();
        /* Largest ppc index we are allowed to consume from (inclusive). */
        this.maxIndicesIndex = 0;
        /* Contains a list of indices of ppcList from which we have sent a message returned through next, but
         * the user has not returned the index back to us via next(idx) */
        this.pendingIndices = new Set();
        /* Logger provided by cache user. Must have 'error' function defined on it. `console` is used by default. */
        this.logger = logger ?? console;
    }

    pendingSize() {
        return this.pendingIndices.size;
    }

    /**
     * Add a set of topic partitions to the cache (empty PPCs).
     * Pre-conditions: ppcList must be empty (cache is inactive)
     */
    addTopicPartitions(topicPartitions) {
        if (this.ppcList.some(ppc => (ppc.currentIndex < ppc.size()) && !ppc.stale)) {
            throw new Error('Cannot add topic partitions to a cache which contains unprocessed, unstale elements.');
        }
        for (const topicPartition of topicPartitions) {
            const key = partitionKey(topicPartition);
            const existing = this.tpToPpc.get(key);
            /* We're erring on the side of caution by including this check, as in the current model,
             * a rebalance occurs only if all the caches are drained. */
            if (existing && existing.cache.length > 0 && !existing.stale && existing.currentIndex < existing.cache.length) {
                this.logger.error("Cache already exists for key " + key + " with messages in it.");
                throw new Error("Cache already exists for key " + key + " with messages in it.");
            }
            this.tpToPpc.set(key, new PerPartitionMessageCache());
        }
    }

    /**
     * Remove a set of topic partitions from the cache.
     * If topicPartitions is null, removes everything.
     * Pre-conditions: ppcList must be empty (cache is inactive)
     */
    removeTopicPartitions(topicPartitions = null) {
        if (this.ppcList.some(ppc => (ppc.currentIndex < ppc.size()) && !ppc.stale)) {
            throw new Error('Cannot remove topic partitions from a cache which contains unprocessed, unstale elements.');
        }

        if (topicPartitions === null) {
            for (const key of this.tpToPpc.keys()) {
                const existing = this.tpToPpc.get(key);
                /* We're erring on the side of caution by including this check, as in the current model,
                * a rebalance occurs only if all the caches are drained. */
                if (existing && existing.cache.length > 0 && !existing.stale && existing.currentIndex < existing.cache.length) {
                    this.logger.error("Cache already exists for key " + key + " with messages in it.");
                    throw new Error("Cache already exists for key " + key + " with messages in it.");
                }
            }
            this.tpToPpc.clear();
            return;
        }
        for (const topicPartition of topicPartitions) {
            const key = partitionKey(topicPartition);
            const existing = this.tpToPpc.get(key);
            /* We're erring on the side of caution by including this check, as in the current model,
             * a rebalance occurs only if all the caches are drained. */
            if (existing && existing.cache.length > 0 && !existing.stale && existing.currentIndex < existing.cache.length) {
                this.logger.error("Cache already exists for key " + key + " with messages in it.");
                throw new Error("Cache already exists for key " + key + " with messages in it.");
            }
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
        if (!cache) {
            this.logger.error("No cache found for message", message);
            throw new Error("Inconsistency between fetched message and partition map");
        }
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
        if (this.pendingSize() > 0) {
            throw new Error(`Cache cannot be added to with ${this.pendingSize()} pending indices.`);
        }

        /* There will be caches in the ppcList which are either stale, or have
         * run out of messages. We need to clear them, else #add() will not add
         * them back to the ppcList since they're not empty. */
        this.ppcList.forEach(cache => cache.clear());
        this.ppcList = [];

        if (this.locallyStaleCaches.length !== 0 && this.locallyStaleCaches.some(tp => {
            const key = partitionKey(tp);
            return this.tpToPpc.get(key).size() !== 0;
        })) {
            logger.error('Locally stale caches should have been cleared before adding messages: ', this.locallyStaleCaches);
            throw new Error('Locally stale caches should have been cleared before adding messages.');
        }

        this.stale = false;
        this.cachedTime = hrtime();

        for (const message of messages)
            this.#add(message);

        // TODO: add ppcList sort step.
        // Rationale: ideally it's best to consume in the ascending order of timestamps.

        /* Reset the indices and pendingIndices because ppcList is being created newly. */
        this.indices.clear();
        if (this.pendingIndices.size > 0) logger.error('addMessages: pendingIndices = ', this.pendingIndices, console.trace());
        this.pendingIndices.clear();
        this.maxIndicesIndex = Math.min(this.maxConcurrency, this.ppcList.length - 1);
        for (let i = 0; i <= this.maxIndicesIndex; i++) {
            this.indices.push(i);
        }
    }

    /**
     * Allows returning the cache index of a consumed message without asking for another message.
     * @param {number} idx  - the index of the message that was consumed.
     * @note This is a no-op if the index is not in the pendingIndices set.
     */
    return(idx) {
        if (!this.pendingIndices.has(idx)) {
            /* The user is behaving well by returning the index to us, but in the meanwhile, it's possible
             * that we ran out of messages and fetched a new batch. So we just discard what the user is
             * returning to us. */
            this.logger.error("Returning unowned index", idx, "to cache. Discarding it.");
        } else {
            this.pendingIndices.delete(idx);
            this.indices.add(idx);
        }
    }

    /**
     * Returns the next element in the cache, or null if none exists.
     *
     * If the current PPC is exhausted, it moves to the next PPC.
     * If all PPCs are exhausted, it returns null.
     * @param {number} idx - after a consumer has consumed a message, it must return the index back to us via this parameter.
     *                       otherwise, no messages from that topic partition will be consumed.
     * @returns {Object} - the next message in the cache, or null if none exists. An `index` field is added to the message.
     * @warning Does not check for global staleness. That is left up to the user.
     *          Skips locally stale messages.
     * The topicPartition, if provided, MUST be one such that the user has fetched
     * the message from the same topicPartition earlier.
     * @note Whenever making changes to this function, ensure that you benchmark perf.
     */
    next(idx = -1) {
        let index = idx;
        if (index !== -1 && !this.pendingIndices.has(index)) {
            /* The user is behaving well by returning the index to us, but in the meanwhile, it's possible
             * that we ran out of messages and fetched a new batch. So we just discard what the user is
             * returning to us. */
            this.logger.error("Returning unowned index", idx, "to cache. Discarding it.");
            index = -1;
        } else if (index !== -1) {
            this.pendingIndices.delete(index);
            /* We don't add the index back to the this.indices here because we're just going to remove it again the
             * first thing in the loop below, so it's slightly better to just avoid doing it. */
        }

        if (index === -1) {
            if (this.indices.size() === 0 || this.pendingIndices.size === this.maxConcurrency) {
                return null;
            }
            index = this.indices.pop(); // index cannot be undefined here since indices.size > 0
        }

        /* This loop will always terminate. Why?
         * On each iteration:
         * 1. We either return (if next is not null).
         * 2. We change the PPC index we're interested in, and there are a finite number of PPCs.
         *    (PPCs don't repeat within the loop since the indices of the PPC are popped from within the
         *     heap and not put back in, or else a new index is created bounded by ppcList.length).
        */
        while (true) {
            const next = this.ppcList[index].next();
            if (this.ppcList[index].isStale() || next === null) {
                /* If the current PPC is stale or empty, then we move on to the next one.
                 * It is equally valid to choose any PPC available within this.indices, or else
                 * move on to the next PPC (maxIndicesIndex + 1) if available.
                 * We prefer the second option a bit more since we don't have to do a heap operation. */
                const toAdd = this.maxIndicesIndex + 1;
                if (toAdd < this.ppcList.length) {
                    this.maxIndicesIndex = toAdd;
                    index = toAdd;
                } else if (!this.indices.isEmpty()) {
                    index = this.indices.pop()
                } else {
                    break; // nothing left.
                }
                continue;
            }

            this.pendingIndices.add(index);
            next.index = index;
            return next;
        }
        return null; // Caller is responsible for triggering fetch logic here if next == null.
    }

    /**
     * Returns the next `size` elements in the cache as an array, or null if none exists.
     *
     * @sa next, the behaviour is similar in other aspects.
     */
    nextN(idx = -1, size = 1) {
        let index = idx;
        if (index !== -1 && !this.pendingIndices.has(index)) {
            /* The user is behaving well by returning the index to us, but in the meanwhile, it's possible
             * that we ran out of messages and fetched a new batch. So we just discard what the user is
             * returning to us. */
            this.logger.error("Returning unowned index", idx, "to cache. Discarding it.");
            index = -1;
        } else if (index !== -1) {
            this.pendingIndices.delete(index);
            /* We don't add the index back to the this.indices here because we're just going to remove it again the
             * first thing in the loop below, so it's slightly better to just avoid doing it. */
        }

        if (index === -1) {
            if (this.indices.size() === 0 || this.pendingIndices.size === this.maxConcurrency) {
                return null;
            }
            index = this.indices.pop(); // index cannot be undefined here since indices.size > 0
        }

        /* This loop will always terminate. Why?
         * On each iteration:
         * 1. We either return (if next is not null).
         * 2. We change the PPC index we're interested in, and there are a finite number of PPCs.
         *    (PPCs don't repeat within the loop since the indices of the PPC are popped from within the
         *     heap and not put back in, or else a new index is created bounded by ppcList.length).
        */
        while (true) {
            const next = this.ppcList[index].nextN(size);
            if (this.ppcList[index].isStale() || next === null) {
                /* If the current PPC is stale or empty, then we move on to the next one.
                 * It is equally valid to choose any PPC available within this.indices, or else
                 * move on to the next PPC (maxIndicesIndex + 1) if available.
                 * We prefer the second option a bit more since we don't have to do a heap operation. */
                const toAdd = this.maxIndicesIndex + 1;
                if (toAdd < this.ppcList.length) {
                    this.maxIndicesIndex = toAdd;
                    index = toAdd;
                } else if (!this.indices.isEmpty()) {
                    index = this.indices.pop()
                } else {
                    break; // nothing left.
                }
                continue;
            }

            this.pendingIndices.add(index);
            /* Arrays are just objects. Setting a property is odd, but not disallowed. */
            next.index = index;
            return next;
        }
        return null; // Caller is responsible for triggering fetch logic here if next == null.
    }

    /**
     * Clears the cache completely.
     * This resets it to a base state, and reduces the capacity of the cache back to 1.
     * Pre-conditions: none
     * Post-conditions: maxSize = 1, all caches are unstale, ppcList is empty, locallyStaleCaches is empty.
     */
    clear() {
        if (this.pendingSize() > 0) {
            this.logger.error('clear: pendingIndices = ', this.pendingIndices, logger.trace());
            throw new Error(`Cache cannot be cleared with ${this.pendingSize()} pending indices.`);
        }
        for (const cache of this.ppcList) {
            cache.clear();
        }
        this.ppcList = [];
        this.maxSize = 1;
        this.increaseCount = 0;
        this.stale = false;
        this.cachedTime = hrtime();
        this.locallyStaleCaches = [];
        this.indices.clear();
        this.currentIndex = 0;
    }
}

module.exports = MessageCache;
