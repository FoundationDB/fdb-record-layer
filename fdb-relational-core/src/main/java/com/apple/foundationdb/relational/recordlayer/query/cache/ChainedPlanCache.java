/*
 * ChainedPlanCache.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2024 Apple Inc. and the FoundationDB project authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.apple.foundationdb.relational.recordlayer.query.cache;

import com.apple.foundationdb.record.RecordStoreState;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.relational.recordlayer.query.LogicalQuery;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * A semi-absurd implementation of a PlanCache using a scan-based approach to resolving cached entries.
 * <p>
 * The basic problem is this: We need to cache plans that correspond to more than just the logical structure
 * of the query itself. Plans are only applicable based on specific state information (such as which indexes
 * are present, serialization formats, etc.). Therefore, we will end up hashing multiple plans for each
 * logical query.
 * <p>
 * This presents a basic problem: How do we lay out entries in the cache such that
 * <p>
 * 1. We can reasonably find all the different permutations of a specific plan structure (e.g. logical query + state) AND
 * 2. When evicting entries, we evict our least-used plans first (to minimize cache invalidation costs).
 * <p>
 * Traditionally, caches are implemented using hashtables, which have the benefit of constant-time access. Within that,
 * you typically would use either link-chaining (where you keep multiple copies of things that hash to the same entry
 * in a set, and iterate over that set to find matches to your query), or open-addressing (where you adjust the value
 * of the hash itself to avoid conflicts).
 * <p>
 * The problem with Open-addressing when using caches is that cache eviction causes holds in the addressing-chain, so
 * you end up either truncating the addressing chain early (if you return on first empty position), or you have to
 * iterate the entire cache set in order to guarantee that you've found the whole set (eliminating the performance
 * benefits of hashing entirely). So we don't want to use that.
 * <p>
 * Entry-chaining is more profitable, because we can then keep multiple copies for the same logical query together,
 * hash to them directly, and then within that bucket iterate over all the possible plans for just that bucket
 * to find the necessary one. When evicting, though, there are annoying details. You don't want to evict the entire
 * bucket, since that could result in commonly-used plans being thrown out with unused ones; this means that you can't
 * use just the logical query structure for the hashing. But hashing the constraints themselves will lead to
 * you missing plans because your state is a superset of that used by hashing. So you have to do complicated
 * eviction schemes that evict within the bucket, but when you do that you have lots of concurrency problems that have
 * to be dealt with (and related things, like what to do with empty buckets, and so on). Plus, you have potential
 * space wastage of having complex data structures designed to handle this kind of chaining directly.
 * <p>
 * In the end, we almost certainly will need to implement just such a complicated datastructure, which is
 * performance-sensitive and accurate, but doing so right _now_ would be a lot of risk, because we aren't even
 * confident of the actual API structure or usage of the cache yet; we don't know the access pattern, or how much
 * space it occupies, how to distribute it, or anything else.
 * <p>
 * So this implementation is a much simpler approach to the problem. It mimics the chained approach by using
 * a sorted-scan based approach. Rather than hashing on entries and dealing with the complexities of that,
 * we simply use a sorted map to store a compound key. The first entry of the key is the logical plan,
 * ensuring that plans for the different state permutations are grouped together within the map, and the second
 * entry is a structural representation of the required state (e.g. a list of required indexes). Thus, attempting
 * to access the entry will have you first block to the logical query, and then you scan through the sorted
 * map to obtain an entry which is valid for your state. We then use a simple LRU-queue to ensure that entries
 * are evicted individually from the map. This is a much simpler cache implementation than the full-bore approach,
 * and it can be used to demonstrate the design and to get us to a place where we can replace it with a more
 * sophisticated design.
 */
public class ChainedPlanCache implements PlanCache {
    private final ConcurrentSkipListMap<CacheKey, CacheValue> cacheMap = new ConcurrentSkipListMap<>();
    private final ConcurrentLinkedQueue<CacheKey> lruQueue = new ConcurrentLinkedQueue<>();

    private final int maxEntries;

    private final Stats stats;

    public ChainedPlanCache(int maxEntries) {
        this.maxEntries = maxEntries;
        this.stats = new Stats();
    }

    @Override
    @Nullable
    public RecordQueryPlan getPlan(@Nonnull LogicalQuery query, @Nonnull SchemaState schemaState) {
        stats.reads.incrementAndGet();

        // Construct a list containing the maximum scannable index in the store. Note that CacheKeys are
        // sorted first by their query and then by their sorted list of used indexes. So, to find all
        // possible plans, we need to look through the map for entries that (1) have the logical query and
        // (2) do not require any non-readable indexes. By constructing a list of the max (lexicographically)
        // readable index, we only exclude plans for this query that contain a non-readable index (as any CacheKey
        // for this query that is greater must have at least one index that sorts higher than the max readable
        // index)
        final RecordStoreState recordStoreState = schemaState.getState();
        final List<String> maxReadableIndexes = schemaState.getSchemaMetaData().getAllIndexes().stream()
                .filter(index -> recordStoreState.getState(index).isScannable())
                .map(Index::getName)
                .max(String::compareTo)
                .map(List::of)
                .orElse(Collections.emptyList());

        CacheKey lowKey = new CacheKey(query, List.of());
        CacheKey highKey = new CacheKey(query, maxReadableIndexes);

        final ConcurrentNavigableMap<CacheKey, CacheValue> possibleEntries = cacheMap.subMap(lowKey, true, highKey, true);
        for (Map.Entry<CacheKey, CacheValue> possibleMatch : possibleEntries.entrySet()) {
            CacheValue v = possibleMatch.getValue();
            if (v.satisfies(query, schemaState)) {
                stats.cacheHits.incrementAndGet();
                //we have a cached entry! Move it to the back of the LRU queue and return
                lruQueue.remove(possibleMatch.getKey());
                lruQueue.offer(possibleMatch.getKey());

                return v.queryPlan;
            }
        }

        stats.cacheMisses.incrementAndGet();
        return null;
    }

    @Override
    public void cacheEntry(@Nonnull LogicalQuery query, @Nonnull RecordQueryPlan plan) {
        stats.writes.incrementAndGet();
        List<CacheValidityCondition> cacheConditions = List.of(
                requireSemanticEquivalence(), //this one may not be needed, but it's demonstration
                requireIndexes()
        );

        //used as the secondary field in the entry
        List<String> indexFields = new ArrayList<>(plan.getUsedIndexes());
        Collections.sort(indexFields); //make sure that we have a uniform ordering
        CacheKey key = new CacheKey(query, indexFields);
        cacheMap.put(key, new CacheValue(query, plan, cacheConditions));
        lruQueue.offer(key);
        if (cacheMap.size() > maxEntries) {
            evict();
        }
    }

    @Override
    public CacheStatistics getStats() {
        return stats;
    }

    /* **********************************************************************************************************/
    /*private helper functions and classes*/
    private void evict() {
        CacheKey toEvict = lruQueue.poll();
        if (toEvict == null) {
            return; //nothing to do
        }
        cacheMap.remove(toEvict);
    }

    private CacheValidityCondition requireIndexes() {
        /*
         * This adds a condition that all the indexes that are required by the underlying
         * query exist and are scannable by the current index.
         */
        return (reqRelExp, planState, schemaState) -> {
            Set<String> requiredIndexes = planState.getPlan().getUsedIndexes();
            for (String reqIdx : requiredIndexes) {
                if (schemaState.getSchemaMetaData().hasIndex(reqIdx)) {
                    if (!schemaState.getState().getState(reqIdx).isScannable()) {
                        return false;
                    }
                } else {
                    return false;
                }
            }
            return true;
        };
    }

    private CacheValidityCondition requireSemanticEquivalence() {
        /*
         * Add a condition that the Relational Expression be semantically equivalent to the
         * RelationalExpression that is held in the entry
         */
        return (requestedRelExp, cachedState, schemaState) -> requestedRelExp.equals(cachedState.getLogicalQuery());
    }

    private class Stats extends AtomicCacheStatistics {

        @Override
        public long numEntries() {
            return cacheMap.size();
        }
    }

    /*
     * Simple holder for non-key objects
     */
    private static class CacheValue implements PlanState {
        private final LogicalQuery logicalQuery;
        private final RecordQueryPlan queryPlan;
        private final List<CacheValidityCondition> conditionFuncs;

        public CacheValue(LogicalQuery logicalQuery, RecordQueryPlan queryPlan, List<CacheValidityCondition> conditionFuncs) {
            this.logicalQuery = logicalQuery;
            this.queryPlan = queryPlan;
            this.conditionFuncs = conditionFuncs;
        }

        public boolean satisfies(LogicalQuery query, SchemaState schemaState) {
            for (CacheValidityCondition cvc : conditionFuncs) {
                if (!cvc.test(query, this, schemaState)) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public RecordQueryPlan getPlan() {
            return queryPlan;
        }

        @Override
        public LogicalQuery getLogicalQuery() {
            return logicalQuery;
        }
    }

    /*
     * An immutable key structure holding the logical keys in the map.
     */
    private static class CacheKey implements Comparable<CacheKey> {
        private final LogicalQuery query;

        //a string representation of the state conditions for this cache entry
        private final Iterable<String> stateConditions;

        private final long logicalQueryHash;

        public CacheKey(@Nonnull LogicalQuery query,
                        @Nonnull Iterable<String> stateConditions) {
            this.query = query;
            //stateConditions needs to be immutable
            this.stateConditions = stateConditions;
            this.logicalQueryHash = query.getQueryHash();
        }

        @Override
        public int compareTo(CacheKey o) {
            if (o == null) {
                return -1; //sort nulls first. It shouldn't happen, but safety first
            }
            int hashCmp = Long.compare(logicalQueryHash, o.logicalQueryHash);
            if (hashCmp != 0) {
                return hashCmp;
            }
            //the logical queries hash, so double check for textual equality
            if (!query.equals(o.query)) {
                //the queries aren't the same, so return their string ordering
                return query.getQuery().compareTo(o.query.getQuery());
            }

            //we are looking at the same logical query, so sort based on the stringified setup
            //of the state conditions
            Iterator<String> myConds = stateConditions.iterator();
            Iterator<String> oConds = o.stateConditions.iterator();
            while (myConds.hasNext() && oConds.hasNext()) {
                hashCmp = myConds.next().compareTo(oConds.next());
                if (hashCmp != 0) {
                    return hashCmp;
                }
            }
            //we have identical state conditions up to the minimum length, so return whichever is shorter first
            if (myConds.hasNext()) {
                return 1; //we are longer than they are, so we go last
            } else if (oConds.hasNext()) {
                return -1; //we are shorter
            }

            //we are identical!
            return 0;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof CacheKey)) {
                return false;
            }
            CacheKey cacheKey = (CacheKey) o;
            return compareTo(cacheKey) == 0;
        }

        @Override
        public int hashCode() {
            int result = Long.hashCode(logicalQueryHash);

            for (String cond : stateConditions) {
                result = result * 32 + cond.hashCode();
            }
            return result;
        }
    }
}
