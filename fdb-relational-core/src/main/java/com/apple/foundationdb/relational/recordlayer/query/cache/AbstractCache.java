/*
 * AbstractCache.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2025 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.record.util.pair.NonnullPair;
import com.apple.foundationdb.relational.api.metrics.MetricCollector;
import com.google.common.annotations.VisibleForTesting;
import org.jspecify.annotations.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

/**
 * This represents a generic plan cache interface intended for streamlining interactions with the cache.
 *
 * @param <K> The type of the primary cache key.
 * @param <S> The type of the secondary cache key.
 * @param <T> The type of the tertiary cache key.
 * @param <V> The value stored in the secondary cache.
 */
@ThreadSafe
public abstract class AbstractCache<K, S, T, V> {

    /**
     * Statistics about the cache. Mostly, a delegation to {@link com.google.common.cache.CacheStats}.
     */
    public abstract class CacheStatistics {

        public abstract long numEntries();

        @VisibleForTesting
        public abstract long numEntriesSlow();

        @Nullable
        public abstract Long numSecondaryEntries(K key);

        @Nullable
        public abstract Long numTertiaryEntries(K key, S secondaryKey);

        @VisibleForTesting
        @Nullable
        public abstract Long numSecondaryEntriesSlow(K key);

        @VisibleForTesting
        @Nullable
        public abstract Long numTertiaryEntriesSlow(K key, S secondaryKey);

        public abstract Set<K> getAllKeys();

        public abstract Set<S> getAllSecondaryKeys(K key);

        public abstract Set<T> getAllTertiaryKeys(K key, S secondaryKey);

        public abstract Map<K, Set<S>> getAllMappings();

        public abstract Map<S, Set<T>> getAllSecondaryMappings(K key);

        public abstract Map<T, V> getAllTertiaryMappings(K key, S secondaryKey);

        public abstract long numHits();

        public abstract Long numSecondaryHits(K key);

        public abstract Long numTertiaryHits(K key, S secondaryKey);

        public abstract long numMisses();

        public abstract Long numSecondaryMisses(K key);

        public abstract Long numTertiaryMisses(K key, S secondaryKey);

        public abstract long numWrites();

        public abstract Long numSecondaryWrites(K key);

        public abstract Long numTertiaryWrites(K key, S secondaryKey);

        public abstract long numReads();

        public abstract Long numSecondaryReads(K key);

        public abstract Long numTertiaryReads(K key, S secondaryKey);
    }

    /**
     * Gets an item from the cache determined by {@code key}, {@code secondaryKey} and {@code tertiaryKey}. If the item does not exist, it adds
     * it to the cache, and retrieves the newly constructed {@code V} value.
     *
     * @param key The key of the item.
     * @param secondaryKey The secondary key of the item.
     * @param tertiaryKey The tertiary key of the item.
     * @param tertiaryKeyValueSupplier supplier for a tertiary key and value pair in case the item is not found.
     * @param valueWithEnvironmentDecorator decorates the retrieved value with an environment preparing it for execution.
     * @param reductionFunction a function for choosing one matching value from a list of matches.
     * @param metricCollector metric collector to consume events from interacting with the cache.
     * @return The value referenced {@code key} and {@code secondaryKey}.
     */
    public abstract V reduce(K key,
                             S secondaryKey,
                             T tertiaryKey,
                             Supplier<NonnullPair<T, V>> tertiaryKeyValueSupplier,
                             Function<V, V> valueWithEnvironmentDecorator,
                             Function<Stream<V>, V> reductionFunction,
                             MetricCollector metricCollector);

    /**
     * Retrieves the statistics of the cache.
     * @return The statistics of the cache.
     */
    public abstract CacheStatistics getStats();
}
