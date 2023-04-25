/*
 * AbstractCache.java
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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Supplier;
import org.apache.commons.lang3.tuple.Pair;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import java.util.Map;
import java.util.Set;

/**
 * This represents a generic plan cache interface intended for streamlining interactions with the cache.
 *
 * @param <K> The type of the primary cache key.
 * @param <S> The type of the secondary cache key.
 * @param <V> The value stored in the secondary cache.
 */
@ThreadSafe
public abstract class AbstractCache<K, S, V> {

    /**
     * Statistics about the cache. Mostly, a delegation to {@link com.google.common.cache.CacheStats}.
     */
    public abstract class CacheStatistics {

        public abstract long numEntries();

        @VisibleForTesting
        public abstract long numEntriesSlow();

        @Nullable
        public abstract Long numSecondaryEntries(@Nonnull final K key);

        @VisibleForTesting
        @Nullable
        public abstract Long numSecondaryEntriesSlow(@Nonnull final K key);

        @Nonnull
        public abstract Set<K> getAllKeys();

        @Nonnull
        public abstract Set<S> getAllSecondaryKeys(@Nonnull final K key);

        @Nonnull
        public abstract Map<K, Set<S>> getAllMappings();

        @Nonnull
        public abstract Map<S, V> getAllSecondaryMappings(@Nonnull final K key);

        public abstract long numHits();

        @Nonnull
        public abstract Long numSecondaryHits(@Nonnull final K key);

        public abstract long numMisses();

        @Nonnull
        public abstract Long numSecondaryMisses(@Nonnull final K key);

        public abstract long numWrites();

        @Nonnull
        public abstract Long numSecondaryWrites(@Nonnull final K key);

        public abstract long numReads();

        @Nonnull
        public abstract Long numSecondaryReads(@Nonnull final K key);
    }

    /**
     * Gets an item from the cache determined by {@code key} and {@code secondaryKey}. If the item does not exist, it adds
     * it to the cache, and retrieves the newly construced {@code V} value.
     *
     * @param key The key of the item.
     * @param secondaryKey The secondary key of the item.
     * @param secondaryKeyValueSupplier supplier for a secondary key and value pair in case the item is not found.
     * @param valueWithEnvironmentDecorator decorates the retrieved value with an environment preparing it for execution.
     * @return The value referenced {@code key} and {@code secondaryKey}.
     */
    @Nonnull
    public abstract V get(@Nonnull final K key,
                   @Nonnull final S secondaryKey,
                   @Nonnull final Supplier<Pair<S, V>> secondaryKeyValueSupplier,
                   @Nonnull final Function<V, V> valueWithEnvironmentDecorator);

    /**
     * Retrieves the statistics of the cache.
     * @return The statistics of the cache.
     */
    public abstract CacheStatistics getStats();
}
