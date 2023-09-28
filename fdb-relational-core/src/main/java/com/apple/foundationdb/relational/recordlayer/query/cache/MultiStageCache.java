/*
 * MultiStageCache.java
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

import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.metrics.RelationalMetric;
import com.apple.foundationdb.relational.util.Assert;
import com.apple.foundationdb.relational.util.ExcludeFromJacocoGeneratedReport;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalListener;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Ticker;
import org.apache.commons.lang3.tuple.Pair;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.AbstractMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * This is a simple generic cache of caches that employs LRU and TTL expiration policies. It uses the {@link Caffeine}
 * cache implementation internally for both primary and secondary caches.
 * <br>
 * Items stored in the primary cache expire after a duration (see {@link Builder#setTtl(long)}) of time, if an item is read
 * the duration is reset (read-TTL).
 * <br>
 * Similarly, items stored in the secondary cache expire after a duration (see {@link Builder#setSecondaryTtl(long)} of time).
 * The duration is set at the moment of loading the item in the cache (write-TTL).
 * <br>
 * If either primary or secondary caches reach its maximum capacity (see {@link Builder#setSize(int)}, resp. {@link Builder#setSecondarySize(int)})
 * an LRU policy is employed to evict the oldest item.
 * <pre>
 * {@code
 *  +---+-----------+
 *  |   | +---+---+ |
 *  | K | | S | V | |
 *  |   | +---+---+ |
 *  +---+---- ^ ----+
 *     ^      |
 *     |      |
 *     |      +----- Secondary Cache
 *     +------------ Main Cache
 * }
 * </pre>
 * <br>
 * Here are some ideas on how to improve this design to support different use cases:
 * <ul>
 *     <li>reduce memory footprint by reusing compiled plans when applicable; in certain cases it is possible that the
 *     <i>same</i> compiled plan is generated in different schema templates. To reduce memory
 *     footprint for these cases, we can store the plans in a separate pool (set), and make the secondary cache value
 *     ({@code V} refer to a plan in the pool via a reference.</li>
 *     <li>return multiple plans instead of one matching plan. It could be useful to consider returning multiple plan
 *     instead of a the first matching compiled plan. If we return more than a plan, we use the optimiser's cost model
 *     to return the cheapest one.</li>
 * </ul>
 *
 *
 * @param <K> The type of the primary cache key.
 * @param <S> The type of the secondary cache key.
 * @param <V> The value stored in the secondary cache.
 */
public class MultiStageCache<K, S, V> extends AbstractCache<K, S, V> {

    @Nonnull
    private final Cache<K, Cache<S, V>> mainCache;

    private final int secondarySize;

    private final long secondaryTtl;

    private final TimeUnit secondaryTtlTimeUnit;

    @Nullable
    private final Executor secondaryExecutor;

    @Nullable
    private final Ticker ticker;

    protected MultiStageCache(int size,
                              int secondarySize,
                              long ttl,
                              TimeUnit ttlTimeUnit,
                              long secondaryTtl,
                              TimeUnit secondaryttlTimeUnit,
                              @Nullable final Executor executor,
                              @Nullable final Executor secondaryExecutor,
                              @Nullable final Ticker ticker) {
        Assert.thatUnchecked(size > 0, String.format("Invalid cache size '%d'", size), ErrorCode.INTERNAL_ERROR);
        Assert.thatUnchecked(secondarySize > 0, String.format("Invalid secondary cache size '%d'", secondarySize), ErrorCode.INTERNAL_ERROR);
        Assert.thatUnchecked(ttl > 0, String.format("Invalid cache ttl '%d'", ttl), ErrorCode.INTERNAL_ERROR);
        Assert.thatUnchecked(secondaryTtl > 0, String.format("Invalid secondary cache ttl '%d'", secondaryTtl), ErrorCode.INTERNAL_ERROR);

        final var mainCacheBuilder = Caffeine.newBuilder().recordStats().maximumSize(size);
        mainCacheBuilder.expireAfterAccess(ttl, ttlTimeUnit);
        if (executor != null) {
            mainCacheBuilder.executor(executor);
        }
        if (ticker != null) {
            mainCacheBuilder.ticker(ticker::read);
        }
        mainCache = mainCacheBuilder.build();

        this.secondarySize = secondarySize;
        this.secondaryTtl = secondaryTtl;
        this.secondaryTtlTimeUnit = secondaryttlTimeUnit;
        this.secondaryExecutor = secondaryExecutor;
        this.ticker = ticker;
    }

    @SuppressWarnings("UnstableApiUsage")
    @Override
    @Nonnull
    public V reduce(@Nonnull final K key,
                    @Nonnull final S secondaryKey,
                    @Nonnull final Supplier<Pair<S, V>> secondaryKeyValueSupplier,
                    @Nonnull final Function<V, V> valueWithEnvironmentDecorator,
                    @Nonnull final Function<Stream<V>, V> reductionFunction,
                    @Nonnull final Consumer<RelationalMetric.RelationalCount> registerCacheEvent) {
        final var secondaryCache = mainCache.get(key, newKey -> {
            registerCacheEvent.accept(RelationalMetric.RelationalCount.PLAN_CACHE_PRIMARY_MISS);
            final var secondaryCacheBuilder = Caffeine.newBuilder()
                    .maximumSize(secondarySize)
                    .recordStats()
                    .removalListener((RemovalListener<S, V>) (k, v, i) -> {
                        final var value = mainCache.getIfPresent(key);
                        if (value != null && value.asMap().size() == 0) {
                            mainCache.invalidate(key); // best effort
                        }
                    });
            if (secondaryTtl > 0) {
                secondaryCacheBuilder.expireAfterWrite(secondaryTtl, secondaryTtlTimeUnit);
            }
            if (secondaryExecutor != null) {
                secondaryCacheBuilder.executor(secondaryExecutor);
            }
            if (ticker != null) {
                secondaryCacheBuilder.ticker(ticker::read);
            }
            return secondaryCacheBuilder.build();
        });

        final var result = reductionFunction.apply(secondaryCache.asMap().entrySet().stream().filter(kvPair -> kvPair.getKey().equals(secondaryKey)).map(Map.Entry::getValue));
        if (result != null) {
            registerCacheEvent.accept(RelationalMetric.RelationalCount.PLAN_CACHE_SECONDARY_HIT);
            return valueWithEnvironmentDecorator.apply(result);
        } else {
            registerCacheEvent.accept(RelationalMetric.RelationalCount.PLAN_CACHE_SECONDARY_MISS);
            final var keyValuePair = secondaryKeyValueSupplier.get();
            secondaryCache.put(keyValuePair.getKey(), keyValuePair.getValue());
            return keyValuePair.getValue();
        }
    }

    @VisibleForTesting
    public void cleanUp() {
        mainCache.asMap().values().forEach(Cache::cleanUp);
        mainCache.cleanUp();
    }

    @Override
    public CacheStatistics getStats() {

        return new CacheStatistics() {
            @Override
            public long numEntries() {
                return mainCache.estimatedSize();
            }

            @Override
            public long numEntriesSlow() {
                return mainCache.asMap().size();
            }

            @Nullable
            @Override
            public Long numSecondaryEntries(@Nonnull final K key) {
                final var secondary = mainCache.getIfPresent(key);
                if (secondary != null) {
                    return secondary.estimatedSize();
                }
                return null;
            }

            @Nullable
            @Override
            public Long numSecondaryEntriesSlow(@Nonnull final K key) {
                final var secondary = mainCache.getIfPresent(key);
                if (secondary != null) {
                    return (long) secondary.asMap().size();
                }
                return null;
            }

            @Nonnull
            @Override
            public Set<K> getAllKeys() {
                return mainCache.asMap().keySet();
            }

            @Nonnull
            @Override
            public Set<S> getAllSecondaryKeys(@Nonnull final K key) {
                final var secondary = mainCache.getIfPresent(key);
                if (secondary != null) {
                    return secondary.asMap().keySet();
                }
                return Set.of();
            }

            @Nonnull
            @Override
            public Map<K, Set<S>> getAllMappings() {
                return mainCache.asMap().entrySet().stream().map(e -> new AbstractMap.SimpleEntry<>(e.getKey(), e.getValue().asMap().keySet())).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
            }

            @Nonnull
            @Override
            public Map<S, V> getAllSecondaryMappings(@Nonnull final K key) {
                final var secondary = mainCache.getIfPresent(key);
                if (secondary != null) {
                    return secondary.asMap();
                }
                return Map.of();
            }

            @Override
            public long numHits() {
                return mainCache.stats().hitCount();
            }

            @Nonnull
            @Override
            public Long numSecondaryHits(@Nonnull final K key) {
                final var secondary = mainCache.getIfPresent(key);
                if (secondary != null) {
                    return secondary.stats().hitCount();
                }
                return 0L;
            }

            @Override
            public long numMisses() {
                return mainCache.stats().missCount();
            }

            @Nonnull
            @Override
            public Long numSecondaryMisses(@Nonnull final K key) {
                final var secondary = mainCache.getIfPresent(key);
                if (secondary != null) {
                    return secondary.stats().hitCount();
                }
                return 0L;
            }

            @Override
            public long numWrites() {
                return mainCache.stats().loadCount();
            }

            @Nonnull
            @Override
            public Long numSecondaryWrites(@Nonnull final K key) {
                final var secondary = mainCache.getIfPresent(key);
                if (secondary != null) {
                    return secondary.stats().loadCount();
                }
                return 0L;
            }

            @Override
            public long numReads() {
                return mainCache.stats().requestCount();
            }

            @Nonnull
            @Override
            public Long numSecondaryReads(@Nonnull final K key) {
                final var secondary = mainCache.getIfPresent(key);
                if (secondary != null) {
                    return secondary.stats().requestCount();
                }
                return 0L;
            }
        };
    }

    @ExcludeFromJacocoGeneratedReport
    public abstract static class Builder<K, S, V, B extends Builder<K, S, V, B>> {

        private static final int DEFAULT_SIZE = 256;

        private static final int DEFAULT_SECONDARY_SIZE = 8;

        private static final int DEFAULT_TTL_MS = 5000;

        private static final int DEFAULT_SECONDARY_TTL_MS = 2000;

        @Nonnull
        private static final TimeUnit DEFAULT_TTL_TIME_UNIT = TimeUnit.MILLISECONDS;

        @Nonnull
        private static final TimeUnit DEFAULT_SECONDARY_TTL_TIME_UNIT = TimeUnit.MILLISECONDS;

        protected int size;

        protected int secondarySize;

        protected long ttl;

        protected long secondaryTtl;

        @Nonnull
        protected TimeUnit ttlTimeUnit;

        @Nonnull
        protected TimeUnit secondaryTtlTimeUnit;

        @Nullable
        protected Executor executor;

        @Nullable
        protected Executor secondaryExecutor;

        @Nullable
        protected Ticker ticker;

        public Builder() {
            size = DEFAULT_SIZE;
            secondarySize = DEFAULT_SECONDARY_SIZE;
            ttl = DEFAULT_TTL_MS;
            secondaryTtl = DEFAULT_SECONDARY_TTL_MS;
            ttlTimeUnit = DEFAULT_TTL_TIME_UNIT;
            secondaryTtlTimeUnit = DEFAULT_SECONDARY_TTL_TIME_UNIT;
            executor = null;
            secondaryExecutor = null;
            ticker = null;
        }

        @Nonnull
        protected abstract B self();

        @Nonnull
        public B setSize(int size) {
            Assert.thatUnchecked(size > 0, String.format("Invalid cache size '%d'", size), ErrorCode.INTERNAL_ERROR);
            this.size = size;
            return self();
        }

        @Nonnull
        public B setSecondarySize(int secondarySize) {
            Assert.thatUnchecked(secondarySize > 0, String.format("Invalid secondary cache size '%d'", secondarySize), ErrorCode.INTERNAL_ERROR);
            this.secondarySize = secondarySize;
            return self();
        }

        @Nonnull
        public B setTtl(long ttlMillis) {
            return setTtl(ttlMillis, TimeUnit.MILLISECONDS);
        }

        @Nonnull
        public B setTtl(long ttl, @Nonnull final TimeUnit timeUnit) {
            Assert.thatUnchecked(ttl > 0, String.format("Invalid cache ttl '%d'", ttl), ErrorCode.INTERNAL_ERROR);
            this.ttl = ttl;
            this.ttlTimeUnit = timeUnit;
            return self();
        }

        @Nonnull
        public B setSecondaryTtl(long secondaryTtlMillis) {
            return setSecondaryTtl(secondaryTtlMillis, TimeUnit.MILLISECONDS);
        }

        @Nonnull
        public B setSecondaryTtl(long secondaryTtl, @Nonnull final TimeUnit timeUnit) {
            Assert.thatUnchecked(secondaryTtl > 0, String.format("Invalid cache secondaryTtl '%d'", secondaryTtl), ErrorCode.INTERNAL_ERROR);
            this.secondaryTtl = secondaryTtl;
            this.secondaryTtlTimeUnit = timeUnit;
            return self();
        }

        @Nonnull
        public B setTicker(@Nonnull final Ticker ticker) {
            this.ticker = ticker;
            return self();
        }

        @Nonnull
        public B setExecutor(@Nonnull final Executor executor) {
            this.executor = executor;
            return self();
        }

        @Nonnull
        public B setSecondaryExecutor(@Nonnull final Executor secondaryExecutor) {
            this.secondaryExecutor = secondaryExecutor;
            return self();
        }

        @Nonnull
        public MultiStageCache<K, S, V> build() {
            return new MultiStageCache<>(size, secondarySize, ttl, ttlTimeUnit, secondaryTtl, secondaryTtlTimeUnit, executor, secondaryExecutor, ticker);
        }
    }

    @ExcludeFromJacocoGeneratedReport
    public static class MultiStageCacheBuilder<K, S, V> extends Builder<K, S, V, MultiStageCacheBuilder<K, S, V>> {
        @Nonnull
        @Override
        protected MultiStageCacheBuilder<K, S, V> self() {
            return this;
        }
    }

    @Nonnull
    public static <K, S, V> MultiStageCacheBuilder<K, S, V> newMultiStageCacheBuilder() {
        return new MultiStageCacheBuilder<>();
    }
}
