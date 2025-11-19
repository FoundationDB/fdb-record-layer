/*
 * MultiStageCache.java
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

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.record.util.pair.NonnullPair;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.metrics.RelationalMetric;
import com.apple.foundationdb.relational.util.Assert;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.RemovalListener;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Ticker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
 * This is a simple generic cache of caches of caches that employs LRU and TTL expiration policies. It uses the {@link Caffeine}
 * cache implementation internally for primary, secondary and tertiary caches.
 * <br>
 * Items stored in the primary and secondary cache expire after a duration (see {@link Builder#setTtl(long)}) of time, if an item is read
 * the duration is reset (read-TTL).
 * <br>
 * Similarly, items stored in the tertiary cache expire after a duration (see {@link Builder#setSecondaryTtl(long)} of time).
 * The duration is set at the moment of loading the item in the cache (write-TTL).
 * <br>
 * If primary, secondary or tertiary caches reach their maximum capacity (see {@link Builder#setSize(int)}, resp. {@link Builder#setSecondarySize(int)}, {@link Builder#setTertiarySize(int)})
 * an LRU policy is employed to evict the oldest item.
 * <pre>
 * {@code
 *  +---+-------------------+
 *  |   | +---+---+---+---+ |
 *  |   | |   | +---+---+ | |
 *  | K | | S | | T | V | | |
 *  |   | |   | +---+---+ | |
 *  |   | +---+---+---+---+ |
 *  +---+---- ^ ----^-------+
 *     ^      |     |
 *     |      |     +--- Tertiary Cache
 *     |      +--------- Secondary Cache
 *     +---------------- Main Cache
 * }
 * </pre>
 * <br>
 * Here are some ideas on how to improve this design to support different use cases:
 * <ul>
 *     <li>reduce memory footprint by reusing compiled plans when applicable; in certain cases it is possible that the
 *     <i>same</i> compiled plan is generated in different schema templates. To reduce memory
 *     footprint for these cases, we can store the plans in a separate pool (set), and make the tertiary cache value
 *     ({@code V} refer to a plan in the pool via a reference.</li>
 *     <li>return multiple plans instead of one matching plan. It could be useful to consider returning multiple plan
 *     instead of a the first matching compiled plan. If we return more than a plan, we use the optimiser's cost model
 *     to return the cheapest one.</li>
 * </ul>
 *
 *
 * @param <K> The type of the primary cache key.
 * @param <S> The type of the secondary cache key.
 * @param <T> The type of the tertiary cache key.
 * @param <V> The value stored in the secondary cache.
 */
@API(API.Status.EXPERIMENTAL)
public class MultiStageCache<K, S, T, V> extends AbstractCache<K, S, T, V> {

    @Nonnull
    private static final Logger logger = LoggerFactory.getLogger(MultiStageCache.class);

    @Nonnull
    private final Cache<K, Cache<S, Cache<T, V>>> mainCache;

    private final int secondarySize;
    private final int tertiarySize;

    private final long secondaryTtl;
    private final long tertiaryTtl;

    private final TimeUnit secondaryTtlTimeUnit;
    private final TimeUnit tertiaryTtlTimeUnit;

    @Nullable
    private final Executor secondaryExecutor;
    private final Executor tertiaryExecutor;

    @Nullable
    private final Ticker ticker;

    protected MultiStageCache(int size,
                              int secondarySize,
                              int tertiarySize,
                              long ttl,
                              TimeUnit ttlTimeUnit,
                              long secondaryTtl,
                              TimeUnit secondaryTtlTimeUnit,
                              long tertiaryTtl,
                              TimeUnit tertiaryTtlTimeUnit,
                              @Nullable final Executor executor,
                              @Nullable final Executor secondaryExecutor,
                              @Nullable final Executor tertiaryExecutor,
                              @Nullable final Ticker ticker) {
        Assert.thatUnchecked(size > 0, ErrorCode.INTERNAL_ERROR, "Invalid cache size '%d'", size);
        Assert.thatUnchecked(secondarySize > 0, ErrorCode.INTERNAL_ERROR, "Invalid secondary cache size '%d'", secondarySize);
        Assert.thatUnchecked(tertiarySize > 0, ErrorCode.INTERNAL_ERROR, "Invalid tertiary cache size '%d'", tertiarySize);
        Assert.thatUnchecked(ttl > 0, ErrorCode.INTERNAL_ERROR, "Invalid cache ttl '%d'", ttl);
        Assert.thatUnchecked(secondaryTtl > 0, ErrorCode.INTERNAL_ERROR, "Invalid secondary cache ttl '%d'", secondaryTtl);
        Assert.thatUnchecked(tertiaryTtl > 0, ErrorCode.INTERNAL_ERROR, "Invalid tertiary cache ttl '%d'", tertiaryTtl);

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
        this.secondaryTtlTimeUnit = secondaryTtlTimeUnit;
        this.secondaryExecutor = secondaryExecutor;
        this.tertiarySize = tertiarySize;
        this.tertiaryTtl = tertiaryTtl;
        this.tertiaryTtlTimeUnit = tertiaryTtlTimeUnit;
        this.tertiaryExecutor = tertiaryExecutor;
        this.ticker = ticker;
    }

    @SuppressWarnings("UnstableApiUsage")
    @Nonnull
    @Override
    public V reduce(@Nonnull final K key,
                    @Nonnull final S secondaryKey,
                    @Nonnull final T tertiaryKey,
                    @Nonnull final Supplier<NonnullPair<T, V>> tertiaryKeyValueSupplier,
                    @Nonnull final Function<V, V> valueWithEnvironmentDecorator,
                    @Nonnull final Function<Stream<V>, V> reductionFunction,
                    @Nonnull final Consumer<RelationalMetric.RelationalCount> registerCacheEvent) {
        final var secondaryCache = mainCache.get(key, newKey -> {
            registerCacheEvent.accept(RelationalMetric.RelationalCount.PLAN_CACHE_PRIMARY_MISS);
            final var secondaryCacheBuilder = Caffeine.newBuilder()
                    .maximumSize(secondarySize)
                    .recordStats()
                    .removalListener((RemovalListener<S, Cache<T, V>>) (k, v, cause) -> {
                        if (logger.isDebugEnabled()) {
                            logger.debug(KeyValueLogMessage.of("Plan cache secondary entry evicted",
                                    "primaryKey", key,
                                    "secondaryKey", k,
                                    "cause", cause.toString(),
                                    "remainingEntries", v != null ? v.estimatedSize() : 0));
                        }
                        final var value = mainCache.getIfPresent(key);
                        if (value != null && value.asMap().isEmpty()) {
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

        final var tertiaryCache = secondaryCache.get(secondaryKey, newKey -> {
            registerCacheEvent.accept(RelationalMetric.RelationalCount.PLAN_CACHE_SECONDARY_MISS);
            final var tertiaryCacheBuilder = Caffeine.newBuilder()
                    .maximumSize(tertiarySize)
                    .recordStats()
                    .removalListener((RemovalListener<T, V>) (k, v, cause) -> {
                        if (logger.isDebugEnabled()) {
                            logger.debug(KeyValueLogMessage.of("Plan cache tertiary entry evicted",
                                    "primaryKey", key,
                                    "secondaryKey", secondaryKey,
                                    "tertiaryKey", k,
                                    "cause", cause.toString(),
                                    "valueType", v != null ? v.getClass().getSimpleName() : "null"));
                        }
                        final var value = secondaryCache.getIfPresent(secondaryKey);
                        if (value != null && value.asMap().isEmpty()) {
                            secondaryCache.invalidate(secondaryKey); // best effort
                        }
                    });
            if (tertiaryTtl > 0) {
                tertiaryCacheBuilder.expireAfterWrite(tertiaryTtl, tertiaryTtlTimeUnit);
            }
            if (tertiaryExecutor != null) {
                tertiaryCacheBuilder.executor(tertiaryExecutor);
            }
            if (ticker != null) {
                tertiaryCacheBuilder.ticker(ticker::read);
            }
            return tertiaryCacheBuilder.build();
        });

        final var result = reductionFunction.apply(tertiaryCache.asMap().entrySet().stream().filter(kvPair -> kvPair.getKey().equals(tertiaryKey)).map(Map.Entry::getValue));
        if (result != null) {
            registerCacheEvent.accept(RelationalMetric.RelationalCount.PLAN_CACHE_TERTIARY_HIT);
            return valueWithEnvironmentDecorator.apply(result);
        } else {
            registerCacheEvent.accept(RelationalMetric.RelationalCount.PLAN_CACHE_TERTIARY_MISS);
            final var keyValuePair = tertiaryKeyValueSupplier.get();
            tertiaryCache.put(keyValuePair.getKey(), keyValuePair.getValue());
            return keyValuePair.getValue();
        }
    }

    @VisibleForTesting
    public void cleanUp() {
        mainCache.asMap().values().forEach(secondaryCache -> secondaryCache.asMap().values().forEach(Cache::cleanUp));
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
            public Long numTertiaryEntries(@Nonnull K key, @Nonnull S secondaryKey) {
                final var secondary = mainCache.getIfPresent(key);
                if (secondary != null) {
                    final var tertiary = secondary.getIfPresent(secondaryKey);
                    if (tertiary != null) {
                        return tertiary.estimatedSize();
                    }
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

            @Nullable
            @Override
            public Long numTertiaryEntriesSlow(@Nonnull K key, @Nonnull S secondaryKey) {
                final var secondary = mainCache.getIfPresent(key);
                if (secondary != null) {
                    final var tertiary = secondary.getIfPresent(secondaryKey);
                    if (tertiary != null) {
                        return (long) tertiary.asMap().size();
                    }
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
            public Set<T> getAllTertiaryKeys(@Nonnull K key, @Nonnull S secondaryKey) {
                final var secondary = mainCache.getIfPresent(key);
                if (secondary != null) {
                    final var tertiary = secondary.getIfPresent(secondaryKey);
                    if (tertiary != null) {
                        return tertiary.asMap().keySet();
                    }
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
            public Map<S, Set<T>> getAllSecondaryMappings(@Nonnull final K key) {
                final var secondary = mainCache.getIfPresent(key);
                if (secondary != null) {
                    return secondary.asMap().entrySet().stream().map(e -> new AbstractMap.SimpleEntry<>(e.getKey(), e.getValue().asMap().keySet())).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
                }
                return Map.of();
            }

            @Nonnull
            @Override
            public Map<T, V> getAllTertiaryMappings(@Nonnull K key, @Nonnull S secondaryKey) {
                final var secondary = mainCache.getIfPresent(key);
                if (secondary != null) {
                    final var tertiary = secondary.getIfPresent(secondaryKey);
                    if (tertiary != null) {
                        return tertiary.asMap();
                    }
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

            @Nonnull
            @Override
            public Long numTertiaryHits(@Nonnull K key, @Nonnull S secondaryKey) {
                final var secondary = mainCache.getIfPresent(key);
                if (secondary != null) {
                    final var tertiary = secondary.getIfPresent(secondaryKey);
                    if (tertiary != null) {
                        return tertiary.stats().hitCount();
                    }
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
                    return secondary.stats().missCount();
                }
                return 0L;
            }

            @Nonnull
            @Override
            public Long numTertiaryMisses(@Nonnull K key, @Nonnull S secondaryKey) {
                final var secondary = mainCache.getIfPresent(key);
                if (secondary != null) {
                    final var tertiary = secondary.getIfPresent(secondaryKey);
                    if (tertiary != null) {
                        return tertiary.stats().missCount();
                    }
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

            @Nonnull
            @Override
            public Long numTertiaryWrites(@Nonnull K key, @Nonnull S secondaryKey) {
                final var secondary = mainCache.getIfPresent(key);
                if (secondary != null) {
                    final var tertiary = secondary.getIfPresent(secondaryKey);
                    if (tertiary != null) {
                        return tertiary.stats().loadCount();
                    }
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

            @Nonnull
            @Override
            public Long numTertiaryReads(@Nonnull K key, @Nonnull S secondaryKey) {
                final var secondary = mainCache.getIfPresent(key);
                if (secondary != null) {
                    final var tertiary = secondary.getIfPresent(secondaryKey);
                    if (tertiary != null) {
                        return tertiary.stats().requestCount();
                    }
                }
                return 0L;
            }
        };
    }

    public abstract static class Builder<K, S, T, V, B extends Builder<K, S, T, V, B>> {

        private static final int DEFAULT_SIZE = 256;

        private static final int DEFAULT_SECONDARY_SIZE = 256;

        private static final int DEFAULT_TERTIARY_SIZE = 8;

        private static final int DEFAULT_TTL_MS = 5000;

        private static final int DEFAULT_SECONDARY_TTL_MS = 5000;

        private static final int DEFAULT_TERTIARY_TTL_MS = 2000;

        @Nonnull
        private static final TimeUnit DEFAULT_TTL_TIME_UNIT = TimeUnit.MILLISECONDS;

        @Nonnull
        private static final TimeUnit DEFAULT_SECONDARY_TTL_TIME_UNIT = TimeUnit.MILLISECONDS;

        @Nonnull
        private static final TimeUnit DEFAULT_TERTIARY_TTL_TIME_UNIT = TimeUnit.MILLISECONDS;

        protected int size;

        protected int secondarySize;

        protected int tertiarySize;

        protected long ttl;

        protected long secondaryTtl;

        protected long tertiaryTtl;

        @Nonnull
        protected TimeUnit ttlTimeUnit;

        @Nonnull
        protected TimeUnit secondaryTtlTimeUnit;

        @Nonnull
        protected TimeUnit tertiaryTtlTimeUnit;

        @Nullable
        protected Executor executor;

        @Nullable
        protected Executor secondaryExecutor;

        @Nullable
        protected Executor tertiaryExecutor;

        @Nullable
        protected Ticker ticker;

        public Builder() {
            size = DEFAULT_SIZE;
            secondarySize = DEFAULT_SECONDARY_SIZE;
            tertiarySize = DEFAULT_TERTIARY_SIZE;
            ttl = DEFAULT_TTL_MS;
            secondaryTtl = DEFAULT_SECONDARY_TTL_MS;
            tertiaryTtl = DEFAULT_TERTIARY_TTL_MS;
            ttlTimeUnit = DEFAULT_TTL_TIME_UNIT;
            secondaryTtlTimeUnit = DEFAULT_SECONDARY_TTL_TIME_UNIT;
            tertiaryTtlTimeUnit = DEFAULT_TERTIARY_TTL_TIME_UNIT;
            executor = null;
            secondaryExecutor = null;
            tertiaryExecutor = null;
            ticker = null;
        }

        @Nonnull
        protected abstract B self();

        @Nonnull
        public B setSize(int size) {
            Assert.thatUnchecked(size > 0, ErrorCode.INTERNAL_ERROR, "Invalid cache size '%d'", size);
            this.size = size;
            return self();
        }

        @Nonnull
        public B setSecondarySize(int secondarySize) {
            Assert.thatUnchecked(secondarySize > 0, ErrorCode.INTERNAL_ERROR, "Invalid secondary cache size '%d'", secondarySize);
            this.secondarySize = secondarySize;
            return self();
        }

        @Nonnull
        public B setTertiarySize(int tertiarySize) {
            Assert.thatUnchecked(tertiarySize > 0, ErrorCode.INTERNAL_ERROR, "Invalid tertiary cache size '%d'", tertiarySize);
            this.tertiarySize = tertiarySize;
            return self();
        }

        @Nonnull
        public B setTtl(long ttlMillis) {
            return setTtl(ttlMillis, TimeUnit.MILLISECONDS);
        }

        @Nonnull
        public B setTtl(long ttl, @Nonnull final TimeUnit timeUnit) {
            Assert.thatUnchecked(ttl > 0, ErrorCode.INTERNAL_ERROR, "Invalid cache ttl '%d'", ttl);
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
            Assert.thatUnchecked(secondaryTtl > 0, ErrorCode.INTERNAL_ERROR, "Invalid cache secondaryTtl '%d'", secondaryTtl);
            this.secondaryTtl = secondaryTtl;
            this.secondaryTtlTimeUnit = timeUnit;
            return self();
        }

        @Nonnull
        public B setTertiaryTtl(long tertiaryTtlMillis) {
            return setTertiaryTtl(tertiaryTtlMillis, TimeUnit.MILLISECONDS);
        }

        @Nonnull
        public B setTertiaryTtl(long tertiaryTtl, @Nonnull final TimeUnit timeUnit) {
            Assert.thatUnchecked(tertiaryTtl > 0, ErrorCode.INTERNAL_ERROR, "Invalid cache tertiaryTtl '%d'", tertiaryTtl);
            this.tertiaryTtl = tertiaryTtl;
            this.tertiaryTtlTimeUnit = timeUnit;
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
        public B setTertiaryExecutor(@Nonnull final Executor tertiaryExecutor) {
            this.tertiaryExecutor = tertiaryExecutor;
            return self();
        }

        @Nonnull
        public MultiStageCache<K, S, T, V> build() {
            return new MultiStageCache<>(size, secondarySize, tertiarySize, ttl, ttlTimeUnit, secondaryTtl, secondaryTtlTimeUnit, tertiaryTtl, tertiaryTtlTimeUnit, executor, secondaryExecutor, tertiaryExecutor, ticker);
        }
    }

    public static class MultiStageCacheBuilder<K, S, T, V> extends Builder<K, S, T, V, MultiStageCacheBuilder<K, S, T, V>> {
        @Nonnull
        @Override
        protected MultiStageCacheBuilder<K, S, T, V> self() {
            return this;
        }
    }

    @Nonnull
    public static <K, S, T, V> MultiStageCacheBuilder<K, S, T, V> newMultiStageCacheBuilder() {
        return new MultiStageCacheBuilder<>();
    }
}
