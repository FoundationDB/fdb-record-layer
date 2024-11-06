/*
 * AsyncLoadingCache.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2018 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.async.MoreAsyncUtil;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import javax.annotation.Nonnull;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * A cache for retaining the result of some asynchronous operation up until some expiration time.
 * @param <K> key type for the cache
 * @param <V> value type for the cache
 */
@API(API.Status.UNSTABLE)
public class AsyncLoadingCache<K, V> {
    /**
     * The default deadline to impose on loading elements.
     */
    public static final long DEFAULT_DEADLINE_TIME_MILLIS = 5000L;
    /**
     * A constant to indicate that an operation should not have a limit. For example, this can be provided
     * to the cache as the deadline time or the max size to indicate that no deadline should be imposed
     * on futures loading entries into the cache or that there should not be a maximum number of cached
     * elements.
     */
    public static final long UNLIMITED = Long.MAX_VALUE;

    @Nonnull
    private final Cache<K, Optional<V>> cache;
    private final long refreshTimeMillis;
    private final long deadlineTimeMillis;
    private final long maxSize;
    @Nonnull
    private final ScheduledExecutorService scheduledExecutor;

    /**
     * Create a new cache that loads items asynchronously.
     *
     * @param refreshTimeMillis the amount of time to let cache entries sit in the cache before reloading them
     * @param deadlineTimeMillis the maximum amount of time in milliseconds that loading a cache element can take
     * @param maxSize the maximum number of elements in the cache
     * @param scheduledExecutor a scheduled executor used to manage asynchronous deadlines
     */
    public AsyncLoadingCache(long refreshTimeMillis, long deadlineTimeMillis, long maxSize, @Nonnull ScheduledExecutorService scheduledExecutor) {
        this.refreshTimeMillis = refreshTimeMillis;
        this.deadlineTimeMillis = deadlineTimeMillis;
        this.maxSize = maxSize;
        this.scheduledExecutor = scheduledExecutor;
        CacheBuilder<Object, Object> cacheBuilder = CacheBuilder.newBuilder()
                .expireAfterWrite(this.refreshTimeMillis, TimeUnit.MILLISECONDS);
        if (maxSize != UNLIMITED) {
            cacheBuilder.maximumSize(maxSize);
        }
        cache = cacheBuilder.build();
    }

    /**
     * If the cache does not contain an entry for <code>key</code>, retrieve the value using the provided asynchronous
     * {@link Supplier}. If the value is not currently cached, then the {@code Supplier} is used to load the value
     * asynchronously. If multiple callers ask for the same key at the same time, then they might duplicate
     * each other's work in that both callers will result in a future being created. Whichever future completes first
     * will insert its value into the cache, and all callers that then complete successfully are guaranteed to see that
     * object until such time as the value is expired from the cache.
     *
     * @param key the key in the cache to lookup
     * @param supplier an asynchronous operation to retrieve the desired value if the cache is empty
     *
     * @return a future containing either the cached value or the result from the supplier
     */
    @SuppressWarnings("squid:S2789") // comparison of null and optional used to differentiate absence of key and presence of null
    @Nonnull
    public CompletableFuture<V> orElseGet(@Nonnull K key, @Nonnull Supplier<CompletableFuture<V>> supplier) {
        try {
            Optional<V> cachedValue = cache.getIfPresent(key);
            if (cachedValue == null) {
                return MoreAsyncUtil.getWithDeadline(deadlineTimeMillis, supplier, scheduledExecutor).thenApply(value -> {
                    // Only insert the computed value into the cache if a concurrent caller hasn't.
                    // Return the value that wound up in the cache.
                    final Optional<V> existingValue = cache.asMap().putIfAbsent(key, Optional.ofNullable(value));
                    return existingValue == null ? value : existingValue.orElse(null);
                });
            } else {
                return CompletableFuture.completedFuture(cachedValue.orElse(null));
            }
        } catch (Exception e) {
            throw new RecordCoreException("failed getting value", e).addLogInfo("cacheKey", key);
        }
    }

    /**
     * Get the amount of time between cache refreshes in seconds.
     *
     * @return the cache refresh time in seconds
     */
    public long getRefreshTimeSeconds() {
        return TimeUnit.MILLISECONDS.toSeconds(refreshTimeMillis);
    }

    /**
     * Get the amount of time between cache refreshes in milliseconds.
     *
     * @return the cache refresh time in milliseconds
     */
    public long getRefreshTimeMillis() {
        return refreshTimeMillis;
    }

    /**
     * Get the deadline time imposed on cache loading in milliseconds. This is used by
     * {@link #orElseGet(Object, Supplier)} to avoid futures hanging during cache loading.
     * If a future takes longer than the returned number of milliseconds to complete, the cache
     * loading operation is failed with a {@link com.apple.foundationdb.async.MoreAsyncUtil.DeadlineExceededException}.
     *
     * @return the cache loading deadline time in milliseconds
     */
    public long getDeadlineTimeMillis() {
        return deadlineTimeMillis;
    }

    /**
     * Get the maximum number of elements stored by the cache. This will return {@link Long#MAX_VALUE} if there
     * is no maximum size enforced by this cache.
     *
     * @return the maximum number of elements stored by the cache
     */
    public long getMaxSize() {
        return maxSize;
    }

    public void clear() {
        cache.invalidateAll();
    }

    @Override
    public String toString() {
        return "CachedResult:" + cache.asMap().toString();
    }

}
