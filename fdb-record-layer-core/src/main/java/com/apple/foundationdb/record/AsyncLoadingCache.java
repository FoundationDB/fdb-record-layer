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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * A cache for retaining the result of some asynchronous operation up until some expiration time.
 * @param <K> key type for the cache
 * @param <V> value type for the cache
 */
@API(API.Status.UNSTABLE)
public class AsyncLoadingCache<K, V> {
    @Nonnull
    private final Cache<K, CompletableFuture<V>> cache;
    private final long refreshTimeMillis;
    private final long deadlineTimeMillis;

    public AsyncLoadingCache(long refreshTimeMillis) {
        this(refreshTimeMillis, 5000L);
    }

    public AsyncLoadingCache(long refreshTimeMillis, long deadlineTimeMillis) {
        this.refreshTimeMillis = refreshTimeMillis;
        this.deadlineTimeMillis = deadlineTimeMillis;
        cache = CacheBuilder.newBuilder().expireAfterWrite(this.refreshTimeMillis, TimeUnit.MILLISECONDS).build();
    }

    /**
     * If the cache does not contain an entry for <code>key</code>, retrieve the value using the provided asynchronous
     * {@link Supplier}. The future returned by the supplier is cached immediately, this is to prevent concurrent
     * attempts to get the value from doing unnecessary (and potentially expensive) work. If the returned asynchronous
     * operation completes exceptionally, the key will be cleared.
     *
     * @param key the key in the cache to lookup
     * @param supplier an asynchronous operation to retrieve the desired value if the cache is empty
     *
     * @return a future containing either the cached value or the result from the supplier
     */
    public CompletableFuture<V> orElseGet(@Nonnull K key, @Nonnull Supplier<CompletableFuture<V>> supplier) {
        try {
            return cache.get(key, () -> MoreAsyncUtil.getWithDeadline(deadlineTimeMillis, supplier)).whenComplete((ignored, e) -> {
                if (e != null) {
                    cache.invalidate(key);
                }
            });
        } catch (Exception e) {
            throw new RecordCoreException("failed getting value", e).addLogInfo("cacheKey", key);
        }
    }

    public long getRefreshTimeSeconds() {
        return TimeUnit.MILLISECONDS.toSeconds(refreshTimeMillis);
    }

    public void clear() {
        cache.invalidateAll();
    }

    @Override
    public String toString() {
        return "CachedResult:" + cache.asMap().toString();
    }

}
