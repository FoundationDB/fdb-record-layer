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
    private final Cache<K, V> cache;
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
    @Nonnull
    public CompletableFuture<V> orElseGet(@Nonnull K key, @Nonnull Supplier<CompletableFuture<V>> supplier) {
        try {
            V cachedValue = cache.getIfPresent(key);
            if (cachedValue == null) {
                return MoreAsyncUtil.getWithDeadline(deadlineTimeMillis, supplier).thenApply(value -> {
                    // Only insert the computed value into the cache if a concurrent caller hasn't.
                    // Return the value that wound up in the cache.
                    final V existingValue = cache.asMap().putIfAbsent(key, value);
                    return existingValue == null ? value : existingValue;
                });
            } else {
                return CompletableFuture.completedFuture(cachedValue);
            }
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
