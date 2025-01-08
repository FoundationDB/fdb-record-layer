/*
 * DistinctSampler.java
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

package com.apple.foundationdb.relational.util;

import com.apple.foundationdb.annotation.API;


import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;

import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

/**
 * A sampler which tests individual elements according to a fixed-throughput adaptive strategy.
 * <p>
 * Essentially, this logger ensures that the given item is only logged as often as a specified rate requires. It
 * does this by maintaining a cache of items. Upon request, if the element is not in the cache, it is logged. Otherwise,
 * it is logged according to the configured {@link TokenBucketSampler} for that item, which maintains a (mostly)
 * constant rate of log sampling.
 *
 * @param <K> the type of item to sample.
 */
@API(API.Status.EXPERIMENTAL)
public class DistinctSampler<K> implements Predicate<K> {
    private static final long NANOS_PER_SECOND = TimeUnit.SECONDS.toNanos(1);
    private final LoadingCache<K, Sampler> cache;

    public DistinctSampler(int maxSize, int maxLogsPerSecond) {
        this(maxSize, () -> new TokenBucketSampler(maxLogsPerSecond, NANOS_PER_SECOND, Clocks.systemClock()));
    }

    public DistinctSampler(int maxCacheSize, java.util.function.Supplier<Sampler> samplerFactory) {
        this.cache = Caffeine.newBuilder()
                .maximumSize(maxCacheSize)
                .build(item -> samplerFactory.get());
    }

    @Override
    public boolean test(K item) {
        //this will never be empty, because the loading cache _should_ generate a new sampler as needed
        return cache.get(item).canSample();
    }

}
