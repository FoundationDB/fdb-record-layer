/*
 * MappedPool.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2020 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.provider.common;

import com.apple.foundationdb.annotation.API;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import javax.annotation.Nonnull;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

/**
 * MappedPool Class Attempts to reuse objects organized by keys.
 * This class can be used for objects that require thread safety but are expensive to create.
 * The number of keys in the ConcurrentHashMap is unbounded but the queue for each key is bounded to 64.
 *
 *  Examples include Ciphers and Compressors
 *
 * @param <K> key
 * @param <V> value type to be pooled
 * @param <E> exception that can be throw, must extend Exception
 */
@API(API.Status.EXPERIMENTAL)
public class MappedPool<K, V, E extends Exception> {
    protected static final int DEFAULT_POOL_SIZE = 64;
    protected static final int DEFAULT_MAX_ENTRIES = 64;
    protected final Cache<K, Queue<V>> pool;
    protected Callable<Queue<V>> loader;
    private final MappedPoolProvider<K, V, E> mappedPoolProvider;

    public MappedPool(MappedPoolProvider<K, V, E> mappedPoolProvider) {
        this(mappedPoolProvider, DEFAULT_POOL_SIZE);
    }

    public MappedPool(MappedPoolProvider<K, V, E> mappedPoolProvider, int poolSize) {
        this(mappedPoolProvider, poolSize, DEFAULT_MAX_ENTRIES);
    }

    public MappedPool(MappedPoolProvider<K, V, E> mappedPoolProvider, int defaultPoolSize, int maxEntries) {
        this.pool = CacheBuilder.newBuilder().maximumSize(maxEntries).build();
        this.loader = () -> new ArrayBlockingQueue<>(defaultPoolSize);
        this.mappedPoolProvider = mappedPoolProvider;
    }

    public V poll(@Nonnull K key) throws E {
        try {
            V next = pool.get(key, loader).poll();
            return next == null ? mappedPoolProvider.get(key) : next;
        } catch (ExecutionException ee) {
            return mappedPoolProvider.get(key);
        }
    }

    /**
     * Offer a key with a value knowing that the queue is bounded and might not accept the offer.
     *
     * @param key key
     * @param value value offered
     * @return boolean if offer was added to pool
     */
    public boolean offer(@Nonnull K key, @Nonnull V value) {
        try {
            return pool.get(key, loader).offer(value);
        } catch (Exception e) {
            return false;
        }
    }

    public Set<K> getKeys() {
        return pool.asMap().keySet();
    }

    /**
     * Pool size for the key.
     *
     * @param key key
     * @return size of pool
     */
    public int getPoolSize(K key) {
        Queue<V> queue = pool.getIfPresent(key);
        return queue == null ? 0 : queue.size();
    }

    /**
     * Invalidate all entries in the pool.
     */
    public void invalidateAll() {
        pool.invalidateAll();
    }

    /**
     * Function with Exceptions to provide the pool.
     *
     * @param <K> key
     * @param <V> value type to be pooled
     * @param <E> exception that can be throw, must extend Exception
     */
    public interface MappedPoolProvider<K, V, E extends Exception> {
        V get(K key) throws E;
    }

}
