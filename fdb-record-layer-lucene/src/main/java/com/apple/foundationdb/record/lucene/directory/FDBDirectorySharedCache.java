/*
 * FDBDirectorySharedCache.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2022 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.lucene.directory;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.commons.lang3.tuple.Pair;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A shared cache for a single {@link FDBDirectory}.
 */
@API(API.Status.EXPERIMENTAL)
@ThreadSafe
public class FDBDirectorySharedCache {

    @Nonnull
    private final Tuple key;
    private final long sequenceNumber;
    @Nonnull
    private final AtomicReference<Map<String, FDBLuceneFileReference>> fileReferences;

    @Nonnull
    private AtomicReference<ConcurrentMap<Long, AtomicInteger>> fieldInfosReferenceCount = new AtomicReference<>();
    @Nonnull
    private final Cache<Pair<Long, Integer>, byte[]> blocks;

    public FDBDirectorySharedCache(@Nonnull Tuple key, long sequenceNumber,
                                   int maximumSize, int concurrencyLevel, int initialCapacity) {
        this.key = key;
        this.sequenceNumber = sequenceNumber;
        this.fileReferences = new AtomicReference<>();
        this.blocks = CacheBuilder.newBuilder()
                .concurrencyLevel(concurrencyLevel)
                .initialCapacity(initialCapacity)
                .maximumSize(maximumSize)
                .recordStats()
                .build();
    }

    /**
     * Get the key for this directory cache.
     * The key is relative to the record store root, so including the index subspace prefix and any grouping keys.
     * @return the key for this directory cache
     */
    @Nonnull
    public Tuple getKey() {
        return key;
    }

    /**
     * Get the sequence number of this directory cache.
     * The sequence number advances whenever new data is written to the directory.
     * @return the sequence number of this directory cache
     */
    public long getSequenceNumber() {
        return sequenceNumber;
    }

    /**
     * Get the set of file references for this directory, if present in the cache.
     * @return cached file references or {@code null} if not cached
     */
    @Nullable
    public Map<String, FDBLuceneFileReference> getFileReferencesIfPresent() {
        return fileReferences.get();
    }

    /**
     * Add set of file references to the cache.
     * @param fileReferences the file references for the associated directory as of the sequence number
     */
    public void setFileReferencesIfAbsent(@Nonnull Map<String, FDBLuceneFileReference> fileReferences) {
        this.fileReferences.compareAndSet(null, fileReferences);
    }

    /**
     * Get a block from a file if present in the cache.
     * @param id file id
     * @param blockNumber block number in the file
     * @return the cached block or {@code null} if not cached
     */
    @Nullable
    public byte[] getBlockIfPresent(long id, int blockNumber) {
        return blocks.getIfPresent(Pair.of(id, blockNumber));
    }

    /**
     * Add a block from a file to the cache.
     * @param id file id
     * @param blockNumber block number in the file
     * @param block the block to be cached
     */
    public void putBlockIfAbsent(long id, int blockNumber, @Nonnull byte[] block) {
        blocks.asMap().putIfAbsent(Pair.of(id, blockNumber), block);
    }

    public void setFieldInfosReferenceCount(final ConcurrentMap<Long, AtomicInteger> fieldInfosReferenceCount) {
        this.fieldInfosReferenceCount.compareAndSet(null, fieldInfosReferenceCount);
    }

    public ConcurrentMap<Long, AtomicInteger> getFieldInfosReferenceCount() {
        return this.fieldInfosReferenceCount.get();
    }
}
