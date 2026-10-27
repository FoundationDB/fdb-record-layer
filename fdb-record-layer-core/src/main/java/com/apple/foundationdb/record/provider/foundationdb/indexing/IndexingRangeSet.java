/*
 * IndexingRangeSet.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2023 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.provider.foundationdb.indexing;

import com.apple.foundationdb.Range;
import com.apple.foundationdb.ReadTransaction;
import com.apple.foundationdb.TransactionContext;
import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.async.AsyncIterator;
import com.apple.foundationdb.async.RangeSet;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.IndexingSubspaces;
import com.apple.foundationdb.subspace.Subspace;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Wrapper around a {@link RangeSet} class that binds the class with a specific transaction. This then
 * exposes a subset of methods from the {@link RangeSet} class and executes them with the given transaction.
 * In general, Record Layer callers should prefer this class to using a {@link RangeSet} directly because
 * this methods are instrumented using the {@link FDBRecordContext}'s {@link FDBStoreTimer}.
 */
@API(API.Status.INTERNAL)
public class IndexingRangeSet {
    @Nonnull
    private final FDBRecordContext context;
    @Nonnull
    private final RangeSet rangeSet;

    private IndexingRangeSet(@Nonnull FDBRecordContext context, @Nonnull RangeSet rangeSet) {
        this.context = context;
        this.rangeSet = rangeSet;
    }

    /**
     * Returns whether the set is empty. This returns {@code false} if any range has been inserted
     * into the set and {@code true} otherwise.
     *
     * @return a future that returns whether or not the set is empty
     */
    @Nonnull
    public CompletableFuture<Boolean> isEmptyAsync() {
        long startTime = System.nanoTime();
        return context.instrument(FDBStoreTimer.Events.RANGE_SET_IS_EMPTY,
                rangeSet.isEmpty(context.ensureActive()), startTime);
    }

    /**
     * Clear the range set of all ranges. This will clear out the subspace backing this range set.
     */
    public void clear() {
        rangeSet.clear(context.ensureActive());
        context.increment(FDBStoreTimer.Counts.RANGE_SET_CLEAR);
    }

    /**
     * Determine if a key is in a range that has been inserted into the range set.
     *
     * @param key the key search for in the set
     *
     * @return a future that will either be {@code true} or {@code false} depending on
     * whether a range containing the key has been inserted
     *
     * @see RangeSet#contains(TransactionContext, byte[])
     */
    @Nonnull
    public CompletableFuture<Boolean> containsAsync(@Nonnull byte[] key) {
        long startTime = System.nanoTime();
        return context.instrument(FDBStoreTimer.Events.RANGE_SET_CONTAINS,
                rangeSet.contains(context.ensureActive(), key), startTime);
    }

    /**
     * Return the first missing range in the set. This behaves like {@link #firstMissingRangeAsync(byte[], byte[])},
     * but it operates on the whole range set.
     *
     * @return a future that will contain the first missing range or {@code null} if the set is complete
     *
     * @see #firstMissingRangeAsync(byte[], byte[])
     * @see RangeSet#missingRanges(ReadTransaction)
     */
    @Nonnull
    public CompletableFuture<Range> firstMissingRangeAsync() {
        return firstMissingRangeAsync(null, null);
    }

    /**
     * Find the first missing range between {@code begin} and {@code end}. This finds the first range of keys
     * that are not in the set yet where the begin endpoint is greater than or equal to {@code begin} and the
     * end endpoint is less than or equal to {@code end}. If no such range exists, then this will return a future
     * containing {@code null}.
     *
     * @param begin the inclusive begin endpoint or {@code null} to indicate the beginning
     * @param end the exclusive end endpoint or {@code null} to indicate the end
     *
     * @return a future that will either contain the first
     *
     * @see RangeSet#missingRanges(ReadTransaction, byte[], byte[])
     */
    @Nonnull
    public CompletableFuture<Range> firstMissingRangeAsync(@Nullable byte[] begin, @Nullable byte[] end) {
        final long startTime = System.nanoTime();
        final AsyncIterator<Range> ranges = rangeSet.missingRanges(context.ensureActive(), begin, end, 1).iterator();
        CompletableFuture<Range> future = ranges.onHasNext().thenApply(hasNext -> {
            if (hasNext) {
                return ranges.next();
            } else {
                return null;
            }
        });
        return context.instrument(FDBStoreTimer.Events.RANGE_SET_FIND_FIRST_MISSING, future, startTime);
    }

    /**
     * Get all missing ranges.
     *
     * @return a future containing a list of missing ranges
     *
     * @see #listMissingRangesAsync(byte[], byte[])
     */
    @Nonnull
    public CompletableFuture<List<Range>> listMissingRangesAsync() {
        return listMissingRangesAsync(null, null);
    }

    /**
     * Get all missing ranges within the given boundaries.
     *
     * @param begin the inclusive begin endpoint or {@code null} to indicate the beginning
     * @param end the exclusive end endpoint or {@code null} to indicate the end
     *
     * @return a future containing a list of missing ranges within the given boundaries
     *
     * @see RangeSet#missingRanges(ReadTransaction, byte[], byte[])
     */
    @Nonnull
    public CompletableFuture<List<Range>> listMissingRangesAsync(@Nullable byte[] begin, @Nullable byte[] end) {
        final long startTime = System.nanoTime();
        CompletableFuture<List<Range>> future = rangeSet.missingRanges(context.ensureActive(), begin, end)
                .asList();
        return context.instrument(FDBStoreTimer.Events.RANGE_SET_LIST_MISSING, future, startTime);
    }

    /**
     * Insert a range into the range set.
     *
     * @param begin the inclusive begin endpoint or {@code null} to indicate the beginning
     * @param end the exclusive end endpoint or {@code null} to indicate the end
     *
     * @return future that is {@code true} if there were modifications to the set and {@code false} otherwise
     *
     * @see RangeSet#insertRange(TransactionContext, byte[], byte[])
     */
    @Nonnull
    public CompletableFuture<Boolean> insertRangeAsync(@Nullable byte[] begin, @Nullable byte[] end) {
        return insertRangeAsync(begin, end, false);
    }

    /**
     * Insert a range into the range set. If {@code requiresEmpty} is set to {@code true}, then this
     * will only actually change the database if the range in the database was initially empty. See
     * {@link RangeSet#insertRange(TransactionContext, Range, boolean)} for more details.
     *
     * @param begin the inclusive begin endpoint or {@code null} to indicate the beginning
     * @param end the exclusive end endpoint or {@code null} to indicate the end
     * @param requireEmpty whether the range should only be inserted if the range was initially empty
     *
     * @return future that is {@code true} if there were modifications to the set and {@code false} otherwise
     *
     * @see RangeSet#insertRange(TransactionContext, byte[], byte[], boolean)
     */
    @Nonnull
    public CompletableFuture<Boolean> insertRangeAsync(@Nullable byte[] begin, @Nullable byte[] end, boolean requireEmpty) {
        final long startTime = System.nanoTime();
        return context.instrument(FDBStoreTimer.Events.RANGE_SET_INSERT,
                rangeSet.insertRange(context.ensureActive(), begin, end, requireEmpty), startTime);
    }

    /**
     * Create a range set for an index's build. The indexing machinery should update this range set as ranges
     * from the index are built.
     *
     * @param store the record store associated with the index build
     * @param index the index being built
     *
     * @return a {@code WrappedRangeSet} for an index build of a specific store
     */
    @Nonnull
    public static IndexingRangeSet forIndexBuild(@Nonnull FDBRecordStore store, @Nonnull Index index) {
        RangeSet rangeSet = new RangeSet(store.indexRangeSubspace(index));
        return new IndexingRangeSet(store.getRecordContext(), rangeSet);
    }

    /**
     * Create a range set for scrubbing the entries of an index. This is used by the index scrubber to
     * track how much of a given index has been scrubbed when looking for corruption such as dangling entries
     * (that is, index entries that no longer correspond to any record).
     *
     * @param store the record store associated with the index build
     * @param index the index being built
     *
     * @return a {@code WrappedRangeSet} for scrubbing the index's entries
     */
    @Nonnull
    public static IndexingRangeSet forScrubbingIndex(@Nonnull FDBRecordStore store, @Nonnull Index index, int rangeId) {
        final Subspace subspace = IndexingSubspaces.indexScrubIndexRangeSubspace(store, index, rangeId);
        final RangeSet rangeSet = new RangeSet(subspace);
        return new IndexingRangeSet(store.getRecordContext(), rangeSet);
    }

    /**
     * Create a range set for scrubbing the records of an index. This is used by the index scrubber to
     * track how much of the records have been scrubbed when looking for corruption such as missing entries
     * (that is, records that should have associated index entries in the index but do not).
     *
     * @param store the record store associated with the index build
     * @param index the index being built
     *
     * @return a {@code WrappedRangeSet} for scrubbing the index's records
     */
    @Nonnull
    public static IndexingRangeSet forScrubbingRecords(@Nonnull FDBRecordStore store, @Nonnull Index index, int rangeId) {
        final Subspace subspace = IndexingSubspaces.indexScrubRecordsRangeSubspace(store, index, rangeId);
        final RangeSet rangeSet = new RangeSet(subspace);
        return new IndexingRangeSet(store.getRecordContext(), rangeSet);
    }
}
