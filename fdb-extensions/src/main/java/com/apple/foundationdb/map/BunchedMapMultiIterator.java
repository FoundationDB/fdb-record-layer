/*
 * BunchedMapMultiIterator.java
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

package com.apple.foundationdb.map;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.ReadTransaction;
import com.apple.foundationdb.async.AsyncPeekIterator;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.ByteArrayUtil;

import org.jspecify.annotations.Nullable;

import java.util.Arrays;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/**
 * An iterator that will return {@link BunchedMapScanEntry} objects while scanning over multiple
 * {@link BunchedMap}s. This is the iterator returned by
 * {@link BunchedMap#scanMulti(ReadTransaction, Subspace, SubspaceSplitter, byte[], byte[], byte[], int, boolean) scanMulti()}.
 * This iterator will use the provided {@link SubspaceSplitter} to associate each key that it
 * reads with an underlying subspace. It will return keys in either ascending order or descending
 * order based on whether it is a "reverse" iterator (i.e., descending) or not. However, it will
 * sort keys first by subspace (using the unsigned byte order of the underlying prefixes) and
 * then by key, so use cases that need all keys in a strict order will have to re-sort the items
 * after they have all been returned by the iterator.
 *
 * @param <K> type of keys in the maps
 * @param <V> type of values in the maps
 * @param <T> type of tags associated with each subspace
 */
@API(API.Status.EXPERIMENTAL)
public class BunchedMapMultiIterator<K, V, T> implements AsyncPeekIterator<BunchedMapScanEntry<K, V, T>> {
    private final AsyncPeekIterator<KeyValue> underlying;
    private final ReadTransaction tr;
    private final Subspace subspace;
    private final byte[] subspaceKey;
    private final SubspaceSplitter<T> splitter;
    private final BunchedMap<K, V> bunchedMap;
    @Nullable private final byte[] continuation;
    private final boolean reverse;
    private final int limit;

    @Nullable private CompletableFuture<Boolean> hasNextFuture;
    private boolean continuationSatisfied;
    @Nullable private BunchedMapIterator<K, V> mapIterator;
    @Nullable private Subspace currentSubspace;
    @Nullable private byte[] currentSubspaceSuffix;
    @Nullable private byte[] currentSubspaceKey;
    @Nullable private T currentSubspaceTag;
    @Nullable private BunchedMapScanEntry<K, V, T> nextEntry;
    @Nullable private K lastKey;
    private boolean hasCurrent;
    private int returned;
    private boolean done;

    // NullAway does not reliably recognize @Nullable on byte[]-typed fields (currentSubspaceSuffix,
    // currentSubspaceKey above), so it both misflags the null literals assigned below and, separately,
    // insists -- incorrectly, since they are declared @Nullable -- that they be definitely assigned here.
    @SuppressWarnings("NullAway")
    BunchedMapMultiIterator(AsyncPeekIterator<KeyValue> underlying,
                            ReadTransaction tr,
                            Subspace subspace,
                            byte[] subspaceKey,
                            SubspaceSplitter<T> splitter,
                            BunchedMap<K, V> bunchedMap,
                            @Nullable byte[] continuation,
                            int limit,
                            boolean reverse) {
        this.underlying = underlying;
        this.tr = tr;
        this.subspace = subspace;
        this.subspaceKey = subspaceKey;
        this.splitter = splitter;
        this.bunchedMap = bunchedMap;
        this.continuation = continuation;
        this.continuationSatisfied = continuation == null;
        this.limit = limit;
        this.reverse = reverse;
        this.mapIterator = null;
        this.currentSubspace = null;
        this.currentSubspaceKey = null;
        this.currentSubspaceTag = null;
        this.nextEntry = null;
        this.returned = 0;
        this.done = false;
    }

    private CompletableFuture<Boolean> getNextMapIterator() {
        return underlying.onHasNext().thenCompose(doesHaveNext -> {
            if (doesHaveNext) {
                KeyValue nextKv = underlying.peek();
                if (!subspace.contains(nextKv.getKey())) {
                    if (ByteArrayUtil.compareUnsigned(nextKv.getKey(), subspaceKey) * (reverse ? -1 : 1) > 0) {
                        // We have already gone past the end of the subspace. We are done.
                        underlying.cancel();
                        return AsyncUtil.READY_FALSE;
                    } else {
                        // We haven't gotten to the subspace we need yet. Advance the iterator
                        // until we are in the correct range and then try again.
                        // In practice, this should only happen when we are given a continuation
                        // key that is at a boundary, so this shouldn't require doing too
                        // much reading, and it shouldn't make too many recursive calls.
                        return AsyncUtil.whileTrue(() -> {
                            KeyValue kv = underlying.peek();
                            if (ByteArrayUtil.compareUnsigned(kv.getKey(), subspaceKey) * (reverse ? -1 : 1) >= 0) {
                                return AsyncUtil.READY_FALSE;
                            } else {
                                underlying.next();
                                return underlying.onHasNext();
                            }
                        }, tr.getExecutor()).thenCompose(vignore -> getNextMapIterator());
                    }
                }
                Subspace nextSubspace = splitter.subspaceOf(nextKv.getKey());
                byte[] nextSubspaceKey = nextSubspace.getKey();
                byte[] nextSubspaceSuffix = Arrays.copyOfRange(nextSubspaceKey, subspaceKey.length, nextSubspaceKey.length);
                K continuationKey = null;
                if (!continuationSatisfied) {
                    // !continuationSatisfied implies continuation != null: continuationSatisfied starts out as
                    // (continuation == null) and is only ever subsequently set to true, never back to false. But
                    // NullAway cannot correlate the nullness of two different fields, so it is reasserted here.
                    final byte[] nonNullContinuation = Objects.requireNonNull(continuation);
                    if (ByteArrayUtil.startsWith(nonNullContinuation, nextSubspaceSuffix)) {
                        continuationKey = bunchedMap.getSerializer().deserializeKey(nonNullContinuation, nextSubspaceSuffix.length);
                        continuationSatisfied = true;
                    } else if (ByteArrayUtil.compareUnsigned(nextSubspaceSuffix, nonNullContinuation) * (reverse ? -1 : 1) > 0) {
                        // We have already satisfied the continuation, so we are can just say it is satisfied
                        continuationSatisfied = true;
                        continuationKey = null;
                    } else {
                        // Skip through the iterator until we are out of the
                        // current subspace.
                        return AsyncUtil.whileTrue(() -> {
                            KeyValue kv = underlying.peek();
                            if (nextSubspace.contains(kv.getKey())) {
                                underlying.next();
                                return underlying.onHasNext();
                            } else {
                                return AsyncUtil.READY_FALSE;
                            }
                        }, tr.getExecutor()).thenCompose(vignore -> getNextMapIterator());
                    }
                }
                BunchedMapIterator<K, V> nextMapIterator = new BunchedMapIterator<>(
                        underlying,
                        tr,
                        nextSubspace,
                        nextSubspaceKey,
                        bunchedMap,
                        continuationKey,
                        (limit == ReadTransaction.ROW_LIMIT_UNLIMITED) ? ReadTransaction.ROW_LIMIT_UNLIMITED : limit - returned,
                        reverse
                );
                final T nextSubspaceTag = splitter.subspaceTag(nextSubspace);
                return nextMapIterator.onHasNext().thenCompose(mapHasNext -> {
                    if (mapHasNext) {
                        currentSubspace = nextSubspace;
                        currentSubspaceKey = nextSubspaceKey;
                        currentSubspaceSuffix = nextSubspaceSuffix;
                        currentSubspaceTag = nextSubspaceTag;
                        mapIterator = nextMapIterator;
                        Map.Entry<K, V> mapEntry = mapIterator.next();
                        nextEntry = new BunchedMapScanEntry<>(currentSubspace, currentSubspaceTag, mapEntry.getKey(), mapEntry.getValue());
                        return AsyncUtil.READY_TRUE;
                    } else {
                        // This can only happen in the case where we had a continuation
                        // that was at the end of the last map iterator's subspace, so
                        // we will only be recursing down a finite amount.
                        return getNextMapIterator();
                    }
                });
            } else {
                done = true;
                return AsyncUtil.READY_FALSE;
            }
        });
    }

    @Override
    public CompletableFuture<Boolean> onHasNext() {
        if (done) {
            return AsyncUtil.READY_FALSE;
        }
        if (hasCurrent) {
            return AsyncUtil.READY_TRUE;
        }
        if (hasNextFuture == null) {
            if (mapIterator != null) {
                // Captured to a local because the null-check above does not survive the lambda boundary below
                // (mapIterator is a mutable field).
                final BunchedMapIterator<K, V> currentMapIterator = mapIterator;
                hasNextFuture = currentMapIterator.onHasNext().thenCompose(mapHasNext -> {
                    if (mapHasNext) {
                        Map.Entry<K, V> entry = currentMapIterator.next();
                        // currentSubspace/currentSubspaceKey are always set together with mapIterator (see
                        // getNextMapIterator()), so they are non-null here too; assert is not used because
                        // NullAway does not treat it as a null-check (and assertions may be disabled at runtime).
                        final Subspace nonNullCurrentSubspace = Objects.requireNonNull(currentSubspace);
                        Objects.requireNonNull(currentSubspaceKey);
                        nextEntry = new BunchedMapScanEntry<>(nonNullCurrentSubspace, currentSubspaceTag, entry.getKey(), entry.getValue());
                        return AsyncUtil.READY_TRUE;
                    } else if (limit != ReadTransaction.ROW_LIMIT_UNLIMITED && returned == limit) {
                        // Because we pass limit information to the sub-iterators,
                        // we only have to check limits here.
                        done = true;
                        return AsyncUtil.READY_FALSE;
                    } else {
                        return getNextMapIterator();
                    }
                });
            } else {
                hasNextFuture = getNextMapIterator();
            }
        }
        return hasNextFuture;
    }

    @Override
    public boolean hasNext() {
        return onHasNext().join();
    }

    @Override
    public BunchedMapScanEntry<K, V, T> peek() {
        if (hasNext() && nextEntry != null) {
            return nextEntry;
        } else {
            throw new NoSuchElementException();
        }
    }

    @Override
    public BunchedMapScanEntry<K, V, T> next() {
        BunchedMapScanEntry<K, V, T> nextEntry = peek();
        lastKey = nextEntry.getKey();
        hasCurrent = false;
        hasNextFuture = null;
        returned += 1;
        return nextEntry;
    }

    /**
     * Returns a continuation that can be passed to future calls of
     * {@link BunchedMap#scanMulti(ReadTransaction, Subspace, SubspaceSplitter, byte[], byte[], byte[], int, boolean) BunchedMap.scanMulti()}.
     * If the iterator has not yet returned anything or if the iterator is exhausted,
     * then this will return <code>null</code>. An iterator will be marked as exhausted
     * only if there are no more elements in any of the maps being scanned.
     *
     * @return a continuation that can be used to resume iteration later
     */
    @Nullable
    // NullAway does not reliably recognize @Nullable on array-typed return values, so the `return null;`
    // below is misflagged as returning @Nullable from a @NonNull-returning method despite the annotation.
    @SuppressWarnings("NullAway")
    public byte[] getContinuation() {
        if (lastKey == null || currentSubspaceKey == null || done && (limit == ReadTransaction.ROW_LIMIT_UNLIMITED || returned < limit)) {
            // We exhausted the scan.
            return null;
        } else {
            // Return a continuation with the information about the subspace key and the last key serialized.
            // currentSubspaceSuffix is always set together with currentSubspaceKey (see getNextMapIterator()),
            // so it is non-null here too, but NullAway cannot correlate the nullness of two different fields.
            final byte[] nonNullCurrentSubspaceSuffix = Objects.requireNonNull(currentSubspaceSuffix);
            return ByteArrayUtil.join(nonNullCurrentSubspaceSuffix, bunchedMap.getSerializer().serializeKey(lastKey));
        }
    }

    /**
     * Returns whether this iterator returns keys in reverse order. Forward order
     * is defined to be ascending order by subspace according to the byte prefixes
     * of each one followed by key order within a subspace. Reverse order is thus
     * equivalent to descending order.
     *
     * @return whether this iterator returns values in reverse order
     */
    public boolean isReverse() {
        return reverse;
    }

    /**
     * Returns the maximum number of entries that this iterator will return.
     * It will return {@link ReadTransaction#ROW_LIMIT_UNLIMITED} if this iterator
     * has no limit.
     *
     * @return the limit of this iterator
     */
    public int getLimit() {
        return limit;
    }

    @Override
    public void cancel() {
        if (mapIterator != null) {
            mapIterator.cancel();
        }
        underlying.cancel();
    }
}
