/*
 * BunchedMapIterator.java
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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;

/**
 * An iterator implementation that will iterate over the keys of a {@link BunchedMap}.
 * This handles "unbunching" the keys in the database so that the user can ignore
 * the underlying complexities of the on-disk format. The iterator may return keys
 * in either ascending or descending order depending on whether it is initialized to
 * be a reverse or non-reverse scan. This class is not thread safe.
 *
 * @param <K> type of keys in the map
 * @param <V> type of values in the map
 */
@API(API.Status.EXPERIMENTAL)
public class BunchedMapIterator<K,V> implements AsyncPeekIterator<Map.Entry<K,V>> {
    @Nonnull private final AsyncPeekIterator<KeyValue> underlying;
    @Nonnull private final Subspace subspace;
    @Nonnull private final ReadTransaction tr;
    @Nonnull private final byte[] subspaceKey;
    @Nonnull private final BunchedSerializer<K,V> serializer;
    @Nonnull private final Comparator<K> keyComparator;
    @Nullable private final K continuationKey;
    private final boolean reverse;
    private final int limit;

    @Nullable private CompletableFuture<Boolean> hasNextFuture;
    private boolean continuationSatisfied;
    @Nullable private List<Map.Entry<K,V>> currEntryList;
    private int currEntryIndex;
    @Nullable private K lastKey;
    private int returned;
    private boolean done;

    // In general, this class should not be instantiated by code outside
    // this package. Instead, it should use the scan() method on BunchedMaps.
    BunchedMapIterator(@Nonnull AsyncPeekIterator<KeyValue> underlying,
                       @Nonnull ReadTransaction tr,
                       @Nonnull Subspace subspace,
                       @Nonnull byte[] subspaceKey,
                       @Nonnull BunchedSerializer<K,V> serializer,
                       @Nonnull Comparator<K> keyComparator,
                       @Nullable K continuationKey,
                       int limit,
                       boolean reverse) {
        this.underlying = underlying;
        this.tr = tr;
        this.subspace = subspace;
        this.subspaceKey = subspaceKey;
        this.serializer = serializer;
        this.keyComparator = keyComparator;
        this.continuationKey = continuationKey;
        this.continuationSatisfied = continuationKey == null;
        this.limit = limit;
        this.reverse = reverse;
        this.currEntryList = null;
        this.currEntryIndex = -1;
        this.lastKey = null;
        this.returned = 0;
        this.done = false;
    }

    @Override
    public CompletableFuture<Boolean> onHasNext() {
        if (done) {
            return AsyncUtil.READY_FALSE;
        }
        if (currEntryList != null && currEntryIndex >= 0 && currEntryIndex < currEntryList.size()) {
            return AsyncUtil.READY_TRUE;
        }
        if (hasNextFuture == null) {
            hasNextFuture = underlying.onHasNext().thenCompose(doesHaveNext -> {
                if (doesHaveNext) {
                    return AsyncUtil.whileTrue(() -> {
                        KeyValue underlyingNext = underlying.peek();
                        if (!subspace.contains(underlyingNext.getKey())) {
                            if (ByteArrayUtil.compareUnsigned(underlyingNext.getKey(), subspaceKey) * (reverse ? -1 : 1) < 0) {
                                // We haven't gotten to this subspace yet, so try the next key.
                                underlying.next();
                                return underlying.onHasNext();
                            } else {
                                // We are done iterating through this subspace. Return
                                // without advancing the scan to support scanning
                                // over multiple subspaces.
                                done = true;
                                return AsyncUtil.READY_FALSE;
                            }
                        }
                        underlying.next(); // Advance the underlying scan.
                        final K boundaryKey = serializer.deserializeKey(underlyingNext.getKey(), subspaceKey.length);
                        List<Map.Entry<K,V>> nextEntryList = serializer.deserializeEntries(boundaryKey, underlyingNext.getValue());
                        if (nextEntryList.isEmpty()) {
                            // No entries in list. Try next key.
                            return underlying.onHasNext();
                        }
                        int nextItemIndex = reverse ? nextEntryList.size() - 1 : 0;
                        if (!continuationSatisfied) {
                            while (nextItemIndex >= 0 && nextItemIndex < nextEntryList.size()
                                    && keyComparator.compare(continuationKey, nextEntryList.get(nextItemIndex).getKey()) * (reverse ? -1 : 1) >= 0) {
                                nextItemIndex += reverse ? -1 : 1;
                            }
                            if (nextItemIndex < 0 || nextItemIndex >= nextEntryList.size()) {
                                // The continuation still wasn't satisfied after going
                                // through this entry. Move on to the next key in the database.
                                return underlying.onHasNext();
                            } else {
                                continuationSatisfied = true;
                            }
                        }
                        // TODO: We can be more exact about conflict ranges here.
                        currEntryIndex = nextItemIndex;
                        currEntryList = nextEntryList;
                        return AsyncUtil.READY_FALSE;
                    }, tr.getExecutor()).thenApply(vignore -> {
                        if (currEntryList == null || currEntryIndex < 0 || currEntryIndex >= currEntryList.size()) {
                            done = true;
                        }
                        return !done;
                    });
                } else {
                    // Exhausted scan.
                    done = true;
                    return AsyncUtil.READY_FALSE;
                }
            });
        }
        return hasNextFuture;
    }

    @Override
    public boolean hasNext() {
        return onHasNext().join();
    }

    @Override
    @Nonnull
    public Map.Entry<K,V> peek() {
        if (hasNext()) {
            // The hasNext method should enforce that currEntryList is not null
            // and that currEntryIndex is in the right range.
            if (currEntryList == null || currEntryIndex >= currEntryList.size() || currEntryIndex < 0) {
                throw new IllegalStateException();
            }
            return currEntryList.get(currEntryIndex);
        } else {
            throw new NoSuchElementException();
        }
    }

    @Override
    @Nonnull
    public Map.Entry<K,V> next() {
        Map.Entry<K,V> nextEntry = peek();
        lastKey = nextEntry.getKey();
        hasNextFuture = null;
        currEntryIndex += reverse ? -1 : 1;
        returned += 1;
        if (limit != ReadTransaction.ROW_LIMIT_UNLIMITED && returned >= limit) {
            done = true;
        }
        return nextEntry;
    }

    /**
     * Returns a continuation that can be passed to future calls of
     * {@link BunchedMap#scan(ReadTransaction, Subspace, byte[], int, boolean) BunchedMap.scan()}.
     * If the iterator has not yet returned anything or if the iterator is exhausted,
     * then this will return <code>null</code>. An iterator will be marked as exhausted
     * only if there are no more elements in the map.
     *
     * @return a continuation that can be used to resume iteration later
     */
    @Nullable
    public byte[] getContinuation() {
        if (lastKey == null || done && (limit == ReadTransaction.ROW_LIMIT_UNLIMITED || returned < limit)) {
            // We exhausted the scan.
            return null;
        } else {
            // We (potentially) hit the limit applied to this scan.
            // It's possible we both exhausted the underlying scan
            // and hit the limit, but the next time this gets called,
            // it just won't return anything.
            return serializer.serializeKey(lastKey);
        }
    }

    /**
     * Returns whether this iterator returns keys in reverse order. Forward order
     * is defined to be ascending order (by the key according to the key comparator
     * used in the corresponding {@link BunchedMap} that generated this iterator).
     * Reverse order is thus equivalent to descending order.
     *
     * @return whether this iterator returns values in reverse order
     */
    public boolean isReverse() {
        return reverse;
    }

    /**
     * Get the limit from the iterator. This is the maximum number of entries that
     * the iterator should return. If this iterator has no limit, it will return
     * {@link ReadTransaction#ROW_LIMIT_UNLIMITED}.
     *
     * @return the limit of this iterator
     */
    public int getLimit() {
        return limit;
    }

    @Override
    public void cancel() {
        underlying.cancel();
    }
}
