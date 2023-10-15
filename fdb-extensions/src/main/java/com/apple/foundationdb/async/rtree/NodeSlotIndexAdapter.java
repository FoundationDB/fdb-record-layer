/*
 * NodeSlotIndexAdapter.java
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

package com.apple.foundationdb.async.rtree;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.ReadTransaction;
import com.apple.foundationdb.StreamingMode;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.base.Verify;
import com.google.common.collect.Lists;

import javax.annotation.Nonnull;
import java.math.BigInteger;
import java.util.List;
import java.util.concurrent.CompletableFuture;

class NodeSlotIndexAdapter {
    @Nonnull
    private final Subspace secondarySubspace;

    NodeSlotIndexAdapter(@Nonnull final Subspace secondarySubspace) {
        this.secondarySubspace = secondarySubspace;
    }

    @Nonnull
    public Subspace getSecondarySubspace() {
        return secondarySubspace;
    }

    @Nonnull
    CompletableFuture<byte[]> scanIndexForNodeId(@Nonnull final ReadTransaction transaction,
                                                 final int level,
                                                 @Nonnull final BigInteger hilbertValue,
                                                 @Nonnull final Tuple key,
                                                 final boolean isInsertUpdate) {
        final List<Object> keys = Lists.newArrayList();
        keys.add(level);
        keys.add(hilbertValue);
        keys.addAll(key.getItems());
        final byte[] packedKey = secondarySubspace.pack(Tuple.fromList(keys));
        return AsyncUtil.collect(transaction.getRange(new Range(packedKey,
                        secondarySubspace.pack(Tuple.from(level + 1))), 1, false, StreamingMode.EXACT))
                .thenCompose(keyValues -> {
                    Verify.verify(keyValues.size() <= 1);
                    if (!keyValues.isEmpty()) {
                        final KeyValue keyValue = keyValues.get(0);
                        return CompletableFuture.completedFuture(keyValue.getValue());
                    }
                    if (!isInsertUpdate) {
                        // no such node; return null if on the delete path
                        return CompletableFuture.completedFuture(null);
                    }
                    // if on the insert/update try to fetch the previous node
                    return AsyncUtil.collect(transaction.getRange(new Range(secondarySubspace.pack(Tuple.from(level)),
                                    packedKey), 1, true, StreamingMode.EXACT))
                            .thenApply(previousKeyValues -> {
                                Verify.verify(previousKeyValues.size() <= 1);
                                if (previousKeyValues.isEmpty()) {
                                    return RTree.rootId;
                                }
                                final KeyValue keyValue = previousKeyValues.get(0);
                                return keyValue.getValue();
                            });
                });
    }

    void writeChildSlot(final @Nonnull Transaction transaction, final int level,
                                @Nonnull final ChildSlot childSlot) {
        transaction.set(secondarySubspace.pack(createIndexKeyTuple(level, childSlot)), childSlot.getChildId());
    }

    void clearChildSlot(final @Nonnull Transaction transaction, final int level, @Nonnull final ChildSlot childSlot) {
        transaction.clear(secondarySubspace.pack(createIndexKeyTuple(level, childSlot)));
    }

    @Nonnull
    private Tuple createIndexKeyTuple(final int level, @Nonnull final ChildSlot childSlot) {
        return createIndexKeyTuple(level, childSlot.getLargestHilbertValue(), childSlot.getLargestKey());
    }

    @Nonnull
    private Tuple createIndexKeyTuple(final int level, @Nonnull final BigInteger largestHilbertValue,
                                      @Nonnull final Tuple largestKey) {
        final List<Object> keys = Lists.newArrayList();
        keys.add(level);
        keys.add(largestHilbertValue);
        keys.addAll(largestKey.getItems());
        return Tuple.fromList(keys);
    }
}
