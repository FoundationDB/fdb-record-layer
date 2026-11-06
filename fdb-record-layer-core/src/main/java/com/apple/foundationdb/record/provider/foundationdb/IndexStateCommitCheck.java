/*
 * IndexStateCommitCheck.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2026 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.provider.foundationdb;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.IndexState;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.tuple.ByteArrayUtil2;

import javax.annotation.Nonnull;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The single commit check that updates the index states of a record store right before commit. It first converts
 * {@link IndexState#READABLE_UNIQUE_PENDING} indexes whose uniqueness violations have all been resolved to
 * {@link IndexState#READABLE}, and then removes any replaced indexes. Doing both in one check guarantees that these
 * index state updates run sequentially.
 */
@API(API.Status.INTERNAL)
public class IndexStateCommitCheck implements FDBRecordContext.CommitCheckAsync {
    @Nonnull
    private final FDBRecordStore store;
    // READABLE_UNIQUE_PENDING indexes whose uniqueness violations may have been resolved
    private final Set<Index> uniquePendingIndexes = ConcurrentHashMap.newKeySet();
    private volatile boolean indexStatesChanged;

    private IndexStateCommitCheck(@Nonnull FDBRecordStore store) {
        this.store = store;
    }

    @Nonnull
    private static IndexStateCommitCheck getOrCreate(@Nonnull FDBRecordStore store) {
        final String commitCheckName = "indexState_" + ByteArrayUtil2.toHexString(store.getSubspace().pack());
        return (IndexStateCommitCheck)store.getRecordContext().getOrCreateCommitCheck(commitCheckName,
                name -> new IndexStateCommitCheck(store));
    }

    /**
     * Note that index states have changed in this transaction, so that replaced indexes are removed before commit.
     *
     * @param store the record store whose index states have changed
     */
    static void markIndexStatesChanged(@Nonnull FDBRecordStore store) {
        getOrCreate(store).indexStatesChanged = true;
    }

    /**
     * If the given index is {@link IndexState#READABLE_UNIQUE_PENDING}, try to mark it {@link IndexState#READABLE}.
     * This function is called only when a duplication is resolved.
     *
     * @param store the record store containing the index
     * @param index the index whose uniqueness violations may have been resolved
     */
    public static void addIfUniquePending(@Nonnull FDBRecordStore store, @Nonnull Index index) {
        if (store.getIndexState(index).isReadableUniquePending()) {
            getOrCreate(store).uniquePendingIndexes.add(index);
        }
    }

    @Nonnull
    @Override
    public CompletableFuture<Void> checkAsync() {
        // If removeReplacedIndexes was requested explicitly, it means that the index state had changed before adding the commit check.
        CompletableFuture<Boolean> anyChanged = CompletableFuture.completedFuture(indexStatesChanged);
        for (Index index : uniquePendingIndexes) {
            anyChanged = anyChanged.thenCompose(changed -> store.getIndexState(index).isReadableUniquePending()
                    ? store.updateIndexStateToReadable(index, true).thenApply(indexChanged -> changed || indexChanged)
                    : CompletableFuture.completedFuture(changed));
        }
        // Commit checks are already running, so remove the replaced indexes here rather than adding another check
        return anyChanged.thenCompose(store::removeReplacedIndexesIfChanged).thenApply(ignore -> null);
    }
}
