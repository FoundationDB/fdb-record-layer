/*
 * MarkReadableIfUniquenessResolvedCommitCheck.java
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
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.record.IndexState;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.tuple.ByteArrayUtil2;

import javax.annotation.Nonnull;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Commit check that marks {@link IndexState#READABLE_UNIQUE_PENDING} indexes as {@link IndexState#READABLE}
 * if no uniqueness violations remain.
 */
@API(API.Status.INTERNAL)
public class MarkReadableIfUniquenessResolvedCommitCheck implements FDBRecordContext.CommitCheckAsync {
    @Nonnull
    private final FDBRecordStore store;
    // List of READABLE_UNIQUE_PENDING indexes to check
    private final Set<Index> indexes = ConcurrentHashMap.newKeySet();

    private MarkReadableIfUniquenessResolvedCommitCheck(@Nonnull FDBRecordStore store) {
        this.store = store;
    }

    /**
     * Add this commit check for the given index, if the index is {@link IndexState#READABLE_UNIQUE_PENDING}.
     *
     * @param store the record store containing the index
     * @param index the index whose uniqueness violations may have been resolved
     */
    public static void addIfUniquePending(@Nonnull FDBRecordStore store, @Nonnull Index index) {
        if (!store.getIndexState(index).isReadableUniquePending()) {
            return;
        }
        final String commitCheckName = "markReadableIfUniquenessResolved_" + ByteArrayUtil2.toHexString(store.getSubspace().pack());
        final MarkReadableIfUniquenessResolvedCommitCheck commitCheck = (MarkReadableIfUniquenessResolvedCommitCheck)
                store.getRecordContext().getOrCreateCommitCheck(commitCheckName, name -> new MarkReadableIfUniquenessResolvedCommitCheck(store));
        commitCheck.indexes.add(index);
    }

    @Nonnull
    @Override
    public CompletableFuture<Void> checkAsync() {
        CompletableFuture<Boolean> anyChanged = AsyncUtil.READY_FALSE;
        for (Index index : indexes) {
            anyChanged = anyChanged.thenCompose(changed -> store.getIndexState(index).isReadableUniquePending()
                    ? store.updateIndexStateToReadable(index, true).thenApply(indexChanged -> changed || indexChanged)
                    : CompletableFuture.completedFuture(changed));
        }
        // This runs while the commit checks are being run, so remove the replaced indexes directly (and only once)
        // rather than adding another commit check
        return anyChanged.thenCompose(store::removeReplacedIndexesIfChanged).thenApply(ignore -> null);
    }
}
