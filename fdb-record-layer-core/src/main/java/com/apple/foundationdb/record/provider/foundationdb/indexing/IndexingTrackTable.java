/*
 * IndexingTrackTable.java
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

package com.apple.foundationdb.record.provider.foundationdb.indexing;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.IndexState;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.IndexingSubspaces;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;

import javax.annotation.Nonnull;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/**
 * The track table of an index that is being built in the {@link IndexState#WRITE_ONLY_WITH_TRACKING} state, bound to the transaction
 * of a record store. It holds the primary keys of the records that user io has already indexed, so that the online
 * indexer will skip them. This class encapsulates both writing to the table and reading from it.
 * <p>
 * Reading is a snapshot read: it adds no conflict, which is what keeps the indexer and user io from conflicting
 * with each other. A caller must therefore resolve conflicts through the record itself.
 * </p>
 */
@API(API.Status.INTERNAL)
public class IndexingTrackTable {
    private static final byte[] TRACKED = new byte[0];

    @Nonnull
    private final FDBRecordContext context;
    @Nonnull
    private final Subspace subspace;

    private IndexingTrackTable(@Nonnull FDBRecordContext context, @Nonnull Subspace subspace) {
        this.context = context;
        this.subspace = subspace;
    }

    /**
     * Get the track table of an index build.
     * @param store the record store associated with the index build
     * @param index the index being built
     * @return the track table of the index
     */
    @Nonnull
    public static IndexingTrackTable forIndexBuild(@Nonnull FDBRecordStore store, @Nonnull Index index) {
        return new IndexingTrackTable(store.getRecordContext(), IndexingSubspaces.indexTrackTableSubspace(store, index));
    }

    /**
     * Mark a record as indexed by user io. If this transaction also reads this record's entry, it must be read before
     * this call (the transaction would read its own write).
     * @param primaryKey the record's primary key
     */
    public void track(@Nonnull Tuple primaryKey) {
        context.ensureActive().set(subspace.pack(primaryKey), TRACKED);
    }

    /**
     * Check if a record was marked as indexed by user io. This is a snapshot read, it adds no conflict.
     * @param primaryKey the record's primary key
     * @return a future that is true if the record is tracked
     */
    @Nonnull
    public CompletableFuture<Boolean> isTrackedAsync(@Nonnull Tuple primaryKey) {
        return context.ensureActive().snapshot().get(subspace.pack(primaryKey)).thenApply(Objects::nonNull);
    }
}
