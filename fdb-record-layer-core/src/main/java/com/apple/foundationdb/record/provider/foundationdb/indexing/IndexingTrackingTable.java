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
 * The track table of an index that is being built in the {@link IndexState#WRITE_ONLY_WITH_TRACKING} state.
 * It holds the primary keys of the records that user I/O has already indexed, so that the {@link com.apple.foundationdb.record.provider.foundationdb.OnlineIndexer}
 * will skip them. This class should be used for all track table operations.
 */
@API(API.Status.INTERNAL)
public class IndexingTrackingTable {
    private static final byte[] TRACKED = new byte[0];

    @Nonnull
    private final FDBRecordContext context;
    @Nonnull
    private final Subspace subspace;

    private IndexingTrackingTable(@Nonnull FDBRecordContext context, @Nonnull Subspace subspace) {
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
    public static IndexingTrackingTable of(@Nonnull FDBRecordStore store, @Nonnull Index index) {
        return new IndexingTrackingTable(store.getRecordContext(), IndexingSubspaces.indexTrackTableSubspace(store, index));
    }

    /**
     * Mark a record as indexed by user io.
     * @param primaryKey the record's primary key
     */
    public void track(@Nonnull Tuple primaryKey) {
        context.ensureActive().set(subspace.pack(primaryKey), TRACKED);
    }

    /**
     * Check if a record was marked as indexed by user io. This read adds a conflict.
     * @param primaryKey the record's primary key
     * @return a future that is true if the record is tracked
     */
    @Nonnull
    public CompletableFuture<Boolean> isTrackedAsync(@Nonnull Tuple primaryKey) {
        return context.ensureActive().get(subspace.pack(primaryKey)).thenApply(Objects::nonNull);
    }

    /**
     * Check if a record was marked as indexed by user io. This is a snapshot read, it adds no conflict.
     * @param primaryKey the record's primary key
     * @return a future that is true if the record is tracked
     */
    @Nonnull
    public CompletableFuture<Boolean> isTrackedSnapshotAsync(@Nonnull Tuple primaryKey) {
        return context.ensureActive().snapshot().get(subspace.pack(primaryKey)).thenApply(Objects::nonNull);
    }
}
