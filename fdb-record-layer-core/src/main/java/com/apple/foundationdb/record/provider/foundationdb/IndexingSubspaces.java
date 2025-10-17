/*
 * IndexingSubspaces.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2025 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.Range;
import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;

import javax.annotation.Nonnull;
import java.util.UUID;

/**
 * List of subspaces related to the indexing/index-scrubbing processes.
 */
@API(API.Status.INTERNAL)
public final class IndexingSubspaces {
    private static final Object INDEX_BUILD_LOCK_KEY = 0L;
    private static final Object INDEX_BUILD_SCANNED_RECORDS = 1L;
    private static final Object INDEX_BUILD_TYPE_VERSION = 2L;
    private static final Object INDEX_SCRUBBED_INDEX_RANGES_ZERO = 3L;
    private static final Object INDEX_SCRUBBED_RECORDS_RANGES_ZERO = 4L;
    private static final Object INDEX_SCRUBBED_RECORDS_RANGES = 5L;
    private static final Object INDEX_SCRUBBED_INDEX_RANGES = 6L;
    private static final Object INDEX_BUILD_HEARTBEAT_PREFIX = 7L;

    private IndexingSubspaces() {
        throw new IllegalStateException("Utility class");
    }

    @Nonnull
    private static Subspace indexBuildSubspace(@Nonnull FDBRecordStoreBase<?> store, @Nonnull Index index, Object key) {
        return store.getUntypedRecordStore().indexBuildSubspace(index).subspace(Tuple.from(key));
    }

    /**
     * Subspace that stores the lock for a synced indexing session.
     * @param store store
     * @param index index
     * @return subspace
     */
    @Nonnull
    public static Subspace indexBuildLockSubspace(@Nonnull FDBRecordStoreBase<?> store, @Nonnull Index index) {
        return indexBuildSubspace(store, index, INDEX_BUILD_LOCK_KEY);
    }

    /**
     * Subspace that stores the number of scanned items during indexing.
     * @param store store
     * @param index index
     * @return subspace
     */
    @Nonnull
    public static Subspace indexBuildScannedRecordsSubspace(@Nonnull FDBRecordStoreBase<?> store, @Nonnull Index index) {
        return indexBuildSubspace(store, index, INDEX_BUILD_SCANNED_RECORDS);
    }

    /**
     * Subspace that stores the indexing build type stamp.
     * @param store store
     * @param index index
     * @return subspace
     */
    @Nonnull
    public static Subspace indexBuildTypeSubspace(@Nonnull FDBRecordStoreBase<?> store, @Nonnull Index index) {
        return indexBuildSubspace(store, index, INDEX_BUILD_TYPE_VERSION);
    }

    /**
     * Subspace that stores the indexing heartbeat.
     * @param store store
     * @param index index
     * @return subspace
     */
    @Nonnull
    public static Subspace indexHeartbeatSubspace(@Nonnull FDBRecordStoreBase<?> store, @Nonnull Index index) {
        return indexBuildSubspace(store, index, INDEX_BUILD_HEARTBEAT_PREFIX);
    }

    /**
     * Subspace that stores the indexing heartbeat.
     * @param store store
     * @param index index
     * @param indexerId session id
     * @return subspace
     */
    @Nonnull
    public static byte[] indexHeartbeatSubspaceBytes(@Nonnull FDBRecordStoreBase<?> store, @Nonnull Index index, @Nonnull UUID indexerId) {
        return indexHeartbeatSubspace(store, index).subspace(Tuple.from(indexerId)).pack();
    }

    /**
     * Subspace that stores scrubbed records ranges of the zero range-id. This subspace is backward compatible
     * to record ranges scrubbed before range-id was introduced.
     * @param store store
     * @param index index
     * @return subspace
     */
    @Nonnull
    private static Subspace indexScrubRecordsRangeSubspaceZero(@Nonnull FDBRecordStoreBase<?> store, @Nonnull Index index) {
        // Backward compatible subspace for range-id zero
        return indexBuildSubspace(store, index, INDEX_SCRUBBED_RECORDS_RANGES_ZERO);
    }

    /**
     * Subspace that stores scrubbed records ranges.
     * @param store store
     * @param index index
     * @return subspace
     */
    @Nonnull
    private static Subspace indexScrubRecordsRangeSubspaceRoot(@Nonnull FDBRecordStoreBase<?> store, @Nonnull Index index) {
        return indexBuildSubspace(store, index, INDEX_SCRUBBED_RECORDS_RANGES);
    }

    /**
     * Subspace that stores scrubbed records ranges.
     * @param store store
     * @param index index
     * @return subspace
     */
    @Nonnull
    public static Subspace indexScrubRecordsRangeSubspace(@Nonnull FDBRecordStoreBase<?> store, @Nonnull Index index, int rangeId) {
        return rangeId == 0 ?
               // Backward compatible
               IndexingSubspaces.indexScrubRecordsRangeSubspaceZero(store, index) :
               IndexingSubspaces.indexScrubRecordsRangeSubspaceRoot(store, index).subspace(Tuple.from(rangeId));
    }

    /**
     * Subspace that stores scrubbed index entries ranges of the zero range-id. This subspace is backward compatible
     * to index entries ranges scrubbed before range-id was introduced.
     * @param store store
     * @param index index
     * @return subspace
     */
    @Nonnull
    private static Subspace indexScrubIndexRangeSubspaceZero(@Nonnull FDBRecordStoreBase<?> store, @Nonnull Index index) {
        // Backward compatible subspace for range-id zero
        return indexBuildSubspace(store, index, INDEX_SCRUBBED_INDEX_RANGES_ZERO);
    }

    /**
     * Subspace that stores scrubbed index entries ranges. This subspace is expected to be followed by a range-id.
     * @param store store
     * @param index index
     * @return subspace
     */
    @Nonnull
    private static Subspace indexScrubIndexRangeSubspaceRoot(@Nonnull FDBRecordStoreBase<?> store, @Nonnull Index index) {
        return indexBuildSubspace(store, index, INDEX_SCRUBBED_INDEX_RANGES);
    }

    /**
     * Subspace that stores scrubbed index entries ranges.
     * @param store store
     * @param index index
     * @param rangeId used by the caller to distinct different scrubbing sessions.
     * @return subspace
     */
    @Nonnull
    public static Subspace indexScrubIndexRangeSubspace(@Nonnull FDBRecordStoreBase<?> store, @Nonnull Index index, int rangeId) {
        return rangeId == 0 ?
            // Backward compatible
            IndexingSubspaces.indexScrubIndexRangeSubspaceZero(store, index) :
            IndexingSubspaces.indexScrubIndexRangeSubspaceRoot(store, index).subspace(Tuple.from(rangeId));
    }

    /**
     * Erasing all the scrubbing ranges data. After calling this function, there would be no memory of scrubbed ranges.
     * @param context user context - the operation will run within this context
     * @param store store
     * @param index index
     */
    public static void eraseAllIndexingScrubbingData(@Nonnull FDBRecordContext context, @Nonnull FDBRecordStore store, @Nonnull Index index) {
        context.clear(Range.startsWith(indexScrubIndexRangeSubspaceZero(store, index).pack()));
        context.clear(Range.startsWith(indexScrubIndexRangeSubspaceRoot(store, index).pack()));
        context.clear(Range.startsWith(indexScrubRecordsRangeSubspaceZero(store, index).pack()));
        context.clear(Range.startsWith(indexScrubRecordsRangeSubspaceRoot(store, index).pack()));
    }

    /**
     * Delete all data related to the indexing/scrubbing process except of the index locks. Index locks are preserved because
     * often this operation is expected to be protected by this lock.
     * @param context user context - the operation will run within this context
     * @param store store
     * @param index index
     */
    public static void eraseAllIndexingDataButTheLock(@Nonnull FDBRecordContext context, @Nonnull FDBRecordStore store, @Nonnull Index index) {
        eraseAllIndexingScrubbingData(context, store, index);
        context.clear(Range.startsWith(indexBuildScannedRecordsSubspace(store, index).pack()));
        context.clear(Range.startsWith(indexBuildTypeSubspace(store, index).pack()));
        // The heartbeats, unlike the sync lock, may be erased here. If needed, an appropriate heartbeat will be set after this clear & within the same transaction.
        context.clear(Range.startsWith(indexHeartbeatSubspace(store, index).pack()));
    }
}
