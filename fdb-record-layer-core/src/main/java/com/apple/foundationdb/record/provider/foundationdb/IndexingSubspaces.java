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
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;

import javax.annotation.Nonnull;

/**
 * List of subspaces related to the indexing/index-scrubbing processes.
 */
public final class IndexingSubspaces {
    private static final Object INDEX_BUILD_LOCK_KEY = 0L;
    private static final Object INDEX_BUILD_SCANNED_RECORDS = 1L;
    private static final Object INDEX_BUILD_TYPE_VERSION = 2L;
    private static final Object INDEX_SCRUBBED_INDEX_RANGES_ZERO = 3L;
    private static final Object INDEX_SCRUBBED_RECORDS_RANGES_ZERO = 4L;
    private static final Object INDEX_SCRUBBED_INDEX_RANGES = 4L;
    private static final Object INDEX_SCRUBBED_RECORDS_RANGES = 5L;

    private IndexingSubspaces() {
        throw new IllegalStateException("Utility class");
    }

    @Nonnull
    private static Subspace indexBuildSubspace(@Nonnull FDBRecordStoreBase<?> store, @Nonnull Index index, Object key) {
        return store.getUntypedRecordStore().indexBuildSubspace(index).subspace(Tuple.from(key));
    }

    @Nonnull
    public static Subspace indexBuildLockSubspace(@Nonnull FDBRecordStoreBase<?> store, @Nonnull Index index) {
        return indexBuildSubspace(store, index, INDEX_BUILD_LOCK_KEY);
    }

    @Nonnull
    public static Subspace indexBuildScannedRecordsSubspace(@Nonnull FDBRecordStoreBase<?> store, @Nonnull Index index) {
        return indexBuildSubspace(store, index, INDEX_BUILD_SCANNED_RECORDS);
    }

    @Nonnull
    public static Subspace indexBuildTypeSubspace(@Nonnull FDBRecordStoreBase<?> store, @Nonnull Index index) {
        return indexBuildSubspace(store, index, INDEX_BUILD_TYPE_VERSION);
    }

    @Nonnull
    public static Subspace indexScrubRecordsRangeSubspaceLegacy(@Nonnull FDBRecordStoreBase<?> store, @Nonnull Index index) {
        return indexBuildSubspace(store, index, INDEX_SCRUBBED_RECORDS_RANGES_ZERO);
    }

    @Nonnull
    public static Subspace indexScrubRecordsRangeSubspace(@Nonnull FDBRecordStoreBase<?> store, @Nonnull Index index) {
        return indexBuildSubspace(store, index, INDEX_SCRUBBED_RECORDS_RANGES);
    }

    @Nonnull
    public static Subspace indexScrubIndexRangeSubspaceLegacy(@Nonnull FDBRecordStoreBase<?> store, @Nonnull Index index) {
        // Backward compatible
        return indexBuildSubspace(store, index, INDEX_SCRUBBED_INDEX_RANGES_ZERO);
    }

    @Nonnull
    public static Subspace indexScrubIndexRangeSubspace(@Nonnull FDBRecordStoreBase<?> store, @Nonnull Index index) {
        return indexBuildSubspace(store, index, INDEX_SCRUBBED_INDEX_RANGES);
    }

    public static void eraseAllIndexingScrubbingData(@Nonnull FDBRecordContext context, @Nonnull FDBRecordStore store, @Nonnull Index index) {
        context.clear(Range.startsWith(indexScrubIndexRangeSubspaceLegacy(store, index).pack()));
        context.clear(Range.startsWith(indexScrubIndexRangeSubspace(store, index).pack()));
        context.clear(Range.startsWith(indexScrubRecordsRangeSubspaceLegacy(store, index).pack()));
        context.clear(Range.startsWith(indexScrubRecordsRangeSubspace(store, index).pack()));
    }

    public static void eraseAllIndexingDataButTheLock(@Nonnull FDBRecordContext context, @Nonnull FDBRecordStore store, @Nonnull Index index) {
        eraseAllIndexingScrubbingData(context, store, index);
        context.clear(Range.startsWith(indexBuildScannedRecordsSubspace(store, index).pack()));
        context.clear(Range.startsWith(indexBuildTypeSubspace(store, index).pack()));
    }
}
