/*
 * LuceneIndexScrubbingToolsDangling.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2024 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.lucene;

import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.IsolationLevel;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorResult;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.cursors.AutoContinuingCursor;
import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabaseRunner;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainerState;
import com.apple.foundationdb.record.provider.foundationdb.IndexOrphanBehavior;
import com.apple.foundationdb.record.provider.foundationdb.IndexScrubbingTools;
import com.apple.foundationdb.record.query.plan.ScanComparisons;
import com.apple.foundationdb.tuple.Tuple;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;

/**
 * Index Scrubbing Toolbox for a Lucene index maintainer. Scrub dangling value index entries - i.e. index entries
 * pointing to non-existing record(s)
 */
public class LuceneIndexScrubbingToolsDangling implements IndexScrubbingTools<IndexEntry> {
    Index index = null;
    boolean isSynthetic = false;
    @Nonnull
    private final LucenePartitioner partitioner; // non-mutable
    @Nonnull
    private final IndexMaintainerState state;

    public LuceneIndexScrubbingToolsDangling(@Nonnull final LucenePartitioner partitioner, @Nonnull final IndexMaintainerState state) {
        this.partitioner = partitioner;
        this.state = state;
    }

    @Override
    public void presetCommonParams(final Index index, final boolean allowRepair, final boolean isSynthetic, final Collection<RecordType> types) {
        this.index = index;
        this.isSynthetic = isSynthetic;
    }

    @Override
    public RecordCursor<IndexEntry> getCursor(final TupleRange range, final FDBRecordStore store, final int limit) {
        // TODO: Range tuple should begin with a null or [groupingKey, timestamp]
        FDBDatabaseRunner runner = state.context.newRunner();
        final ScanProperties scanProperties = new ScanProperties(ExecuteProperties.newBuilder()
                .setIsolationLevel(IsolationLevel.SERIALIZABLE)
                .setReturnedRowLimit(limit)
                .build());
        // TODO: start from continuation
        LuceneQueryClause search = LuceneQuerySearchClause.MATCH_ALL_DOCS_QUERY;
        LuceneScanParameters scan = new LuceneScanQueryParameters(
                ScanComparisons.EMPTY,
                search,
                null, null, null,
                null);
        // See paritiiionManager
        return new AutoContinuingCursor<>(
                runner,
                (context, continuation) -> {
                    LuceneScanBounds scanBounds = scan.bind(store, index, EvaluationContext.EMPTY);
                    return store.scanIndex(index, scanBounds, continuation, scanProperties);
                });
    }

    @Override
    public Tuple getKeyFromCursorResult(final RecordCursorResult<IndexEntry> result) {
        final IndexEntry indexEntry = result.get();
        return indexEntry == null ? null : indexEntry.getKey();
        // Todo: return tuple that contains groupId, timestmap

    }

    @Override
    public CompletableFuture<Issue> handleOneItem(final FDBRecordStore store, final RecordCursorResult<IndexEntry> result) {
        if (index == null) {
            throw new IllegalStateException("presetParams was not called appropriately for this scrubbing tool");
        }

        final IndexEntry indexEntry = result.get();
        if (indexEntry == null) {
            return CompletableFuture.completedFuture(null);
        }

        if (isSynthetic) {
            return store.loadSyntheticRecord(indexEntry.getPrimaryKey()).thenApply(syntheticRecord -> {
                if (syntheticRecord.getConstituents().isEmpty()) {
                    // None of the constituents of this synthetic type are present, so it must be dangling
                    return scrubDanglingEntry(indexEntry);
                }
                return null;
            });
        } else {
            return store.loadIndexEntryRecord(indexEntry, IndexOrphanBehavior.RETURN).thenApply(indexedRecord -> {
                if (!indexedRecord.hasStoredRecord()) {
                    // Here: Oh, No! this index is dangling!
                    return scrubDanglingEntry(indexEntry);
                }
                return null;
            });
        }
    }

    private Issue scrubDanglingEntry(@Nonnull IndexEntry indexEntry) {
        // Here: the index entry is dangling. Fix it (if allowed) and report the issue.
        final Tuple valueKey = indexEntry.getKey();
        return new Issue(
                KeyValueLogMessage.build("Scrubber: dangling index entry",
                        LogMessageKeys.KEY, valueKey,
                        LogMessageKeys.PRIMARY_KEY, indexEntry.getPrimaryKey()),
                FDBStoreTimer.Counts.INDEX_SCRUBBER_DANGLING_ENTRIES,
                null);
    }
}
