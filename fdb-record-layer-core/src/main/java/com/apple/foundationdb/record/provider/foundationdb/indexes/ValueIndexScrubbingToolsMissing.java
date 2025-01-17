/*
 * ValueIndexScrubbingToolsMissing.java
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

package com.apple.foundationdb.record.provider.foundationdb.indexes;

import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.IsolationLevel;
import com.apple.foundationdb.record.PipelineOperation;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorResult;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoredRecord;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainer;
import com.apple.foundationdb.record.provider.foundationdb.IndexScrubbingTools;
import com.apple.foundationdb.record.query.plan.RecordQueryPlanner;
import com.apple.foundationdb.record.query.plan.synthetic.SyntheticRecordFromStoredRecordPlan;
import com.apple.foundationdb.record.query.plan.synthetic.SyntheticRecordPlanner;
import com.apple.foundationdb.tuple.Tuple;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;


/**
 * Index Scrubbing Toolbox for a Value index maintainer. Scrub missing value index entries - i.e. detect record(s) that should
 * have had generated index entries, but these index entries cannot be found.
 */
public class ValueIndexScrubbingToolsMissing implements IndexScrubbingTools<FDBStoredRecord<Message>> {
    private Collection<RecordType> recordTypes = null;
    private Index index;
    private boolean allowRepair;
    private boolean isSynthetic;

    @Override
    public void presetCommonParams(Index index, boolean allowRepair, boolean isSynthetic, Collection<RecordType> types) {
        this.recordTypes = types;
        this.index = index;
        this.allowRepair = allowRepair;
        this.isSynthetic = isSynthetic;
    }

    @Override
    public RecordCursor<FDBStoredRecord<Message>> getCursor(final TupleRange tupleRange, final FDBRecordStore store, int limit) {
        final IsolationLevel isolationLevel = IsolationLevel.SNAPSHOT;
        final ExecuteProperties.Builder executeProperties = ExecuteProperties.newBuilder()
                .setIsolationLevel(isolationLevel)
                .setReturnedRowLimit(limit);

        final ScanProperties scanProperties = new ScanProperties(executeProperties.build(), false);
        return store.scanRecords(tupleRange, null, scanProperties);
    }

    @Override
    public Tuple getKeyFromCursorResult(final RecordCursorResult<FDBStoredRecord<Message>> result) {
        final FDBStoredRecord<Message> storedRecord = result.get();
        return storedRecord == null ? null : storedRecord.getPrimaryKey();
    }

    @Override
    @Nullable
    public CompletableFuture<Issue> handleOneItem(FDBRecordStore store,  final RecordCursorResult<FDBStoredRecord<Message>> result) {
        if (recordTypes == null || index == null) {
            throw new IllegalStateException("presetParams was not called appropriately for this scrubbing tool");
        }

        final FDBStoredRecord<Message> rec = result.get();
        if (rec == null || !recordTypes.contains(rec.getRecordType())) {
            return CompletableFuture.completedFuture(null);
        }

        return getMissingIndexKeys(store, rec)
                .thenApply(missingIndexesKeys -> {
                    if (missingIndexesKeys.isEmpty()) {
                        return null;
                    }
                    // Here: Oh, No! the index is missing!!
                    // (Maybe) report an error and (maybe) return this record to be index
                    return new Issue(
                            KeyValueLogMessage.build("Scrubber: missing index entry",
                                    LogMessageKeys.KEY, rec.getPrimaryKey().toString(),
                                    LogMessageKeys.INDEX_KEY, missingIndexesKeys.toString()),
                            FDBStoreTimer.Counts.INDEX_SCRUBBER_MISSING_ENTRIES,
                            allowRepair ? rec : null);
                });
    }

    private CompletableFuture<List<Tuple>> getMissingIndexKeys(FDBRecordStore store, FDBStoredRecord<Message> rec) {
        final IndexMaintainer maintainer = store.getIndexMaintainer(index);
        return indexEntriesForRecord(store, rec)
                .mapPipelined(indexEntry -> {
                    final Tuple valueKey = indexEntry.getKey();
                    final byte[] keyBytes = maintainer.getIndexSubspace().pack(valueKey);
                    return store.getContext().ensureActive().get(keyBytes).thenApply(indexVal -> indexVal == null ? valueKey : null);
                }, store.getPipelineSize(PipelineOperation.INDEX_TO_RECORD))
                .filter(Objects::nonNull)
                .asList();
    }

    @Nonnull
    private RecordCursor<IndexEntry> indexEntriesForRecord(@Nonnull FDBRecordStore store, @Nonnull FDBStoredRecord<Message> rec) {
        final IndexMaintainer maintainer = store.getIndexMaintainer(index);
        if (isSynthetic) {
            final RecordQueryPlanner queryPlanner =
                    new RecordQueryPlanner(store.getRecordMetaData(), store.getRecordStoreState().withWriteOnlyIndexes(Collections.singletonList(index.getName())));
            final SyntheticRecordPlanner syntheticPlanner = new SyntheticRecordPlanner(store, queryPlanner);
            SyntheticRecordFromStoredRecordPlan syntheticPlan = syntheticPlanner.forIndex(index);

            return RecordCursor.flatMapPipelined(
                    outerContinuation -> syntheticPlan.execute(store, rec),
                    (syntheticRecord, innerContinuation) -> {
                        final List<IndexEntry> entriesForSyntheticRecord = maintainer.filteredIndexEntries(syntheticRecord);
                        if (entriesForSyntheticRecord == null) {
                            return RecordCursor.empty();
                        } else {
                            return RecordCursor.fromList(store.getExecutor(), entriesForSyntheticRecord, innerContinuation)
                                    .map(entryNoPK -> rewriteWithPrimaryKey(entryNoPK, syntheticRecord));
                        }
                    },
                    null,
                    store.getPipelineSize(PipelineOperation.SYNTHETIC_RECORD_JOIN)
            );
        } else {
            List<IndexEntry> indexEntryNoPKs = maintainer.filteredIndexEntries(rec);
            if (indexEntryNoPKs == null) {
                return RecordCursor.empty();
            }
            return RecordCursor.fromList(store.getExecutor(), indexEntryNoPKs)
                    .map(entryNoPK -> rewriteWithPrimaryKey(entryNoPK, rec));
        }
    }

    @Nonnull
    private IndexEntry rewriteWithPrimaryKey(@Nonnull IndexEntry indexEntry, @Nonnull FDBRecord<? extends Message> rec) {
        return new IndexEntry(indexEntry.getIndex(), FDBRecordStoreBase.indexEntryKey(indexEntry.getIndex(), indexEntry.getKey(), rec.getPrimaryKey()), indexEntry.getValue(), rec.getPrimaryKey());
    }

}
