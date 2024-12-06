/*
 * ValueIndexScrubbingToolsDangling.java
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

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.IsolationLevel;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorResult;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.NestedRecordType;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.record.metadata.SyntheticRecordType;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.IndexOrphanBehavior;
import com.apple.foundationdb.record.provider.foundationdb.IndexScrubbingTools;
import com.apple.foundationdb.tuple.Tuple;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Index Scrubbing Toolbox for a Value index maintainer. Scrub dangling value index entries - i.e. index entries
 * pointing to non-existing record(s)
 */
public class ValueIndexScrubbingToolsDangling implements IndexScrubbingTools<IndexEntry> {
    private Index index = null;
    private boolean allowRepair;
    private boolean isSynthetic;

    @Override
    public void presetCommonParams(final Index index, final boolean allowRepair, final boolean isSynthetic, final Collection<RecordType> typesIgnored) {
        this.index = index;
        this.allowRepair = allowRepair;
        this.isSynthetic = isSynthetic;
    }

    @Override
    public RecordCursor<IndexEntry> getCursor(final TupleRange range, final FDBRecordStore store, final int limit) {
        // IsolationLevel.SNAPSHOT will not cause range conflicts, which is ok because this scrubbing is - by definition - idempotent.
        //IIf a repair is made, any related component (in this case - index entries) should be explicitly added to the conflict list.
        final IsolationLevel isolationLevel = IsolationLevel.SNAPSHOT;
        final ExecuteProperties.Builder executeProperties = ExecuteProperties.newBuilder()
                .setIsolationLevel(isolationLevel)
                .setReturnedRowLimit(limit);

        final ScanProperties scanProperties = new ScanProperties(executeProperties.build(), false);
        return store.scanIndex(index, IndexScanType.BY_VALUE, range, null, scanProperties);
    }

    @Override
    public Tuple getKeyFromCursorResult(final RecordCursorResult<IndexEntry> result) {
        final IndexEntry indexEntry = result.get();
        return indexEntry == null ? null : indexEntry.getKey();
    }

    @Override
    public CompletableFuture<Issue> handleOneItem(final FDBRecordStore store, final Transaction transaction, final RecordCursorResult<IndexEntry> result) {
        if (index == null) {
            throw new InternalError("presetParams was not called appropriately for this scrubbing tool");
        }

        final IndexEntry indexEntry = result.get();
        if (indexEntry == null) {
            return CompletableFuture.completedFuture(null);
        }

        if (isSynthetic) {
            return store.loadSyntheticRecord(indexEntry.getPrimaryKey()).thenApply(syntheticRecord -> {
                if (syntheticRecord.getConstituents().isEmpty()) {
                    // None of the constituents of this synthetic type are present, so it must be dangling
                    List<Tuple> primaryKeysForConflict = new ArrayList<>(indexEntry.getPrimaryKey().size() - 1);
                    SyntheticRecordType<?> syntheticRecordType = store.getRecordMetaData().getSyntheticRecordTypeFromRecordTypeKey(indexEntry.getPrimaryKey().get(0));
                    for (int i = 0; i < syntheticRecordType.getConstituents().size(); i++) {
                        if (!(syntheticRecordType.getConstituents().get(i).getRecordType() instanceof NestedRecordType) && indexEntry.getPrimaryKey().get(i + 1) != null) {
                            primaryKeysForConflict.add(indexEntry.getPrimaryKey().getNestedTuple(i + 1));
                        }
                    }
                    return scrubDanglingEntry(store, indexEntry, primaryKeysForConflict);
                }
                return null;
            });
        } else {
            return store.loadIndexEntryRecord(indexEntry, IndexOrphanBehavior.RETURN).thenApply(indexedRecord -> {
                if (!indexedRecord.hasStoredRecord()) {
                    // Here: Oh, No! this index is dangling!
                    return scrubDanglingEntry(store, indexEntry, List.of(indexEntry.getPrimaryKey()));
                }
                return null;
            });
        }
    }

    private Issue scrubDanglingEntry(@Nonnull FDBRecordStore store, @Nonnull IndexEntry indexEntry, @Nonnull List<Tuple> conflictPrimaryKeys) {
        // Here: the index entry is dangling. Fix it (if allowed) and report the issue.
        final Tuple valueKey = indexEntry.getKey();

        if (allowRepair) {
            // remove this index entry
            // Note that we can't add the missing record to the conflict list, so we'll add the index entries.
            for (Tuple primaryKey : conflictPrimaryKeys) {
                store.addRecordReadConflict(primaryKey);
            }
            final byte[] keyBytes = store.indexSubspace(index).pack(valueKey);
            store.getContext().ensureActive().clear(keyBytes);
        }

        return new Issue(
                KeyValueLogMessage.build("Scrubber: dangling index entry",
                        LogMessageKeys.KEY, valueKey,
                        LogMessageKeys.PRIMARY_KEY, indexEntry.getPrimaryKey()),
                FDBStoreTimer.Counts.INDEX_SCRUBBER_DANGLING_ENTRIES,
                null);
    }
}
