/*
 * LuceneIndexScrubbingToolsMissing.java
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

import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.IsolationLevel;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorResult;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoredRecord;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainerState;
import com.apple.foundationdb.record.provider.foundationdb.IndexScrubbingTools;
import com.apple.foundationdb.tuple.Tuple;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class LuceneIndexScrubbingToolsMissing implements IndexScrubbingTools<FDBStoredRecord<Message>> {
    private Collection<RecordType> recordTypes = null;
    private Index index;
    private boolean isSynthetic;

    @Nonnull
    private final LucenePartitioner partitioner; // non-mutable
    @Nonnull
    private final IndexMaintainerState state;

    public LuceneIndexScrubbingToolsMissing(@Nonnull final LucenePartitioner partitioner, @Nonnull final IndexMaintainerState state) {
        this.partitioner = partitioner;
        this.state = state;
    }


    @Override
    public void presetCommonParams(Index index, boolean allowRepair, boolean isSynthetic, Collection<RecordType> types) {
        this.recordTypes = types;
        this.index = index;
        this.isSynthetic = isSynthetic;
    }

    @Override
    public RecordCursor<FDBStoredRecord<Message>> getCursor(final TupleRange range, final FDBRecordStore store, final int limit) {
        final IsolationLevel isolationLevel = IsolationLevel.SNAPSHOT;
        final ExecuteProperties.Builder executeProperties = ExecuteProperties.newBuilder()
                .setIsolationLevel(isolationLevel)
                .setReturnedRowLimit(limit);

        final ScanProperties scanProperties = new ScanProperties(executeProperties.build(), false);
        return store.scanRecords(range, null, scanProperties);
    }

    @Override
    public Tuple getKeyFromCursorResult(final RecordCursorResult<FDBStoredRecord<Message>> result) {
        final FDBStoredRecord<Message> storedRecord = result.get();
        return storedRecord == null ? null : storedRecord.getPrimaryKey();
    }

    @Override
    public CompletableFuture<Issue> handleOneItem(final FDBRecordStore store, final RecordCursorResult<FDBStoredRecord<Message>> result) {
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
                            null);
                });
    }

    private CompletableFuture<List<Tuple>> getMissingIndexKeys(FDBRecordStore store, FDBStoredRecord<Message> rec) {
        // follow the logic of LuceneIndexMaintainer::tryDelete
        


        return CompletableFuture.completedFuture(null);
    }
}
