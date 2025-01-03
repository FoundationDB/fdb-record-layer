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

import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.IsolationLevel;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorResult;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.lucene.directory.FDBDirectoryManager;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoredRecord;
import com.apple.foundationdb.record.provider.foundationdb.IndexScrubbingTools;
import com.apple.foundationdb.record.util.pair.Pair;
import com.apple.foundationdb.tuple.Tuple;
import com.google.protobuf.Message;
import org.apache.lucene.index.DirectoryReader;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * Index Scrubbing Toolbox for a Lucene index maintainer. Scrub missing value index entries - i.e. detect record(s) that should
 * cannot be found in the segment index.
 */
public class LuceneIndexScrubbingToolsMissing implements IndexScrubbingTools<FDBStoredRecord<Message>> {
    private Collection<RecordType> recordTypes = null;
    private Index index;

    @Nonnull
    private final LucenePartitioner partitioner;
    @Nonnull
    private final FDBDirectoryManager directoryManager;
    @Nonnull
    private final LuceneAnalyzerCombinationProvider indexAnalyzerSelector;

    public LuceneIndexScrubbingToolsMissing(@Nonnull LucenePartitioner partitioner, @Nonnull FDBDirectoryManager directoryManager,
                                            @Nonnull LuceneAnalyzerCombinationProvider indexAnalyzerSelector) {
        this.partitioner = partitioner;
        this.directoryManager = directoryManager;
        this.indexAnalyzerSelector = indexAnalyzerSelector;
    }


    @Override
    public void presetCommonParams(Index index, boolean allowRepair, boolean isSynthetic, Collection<RecordType> types) {
        this.recordTypes = types;
        this.index = index;
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

    /**
     * Provide a lucene specific reason for detecting a "missing" index entry.
     */
    public enum MissingIndexReason {
        NOT_IN_PARTITION,
        NOT_IN_PK_SEGMENT_INDEX,
        EMPTY_RECORDS_FIELDS,
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

        return detectMissingIndexKeys(rec)
                .thenApply(missingIndexesKeys -> {
                    if (missingIndexesKeys == null) {
                        return null;
                    }
                    // Here: Oh, No! the index is missing!!
                    // (Maybe) report an error
                    return new Issue(
                            KeyValueLogMessage.build("Scrubber: missing index entry",
                                    LogMessageKeys.KEY, rec.getPrimaryKey(),
                                    LogMessageKeys.GROUPING_KEY, missingIndexesKeys.getValue(),
                                    LogMessageKeys.REASON, missingIndexesKeys.getKey()),
                            FDBStoreTimer.Counts.INDEX_SCRUBBER_MISSING_ENTRIES,
                            null);
                });
    }

    public CompletableFuture<Pair<MissingIndexReason, Tuple>> detectMissingIndexKeys(FDBStoredRecord<Message> rec) {
        // return the first missing (if any).
        final KeyExpression root = index.getRootExpression();
        final Map<Tuple, List<LuceneDocumentFromRecord.DocumentField>> recordFields = LuceneDocumentFromRecord.getRecordFields(root, rec);
        if (recordFields.isEmpty()) {
            // Could recordFields be an empty map?
            return CompletableFuture.completedFuture(Pair.of(MissingIndexReason.EMPTY_RECORDS_FIELDS, null));
        }
        if (recordFields.size() == 1) {
            // A single grouping key
            return checkMissingIndexKey(rec, recordFields.keySet().stream().findFirst().get());
        }

        // Here: more than one grouping key
        final Map<Tuple, MissingIndexReason> keys = Collections.synchronizedMap(new HashMap<>());
        return AsyncUtil.whenAll( recordFields.keySet().stream().map(groupingKey ->
                        checkMissingIndexKey(rec, groupingKey)
                                .thenApply(missing -> keys.put(missing.getValue(), missing.getKey()))
                ).collect(Collectors.toList()))
                .thenApply(ignore -> {
                    final Optional<Map.Entry<Tuple, MissingIndexReason>> first = keys.entrySet().stream().findFirst();
                    return first.map(tupleStringEntry -> Pair.of(tupleStringEntry.getValue(), tupleStringEntry.getKey())).orElse(null);
                });
    }

    private CompletableFuture<Pair<MissingIndexReason, Tuple>> checkMissingIndexKey(FDBStoredRecord<Message> rec, Tuple groupingKey) {
        if (!partitioner.isPartitioningEnabled()) {
            return CompletableFuture.completedFuture(
                    isMissingIndexKey(rec, null, groupingKey) ?
                    Pair.of(MissingIndexReason.NOT_IN_PK_SEGMENT_INDEX, null) :
                    null);
        }
        return partitioner.tryGetPartitionInfo(rec, groupingKey).thenApply(partitionInfo -> {
            if (partitionInfo == null) {
                return Pair.of(MissingIndexReason.NOT_IN_PARTITION, groupingKey);
            }
            if (isMissingIndexKey(rec, partitionInfo.getId(), groupingKey)) {
                return Pair.of(MissingIndexReason.NOT_IN_PK_SEGMENT_INDEX, groupingKey);
            }
            return null;
        });
    }


    private boolean isMissingIndexKey(FDBStoredRecord<Message> rec, Integer partitionId, Tuple groupingKey) {
        @Nullable final LucenePrimaryKeySegmentIndex segmentIndex = directoryManager.getDirectory(groupingKey, partitionId).getPrimaryKeySegmentIndex();
        if (segmentIndex == null) {
            // Here: iternal error, getIndexScrubbingTools should have indicated that scrub missing is not supported.
            throw new IllegalStateException("This scrubber should not have been used");
        }

        try {
            // TODO: this is called to initilize the writer, else we get an exception at getDirectoryReader. Should it really be done for a RO operation?
            directoryManager.getIndexWriter(groupingKey, partitionId, indexAnalyzerSelector.provideIndexAnalyzer(""));
        } catch (IOException e) {
            throw LuceneExceptions.toRecordCoreException("failed getIndexWriter", e);
        }
        try {
            DirectoryReader directoryReader = directoryManager.getDirectoryReader(groupingKey, partitionId);
            final LucenePrimaryKeySegmentIndex.DocumentIndexEntry documentIndexEntry = segmentIndex.findDocument(directoryReader, rec.getPrimaryKey());
            if (documentIndexEntry == null) {
                // Here: the document had not been found in the PK segment index
                return true;
            }
        } catch (IOException ex) {
            // Here: probably an fdb exception. Unwrap and rethrow.
            throw LuceneExceptions.toRecordCoreException("Error while finding document", ex);
        }
        return false;
    }

}
