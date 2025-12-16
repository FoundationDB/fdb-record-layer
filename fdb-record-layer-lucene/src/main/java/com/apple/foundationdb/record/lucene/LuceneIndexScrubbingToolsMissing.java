/*
 * LuceneIndexScrubbingToolsMissing.java
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

package com.apple.foundationdb.record.lucene;

import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorResult;
import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.lucene.directory.FDBDirectoryManager;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBIndexableRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoredRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBSyntheticRecord;
import com.apple.foundationdb.record.provider.foundationdb.indexes.ValueIndexScrubbingToolsMissing;
import com.apple.foundationdb.record.query.plan.RecordQueryPlanner;
import com.apple.foundationdb.record.query.plan.synthetic.SyntheticRecordFromStoredRecordPlan;
import com.apple.foundationdb.record.query.plan.synthetic.SyntheticRecordPlanner;
import com.apple.foundationdb.record.util.pair.Pair;
import com.apple.foundationdb.tuple.Tuple;
import com.google.protobuf.Message;
import org.apache.lucene.index.DirectoryReader;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * Index Scrubbing Toolbox for a Lucene index maintainer. Scrub missing value index entries - i.e. detect record(s) that should
 * have been indexed, but cannot be found in the segment index.
 */
public class LuceneIndexScrubbingToolsMissing extends ValueIndexScrubbingToolsMissing {
    private Collection<RecordType> recordTypes = null;
    private Index index;
    private boolean isSynthetic;

    @Nonnull
    private final LucenePartitioner partitioner;
    @Nonnull
    private final FDBDirectoryManager directoryManager;
    @Nonnull
    private final LuceneIndexMaintainer indexMaintainer;

    public LuceneIndexScrubbingToolsMissing(@Nonnull LucenePartitioner partitioner, @Nonnull FDBDirectoryManager directoryManager, @Nonnull LuceneIndexMaintainer indexMaintainer) {
        this.partitioner = partitioner;
        this.directoryManager = directoryManager;
        this.indexMaintainer = indexMaintainer;
    }


    @Override
    public void presetCommonParams(Index index, boolean allowRepair, boolean isSynthetic, Collection<RecordType> types) {
        this.recordTypes = types;
        this.index = index;
        this.isSynthetic = isSynthetic;
        // call super, but force allowRepair as false
        super.presetCommonParams(index, false, isSynthetic, types);
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
    @Nullable
    public CompletableFuture<Issue> handleOneItem(final FDBRecordStore store, final RecordCursorResult<FDBStoredRecord<Message>> result) {
        if (recordTypes == null || index == null) {
            throw new IllegalStateException("presetParams was not called appropriately for this scrubbing tool");
        }

        final FDBStoredRecord<Message> rec = result.get();
        if (!shouldHandleItem(rec)) {
            return CompletableFuture.completedFuture(null);
        }

        return detectMissingIndexKeys(store, rec)
                .thenApply(missingIndexesKeys -> {
                    if (missingIndexesKeys == null) {
                        return null;
                    }
                    // Here: Oh, No! an index entry is missing!!
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

    private boolean shouldHandleItem(FDBStoredRecord<Message> rec) {
        if (rec == null || !recordTypes.contains(rec.getRecordType())) {
            return false;
        }
        return indexMaintainer.maybeFilterRecord(rec) != null;
    }

    @SuppressWarnings("PMD.CloseResource")
    private CompletableFuture<Pair<MissingIndexReason, Tuple>> detectMissingIndexKeys(final FDBRecordStore store, FDBStoredRecord<Message> rec) {
        // Generate synthetic record (if applicable) and return the first detected missing (if any).
        final AtomicReference<Pair<MissingIndexReason, Tuple>> issue = new AtomicReference<>();

        if (!isSynthetic) {
            return checkMissingIndexKey(rec, issue).thenApply(ignore -> issue.get());
        }
        final RecordQueryPlanner queryPlanner =
                new RecordQueryPlanner(store.getRecordMetaData(), store.getRecordStoreState().withWriteOnlyIndexes(Collections.singletonList(index.getName())));
        final SyntheticRecordPlanner syntheticPlanner = new SyntheticRecordPlanner(store, queryPlanner);
        SyntheticRecordFromStoredRecordPlan syntheticPlan = syntheticPlanner.forIndex(index);
        final RecordCursor<FDBSyntheticRecord> recordCursor = syntheticPlan.execute(store, rec);

        return AsyncUtil.whenAll(
                recordCursor.asStream().map(syntheticRecord -> checkMissingIndexKey(syntheticRecord, issue))
                        .collect(Collectors.toList()))
                .whenComplete((ret, e) -> recordCursor.close())
                .thenApply(ignore -> issue.get());

    }

    private CompletableFuture<Void> checkMissingIndexKey(FDBIndexableRecord<Message> rec,
                                                         AtomicReference<Pair<MissingIndexReason, Tuple>> issue) {
        // Iterate grouping keys (if any) and detect missing index entry (if any)
        final KeyExpression root = index.getRootExpression();
        final Map<Tuple, List<LuceneDocumentFromRecord.DocumentField>> recordFields = LuceneDocumentFromRecord.getRecordFields(root, rec);
        if (recordFields.isEmpty()) {
            // recordFields should not be an empty map
            issue.compareAndSet(null, Pair.of(MissingIndexReason.EMPTY_RECORDS_FIELDS, null));
            return AsyncUtil.DONE;
        }
        if (recordFields.size() == 1) {
            // A single grouping key, simple check.
            return checkMissingIndexKey(rec, recordFields.keySet().iterator().next(), issue);
        }

        // Here: more than one grouping key, declare an issue if at least one of them is missing
        return AsyncUtil.whenAll( recordFields.keySet().stream().map(groupingKey ->
                        checkMissingIndexKey(rec, groupingKey, issue)
                ).collect(Collectors.toList()))
                .thenApply(ignore -> null);
    }

    private CompletableFuture<Void> checkMissingIndexKey(FDBIndexableRecord<Message> rec, Tuple groupingKey, AtomicReference<Pair<MissingIndexReason, Tuple>> issue) {
        // Get partition (if applicable) and detect missing index entry (if any)
        if (!partitioner.isPartitioningEnabled()) {
            if (isMissingIndexKey(rec, null, groupingKey)) {
                issue.compareAndSet(null, Pair.of(MissingIndexReason.NOT_IN_PK_SEGMENT_INDEX, null));
            }
            return AsyncUtil.DONE;
        }
        return partitioner.tryGetPartitionInfo(rec, groupingKey).thenAccept(partitionInfo -> {
            if (partitionInfo == null) {
                issue.compareAndSet(null, Pair.of(MissingIndexReason.NOT_IN_PARTITION, groupingKey));
            } else if (isMissingIndexKey(rec, partitionInfo.getId(), groupingKey)) {
                issue.compareAndSet(null, Pair.of(MissingIndexReason.NOT_IN_PK_SEGMENT_INDEX, groupingKey));
            }
        });
    }

    @SuppressWarnings("PMD.CloseResource")
    private boolean isMissingIndexKey(FDBIndexableRecord<Message> rec, Integer partitionId, Tuple groupingKey) {
        @Nullable final LucenePrimaryKeySegmentIndex segmentIndex = directoryManager.getDirectory(groupingKey, partitionId).getPrimaryKeySegmentIndex();
        if (segmentIndex == null) {
            // Here: internal error, getIndexScrubbingTools should have indicated that scrub missing is not supported.
            throw new IllegalStateException("LucneIndexScrubbingToolsMissing without a LucenePrimaryKeySegmentIndex");
        }

        try {
            // TODO: this is called to initialize the writer, else we get an exception at getDirectoryReader. Should it really be done for a RO operation?
            directoryManager.getIndexWriter(groupingKey, partitionId);
        } catch (IOException e) {
            throw LuceneExceptions.toRecordCoreException("failed getIndexWriter", e);
        }
        try {
            DirectoryReader directoryReader = directoryManager.getWriterReader(groupingKey, partitionId);
            final LucenePrimaryKeySegmentIndex.DocumentIndexEntry documentIndexEntry = segmentIndex.findDocument(directoryReader, rec.getPrimaryKey());
            if (documentIndexEntry == null) {
                // Here: the document had not been found in the PK segment index
                return true;
            }
        } catch (IOException ex) {
            // Here: an unexpected exception. Unwrap and rethrow.
            throw LuceneExceptions.toRecordCoreException("Error while finding document", ex);
        }
        return false;
    }

}
