/*
 * LucenePartitioner.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2023 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.StreamingMode;
import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.async.AsyncIterable;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.KeyRange;
import com.apple.foundationdb.record.PipelineOperation;
import com.apple.foundationdb.record.RecordCoreArgumentException;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.cursors.ChainedCursor;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.lucene.directory.FDBDirectoryManager;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.record.metadata.expressions.FieldKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.GroupingKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBIndexableRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBIndexedRecord;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainerState;
import com.apple.foundationdb.record.provider.foundationdb.IndexOrphanBehavior;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.ScanComparisons;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.base.Verify;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer.Waits.WAIT_LOAD_LUCENE_PARTITION_METADATA;

/**
 * Manage partitioning info for a <b>logical</b>, partitioned lucene index, in which each partition is a separate physical lucene index.
 */
@API(API.Status.EXPERIMENTAL)
public class LucenePartitioner {
    private static final ConcurrentHashMap<String, KeyExpression> partitioningKeyExpressionCache = new ConcurrentHashMap<>();
    public static final int PARTITION_META_SUBSPACE = 0;
    public static final int PARTITION_DATA_SUBSPACE = 1;
    private final IndexMaintainerState state;
    private final boolean partitioningEnabled;
    private final String partitionTimestampFieldName;
    private final int indexPartitionHighWatermark;
    private final KeyExpression partitioningKeyExpression;

    public LucenePartitioner(@Nonnull IndexMaintainerState state) {
        this.state = state;
        partitionTimestampFieldName = state.index.getOption(LuceneIndexOptions.INDEX_PARTITION_BY_TIMESTAMP);
        this.partitioningEnabled = partitionTimestampFieldName != null;
        if (partitioningEnabled && (partitionTimestampFieldName.isEmpty() || partitionTimestampFieldName.isBlank())) {
            throw new RecordCoreArgumentException("Invalid partition timestamp field name", LogMessageKeys.FIELD_NAME, partitionTimestampFieldName);
        }
        String strIndexPartitionHighWatermark = state.index.getOption(LuceneIndexOptions.INDEX_PARTITION_HIGH_WATERMARK);
        indexPartitionHighWatermark = strIndexPartitionHighWatermark == null ?
                                          Objects.requireNonNull(state.context.getPropertyStorage().getPropertyValue(LuceneRecordContextProperties.LUCENE_PARTITION_HIGH_WATERMARK)) :
                                          Integer.parseInt(strIndexPartitionHighWatermark);
        this.partitioningKeyExpression = makePartitioningKeyExpression(partitionTimestampFieldName);
    }

    /**
     * make (and cache) a key expression from the partitioning field name.
     *
     * @param partitionTimestampFieldName partitioning field name
     * @return key expression
     */
    @Nullable
    private KeyExpression makePartitioningKeyExpression(@Nullable final String partitionTimestampFieldName) {
        if (partitionTimestampFieldName == null) {
            return null;
        }

        return partitioningKeyExpressionCache.computeIfAbsent(partitionTimestampFieldName, k -> {
            // here, partitionTimestampFieldName is not null/empty/blank
            String[] nameComponents = k.split("\\.");

            // nameComponents.length >= 1
            if (nameComponents.length == 1) {
                // no nesting
                return field(nameComponents[0]);
            }
            // nameComponents.length >= 2
            List<KeyExpression> fields = Arrays.stream(nameComponents).map(Key.Expressions::field).collect(Collectors.toList());
            for (int i = fields.size() - 1; i > 0; i--) {
                fields.set(i - 1, ((FieldKeyExpression) fields.get(i - 1)).nest(fields.get(i)));
            }
            return fields.get(0);
        });
    }

    /**
     * return the partition ID on which to run a query, given a grouping key.
     * For now, the most recent partition is returned.
     *
     * @param groupKey group key
     * @return partition id, or <code>null</code> if partitioning isn't enabled
     */
    @Nullable
    public Integer selectQueryPartitionId(@Nonnull Tuple groupKey) {
        if (isPartitioningEnabled()) {
            LucenePartitionInfoProto.LucenePartitionInfo partitionInfo = selectQueryPartition(groupKey);
            if (partitionInfo != null) {
                return partitionInfo.getId();
            }
        }
        return null;
    }

    /**
     * return the partition ID on which to run a query, given a grouping key.
     * For now, the most recent partition is returned.
     *
     * @param groupKey group key
     * @return partition, or <code>null</code> if partitioning isn't enabled or no partitioning metadata exist
     */
    @Nullable
    public LucenePartitionInfoProto.LucenePartitionInfo selectQueryPartition(@Nonnull Tuple groupKey) {
        return isPartitioningEnabled() ?
               state.context.asyncToSync(WAIT_LOAD_LUCENE_PARTITION_METADATA, getNewestPartition(groupKey))
               :
               null;
    }

    /**
     * get whether this index has partitioning enabled.
     *
     * @return true if partitioning is enabled
     */
    public boolean isPartitioningEnabled() {
        return partitioningEnabled;
    }

    /**
     * get the record field name that contains the document timestamp, which will be used to determine
     * partition assignment. The record field name may be qualified, when nested (e.g. <code>nestedRecord.fieldN</code>
     *
     * @return field name or <code>null</code>
     */
    @Nullable
    public String getPartitionTimestampFieldName() {
        return partitionTimestampFieldName;
    }

    /**
     * add a new written record to its partition metadata.
     *
     * @param newRecord record to be written
     * @param groupingKey grouping key
     * @param <M> message
     * @return partition id or <code>null</code> if partitioning isn't enabled on index
     */
    @Nullable
    public <M extends Message> Integer addToAndSavePartitionMetadata(@Nonnull FDBIndexableRecord<M> newRecord, @Nonnull Tuple groupingKey) {
        if (!isPartitioningEnabled()) {
            return null;
        }
        return addToAndSavePartitionMetadata(
                groupingKey,
                getPartitioningTimestampValue(newRecord));
    }

    /**
     * add a timestamp to the metadata of a given partition and save to db.
     * The <code>count</code> will be incremented, and the <code>from</code> or <code>to</code> timestamps will
     * be adjusted if applicable.
     *
     * @param groupKey  grouping key
     * @param timestamp document timestamp
     * @return assigned partition id
     */
    @Nonnull
    private Integer addToAndSavePartitionMetadata(@Nonnull final Tuple groupKey, @Nonnull final Long timestamp) {
        LucenePartitionInfoProto.LucenePartitionInfo assignedPartition =
                state.context.asyncToSync(WAIT_LOAD_LUCENE_PARTITION_METADATA, getOrCreatePartitionInfo(groupKey, timestamp));

        // assignedPartition is not null, since a new one is created by the previous call if none exist
        LucenePartitionInfoProto.LucenePartitionInfo.Builder builder = Objects.requireNonNull(assignedPartition).toBuilder();
        builder.setCount(assignedPartition.getCount() + 1);
        if (timestamp < getFrom(assignedPartition)) {
            // clear the previous key
            byte[] oldKey = partitionMetadataKeyFromTimestamp(groupKey, getFrom(assignedPartition));
            state.context.ensureActive().clear(oldKey);
            builder.setFrom(ByteString.copyFrom(Tuple.from(timestamp).pack()));
        }
        if (timestamp > getTo(assignedPartition)) {
            builder.setTo(ByteString.copyFrom(Tuple.from(timestamp).pack()));
        }
        savePartitionMetadata(groupKey, builder);
        return assignedPartition.getId();
    }

    /**
     * remove a deleted document from its partition metadata.
     *
     * @param oldRecord record to be deleted
     * @param groupingKey grouping key
     * @param <M> message
     * @return partition id or <code>null</code> if partitioning isn't enabled on index
     */
    @Nullable
    public <M extends Message> Integer removeFromAndSavePartitionMetadata(@Nonnull FDBIndexableRecord<M> oldRecord, @Nonnull Tuple groupingKey) {
        if (!isPartitioningEnabled()) {
            return null;
        }
        return removeFromAndSavePartitionMetadata(groupingKey, getPartitioningTimestampValue(oldRecord));
    }

    /**
     * remove a document from a partition metadata and save to db.
     * Note that only the document count is changed (decremented). <code>from</code> and <code>to</code> are unchanged, and remain valid.
     *
     * @param groupKey group key
     * @param timestamp removed document's timestamp
     * @return assigned partition id
     */
    @Nonnull
    private Integer removeFromAndSavePartitionMetadata(@Nonnull final Tuple groupKey, long timestamp) {
        LucenePartitionInfoProto.LucenePartitionInfo assignedPartition =
                state.context.asyncToSync(WAIT_LOAD_LUCENE_PARTITION_METADATA, getPartitionInfoOrFail(groupKey, timestamp));

        // assignedPartition is not null here, otherwise the call above would have thrown an exception
        LucenePartitionInfoProto.LucenePartitionInfo.Builder builder = Objects.requireNonNull(assignedPartition).toBuilder();
        // note that the to/from of the partition do not get updated, since that would require us to know what the next potential boundary
        // value(s) are. The values, nonetheless, remain valid.
        builder.setCount(assignedPartition.getCount() - 1);

        if (builder.getCount() < 0) {
            // should never happen
            throw new RecordCoreException("Issue updating Lucene partition metadata (resulting count < 0)", LogMessageKeys.PARTITION_ID, assignedPartition.getId());
        }
        savePartitionMetadata(groupKey, builder);
        return assignedPartition.getId();
    }

    /**
     * create a partition metadata key.
     *
     * @param groupKey group key
     * @param timestamp timestamp
     * @return partition metadata key
     */
    @Nonnull
    byte[] partitionMetadataKeyFromTimestamp(@Nonnull Tuple groupKey, long timestamp) {
        return state.indexSubspace.pack(Tuple.from(groupKey, PARTITION_META_SUBSPACE, timestamp));
    }

    /**
     * save partition metadata persistently.
     *
     * @param groupKey group key
     * @param builder builder instance
     */
    void savePartitionMetadata(@Nonnull Tuple groupKey, @Nonnull final LucenePartitionInfoProto.LucenePartitionInfo.Builder builder) {
        LucenePartitionInfoProto.LucenePartitionInfo updatedPartition = builder.build();
        state.context.ensureActive().set(
                partitionMetadataKeyFromTimestamp(groupKey, Tuple.fromBytes(builder.getFrom().toByteArray()).getLong(0)),
                updatedPartition.toByteArray());
    }

    @Nonnull
    CompletableFuture<LucenePartitionInfoProto.LucenePartitionInfo> findPartitionInfo(@Nonnull Tuple groupKey, long timestamp) {
        Range range = new Range(state.indexSubspace.subspace(Tuple.from(groupKey, PARTITION_META_SUBSPACE)).pack(),
                state.indexSubspace.subspace(Tuple.from(groupKey, PARTITION_META_SUBSPACE, timestamp)).pack());

        final AsyncIterable<KeyValue> rangeIterable = state.context.ensureActive().getRange(range, 1, true, StreamingMode.WANT_ALL);

        return AsyncUtil.collect(rangeIterable, state.context.getExecutor())
                .thenApply(targetPartition -> targetPartition.isEmpty() ? null : partitionInfoFromKV(targetPartition.get(0)));
    }

    /**
     * get or create the partition that should contain the given timestamp.
     *
     * @param groupKey group key
     * @param timestamp timestamp
     * @return partition metadata future
     */
    @Nonnull
    private CompletableFuture<LucenePartitionInfoProto.LucenePartitionInfo> getOrCreatePartitionInfo(@Nonnull Tuple groupKey, long timestamp) {
        return assignPartitionInternal(groupKey, timestamp, true);
    }

    /**
     * get the partition metadata containing the given timestamp or fail if not found.
     *
     * @param groupKey group key
     * @param timestamp timestamp
     * @return partition metadata future
     * @throws RecordCoreException if no partition is found
     */
    @Nonnull
    private CompletableFuture<LucenePartitionInfoProto.LucenePartitionInfo> getPartitionInfoOrFail(@Nonnull Tuple groupKey, long timestamp) {
        return assignPartitionInternal(groupKey, timestamp, false);
    }

    /**
     * assign a partition for a document insert or delete.
     *
     * @param groupKey group key
     * @param timestamp document timestamp
     * @param createIfNotExists if no suitable partition is found for this timestamp,
     *                          create when <code>true</code>. This parameter should be set to <code>true</code> when
     *                          inserting a document, and <code>false</code> when deleting.
     * @return partition metadata future
     * @throws RecordCoreException if <code>createIfNotExists</code> is <code>false</code> and no suitable partition is found
     */
    @Nonnull
    private CompletableFuture<LucenePartitionInfoProto.LucenePartitionInfo> assignPartitionInternal(@Nonnull Tuple groupKey, long timestamp, boolean createIfNotExists) {
        Range range = new Range(state.indexSubspace.subspace(Tuple.from(groupKey, PARTITION_META_SUBSPACE)).pack(),
                                state.indexSubspace.subspace(Tuple.from(groupKey, PARTITION_META_SUBSPACE, timestamp + 1)).pack());

        final AsyncIterable<KeyValue> rangeIterable = state.context.ensureActive().getRange(range, 1, true, StreamingMode.WANT_ALL);

        return AsyncUtil.collect(rangeIterable).thenComposeAsync(targetPartition -> {
            if (targetPartition.isEmpty()) {
                return getOldestPartition(groupKey).thenApply(oldestPartition -> {
                    if (oldestPartition == null) {
                        if (!createIfNotExists) {
                            throw new RecordCoreException("Partition metadata not found", LogMessageKeys.PARTITIONING_TIMESTAMP, timestamp);
                        } else {
                            return newPartitionMetadata(timestamp, 0);
                        }
                    } else {
                        return oldestPartition;
                    }
                });
            } else {
                return CompletableFuture.completedFuture(partitionInfoFromKV(targetPartition.get(0)));
            }
        });
    }

    /**
     * get the <code>long</code> timestamp value from a {@link FDBIndexableRecord}, given a field name.
     *
     * @param <M> record type
     * @param rec record
     * @return long if field is found
     * @throws RecordCoreException if no field of type <code>long</code> with given name is found
     */
    @Nonnull
    private <M extends Message> Long getPartitioningTimestampValue(@Nonnull FDBIndexableRecord<M> rec) {
        try {
            Key.Evaluated evaluatedKey = partitioningKeyExpression.evaluateSingleton(rec);
            if (evaluatedKey.size() == 1) {
                return evaluatedKey.getLong(0);
            }
        } catch (Exception e) {
            throw new RecordCoreException("invalid type for partitioning key", e);
        }
        // evaluatedKey.size() != 1
        throw new RecordCoreException("unexpected result when evaluating partition field");
    }


    /**
     * helper - create a new partition metadata instance.
     *
     * @return partition metadata instance
     */
    @Nonnull
    private LucenePartitionInfoProto.LucenePartitionInfo newPartitionMetadata(long timestamp, int id) {
        return LucenePartitionInfoProto.LucenePartitionInfo.newBuilder()
                .setCount(0)
                .setTo(ByteString.copyFrom(Tuple.from(timestamp).pack()))
                .setFrom(ByteString.copyFrom(Tuple.from(timestamp).pack()))
                .setId(id)
                .build();
    }

    /**
     * get most recent partition's info.
     *
     * @param groupKey group key
     * @return partition metadata future
     */
    @Nonnull
    private CompletableFuture<LucenePartitionInfoProto.LucenePartitionInfo> getNewestPartition(@Nonnull Tuple groupKey) {
        return getEdgePartition(groupKey, true);
    }

    /**
     * get oldest partition's info.
     *
     * @param groupKey group key
     * @return partition metadata future
     */
    @Nonnull
    private CompletableFuture<LucenePartitionInfoProto.LucenePartitionInfo> getOldestPartition(@Nonnull Tuple groupKey) {
        return getEdgePartition(groupKey, false);
    }

    /**
     * get either the oldest or the most recent partition's metadata.
     *
     * @param groupKey group key
     * @param reverse scan order, (get earliest if false, most recent if true)
     * @return partition metadata future
     */
    @Nonnull
    private CompletableFuture<LucenePartitionInfoProto.LucenePartitionInfo> getEdgePartition(@Nonnull Tuple groupKey, boolean reverse) {
        Range range = state.indexSubspace.subspace(Tuple.from(groupKey, PARTITION_META_SUBSPACE)).range();
        final AsyncIterable<KeyValue> rangeIterable = state.context.ensureActive().getRange(range, 1, reverse, StreamingMode.WANT_ALL);
        return AsyncUtil.collect(rangeIterable).thenApply(all -> all.isEmpty() ? null : partitionInfoFromKV(all.get(0)));
    }

    /**
     * helper - parse an instance of {@link LucenePartitionInfoProto.LucenePartitionInfo}
     * from a {@link KeyValue}.
     *
     * @param keyValue encoded key/value
     * @return partition metadata
     */
    @Nonnull
    static LucenePartitionInfoProto.LucenePartitionInfo partitionInfoFromKV(@Nonnull final KeyValue keyValue) {
        try {
            return LucenePartitionInfoProto.LucenePartitionInfo.parseFrom(keyValue.getValue());
        } catch (InvalidProtocolBufferException e) {
            throw new RecordCoreException(e);
        }
    }

    /**
     * helper - get the <code>long</code> {@link LucenePartitionInfoProto.LucenePartitionInfo#getFrom()} value from
     * the encoded byte string.
     *
     * @param partitionInfo partition metadata instance
     * @return long
     */
    public static long getFrom(@Nonnull LucenePartitionInfoProto.LucenePartitionInfo partitionInfo) {
        return Tuple.fromBytes(partitionInfo.getFrom().toByteArray()).getLong(0);
    }

    /**
     * helper - get the <code>long</code> {@link LucenePartitionInfoProto.LucenePartitionInfo#getTo()} value from
     * the encoded byte string.
     *
     * @param partitionInfo partition metadata instance
     * @return long
     */
    public static long getTo(@Nonnull LucenePartitionInfoProto.LucenePartitionInfo partitionInfo) {
        return Tuple.fromBytes(partitionInfo.getTo().toByteArray()).getLong(0);
    }

    /**
     * Re-balance full partitions, if applicable.
     *
     * @return void future
     */
    @Nonnull
    public CompletableFuture<Void> rebalancePartitions() {
        if (!isPartitioningEnabled()) {
            return AsyncUtil.DONE;
        }

        // This function will iterate the grouping keys
        final ScanProperties scanProperties = ScanProperties.FORWARD_SCAN.with(
                props -> props.clearState().setReturnedRowLimit(1));

        final Range range = state.indexSubspace.range();
        final KeyRange keyRange = new KeyRange(range.begin, range.end);
        final Subspace subspace = state.indexSubspace;
        final KeyExpression rootExpression = state.index.getRootExpression();

        if (! (rootExpression instanceof GroupingKeyExpression)) {
            return processPartitionRebalancing(Tuple.from());
        }

        GroupingKeyExpression expression = (GroupingKeyExpression) rootExpression;
        final int groupingCount = expression.getGroupingCount();

        try (RecordCursor<Tuple> cursor = new ChainedCursor<>(
                state.context,
                lastKey -> FDBDirectoryManager.nextTuple(state.context, subspace, keyRange, lastKey, scanProperties, groupingCount),
                Tuple::pack,
                Tuple::fromBytes,
                null,
                ScanProperties.FORWARD_SCAN)) {

            return cursor.map(tuple -> Tuple.fromItems(tuple.getItems().subList(0, groupingCount)))
                    .forEachAsync(this::processPartitionRebalancing, 1);
        }
    }

    /**
     * Re-balance the first partition in a given grouping key by moving documents out of it.
     *
     * Note that in order to finish the task within the bounds of a single transaction, only the first
     * partition needing re-balancing will be processed. If there are other partitions that need re-balancing,
     * they will be processed during subsequent calls.
     *
     * @param groupingKey grouping key
     * @return void future
     */
    @Nonnull
    private CompletableFuture<Void> processPartitionRebalancing(@Nonnull final Tuple groupingKey) {

        return getAllPartitionMetaInfo(groupingKey).thenCompose(partitionInfos -> {
            // need to track the next partition id to use when creating a new one
            int maxPartitionId = partitionInfos.stream().map(LucenePartitionInfoProto.LucenePartitionInfo::getId).max(Integer::compare).orElse(0);

            for (LucenePartitionInfoProto.LucenePartitionInfo partitionInfo : partitionInfos) {
                if (partitionInfo.getCount() > indexPartitionHighWatermark) {
                    // process one partition
                    return moveDocsFromPartition(partitionInfo, groupingKey, maxPartitionId, getOldestNDocuments(partitionInfo, groupingKey,
                            Objects.requireNonNull(state.context.getPropertyStorage().getPropertyValue(LuceneRecordContextProperties.LUCENE_REPARTITION_DOCUMENT_COUNT)) + 1));
                }
            }
            // here: no partitions need re-balancing
            return AsyncUtil.DONE;
        });
    }

    /**
     * get the oldest N index entries in a given Lucene partition.
     *
     * @param partitionInfo partition metadata
     * @param groupingKey grouping key
     * @param count count of index entries to return
     * @return cursor over the N (or fewer) oldest index entries
     */
    @Nonnull
    public LuceneRecordCursor getOldestNDocuments(@Nonnull final LucenePartitionInfoProto.LucenePartitionInfo partitionInfo,
                                                  @Nonnull final Tuple groupingKey,
                                                  int count) {
        final var fieldInfos = LuceneIndexExpressions.getDocumentFieldDerivations(state.index, state.store.getRecordMetaData());
        LuceneScanParameters scan = new LuceneScanQueryParameters(
                Verify.verifyNotNull(ScanComparisons.from(new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, groupingKey.get(0)))),
                new LuceneQuerySearchClause(LuceneQueryType.QUERY, "*:*", false),
                new Sort(new SortField(partitionTimestampFieldName, SortField.Type.LONG, false)),
                null,
                null,
                null);
        ScanProperties scanProperties = ExecuteProperties.newBuilder().setReturnedRowLimit(count).build().asScanProperties(false);
        LuceneScanQuery scanQuery = (LuceneScanQuery) scan.bind(state.store, state.index, EvaluationContext.EMPTY);

        // we create the cursor here explicitly (vs. e.g. calling state.store.scanIndex(...)) because we want the search
        // to be performed specifically in the provided partition.
        // alternatively we can include a partitionInfo in the lucene scan parameters--tbd
        try (LuceneRecordCursor cursor = new LuceneRecordCursor(state.context.getExecutor(),
                state.context.getPropertyStorage().getPropertyValue(LuceneRecordContextProperties.LUCENE_EXECUTOR_SERVICE),
                Objects.requireNonNull(state.context.getPropertyStorage().getPropertyValue(LuceneRecordContextProperties.LUCENE_INDEX_CURSOR_PAGE_SIZE)),
                scanProperties, state, scanQuery.getQuery(), scanQuery.getSort(), null,
                scanQuery.getGroupKey(), partitionInfo, scanQuery.getLuceneQueryHighlightParameters(), scanQuery.getTermMap(),
                scanQuery.getStoredFields(), scanQuery.getStoredFieldTypes(),
                LuceneAnalyzerRegistryImpl.instance().getLuceneAnalyzerCombinationProvider(state.index, LuceneAnalyzerType.FULL_TEXT, fieldInfos),
                LuceneAnalyzerRegistryImpl.instance().getLuceneAnalyzerCombinationProvider(state.index, LuceneAnalyzerType.AUTO_COMPLETE, fieldInfos))) {

            return cursor;
        }
    }

    /**
     * Move documents from one Lucene partition to another.
     *
     * @param partitionInfo partition to move documents from
     * @param groupingKey grouping key
     * @param maxPartitionId current max partition id
     * @param cursor documents to move
     * @return void future
     */
    @Nonnull
    private CompletableFuture<Void> moveDocsFromPartition(@Nonnull final LucenePartitionInfoProto.LucenePartitionInfo partitionInfo,
                                                          @Nonnull final Tuple groupingKey,
                                                          final int maxPartitionId,
                                                          @Nonnull final LuceneRecordCursor cursor) {
        Collection<RecordType> recordTypes = state.store.getRecordMetaData().recordTypesForIndex(state.index);
        if (recordTypes.stream().map(RecordType::isSynthetic).distinct().count() > 1) {
            // don't support mix of synthetic/regular
            throw new RecordCoreException("mix of synthetic and non-synthetic record types in index is not supported");
        }

        return (recordTypes.iterator().next().isSynthetic() ?
            cursor.mapPipelined(indexEntry -> state.store.loadSyntheticRecord(indexEntry.getPrimaryKey()),
                state.store.getPipelineSize(PipelineOperation.INDEX_TO_RECORD)).asList()
                :
                state.store.fetchIndexRecords(cursor, IndexOrphanBehavior.SKIP).map(FDBIndexedRecord::getStoredRecord).asList())
            .thenCompose(records -> {
                if (records.size() > 0) {
                    // the newest record is the one we intend to leave in the current partition; we need it in order to set this partition's new
                    // `from` value.
                    final long newBoundaryTimestamp = getPartitioningTimestampValue(records.get(records.size() - 1));

                    // remove the (n + 1)th record from the records to be moved
                    records.remove(records.size() - 1);

                    LuceneIndexMaintainer indexMaintainer = (LuceneIndexMaintainer) state.store.getIndexMaintainer(state.index);
                    // shortcut delete docs from current partition
                    // (we do this, instead of calling LuceneIndexMaintainer.update() in order to avoid a chicken-and-egg
                    // situation with the partition metadata keys.
                    records.forEach(r -> {
                        try {
                            indexMaintainer.deleteDocument(groupingKey, partitionInfo.getId(), r.getPrimaryKey());
                        } catch (IOException e) {
                            throw new RecordCoreException(e);
                        }
                    });

                    // update current partition's meta
                    state.context.ensureActive().clear(partitionMetadataKeyFromTimestamp(groupingKey, getFrom(partitionInfo)));
                    LucenePartitionInfoProto.LucenePartitionInfo.Builder builder = partitionInfo.toBuilder()
                            .setCount(partitionInfo.getCount() - records.size())
                            .setFrom(ByteString.copyFrom(Tuple.from(newBoundaryTimestamp).pack()));
                    savePartitionMetadata(groupingKey, builder);

                    // value of the "destination" partition's `from` timestamp
                    final long overflowPartitionFromTimestamp = getPartitioningTimestampValue(records.get(0));
                    return findPartitionInfo(groupingKey, overflowPartitionFromTimestamp).thenCompose(previousPartition -> {
                        if (previousPartition == null || previousPartition.getCount() + records.size() > indexPartitionHighWatermark || previousPartition.getId() == partitionInfo.getId()) {
                            // create a new "overflow" partition
                            savePartitionMetadata(groupingKey, newPartitionMetadata(overflowPartitionFromTimestamp, maxPartitionId + 1).toBuilder());
                        }

                        Iterator<? extends FDBIndexableRecord<Message>> recordIterator = records.iterator();
                        return AsyncUtil.whileTrue(() -> indexMaintainer.update(null, recordIterator.next())
                                .thenApply(ignored -> recordIterator.hasNext()));
                    });
                }
                return AsyncUtil.DONE;
            });
    }

    /**
     * Get all partition metadata for a given grouping key.
     *
     * @param groupingKey grouping key
     * @return future list of partition metadata
     */
    private CompletableFuture<List<LucenePartitionInfoProto.LucenePartitionInfo>> getAllPartitionMetaInfo(@Nonnull final Tuple groupingKey) {
        Range range = state.indexSubspace.subspace(Tuple.from(groupingKey, PARTITION_META_SUBSPACE)).range();
        final AsyncIterable<KeyValue> rangeIterable = state.context.ensureActive().getRange(range, Integer.MAX_VALUE, true, StreamingMode.WANT_ALL);
        return AsyncUtil.collect(rangeIterable).thenApply(all -> all.stream().map(LucenePartitioner::partitionInfoFromKV).collect(Collectors.toList()));
    }
}
