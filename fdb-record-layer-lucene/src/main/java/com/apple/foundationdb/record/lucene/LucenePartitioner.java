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
import com.apple.foundationdb.record.EndpointType;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.KeyRange;
import com.apple.foundationdb.record.PipelineOperation;
import com.apple.foundationdb.record.RecordCoreArgumentException;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorContinuation;
import com.apple.foundationdb.record.RecordCursorEndContinuation;
import com.apple.foundationdb.record.RecordCursorStartContinuation;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.cursors.ChainedCursor;
import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.lucene.directory.FDBDirectoryManager;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.record.metadata.expressions.FieldKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.GroupingKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBIndexableRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBIndexedRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainerState;
import com.apple.foundationdb.record.provider.foundationdb.IndexOrphanBehavior;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.ScanComparisons;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * Manage partitioning info for a <b>logical</b>, partitioned lucene index, in which each partition is a separate physical lucene index.
 */
@API(API.Status.EXPERIMENTAL)
public class LucenePartitioner {

    private static final FDBStoreTimer.Waits WAIT_LOAD_LUCENE_PARTITION_METADATA = FDBStoreTimer.Waits.WAIT_LOAD_LUCENE_PARTITION_METADATA;
    private static final Logger LOGGER = LoggerFactory.getLogger(LucenePartitioner.class);
    private static final int DEFAULT_PARTITION_HIGH_WATERMARK = 400_000;
    private static final ConcurrentHashMap<String, KeyExpression> partitioningKeyExpressionCache = new ConcurrentHashMap<>();
    public static final int PARTITION_META_SUBSPACE = 0;
    public static final int PARTITION_DATA_SUBSPACE = 1;
    private final IndexMaintainerState state;
    private final boolean partitioningEnabled;
    private final String partitionFieldNameInLucene;
    private final int indexPartitionHighWatermark;
    private final KeyExpression partitioningKeyExpression;

    public LucenePartitioner(@Nonnull IndexMaintainerState state) {
        this.state = state;
        String partitionFieldName = state.index.getOption(LuceneIndexOptions.INDEX_PARTITION_BY_FIELD_NAME);
        this.partitioningEnabled = partitionFieldName != null;
        if (partitioningEnabled && (partitionFieldName.isEmpty() || partitionFieldName.isBlank())) {
            throw new RecordCoreArgumentException("Invalid partition field name", LogMessageKeys.FIELD_NAME, partitionFieldName);
        }
        // partition field name in lucene, when nested, has `_` in place of `.`
        partitionFieldNameInLucene = partitionFieldName == null ? null : partitionFieldName.replace('.', '_');

        String strIndexPartitionHighWatermark = state.index.getOption(LuceneIndexOptions.INDEX_PARTITION_HIGH_WATERMARK);
        indexPartitionHighWatermark = strIndexPartitionHighWatermark == null ?
                                      DEFAULT_PARTITION_HIGH_WATERMARK :
                                      Integer.parseInt(strIndexPartitionHighWatermark);
        this.partitioningKeyExpression = makePartitioningKeyExpression(partitionFieldName);
    }

    /**
     * make (and cache) a key expression from the partitioning field name.
     *
     * @param partitionFieldName partitioning field name
     * @return key expression
     */
    @Nullable
    private KeyExpression makePartitioningKeyExpression(@Nullable final String partitionFieldName) {
        if (partitionFieldName == null) {
            return null;
        }
        return partitioningKeyExpressionCache.computeIfAbsent(partitionFieldName, k -> {
            // here, partitionFieldName is not null/empty/blank
            String[] nameComponents = k.split("\\.");

            // nameComponents.length >= 1
            if (nameComponents.length == 1) {
                // no nesting
                return Key.Expressions.field(nameComponents[0]);
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
            LucenePartitionInfoProto.LucenePartitionInfo partitionInfo = selectQueryPartition(groupKey, null).startPartition;
            if (partitionInfo != null) {
                return partitionInfo.getId();
            }
        }
        return null;
    }

    /**
     * Synchronous counterpart of {@link #selectQueryPartitionAsync(Tuple, LuceneScanQuery)}.
     *
     * @param groupKey group key
     * @param luceneScanQuery query
     * @return partition query hint, or <code>null</code> if partitioning isn't enabled or
     * no partitioning metadata exist for the given query
     */
    public PartitionedQueryHint selectQueryPartition(@Nonnull Tuple groupKey, @Nullable LuceneScanQuery luceneScanQuery) {
        return state.context.asyncToSync(WAIT_LOAD_LUCENE_PARTITION_METADATA, selectQueryPartitionAsync(groupKey, luceneScanQuery));
    }

    /**
     * Return the partition ID on which to run a query, given a grouping key.
     * If no partitioning field predicate can be used to determine a particular
     * starting partition, the most recent partition is returned, unless the query
     * is sorted by the partitioning field in ascending order, in which case the
     * oldest partition is returned.
     *
     * If the query contains a top level partitioning field predicate in conjunction
     * with the rest of the predicates, and the predicate can be used to determine
     * a specific starting partition, that partition will be computed and returned.
     *
     * @param groupKey group key
     * @param luceneScanQuery query
     * @return partition query hint, or <code>null</code> if partitioning isn't enabled or
     * no partitioning metadata exist for the given query
     */
    public CompletableFuture<PartitionedQueryHint> selectQueryPartitionAsync(@Nonnull Tuple groupKey, @Nullable LuceneScanQuery luceneScanQuery) {
        if (!isPartitioningEnabled()) {
            return CompletableFuture.completedFuture(new PartitionedQueryHint(true, null));
        }
        if (luceneScanQuery == null) {
            return getNewestPartition(groupKey, state.context, state.indexSubspace).thenApply(newestPartition -> new PartitionedQueryHint(true, newestPartition));
        }

        final PartitionedSortContext sortCriteria = luceneScanQuery.getSort() == null ? null : isSortedByPartitionField(luceneScanQuery.getSort());
        final LuceneComparisonQuery partitionFieldPredicate = checkQueryForPartitionFieldPredicate(luceneScanQuery);
        final boolean isAscending = sortCriteria != null && sortCriteria.isByPartitionField && !sortCriteria.isReverse;
        final Comparisons.Type comparisonType = partitionFieldPredicate == null ? null : partitionFieldPredicate.getComparisonType();

        if (comparisonType == null) {
            CompletableFuture<LucenePartitionInfoProto.LucenePartitionInfo> noPredicatePartition = isAscending ?
                                                                                                   getOldestPartition(groupKey) :
                                                                                                   getNewestPartition(groupKey, state.context, state.indexSubspace);
            return noPredicatePartition.thenApply(partition -> new PartitionedQueryHint(true, partition));
        }


        Tuple partitionField = Tuple.from(Objects.requireNonNull(partitionFieldPredicate).getComparand());
        // <
        byte[] lowEnd = state.indexSubspace.subspace(groupKey.add(PARTITION_META_SUBSPACE)).pack();
        // >
        byte[] highEnd = ByteArrayUtil.strinc(state.indexSubspace.subspace(groupKey.add(PARTITION_META_SUBSPACE)).pack());
        // p
        byte[] partitionFieldSubspace = state.indexSubspace.subspace(groupKey.add(PARTITION_META_SUBSPACE).addAll(partitionField)).pack();
        // p+
        byte[] partitionFieldSubsequentValueSubspace = ByteArrayUtil.strinc(state.indexSubspace.subspace(groupKey.add(PARTITION_META_SUBSPACE).addAll(partitionField)).pack());

        // (<, p)
        Range lowEndToPartitionField = new Range(lowEnd, partitionFieldSubspace);
        // (p, >)
        Range partitionFieldToHighEnd = new Range(partitionFieldSubspace, highEnd);
        // (<, p+)
        Range lowEndToPartitionFieldSubsequent = new Range(lowEnd, partitionFieldSubsequentValueSubspace);
        // (p+, >)
        Range partitionFieldSubsequentToHighEnd = new Range(partitionFieldSubsequentValueSubspace, highEnd);
        if (isAscending) {
            switch (comparisonType) {
                case EQUALS:
                    return scanRange(lowEndToPartitionField, true).thenCompose(candidate1 -> {
                        if (candidate1 == null || isNewerThan(partitionField, candidate1)) {
                            return scanRange(partitionFieldToHighEnd, false).thenApply(candidate2 ->
                                    candidate2 == null || isPrefixOlderThanPartition(partitionField, candidate2) ?
                                    PartitionedQueryHint.NO_MATCHES :
                                    new PartitionedQueryHint(true, candidate2));
                        } else {
                            return CompletableFuture.completedFuture(new PartitionedQueryHint(true, candidate1));
                        }
                    });
                case GREATER_THAN_OR_EQUALS:
                    return scanRange(lowEndToPartitionField, true).thenCompose(candidate1 -> {
                        if (candidate1 == null || isNewerThan(partitionField, candidate1)) {
                            return scanRange(partitionFieldToHighEnd, false).thenApply(candidate2 ->
                                    candidate2 == null ?
                                    PartitionedQueryHint.NO_MATCHES :
                                    new PartitionedQueryHint(true, candidate2));
                        } else {
                            return CompletableFuture.completedFuture(new PartitionedQueryHint(true, candidate1));
                        }
                    });
                case GREATER_THAN:
                    // (<, p+)-reverse else (p+, >)-forward
                    return scanRange(lowEndToPartitionFieldSubsequent, true).thenCompose(candidate1 -> {
                        if (candidate1 == null || isNewerThan(partitionField, candidate1)) {
                            return scanRange(partitionFieldSubsequentToHighEnd, false).thenApply(candidate2 ->
                                    candidate2 == null ? PartitionedQueryHint.NO_MATCHES :
                                    new PartitionedQueryHint(true, candidate2));
                        } else {
                            return CompletableFuture.completedFuture(new PartitionedQueryHint(true, candidate1));
                        }
                    });
                case LESS_THAN:
                case LESS_THAN_OR_EQUALS:
                    return getOldestPartition(groupKey).thenApply(candidate ->
                            candidate == null ||
                                    (comparisonType == Comparisons.Type.LESS_THAN && isOlderThan(partitionField, candidate)) ||
                                    (comparisonType == Comparisons.Type.LESS_THAN_OR_EQUALS && isPrefixOlderThanPartition(partitionField, candidate)) ?
                            PartitionedQueryHint.NO_MATCHES : new PartitionedQueryHint(true, candidate));
                default:
                    return getOldestPartition(groupKey).thenApply(oldestPartition -> new PartitionedQueryHint(true, oldestPartition));
            }
        } else {
            switch (comparisonType) {
                case EQUALS:
                    // (<, p+)-reverse
                    // if not in, return no results
                    return scanRange(lowEndToPartitionFieldSubsequent, true).thenApply(candidate ->
                            candidate == null || isNewerThan(partitionField, candidate) ?
                            PartitionedQueryHint.NO_MATCHES : new PartitionedQueryHint(true, candidate));
                case LESS_THAN_OR_EQUALS:
                    // (<, p+)-reverse
                    // if not in, return no results
                    return scanRange(lowEndToPartitionFieldSubsequent, true).thenApply(candidate ->
                            candidate == null ?
                            PartitionedQueryHint.NO_MATCHES : new PartitionedQueryHint(true, candidate));
                case LESS_THAN:
                    // (<, p)-reverse
                    return scanRange(lowEndToPartitionField, true).thenApply(candidate ->
                            candidate == null ?
                            PartitionedQueryHint.NO_MATCHES : new PartitionedQueryHint(true, candidate));
                case GREATER_THAN:
                case GREATER_THAN_OR_EQUALS:
                    return getNewestPartition(groupKey, state.context, state.indexSubspace).thenApply(candidate ->
                            candidate == null || isNewerThan(partitionField, candidate) ?
                            PartitionedQueryHint.NO_MATCHES : new PartitionedQueryHint(true, candidate));
                default:
                    return getNewestPartition(groupKey, state.context, state.indexSubspace).thenApply(newestPartition -> new PartitionedQueryHint(true, newestPartition));
            }
        }
    }

    /**
     * helper function that scans a given range for a single partition info record.
     *
     * @param range range
     * @param reverse reverse scan if <code>true</code>
     * @return future of <code>null</code> or matched partition info
     */
    @Nonnull
    private CompletableFuture<LucenePartitionInfoProto.LucenePartitionInfo> scanRange(Range range, boolean reverse) {
        final AsyncIterable<KeyValue> rangeIterable = state.context.ensureActive().getRange(range, 1, reverse, StreamingMode.WANT_ALL);
        return AsyncUtil.collect(rangeIterable, state.context.getExecutor())
                .thenApply(targetPartitions -> targetPartitions.isEmpty() ? null : partitionInfoFromKV(targetPartitions.get(0)));
    }

    /**
     * check whether the query is predicate on the partitioning field, or is a {@link BooleanQuery} that contains
     * a {@link org.apache.lucene.search.BooleanClause.Occur#MUST} top-level predicate on the partition field,
     * and return it.
     *
     * @param luceneScanQuery lucene query
     * @return <code>null</code> if zero or more than one partitioning field predicate exist in the query's top-level
     * clauses, otherwise the predicate is returned
     */
    @Nullable
    LuceneComparisonQuery checkQueryForPartitionFieldPredicate(final @Nonnull LuceneScanQuery luceneScanQuery) {
        Query query = luceneScanQuery.getQuery();
        if (isAPartitionFieldPredicate(query)) {
            return (LuceneComparisonQuery)query;
        } else if (query instanceof BooleanQuery) {
            List<BooleanClause> clauses = ((BooleanQuery) query).clauses();

            List<LuceneComparisonQuery> partitionFieldPredicates = new ArrayList<>();
            // we only care about "top level" clauses, and won't descend

            for (BooleanClause clause : clauses) {
                if (clause.getOccur() != BooleanClause.Occur.MUST &&
                        clause.getOccur() != BooleanClause.Occur.FILTER &&
                        !(clause.getOccur() == BooleanClause.Occur.SHOULD && clauses.size() == 1)) {
                    // we only care about clauses that are either (a) not optional, or (b) optional but single
                    // note that we don't deal with MUST_NOT clauses since it would require negating the clause for
                    // determining the starting partition; support for this can be added in a future update.
                    continue;
                }
                Query clauseQuery = clause.getQuery();
                if (isAPartitionFieldPredicate(clauseQuery)) {
                    partitionFieldPredicates.add((LuceneComparisonQuery)clauseQuery);
                }
            }
            return partitionFieldPredicates.size() == 1 ? partitionFieldPredicates.get(0) : null;
        }
        return null;
    }

    private boolean isAPartitionFieldPredicate(Query query) {
        return query instanceof LuceneComparisonQuery && ((LuceneComparisonQuery) query).getFieldName().equals(partitionFieldNameInLucene);
    }

    /**
     * checks whether the provided <code>Sort</code> is by the partitioning field and whether it's in
     * reverse order.
     *
     * @param sort sort
     * @return PartitionedSortContext object
     */
    @Nonnull
    public PartitionedSortContext isSortedByPartitionField(@Nonnull Sort sort) {
        boolean sortedByPartitioningKey = false;
        boolean isReverseSort = false;
        SortField[] updatedSortFields = null;

        // check whether the sort is by the partitioning field (could be a multi-field sort order, but
        // we only care if the first sort field is the partitioning one)
        int sortFieldCount = Objects.requireNonNull(sort.getSort()).length;
        if (sortFieldCount > 0) {
            SortField sortField = sort.getSort()[0];
            String sortFieldName = sortField.getField();
            String partitioningFieldName = Objects.requireNonNull(getPartitionFieldNameInLucene());
            if (partitioningFieldName.equals(sortFieldName)) {
                sortedByPartitioningKey = sortFieldCount == 1 ||
                        (sortFieldCount == 2 && LuceneIndexMaintainer.PRIMARY_KEY_SEARCH_NAME.equals(sort.getSort()[1].getField()));
                updatedSortFields = ensurePrimaryKeyIsInSort(sort);

            }
            isReverseSort = sortField.getReverse();
        }
        return new PartitionedSortContext(sortedByPartitioningKey, isReverseSort, updatedSortFields);
    }

    /**
     * add the primary key to the sort fields when these contain only the partition field.
     *
     * @param sort sort
     * @return <code>null</code> if primary field is already included in the sort fields,
     * otherwise the updated list of sort fields.
     */
    @Nullable
    private SortField[] ensurePrimaryKeyIsInSort(Sort sort) {
        // precondition: sort is by partition key (see LucenePartitioner.isSortedByPartitionField())
        // so, either partition field + primary key (explicitly) or just partition field.
        SortField[] fields = sort.getSort();
        if (fields.length < 2) {
            SortField[] updatedFields = new SortField[2];
            updatedFields[0] = fields[0];
            updatedFields[1] = new SortField(LuceneIndexMaintainer.PRIMARY_KEY_SEARCH_NAME, SortField.Type.STRING, fields[0].getReverse());
            return updatedFields;
        }
        return null;
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
     * get the document field name the contains the document partition field, as it is stored in Lucene.
     *
     * @return Lucene document field name, or <code>null</code>
     */
    @Nullable
    public String getPartitionFieldNameInLucene() {
        return partitionFieldNameInLucene;
    }

    /**
     * add a new written record to its partition metadata.
     *
     * @param newRecord record to be written
     * @param groupingKey grouping key
     * @param <M> message
     * @return partition id or <code>null</code> if partitioning isn't enabled on index
     */
    @Nonnull
    public <M extends Message> CompletableFuture<Integer> addToAndSavePartitionMetadata(@Nonnull FDBIndexableRecord<M> newRecord, @Nonnull Tuple groupingKey) {
        if (!isPartitioningEnabled()) {
            return CompletableFuture.completedFuture(null);
        }
        return addToAndSavePartitionMetadata(groupingKey, toPartitionKey(newRecord));
    }

    /**
     * add a partition field value to the metadata of a given partition and save to db.
     * The <code>count</code> will be incremented, and the <code>from</code> or <code>to</code> boundary values will
     * be adjusted if applicable.
     *
     * @param partitioningKey partitioning key
     * @return assigned partition id
     */
    @Nonnull
    private CompletableFuture<Integer> addToAndSavePartitionMetadata(@Nonnull final Tuple groupingKey, @Nonnull final Tuple partitioningKey) {
        return getOrCreatePartitionInfo(groupingKey, partitioningKey).thenApply(assignedPartition -> {
            // assignedPartition is not null, since a new one is created by the previous call if none exist
            LucenePartitionInfoProto.LucenePartitionInfo.Builder builder = Objects.requireNonNull(assignedPartition).toBuilder();
            builder.setCount(assignedPartition.getCount() + 1);
            if (isOlderThan(partitioningKey, assignedPartition)) {
                // clear the previous key
                state.context.ensureActive().clear(partitionMetadataKeyFromPartitioningValue(groupingKey, getPartitionKey(assignedPartition)));
                builder.setFrom(ByteString.copyFrom(partitioningKey.pack()));
            }
            if (isNewerThan(partitioningKey, assignedPartition)) {
                builder.setTo(ByteString.copyFrom(partitioningKey.pack()));
            }
            savePartitionMetadata(groupingKey, builder);
            return assignedPartition.getId();
        });
    }

    /**
     * remove a deleted document from its partition metadata.
     *
     * @param oldRecord record to be deleted
     * @param groupingKey grouping key
     * @param <M> message
     * @return partition id or <code>null</code> if partitioning isn't enabled on index
     */
    @Nonnull
    public <M extends Message> CompletableFuture<Integer> removeFromAndSavePartitionMetadata(@Nonnull FDBIndexableRecord<M> oldRecord, @Nonnull Tuple groupingKey) {
        if (!isPartitioningEnabled()) {
            return CompletableFuture.completedFuture(null);
        }
        return removeFromAndSavePartitionMetadata(groupingKey, toPartitionKey(oldRecord));
    }

    /**
     * remove a document from a partition metadata and save to db.
     * Note that only the document count is changed (decremented). <code>from</code> and <code>to</code> are unchanged, and remain valid.
     *
     * @param partitioningKey partitioning key
     * @return assigned partition id
     */
    @Nonnull
    private CompletableFuture<Integer> removeFromAndSavePartitionMetadata(@Nonnull Tuple groupingKey, @Nonnull final Tuple partitioningKey) {
        return getPartitionInfoOrFail(groupingKey, partitioningKey).thenApply(assignedPartition -> {
            // assignedPartition is not null here, otherwise the call above would have thrown an exception
            LucenePartitionInfoProto.LucenePartitionInfo.Builder builder = Objects.requireNonNull(assignedPartition).toBuilder();
            // note that the to/from of the partition do not get updated, since that would require us to know what the next potential boundary
            // value(s) are. The values, nonetheless, remain valid.
            builder.setCount(assignedPartition.getCount() - 1);

            if (builder.getCount() < 0) {
                // should never happen
                throw new RecordCoreException("Issue updating Lucene partition metadata (resulting count < 0)", LogMessageKeys.PARTITION_ID, assignedPartition.getId());
            }
            savePartitionMetadata(groupingKey, builder);
            return assignedPartition.getId();
        });
    }

    /**
     * create a partition metadata key.
     *
     * @param groupKey group key
     * @param partitionKey partitioning key
     * @return partition metadata key
     */
    @Nonnull
    byte[] partitionMetadataKeyFromPartitioningValue(@Nonnull Tuple groupKey, @Nonnull Tuple partitionKey) {
        return state.indexSubspace.pack(partitionMetadataKeyTuple(groupKey, partitionKey));
    }

    private static Tuple partitionMetadataKeyTuple(final @Nonnull Tuple groupKey, @Nonnull Tuple partitionKey) {
        return groupKey.add(PARTITION_META_SUBSPACE).addAll(partitionKey);
    }

    /**
     * save partition metadata persistently.
     *
     * @param builder builder instance
     */
    void savePartitionMetadata(@Nonnull Tuple groupingKey,
                               @Nonnull final LucenePartitionInfoProto.LucenePartitionInfo.Builder builder) {
        LucenePartitionInfoProto.LucenePartitionInfo updatedPartition = builder.build();
        state.context.ensureActive().set(
                partitionMetadataKeyFromPartitioningValue(groupingKey, getPartitionKey(updatedPartition)),
                updatedPartition.toByteArray());
    }

    @Nonnull
    CompletableFuture<LucenePartitionInfoProto.LucenePartitionInfo> findPartitionInfo(@Nonnull Tuple groupingKey, @Nonnull Tuple partitioningKey) {
        Range range = new Range(state.indexSubspace.subspace(groupingKey.add(PARTITION_META_SUBSPACE)).pack(),
                state.indexSubspace.subspace(groupingKey.add(PARTITION_META_SUBSPACE).addAll(partitioningKey)).pack());

        final AsyncIterable<KeyValue> rangeIterable = state.context.ensureActive().getRange(range, 1, true, StreamingMode.WANT_ALL);

        return AsyncUtil.collect(rangeIterable, state.context.getExecutor())
                .thenApply(targetPartition -> targetPartition.isEmpty() ? null : partitionInfoFromKV(targetPartition.get(0)));
    }

    /**
     * get or create the partition that should contain the given partitioning value.
     *
     * @param partitioningKey partitioning key
     * @return partition metadata future
     */
    @Nonnull
    private CompletableFuture<LucenePartitionInfoProto.LucenePartitionInfo> getOrCreatePartitionInfo(@Nonnull Tuple groupingKey, @Nonnull Tuple partitioningKey) {
        return assignPartitionInternal(groupingKey, partitioningKey, true).thenCompose(assignedPartitionInfo -> {
            // optimization: if assigned partition is full and doc to be added is older than the partition's `from` value,
            // we create a new partition for it, in order to avoid unnecessary re-balancing later.
            if (assignedPartitionInfo.getCount() >= indexPartitionHighWatermark && isOlderThan(partitioningKey, assignedPartitionInfo) ) {
                return getAllPartitionMetaInfo(groupingKey).thenApply(partitionInfos -> {
                    int maxPartitionId = partitionInfos.stream()
                            .map(LucenePartitionInfoProto.LucenePartitionInfo::getId)
                            .max(Integer::compare)
                            .orElse(0);
                    return newPartitionMetadata(partitioningKey, maxPartitionId + 1);
                });
            }
            // else
            return CompletableFuture.completedFuture(assignedPartitionInfo);
        });
    }

    /**
     * get the partition metadata containing the given partitioning key or fail if not found.
     *
     * @param partitioningKey partitioning key
     * @return partition metadata future
     * @throws RecordCoreException if no partition is found
     */
    @Nonnull
    private CompletableFuture<LucenePartitionInfoProto.LucenePartitionInfo> getPartitionInfoOrFail(@Nonnull Tuple groupingKey, @Nonnull Tuple partitioningKey) {
        return assignPartitionInternal(groupingKey, partitioningKey, false);
    }

    /**
     * assign a partition for a document insert or delete.
     *
     * @param partitioningKey partitioning key
     * @param createIfNotExists if no suitable partition is found for this partitioning key,
     *                          create when <code>true</code>. This parameter should be set to <code>true</code> when
     *                          inserting a document, and <code>false</code> when deleting.
     * @return partition metadata future
     * @throws RecordCoreException if <code>createIfNotExists</code> is <code>false</code> and no suitable partition is found
     */
    @Nonnull
    private CompletableFuture<LucenePartitionInfoProto.LucenePartitionInfo> assignPartitionInternal(@Nonnull Tuple groupingKey,
                                                                                                    @Nonnull Tuple partitioningKey,
                                                                                                    boolean createIfNotExists) {

        final Range range = TupleRange.toRange(
                state.indexSubspace.subspace(groupingKey.add(PARTITION_META_SUBSPACE)).pack(),
                state.indexSubspace.subspace(groupingKey.add(PARTITION_META_SUBSPACE).addAll(partitioningKey)).pack(),
                EndpointType.RANGE_INCLUSIVE,
                EndpointType.RANGE_INCLUSIVE);

        final AsyncIterable<KeyValue> rangeIterable = state.context.ensureActive().getRange(range, 1, true, StreamingMode.WANT_ALL);

        return AsyncUtil.collect(rangeIterable, state.context.getExecutor()).thenCompose(targetPartition -> {
            if (targetPartition.isEmpty()) {
                return getOldestPartition(groupingKey).thenApply(oldestPartition -> {
                    if (oldestPartition == null) {
                        if (!createIfNotExists) {
                            throw new RecordCoreException("Partition metadata not found", LogMessageKeys.PARTITIONING_KEY, partitioningKey);
                        } else {
                            return newPartitionMetadata(partitioningKey, 0);
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
     * get the <code>long</code> partitioning field value from a {@link FDBIndexableRecord}, given a field name.
     *
     * @param <M> record type
     * @param rec record
     * @return long if field is found
     * @throws RecordCoreException if no field of type <code>long</code> with given name is found
     */
    @Nonnull
    private <M extends Message> Object getPartitioningFieldValue(@Nonnull FDBIndexableRecord<M> rec) {
        Key.Evaluated evaluatedKey = partitioningKeyExpression.evaluateSingleton(rec);
        if (evaluatedKey.size() == 1) {
            Object value = evaluatedKey.getObject(0);
            if (value == null) {
                throw new RecordCoreException("partitioning field is null");
            }
            return value;
        }
        // evaluatedKey.size() != 1
        throw new RecordCoreException("unexpected result when evaluating partition field");
    }

    /**
     * helper - create a new partition metadata instance.
     *
     * @param partitioningKey partitioning key
     * @param id partition id
     * @return partition metadata instance
     */
    @Nonnull
    private LucenePartitionInfoProto.LucenePartitionInfo newPartitionMetadata(@Nonnull final Tuple partitioningKey, int id) {
        return LucenePartitionInfoProto.LucenePartitionInfo.newBuilder()
                .setCount(0)
                .setTo(ByteString.copyFrom(partitioningKey.pack()))
                .setFrom(ByteString.copyFrom(partitioningKey.pack()))
                .setId(id)
                .build();
    }

    /**
     * get most recent partition's info.
     *
     * @param groupKey group key
     * @param context the context in which to execute; should generally be {@code state.context}
     * @param indexSubspace the index subspace; should generally be {@code state.indexSubspace}
     * @return partition metadata future
     */
    @Nonnull
    private static CompletableFuture<LucenePartitionInfoProto.LucenePartitionInfo> getNewestPartition(
            @Nonnull Tuple groupKey, @Nonnull final FDBRecordContext context, @Nonnull final Subspace indexSubspace) {
        return getEdgePartition(groupKey, true, context, indexSubspace);
    }

    /**
     * get oldest partition's info.
     *
     * @param groupKey group key
     * @return partition metadata future
     */
    @Nonnull
    private CompletableFuture<LucenePartitionInfoProto.LucenePartitionInfo> getOldestPartition(@Nonnull Tuple groupKey) {
        return getEdgePartition(groupKey, false, state.context, state.indexSubspace);
    }

    /**
     * get either the oldest or the most recent partition's metadata.
     *
     * @param groupKey group key
     * @param reverse scan order, (get earliest if false, most recent if true)
     * @param context the context in which to execute; should generally be {@code state.context}
     * @param indexSubspace the index subspace; should generally be {@code state.indexSubspace}
     * @return partition metadata future
     */
    @Nonnull
    private static CompletableFuture<LucenePartitionInfoProto.LucenePartitionInfo> getEdgePartition(
            @Nonnull Tuple groupKey, boolean reverse, @Nonnull final FDBRecordContext context,
            @Nonnull final Subspace indexSubspace) {
        Range range = indexSubspace.subspace(groupKey.add(PARTITION_META_SUBSPACE)).range();
        final AsyncIterable<KeyValue> rangeIterable = context.ensureActive().getRange(range, 1, reverse, StreamingMode.WANT_ALL);
        return AsyncUtil.collect(rangeIterable, context.getExecutor()).thenApply(all -> all.isEmpty() ? null : partitionInfoFromKV(all.get(0)));
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
     * Re-balance full partitions, if applicable.
     *
     * @param start The continuation at which to resume rebalancing, as returned from a previous call to
     * {@code rebalancePartitions}.
     * @param documentCount max number of documents to move in each transaction
     * @return a continuation at which to resume rebalancing in another call to {@code rebalancePartitions}
     */
    @Nonnull
    public CompletableFuture<RecordCursorContinuation> rebalancePartitions(RecordCursorContinuation start, int documentCount) {
        // This function will iterate the grouping keys
        final KeyExpression rootExpression = state.index.getRootExpression();

        if (! (rootExpression instanceof GroupingKeyExpression)) {
            return processPartitionRebalancing(Tuple.from(), documentCount).thenApply(result -> {
                if (result.getLeft() > 0) {
                    // we did something, repeat
                    return RecordCursorStartContinuation.START;
                } else {
                    return RecordCursorEndContinuation.END;
                }
            });
        }

        GroupingKeyExpression expression = (GroupingKeyExpression) rootExpression;
        final int groupingCount = expression.getGroupingCount();

        final ScanProperties scanProperties = ScanProperties.FORWARD_SCAN.with(
                props -> props.clearState().setReturnedRowLimit(1));

        final Range range = state.indexSubspace.range();
        final KeyRange keyRange = new KeyRange(range.begin, range.end);
        final Subspace subspace = state.indexSubspace;
        try (RecordCursor<Tuple> cursor = new ChainedCursor<>(
                state.context,
                lastKey -> FDBDirectoryManager.nextTuple(state.context, subspace, keyRange, lastKey, scanProperties, groupingCount),
                Tuple::pack,
                Tuple::fromBytes,
                start.toBytes(),
                ScanProperties.FORWARD_SCAN)) {
            AtomicReference<RecordCursorContinuation> continuation = new AtomicReference<>(start);
            return AsyncUtil.whileTrue(() -> cursor.onNext().thenCompose(cursorResult -> {
                if (cursorResult.hasNext()) {
                    final Tuple groupingKey = Tuple.fromItems(cursorResult.get().getItems().subList(0, groupingCount));
                    return processPartitionRebalancing(groupingKey, documentCount)
                            .thenCompose(repartitionResult -> {
                                if (repartitionResult.getLeft() > 0) {
                                    // we did something, stop so we can create a new transaction
                                    return AsyncUtil.READY_FALSE;
                                } else {
                                    // we didn't do anything, we can proceed to the next group
                                    continuation.set(cursorResult.getContinuation());
                                    return AsyncUtil.READY_TRUE;
                                }
                            });
                } else {
                    continuation.set(cursorResult.getContinuation());
                    return AsyncUtil.READY_FALSE;
                }
            }), state.context.getExecutor()).thenApply(ignored -> continuation.get());
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
     * @param repartitionDocumentCount max number of documents to move in each transaction
     * @return {@code true} future if there is more repartitioning to be done in this group
     */
    @Nonnull
    public CompletableFuture<Pair<Integer, Integer>> processPartitionRebalancing(@Nonnull final Tuple groupingKey, int repartitionDocumentCount) {
        if (repartitionDocumentCount <= 0) {
            throw new IllegalArgumentException("number of documents to move can't be zero");
        }
        return getAllPartitionMetaInfo(groupingKey).thenCompose(partitionInfos -> {
            // need to track the next partition id to use when creating a new one
            int maxPartitionId = partitionInfos.stream().map(LucenePartitionInfoProto.LucenePartitionInfo::getId).max(Integer::compare).orElse(0);
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace(partitionInfos.stream()
                        .sorted(Comparator.comparing(pi -> Tuple.fromBytes(pi.getFrom().toByteArray())))
                        .map(pi -> "pi[" + pi.getId() + "]@" + pi.getCount() + Tuple.fromBytes(pi.getFrom().toByteArray()) + "->" + Tuple.fromBytes(pi.getTo().toByteArray()))
                        .collect(Collectors.joining(", ", "Rebalancing partitions (group=" + groupingKey + "): ", "")));
            }

            for (LucenePartitionInfoProto.LucenePartitionInfo partitionInfo : partitionInfos) {
                if (partitionInfo.getCount() > indexPartitionHighWatermark) {
                    // process one partition
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug(repartitionLogMessage("Repartitioning records", groupingKey, repartitionDocumentCount, partitionInfo));
                    }
                    // get the N oldest documents in the partition (note N = (count of docs to move) + 1, since we need
                    // the (N+1)th doc's timestamp to update the partition's "from" field.
                    final int count = 1 + Math.min(repartitionDocumentCount, indexPartitionHighWatermark);
                    long startTimeNanos = System.nanoTime();
                    LuceneRecordCursor luceneRecordCursor = getOldestNDocuments(partitionInfo, groupingKey, count);

                    return moveDocsFromPartition(partitionInfo, groupingKey, maxPartitionId, luceneRecordCursor)
                            .thenApply(movedCount -> {
                                state.context.record(LuceneEvents.Events.LUCENE_REBALANCE_PARTITION, System.nanoTime() - startTimeNanos);
                                state.context.recordSize(LuceneEvents.SizeEvents.LUCENE_REBALANCE_PARTITION_DOCS, movedCount);
                                return Pair.of(
                                        movedCount,
                                        Math.max(partitionInfo.getCount() - movedCount - indexPartitionHighWatermark, 0));
                            });
                }
            }
            // here: no partitions need re-balancing
            return CompletableFuture.completedFuture(Pair.of(0, 0));
        });
    }

    private String repartitionLogMessage(final String staticMessage,
                                         final @Nonnull Tuple groupingKey,
                                         final int repartitionDocumentCount,
                                         final @Nonnull LucenePartitionInfoProto.LucenePartitionInfo partitionInfo) {
        return KeyValueLogMessage.of(staticMessage,
                LogMessageKeys.INDEX_SUBSPACE, state.indexSubspace,
                LuceneLogMessageKeys.GROUP, groupingKey,
                LuceneLogMessageKeys.INDEX_PARTITION, partitionInfo.getId(),
                LuceneLogMessageKeys.TOTAL_COUNT, partitionInfo.getCount(),
                LuceneLogMessageKeys.COUNT, repartitionDocumentCount,
                LuceneLogMessageKeys.PARTITION_HIGH_WATERMARK, indexPartitionHighWatermark);
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
        ScanComparisons comparisons = groupingKey.isEmpty() ?
                                      ScanComparisons.EMPTY :
                                      Objects.requireNonNull(ScanComparisons.from(new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, groupingKey.get(0))));
        LuceneScanParameters scan = new LuceneScanQueryParameters(
                comparisons,
                new LuceneQuerySearchClause(LuceneQueryType.QUERY, "*:*", false),
                new Sort(new SortField(partitionFieldNameInLucene, SortField.Type.LONG, false),
                         new SortField(LuceneIndexMaintainer.PRIMARY_KEY_SEARCH_NAME, SortField.Type.STRING, false)),
                null,
                null,
                null);
        ScanProperties scanProperties = ExecuteProperties.newBuilder().setReturnedRowLimit(count).build().asScanProperties(false);
        LuceneScanQuery scanQuery = (LuceneScanQuery) scan.bind(state.store, state.index, EvaluationContext.EMPTY);

        // we create the cursor here explicitly (vs. e.g. calling state.store.scanIndex(...)) because we want the search
        // to be performed specifically in the provided partition.
        // alternatively we can include a partitionInfo in the lucene scan parameters--tbd
        try (LuceneRecordCursor cursor = new LuceneRecordCursor(
                state.context.getExecutor(),
                state.context.getPropertyStorage().getPropertyValue(LuceneRecordContextProperties.LUCENE_EXECUTOR_SERVICE),
                this,
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
     * @return A future containing the amount of documents that were moved
     */
    @Nonnull
    private CompletableFuture<Integer> moveDocsFromPartition(@Nonnull final LucenePartitionInfoProto.LucenePartitionInfo partitionInfo,
                                                             @Nonnull final Tuple groupingKey,
                                                             final int maxPartitionId,
                                                             @Nonnull final LuceneRecordCursor cursor) {
        Collection<RecordType> recordTypes = state.store.getRecordMetaData().recordTypesForIndex(state.index);
        if (recordTypes.stream().map(RecordType::isSynthetic).distinct().count() > 1) {
            // don't support mix of synthetic/regular
            throw new RecordCoreException("mix of synthetic and non-synthetic record types in index is not supported");
        }

        final CompletableFuture<? extends List<? extends FDBIndexableRecord<Message>>> fetchedRecordsFuture;
        if (recordTypes.iterator().next().isSynthetic()) {
            fetchedRecordsFuture = cursor.mapPipelined(indexEntry -> state.store.loadSyntheticRecord(indexEntry.getPrimaryKey()),
                    state.store.getPipelineSize(PipelineOperation.INDEX_TO_RECORD)).asList();
        } else {
            fetchedRecordsFuture = state.store.fetchIndexRecords(cursor, IndexOrphanBehavior.SKIP).map(FDBIndexedRecord::getStoredRecord).asList();
        }

        return fetchedRecordsFuture.thenCompose(records -> {
            if (records.size() > 1) { // one record to update the boundary, and one to move
                // the newest record is the one we intend to leave in the current partition; we need it in order to set this partition's new
                // `from` value.
                final Tuple newBoundaryPartitionKey = toPartitionKey(records.get(records.size() - 1));

                // remove the (n + 1)th record from the records to be moved
                records.remove(records.size() - 1);

                LuceneIndexMaintainer indexMaintainer = (LuceneIndexMaintainer)state.store.getIndexMaintainer(state.index);
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
                state.context.ensureActive().clear(partitionMetadataKeyFromPartitioningValue(groupingKey, getPartitionKey(partitionInfo)));
                LucenePartitionInfoProto.LucenePartitionInfo.Builder builder = partitionInfo.toBuilder()
                        .setCount(partitionInfo.getCount() - records.size())
                        .setFrom(ByteString.copyFrom(newBoundaryPartitionKey.pack()));
                savePartitionMetadata(groupingKey, builder);
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug(repartitionLogMessage("Repartitoned records", groupingKey, records.size(), partitionInfo));
                }
                // value of the "destination" partition's `from` value
                final Tuple overflowPartitioningKey = toPartitionKey(records.get(0));
                return findPartitionInfo(groupingKey, overflowPartitioningKey).thenCompose(previousPartition -> {
                    if (previousPartition == null || previousPartition.getCount() + records.size() > indexPartitionHighWatermark || previousPartition.getId() == partitionInfo.getId()) {
                        // create a new "overflow" partition
                        savePartitionMetadata(groupingKey, newPartitionMetadata(overflowPartitioningKey,  maxPartitionId + 1).toBuilder());
                    }

                    Iterator<? extends FDBIndexableRecord<Message>> recordIterator = records.iterator();
                    return AsyncUtil.whileTrue(() -> indexMaintainer.update(null, recordIterator.next())
                            .thenApply(ignored -> recordIterator.hasNext()), state.context.getExecutor());
                }).thenApply(ignored -> records.size());
            }
            return CompletableFuture.completedFuture(0);
        });
    }

    /**
     * Get all partition metadata for a given grouping key.
     *
     * @param groupingKey grouping key
     * @return future list of partition metadata
     */
    @VisibleForTesting
    public CompletableFuture<List<LucenePartitionInfoProto.LucenePartitionInfo>> getAllPartitionMetaInfo(@Nonnull final Tuple groupingKey) {
        Range range = state.indexSubspace.subspace(groupingKey.add(PARTITION_META_SUBSPACE)).range();
        final AsyncIterable<KeyValue> rangeIterable = state.context.ensureActive().getRange(range, Integer.MAX_VALUE, true, StreamingMode.WANT_ALL);
        return AsyncUtil.collect(rangeIterable, state.context.getExecutor()).thenApply(all -> all.stream().map(LucenePartitioner::partitionInfoFromKV).collect(Collectors.toList()));
    }

    /**
     * find the partition metadata for a given partition id.
     *
     * @param partitionId partition id
     * @param groupingKey grouping key
     * @return future of: partition info, or null if not found
     */
    @Nonnull
    public CompletableFuture<LucenePartitionInfoProto.LucenePartitionInfo> getPartitionMetaInfoById(int partitionId, @Nonnull final Tuple groupingKey) {
        return getAllPartitionMetaInfo(groupingKey)
                .thenApply(partitionInfos -> partitionInfos.stream()
                        .filter(partition -> partition.getId() == partitionId)
                        .findAny()
                        .orElse(null));
    }

    /**
     * get the next "older" partition for a given partition key.
     *
     * @param context FDB record context
     * @param groupingKey grouping key
     * @param previousKey partition key
     * @param indexSubspace index subspace
     * @return partition future
     */
    @Nonnull
    public static CompletableFuture<LucenePartitionInfoProto.LucenePartitionInfo> getNextOlderPartitionInfo(
            @Nonnull final FDBRecordContext context,
            @Nonnull final Tuple groupingKey,
            @Nullable final Tuple previousKey,
            @Nonnull final Subspace indexSubspace) {
        if (previousKey == null) {
            return getNewestPartition(groupingKey, context, indexSubspace);
        } else {
            final Range range = new TupleRange(
                    groupingKey.add(PARTITION_META_SUBSPACE),
                    groupingKey.add(PARTITION_META_SUBSPACE).addAll(previousKey),
                    EndpointType.TREE_START,
                    EndpointType.RANGE_EXCLUSIVE).toRange(indexSubspace);
            final AsyncIterable<KeyValue> rangeIterable = context.ensureActive().getRange(range, Integer.MAX_VALUE, true, StreamingMode.WANT_ALL);
            return AsyncUtil.collect(rangeIterable, context.getExecutor())
                    .thenApply(all -> all.stream().map(LucenePartitioner::partitionInfoFromKV).findFirst().orElse(null));
        }
    }

    /**
     * get the next "newer" partition for a given partition key.
     *
     * @param context FDB record context
     * @param groupingKey grouping key
     * @param currentPartitionKey current partition key
     * @param indexSubspace index subspace
     * @return partition future
     */
    @Nonnull
    public static CompletableFuture<LucenePartitionInfoProto.LucenePartitionInfo> getNextNewerPartitionInfo(
            @Nonnull final FDBRecordContext context,
            @Nonnull final Tuple groupingKey,
            @Nullable final Tuple currentPartitionKey,
            @Nonnull final Subspace indexSubspace) {
        if (currentPartitionKey == null) {
            return getNewestPartition(groupingKey, context, indexSubspace);
        }
        final Range range = new Range(ByteArrayUtil.strinc(indexSubspace.subspace(groupingKey.add(PARTITION_META_SUBSPACE).addAll(currentPartitionKey)).pack()),
                ByteArrayUtil.strinc(indexSubspace.subspace(groupingKey.add(PARTITION_META_SUBSPACE)).pack()));
        final AsyncIterable<KeyValue> rangeIterable = context.ensureActive().getRange(range, 1, false, StreamingMode.WANT_ALL);
        return AsyncUtil.collect(rangeIterable, context.getExecutor())
                .thenApply(all -> all.stream().map(LucenePartitioner::partitionInfoFromKV).findFirst().orElse(null));
    }

    /**
     * convenience function that gets the partitioning key value given a record.
     *
     * @param record record
     * @param <M> record message
     * @return partitioning key tuple
     */
    @Nonnull
    private <M extends Message> Tuple toPartitionKey(@Nonnull final FDBIndexableRecord<M> record) {
        return toPartitionKey(getPartitioningFieldValue(record), record.getPrimaryKey());
    }

    /**
     * convenience function that builds a partitioning key value from a partitioning field value and
     * a primary key.
     *
     * @param partitioningFieldValue partitioning field value
     * @param primaryKey record primary key
     * @return partitioning key value tuple
     */
    @Nonnull
    public Tuple toPartitionKey(@Nonnull Object partitioningFieldValue,
                                @Nonnull final Tuple primaryKey) {
        return Tuple.from(partitioningFieldValue).addAll(primaryKey);
    }

    /**
     * convenience function that evaluates whether a given partitioning key tuple is "older" than the
     * given partitioning metadata.
     *
     * @param key partitioning key tuple
     * @param partitionInfo partitioning meta data
     * @return true if key is "older" than partitionInfo
     */
    public static boolean isOlderThan(@Nonnull final Tuple key, @Nonnull final LucenePartitionInfoProto.LucenePartitionInfo partitionInfo) {
        return key.compareTo(Tuple.fromBytes(partitionInfo.getFrom().toByteArray())) < 0;
    }

    public static boolean isPrefixOlderThanPartition(@Nonnull final Tuple prefix, @Nonnull LucenePartitionInfoProto.LucenePartitionInfo partitionInfo) {
        return getPartitionKey(partitionInfo).compareTo(Tuple.fromBytes(ByteArrayUtil.strinc(prefix.pack()))) >= 0;
    }

    /**
     * convenience function that evaluates whether a given partitioning key tuple is "newer" than the
     * given partitioning metadata.
     *
     * @param key partitioning key tuple
     * @param partitionInfo partitioning meta data
     * @return true if key is "newer" than partitionInfo
     */
    public static boolean isNewerThan(@Nonnull final Tuple key, @Nonnull final LucenePartitionInfoProto.LucenePartitionInfo partitionInfo) {
        return key.compareTo(Tuple.fromBytes(partitionInfo.getTo().toByteArray())) > 0;
    }

    /**
     * convenience function that returns the partitioning key of a given partition metadata object.
     *
     * @param partitionInfo partition metadata
     * @return partition key tuple
     */
    @Nonnull
    public static Tuple getPartitionKey(@Nonnull final LucenePartitionInfoProto.LucenePartitionInfo partitionInfo) {
        return Tuple.fromBytes(partitionInfo.getFrom().toByteArray());
    }

    /**
     * convenience function that returns the <code>to</code> value of a given partition metadata object.
     *
     * @param partitionInfo partition metadata
     * @return to tuple
     */
    @Nonnull
    public static Tuple getToTuple(@Nonnull final LucenePartitionInfoProto.LucenePartitionInfo partitionInfo) {
        return Tuple.fromBytes(partitionInfo.getTo().toByteArray());
    }

    /**
     * describes the sort characteristics of a given Lucene search over a partitioned index.
     */
    static class PartitionedSortContext {
        /**
         * <code>true</code> if the sort is by the partition field only.
         */
        boolean isByPartitionField;
        /**
         * <code>true</code> if the order is reverse.
         */
        boolean isReverse;
        /**
         * if the sort fields contain only the partition field, this will contain both the
         * partition field and, as the second item, the primary key {@link LuceneIndexMaintainer#PRIMARY_KEY_SEARCH_NAME}.
         */
        @Nullable
        SortField[] updatedSortFields;

        PartitionedSortContext(boolean isByPartitionField, boolean isReverse, @Nullable final SortField[] updatedSortFields) {
            this.isByPartitionField = isByPartitionField;
            this.isReverse = isReverse;
            this.updatedSortFields = updatedSortFields;
        }
    }

    /**
     * describes the result of checking the Lucene query for a partitioning field
     * predicate.
     */
    static class PartitionedQueryHint {
        static final PartitionedQueryHint NO_MATCHES = new PartitionedQueryHint(false, null);
        /**
         * Partition in which to start the query scan. <code>null</code> if the
         * index isn't partitioned or if the query contains a partitioning field
         * predicate that cannot be satisfied from any existing partition.
         */
        @Nullable
        final LucenePartitionInfoProto.LucenePartitionInfo startPartition;
        /**
         * <code>true</code> if the query <i>can</i> have matches in the given
         * starting partition (but doesn't necessarily have to).
         */
        final boolean canHaveMatches;

        PartitionedQueryHint(boolean canHaveMatches, LucenePartitionInfoProto.LucenePartitionInfo startPartition) {
            this.canHaveMatches = canHaveMatches;
            this.startPartition = startPartition;
        }

        @Override
        public String toString() {
            return "PartitionedQueryHint{" +
                    "startPartition=" + startPartition +
                    ", canHaveMatches=" + canHaveMatches +
                    '}';
        }
    }
}
