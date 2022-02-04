/*
 * LuceneIndexQueryPlan.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2021 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.record.metadata.expressions.GroupingKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.AvailableFields;
import com.apple.foundationdb.record.query.plan.IndexKeyValueToPartialRecord;
import com.apple.foundationdb.record.query.plan.ScanComparisons;
import com.apple.foundationdb.record.query.plan.plans.QueryPlanUtils;
import com.apple.foundationdb.record.query.plan.plans.QueryResult;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryIndexPlan;
import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Objects;

import static com.google.common.base.Verify.verify;

/**
 * Lucene query plan for including sort parameters.
 */
public class LuceneIndexQueryPlan extends RecordQueryIndexPlan {
    private KeyExpression sort;
    private Boolean duplicates = false;
    private final ScanComparisons groupingComparisons;

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        final LuceneIndexQueryPlan that = (LuceneIndexQueryPlan)o;
        return Objects.equals(sort, that.sort);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), sort);
    }

    @Override
    public int planHash(@Nonnull final PlanHashKind hashKind) {
        switch (hashKind) {
            case LEGACY:
            case FOR_CONTINUATION:
            case STRUCTURAL_WITHOUT_LITERALS:
                return PlanHashable.objectsPlanHash(hashKind, BASE_HASH, indexName, scanType, comparisons, sort, reverse);
            default:
                throw new UnsupportedOperationException("Hash kind " + hashKind.name() + " is not supported");
        }
    }

    public LuceneIndexQueryPlan(@Nonnull final String indexName, @Nonnull Comparisons.LuceneComparison comparison,
                                final boolean reverse, final ScanComparisons groupingComparisons) {
        this(indexName, IndexScanType.BY_LUCENE, comparison, reverse, null, groupingComparisons);
    }

    public LuceneIndexQueryPlan(@Nonnull final String indexName, @Nonnull final IndexScanType scanType,
                                @Nonnull Comparisons.LuceneComparison comparison, final boolean reverse,
                                @Nullable KeyExpression sort, final ScanComparisons groupingComparisons) {
        super(indexName, scanType, Objects.requireNonNull(ScanComparisons.from(comparison)), reverse);
        this.sort = sort;
        this.groupingComparisons = groupingComparisons;
    }

    public boolean createsDuplicates() {
        return duplicates;
    }

    public void setCreatesDuplicates() {
        duplicates = true;
    }

    public Comparisons.LuceneComparison getComparison() {
        return (Comparisons.LuceneComparison)comparisons.getEqualityComparisons().get(0);
    }

    public String getLuceneQueryString() {
        return (String)getComparison().getComparand();
    }

    public static LuceneIndexQueryPlan merge(LuceneIndexQueryPlan plan1, LuceneIndexQueryPlan plan2, String type) {
        verify(plan1.indexName.equals(plan2.indexName));
        verify(plan1.sort == null || plan2.sort == null || plan1.sort.equals(plan2.sort));
        KeyExpression newSort = plan1.sort != null ? plan1.sort : plan2.sort;
        String newQuery = String.format("(%s) %s (%s)", plan1.getLuceneQueryString(), type, plan2.getLuceneQueryString());
        Comparisons.LuceneComparison comparison = new Comparisons.LuceneComparison(newQuery);
        boolean newReverse = plan1.isReverse() ? plan1.isReverse() : plan2.isReverse();
        IndexScanType scanType = IndexScanType.BY_LUCENE;
        if (plan1.scanType == IndexScanType.BY_LUCENE_FULL_TEXT || plan2.scanType == IndexScanType.BY_LUCENE_FULL_TEXT) {
            scanType = IndexScanType.BY_LUCENE_FULL_TEXT;
        }
        ScanComparisons newGrouping = plan1.groupingComparisons == null ? ScanComparisons.EMPTY : plan1.groupingComparisons;
        if (newGrouping.isEmpty()) {
            newGrouping = plan2.groupingComparisons;
        } else if (plan2.groupingComparisons != null) {
            newGrouping = newGrouping.merge(plan2.groupingComparisons);
        }
        LuceneIndexQueryPlan plan =  new LuceneIndexQueryPlan(plan1.indexName, scanType, comparison, newReverse, newSort, newGrouping);
        if (plan1.createsDuplicates() || plan2.createsDuplicates()) {
            plan.setCreatesDuplicates();
        }
        return plan;
    }

    /**
     * Override here to have specific logic to build the {@link QueryResult} for lucene auto complete suggestion result.
     */
    @Nonnull
    @Override
    public <M extends Message> RecordCursor<QueryResult> executePlan(@Nonnull FDBRecordStoreBase<M> store,
                                                                      @Nonnull EvaluationContext context,
                                                                      @Nullable byte[] continuation,
                                                                      @Nonnull ExecuteProperties executeProperties) {
        final RecordMetaData metaData = store.getRecordMetaData();
        final Index index = metaData.getIndex(indexName);
        final Collection<RecordType> recordTypes = metaData.recordTypesForIndex(index);
        if (scanType == IndexScanType.BY_LUCENE_AUTO_COMPLETE || scanType == IndexScanType.BY_LUCENE_SPELLCHECK) {
            if (recordTypes.size() != 1) {
                throw new RecordCoreException("No lucene index should span multiple record types");
            }
            final RecordType recordType = recordTypes.iterator().next();
            final RecordCursor<IndexEntry> entryRecordCursor = executeEntries(store, context, continuation, executeProperties);
            return entryRecordCursor
                    .map(QueryPlanUtils.getCoveringIndexEntryToPartialRecordFunction(store, recordType.getName(), indexName,
                            getToPartialRecord(index, recordType, scanType), scanType))
                    .map(QueryResult::of);
        }
        return super.executePlan(store, context, continuation, executeProperties);
    }

    /**
     * Get the {@link IndexKeyValueToPartialRecord} instance for an {@link IndexEntry} representing a result of Lucene auto-complete suggestion.
     * The partial record contains the suggestion in the field where it is indexed from, and the grouping keys if there are any.
     */
    @VisibleForTesting
    public static IndexKeyValueToPartialRecord getToPartialRecord(@Nonnull Index index,
                                                                  @Nonnull RecordType recordType,
                                                                  @Nonnull IndexScanType scanType) {
        final IndexKeyValueToPartialRecord.Builder builder = IndexKeyValueToPartialRecord.newBuilder(recordType);

        KeyExpression root = index.getRootExpression();
        if (root instanceof GroupingKeyExpression) {
            KeyExpression groupingKey = ((GroupingKeyExpression) root).getGroupingSubKey();
            for (int i = 0; i < groupingKey.getColumnSize(); i++) {
                AvailableFields.addCoveringField(groupingKey, AvailableFields.FieldData.of(IndexKeyValueToPartialRecord.TupleSource.KEY, i), builder);
            }
        }

        builder.addRequiredMessageFields();
        if (!builder.isValid(true)) {
            throw new RecordCoreException("Missing required field for result record")
                    .addLogInfo(LogMessageKeys.INDEX_NAME, index.getName())
                    .addLogInfo(LogMessageKeys.RECORD_TYPE, recordType.getName())
                    .addLogInfo(LogMessageKeys.SCAN_TYPE, scanType);
        }

        builder.addRegularCopier(new LuceneIndexKeyValueToPartialRecordUtils.LuceneAutoCompleteCopier());

        return builder.build();
    }

    @Nonnull
    @Override
    public <M extends Message> RecordCursor<IndexEntry> executeEntries(@Nonnull final FDBRecordStoreBase<M> store,
                                                                       @Nonnull final EvaluationContext context,
                                                                       @Nullable final byte[] continuation,
                                                                       @Nonnull final ExecuteProperties executeProperties) {
        final TupleRange range = groupingComparisons == null ? comparisons.toTupleRange() : comparisons.append(groupingComparisons).toTupleRange(store, context);
        final RecordMetaData metaData = store.getRecordMetaData();
        LuceneScanProperties scanProperties = new LuceneScanProperties(executeProperties, sort, reverse);
        return store.scanIndex(metaData.getIndex(indexName), scanType, range,
                continuation, scanProperties);
    }
}
