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
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.record.metadata.expressions.GroupingKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.provider.foundationdb.IndexScanParameters;
import com.apple.foundationdb.record.query.plan.AvailableFields;
import com.apple.foundationdb.record.query.plan.IndexKeyValueToPartialRecord;
import com.apple.foundationdb.record.query.plan.PlanOrderingKey;
import com.apple.foundationdb.record.query.plan.PlanWithOrderingKey;
import com.apple.foundationdb.record.query.plan.PlanWithStoredFields;
import com.apple.foundationdb.record.query.plan.plans.QueryPlanUtils;
import com.apple.foundationdb.record.query.plan.plans.QueryResult;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryIndexPlan;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Verify;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

/**
 * Lucene query plan for including search-related scan parameters.
 */
public class LuceneIndexQueryPlan extends RecordQueryIndexPlan implements PlanWithOrderingKey, PlanWithStoredFields {
    @Nullable
    private final PlanOrderingKey planOrderingKey;
    @Nullable
    private final List<KeyExpression> storedFields;

    public LuceneIndexQueryPlan(@Nonnull String indexName, @Nonnull LuceneScanParameters scanParameters, boolean reverse,
                                @Nullable PlanOrderingKey planOrderingKey, @Nullable List<KeyExpression> storedFields) {
        super(indexName, scanParameters, reverse);
        this.planOrderingKey = planOrderingKey;
        this.storedFields = storedFields;
    }

    /**
     * Override here to have specific logic to build the {@link QueryResult} for lucene auto-complete and spell-check results.
     * For these 2 scan types, results are returned as partial records that have only the grouping keys, primary key,
     * and the text fields indexed by Lucene which have matches with the search key.
     * So other fields than the matching text field won't be able to be read from the partial records.
     * For an indexed record that have matches from multiple text fields, matching suggestions are returned as multiple partial records with different text fields populated,
     * which share the same grouping keys and primary key, so they are considered as multiple results.
     */
    @Nonnull
    @Override
    @SuppressWarnings("PMD.CloseResource")
    public <M extends Message> RecordCursor<QueryResult> executePlan(@Nonnull FDBRecordStoreBase<M> store,
                                                                     @Nonnull EvaluationContext context,
                                                                     @Nullable byte[] continuation,
                                                                     @Nonnull ExecuteProperties executeProperties) {
        final RecordMetaData metaData = store.getRecordMetaData();
        final Index index = metaData.getIndex(indexName);
        final Collection<RecordType> recordTypes = metaData.recordTypesForIndex(index);
        if (recordTypes.size() != 1) {
            throw new RecordCoreException("No lucene index should span multiple record types");
        }
        final IndexScanType scanType = getScanType();
        if (scanType.equals(LuceneScanTypes.BY_LUCENE_AUTO_COMPLETE) || scanType.equals(LuceneScanTypes.BY_LUCENE_SPELL_CHECK)) {
            final RecordType recordType = recordTypes.iterator().next();
            final RecordCursor<IndexEntry> entryRecordCursor = executeEntries(store, context, continuation, executeProperties);
            return entryRecordCursor
                    .map(QueryPlanUtils.getCoveringIndexEntryToPartialRecordFunction(store, recordType.getName(), indexName,
                            getToPartialRecord(index, recordType, scanType), scanType.equals(LuceneScanTypes.BY_LUCENE_AUTO_COMPLETE)))
                    .map(QueryResult::fromQueriedRecord);
        }
        return super.executePlan(store, context, continuation, executeProperties);
    }

    /**
     * Get the {@link IndexKeyValueToPartialRecord} instance for an {@link IndexEntry} representing a result of Lucene auto-complete suggestion.
     * The partial record contains the suggestion in the field where it is indexed from, and the grouping keys if there are any.
     * @param index the index being scanned
     * @param recordType the record type for indexed records
     * @param scanType the type of scan
     * @return a partial record generator
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

        builder.addRegularCopier(new LuceneIndexKeyValueToPartialRecordUtils.LuceneAutoCompleteCopier(scanType.equals(LuceneScanTypes.BY_LUCENE_AUTO_COMPLETE),
                recordType.getPrimaryKey()));

        return builder.build();
    }

    /**
     * Auto-Complete and Spell-Check scan has their own implementation for {@link IndexKeyValueToPartialRecord} to build partial records,
     * so they are not appropriate for the optimization by {@link com.apple.foundationdb.record.query.plan.plans.RecordQueryCoveringIndexPlan}.
     */
    @Override
    public boolean allowedForCoveringIndexPlan() {
        return !getScanType().equals(LuceneScanTypes.BY_LUCENE_AUTO_COMPLETE)
               && !getScanType().equals(LuceneScanTypes.BY_LUCENE_SPELL_CHECK);
    }

    @Nullable
    @Override
    public PlanOrderingKey getPlanOrderingKey() {
        return planOrderingKey;
    }

    @Override
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    public void getStoredFields(@Nonnull List<KeyExpression> keyFields, @Nonnull List<KeyExpression> nonStoredFields) {
        int i = 0;
        while (i < nonStoredFields.size()) {
            KeyExpression field = nonStoredFields.get(i);
            KeyExpression origField = field;
            // Unwrap functions that are just annotations.
            while (field instanceof LuceneFunctionKeyExpression) {
                if (field instanceof LuceneFunctionKeyExpression.LuceneSorted) {
                    field = ((LuceneFunctionKeyExpression.LuceneSorted)field).getSortedExpression();
                } else if (field instanceof LuceneFunctionKeyExpression.LuceneStored) {
                    field = ((LuceneFunctionKeyExpression.LuceneStored)field).getStoredExpression();
                } else {
                    break;
                }
            }
            if (field != origField) {
                nonStoredFields.set(i, field);
                int j = keyFields.indexOf(origField);
                if (j >= 0) {
                    keyFields.set(j, field);
                }
            }
            if (storedFields != null && storedFields.contains(origField)) {
                nonStoredFields.remove(i);
            } else {
                i++;
            }
        }
    }

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
        return Objects.equals(planOrderingKey, that.planOrderingKey) &&
               Objects.equals(storedFields, that.storedFields);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), planOrderingKey, storedFields);
    }

    @Nonnull
    @Override
    protected RecordQueryIndexPlan withIndexScanParameters(@Nonnull final IndexScanParameters newIndexScanParameters) {
        Verify.verify(newIndexScanParameters instanceof LuceneScanParameters);

        // TODO this seems to be too simplistic
        return new LuceneIndexQueryPlan(getIndexName(), (LuceneScanParameters)newIndexScanParameters, reverse, planOrderingKey, storedFields);
    }
}
