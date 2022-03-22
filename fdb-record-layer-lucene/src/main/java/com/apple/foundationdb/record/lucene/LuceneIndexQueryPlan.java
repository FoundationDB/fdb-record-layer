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
import com.apple.foundationdb.record.query.plan.AvailableFields;
import com.apple.foundationdb.record.query.plan.IndexKeyValueToPartialRecord;
import com.apple.foundationdb.record.query.plan.plans.QueryPlanUtils;
import com.apple.foundationdb.record.query.plan.plans.QueryResult;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryIndexPlan;
import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;

/**
 * Lucene query plan for including sort parameters.
 */
public class LuceneIndexQueryPlan extends RecordQueryIndexPlan {
    public LuceneIndexQueryPlan(@Nonnull String indexName, @Nonnull LuceneScanParameters scanParameters, boolean reverse) {
        super(indexName, scanParameters, reverse);
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
        if (recordTypes.size() != 1) {
            throw new RecordCoreException("No lucene index should span multiple record types");
        }
        final IndexScanType scanType = getScanType();
        if (scanType == LuceneScanTypes.BY_LUCENE_AUTO_COMPLETE || scanType == LuceneScanTypes.BY_LUCENE_SPELL_CHECK) {
            final RecordType recordType = recordTypes.iterator().next();
            final RecordCursor<IndexEntry> entryRecordCursor = executeEntries(store, context, continuation, executeProperties);
            return entryRecordCursor
                    .map(QueryPlanUtils.getCoveringIndexEntryToPartialRecordFunction(store, recordType.getName(), indexName,
                            getToPartialRecord(index, recordType, scanType), false))
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
}
