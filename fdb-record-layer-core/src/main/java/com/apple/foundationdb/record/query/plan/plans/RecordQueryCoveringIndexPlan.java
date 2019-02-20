/*
 * RecordQueryCoveringIndexPlan.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2018 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.plans;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.record.provider.common.StoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.FDBQueriedRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.query.plan.IndexKeyValueToPartialRecord;
import com.apple.foundationdb.record.query.plan.ScanComparisons;
import com.apple.foundationdb.record.query.plan.temp.ExpressionRef;
import com.apple.foundationdb.record.query.plan.temp.PlannerExpression;
import com.apple.foundationdb.record.query.plan.temp.SingleExpressionRef;
import com.google.common.collect.Iterators;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.Objects;
import java.util.Set;

/**
 * A query plan that reconstructs records from the entries in a covering index.
 */
@API(API.Status.MAINTAINED)
public class RecordQueryCoveringIndexPlan implements RecordQueryPlanWithChild {

    @Nonnull
    private final ExpressionRef<RecordQueryPlanWithIndex> indexPlan;
    @Nonnull
    private final String recordTypeName;
    @Nonnull
    private final IndexKeyValueToPartialRecord toRecord;

    public RecordQueryCoveringIndexPlan(@Nonnull final String indexName, @Nonnull IndexScanType scanType, @Nonnull final ScanComparisons comparisons, final boolean reverse,
                                        @Nonnull final String recordTypeName, @Nonnull IndexKeyValueToPartialRecord toRecord) {
        this(new RecordQueryIndexPlan(indexName, scanType, comparisons, reverse), recordTypeName, toRecord);
    }

    public RecordQueryCoveringIndexPlan(@Nonnull RecordQueryPlanWithIndex plan,
                                        @Nonnull final String recordTypeName, @Nonnull IndexKeyValueToPartialRecord toRecord) {
        this.indexPlan = SingleExpressionRef.of(plan);
        this.recordTypeName = recordTypeName;
        this.toRecord = toRecord;
    }

    @Nonnull
    @Override
    @SuppressWarnings("unchecked")
    public <M extends Message> RecordCursor<FDBQueriedRecord<M>> execute(@Nonnull FDBRecordStoreBase<M> store,
                                                                         @Nonnull EvaluationContext context,
                                                                         @Nullable byte[] continuation,
                                                                         @Nonnull ExecuteProperties executeProperties) {
        final RecordMetaData metaData = store.getRecordMetaData();
        final RecordType recordType = metaData.getRecordType(recordTypeName);
        final Index index = metaData.getIndex(getIndexName());
        final Descriptors.Descriptor recordDescriptor = recordType.getDescriptor();
        boolean hasPrimaryKey = getScanType() != IndexScanType.BY_GROUP;
        return indexPlan.get().executeEntries(store, context, continuation, executeProperties)
                .map(indexEntry -> store.coveredIndexQueriedRecord(index, indexEntry, recordType, (M) toRecord.toRecord(recordDescriptor, indexEntry), hasPrimaryKey));
    }

    @Nonnull
    public String getIndexName() {
        return indexPlan.get().getIndexName();
    }

    @Nonnull
    public IndexScanType getScanType() {
        return indexPlan.get().getScanType();
    }

    @Override
    public boolean isReverse() {
        return getChild().isReverse();
    }

    @Override
    public boolean hasRecordScan() {
        return false;
    }

    @Override
    public boolean hasFullRecordScan() {
        return false;
    }

    @Override
    public boolean hasIndexScan(@Nonnull String indexName) {
        return getChild().hasIndexScan(indexName);
    }

    @Nonnull
    @Override
    public Set<String> getUsedIndexes() {
        return getChild().getUsedIndexes();
    }

    @Nonnull
    @Override
    public String toString() {
        return "Covering(" + getChild() + " -> " + toRecord + ")";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        RecordQueryCoveringIndexPlan that = (RecordQueryCoveringIndexPlan) o;
        return Objects.equals(getChild(), that.getChild()) &&
               Objects.equals(recordTypeName, that.recordTypeName) &&
               Objects.equals(toRecord, that.toRecord);
    }

    @Override
    public int hashCode() {
        return Objects.hash(getChild(), recordTypeName, toRecord);
    }

    @Override
    public void logPlanStructure(StoreTimer timer) {
        timer.increment(FDBStoreTimer.Counts.PLAN_COVERING_INDEX);
    }

    @Override
    public int getComplexity() {
        return getChild().getComplexity();
    }

    @Override
    public RecordQueryPlan getChild() {
        return indexPlan.get();
    }

    @Override
    public int planHash() {
        return getChild().planHash();
    }

    @Nonnull
    @Override
    @API(API.Status.EXPERIMENTAL)
    public Iterator<? extends ExpressionRef<? extends PlannerExpression>> getPlannerExpressionChildren() {
        return Iterators.singletonIterator(indexPlan);
    }
}
