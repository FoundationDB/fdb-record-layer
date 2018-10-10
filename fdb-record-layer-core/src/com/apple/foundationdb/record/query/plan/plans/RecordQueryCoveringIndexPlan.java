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

import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.record.provider.common.StoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.FDBEvaluationContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBQueriedRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.query.plan.IndexKeyValueToPartialRecord;
import com.apple.foundationdb.record.query.plan.ScanComparisons;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Objects;

/**
 * A query plan that reconstructs records from the entries in a covering index.
 */
public class RecordQueryCoveringIndexPlan extends RecordQueryIndexPlan {

    @Nonnull
    private final String recordTypeName;
    @Nonnull
    private final IndexKeyValueToPartialRecord toRecord;

    public RecordQueryCoveringIndexPlan(@Nonnull final String indexName, @Nonnull IndexScanType scanType, @Nonnull final ScanComparisons comparisons, final boolean reverse,
                                        @Nonnull final String recordTypeName, @Nonnull IndexKeyValueToPartialRecord toRecord) {
        super(indexName, scanType, comparisons, reverse);
        this.recordTypeName = recordTypeName;
        this.toRecord = toRecord;
    }

    public RecordQueryCoveringIndexPlan(@Nonnull RecordQueryIndexPlan plan,
                                        @Nonnull final String recordTypeName, @Nonnull IndexKeyValueToPartialRecord toRecord) {
        this(plan.indexName, plan.scanType, plan.comparisons, plan.reverse, recordTypeName, toRecord);
    }

    @Nonnull
    @Override
    @SuppressWarnings("unchecked")
    public <M extends Message> RecordCursor<FDBQueriedRecord<M>> execute(@Nonnull FDBEvaluationContext<M> context,
                                                                         @Nullable byte[] continuation,
                                                                         @Nonnull ExecuteProperties executeProperties) {
        final FDBRecordStoreBase<M> store = context.getStore();
        final RecordMetaData metaData = store.getRecordMetaData();
        final RecordType recordType = metaData.getRecordType(recordTypeName);
        final Index index = metaData.getIndex(indexName);
        final TupleRange range = comparisons.toTupleRange(context);
        final Descriptors.Descriptor recordDescriptor = recordType.getDescriptor();
        final boolean hasPrimaryKey = scanType != IndexScanType.BY_GROUP;
        return store.scanIndex(index, scanType, range, continuation,
                executeProperties.asScanProperties(reverse))
                .map(indexEntry ->
                        store.coveredIndexQueriedRecord(index, indexEntry, recordType, (M) toRecord.toRecord(recordDescriptor, indexEntry), hasPrimaryKey));
    }

    @Nonnull
    @Override
    public String toString() {
        StringBuilder str = new StringBuilder("CoveringIndex(");
        appendScanDetails(str);
        str.append(")");
        return str.toString();
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
        return Objects.equals(recordTypeName, that.recordTypeName) &&
                Objects.equals(toRecord, that.toRecord);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), recordTypeName, toRecord);
    }

    @Override
    protected void appendScanDetails(StringBuilder str) {
        super.appendScanDetails(str);
        str.append(" -> ").append(toRecord);
    }

    @Override
    public void logPlanStructure(StoreTimer timer) {
        timer.increment(FDBStoreTimer.Counts.PLAN_COVERING_INDEX);
    }

}
