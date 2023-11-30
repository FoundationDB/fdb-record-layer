/*
 * SyntheticRecordConcatPlan.java
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

package com.apple.foundationdb.record.query.plan.synthetic;

import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PipelineOperation;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoredRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBSyntheticRecord;
import com.apple.foundationdb.tuple.Tuple;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Concatenate several {@linkplain SyntheticRecordFromStoredRecordPlan plan} outputs together.
 * 
 * Similar to {@link com.apple.foundationdb.record.query.plan.plans.RecordQueryUnorderedUnionPlan}.
 */
class SyntheticRecordConcatPlan implements SyntheticRecordFromStoredRecordPlan  {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Synthetic-Record-Concat-Plan");

    @Nonnull
    private final List<SyntheticRecordFromStoredRecordPlan> subPlans;
    private final boolean needDistinct;
    @Nonnull
    private final Set<String> storedRecordTypes;
    @Nonnull
    private final Set<String> syntheticRecordTypes;

    public SyntheticRecordConcatPlan(@Nonnull List<SyntheticRecordFromStoredRecordPlan> subPlans, boolean needDistinct) {
        this.subPlans = subPlans;
        this.needDistinct = needDistinct;

        storedRecordTypes = new HashSet<>();
        syntheticRecordTypes = new HashSet<>();
        for (SyntheticRecordFromStoredRecordPlan subPlan : subPlans) {
            storedRecordTypes.addAll(subPlan.getStoredRecordTypes());
            syntheticRecordTypes.addAll(subPlan.getSyntheticRecordTypes());
        }
    }

    @Nonnull
    public List<SyntheticRecordFromStoredRecordPlan> getSubPlans() {
        return subPlans;
    }

    public boolean isNeedDistinct() {
        return needDistinct;
    }

    @Override
    @Nonnull
    public Set<String> getStoredRecordTypes() {
        return storedRecordTypes;
    }

    @Override
    @Nonnull
    public Set<String> getSyntheticRecordTypes() {
        return syntheticRecordTypes;
    }

    @Override
    @Nonnull
    @SuppressWarnings("PMD.CloseResource")
    public <M extends Message> RecordCursor<FDBSyntheticRecord> execute(@Nonnull FDBRecordStore store,
                                                                        @Nonnull FDBStoredRecord<M> record,
                                                                        @Nullable byte[] continuation,
                                                                        @Nonnull ExecuteProperties executeProperties) {
        final ExecuteProperties baseProperties = executeProperties.clearSkipAndLimit();
        RecordCursor<FDBSyntheticRecord> cursor = RecordCursor.flatMapPipelined(
                outerContinuation -> RecordCursor.fromList(store.getExecutor(), subPlans, outerContinuation),
                (subPlan, innerContinuation) -> subPlan.execute(store, record, innerContinuation, baseProperties),
                continuation,
                store.getPipelineSize(PipelineOperation.SYNTHETIC_RECORD_JOIN));
        if (needDistinct) {
            cursor = addDistinct(cursor);
        }
        cursor = cursor.skipThenLimit(executeProperties.getSkip(), executeProperties.getReturnedRowLimit());
        return cursor;
    }

    public static RecordCursor<FDBSyntheticRecord> addDistinct(RecordCursor<FDBSyntheticRecord> cursor) {
        final Set<Tuple> seen = new HashSet<>();
        return cursor.filter(r -> seen.add(r.getPrimaryKey()));
    }

    @Override
    public String toString() {
        return subPlans.stream().map(Object::toString).collect(Collectors.joining(" + "));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SyntheticRecordConcatPlan that = (SyntheticRecordConcatPlan)o;
        return needDistinct == that.needDistinct &&
               Objects.equals(subPlans, that.subPlans);
    }

    @Override
    public int hashCode() {
        return Objects.hash(subPlans, needDistinct);
    }

    @Override
    public int planHash(@Nonnull final PlanHashMode mode) {
        switch (mode.getKind()) {
            case LEGACY:
                return PlanHashable.planHash(mode, subPlans) + (needDistinct ? 1 : 0);
            case FOR_CONTINUATION:
                return PlanHashable.objectsPlanHash(mode, BASE_HASH, subPlans, needDistinct);
            default:
                throw new UnsupportedOperationException("Hash kind " + mode.getKind() + " is not supported");
        }
    }
}
