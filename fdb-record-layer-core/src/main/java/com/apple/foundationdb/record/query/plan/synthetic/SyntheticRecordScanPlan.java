/*
 * SyntheticRecordScanPlan.java
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
import com.apple.foundationdb.record.provider.foundationdb.FDBSyntheticRecord;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Objects;

/**
 * Generate synthetic records by querying records and passing to a {@link SyntheticRecordFromStoredRecordPlan}.
 */
class SyntheticRecordScanPlan implements SyntheticRecordPlan  {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Synthetic-Record-Scan-Plan");

    @Nonnull
    private final RecordQueryPlan seedPlan;
    @Nonnull
    private final SyntheticRecordFromStoredRecordPlan fromSeedPlan;
    private final boolean needDistinct;

    public SyntheticRecordScanPlan(@Nonnull RecordQueryPlan seedPlan,
                                   @Nonnull SyntheticRecordFromStoredRecordPlan fromSeedPlan,
                                   boolean needDistinct) {
        this.seedPlan = seedPlan;
        this.fromSeedPlan = fromSeedPlan;
        this.needDistinct = needDistinct;
    }

    @Nonnull
    public RecordQueryPlan getSeedPlan() {
        return seedPlan;
    }

    @Nonnull
    public SyntheticRecordFromStoredRecordPlan getFromSeedPlan() {
        return fromSeedPlan;
    }

    public boolean isNeedDistinct() {
        return needDistinct;
    }

    @Override
    @Nonnull
    @SuppressWarnings("PMD.CloseResource")
    public RecordCursor<FDBSyntheticRecord> execute(@Nonnull FDBRecordStore store,
                                                    @Nullable byte[] continuation,
                                                    @Nonnull ExecuteProperties executeProperties) {
        final ExecuteProperties baseProperties = executeProperties.clearSkipAndLimit();
        RecordCursor<FDBSyntheticRecord> cursor = RecordCursor.flatMapPipelined(
                outerContinuation -> store.executeQuery(seedPlan, outerContinuation, baseProperties),
                (queriedRecord, innerContinuation) -> fromSeedPlan.execute(store, queriedRecord.getStoredRecord(), innerContinuation, baseProperties),
                continuation,
                store.getPipelineSize(PipelineOperation.SYNTHETIC_RECORD_JOIN));
        if (needDistinct) {
            cursor = SyntheticRecordConcatPlan.addDistinct(cursor);
        }
        cursor = cursor.skipThenLimit(executeProperties.getSkip(), executeProperties.getReturnedRowLimit());
        return cursor;
    }

    @Override
    public String toString() {
        return seedPlan + " | " + fromSeedPlan;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SyntheticRecordScanPlan that = (SyntheticRecordScanPlan)o;
        return needDistinct == that.needDistinct &&
               Objects.equals(seedPlan, that.seedPlan) &&
               Objects.equals(fromSeedPlan, that.fromSeedPlan);
    }

    @Override
    public int hashCode() {
        return Objects.hash(seedPlan, fromSeedPlan, needDistinct);
    }

    @Override
    public int planHash(@Nonnull final PlanHashMode mode) {
        switch (mode.getKind()) {
            case LEGACY:
                return seedPlan.planHash(mode) + fromSeedPlan.planHash(mode) + (needDistinct ? 1 : 0);
            case FOR_CONTINUATION:
                return PlanHashable.objectsPlanHash(mode, BASE_HASH, seedPlan, fromSeedPlan, needDistinct);
            default:
                throw new UnsupportedOperationException("Hash kind " + mode.getKind() + " is not supported");
        }
    }
}
