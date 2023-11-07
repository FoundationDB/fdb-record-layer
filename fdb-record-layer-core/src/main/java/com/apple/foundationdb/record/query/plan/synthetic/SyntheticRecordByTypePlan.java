/*
 * SyntheticRecordByTypePlan.java
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
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoredRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBSyntheticRecord;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Select a synthetic record sub-plan based on the record type of the given record and then execute
 * that sub-plan.
 */
class SyntheticRecordByTypePlan implements SyntheticRecordFromStoredRecordPlan  {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Synthetic-Record-By-Type-Plan");

    @Nonnull
    private final Map<String, SyntheticRecordFromStoredRecordPlan> subPlans;
    @Nonnull
    private final Set<String> syntheticRecordTypes;

    public SyntheticRecordByTypePlan(@Nonnull Map<String, SyntheticRecordFromStoredRecordPlan> subPlans) {
        this.subPlans = subPlans;

        syntheticRecordTypes = new HashSet<>();
        for (SyntheticRecordFromStoredRecordPlan subPlan : subPlans.values()) {
            syntheticRecordTypes.addAll(subPlan.getSyntheticRecordTypes());
        }
    }

    @Nonnull
    public Map<String, SyntheticRecordFromStoredRecordPlan> getSubPlans() {
        return subPlans;
    }

    @Override
    @Nonnull
    public Set<String> getStoredRecordTypes() {
        return subPlans.keySet();
    }

    @Override
    @Nonnull
    public Set<String> getSyntheticRecordTypes() {
        return syntheticRecordTypes;
    }

    @Override
    @Nonnull
    public <M extends Message> RecordCursor<FDBSyntheticRecord> execute(@Nonnull FDBRecordStore store,
                                                                        @Nonnull FDBStoredRecord<M> record,
                                                                        @Nullable byte[] continuation,
                                                                        @Nonnull ExecuteProperties executeProperties) {
        final SyntheticRecordFromStoredRecordPlan subPlan = subPlans.get(record.getRecordType().getName());
        if (subPlan == null) {
            return RecordCursor.empty();
        } else {
            return subPlan.execute(store, record, continuation, executeProperties);
        }
    }

    @Override
    public String toString() {
        return subPlans.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SyntheticRecordByTypePlan that = (SyntheticRecordByTypePlan)o;
        return Objects.equals(subPlans, that.subPlans);
    }

    @Override
    public int hashCode() {
        return Objects.hash(subPlans);
    }

    @Override
    public int planHash(@Nonnull final PlanHashMode mode) {
        switch (mode.getKind()) {
            case LEGACY:
                int hash = 1;
                for (Map.Entry<String, SyntheticRecordFromStoredRecordPlan> entry : subPlans.entrySet()) {
                    hash += entry.getKey().hashCode() * 31 + entry.getValue().planHash(mode);
                }
                return hash;
            case FOR_CONTINUATION:
                return PlanHashable.objectsPlanHash(mode, BASE_HASH, subPlans.keySet(), subPlans.values());
            default:
                throw new UnsupportedOperationException("Hash kind " + mode.getKind() + " is not supported");
        }

    }
}
