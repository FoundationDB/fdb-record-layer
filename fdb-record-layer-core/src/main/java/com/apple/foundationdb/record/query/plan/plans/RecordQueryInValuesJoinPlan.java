/*
 * RecordQueryInValuesJoinPlan.java
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
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.provider.common.StoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;

/**
 * A query plan that executes a child plan once for each of the elements of a constant {@code IN} list.
 */
@API(API.Status.MAINTAINED)
public class RecordQueryInValuesJoinPlan extends RecordQueryInJoinPlan {
    @Nullable
    private final List<Object> values;

    public RecordQueryInValuesJoinPlan(RecordQueryPlan plan, String bindingName, @Nullable List<Object> values, boolean sortValues, boolean sortReverse) {
        super(plan, bindingName, sortValues, sortReverse);
        this.values = sortValues(values);
    }

    @Override
    @Nullable
    public List<Object> getValues(EvaluationContext context) {
        return values;
    }


    @Override
    public String toString() {
        StringBuilder str = new StringBuilder(getInner().toString());
        str.append(" WHERE ").append(bindingName)
                .append(" IN ").append(values);
        if (sortValuesNeeded) {
            str.append(" SORTED");
            if (sortReverse) {
                str.append(" DESC");
            }
        }
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
        RecordQueryInValuesJoinPlan that = (RecordQueryInValuesJoinPlan) o;
        return Objects.equals(values, that.values);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), values);
    }

    @Override
    public int planHash() {
        return super.planHash() + PlanHashable.iterablePlanHash(values);
    }

    @Override
    public void logPlanStructure(StoreTimer timer) {
        timer.increment(FDBStoreTimer.Counts.PLAN_IN_VALUES);
        getChild().logPlanStructure(timer);
    }
}
