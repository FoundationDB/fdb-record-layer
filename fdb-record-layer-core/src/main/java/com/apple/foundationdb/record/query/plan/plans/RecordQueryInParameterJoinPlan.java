/*
 * RecordQueryInParameterJoinPlan.java
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
import com.apple.foundationdb.record.provider.common.StoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;

/**
 * A query plan that executes a child plan once for each of the elements of an {@code IN} list taken from a parameter.
 */
@API(API.Status.MAINTAINED)
public class RecordQueryInParameterJoinPlan extends RecordQueryInJoinPlan {
    private final String externalBinding;

    public RecordQueryInParameterJoinPlan(RecordQueryPlan plan, String bindingName, String externalBinding, boolean sortValues, boolean sortReverse) {
        super(plan, bindingName, sortValues, sortReverse);
        this.externalBinding = externalBinding;
    }

    @Override
    @Nullable
    @SuppressWarnings("unchecked")
    protected List<Object> getValues(EvaluationContext context) {
        return sortValues((List)context.getBinding(externalBinding));
    }

    public String getExternalBinding() {
        return externalBinding;
    }

    @Override
    public String toString() {
        StringBuilder str = new StringBuilder(getInner().toString());
        str.append(" WHERE ").append(bindingName)
                .append(" IN $").append(externalBinding);
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
        RecordQueryInParameterJoinPlan that = (RecordQueryInParameterJoinPlan) o;
        return Objects.equals(externalBinding, that.externalBinding);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), externalBinding);
    }

    @Override
    public int planHash() {
        return super.planHash() + externalBinding.hashCode();
    }

    @Override
    public void logPlanStructure(StoreTimer timer) {
        timer.increment(FDBStoreTimer.Counts.PLAN_IN_PARAMETER);
        getInner().logPlanStructure(timer);
    }
}
