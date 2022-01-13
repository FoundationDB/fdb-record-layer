/*
 * InValuesSource.java
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

package com.apple.foundationdb.record.query.plan.plans;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.Bindings;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.query.plan.temp.Quantifier;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;

/**
 * Helper class which represents a specialized {@link InSource} whose input is a list of literal values.
 * The logic in this class is agnostic about a potential intrinsic sorted-ness of the elements in the list.
 * If reasoning about sorted-ness is a requirement for a use case, {@link SortedInValuesSource} is preferable.
 * This source is used by {@link RecordQueryInJoinPlan}s and {@link RecordQueryInUnionPlan}s.
 */
@API(API.Status.INTERNAL)
public class InValuesSource extends InSource {
    @Nonnull
    private static final ObjectPlanHash OBJECT_PLAN_HASH_IN_VALUES_SOURCE = new ObjectPlanHash("In-Values");

    @Nonnull
    private final List<Object> values;

    public InValuesSource(@Nonnull String bindingName, @Nonnull final List<Object> values) {
        super(bindingName);
        this.values = values;
    }

    @Nonnull
    @Override
    public List<Object> getValues() {
        return values;
    }

    @Nonnull
    @Override
    protected List<Object> getValues(@Nullable final EvaluationContext context) {
        return values;
    }

    @Override
    public boolean isSorted() {
        return false;
    }

    @Override
    public boolean isReverse() {
        return false;
    }

    @Override
    public int planHash(@Nonnull final PlanHashKind hashKind) {
        if (hashKind == PlanHashKind.STRUCTURAL_WITHOUT_LITERALS) {
            return baseHash(hashKind, OBJECT_PLAN_HASH_IN_VALUES_SOURCE);
        } else {
            return PlanHashable.objectsPlanHash(hashKind, baseHash(hashKind, OBJECT_PLAN_HASH_IN_VALUES_SOURCE), values);
        }
    }

    @Override
    protected int size(@Nonnull final EvaluationContext context) {
        return values.size();
    }

    @Nonnull
    @Override
    public RecordQueryInJoinPlan toInJoinPlan(@Nonnull final Quantifier.Physical innerQuantifier) {
        return new RecordQueryInValuesJoinPlan(innerQuantifier, this, Bindings.Internal.CORRELATION);
    }

    @Nonnull
    @Override
    public String toString() {
        return getBindingName() + " IN " + values;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final var inValuesSource = (InValuesSource)o;
        if (!getBindingName().equals(inValuesSource.getBindingName())) {
            return false;
        }
        return values.equals(inValuesSource.values);
    }

    @Override
    public int hashCode() {
        return values.hashCode();
    }
}
