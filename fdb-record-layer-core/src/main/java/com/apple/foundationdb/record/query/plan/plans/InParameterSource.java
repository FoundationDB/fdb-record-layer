/*
 * InParameterSource.java
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
import com.apple.foundationdb.annotation.SpotBugsSuppressWarnings;
import com.apple.foundationdb.record.Bindings;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.query.plan.temp.Quantifier;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Helper class which represents a specialized {@link InSource} whose input is an outer binding (a parameter).
 * The logic in this class is agnostic about a potential intrinsic sorted-ness of the elements in the list.
 * If reasoning about sorted-ness is a requirement for a use case, {@link SortedInParameterSource} is preferable.
 * This source is used by {@link RecordQueryInJoinPlan}s and {@link RecordQueryInUnionPlan}s.
 */
@API(API.Status.INTERNAL)
public class InParameterSource extends InSource {
    @Nonnull
    private static final ObjectPlanHash OBJECT_PLAN_HASH_IN_PARAMETER_SOURCE = new ObjectPlanHash("In-Parameter");

    @Nonnull
    private final String parameterName;

    public InParameterSource(@Nonnull String bindingName, @Nonnull final String parameterName) {
        super(bindingName);
        this.parameterName = parameterName;
    }

    @Nonnull
    public String getParameterName() {
        return parameterName;
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
        return PlanHashable.objectsPlanHash(hashKind, baseHash(hashKind, OBJECT_PLAN_HASH_IN_PARAMETER_SOURCE), parameterName);
    }

    @Override
    protected int size(@Nonnull final EvaluationContext context) {
        return getValues(context).size();
    }

    @Nonnull
    @Override
    @SpotBugsSuppressWarnings("NP_PARAMETER_MUST_BE_NONNULL_BUT_MARKED_AS_NULLABLE")
    protected List<Object> getValues(@Nullable final EvaluationContext context) {
        return getBoundValues(Objects.requireNonNull(context));
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    protected List<Object> getBoundValues(@Nonnull final EvaluationContext context) {
        final List<Object> binding = (List<Object>)context.getBinding(getParameterName());
        return Objects.requireNonNullElse(binding, Collections.emptyList());
    }

    @Nonnull
    @Override
    public RecordQueryInJoinPlan toInJoinPlan(@Nonnull final Quantifier.Physical innerQuantifier) {
        return new RecordQueryInParameterJoinPlan(innerQuantifier, this, Bindings.Internal.CORRELATION);
    }

    @Nonnull
    @Override
    public String toString() {
        return getBindingName() + " IN $" + parameterName;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final var inParameterSource = (InParameterSource)o;
        if (!getBindingName().equals(inParameterSource.getBindingName())) {
            return false;
        }
        return parameterName.equals(inParameterSource.parameterName);
    }

    @Override
    public int hashCode() {
        return parameterName.hashCode();
    }
}
