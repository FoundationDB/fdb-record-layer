/*
 * InSource.java
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
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.query.plan.temp.Quantifier;
import com.apple.foundationdb.record.query.plan.temp.expressions.ExplodeExpression;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Helper class to describe an IN-list for physical {@link RecordQueryInJoinPlan}s and {@link RecordQueryInUnionPlan}s.
 * IN lists can be based on values or other outer bindings through the use of parameter names or correlations.
 * This class is more or less a physical counterpart of a {@link Quantifier} ranging over an {@link ExplodeExpression}.
 */
@API(API.Status.INTERNAL)
public abstract class InSource implements PlanHashable {
    @SuppressWarnings("unchecked")
    private static final Comparator<Object> VALUE_COMPARATOR = Comparator.comparing(Comparable.class::cast);

    @Nonnull
    private final String bindingName;

    protected InSource(@Nonnull final String bindingName) {
        this.bindingName = bindingName;
    }

    @Nonnull
    public String getBindingName() {
        return bindingName;
    }

    public abstract boolean isSorted();

    public abstract boolean isReverse();

    protected abstract int size(@Nonnull EvaluationContext context);

    @Nonnull
    public List<Object> getValues() {
        return getValues(null);
    }

    @Nonnull
    protected abstract List<Object> getValues(@Nullable EvaluationContext context);

    @Nonnull
    public abstract RecordQueryInJoinPlan toInJoinPlan(@Nonnull final Quantifier.Physical innerQuantifier);

    public int baseHash(@Nonnull final PlanHashKind hashKind, @Nonnull ObjectPlanHash objectPlanHash) {
        // TODO We should really use objectPlanHash here, too, but it seems doing so will change a lot
        //      of plan hashes.
        return objectPlanHash.planHash(hashKind);
    }

    @Nonnull
    public static List<Object> sortValues(@Nonnull List<Object> values, final boolean isReversed) {
        if (values.size() < 2 ) {
            return values;
        }
        List<Object> copy = new ArrayList<>(values);
        copy.sort(isReversed ? VALUE_COMPARATOR.reversed() : VALUE_COMPARATOR);
        return copy;
    }
}
