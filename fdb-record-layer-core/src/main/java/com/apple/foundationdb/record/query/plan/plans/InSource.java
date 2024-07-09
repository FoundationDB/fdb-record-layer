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
import com.apple.foundationdb.record.PlanSerializable;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.planprotos.PInSource;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.expressions.ExplodeExpression;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.typing.Typed;
import com.apple.foundationdb.record.query.plan.serialization.PlanSerialization;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

/**
 * Helper class to describe an IN-list for physical {@link RecordQueryInJoinPlan}s and {@link RecordQueryInUnionPlan}s.
 * IN lists can be based on values or other outer bindings through the use of parameter names or correlations.
 * This class is more or less a physical counterpart of a {@link Quantifier} ranging over an {@link ExplodeExpression}.
 */
@API(API.Status.INTERNAL)
public abstract class InSource implements PlanHashable, PlanSerializable, Typed {
    @SuppressWarnings("unchecked")
    private static final Comparator<Object> VALUE_COMPARATOR = Comparator.comparing(Comparable.class::cast);

    @Nonnull
    private final String bindingName;

    @SuppressWarnings("unused")
    protected InSource(@Nonnull final PlanSerializationContext serializationContext,
                       @Nonnull final PInSource.Super inSourceProto) {
        this(Objects.requireNonNull(inSourceProto.getBindingName()));
    }

    protected InSource(@Nonnull final String bindingName) {
        this.bindingName = bindingName;
    }

    @Nonnull
    public String getBindingName() {
        return bindingName;
    }

    @Nonnull
    @Override
    public Type getResultType() {
        return Type.any();
    }

    public abstract boolean isSorted();

    public abstract boolean isReverse();

    @Nonnull
    public abstract String valuesString();

    protected abstract int size(@Nonnull EvaluationContext context);

    @Nonnull
    public List<Object> getValues() {
        return getValues(null);
    }

    @Nonnull
    protected abstract List<Object> getValues(@Nullable EvaluationContext context);

    @Nonnull
    public abstract RecordQueryInJoinPlan toInJoinPlan(@Nonnull Quantifier.Physical innerQuantifier);

    public int baseHash(@Nonnull final PlanHashMode mode, @Nonnull ObjectPlanHash objectPlanHash) {
        // TODO We should really use objectPlanHash here, too, but it seems doing so will change a lot
        //      of plan hashes.
        return objectPlanHash.planHash(mode);
    }

    @Nonnull
    protected abstract PInSource toInSourceProto(@Nonnull PlanSerializationContext serializationContext);

    @Nonnull
    protected static InSource fromInSourceProto(@Nonnull final PlanSerializationContext serializationContext,
                                                @Nonnull final PInSource inSourceProto) {
        return (InSource)PlanSerialization.dispatchFromProtoContainer(serializationContext, inSourceProto);
    }

    @Nonnull
    @SuppressWarnings("unused")
    protected PInSource.Super toInSourceSuperProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PInSource.Super.newBuilder()
                .setBindingName(bindingName)
                .build();
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
