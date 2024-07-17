/*
 * IndexAggregateFunction.java
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

package com.apple.foundationdb.record.metadata;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.PlanSerializable;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.planprotos.PIndexAggregateFunction;
import com.apple.foundationdb.record.query.plan.serialization.PlanSerialization;
import com.google.protobuf.Descriptors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Objects;

/**
 * An aggregate function implemented by scanning an appropriate index.
 * @see com.apple.foundationdb.record.FunctionNames
 * @see com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore#evaluateAggregateFunction
 */
@API(API.Status.MAINTAINED)
public class IndexAggregateFunction implements PlanHashable, PlanSerializable {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Index-Aggregate-Function");

    @Nonnull
    private final String name;
    @Nonnull
    private final KeyExpression operand;
    @Nullable
    private final String index;

    public IndexAggregateFunction(@Nonnull String name, @Nonnull KeyExpression operand, @Nullable String index) {
        this.name = name;
        this.operand = operand;
        this.index = index;
    }

    @Nonnull
    public String getName() {
        return name;
    }

    @Nonnull
    public KeyExpression getOperand() {
        return operand;
    }

    @Nullable
    public String getIndex() {
        return index;
    }

    @Nonnull
    public IndexAggregateFunction cloneWithOperand(@Nonnull KeyExpression operand) {
        return new IndexAggregateFunction(getName(), operand, getIndex());
    }

    @Nonnull
    public IndexAggregateFunction cloneWithIndex(@Nonnull String index) {
        return new IndexAggregateFunction(getName(), getOperand(), index);
    }

    public void validate(@Nonnull Descriptors.Descriptor descriptor) {
        operand.validate(descriptor);
    }

    @Nonnull
    public TupleRange adjustRange(@Nonnull EvaluationContext context, @Nonnull TupleRange tupleRange) {
        return tupleRange;
    }

    @Override
    public String toString() {
        StringBuilder str = new StringBuilder();
        if (index != null) {
            str.append(index).append('.');
        }
        str.append(name).append('(').append(operand).append(')');
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

        IndexAggregateFunction that = (IndexAggregateFunction) o;

        return this.name.equals(that.name) &&
            this.operand.equals(that.operand) &&
            Objects.equals(this.index, that.index);
    }

    @Override
    public int hashCode() {
        int result = name.hashCode();
        result = 31 * result + operand.hashCode();
        if (index != null) {
            result = 31 * result + index.hashCode();
        }
        return result;
    }

    @Override
    public int planHash(@Nonnull final PlanHashMode mode) {
        switch (mode.getKind()) {
            case LEGACY:
                return name.hashCode() + operand.planHash(mode) + Objects.hashCode(index);
            case FOR_CONTINUATION:
                return PlanHashable.objectsPlanHash(mode, BASE_HASH, name, operand, index);
            default:
                throw new UnsupportedOperationException("Hash kind " + mode.getKind() + " is not supported");
        }
    }

    @Nonnull
    @Override
    public PIndexAggregateFunction toProto(@Nonnull final PlanSerializationContext serializationContext) {
        final PIndexAggregateFunction.Builder builder = PIndexAggregateFunction.newBuilder()
                .setName(name)
                .setOperand(operand.toKeyExpression());
        if (index != null) {
            builder.setIndex(index);
        }
        return builder.build();
    }

    @Nonnull
    @SuppressWarnings("unused")
    public static IndexAggregateFunction fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                                   @Nonnull final PIndexAggregateFunction indexAggregateFunctionProto) {
        return new IndexAggregateFunction(Objects.requireNonNull(indexAggregateFunctionProto.getName()),
                KeyExpression.fromProto(Objects.requireNonNull(indexAggregateFunctionProto.getOperand())),
                PlanSerialization.getFieldOrNull(indexAggregateFunctionProto,
                        PIndexAggregateFunction::hasIndex,
                        PIndexAggregateFunction::getIndex));
    }
}
