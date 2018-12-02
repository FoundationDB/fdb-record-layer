/*
 * OneOfThemWithComparison.java
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

package com.apple.foundationdb.record.query.expressions;

import com.apple.foundationdb.API;
import com.apple.foundationdb.record.provider.foundationdb.FDBEvaluationContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecord;
import com.apple.foundationdb.record.query.plan.temp.ExpressionRef;
import com.apple.foundationdb.record.query.plan.temp.PlannerExpression;
import com.apple.foundationdb.record.query.plan.temp.SingleExpressionRef;
import com.google.common.collect.Iterators;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

/**
 * A {@link QueryComponent} that evaluates a {@link com.apple.foundationdb.record.query.expressions.Comparisons.Comparison} against each of the values of a repeated field and is satisfied if any of those are.
 */
@API(API.Status.MAINTAINED)
public class OneOfThemWithComparison extends BaseRepeatedField implements ComponentWithComparison {
    @Nonnull
    private final ExpressionRef<Comparisons.Comparison> comparison;

    public OneOfThemWithComparison(@Nonnull String fieldName, @Nonnull Comparisons.Comparison comparison) {
        super(fieldName);
        this.comparison = SingleExpressionRef.of(comparison);
    }

    @Override
    @Nullable
    public <C extends Message, M extends C> Boolean evalMessage(@Nonnull FDBEvaluationContext<C> context, @Nullable FDBRecord<M> record, @Nullable Message message) {
        if (message == null ) {
            return getComparison().eval(context, null);
        }
        List<Object> values = getValues(message);
        if (values == null) {
            return null;
        } else {
            for (Object value : values) {
                final Boolean val = getComparison().eval(context, value);
                if (val != null && val) {
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public void validate(@Nonnull Descriptors.Descriptor descriptor) {
        final Descriptors.FieldDescriptor field = validateRepeatedField(descriptor);
        requirePrimitiveField(field);
        getComparison().validate(field, true);
    }

    @Nonnull
    public Comparisons.Comparison getComparison() {
        return comparison.get();
    }

    @Override
    public QueryComponent withOtherComparison(Comparisons.Comparison comparison) {
        return new OneOfThemWithComparison(getFieldName(), comparison);
    }

    @Nonnull
    @Override
    @API(API.Status.EXPERIMENTAL)
    public Iterator<? extends ExpressionRef<? extends PlannerExpression>> getPlannerExpressionChildren() {
        return Iterators.singletonIterator(this.comparison);
    }

    @Override
    public String toString() {
        return "one of " + getFieldName() + " " + comparison.get();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        OneOfThemWithComparison that = (OneOfThemWithComparison) o;
        return Objects.equals(getComparison(), that.getComparison());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getComparison());
    }

    @Override
    public int planHash() {
        return getComparison().planHash();
    }
}
