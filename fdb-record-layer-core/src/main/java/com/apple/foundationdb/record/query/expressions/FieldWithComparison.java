/*
 * FieldWithComparison.java
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

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.plan.temp.ExpressionRef;
import com.apple.foundationdb.record.query.plan.temp.PlannerExpression;
import com.apple.foundationdb.record.query.plan.temp.view.Element;
import com.apple.foundationdb.record.query.plan.temp.view.FieldElement;
import com.apple.foundationdb.record.query.plan.temp.view.Source;
import com.apple.foundationdb.record.query.predicates.ElementPredicate;
import com.apple.foundationdb.record.query.predicates.QueryPredicate;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.google.protobuf.MessageOrBuilder;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Iterator;
import java.util.Objects;
import java.util.function.Function;

/**
 * A {@link QueryComponent} that implements a {@link com.apple.foundationdb.record.query.expressions.Comparisons.Comparison} against a field of the record.
 */
@API(API.Status.MAINTAINED)
public class FieldWithComparison extends BaseField implements ComponentWithComparison {
    @Nonnull
    private final Comparisons.Comparison comparison;

    public FieldWithComparison(@Nonnull String fieldName, @Nonnull Comparisons.Comparison comparison) {
        super(fieldName);
        this.comparison = comparison;
    }

    @Override
    @Nullable
    public <M extends Message> Boolean evalMessage(@Nonnull FDBRecordStoreBase<M> store, @Nonnull EvaluationContext context,
                                                   @Nullable FDBRecord<M> record, @Nullable Message message) {
        if (message == null) {
            getComparison().eval(store, context, null);
        }
        final Object value = getFieldValue(message);
        if (value == null) {
            return getComparison().eval(store, context, null);
        } else if (value instanceof MessageOrBuilder && !allowWholeMessage()) {
            throw new Query.InvalidExpressionException("Expression requiring primitive found a message value");
        } else {
            return getComparison().eval(store, context, value);
        }
    }

    @Override
    public void validate(@Nonnull Descriptors.Descriptor descriptor) {
        final Descriptors.FieldDescriptor field = super.validateFieldExistence(descriptor);
        if (!allowWholeMessage()) {
            requirePrimitiveField(field);
        }
        getComparison().validate(field, false);
    }

    private boolean allowWholeMessage() {
        // Can check nullity of a nested message as well as of a field in it.
        return getComparison().getType() == Comparisons.Type.IS_NULL || getComparison().getType() == Comparisons.Type.NOT_NULL;
    }

    @Override
    @Nonnull
    public Comparisons.Comparison getComparison() {
        return this.comparison;
    }

    @Nonnull
    @Override
    @API(API.Status.EXPERIMENTAL)
    public Iterator<? extends ExpressionRef<? extends PlannerExpression>> getPlannerExpressionChildren() {
        return Collections.emptyIterator();
    }

    @Override
    public String toString() {
        return getFieldName() + " " + getComparison();
    }

    @Override
    @API(API.Status.EXPERIMENTAL)
    public boolean equalsWithoutChildren(@Nonnull PlannerExpression otherExpression) {
        return equals(otherExpression);
    }

    @Nonnull
    @Override
    public QueryPredicate normalizeForPlanner(@Nonnull Source rootSource, @Nonnull Function<Element, Element> elementModifier) {
        return new ElementPredicate(elementModifier.apply(new FieldElement(rootSource, getFieldName())), comparison);
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
        FieldWithComparison that = (FieldWithComparison) o;
        return Objects.equals(getComparison(), that.getComparison());
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), getComparison());
    }

    @Override
    public int planHash() {
        return super.planHash() + getComparison().planHash();
    }

    @Override
    public QueryComponent withOtherComparison(Comparisons.Comparison comparison) {
        return new FieldWithComparison(getFieldName(), comparison);
    }
}
