/*
 * NestedField.java
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
import com.apple.foundationdb.record.query.plan.temp.SingleExpressionRef;
import com.apple.foundationdb.record.query.plan.temp.view.Source;
import com.apple.foundationdb.record.query.predicates.QueryPredicate;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;

/**
 * A {@link QueryComponent} that evaluates a nested component against a {@link com.google.protobuf.Message}-valued field.
 */
@API(API.Status.MAINTAINED)
public class NestedField extends BaseNestedField {

    public NestedField(@Nonnull String fieldName, @Nonnull QueryComponent childComponent) {
        this(fieldName, SingleExpressionRef.of(childComponent));
    }

    @API(API.Status.EXPERIMENTAL)
    public NestedField(@Nonnull String fieldName, @Nonnull ExpressionRef<QueryComponent> childComponent) {
        super(fieldName, childComponent);
    }

    @Override
    @Nullable
    public <M extends Message> Boolean evalMessage(@Nonnull FDBRecordStoreBase<M> store, @Nonnull EvaluationContext context,
                                                   @Nullable FDBRecord<M> record, @Nullable Message message) {
        final QueryComponent component = getChild();
        if (message == null) {
            return component.evalMessage(store, context, record, null);
        }
        final Object value = getFieldValue(message);
        if (value == null) {
            return component.evalMessage(store, context, record, null);
        } else if (value instanceof Message) {
            return component.evalMessage(store, context, record, (Message) value);
        } else {
            throw new Query.InvalidExpressionException("Expression requiring nesting found a non-message value");
        }
    }

    @Override
    public void validate(@Nonnull Descriptors.Descriptor descriptor) {
        final Descriptors.FieldDescriptor field = super.validateFieldExistence(descriptor);
        final QueryComponent child = getChild();
        requireMessageField(field);
        requireScalarField(field);
        child.validate(field.getMessageType());
    }

    @Override
    public QueryComponent withOtherChild(QueryComponent newChild) {
        if (newChild == getChild()) {
            return this;
        }
        return new NestedField(getFieldName(), newChild);
    }

    @Override
    public String toString() {
        return getFieldName() + "/{" + getChild() + "}";
    }

    @Nonnull
    @Override
    public QueryPredicate normalizeForPlanner(@Nonnull Source rootSource, @Nonnull List<String> fieldNamePrefix) {
        ImmutableList<String> fieldNames = ImmutableList.<String>builder()
                .addAll(fieldNamePrefix)
                .add(getFieldName())
                .build();
        return childComponent.get().normalizeForPlanner(rootSource, fieldNames);
    }

    @Override
    @API(API.Status.EXPERIMENTAL)
    public boolean equalsWithoutChildren(@Nonnull PlannerExpression otherExpression) {
        return otherExpression instanceof NestedField &&
               ((NestedField)otherExpression).getFieldName().equals(getFieldName());
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
        NestedField that = (NestedField) o;
        return Objects.equals(getChild(), that.getChild());
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), getChild());
    }

    @Override
    public int planHash() {
        return super.planHash() + getChild().planHash();
    }

}
