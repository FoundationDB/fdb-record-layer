/*
 * OneOfThemWithComponent.java
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
import com.google.common.collect.Iterators;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

/**
 * A {@link QueryComponent} that evaluates a nested component against each of the values of a repeated field and is satisfied if any of those are.
 */
@API(API.Status.MAINTAINED)
public class OneOfThemWithComponent extends BaseRepeatedField implements ComponentWithSingleChild {
    @Nonnull
    private final ExpressionRef<QueryComponent> child;

    public OneOfThemWithComponent(@Nonnull String fieldName, @Nonnull QueryComponent child) {
        super(fieldName);
        this.child = SingleExpressionRef.of(child);
    }

    @Override
    @Nullable
    public <M extends Message> Boolean evalMessage(@Nonnull FDBRecordStoreBase<M> store, @Nonnull EvaluationContext context,
                                                   @Nullable FDBRecord<M> record, @Nullable Message message) {
        if (message == null) {
            return null;
        }
        List<Object> values = getValues(message);
        if (values == null) {
            return null;
        } else {
            for (Object value : values) {
                if (value != null) {
                    if (value instanceof Message) {
                        final Boolean val = getChild().evalMessage(store, context, record, (Message) value);
                        if (val != null && val) {
                            return true;
                        }
                    }
                }
            }
        }
        return false;
    }

    @Override
    public void validate(@Nonnull Descriptors.Descriptor descriptor) {
        final Descriptors.FieldDescriptor field = validateRepeatedField(descriptor);
        final QueryComponent component = getChild();
        requireMessageField(field);
        component.validate(field.getMessageType());
    }

    @Override
    @Nonnull
    public QueryComponent getChild() {
        return child.get();
    }

    @Override
    @Nonnull
    @API(API.Status.EXPERIMENTAL)
    public Iterator<? extends ExpressionRef<? extends PlannerExpression>> getPlannerExpressionChildren() {
        return Iterators.singletonIterator(this.child);
    }

    @Override
    public QueryComponent withOtherChild(QueryComponent newChild) {
        return new OneOfThemWithComponent(getFieldName(), newChild);
    }

    @Override
    public String toString() {
        return "one of " + getFieldName() + "/{" + getChild() + "}";
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
        OneOfThemWithComponent that = (OneOfThemWithComponent) o;
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
