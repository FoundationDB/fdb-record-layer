/*
 * BaseField.java
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
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.metadata.expressions.TupleFieldsHelper;
import com.apple.foundationdb.record.query.plan.temp.ExpressionRef;
import com.apple.foundationdb.record.query.plan.temp.NestedContext;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.google.protobuf.MessageOrBuilder;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * An abstract base class for field-like {@link QueryComponent}s that involve predicates on one particular record field,
 * as specified by the {@link #fieldName} member.
 */
public abstract class BaseField implements PlanHashable, QueryComponent {
    @Nonnull
    private final String fieldName;

    protected BaseField(@Nonnull String fieldName) {
        this.fieldName = fieldName;
    }

    @Nonnull
    protected Descriptors.FieldDescriptor findFieldDescriptor(@Nonnull MessageOrBuilder message) {
        final Descriptors.FieldDescriptor field = message.getDescriptorForType().findFieldByName(fieldName);
        if (field == null) {
            throw new Query.InvalidExpressionException("Missing field " + fieldName);
        }
        return field;
    }

    @Nullable
    protected Object getFieldValue(@Nullable MessageOrBuilder message) {
        if (message == null) {
            return null;
        }
        final Descriptors.FieldDescriptor field = findFieldDescriptor(message);
        if (field.isRepeated()) {
            int count = message.getRepeatedFieldCount(field);
            List<Object> list = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                list.add(message.getRepeatedField(field, i));
            }
            return list;
        }
        if (field.hasDefaultValue() || message.hasField(field)) {
            if (field.getJavaType() == Descriptors.FieldDescriptor.JavaType.MESSAGE &&
                    TupleFieldsHelper.isTupleField(field.getMessageType())) {
                return TupleFieldsHelper.fromProto((Message)message.getField(field), field.getMessageType());
            } else {
                return message.getField(field);
            }
        } else {
            return null;
        }
    }

    @Nonnull
    protected Descriptors.FieldDescriptor validateFieldExistence(@Nonnull Descriptors.Descriptor descriptor) {
        Descriptors.FieldDescriptor field = descriptor.findFieldByName(fieldName);
        if (field == null) {
            throw new Query.InvalidExpressionException("Missing field " + fieldName);
        }
        return field;
    }

    protected void requirePrimitiveField(@Nonnull Descriptors.FieldDescriptor field) {
        if (field.getType() == Descriptors.FieldDescriptor.Type.MESSAGE && !TupleFieldsHelper.isTupleField(field.getMessageType())) {
            throw new Query.InvalidExpressionException("Required primitive field, but got message " + fieldName);
        }
    }

    protected void requireMessageField(@Nonnull Descriptors.FieldDescriptor field) {
        if (field.getType() != Descriptors.FieldDescriptor.Type.MESSAGE || TupleFieldsHelper.isTupleField(field.getMessageType())) {
            throw new Query.InvalidExpressionException("Required nested field, but got primitive field " + fieldName);
        }
    }

    protected void requireScalarField(@Nonnull Descriptors.FieldDescriptor field) {
        if (field.isRepeated()) {
            throw new Query.InvalidExpressionException("Required scalar field, but got repeated field " + fieldName);
        }
    }

    @Nonnull
    public String getName() {
        return getFieldName();
    }

    @Nonnull
    public String getFieldName() {
        return fieldName;
    }

    @Nullable
    @Override
    @API(API.Status.EXPERIMENTAL)
    public ExpressionRef<QueryComponent> asUnnestedWith(@Nonnull NestedContext nestedContext,
                                                        @Nonnull ExpressionRef<QueryComponent> thisRef) {
        return unnestedWith(nestedContext, thisRef);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof BaseField)) {
            return false;
        }
        BaseField baseField = (BaseField) o;
        return Objects.equals(fieldName, baseField.fieldName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fieldName);
    }

    @Override
    public int planHash() {
        return fieldName.hashCode();
    }

    @Nonnull
    @API(API.Status.EXPERIMENTAL)
    public static ExpressionRef<QueryComponent> unnestedWith(@Nonnull NestedContext nestedContext,
                                                             @Nonnull ExpressionRef<QueryComponent> thisRef) {
        final QueryComponent nest;
        if (nestedContext.isParentFieldRepeated()) {
            nest = new OneOfThemWithComponent(nestedContext.getParentField().getFieldName(), thisRef);
        } else {
            nest = new NestedField(nestedContext.getParentField().getFieldName(), thisRef);
        }
        return thisRef.getNewRefWith(nest);
    }
}
