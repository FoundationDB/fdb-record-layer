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
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.metadata.expressions.TupleFieldsHelper;
import com.apple.foundationdb.record.query.plan.temp.MessageValue;
import com.google.protobuf.Descriptors;
import com.google.protobuf.MessageOrBuilder;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Objects;

/**
 * An abstract base class for field-like {@link QueryComponent}s that involve predicates on one particular record field,
 * as specified by the {@link #fieldName} member.
 */
@API(API.Status.INTERNAL)
public abstract class BaseField implements PlanHashable, QueryComponent {
    @Nonnull
    private final String fieldName;

    protected BaseField(@Nonnull String fieldName) {
        this.fieldName = fieldName;
    }

    @Nonnull
    protected Descriptors.FieldDescriptor findFieldDescriptor(@Nonnull MessageOrBuilder message) {
        return MessageValue.findFieldDescriptorOnMessage(message, fieldName);
    }

    @Nullable
    protected Object getFieldValue(@Nullable MessageOrBuilder message) {
        if (message == null) {
            return null;
        }
        return MessageValue.getFieldOnMessage(message, fieldName);
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

    /**
     * Base implementation of {@link #planHash}.
     * This implementation makes each concrete subclass implement its own version of {@link #planHash} so that they are
     * guided to add their own class modifier (See {@link com.apple.foundationdb.record.ObjectPlanHash ObjectPlanHash}).
     * This implementation is meant to give subclasses common functionality for their own implementation.
     * @param hashKind the plan hash kind to use
     * @param baseHash the subclass' base hash (concrete identifier)
     * @param hashables the rest of the subclass' hashable parameters (if any)
     * @return the plan hash value calculated
     */
    protected int basePlanHash(@Nonnull final PlanHashKind hashKind, ObjectPlanHash baseHash, Object... hashables) {
        switch (hashKind) {
            case LEGACY:
                return fieldName.hashCode();
            case FOR_CONTINUATION:
            case STRUCTURAL_WITHOUT_LITERALS:
                return PlanHashable.objectsPlanHash(hashKind, baseHash, fieldName, hashables);
            default:
                throw new UnsupportedOperationException("Hash kind " + hashKind.name() + " is not supported");
        }
    }
}
