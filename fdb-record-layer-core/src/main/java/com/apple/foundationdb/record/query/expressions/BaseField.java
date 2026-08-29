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
import com.apple.foundationdb.record.query.plan.cascades.values.MessageHelpers;
import com.google.protobuf.Descriptors;
import com.google.protobuf.MessageOrBuilder;

import org.jspecify.annotations.Nullable;

import java.util.Objects;

/**
 * An abstract base class for field-like {@link QueryComponent}s that involve predicates on one particular record field,
 * as specified by the {@link #fieldName} member.
 */
@API(API.Status.INTERNAL)
public abstract class BaseField implements PlanHashable, QueryComponent {
    private final String fieldName;

    protected BaseField(String fieldName) {
        this.fieldName = fieldName;
    }

    protected Descriptors.FieldDescriptor findFieldDescriptor(MessageOrBuilder message) {
        return MessageHelpers.findFieldDescriptorOnMessage(message, fieldName);
    }

    @Nullable
    protected Object getFieldValue(@Nullable MessageOrBuilder message) {
        if (message == null) {
            return null;
        }
        return MessageHelpers.getFieldOnMessage(message, fieldName);
    }

    protected Descriptors.FieldDescriptor validateFieldExistence(Descriptors.Descriptor descriptor) {
        Descriptors.FieldDescriptor field = descriptor.findFieldByName(fieldName);
        if (field == null) {
            throw new Query.InvalidExpressionException("Missing field " + fieldName);
        }
        return field;
    }

    protected void requirePrimitiveField(Descriptors.FieldDescriptor field) {
        if (field.getType() == Descriptors.FieldDescriptor.Type.MESSAGE && !TupleFieldsHelper.isTupleField(field.getMessageType())) {
            throw new Query.InvalidExpressionException("Required primitive field, but got message " + fieldName);
        }
    }

    protected void requireMessageField(Descriptors.FieldDescriptor field) {
        if (field.getType() != Descriptors.FieldDescriptor.Type.MESSAGE || TupleFieldsHelper.isTupleField(field.getMessageType())) {
            throw new Query.InvalidExpressionException("Required nested field, but got primitive field " + fieldName);
        }
    }

    protected void requireScalarField(Descriptors.FieldDescriptor field) {
        if (field.isRepeated()) {
            throw new Query.InvalidExpressionException("Required scalar field, but got repeated field " + fieldName);
        }
    }

    public String getName() {
        return getFieldName();
    }

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
     * Base implementation of {@link #planHash(PlanHashMode)}.
     * This implementation makes each concrete subclass implement its own version of {@link #planHash(PlanHashMode)}
     * so that they are guided to add their own class modifier (See {@link ObjectPlanHash ObjectPlanHash}).
     * This implementation is meant to give subclasses common functionality for their own implementation.
     * @param mode the plan hash kind to use
     * @param baseHash the subclass' base hash (concrete identifier)
     * @param hashables the rest of the subclass' hashable parameters (if any)
     * @return the plan hash value calculated
     */
    protected int basePlanHash(final PlanHashMode mode, ObjectPlanHash baseHash, Object... hashables) {
        switch (mode.getKind()) {
            case LEGACY:
                return fieldName.hashCode();
            case FOR_CONTINUATION:
                return PlanHashable.objectsPlanHash(mode, baseHash, fieldName, hashables);
            default:
                throw new UnsupportedOperationException("Hash kind " + mode.getKind() + " is not supported");
        }
    }
}
