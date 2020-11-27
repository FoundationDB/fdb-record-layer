/*
 * BaseRepeatedField.java
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

import com.apple.foundationdb.record.PlanHashable;
import com.google.protobuf.Descriptors;
import com.google.protobuf.MessageOrBuilder;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;

abstract class BaseRepeatedField extends BaseField {
    @Nonnull
    private final Field.OneOfThemEmptyMode emptyMode;

    public BaseRepeatedField(@Nonnull String fieldName, Field.OneOfThemEmptyMode emptyMode) {
        super(fieldName);
        this.emptyMode = emptyMode;
    }

    @Nonnull
    public Field.OneOfThemEmptyMode getEmptyMode() {
        return emptyMode;
    }

    @Nullable
    @SuppressWarnings("unchecked")
    protected List<Object> getValues(@Nonnull MessageOrBuilder message) {
        final Descriptors.FieldDescriptor field = findFieldDescriptor(message);
        if (emptyMode == Field.OneOfThemEmptyMode.EMPTY_UNKNOWN && message.getRepeatedFieldCount(field) == 0) {
            return null;
        } else {
            return (List<Object>) message.getField(field);
        }
    }

    @Nonnull
    protected Descriptors.FieldDescriptor validateRepeatedField(@Nonnull Descriptors.Descriptor descriptor) {
        final Descriptors.FieldDescriptor field = validateFieldExistence(descriptor);
        if (!field.isRepeated()) {
            throw new Query.InvalidExpressionException("Expected repeated field, but it was scalar " + getFieldName());
        }
        return field;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof BaseRepeatedField)) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        BaseRepeatedField baseRepeatedField = (BaseRepeatedField) o;
        return emptyMode == baseRepeatedField.emptyMode;
    }

    @Override
    public int hashCode() {
        return super.hashCode() + emptyMode.hashCode();
    }

    @Override
    public int planHash(@Nonnull final PlanHashKind hashKind) {
        switch (hashKind) {
            case LEGACY:
                return super.planHash(hashKind) + emptyMode.ordinal();
            case FOR_CONTINUATION:
            case STRUCTURAL_WITHOUT_LITERALS:
                // No BASE_HASH since this is abstract
                return PlanHashable.objectsPlanHash(hashKind, super.planHash(hashKind), emptyMode.ordinal());
            default:
                throw new UnsupportedOperationException("Hash kind " + hashKind.name() + " is not supported");
        }
    }
}
