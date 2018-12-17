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

import com.google.protobuf.Descriptors;
import com.google.protobuf.MessageOrBuilder;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;

class BaseRepeatedField extends BaseField {

    public BaseRepeatedField(@Nonnull String fieldName) {
        super(fieldName);
    }

    @Nullable
    @SuppressWarnings("unchecked")
    protected List<Object> getValues(@Nonnull MessageOrBuilder message) {
        final Descriptors.FieldDescriptor field = findFieldDescriptor(message);
        if (message.getRepeatedFieldCount(field) == 0) {
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
}
