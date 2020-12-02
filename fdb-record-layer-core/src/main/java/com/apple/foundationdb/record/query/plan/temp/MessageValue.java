/*
 * MessageValue.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2020 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.temp;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.metadata.expressions.TupleFieldsHelper;
import com.apple.foundationdb.record.query.expressions.Query;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.google.protobuf.MessageOrBuilder;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

/**
 * A utility class for extracting data and meta-data from Protocol Buffer {@link Message}s, as used in the Record Layer.
 */
@API(API.Status.EXPERIMENTAL)
public class MessageValue {
    /**
     * Get the value of the (nested) field on the path from the message defined by {@code fieldNames}.
     * The given field names define a path through the nested structure of the given message; this method traverses
     * that path and returns the value at the leaf, using the return semantics of {@link #getFieldOnMessage(MessageOrBuilder, String)}.
     * @param message a message
     * @param fieldNames a list of field names defining a path starting at {@code message}
     * @return the value at the end of hte path
     */
    @Nullable
    public static Object getFieldValue(@Nonnull MessageOrBuilder message, @Nonnull List<String> fieldNames) {
        if (fieldNames.isEmpty()) {
            throw new RecordCoreException("empty list of field names");
        }
        MessageOrBuilder current = message;
        int fieldNamesIndex;
        // Notice that up to fieldNames.size() - 2 are calling getFieldMessageOnMessage, and fieldNames.size() - 1 is calling getFieldOnMessage
        for (fieldNamesIndex = 0; fieldNamesIndex < fieldNames.size() - 1; fieldNamesIndex++) {
            current = getFieldMessageOnMessage(current, fieldNames.get(fieldNamesIndex));
            if (current == null) {
                return null;
            }
        }
        return getFieldOnMessage(current, fieldNames.get(fieldNames.size() - 1));
    }

    /**
     * Get the value of the field with the given field name on the given message.
     * If the field is repeated, the repeated values are combined into a list. If the field has a message type,
     * the value is returned as a {@link Message} of that type. Otherwise, the field is returned as a primitive.
     * @param message a message or builder to extract the field from
     * @param fieldName the field to extract
     * @return the value of the field as described above
     */
    @Nullable
    public static Object getFieldOnMessage(@Nonnull MessageOrBuilder message, @Nonnull String fieldName) {
        final Descriptors.FieldDescriptor field = findFieldDescriptorOnMessage(message, fieldName);
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

    @Nullable
    private static Message getFieldMessageOnMessage(@Nonnull MessageOrBuilder message, @Nonnull String fieldName) {
        final Descriptors.FieldDescriptor field = findFieldDescriptorOnMessage(message, fieldName);
        if (!field.isRepeated() &&
                (field.hasDefaultValue() || message.hasField(field)) &&
                field.getJavaType() == Descriptors.FieldDescriptor.JavaType.MESSAGE) {
            return (Message)message.getField(field);
        }
        return null;
    }

    @Nonnull
    public static Descriptors.FieldDescriptor findFieldDescriptorOnMessage(@Nonnull MessageOrBuilder message, @Nonnull String fieldName) {
        final Descriptors.FieldDescriptor field = message.getDescriptorForType().findFieldByName(fieldName);
        if (field == null) {
            throw new Query.InvalidExpressionException("Missing field " + fieldName);
        }
        return field;
    }

    private MessageValue() {}
}
