/*
 * NullableArrayTypeUtils.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2022 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.cascades;

import com.apple.foundationdb.record.RecordMetaDataProto;
import com.apple.foundationdb.record.metadata.expressions.FieldKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.NestingKeyExpression;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.MessageValue;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * A Utils class that holds logic related to nullable arrays.
 */
public class NullableArrayTypeUtils {
    @Nonnull
    private static final String REPEATED_FIELD_NAME = "values";

    private NullableArrayTypeUtils() {
        throw new IllegalStateException("Utility class");
    }

    /**
     * Get the fieldName of the repeated field.
     *
     * @return fieldName of the repeated field
     */
    @Nonnull
    public static String getRepeatedFieldName() {
        return REPEATED_FIELD_NAME;
    }

    /**
     * Check whether a descriptor describes a wrapped array. Message that looks like:
     * message M {
     * repeated R values = 1;
     * }
     * is considered to be a wrapped array.
     *
     * @param descriptor The protobuf descriptor.
     *
     * @return <code>true</code> if it describes a wrapped array, otherwise <code>false</code>.
     */
    public static boolean describesWrappedArray(@Nonnull Descriptors.Descriptor descriptor) {
        if (descriptor.getFields().size() == 1) {
            Descriptors.FieldDescriptor fieldDescriptor = descriptor.getFields().get(0);
            return fieldDescriptor.isRepeated() && REPEATED_FIELD_NAME.equals(fieldDescriptor.getName());
        } else {
            return false;
        }
    }

    /**
     * Check whether a nesting keyExpression is a wrapped array field.
     *
     * @param nestingKeyExpression the nesting keyExpression.
     *
     * @return <code>true</code> if it describes a wrapped array, otherwise <code>false</code>.
     */
    public static boolean isArrayWrapper(@Nonnull NestingKeyExpression nestingKeyExpression) {
        RecordMetaDataProto.KeyExpression child = nestingKeyExpression.getChild().toKeyExpression();
        if (child.hasNesting()) {
            // if child is Nesting, check child.parent
            RecordMetaDataProto.Field firstChild = child.getNesting().getParent();
            return isWrappedField(firstChild);
        } else if (child.hasField()) {
            // if child is Field, check itself
            return isWrappedField(child.getField());
        }
        return false;
    }

    /**
     * Unwrap nested array in nestingKeyExpression.
     *
     * @param nestingKeyExpression The input keyExpression
     *
     * @return a keyExpression without wrapped array
     */
    @Nonnull
    public static NestingKeyExpression unwrapArrayInNestingKeyExpression(@Nonnull NestingKeyExpression nestingKeyExpression) {
        final FieldKeyExpression parent = nestingKeyExpression.getParent();
        final RecordMetaDataProto.KeyExpression child = nestingKeyExpression.getChild().toKeyExpression();
        RecordMetaDataProto.Nesting.Builder newNestingBuilder = RecordMetaDataProto.Nesting.newBuilder()
                .setParent(parent.toProto().toBuilder().setFanType(RecordMetaDataProto.Field.FanType.FAN_OUT))
                .setChild(child.getNesting().getChild());
        return new NestingKeyExpression(newNestingBuilder.build());
    }

    /**
     * Unwrap nested array in fieldKeyExpression.
     *
     * @param fieldKeyExpression The input keyExpression
     *
     * @return a keyExpression without wrapped array
     */
    @Nonnull
    public static FieldKeyExpression unwrapArrayInFieldKeyExpression(@Nonnull FieldKeyExpression fieldKeyExpression) {
        return new FieldKeyExpression(fieldKeyExpression.toProto().toBuilder().setFanType(RecordMetaDataProto.Field.FanType.FAN_OUT).build());
    }

    /**
     * If the value is a nullable array, unwrap the value wrapper.
     *
     * @param wrappedValue The input value
     * @param type The input type
     *
     * @return The unwrapped value
     */
    @Nullable
    public static Object unwrapIfArray(@Nullable Object wrappedValue, @Nonnull Type type) {
        //
        // If the last step in the field path is an array that is also nullable, then we need to unwrap the value
        // wrapper.
        //
        if (wrappedValue != null && type.getTypeCode() == Type.TypeCode.ARRAY && type.isNullable()) {
            return MessageValue.getFieldOnMessage((Message)wrappedValue, NullableArrayTypeUtils.getRepeatedFieldName());
        }
        return wrappedValue;
    }

    /**
     * Return whether a Field is a wrapped array.
     *
     * @param field The input field
     *
     * @return <code>true</code> if it is a wrapped array, otherwise <code>false</code>.
     */
    private static boolean isWrappedField(@Nonnull RecordMetaDataProto.Field field) {
        return REPEATED_FIELD_NAME.equals(field.getFieldName()) && RecordMetaDataProto.Field.FanType.FAN_OUT.equals(field.getFanType());
    }
}
