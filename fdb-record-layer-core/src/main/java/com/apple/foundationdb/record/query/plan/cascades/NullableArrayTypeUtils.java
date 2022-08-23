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
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.metadata.expressions.NestingKeyExpression;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.MessageValue;
import com.google.common.base.Verify;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * A Utils class that holds logic related to nullable arrays.
 */
public class NullableArrayTypeUtils {
    private static final String repeatedFieldName = "values";

    /**
     * Get the fieldName of the repeated field.
     *
     * @return fieldName of the repeated field
     */
    public static String getRepeatedFieldName() {
        return repeatedFieldName;
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
    public static boolean describesWrappedArray(Descriptors.Descriptor descriptor) {
        if (descriptor.getFields().size() == 1) {
            Descriptors.FieldDescriptor fieldDescriptor = descriptor.getFields().get(0);
            return fieldDescriptor.isRepeated() && repeatedFieldName.equals(fieldDescriptor.getName());
        } else {
            return false;
        }
    }

    /**
     * Unwrap nested array in keyExpression.
     *
     * @param nestingKeyExpression The input keyExpression
     *
     * @return a keyExpression without wrapped array
     */
    public static NestingKeyExpression unwrapArrayInKeyExpression(NestingKeyExpression nestingKeyExpression) {
        final FieldKeyExpression parent = nestingKeyExpression.getParent();
        final KeyExpression child = nestingKeyExpression.getChild();
        if (KeyExpression.FanType.None.equals(parent.getFanType()) && child.toKeyExpression().hasNesting()) {
            RecordMetaDataProto.Field firstChild = child.toKeyExpression().getNesting().getParent();
            if (repeatedFieldName.equals(firstChild.getFieldName()) && RecordMetaDataProto.Field.FanType.FAN_OUT.equals(firstChild.getFanType())) {
                // remove firstChild from the KeyExpression
                RecordMetaDataProto.KeyExpression newChild = child.toKeyExpression().getNesting().getChild();
                RecordMetaDataProto.Nesting.Builder newNestingBuilder;
                if (newChild.hasNesting()) {
                    // recursively remove all wrapped array
                    NestingKeyExpression unwrappedChild = unwrapArrayInKeyExpression(new NestingKeyExpression(newChild.getNesting()));
                    newNestingBuilder = RecordMetaDataProto.Nesting.newBuilder()
                            .setParent(parent.toProto().toBuilder().setFanType(RecordMetaDataProto.Field.FanType.FAN_OUT))
                            .setChild(unwrappedChild.toKeyExpression());
                } else {
                    newNestingBuilder = RecordMetaDataProto.Nesting.newBuilder()
                            .setParent(parent.toProto().toBuilder().setFanType(RecordMetaDataProto.Field.FanType.FAN_OUT))
                            .setChild(newChild);
                }
                return new NestingKeyExpression(newNestingBuilder.build());
            }
        }
        return nestingKeyExpression;
    }

    @Nullable
    public static Object unwrapIfArray(@Nullable Object wrappedValue, @Nonnull Type type) {
        //
        // If the last step in the field path is an array that is also nullable, then we need to unwrap the value
        // wrapper.
        //
        if (wrappedValue != null && type.getTypeCode() == Type.TypeCode.ARRAY && type.isNullable()) {
            final var arrayType = (Type.Array)type;
            Verify.verify(arrayType.needsWrapper());

            return MessageValue.getFieldOnMessage((Message)wrappedValue, NullableArrayTypeUtils.getRepeatedFieldName());
        }
        return wrappedValue;
    }
}
