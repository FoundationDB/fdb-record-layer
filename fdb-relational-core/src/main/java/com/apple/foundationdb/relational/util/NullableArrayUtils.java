/*
 * NullableArrayUtils.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2024 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.util;

import com.apple.foundationdb.record.RecordMetaDataProto;

import com.google.protobuf.Descriptors;

/**
 * A Utils class that holds logic related to nullable arrays.
 * Nullable Arrays are arrays that, if unset, will be NULL.
 * Non-nullable arrays are arrays that, if unset, will be empty list.
 */
public final class NullableArrayUtils {
    private static final String REPEATED_FIELD_NAME = "values";

    private NullableArrayUtils() {
        throw new IllegalStateException("Utility class");
    }

    public static String getRepeatedFieldName() {
        return REPEATED_FIELD_NAME;
    }

    public static boolean isWrappedArrayDescriptor(String fieldName, Descriptors.Descriptor parentDescriptor) {
        try {
            Descriptors.Descriptor childDescriptor = parentDescriptor.findFieldByName(fieldName).getMessageType();
            return isWrappedArrayDescriptor(childDescriptor);
        } catch (Exception ex) {
            return false;
        }
    }

    public static boolean isWrappedArrayDescriptor(Descriptors.Descriptor descriptor) {
        return descriptor.getFields().size() == 1 && REPEATED_FIELD_NAME.equals(descriptor.getFields().get(0).getName()) && descriptor.findFieldByName(REPEATED_FIELD_NAME).isRepeated();
    }

    /*
    Add the wrapped array structure in key expressions if the schema doesn't contain non-nullable array.
    For example, reviews.rating -> reviews.values.rating
    (TODO): Add the wrapped array structure for nullable arrays.
     */
    public static RecordMetaDataProto.KeyExpression wrapArray(RecordMetaDataProto.KeyExpression original, Descriptors.Descriptor parentDescriptor, boolean containsNonNullableArray) {
        if (containsNonNullableArray) {
            return original;
        }
        if (original.hasThen()) {
            RecordMetaDataProto.Then.Builder newThenBuilder = RecordMetaDataProto.Then.newBuilder();
            for (RecordMetaDataProto.KeyExpression k : original.getThen().getChildList()) {
                newThenBuilder.addChild(wrapArray(k, parentDescriptor, containsNonNullableArray));
            }
            return RecordMetaDataProto.KeyExpression.newBuilder().setThen(newThenBuilder).build();
        } else if (original.hasNesting()) {
            RecordMetaDataProto.Field parent = original.getNesting().getParent();
            RecordMetaDataProto.KeyExpression child = original.getNesting().getChild();
            if (NullableArrayUtils.isWrappedArrayDescriptor(parent.getFieldName(), parentDescriptor)) {
                RecordMetaDataProto.Nesting nestedParent = wrapArray(parent);
                RecordMetaDataProto.KeyExpression newChild = RecordMetaDataProto.KeyExpression.newBuilder()
                        .setNesting(RecordMetaDataProto.Nesting.newBuilder()
                                .setParent(nestedParent.getChild().getField())
                                .setChild(wrapArray(child, parentDescriptor.findFieldByName(parent.getFieldName()).getMessageType().findFieldByName(getRepeatedFieldName()).getMessageType(), containsNonNullableArray))).build();
                return RecordMetaDataProto.KeyExpression.newBuilder().setNesting(RecordMetaDataProto.Nesting.newBuilder().setParent(nestedParent.getParent()).setChild(newChild)).build();
            } else {
                return RecordMetaDataProto.KeyExpression.newBuilder().setNesting(RecordMetaDataProto.Nesting.newBuilder().setParent(parent).setChild(wrapArray(child, parentDescriptor.findNestedTypeByName(parent.getFieldName()), containsNonNullableArray))).build();
            }
        } else if (original.hasField()) {
            if (NullableArrayUtils.isWrappedArrayDescriptor(original.getField().getFieldName(), parentDescriptor)) {
                return RecordMetaDataProto.KeyExpression.newBuilder().setNesting(wrapArray(original.getField())).build();
            } else {
                return original;
            }
        } else if (original.hasGrouping()) {
            RecordMetaDataProto.KeyExpression newWholeKey = wrapArray(original.getGrouping().getWholeKey(), parentDescriptor, containsNonNullableArray);
            return RecordMetaDataProto.KeyExpression.newBuilder().setGrouping(original.getGrouping().toBuilder().setWholeKey(newWholeKey)).build();
        } else if (original.hasSplit()) {
            RecordMetaDataProto.KeyExpression newJoined = wrapArray(original.getSplit().getJoined(), parentDescriptor, containsNonNullableArray);
            return RecordMetaDataProto.KeyExpression.newBuilder().setSplit(original.getSplit().toBuilder().setJoined(newJoined)).build();
        } else if (original.hasFunction()) {
            RecordMetaDataProto.KeyExpression newArguments = wrapArray(original.getFunction().getArguments(), parentDescriptor, containsNonNullableArray);
            return RecordMetaDataProto.KeyExpression.newBuilder().setFunction(original.getFunction().toBuilder().setArguments(newArguments)).build();
        } else if (original.hasKeyWithValue()) {
            RecordMetaDataProto.KeyExpression newInnerKey = wrapArray(original.getKeyWithValue().getInnerKey(), parentDescriptor, containsNonNullableArray);
            return RecordMetaDataProto.KeyExpression.newBuilder().setKeyWithValue(original.getKeyWithValue().toBuilder().setInnerKey(newInnerKey)).build();
        } else if (original.hasList()) {
            RecordMetaDataProto.List.Builder newListBuilder = RecordMetaDataProto.List.newBuilder();
            for (RecordMetaDataProto.KeyExpression k : original.getList().getChildList()) {
                newListBuilder.addChild(wrapArray(k, parentDescriptor, containsNonNullableArray));
            }
            return RecordMetaDataProto.KeyExpression.newBuilder().setList(newListBuilder).build();
        } else {
            return original;
        }
    }

    // wrap repeated fields in a Field type keyExpression
    private static RecordMetaDataProto.Nesting wrapArray(RecordMetaDataProto.Field original) {
        RecordMetaDataProto.Field.Builder nestedArrayBuilder = RecordMetaDataProto.Field.newBuilder()
                .setFieldName(original.getFieldName())
                .setFanType(RecordMetaDataProto.Field.FanType.SCALAR)
                .setNullInterpretation(original.getNullInterpretation());
        RecordMetaDataProto.KeyExpression.Builder arrayValueBuilder = RecordMetaDataProto.KeyExpression.newBuilder()
                .setField(RecordMetaDataProto.Field.newBuilder().setFieldName(getRepeatedFieldName()).setFanType(original.getFanType()).setNullInterpretation(original.getNullInterpretation()));
        return RecordMetaDataProto.Nesting.newBuilder().setParent(nestedArrayBuilder).setChild(arrayValueBuilder).build();
    }
}
