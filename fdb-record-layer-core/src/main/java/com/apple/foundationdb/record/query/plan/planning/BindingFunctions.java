/*
 * BindingFunctions.java
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

package com.apple.foundationdb.record.query.plan.planning;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.record.query.expressions.FieldWithComparison;
import com.apple.foundationdb.record.query.expressions.NestedField;
import com.apple.foundationdb.record.query.expressions.QueryComponent;
import com.apple.foundationdb.tuple.Tuple;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.function.Function;

/**
 * Helper methods for manipulating parameter values passed in {@link com.apple.foundationdb.record.Bindings}.
 */
@API(API.Status.INTERNAL)
public class BindingFunctions {
    private BindingFunctions() {
    }

    /**
     * Get a function for extracting a comparison value from a <code>Tuple</code> based on the type of the field
     * being compared. The most important case this covers is a <code>Long</code> tuple element that needs to
     * be compared with an <code>int32</code> field.
     */
    @Nonnull
    static Function<Tuple, Object> comparisonBindingFunction(@Nonnull QueryComponent fieldComparison,
                                                             @Nonnull Index index, @Nonnull RecordMetaData metaData) {
        final Descriptors.FieldDescriptor.JavaType javaType = javaComparisonType(fieldComparison, index, metaData);
        if (javaType != null) {
            switch (javaType) {
                case INT:
                    return tuple -> {
                        Long value = (Long)tuple.get(0);
                        return value == null ? null : value.intValue();
                    };
                case LONG:
                    return tuple -> (Long)tuple.get(0);
                case FLOAT:
                    return tuple -> {
                        Double value = (Double)tuple.get(0);
                        return value == null ? null : value.floatValue();
                    };
                case DOUBLE:
                    return tuple -> (Double)tuple.get(0);
                case BOOLEAN:
                    return tuple -> (Boolean)tuple.get(0);
                case STRING:
                    return tuple -> (String)tuple.get(0);
                case BYTE_STRING:
                    return tuple -> {
                        byte[] value = (byte[])tuple.get(0);
                        return value == null ? null : ByteString.copyFrom(value);
                    };
                default:
                    break;
            }
        }
        return t -> t.get(0);
    }

    @Nullable
    private static Descriptors.FieldDescriptor.JavaType javaComparisonType(@Nonnull QueryComponent fieldComparison,
                                                                           @Nonnull Index index, @Nonnull RecordMetaData metaData) {
        Descriptors.FieldDescriptor.JavaType javaType = null;
        for (RecordType recordType : metaData.recordTypesForIndex(index)) {
            Descriptors.Descriptor descriptor = recordType.getDescriptor();
            QueryComponent component = fieldComparison;
            while (component instanceof NestedField) {
                Descriptors.FieldDescriptor fieldDescriptor = descriptor.findFieldByName(((NestedField)component).getName());
                if (fieldDescriptor == null) {
                    return null;
                }
                descriptor = fieldDescriptor.getMessageType();
                component = ((NestedField)component).getChild();
            }
            if (component instanceof FieldWithComparison) {
                Descriptors.FieldDescriptor fieldDescriptor = descriptor.findFieldByName(((FieldWithComparison)component).getName());
                if (fieldDescriptor == null) {
                    return null;
                }
                if (javaType == null) {
                    javaType = fieldDescriptor.getJavaType();
                } else if (javaType != fieldDescriptor.getJavaType()) {
                    return null;
                }
            } else {
                return null;
            }
        }
        return javaType;
    }
}
