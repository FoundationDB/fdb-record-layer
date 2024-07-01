/*
 * BindingFunction.java
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
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.record.planprotos.PScoreForRank.PBindingFunction;
import com.apple.foundationdb.record.query.expressions.FieldWithComparison;
import com.apple.foundationdb.record.query.expressions.NestedField;
import com.apple.foundationdb.record.query.expressions.QueryComponent;
import com.apple.foundationdb.tuple.Tuple;
import com.google.protobuf.Descriptors;
import com.google.protobuf.ZeroCopyByteString;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.function.Function;

/**
 * Helper methods for manipulating parameter values passed in {@link com.apple.foundationdb.record.Bindings}.
 */
@SuppressWarnings("RedundantCast")
@API(API.Status.INTERNAL)
public enum BindingFunction {
    INT(tuple -> {
        Long value = (Long)tuple.get(0);
        return value == null ? null : value.intValue();
    }),
    LONG(tuple -> (Long)tuple.get(0)),
    FLOAT(tuple -> {
        Double value = (Double)tuple.get(0);
        return value == null ? null : value.floatValue();
    }),
    DOUBLE(tuple -> (Double)tuple.get(0)),
    BOOLEAN(tuple -> (Boolean)tuple.get(0)),
    STRING(tuple -> (String)tuple.get(0)),
    BYTE_STRING(tuple -> {
        byte[] value = (byte[])tuple.get(0);
        return value == null ? null : ZeroCopyByteString.wrap(value);
    }),
    TUPLE(tuple -> tuple.get(0));

    @Nonnull
    private Function<Tuple, Object> function;

    BindingFunction(@Nonnull final Function<Tuple, Object> function) {
        this.function = function;
    }

    @Nullable
    public Object apply(@Nonnull final Tuple tuple) {
        return function.apply(tuple);
    }

    @Nonnull
    public PBindingFunction toProto(@Nonnull final PlanSerializationContext serializationContext) {
        switch (this) {
            case INT:
                return PBindingFunction.INT;
            case LONG:
                return PBindingFunction.LONG;
            case FLOAT:
                return PBindingFunction.FLOAT;
            case DOUBLE:
                return PBindingFunction.DOUBLE;
            case BOOLEAN:
                return PBindingFunction.BOOLEAN;
            case STRING:
                return PBindingFunction.STRING;
            case BYTE_STRING:
                return PBindingFunction.BYTE_STRING;
            case TUPLE:
                return PBindingFunction.TUPLE;
            default:
                throw new RecordCoreException("unknown binding function mapping. did you forget to add it?");
        }
    }

    @Nonnull
    @SuppressWarnings("unused")
    public static BindingFunction fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                            @Nonnull final PBindingFunction bindingFunctionProto) {
        switch (bindingFunctionProto) {
            case INT:
                return INT;
            case LONG:
                return LONG;
            case FLOAT:
                return FLOAT;
            case DOUBLE:
                return DOUBLE;
            case BOOLEAN:
                return BOOLEAN;
            case STRING:
                return STRING;
            case BYTE_STRING:
                return BYTE_STRING;
            case TUPLE:
                return TUPLE;
            default:
                throw new RecordCoreException("unknown binding function mapping. did you forget to add it?");
        }
    }

    /**
     * Get a function for extracting a comparison value from a <code>Tuple</code> based on the type of the field
     * being compared. The most important case this covers is a <code>Long</code> tuple element that needs to
     * be compared with an <code>int32</code> field.
     */
    @Nonnull
    static BindingFunction comparisonBindingFunction(@Nonnull final QueryComponent fieldComparison,
                                                     @Nonnull final Index index, @Nonnull final RecordMetaData metaData) {
        final Descriptors.FieldDescriptor.JavaType javaType = javaComparisonType(fieldComparison, index, metaData);
        if (javaType != null) {
            switch (javaType) {
                case INT:
                    return INT;
                case LONG:
                    return LONG;
                case FLOAT:
                    return FLOAT;
                case DOUBLE:
                    return DOUBLE;
                case BOOLEAN:
                    return BOOLEAN;
                case STRING:
                    return STRING;
                case BYTE_STRING:
                    return BYTE_STRING;
                default:
                    break;
            }
        }
        return TUPLE;
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
