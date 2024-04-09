/*
 * LiteralsUtils.java
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

package com.apple.foundationdb.relational.recordlayer.query;

import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.typing.TypeRepository;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.serialization.PlanSerialization;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.continuation.LiteralObject;
import com.apple.foundationdb.relational.util.Assert;

import com.google.common.collect.Lists;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.ZeroCopyByteString;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.sql.Array;
import java.sql.Struct;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static com.apple.foundationdb.relational.api.exceptions.ErrorCode.CANNOT_CONVERT_TYPE;
import static com.apple.foundationdb.relational.api.exceptions.ErrorCode.DATATYPE_MISMATCH;
import static com.apple.foundationdb.relational.api.exceptions.ErrorCode.INTERNAL_ERROR;

public final class LiteralsUtils {
    private LiteralsUtils() {
        // prevent instantiation
    }

    @Nonnull
    public static Type.Array resolveArrayTypeFromObjectsList(List<Object> objects) {
        return resolveArrayTypeFromElementTypes(
                objects.stream().map(o -> {
                    if (o instanceof byte[]) {
                        return Type.fromObject(ZeroCopyByteString.wrap((byte[]) o));
                    } else if (o instanceof Struct || o instanceof Array) {
                        throw new RelationalException("Array of complex types are not yet supported", CANNOT_CONVERT_TYPE)
                                .toUncheckedWrappedException();
                    } else {
                        return Type.fromObject(o);
                    }
                }).collect(Collectors.toList()));
    }

    public static Type.Array resolveArrayTypeFromValues(List<Value> values) {
        return resolveArrayTypeFromElementTypes(values.stream().map(Value::getResultType).collect(Collectors.toList()));
    }

    private static Type.Array resolveArrayTypeFromElementTypes(List<Type> types) {
        Type elementType;
        if (types.isEmpty()) {
            elementType = Type.nullType();
        } else {
            // all values must have the same type.
            final var distinctTypes = types.stream().filter(type -> type != Type.nullType()).distinct().collect(Collectors.toList());
            Assert.thatUnchecked(distinctTypes.size() == 1, "could not determine type of array literal", DATATYPE_MISMATCH);
            elementType = distinctTypes.get(0);
        }
        return new Type.Array(elementType);
    }

    @Nonnull
    public static LiteralObject objectToliteralObjectProto(@Nonnull final Type type, @Nullable final Object object) {
        final var builder = LiteralObject.newBuilder();
        if (object == null) {
            return builder.build();
        }
        if (type.isRecord()) {
            final var message = (Message) object;
            builder.setRecordObject(message.toByteString());
        } else if (type.isArray()) {
            final var elementType = Objects.requireNonNull(((Type.Array) type).getElementType());
            final var array = (List<?>) object;
            final var arrayBuilder = LiteralObject.Array.newBuilder();
            for (final Object element : array) {
                arrayBuilder.addElementObjects(objectToliteralObjectProto(elementType, element));
            }
            builder.setArrayObject(arrayBuilder.build());
        } else {
            // scalar
            builder.setScalarObject(PlanSerialization.valueObjectToProto(object));
        }
        return builder.build();
    }

    @Nullable
    public static Object objectFromLiteralObjectProto(@Nonnull final TypeRepository typeRepository,
                                                      @Nonnull final Type type,
                                                      @Nonnull final LiteralObject literalObject) {
        if (literalObject.hasScalarObject()) {
            return PlanSerialization.protoToValueObject(literalObject.getScalarObject());
        }
        if (literalObject.hasRecordObject()) {
            final var typeDescriptor = Objects.requireNonNull(typeRepository.getMessageDescriptor(type));
            try {
                return DynamicMessage.parseFrom(typeDescriptor, literalObject.getRecordObject());
            } catch (InvalidProtocolBufferException e) {
                throw new RelationalException("unable to parse object", INTERNAL_ERROR, e).toUncheckedWrappedException();
            }
        }
        if (literalObject.hasArrayObject()) {
            final var arrayObject = literalObject.getArrayObject();
            final var elementType = Objects.requireNonNull(((Type.Array) type).getElementType());
            final var array = Lists.newArrayListWithExpectedSize(arrayObject.getElementObjectsCount());
            for (int i = 0; i < arrayObject.getElementObjectsCount(); i++) {
                array.add(objectFromLiteralObjectProto(typeRepository, elementType, arrayObject.getElementObjects(i)));
            }
            return array;
        }
        return null;
    }
}
