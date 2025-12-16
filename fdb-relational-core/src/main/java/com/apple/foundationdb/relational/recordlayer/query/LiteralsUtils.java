/*
 * LiteralsUtils.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2025 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.linear.RealVector;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.typing.TypeRepository;
import com.apple.foundationdb.record.query.plan.serialization.PlanSerialization;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.api.metadata.DataType;
import com.apple.foundationdb.relational.continuation.LiteralObject;
import com.apple.foundationdb.relational.recordlayer.metadata.DataTypeUtils;
import com.apple.foundationdb.relational.util.Assert;
import com.google.common.collect.Lists;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;

import static com.apple.foundationdb.relational.api.exceptions.ErrorCode.DATATYPE_MISMATCH;
import static com.apple.foundationdb.relational.api.exceptions.ErrorCode.INTERNAL_ERROR;

@API(API.Status.EXPERIMENTAL)
public final class LiteralsUtils {
    private LiteralsUtils() {
        // prevent instantiation
    }

    // This is not sufficient, as we should try to coercion struct types rather than checking for them being
    // identical. https://github.com/FoundationDB/fdb-record-layer/issues/3472
    @Nonnull
    public static Type.Array resolveArrayTypeFromObjectsList(List<Object> objects) {
        DataType distinctType = null;
        for (var object: objects) {
            final var objectType = DataType.getDataTypeFromObject(object);
            if (distinctType == null) {
                distinctType = objectType;
            } else if (distinctType instanceof DataType.CompositeType) {
                Assert.thatUnchecked(((DataType.CompositeType)distinctType).hasIdenticalStructure(objectType), DATATYPE_MISMATCH, "could not determine type of array literal");
            } else {
                Assert.thatUnchecked(distinctType.equals(objectType), DATATYPE_MISMATCH, "could not determine type of array literal");
            }
        }
        return new Type.Array(distinctType == null ? Type.nullType() : DataTypeUtils.toRecordLayerType(distinctType));
    }

    @Nonnull
    public static LiteralObject objectToLiteralObjectProto(@Nonnull final Type type, @Nullable final Object object) {
        final var builder = LiteralObject.newBuilder();
        if (object == null) {
            return builder.build();
        }
        if (type.isRecord()) {
            final var message = (Message) object;
            builder.setRecordObject(message.toByteString());
        } else if (type.isArray()) {
            final var elementType = Objects.requireNonNull(((Type.Array)type).getElementType());
            final var array = (List<?>)object;
            final var arrayBuilder = LiteralObject.Array.newBuilder();
            for (final Object element : array) {
                arrayBuilder.addElementObjects(objectToLiteralObjectProto(elementType, element));
            }
            builder.setArrayObject(arrayBuilder.build());
        } else if (type.isVector() || object instanceof RealVector) {
            final var typeProto = type.isVector() ? type.toTypeProto(PlanSerializationContext.newForCurrentMode())
                                  : Type.fromObject(object).toTypeProto(PlanSerializationContext.newForCurrentMode());
            builder.setScalarObject(PlanSerialization.valueObjectToProto(object, typeProto));
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
