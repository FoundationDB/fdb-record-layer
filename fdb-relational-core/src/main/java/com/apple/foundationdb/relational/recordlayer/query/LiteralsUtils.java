/*
 * LiteralsUtils.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2024 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.typing.TypeRepository;
import com.apple.foundationdb.record.query.plan.serialization.PlanSerialization;
import com.apple.foundationdb.relational.api.SqlTypeSupport;
import com.apple.foundationdb.relational.api.RelationalStruct;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.continuation.LiteralObject;
import com.apple.foundationdb.relational.util.Assert;

import com.google.common.collect.Lists;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static com.apple.foundationdb.relational.api.exceptions.ErrorCode.DATATYPE_MISMATCH;
import static com.apple.foundationdb.relational.api.exceptions.ErrorCode.INTERNAL_ERROR;

@API(API.Status.EXPERIMENTAL)
public final class LiteralsUtils {
    private LiteralsUtils() {
        // prevent instantiation
    }

    @Nonnull
    public static Type.Array resolveArrayTypeFromObjectsList(List<Object> objects) {
        final var distinctTypes = objects.stream().filter(Objects::nonNull).map(SqlTypeSupport::getSqlTypeCodeFromObject).distinct().collect(Collectors.toList());
        if (distinctTypes.isEmpty()) {
            // has all nulls or is empty
            return new Type.Array(Type.nullType());
        }
        Assert.thatUnchecked(distinctTypes.size() == 1, DATATYPE_MISMATCH, "could not determine type of array literal");
        final var theType = distinctTypes.get(0);
        if (theType != Types.STRUCT) {
            return new Type.Array(Type.primitiveType(SqlTypeSupport.sqlTypeToRecordType(theType), false));
        }
        final var distinctStructMetadata = objects.stream().filter(Objects::nonNull).map(o -> {
            try {
                return ((RelationalStruct) o).getMetaData();
            } catch (SQLException e) {
                throw new RelationalException(e).toUncheckedWrappedException();
            }
        }).distinct().collect(Collectors.toList());
        Assert.thatUnchecked(distinctStructMetadata.size() == 1, DATATYPE_MISMATCH, "Elements of struct array literal are not of identical shape!");
        return new Type.Array(SqlTypeSupport.structMetadataToRecordType(distinctStructMetadata.get(0), false));
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
            final var elementType = Objects.requireNonNull(((Type.Array) type).getElementType());
            final var array = (List<?>) object;
            final var arrayBuilder = LiteralObject.Array.newBuilder();
            for (final Object element : array) {
                arrayBuilder.addElementObjects(objectToLiteralObjectProto(elementType, element));
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
