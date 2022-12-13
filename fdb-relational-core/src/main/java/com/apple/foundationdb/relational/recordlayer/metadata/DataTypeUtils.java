/*
 * DataTypeUtils.java
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

package com.apple.foundationdb.relational.recordlayer.metadata;

import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.relational.api.metadata.DataType;
import com.apple.foundationdb.relational.recordlayer.util.Assert;
import com.apple.foundationdb.relational.util.SpotBugsSuppressWarnings;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

public class DataTypeUtils {

    @Nonnull
    private static final BiMap<DataType, Type> primitivesMap;

    /**
     * Converts a Record Layer {@link Type} into a Relational {@link DataType}.
     *
     * Note: This method is expensive, use with care, i.e. try to cache its result as much as possible.
     *
     * @param type The Relational data type.
     * @return The corresponding Record Layer type.
     */
    @SpotBugsSuppressWarnings(value = "NP_NONNULL_RETURN_VIOLATION", justification = "should never happen, there is failUnchecked directly before that.")
    @Nonnull
    public static DataType toRelationalType(@Nonnull final Type type) {
        if (primitivesMap.containsValue(type)) {
            return primitivesMap.inverse().get(type);
        }
        Assert.thatUnchecked(!type.isPrimitive());

        switch (type.getTypeCode()) {
            case RECORD:
                final var record = (Type.Record) type;
                final var columns = record.getFields().stream().map(field -> DataType.StructType.Field.from(field.getFieldName(), toRelationalType(field.getFieldType()))).collect(Collectors.toList());
                return DataType.StructType.from(record.getName() == null ? toProtoBufCompliantName(UUID.randomUUID().toString()) : record.getName(), columns, record.isNullable());
            case ARRAY:
                final var asArray = (Type.Array) type;
                return DataType.ArrayType.from(toRelationalType(Assert.notNullUnchecked(asArray.getElementType())), asArray.isNullable());
            case ENUM:
                final var asEnum = (Type.Enum) type;
                final var enumValues = asEnum.getEnumValues().stream().map(Type.Enum.EnumValue::getName).collect(Collectors.toList());
                return DataType.EnumType.from(asEnum.getName() == null ? toProtoBufCompliantName(UUID.randomUUID().toString()) : asEnum.getName(), enumValues, asEnum.isNullable());
            default:
                Assert.failUnchecked(String.format("unexpected type %s", type));
                return null; // make compiler happy.
        }
    }

    @Nonnull
    private static String toProtoBufCompliantName(@Nonnull final String input) {
        Assert.thatUnchecked(input.length() > 0);
        final var modified = input.replace("-", "_");
        final char c = input.charAt(0);
        if (c == '_' || ('a' <= c && c <= 'z') || ('A' <= c && c <= 'Z')) {
            return modified;
        }
        return "id" + modified;
    }

    /**
     * Converts a given Relational {@link DataType} into a coresponding Record Layer {@link Type}.
     *
     * Note: This method is expensive, use with care, i.e. try to cache its result as much as possible.
     *
     * @param type The Relational data type.
     * @return The corresponding Record Layer type.
     */
    @SpotBugsSuppressWarnings(value = {"NP_NONNULL_RETURN_VIOLATION", "NP_NULL_ON_SOME_PATH_FROM_RETURN_VALUE"},
            justification = "should never happen, there is failUnchecked directly before that.")
    @Nonnull
    public static Type toRecordLayerType(@Nonnull final DataType type) {
        if (primitivesMap.containsKey(type)) {
            return primitivesMap.get(type);
        }

        switch (type.getCode()) {
            case STRUCT:
                final var struct = (DataType.StructType) type;
                final var fields = struct.getFields().stream().map(field -> Type.Record.Field.of(DataTypeUtils.toRecordLayerType(field.getType()), Optional.of(field.getName()))).collect(Collectors.toList());
                return Type.Record.fromFieldsWithName(struct.getName(), struct.isNullable(), fields);
            case ARRAY:
                final var asArray = (DataType.ArrayType) type;
                return new Type.Array(asArray.isNullable(), toRecordLayerType(asArray.getElementType()));
            case ENUM:
                final var asEnum = (DataType.EnumType) type;
                final List<Type.Enum.EnumValue> enumValues = new ArrayList<>(asEnum.getValues().size());
                for (int i = 1; i <= asEnum.getValues().size(); i++) {
                    enumValues.add(new Type.Enum.EnumValue(asEnum.getValues().get(i - 1), i));
                }
                return new Type.Enum(asEnum.isNullable(), enumValues, asEnum.getName());
            case UNKNOWN:
                return new Type.Any();
            default:
                Assert.failUnchecked(String.format("unexpected type %s", type));
                return null; // make compiler happy.
        }
    }

    static {
        primitivesMap = HashBiMap.create();

        primitivesMap.put(DataType.Primitives.BOOLEAN.type(), Type.primitiveType(Type.TypeCode.BOOLEAN, false));
        primitivesMap.put(DataType.Primitives.INTEGER.type(), Type.primitiveType(Type.TypeCode.INT, false));
        primitivesMap.put(DataType.Primitives.LONG.type(), Type.primitiveType(Type.TypeCode.LONG, false));
        primitivesMap.put(DataType.Primitives.DOUBLE.type(), Type.primitiveType(Type.TypeCode.DOUBLE, false));
        primitivesMap.put(DataType.Primitives.FLOAT.type(), Type.primitiveType(Type.TypeCode.FLOAT, false));
        primitivesMap.put(DataType.Primitives.BYTES.type(), Type.primitiveType(Type.TypeCode.BYTES, false));
        primitivesMap.put(DataType.Primitives.STRING.type(), Type.primitiveType(Type.TypeCode.STRING, false));

        primitivesMap.put(DataType.Primitives.NULLABLE_BOOLEAN.type(), Type.primitiveType(Type.TypeCode.BOOLEAN, true));
        primitivesMap.put(DataType.Primitives.NULLABLE_INTEGER.type(), Type.primitiveType(Type.TypeCode.INT, true));
        primitivesMap.put(DataType.Primitives.NULLABLE_LONG.type(), Type.primitiveType(Type.TypeCode.LONG, true));
        primitivesMap.put(DataType.Primitives.NULLABLE_DOUBLE.type(), Type.primitiveType(Type.TypeCode.DOUBLE, true));
        primitivesMap.put(DataType.Primitives.NULLABLE_FLOAT.type(), Type.primitiveType(Type.TypeCode.FLOAT, true));
        primitivesMap.put(DataType.Primitives.NULLABLE_BYTES.type(), Type.primitiveType(Type.TypeCode.BYTES, true));
        primitivesMap.put(DataType.Primitives.NULLABLE_STRING.type(), Type.primitiveType(Type.TypeCode.STRING, true));
    }
}
