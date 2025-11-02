/*
 * DataTypeUtils.java
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

package com.apple.foundationdb.relational.recordlayer.metadata;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.metadata.DataType;
import com.apple.foundationdb.relational.util.Assert;
import com.apple.foundationdb.relational.util.SpotBugsSuppressWarnings;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.UUID;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@API(API.Status.EXPERIMENTAL)
public class DataTypeUtils {

    private static final String DOUBLE_UNDERSCORE_ESCAPE = "__0";
    private static final String DOLLAR_ESCAPE = "__1";
    private static final String DOT_ESCAPE = "__2";

    private static final List<String> INVALID_START_SEQUENCES = List.of(".", "$", "__0", "__1", "__2");

    private static final Pattern VALID_PROTOBUF_COMPLIANT_NAME_PATTERN = Pattern.compile("^[A-Za-z_][A-Za-z0-9_]*$");

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
        return toRelationalType(type, false);
    }

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
    public static DataType toRelationalType(@Nonnull final Type type, boolean toUserIdentifier) {
        if (primitivesMap.containsValue(type)) {
            return primitivesMap.inverse().get(type);
        }

        final var typeCode = type.getTypeCode();

        if (typeCode == Type.TypeCode.ANY || typeCode == Type.TypeCode.NONE || typeCode == Type.TypeCode.NULL || typeCode == Type.TypeCode.UNKNOWN) {
            return DataType.UnknownType.instance();
        }

        Assert.thatUnchecked(!type.isPrimitive());

        switch (typeCode) {
            case RECORD:
                final var record = (Type.Record) type;
                final var columns = record.getFields().stream().map(field -> {
                    final var fieldName = toUserIdentifier ? DataTypeUtils.toUserIdentifier(field.getFieldName()) : field.getFieldName();
                    return DataType.StructType.Field.from(fieldName, toRelationalType(field.getFieldType(), toUserIdentifier), field.getFieldIndex());
                }).collect(Collectors.toList());
                final var name = record.getName() == null ? getUniqueName() : (toUserIdentifier ? DataTypeUtils.toUserIdentifier(record.getName()) : record.getName());
                return DataType.StructType.from(name, columns, record.isNullable());
            case ARRAY:
                final var asArray = (Type.Array) type;
                return DataType.ArrayType.from(toRelationalType(Assert.notNullUnchecked(asArray.getElementType())), asArray.isNullable());
            case ENUM:
                final var asEnum = (Type.Enum) type;
                final var enumValues = asEnum.getEnumValues().stream().map(v -> DataType.EnumType.EnumValue.of(v.getName(), v.getNumber())).collect(Collectors.toList());
                return DataType.EnumType.from(asEnum.getName() == null ? getUniqueName() : asEnum.getName(), enumValues, asEnum.isNullable());
            default:
                Assert.failUnchecked(String.format(Locale.ROOT, "unexpected type %s", type));
                return null; // make compiler happy.
        }
    }

    @Nonnull
    private static String getUniqueName() {
        final var uuid = UUID.randomUUID().toString();
        final var modified = uuid.replace("-", "_");
        final char c = uuid.charAt(0);
        if (c == '_' || ('a' <= c && c <= 'z') || ('A' <= c && c <= 'Z')) {
            return modified;
        }
        return "id" + modified;
    }

    @Nonnull
    public static String toProtoBufCompliantName(final String name) {
        Assert.thatUnchecked(INVALID_START_SEQUENCES.stream().noneMatch(name::startsWith), ErrorCode.INVALID_NAME, "name cannot start with %s", INVALID_START_SEQUENCES);
        String translated;
        if (name.startsWith("__")) {
            translated = "__" + translateSpecialCharacters(name.substring(2));
        } else {
            Assert.thatUnchecked(!name.isEmpty(), ErrorCode.INVALID_NAME, "name cannot be empty String.");
            translated = translateSpecialCharacters(name);
        }
        checkValidProtoBufCompliantName(translated);
        return translated;
    }

    @Nonnull
    private static String translateSpecialCharacters(final String userIdentifier) {
        return userIdentifier.replace("__", DOUBLE_UNDERSCORE_ESCAPE).replace("$", DOLLAR_ESCAPE).replace(".", DOT_ESCAPE);
    }

    public static void checkValidProtoBufCompliantName(String name) {
        Assert.thatUnchecked(VALID_PROTOBUF_COMPLIANT_NAME_PATTERN.matcher(name).matches(), ErrorCode.INVALID_NAME, name + " is not a valid name!");
    }

    public static String toUserIdentifier(String protoIdentifier) {
        return protoIdentifier.replace(DOT_ESCAPE, ".").replace(DOLLAR_ESCAPE, "$").replace(DOUBLE_UNDERSCORE_ESCAPE, "__");
    }

    /**
     * Converts a given Relational {@link DataType} into a corresponding Record Layer {@link Type}.
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
                final var fields = struct.getFields().stream().map(field -> Type.Record.Field.of(DataTypeUtils.toRecordLayerType(field.getType()), Optional.of(field.getName()), Optional.of(field.getIndex()))).collect(Collectors.toList());
                return Type.Record.fromFieldsWithName(struct.getName(), struct.isNullable(), fields);
            case ARRAY:
                final var asArray = (DataType.ArrayType) type;
                // Currently, Record-Layer does not support Nullable array elements. In the Postgres world, the elements of an array are by default nullable,
                // but since in RL we store the elements as a 'repeated' field, there is not a way to tell if an element is explicitly 'null'.
                // The current RL behavior loses the nullability information even if the constituent of Type.Array is explicitly marked 'nullable'. Hence,
                // the check here avoids silently swallowing the requirement.
                Assert.thatUnchecked(asArray.getElementType().getCode() == DataType.Code.NULL || !asArray.getElementType().isNullable(), ErrorCode.UNSUPPORTED_OPERATION, "No support for nullable array elements.");
                return new Type.Array(asArray.isNullable(), toRecordLayerType(asArray.getElementType()));
            case ENUM:
                final var asEnum = (DataType.EnumType) type;
                final List<Type.Enum.EnumValue> enumValues = asEnum.getValues().stream().map(v -> new Type.Enum.EnumValue(v.getName(), v.getNumber())).collect(Collectors.toList());
                return new Type.Enum(asEnum.isNullable(), enumValues, asEnum.getName());
            case UNKNOWN:
                return new Type.Any();
            default:
                Assert.failUnchecked(String.format(Locale.ROOT, "unexpected type %s", type));
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
        primitivesMap.put(DataType.Primitives.VERSION.type(), Type.primitiveType(Type.TypeCode.VERSION, false));
        primitivesMap.put(DataType.Primitives.UUID.type(), Type.uuidType(false));

        primitivesMap.put(DataType.Primitives.NULLABLE_BOOLEAN.type(), Type.primitiveType(Type.TypeCode.BOOLEAN, true));
        primitivesMap.put(DataType.Primitives.NULLABLE_INTEGER.type(), Type.primitiveType(Type.TypeCode.INT, true));
        primitivesMap.put(DataType.Primitives.NULLABLE_LONG.type(), Type.primitiveType(Type.TypeCode.LONG, true));
        primitivesMap.put(DataType.Primitives.NULLABLE_DOUBLE.type(), Type.primitiveType(Type.TypeCode.DOUBLE, true));
        primitivesMap.put(DataType.Primitives.NULLABLE_FLOAT.type(), Type.primitiveType(Type.TypeCode.FLOAT, true));
        primitivesMap.put(DataType.Primitives.NULLABLE_BYTES.type(), Type.primitiveType(Type.TypeCode.BYTES, true));
        primitivesMap.put(DataType.Primitives.NULLABLE_STRING.type(), Type.primitiveType(Type.TypeCode.STRING, true));
        primitivesMap.put(DataType.Primitives.NULLABLE_VERSION.type(), Type.primitiveType(Type.TypeCode.VERSION, true));
        primitivesMap.put(DataType.Primitives.NULLABLE_UUID.type(), Type.uuidType(true));

        primitivesMap.put(DataType.Primitives.NULL.type(), Type.nullType());
    }
}
