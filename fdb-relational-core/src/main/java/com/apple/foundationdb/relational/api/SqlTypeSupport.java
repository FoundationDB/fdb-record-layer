/*
 * SqlTypeSupport.java
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

package com.apple.foundationdb.relational.api;

import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;

import javax.annotation.Nonnull;
import java.sql.Array;
import java.sql.DatabaseMetaData;
import java.sql.Struct;
import java.sql.Types;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public final class SqlTypeSupport {
    //TODO(bfines) eventually this should move into the Planner (or closer to there, anyway), but for now
    //we will hold on to it here
    private static final Set<String> KNOWN_PHANTOM_COLUMNS = Set.of("__TYPE_KEY");

    private SqlTypeSupport() {

    }

    public static String getSqlTypeName(int sqlTypeCode) {
        return SqlTypeNamesSupport.getSqlTypeName(sqlTypeCode);
    }

    public static int getSqlTypeCodeFromObject(Object obj) {
        if (obj instanceof Long) {
            return Types.BIGINT;
        } else if (obj instanceof Integer) {
            return Types.INTEGER;
        } else if (obj instanceof Boolean) {
            return Types.BOOLEAN;
        } else if (obj instanceof byte[]) {
            return Types.BINARY;
        } else if (obj instanceof Float) {
            return Types.FLOAT;
        } else if (obj instanceof Double) {
            return Types.DOUBLE;
        } else if (obj instanceof String) {
            return Types.VARCHAR;
        } else if (obj instanceof Array) {
            return Types.ARRAY;
        } else if (obj instanceof Struct) {
            return Types.STRUCT;
        } else {
            throw new IllegalStateException("Unexpected object type: " + obj.getClass().getName());
        }
    }

    @SuppressWarnings("DuplicateBranchesInSwitch") //intentional duplicates for readability
    public static int recordTypeToSqlType(Type.TypeCode typeCode) {
        switch (typeCode) {
            case UNKNOWN:
                return Types.JAVA_OBJECT;
            case ANY:
                return Types.JAVA_OBJECT;
            case BOOLEAN:
                return Types.BOOLEAN;
            case BYTES:
                return Types.BINARY;
            case DOUBLE:
                return Types.DOUBLE;
            case FLOAT:
                return Types.FLOAT;
            case INT:
                return Types.INTEGER;
            case LONG:
                return Types.BIGINT;
            case STRING:
                return Types.VARCHAR;
            case VERSION:
                return Types.BINARY;
            case ENUM:
                //TODO(bfines) should be string?
                return Types.JAVA_OBJECT;
            case RECORD:
                return Types.STRUCT;
            case ARRAY:
                return Types.ARRAY;
            case RELATION:
                //TODO(bfines) not sure if this is correct or not
                return Types.JAVA_OBJECT;
            default:
                throw new IllegalStateException("Unexpected Type Code " + typeCode);
        }
    }

    public static Type.TypeCode sqlTypeToRecordType(int sqlTypeCode) {
        switch (sqlTypeCode) {
            case Types.FLOAT:
                return Type.TypeCode.FLOAT;
            case Types.VARCHAR:
                return Type.TypeCode.STRING;
            case Types.BINARY:
                return Type.TypeCode.BYTES;
            case Types.DOUBLE:
                return Type.TypeCode.DOUBLE;
            case Types.BIGINT:
                return Type.TypeCode.LONG;
            case Types.INTEGER:
                return Type.TypeCode.INT;
            case Types.BOOLEAN:
                return Type.TypeCode.BOOLEAN;
            case Types.STRUCT:
                return Type.TypeCode.RECORD;
            case Types.ARRAY:
                return Type.TypeCode.ARRAY;
            case Types.NULL:
                return Type.TypeCode.NULL;
            default:
                throw new IllegalStateException("No conversion for SQL type " + getSqlTypeName(sqlTypeCode));
        }
    }

    @Nonnull
    public static StructMetaData recordToMetaData(@Nonnull Type.Record record) throws RelationalException {
        FieldDescription[] fields = new FieldDescription[record.getFields().size()];
        for (int i = 0; i < record.getFields().size(); i++) {
            Type.Record.Field field = record.getFields().get(i);
            final FieldDescription fieldDescription = fieldToDescription(field);
            fields[i] = fieldDescription;
        }
        return new RelationalStructMetaData(fields);
    }

    @Nonnull
    public static Type.Record structMetadataToRecordType(@Nonnull StructMetaData metaData, boolean isNullable) {
        final var fields = ((RelationalStructMetaData) metaData)
                .getFields().stream()
                .map(SqlTypeSupport::descriptionToField)
                .collect(Collectors.toList());
        return Type.Record.fromFields(isNullable, fields);
    }

    @Nonnull
    public static Type.Array arrayMetadataToArrayType(@Nonnull StructMetaData metaData, boolean isNullable) {
        final var field = descriptionToField(((RelationalStructMetaData) metaData).getFields().get(0));
        return new Type.Array(isNullable, field.getFieldType());
    }

    @Nonnull
    private static Type.Record.Field descriptionToField(@Nonnull FieldDescription description) {
        final Type.TypeCode recordTypeCode = sqlTypeToRecordType(description.getSqlTypeCode());
        final var isNullable = description.isNullable() == DatabaseMetaData.columnNullable;
        Type type;
        if (recordTypeCode.equals(Type.TypeCode.RECORD)) {
            type = structMetadataToRecordType(description.getFieldMetaData(), isNullable);
        } else if (recordTypeCode.equals(Type.TypeCode.ARRAY)) {
            type = arrayMetadataToArrayType(description.getArrayMetaData(), isNullable);
        } else if (recordTypeCode == Type.TypeCode.NULL) {
            type = Type.nullType();
        } else {
            type = Type.primitiveType(recordTypeCode, isNullable);
        }
        return Type.Record.Field.of(type, Optional.of(description.getName()));
    }

    @Nonnull
    private static FieldDescription fieldToDescription(@Nonnull Type.Record.Field field) throws RelationalException {
        final Type fieldType = field.getFieldType();
        if (fieldType.isPrimitive() || fieldType instanceof Type.Enum) {
            return FieldDescription.primitive(field.getFieldName(),
                    SqlTypeSupport.recordTypeToSqlType(fieldType.getTypeCode()),
                    fieldType.isNullable() ? DatabaseMetaData.columnNullable : DatabaseMetaData.columnNoNulls,
                    KNOWN_PHANTOM_COLUMNS.contains(field.getFieldName())
            );
        } else if (fieldType instanceof Type.Array) {
            Type.Array arrayType = (Type.Array) fieldType;
            Type elementType = Objects.requireNonNull(arrayType.getElementType());
            StructMetaData arrayMeta;
            if (elementType.isPrimitive()) {
                FieldDescription desc = FieldDescription.primitive(field.getFieldName(),
                        SqlTypeSupport.recordTypeToSqlType(elementType.getTypeCode()),
                        elementType.isNullable() ? DatabaseMetaData.columnNullable : DatabaseMetaData.columnNoNulls);
                arrayMeta = new RelationalStructMetaData(desc);
            } else if (elementType instanceof Type.Record) {
                //get a StructMetaData recursively
                Type.Record structType = (Type.Record) elementType;
                arrayMeta = recordToMetaData(structType);
            } else {
                //TODO(bfines) not sure if this is true or not, but I like the rule.
                throw new RelationalException("Cannot have an array of arrays right now", ErrorCode.UNSUPPORTED_OPERATION);
            }
            return FieldDescription.array(field.getFieldName(), DatabaseMetaData.columnNoNulls, arrayMeta);
        } else if (fieldType instanceof Type.Record) {
            Type.Record recType = (Type.Record) fieldType;
            StructMetaData smd = recordToMetaData(recType);
            return FieldDescription.struct(field.getFieldName(),
                    fieldType.isNullable() ? DatabaseMetaData.columnNullable : DatabaseMetaData.columnNoNulls, smd);
        } else {
            throw new RelationalException("Unexpected Data Type " + fieldType.getClass(), ErrorCode.UNSUPPORTED_OPERATION);
        }
    }

    @Nonnull
    public static StructMetaData typeToMetaData(@Nonnull Type type) throws RelationalException {
        if (type instanceof Type.Record) {
            return recordToMetaData((Type.Record) type);
        } else if (type instanceof Type.Relation) {
            return typeToMetaData(((Type.Relation) type).getInnerType());
        } else {
            throw new RelationalException("Unexpected Data type " + type.getTypeCode(), ErrorCode.UNSUPPORTED_OPERATION);
        }
    }
}
