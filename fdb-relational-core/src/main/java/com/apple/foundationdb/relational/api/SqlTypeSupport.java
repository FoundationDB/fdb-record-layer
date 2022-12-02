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
import com.apple.foundationdb.relational.recordlayer.util.Assert;

import javax.annotation.Nonnull;
import java.sql.Types;
import java.util.Objects;
import java.util.Set;

public final class SqlTypeSupport {
    //TODO(bfines) eventually this should move into the Planner (or closer to there, anyway), but for now
    //we will hold on to it here
    private static final Set<String> KNOWN_PHANTOM_COLUMNS = Set.of("__TYPE_KEY");

    private SqlTypeSupport() {

    }

    public static String getSqlTypeName(int sqlTypeCode) {
        return SqlTypeNamesSupport.getSqlTypeName(sqlTypeCode);
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
            case LONG:
                return Types.BIGINT;
            case STRING:
                return Types.VARCHAR;
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

    @Nonnull
    public static StructMetaData recordToMetaData(@Nonnull Type.Record record) throws RelationalException {
        // Make sure that fields are already sorted
        FieldDescription[] fields = new FieldDescription[record.getFields().size()];
        int previousFieldIndex = 0;
        for (int i = 0; i < record.getFields().size(); i++) {
            Type.Record.Field field = record.getFields().get(i);
            Assert.that(field.getFieldIndex() > previousFieldIndex, "Record field's indexes should be monotonically increasing", ErrorCode.INTERNAL_ERROR);
            previousFieldIndex = field.getFieldIndex();
            final FieldDescription fieldDescription = fieldToDescription(field);
            fields[i] = fieldDescription;
        }
        return new RelationalStructMetaData(fields);
    }

    private static FieldDescription fieldToDescription(Type.Record.Field field) throws RelationalException {
        final Type fieldType = field.getFieldType();
        if (fieldType.isPrimitive() || fieldType instanceof Type.Enum) {
            return FieldDescription.primitive(field.getFieldName(),
                    SqlTypeSupport.recordTypeToSqlType(fieldType.getTypeCode()),
                    fieldType.isNullable(), KNOWN_PHANTOM_COLUMNS.contains(field.getFieldName())
            );
        } else if (fieldType instanceof Type.Array) {
            Type.Array arrayType = (Type.Array) fieldType;
            Type elementType = Objects.requireNonNull(arrayType.getElementType());
            StructMetaData arrayMeta;
            if (elementType.isPrimitive()) {
                FieldDescription desc = FieldDescription.primitive(field.getFieldName(),
                        SqlTypeSupport.recordTypeToSqlType(elementType.getTypeCode()),
                        elementType.isNullable());
                arrayMeta = new RelationalStructMetaData(desc);
            } else if (elementType instanceof Type.Record) {
                //get a StructMetaData recursively
                Type.Record structType = (Type.Record) elementType;
                arrayMeta = recordToMetaData(structType);
            } else {
                //TODO(bfines) not sure if this is true or not, but I like the rule.
                throw new RelationalException("Cannot have an array of arrays right now", ErrorCode.UNSUPPORTED_OPERATION);
            }
            return FieldDescription.array(field.getFieldName(), false, arrayMeta);
        } else if (fieldType instanceof Type.Record) {
            Type.Record recType = (Type.Record) fieldType;
            StructMetaData smd = recordToMetaData(recType);
            return FieldDescription.struct(field.getFieldName(), fieldType.isNullable(), smd);
        } else {
            throw new RelationalException("Unexpected Data Type " + fieldType.getClass(), ErrorCode.UNSUPPORTED_OPERATION);
        }
    }

    public static StructMetaData typeToMetaData(Type type) throws RelationalException {
        if (type instanceof Type.Record) {
            return recordToMetaData((Type.Record) type);
        } else if (type instanceof Type.Relation) {
            return typeToMetaData(((Type.Relation) type).getInnerType());
        } else {
            throw new RelationalException("Unexpected Data type " + type.getTypeCode(), ErrorCode.UNSUPPORTED_OPERATION);
        }
    }
}
