/*
 * ParameterHelper.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2025 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.jdbc;

import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.metadata.DataType;
import com.apple.foundationdb.relational.jdbc.grpc.v1.Parameter;
import com.apple.foundationdb.relational.jdbc.grpc.v1.column.Column;
import com.apple.foundationdb.relational.jdbc.grpc.v1.column.Type;
import com.apple.foundationdb.relational.jdbc.grpc.v1.column.Uuid;
import com.google.protobuf.ByteString;

import java.sql.Array;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class ParameterHelper {

    public static Parameter ofBoolean(boolean b) {
        return Parameter.newBuilder()
                .setType(Type.BOOLEAN)
                .setJavaSqlTypesCode(Types.BOOLEAN)
                .setParameter(Column.newBuilder().setBoolean(b))
                .build();
    }

    public static Parameter ofInt(int i) {
        return Parameter.newBuilder()
                .setType(Type.INTEGER)
                .setJavaSqlTypesCode(Types.INTEGER)
                .setParameter(Column.newBuilder().setInteger(i))
                .build();
    }

    public static Parameter ofLong(long l) {
        return Parameter.newBuilder()
                .setType(Type.LONG)
                .setJavaSqlTypesCode(Types.BIGINT)
                .setParameter(Column.newBuilder().setLong(l))
                .build();
    }

    public static Parameter ofFloat(float f) {
        return Parameter.newBuilder()
                .setType(Type.FLOAT)
                .setJavaSqlTypesCode(Types.FLOAT)
                .setParameter(Column.newBuilder().setFloat(f))
                .build();
    }

    public static Parameter ofDouble(double d) {
        return Parameter.newBuilder()
                .setType(Type.DOUBLE)
                .setJavaSqlTypesCode(Types.DOUBLE)
                .setParameter(Column.newBuilder().setDouble(d))
                .build();
    }

    public static Parameter ofString(String s) {
        return Parameter.newBuilder()
                .setType(Type.STRING)
                .setJavaSqlTypesCode(Types.VARCHAR)
                .setParameter(Column.newBuilder().setString(s))
                .build();
    }

    public static Parameter ofUUID(UUID id) {
        return Parameter.newBuilder()
                .setType(Type.UUID)
                .setJavaSqlTypesCode(Types.OTHER)
                .setParameter(Column.newBuilder().setUuid(Uuid.newBuilder()
                        .setMostSignificantBits(id.getMostSignificantBits())
                        .setLeastSignificantBits(id.getLeastSignificantBits())
                        .build()))
                .build();
    }

    public static Parameter ofBytes(byte[] bytes) {
        return Parameter.newBuilder()
                .setType(Type.BYTES)
                .setJavaSqlTypesCode(Types.BINARY)
                .setParameter(Column.newBuilder().setBinary(ByteString.copyFrom(bytes)))
                .build();
    }

    public static Parameter ofNull(int sqlType) {
        return Parameter.newBuilder()
                .setType(Type.NULL)
                .setJavaSqlTypesCode(Types.NULL)
                .setParameter(Column.newBuilder().setNullType(sqlType))
                .build();
    }

    public static Parameter ofArray(final Array a) throws SQLException {
        List<Column> elements = new ArrayList<>();
        if (a instanceof JDBCArrayImpl) {
            // we can shortcut the process and use the existing columns
            JDBCArrayImpl arrayImpl = (JDBCArrayImpl)a;
            elements.addAll(arrayImpl.getUnderlying().getElementList());
        } else {
            throw new SQLException("Array type not supported: " + a.getClass().getName(), ErrorCode.INVALID_PARAMETER.getErrorCode());
        }
        return Parameter.newBuilder()
                .setType(Type.ARRAY)
                .setJavaSqlTypesCode(Types.ARRAY)
                .setParameter(Column.newBuilder()
                        .setArray(com.apple.foundationdb.relational.jdbc.grpc.v1.column.Array.newBuilder()
                                .setElementType(a.getBaseType())
                                .addAllElement(elements)))
                .build();
    }

    public static Parameter ofObject(Object x) throws SQLException {
        // We need to keep an exception for the case of Array, because JDBC client array creations does not inherit
        // RelationalArray. Since the Relational dataType works with Relational constructs, this is an exception. We
        // should probably strive to bring this under Relational umbrella, until then treat the case separately.
        if (x instanceof Array) {
            return ofArray((Array)x);
        }
        final DataType type = DataType.getDataTypeFromObject(x);
        switch (type.getCode()) {
            case LONG:
                return ofLong((Long)x);
            case INTEGER:
                return ofInt((Integer)x);
            case BOOLEAN:
                return ofBoolean((Boolean)x);
            case BYTES:
                return ofBytes((byte[])x);
            case FLOAT:
                return ofFloat((Float)x);
            case DOUBLE:
                return ofDouble((Double)x);
            case STRING:
                return ofString((String)x);
            case UUID:
                return ofUUID((UUID)x);
            case NULL:
                return ofNull(type.getJdbcSqlCode()); // TODO: This would be generic null...
            default:
                throw new SQLException("setObject Not supported for type: " + type,
                        ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
        }
    }
}
