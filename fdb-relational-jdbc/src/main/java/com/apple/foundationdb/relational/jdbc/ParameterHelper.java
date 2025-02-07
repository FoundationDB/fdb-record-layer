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

import com.apple.foundationdb.relational.api.SqlTypeNamesSupport;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.jdbc.grpc.v1.Parameter;
import com.apple.foundationdb.relational.jdbc.grpc.v1.column.Column;
import com.google.protobuf.ByteString;

import java.sql.Array;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

public class ParameterHelper {

    public static Parameter ofBoolean(boolean b) {
        return Parameter.newBuilder()
                .setJavaSqlTypesCode(Types.BOOLEAN)
                .setParameter(Column.newBuilder().setBoolean(b))
                .build();
    }

    public static Parameter ofInt(int i) {
        return Parameter.newBuilder()
                .setJavaSqlTypesCode(Types.INTEGER)
                .setParameter(Column.newBuilder().setInteger(i))
                .build();
    }

    public static Parameter ofLong(long l) {
        return Parameter.newBuilder()
                .setJavaSqlTypesCode(Types.BIGINT)
                .setParameter(Column.newBuilder().setLong(l))
                .build();
    }

    public static Parameter ofFloat(float f) {
        return Parameter.newBuilder()
                .setJavaSqlTypesCode(Types.FLOAT)
                .setParameter(Column.newBuilder().setFloat(f))
                .build();
    }

    public static Parameter ofDouble(double d) {
        return Parameter.newBuilder()
                .setJavaSqlTypesCode(Types.DOUBLE)
                .setParameter(Column.newBuilder().setDouble(d))
                .build();
    }

    public static Parameter ofString(String s) {
        return Parameter.newBuilder()
                .setJavaSqlTypesCode(Types.VARCHAR)
                .setParameter(Column.newBuilder().setString(s))
                .build();
    }

    public static Parameter ofBytes(byte[] bytes) {
        return Parameter.newBuilder()
                .setJavaSqlTypesCode(Types.BINARY)
                .setParameter(Column.newBuilder().setBinary(ByteString.copyFrom(bytes)))
                .build();
    }

    public static Parameter ofNull(int sqlType) throws SQLException {
        return Parameter.newBuilder()
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
            // TODO: Do we even want to allow creation of parameter from an array created by another connection?
            Object[] arrayElements = (Object[])a.getArray();
            for (Object o : arrayElements) {
                Parameter p = ofObject(o);
                if (p.getJavaSqlTypesCode() != a.getBaseType()) {
                    throw new SQLException("Array base type does not match element type: " + a.getBaseType() + ":" + p.getJavaSqlTypesCode());
                }
                elements.add(p.getParameter());
            }
        }
        return Parameter.newBuilder()
                .setJavaSqlTypesCode(Types.ARRAY)
                .setParameter(Column.newBuilder()
                        .setArray(com.apple.foundationdb.relational.jdbc.grpc.v1.column.Array.newBuilder()
                                .setElementType(a.getBaseType())
                                .addAllElement(elements)))
                .build();
    }

    public static Parameter ofObject(Object x) throws SQLException {
        final int typeCodeFromObject = SqlTypeNamesSupport.getSqlTypeCodeFromObject(x);
        switch (typeCodeFromObject) {
            case Types.BIGINT:
                return ofLong((Long)x);
            case Types.INTEGER:
                return ofInt((Integer)x);
            case Types.BOOLEAN:
                return ofBoolean((Boolean)x);
            case Types.BINARY:
                return ofBytes((byte[])x);
            case Types.FLOAT:
                return ofFloat((Float)x);
            case Types.DOUBLE:
                return ofDouble((Double)x);
            case Types.VARCHAR:
                return ofString((String)x);
            case Types.NULL:
                return ofNull(Types.NULL); // TODO: THis would be generic null...
            case Types.ARRAY:
                return ofArray((Array)x);
            default:
                throw new SQLException("setObject Not supported for type " + typeCodeFromObject,
                        ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
        }
    }
}
