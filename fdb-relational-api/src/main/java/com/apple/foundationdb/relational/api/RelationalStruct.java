/*
 * RelationalStruct.java
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

import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.InvalidColumnReferenceException;
import com.apple.foundationdb.relational.api.metadata.DataType;

import javax.annotation.Nonnull;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Struct;
import java.sql.Wrapper;
import java.util.Map;
import java.util.UUID;

/**
 * A {@link Struct} but with metadata describing the instance.
 */
public interface RelationalStruct extends Struct, Wrapper, WithMetadata {

    StructMetaData getMetaData() throws SQLException;

    boolean getBoolean(int oneBasedPosition) throws SQLException;

    boolean getBoolean(String fieldName) throws SQLException;

    int getInt(int oneBasedPosition) throws SQLException;

    int getInt(String fieldName) throws SQLException;

    long getLong(int oneBasedPosition) throws SQLException;

    long getLong(String fieldName) throws SQLException;

    float getFloat(int oneBasedPosition) throws SQLException;

    float getFloat(String fieldName) throws SQLException;

    double getDouble(int oneBasedPosition) throws SQLException;

    double getDouble(String fieldName) throws SQLException;

    byte[] getBytes(int oneBasedPosition) throws SQLException;

    byte[] getBytes(String fieldName) throws SQLException;

    String getString(int oneBasedPosition) throws SQLException;

    String getString(String fieldName) throws SQLException;

    Object getObject(int oneBasedPosition) throws SQLException;

    Object getObject(String fieldName) throws SQLException;

    RelationalStruct getStruct(int oneBasedPosition) throws SQLException;

    RelationalStruct getStruct(String fieldName) throws SQLException;

    RelationalArray getArray(int oneBasedPosition) throws SQLException;

    RelationalArray getArray(String fieldName) throws SQLException;

    UUID getUUID(int oneBasedPosition) throws SQLException;

    UUID getUUID(String fieldName) throws SQLException;

    /**
     * Reports whether the last column read had a value of SQL NULL. See {@link ResultSet#wasNull()}
     */
    boolean wasNull() throws SQLException;

    @Override
    default String getSQLTypeName() throws SQLException {
        return "STRUCT";
    }

    @Override
    default Object[] getAttributes() throws SQLException {
        StructMetaData metaData = getMetaData();
        Object[] arr = new Object[metaData.getColumnCount()];
        for (int i = 1; i <= arr.length; i++) {
            arr[i - 1] = getObject(i);
        }
        return arr;
    }

    @Override
    @SuppressWarnings("PMD.PreserveStackTrace")
    default Object[] getAttributes(Map<String, Class<?>> map) throws SQLException {
        StructMetaData metaData = getMetaData();
        Object[] arr = new Object[metaData.getColumnCount()];
        for (int i = 1; i <= arr.length; i++) {
            Object o = getObject(i);
            if (o == null) {
                //TODO(bfines) replace this with default value when necessary
                arr[i - 1] = null;
            } else {
                String columnName = metaData.getColumnName(i);
                final Class<?> type = map.getOrDefault(columnName, Object.class);
                try {
                    arr[i - 1] = type.cast(o);
                } catch (ClassCastException cce) {
                    throw new SQLException("Unable to cast column <" + columnName + "> to type <" + type + ">",
                            ErrorCode.INVALID_PARAMETER.getErrorCode());
                }
            }
        }
        return arr;
    }

    @Override
    default <T> T unwrap(Class<T> iface) throws SQLException {
        if (isWrapperFor(iface)) {
            return iface.cast(this);
        }
        throw new SQLException("Unwrap failed for: " + iface);
    }

    @Override
    default boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isInstance(this);
    }

    @Nonnull
    @Override
    default DataType getRelationalMetaData() throws SQLException {
        return getMetaData().getRelationalDataType();
    }

    /**
     * Utility for figuring out one-based-position of a column when passed columnName.
     * @param columnName Name to lookup.
     * @param relationalStruct Where to do the lookup.
     * @return One-based positional index to use accessing the column.
     * @throws SQLException Thrown if we can't find matching column of we have trouble getting metadata.
     */
    static int getOneBasedPosition(String columnName, RelationalStruct relationalStruct) throws SQLException {
        StructMetaData metaData = relationalStruct.getMetaData();
        for (int i = 1; i <= metaData.getColumnCount(); i++) {
            if (metaData.getColumnName(i).equalsIgnoreCase(columnName)) {
                return i;
            }
        }
        throw new InvalidColumnReferenceException(columnName).toSqlException();
    }
}
