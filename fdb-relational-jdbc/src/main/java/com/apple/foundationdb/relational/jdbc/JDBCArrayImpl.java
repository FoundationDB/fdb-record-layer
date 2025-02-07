/*
 * JDBCArrayImpl.java
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

import javax.annotation.Nonnull;
import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Map;

/**
 * A simplistic implementation of a {@link Array} that wraps around a
 * {@link com.apple.foundationdb.relational.jdbc.grpc.v1.column.Array}.
 * TODO: We can't use the other implementation of array in {@link RelationalArrayFacade} as it makes several assumptions
 * that would be incompatible with the need to transfer an array across the wire (e.g. the type of element is missing,
 * {@link RelationalArrayFacade#getArray()} returns a type other than a Java array).
 */
public class JDBCArrayImpl implements Array {
    @Nonnull
    private final com.apple.foundationdb.relational.jdbc.grpc.v1.column.Array underlying;

    public JDBCArrayImpl(@Nonnull final com.apple.foundationdb.relational.jdbc.grpc.v1.column.Array underlying) {
        this.underlying = underlying;
    }

    @Override
    public String getBaseTypeName() throws SQLException {
        return SqlTypeNamesSupport.getSqlTypeName(getBaseType());
    }

    @Override
    public int getBaseType() throws SQLException {
        return underlying.getElementType();
    }

    @Override
    public Object getArray() throws SQLException {
        return TypeConversion.fromArray(underlying);
    }

    @Override
    public Object getArray(final Map<String, Class<?>> map) throws SQLException {
        throw new SQLFeatureNotSupportedException("Custom type mapping is not supported");
    }

    @Override
    public Object getArray(final long index, final int count) throws SQLException {
        throw new SQLFeatureNotSupportedException("Array slicing is not supported");
    }

    @Override
    public Object getArray(final long index, final int count, final Map<String, Class<?>> map) throws SQLException {
        throw new SQLFeatureNotSupportedException("Custom type mapping is not supported");
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        throw new SQLFeatureNotSupportedException("Array as result set is not supported");
    }

    @Override
    public ResultSet getResultSet(final Map<String, Class<?>> map) throws SQLException {
        throw new SQLFeatureNotSupportedException("Array as result set is not supported");
    }

    @Override
    public ResultSet getResultSet(final long index, final int count) throws SQLException {
        throw new SQLFeatureNotSupportedException("Array as result set is not supported");
    }

    @Override
    public ResultSet getResultSet(final long index, final int count, final Map<String, Class<?>> map) throws SQLException {
        throw new SQLFeatureNotSupportedException("Array as result set is not supported");
    }

    @Override
    public void free() throws SQLException {
    }

    /**
     * Package protected getter.
     * @return the underlying protobuf struct
     */
    @Nonnull
    public com.apple.foundationdb.relational.jdbc.grpc.v1.column.Array getUnderlying() {
        return underlying;
    }
}
