/*
 * RelationalArray.java
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

import com.apple.foundationdb.relational.api.metadata.DataType;

import javax.annotation.Nonnull;
import java.sql.Array;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Wrapper;
import java.util.ArrayList;
import java.util.Map;

public interface RelationalArray extends Array, Wrapper, WithMetadata {

    ArrayMetaData getMetaData() throws SQLException;

    @Override
    default Object getArray() throws SQLException {
        return getArray(1L, Integer.MAX_VALUE);
    }

    @Override
    default Object getArray(long oneBasedIndex, int count) throws SQLException {
        final var array = new ArrayList<>();
        final var rs = getResultSet(oneBasedIndex, count);
        while (rs.next()) {
            array.add(rs.getObject(2));
        }
        return array.toArray();
    }

    @Override
    default Object getArray(Map<String, Class<?>> map) throws SQLException {
        throw new SQLFeatureNotSupportedException("Custom type mapping is not supported in Relational Arrays");
    }

    @Override
    default Object getArray(long index, int count, Map<String, Class<?>> map) throws SQLException {
        throw new SQLFeatureNotSupportedException("Custom type mapping is not supported in Relational Arrays");
    }

    @Override
    RelationalResultSet getResultSet(long index, int count) throws SQLException;

    @Override
    default RelationalResultSet getResultSet() throws SQLException {
        return getResultSet(1L, Integer.MAX_VALUE);
    }

    @Override
    default RelationalResultSet getResultSet(Map<String, Class<?>> map) throws SQLException {
        throw new SQLFeatureNotSupportedException("Custom type mapping is not supported in Relational Arrays");
    }

    @Override
    default RelationalResultSet getResultSet(long index, int count, Map<String, Class<?>> map) throws SQLException {
        throw new SQLFeatureNotSupportedException("Custom type mapping is not supported in Relational Arrays");
    }

    @Override
    default String getBaseTypeName() throws SQLException {
        return SqlTypeNamesSupport.getSqlTypeName(getBaseType());
    }

    @Override
    @SuppressWarnings("PMD.EmptyMethodInAbstractClassShouldBeAbstract") //Don't know why PMD doesn't like this, it's quite obviously intentional
    default void free() {
        //no-op
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
        return getMetaData().asRelationalType();
    }
}
