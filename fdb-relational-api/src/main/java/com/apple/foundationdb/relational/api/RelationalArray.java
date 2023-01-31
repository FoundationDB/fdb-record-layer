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

import java.sql.Array;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Wrapper;
import java.util.Map;

public abstract class RelationalArray implements Array, Wrapper {

    @Override
    public Object getArray() throws SQLException {
        return getArray(1L, Integer.MAX_VALUE);
    }

    @Override
    public Object getArray(Map<String, Class<?>> map) throws SQLException {
        throw new SQLFeatureNotSupportedException("Custom type mapping is not supported in Relational Arrays");
    }

    @Override
    public Object getArray(long index, int count, Map<String, Class<?>> map) throws SQLException {
        throw new SQLFeatureNotSupportedException("Custom type mapping is not supported in Relational Arrays");
    }

    @Override
    public abstract RelationalResultSet getResultSet(long index, int count) throws SQLException;

    @Override
    public RelationalResultSet getResultSet() throws SQLException {
        return getResultSet(1L, Integer.MAX_VALUE);
    }

    @Override
    public RelationalResultSet getResultSet(Map<String, Class<?>> map) throws SQLException {
        throw new SQLFeatureNotSupportedException("Custom type mapping is not supported in Relational Arrays");
    }

    @Override
    public RelationalResultSet getResultSet(long index, int count, Map<String, Class<?>> map) throws SQLException {
        throw new SQLFeatureNotSupportedException("Custom type mapping is not supported in Relational Arrays");
    }

    @Override
    @SuppressWarnings("PMD.EmptyMethodInAbstractClassShouldBeAbstract") //Don't know why PMD doesn't like this, it's quite obviously intentional
    public void free() {
        //no-op
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (isWrapperFor(iface)) {
            return iface.cast(this);
        }
        throw new SQLException("Unwrap failed for: " + iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isInstance(this);
    }
}
