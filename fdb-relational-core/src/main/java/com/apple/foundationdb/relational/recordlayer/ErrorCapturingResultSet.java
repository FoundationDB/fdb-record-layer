/*
 * ErrorCapturingResultSet.java
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

package com.apple.foundationdb.relational.recordlayer;

import com.apple.foundationdb.annotation.API;

import com.apple.foundationdb.relational.api.Continuation;
import com.apple.foundationdb.relational.api.RelationalArray;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.RelationalResultSetMetaData;
import com.apple.foundationdb.relational.api.RelationalStruct;
import com.apple.foundationdb.relational.recordlayer.util.ExceptionUtil;
import com.apple.foundationdb.relational.util.ExcludeFromJacocoGeneratedReport;

import javax.annotation.Nonnull;
import java.sql.SQLException;

@ExcludeFromJacocoGeneratedReport //there's nothing to test, just exception translation
@API(API.Status.EXPERIMENTAL)
public class ErrorCapturingResultSet implements RelationalResultSet {
    private final RelationalResultSet delegate;

    public ErrorCapturingResultSet(RelationalResultSet delegate) {
        this.delegate = delegate;
    }

    @Override
    public boolean next() throws SQLException {
        try {
            return delegate.next();
        } catch (RuntimeException re) {
            throw ExceptionUtil.toRelationalException(re).toSqlException();
        }
    }

    @Override
    public void close() throws SQLException {
        try {
            delegate.close();
        } catch (RuntimeException re) {
            throw ExceptionUtil.toRelationalException(re).toSqlException();
        }
    }

    @Override
    public boolean wasNull() throws SQLException {
        return delegate.wasNull();
    }

    @Override
    public String getString(int columnIndex) throws SQLException {
        try {
            return delegate.getString(columnIndex);
        } catch (RuntimeException re) {
            throw ExceptionUtil.toRelationalException(re).toSqlException();
        }
    }

    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {
        try {
            return delegate.getBoolean(columnIndex);
        } catch (RuntimeException re) {
            throw ExceptionUtil.toRelationalException(re).toSqlException();
        }
    }

    @Override
    public int getInt(int columnIndex) throws SQLException {
        try {
            return delegate.getInt(columnIndex);
        } catch (RuntimeException re) {
            throw ExceptionUtil.toRelationalException(re).toSqlException();
        }
    }

    @Override
    public long getLong(int columnIndex) throws SQLException {
        try {
            return delegate.getLong(columnIndex);
        } catch (RuntimeException re) {
            throw ExceptionUtil.toRelationalException(re).toSqlException();
        }
    }

    @Override
    public float getFloat(int columnIndex) throws SQLException {
        try {
            return delegate.getFloat(columnIndex);
        } catch (RuntimeException re) {
            throw ExceptionUtil.toRelationalException(re).toSqlException();
        }
    }

    @Override
    public double getDouble(int columnIndex) throws SQLException {
        try {
            return delegate.getDouble(columnIndex);
        } catch (RuntimeException re) {
            throw ExceptionUtil.toRelationalException(re).toSqlException();
        }
    }

    @Override
    public byte[] getBytes(int columnIndex) throws SQLException {
        try {
            return delegate.getBytes(columnIndex);
        } catch (RuntimeException re) {
            throw ExceptionUtil.toRelationalException(re).toSqlException();
        }
    }

    @Override
    public String getString(String columnLabel) throws SQLException {
        try {
            return delegate.getString(columnLabel);
        } catch (RuntimeException re) {
            throw ExceptionUtil.toRelationalException(re).toSqlException();
        }
    }

    @Override
    public boolean getBoolean(String columnLabel) throws SQLException {
        try {
            return delegate.getBoolean(columnLabel);
        } catch (RuntimeException re) {
            throw ExceptionUtil.toRelationalException(re).toSqlException();
        }
    }

    @Override
    public int getInt(String columnLabel) throws SQLException {
        try {
            return delegate.getInt(columnLabel);
        } catch (RuntimeException re) {
            throw ExceptionUtil.toRelationalException(re).toSqlException();
        }
    }

    @Override
    public long getLong(String columnLabel) throws SQLException {
        try {
            return delegate.getLong(columnLabel);
        } catch (RuntimeException re) {
            throw ExceptionUtil.toRelationalException(re).toSqlException();
        }
    }

    @Override
    public float getFloat(String columnLabel) throws SQLException {
        try {
            return delegate.getFloat(columnLabel);
        } catch (RuntimeException re) {
            throw ExceptionUtil.toRelationalException(re).toSqlException();
        }
    }

    @Override
    public double getDouble(String columnLabel) throws SQLException {
        try {
            return delegate.getDouble(columnLabel);
        } catch (RuntimeException re) {
            throw ExceptionUtil.toRelationalException(re).toSqlException();
        }
    }

    @Override
    public byte[] getBytes(String columnLabel) throws SQLException {
        try {
            return delegate.getBytes(columnLabel);
        } catch (RuntimeException re) {
            throw ExceptionUtil.toRelationalException(re).toSqlException();
        }
    }

    @Override
    public RelationalResultSetMetaData getMetaData() throws SQLException {
        try {
            return delegate.getMetaData();
        } catch (RuntimeException re) {
            throw ExceptionUtil.toRelationalException(re).toSqlException();
        }
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {
        try {
            return delegate.getObject(columnIndex);
        } catch (RuntimeException re) {
            throw ExceptionUtil.toRelationalException(re).toSqlException();
        }
    }

    @Override
    public Object getObject(String columnLabel) throws SQLException {
        try {
            return delegate.getObject(columnLabel);
        } catch (RuntimeException re) {
            throw ExceptionUtil.toRelationalException(re).toSqlException();
        }
    }

    @Override
    public RelationalArray getArray(int columnIndex) throws SQLException {
        try {
            return delegate.getArray(columnIndex);
        } catch (RuntimeException re) {
            throw ExceptionUtil.toRelationalException(re).toSqlException();
        }
    }

    @Override
    public RelationalArray getArray(String columnLabel) throws SQLException {
        try {
            return delegate.getArray(columnLabel);
        } catch (RuntimeException re) {
            throw ExceptionUtil.toRelationalException(re).toSqlException();
        }
    }

    @Override
    public boolean isClosed() throws SQLException {
        return delegate.isClosed();
    }

    @Nonnull
    @Override
    public Continuation getContinuation() throws SQLException {
        try {
            return delegate.getContinuation();
        } catch (RuntimeException re) {
            throw ExceptionUtil.toRelationalException(re).toSqlException();
        }
    }

    @Override
    public RelationalStruct getStruct(String columnLabel) throws SQLException {
        try {
            return delegate.getStruct(columnLabel);
        } catch (RuntimeException re) {
            throw ExceptionUtil.toRelationalException(re).toSqlException();
        }
    }

    @Override
    public RelationalStruct getStruct(int oneBasedColumn) throws SQLException {
        try {
            return delegate.getStruct(oneBasedColumn);
        } catch (RuntimeException re) {
            throw ExceptionUtil.toRelationalException(re).toSqlException();
        }
    }
}
