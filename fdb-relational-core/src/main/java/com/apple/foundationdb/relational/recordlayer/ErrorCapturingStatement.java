/*
 * ErrorCapturingStatement.java
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

import com.apple.foundationdb.relational.api.DynamicMessageBuilder;
import com.apple.foundationdb.relational.api.KeySet;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.RelationalStatement;
import com.apple.foundationdb.relational.api.RelationalStruct;
import com.apple.foundationdb.relational.recordlayer.util.ExceptionUtil;
import com.apple.foundationdb.relational.util.ExcludeFromJacocoGeneratedReport;

import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;


/**
 * A Delegating statement whose job is just to catch Runtime exceptions that don't match our expected
 * usage of Statements, and translate them to the correct SQL/Relational exceptions instead.
 */
@ExcludeFromJacocoGeneratedReport
public class ErrorCapturingStatement implements RelationalStatement {
    private final RelationalStatement delegate;

    public ErrorCapturingStatement(RelationalStatement delegate) {
        this.delegate = delegate;
    }

    @Override
    public int getUpdateCount() throws SQLException {
        try {
            return delegate.getUpdateCount();
        } catch (RuntimeException re) {
            throw ExceptionUtil.toRelationalException(re).toSqlException();
        }
    }

    @Override
    public RelationalResultSet getResultSet() throws SQLException {
        try {
            return delegate.getResultSet();
        } catch (RuntimeException re) {
            throw ExceptionUtil.toRelationalException(re).toSqlException();
        }
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        try {
            return delegate.execute(sql);
        } catch (RuntimeException re) {
            throw ExceptionUtil.toRelationalException(re).toSqlException();
        }
    }

    @Nonnull
    @Override
    public RelationalResultSet executeScan(@Nonnull String tableName, @Nonnull KeySet prefix, @Nonnull Options options) throws SQLException {
        try {
            return delegate.executeScan(tableName, prefix, options);
        } catch (RuntimeException re) {
            throw ExceptionUtil.toRelationalException(re).toSqlException();
        }
    }

    @Nonnull
    @Override
    public RelationalResultSet executeGet(@Nonnull String tableName, @Nonnull KeySet key, @Nonnull Options options) throws SQLException {
        try {
            return delegate.executeGet(tableName, key, options);
        } catch (RuntimeException re) {
            throw ExceptionUtil.toRelationalException(re).toSqlException();
        }
    }

    @Override
    public int executeInsert(@Nonnull String tableName, @Nonnull Iterator<? extends Message> data, @Nonnull Options options) throws SQLException {
        try {
            return delegate.executeInsert(tableName, data, options);
        } catch (RuntimeException re) {
            throw ExceptionUtil.toRelationalException(re).toSqlException();
        }
    }

    @Override
    public int executeInsert(@Nonnull String tableName, @Nonnull List<RelationalStruct> data, @Nonnull Options options)
            throws SQLException {
        try {
            return delegate.executeInsert(tableName, data, options);
        } catch (RuntimeException re) {
            throw ExceptionUtil.toRelationalException(re).toSqlException();
        }
    }

    @Override
    public DynamicMessageBuilder getDataBuilder(@Nonnull String tableName) throws SQLException {
        try {
            return delegate.getDataBuilder(tableName);
        } catch (RuntimeException re) {
            throw ExceptionUtil.toRelationalException(re).toSqlException();
        }
    }

    @Override
    public DynamicMessageBuilder getDataBuilder(@Nonnull final String maybeQualifiedTableName, @Nonnull final List<String> nestedFields) throws SQLException {
        try {
            return delegate.getDataBuilder(maybeQualifiedTableName, nestedFields);
        } catch (RuntimeException re) {
            throw ExceptionUtil.toRelationalException(re).toSqlException();
        }
    }

    @Override
    public int executeDelete(@Nonnull String tableName, @Nonnull Iterator<KeySet> keys, @Nonnull Options options) throws SQLException {
        try {
            return delegate.executeDelete(tableName, keys, options);
        } catch (RuntimeException re) {
            throw ExceptionUtil.toRelationalException(re).toSqlException();
        }
    }

    @Override
    public void executeDeleteRange(@Nonnull String tableName, @Nonnull KeySet prefix, @Nonnull Options options) throws SQLException {
        try {
            delegate.executeDeleteRange(tableName, prefix, options);
        } catch (RuntimeException re) {
            throw ExceptionUtil.toRelationalException(re).toSqlException();
        }
    }

    @Override
    public RelationalResultSet executeQuery(String sql) throws SQLException {
        try {
            return delegate.executeQuery(sql);
        } catch (RuntimeException re) {
            throw ExceptionUtil.toRelationalException(re).toSqlException();
        }
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        try {
            return delegate.executeUpdate(sql);
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
}
