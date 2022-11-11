/*
 * JDBCRelationalStatement.java
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

package com.apple.foundationdb.relational.jdbc;

import com.apple.foundationdb.relational.api.DynamicMessageBuilder;
import com.apple.foundationdb.relational.api.KeySet;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.RelationalStatement;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.grpc.jdbc.v1.StatementRequest;
import com.apple.foundationdb.relational.grpc.jdbc.v1.StatementResponse;
import com.apple.foundationdb.relational.util.Assert;
import com.apple.foundationdb.relational.util.ExcludeFromJacocoGeneratedReport;
import com.apple.foundationdb.relational.util.SpotBugsSuppressWarnings;

import com.google.protobuf.Message;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.Iterator;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

// TODO: We throw RelationalExceptions in here and not SQLException. FIX.
class JDBCRelationalStatement implements RelationalStatement {
    private volatile boolean closed;
    @Nonnull
    private final JDBCRelationalConnection connection;
    @Nullable
    private RelationalResultSet currentResultSet;

    JDBCRelationalStatement(@Nonnull final JDBCRelationalConnection connection) {
        this.connection = connection;
    }

    @SuppressWarnings({"PMD.UnusedFormalParameter"}) // Will use it later.
    private RelationalResultSet execute(@Nonnull String sql, @Nonnull Options options)
            throws RelationalException, SQLException {
        Assert.notNull(sql);
        // Punt on transaction/autocommit consideration for now (autocommit==true).
        StatementResponse statementResponse =
                this.connection.getStub().execute(StatementRequest.newBuilder()
                        .setSql(sql).setDatabase(this.connection.getDatabase()).setSchema(this.connection.getSchema())
                        .build());
        return statementResponse.hasResultSet() ? new JDBCRelationalResultSet(statementResponse.getResultSet()) : null;
    }

    @SuppressWarnings({"PMD.UnusedFormalParameter"}) // Will use it later.
    private int update(@Nonnull String sql, @Nonnull Options options)
            throws RelationalException, SQLException {
        Assert.notNull(sql);
        // Punt on transaction/autocommit consideration for now (autocommit==true).
        StatementResponse statementResponse =
                this.connection.getStub().execute(StatementRequest.newBuilder()
                        .setSql(sql).setDatabase(this.connection.getDatabase()).setSchema(this.connection.getSchema())
                        .build());
        if (statementResponse.hasResultSet()) {
            throw new SQLException(String.format("Query '%s' returns a ResultSet; use JDBC executeQuery instead", sql));
        }
        return statementResponse.getRowCount();
    }

    @Override
    public RelationalResultSet executeQuery(String sql) throws SQLException {
        try {
            return execute(sql, Options.NONE);
        } catch (RelationalException ve) {
            throw ve.toSqlException();
        }
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        try {
            return update(sql, Options.NONE);
        } catch (RelationalException ve) {
            throw ve.toSqlException();
        }
    }

    @Override
    public void close() throws SQLException {
        this.closed = true;
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public int getMaxFieldSize() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public void setMaxFieldSize(int max) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public int getMaxRows() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public void setMaxRows(int max) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public void setEscapeProcessing(boolean enable) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public int getQueryTimeout() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public void setQueryTimeout(int seconds) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public void cancel() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public SQLWarning getWarnings() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public void clearWarnings() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public void setCursorName(String name) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public boolean execute(String sql) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public RelationalResultSet getResultSet() throws SQLException {
        if (this.currentResultSet != null /*&& !currentResultSet.isClosed()  todo implement this*/) {
            return this.currentResultSet;
        }
        throw new SQLException("No open result set available");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public int getUpdateCount() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public boolean getMoreResults() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public void setFetchDirection(int direction) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public int getFetchDirection() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public void setFetchSize(int rows) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public int getFetchSize() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public int getResultSetConcurrency() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public int getResultSetType() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public void addBatch(String sql) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public void clearBatch() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public int[] executeBatch() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public Connection getConnection() throws SQLException {
        return this.connection;
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException {
        return false;
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public ResultSet getGeneratedKeys() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        try {
            RelationalResultSet resultSet = execute(sql, Options.NONE);
            if (resultSet != null) {
                this.currentResultSet = resultSet;
                return true;
            } else {
                this.currentResultSet = null;
                return false;
            }
        } catch (RelationalException ve) {
            throw ve.toSqlException();
        }
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public int getResultSetHoldability() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public boolean isClosed() throws SQLException {
        return this.closed;
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public void setPoolable(boolean poolable) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public boolean isPoolable() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public void closeOnCompletion() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public boolean isCloseOnCompletion() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return iface.cast(this);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isInstance(this);
    }

    @Nonnull
    @Override
    @SpotBugsSuppressWarnings(value = "NP_NONNULL_RETURN_VIOLATION", justification = "Temporary until implemented.")
    @ExcludeFromJacocoGeneratedReport
    public RelationalResultSet executeScan(@Nonnull String tableName, @Nonnull KeySet keyPrefix, @Nonnull Options options) throws RelationalException {
        return null;
    }

    @Nonnull
    @Override
    @SpotBugsSuppressWarnings(value = "NP_NONNULL_RETURN_VIOLATION", justification = "Temporary until implemented.")
    @ExcludeFromJacocoGeneratedReport
    public RelationalResultSet executeGet(@Nonnull String tableName, @Nonnull KeySet key, @Nonnull Options options) throws RelationalException {
        return null;
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public int executeInsert(@Nonnull String tableName, @Nonnull Iterator<? extends Message> data, @Nonnull Options options) throws RelationalException {
        return -1;
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public DynamicMessageBuilder getDataBuilder(@Nonnull String typeName) throws RelationalException {
        return null;
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public int executeDelete(@Nonnull String tableName, @Nonnull Iterator<KeySet> keys, @Nonnull Options options) throws RelationalException {
        return -1;
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public void executeDeleteRange(@Nonnull String tableName, @Nonnull KeySet keyPrefix, @Nonnull Options options) throws RelationalException {
    }
}
