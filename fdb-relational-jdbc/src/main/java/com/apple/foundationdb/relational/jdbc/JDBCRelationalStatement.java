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
import com.apple.foundationdb.relational.api.RelationalStruct;
import com.apple.foundationdb.relational.jdbc.grpc.GrpcSQLException;
import com.apple.foundationdb.relational.jdbc.grpc.v1.GetRequest;
import com.apple.foundationdb.relational.jdbc.grpc.v1.GetResponse;
import com.apple.foundationdb.relational.jdbc.grpc.v1.InsertRequest;
import com.apple.foundationdb.relational.jdbc.grpc.v1.InsertResponse;
import com.apple.foundationdb.relational.jdbc.grpc.v1.ListBytes;
import com.apple.foundationdb.relational.jdbc.grpc.v1.ScanRequest;
import com.apple.foundationdb.relational.jdbc.grpc.v1.ScanResponse;
import com.apple.foundationdb.relational.jdbc.grpc.v1.StatementRequest;
import com.apple.foundationdb.relational.jdbc.grpc.v1.StatementResponse;
import com.apple.foundationdb.relational.util.ExcludeFromJacocoGeneratedReport;
import com.apple.foundationdb.relational.util.SpotBugsSuppressWarnings;

import com.google.protobuf.Message;
import io.grpc.StatusRuntimeException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.Iterator;
import java.util.List;


class JDBCRelationalStatement implements RelationalStatement {
    private volatile boolean closed;
    @Nonnull
    private final JDBCRelationalConnection connection;
    @Nullable
    private RelationalResultSet currentResultSet;

    /**
     * Running result state.
     * For {@link #getUpdateCount()}.
     * TODO: Related to {@link #currentResultSet} ?
     * TODO: More precision around when this value changes and how it changes (study other jdbc drivers).
     */
    private int updateCount = STATEMENT_NO_RESULT;

    /**
     * Special value that is used to indicate that a statement returned a {@link ResultSet}. The
     * method {@link Statement#getUpdateCount()} will return this value if the previous statement that
     * was executed with {@link Statement#execute(String)} returned a {@link ResultSet}.
     */
    public static final int STATEMENT_RESULT_SET = -1;
    /**
     * Special value that is used to indicate that a statement had no result. The method {@link
     * Statement#getUpdateCount()} will return this value if the previous statement that was executed
     * with {@link Statement#execute(String)}, such as DDL statements.
     */
    public static final int STATEMENT_NO_RESULT = -2;

    JDBCRelationalStatement(@Nonnull final JDBCRelationalConnection connection) {
        this.connection = connection;
    }

    private void checkOpen() throws SQLException {
        if (isClosed()) {
            throw new SQLException("Statement closed");
        }
    }

    @Override
    public int getUpdateCount() throws SQLException {
        checkOpen();
        // "Retrieves the current result as an update count; if the result is a ResultSet object or there are no more
        // results, -1 is returned. This method should be called only once per result."
        return this.updateCount;
    }

    @SuppressWarnings({"PMD.UnusedFormalParameter"}) // Will use it later.
    private RelationalResultSet execute(@Nonnull String sql, @Nonnull Options options)
            throws SQLException {
        checkOpen();
        // Punt on transaction/autocommit consideration for now (autocommit==true).
        StatementResponse statementResponse = null;
        try {
            statementResponse = this.connection.getStub().execute(StatementRequest.newBuilder()
                    .setSql(sql).setDatabase(this.connection.getDatabase()).setSchema(this.connection.getSchema())
                    .build());
        } catch (StatusRuntimeException statusRuntimeException) {
            // Is this incoming statusRuntimeException carrying a SQLException?
            SQLException sqlException = GrpcSQLException.map(statusRuntimeException);
            if (sqlException == null) {
                throw statusRuntimeException;
            }
            throw sqlException;
        }
        this.updateCount = STATEMENT_RESULT_SET;
        return statementResponse.hasResultSet() ? new RelationalResultSetFacade(statementResponse.getResultSet()) : null;
    }

    @SuppressWarnings({"PMD.UnusedFormalParameter"}) // Will use it later.
    private int update(@Nonnull String sql, @Nonnull Options options)
            throws SQLException {
        checkOpen();
        // Punt on transaction/autocommit consideration for now (autocommit==true).
        StatementResponse statementResponse = null;
        try {
            statementResponse = this.connection.getStub().execute(StatementRequest.newBuilder()
                    .setSql(sql).setDatabase(this.connection.getDatabase()).setSchema(this.connection.getSchema())
                    .build());
        } catch (StatusRuntimeException statusRuntimeException) {
            // Is this incoming statusRuntimeException carrying a SQLException?
            SQLException sqlException = GrpcSQLException.map(statusRuntimeException);
            if (sqlException == null) {
                throw statusRuntimeException;
            }
            throw sqlException;
        }
        if (statementResponse.hasResultSet()) {
            throw new SQLException(String.format("Query '%s' returns a ResultSet; use JDBC executeQuery instead", sql));
        }
        this.updateCount = statementResponse.getRowCount();
        return this.updateCount;
    }

    @Override
    public RelationalResultSet executeQuery(String sql) throws SQLException {
        checkOpen();
        this.currentResultSet = execute(sql, Options.NONE);
        return this.currentResultSet;
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        checkOpen();
        return update(sql, Options.NONE);
    }

    @Override
    public void close() throws SQLException {
        this.closed = true;
    }

    @Override
    public void cancel() throws SQLException {
        // TODO: For now swallow until there is something to cancel.
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        // TODO: For now just return null.
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {
        // TODO: For now, do nothing.
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        checkOpen();
        this.currentResultSet = this.execute(sql, Options.NONE);
        return this.currentResultSet != null;
    }

    @Override
    public RelationalResultSet getResultSet() throws SQLException {
        checkOpen();
        if (this.currentResultSet != null /*&& !currentResultSet.isClosed()  todo implement this*/) {
            return this.currentResultSet;
        }
        throw new SQLException("No open result set available");
    }

    @Override
    public Connection getConnection() throws SQLException {
        checkOpen();
        return this.connection;
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        checkOpen();
        RelationalResultSet resultSet = execute(sql, Options.NONE);
        if (resultSet != null) {
            this.currentResultSet = resultSet;
            return true;
        } else {
            this.currentResultSet = null;
            return false;
        }
    }

    @Override
    public boolean isClosed() throws SQLException {
        return this.closed;
    }

    @Nonnull
    @Override
    @SpotBugsSuppressWarnings(value = "NP_NONNULL_RETURN_VIOLATION", justification = "Temporary until implemented.")
    @ExcludeFromJacocoGeneratedReport
    public RelationalResultSet executeGet(@Nonnull String tableName, @Nonnull KeySet keySet, @Nonnull Options options) throws SQLException {
        checkOpen();
        GetResponse getResponse = null;
        try {
            getResponse = this.connection.getStub().get(GetRequest.newBuilder()
                    .setKeySet(TypeConversion.toProtobuf(keySet))
                    .setDatabase(this.connection.getDatabase())
                    .setSchema(this.connection.getSchema())
                    .setTableName(tableName)
                    .build());
        } catch (StatusRuntimeException statusRuntimeException) {
            // Is this incoming statusRuntimeException carrying a SQLException?
            SQLException sqlException = GrpcSQLException.map(statusRuntimeException);
            if (sqlException == null) {
                throw statusRuntimeException;
            }
            throw sqlException;
        }
        this.updateCount = STATEMENT_RESULT_SET;
        return getResponse == null ? null : new RelationalResultSetFacade(getResponse.getResultSet());
    }

    @Nonnull
    @Override
    @SpotBugsSuppressWarnings(value = "NP_NONNULL_RETURN_VIOLATION", justification = "Temporary until implemented.")
    @ExcludeFromJacocoGeneratedReport
    public RelationalResultSet executeScan(@Nonnull String tableName, @Nonnull KeySet keySet, @Nonnull Options options)
            throws SQLException {
        checkOpen();
        ScanResponse response = null;
        try {
            response = this.connection.getStub().scan(ScanRequest.newBuilder()
                    .setKeySet(TypeConversion.toProtobuf(keySet))
                    .setDatabase(this.connection.getDatabase())
                    .setSchema(this.connection.getSchema())
                    .setTableName(tableName)
                    .build());
        } catch (StatusRuntimeException statusRuntimeException) {
            // Is this incoming statusRuntimeException carrying a SQLException?
            SQLException sqlException = GrpcSQLException.map(statusRuntimeException);
            if (sqlException == null) {
                throw statusRuntimeException;
            }
            throw sqlException;
        }
        return response == null ? null : new RelationalResultSetFacade(response.getResultSet());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public int executeInsert(@Nonnull String tableName, @Nonnull Iterator<? extends Message> data, @Nonnull Options options) throws SQLException {
        checkOpen();
        InsertResponse insertResponse = null;
        try {
            // Add the messages as opaque ByteStrings. On the other side, it uses context to figure how to parse the
            // bytes back up into a protobuf Message again.
            var listBytesBuilder = ListBytes.newBuilder();
            while (data.hasNext()) {
                listBytesBuilder.addBytes(data.next().toByteString());
            }
            insertResponse = this.connection.getStub().insert(InsertRequest.newBuilder()
                    .setDataListBytes(listBytesBuilder.build())
                    .setDatabase(this.connection.getDatabase())
                    .setSchema(this.connection.getSchema())
                    .setTableName(tableName)
                    .build());
        } catch (StatusRuntimeException statusRuntimeException) {
            // Is this incoming statusRuntimeException carrying a SQLException?
            SQLException sqlException = GrpcSQLException.map(statusRuntimeException);
            if (sqlException == null) {
                throw statusRuntimeException;
            }
            throw sqlException;
        }
        this.updateCount = insertResponse == null ? STATEMENT_NO_RESULT : insertResponse.getRowCount();
        return this.updateCount;
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public int executeInsert(@Nonnull String tableName, @Nonnull List<RelationalStruct> data, @Nonnull Options options)
            throws SQLException {
        checkOpen();
        InsertResponse insertResponse = null;
        try {
            insertResponse = this.connection.getStub().insert(InsertRequest.newBuilder()
                    .setDataResultSet(TypeConversion.toResultSetProtobuf(data))
                    .setDatabase(this.connection.getDatabase())
                    .setSchema(this.connection.getSchema())
                    .setTableName(tableName)
                    .build());
        } catch (StatusRuntimeException statusRuntimeException) {
            // Is this incoming statusRuntimeException carrying a SQLException?
            SQLException sqlException = GrpcSQLException.map(statusRuntimeException);
            if (sqlException == null) {
                throw statusRuntimeException;
            }
            throw sqlException;
        }
        this.updateCount = insertResponse == null ? STATEMENT_NO_RESULT : insertResponse.getRowCount();
        return this.updateCount;
    }

    @Deprecated
    @Override
    @ExcludeFromJacocoGeneratedReport
    @SpotBugsSuppressWarnings(value = "NP_NONNULL_RETURN_VIOLATION", justification = "Temporary until implemented.")
    public DynamicMessageBuilder getDataBuilder(@Nonnull String tableName) throws SQLException {
        throw new SQLException("Not implemented " + Thread.currentThread() .getStackTrace()[1] .getMethodName());
    }

    @Override
    @SpotBugsSuppressWarnings(value = "NP_NONNULL_RETURN_VIOLATION", justification = "Temporary until implemented.")
    @ExcludeFromJacocoGeneratedReport
    public DynamicMessageBuilder getDataBuilder(@Nonnull String maybeQualifiedTableName, @Nonnull final List<String> nestedFields) throws SQLException {
        throw new SQLException("Not implemented " + Thread.currentThread() .getStackTrace()[1] .getMethodName());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public int executeDelete(@Nonnull String tableName, @Nonnull Iterator<KeySet> keys, @Nonnull Options options) throws SQLException {
        checkOpen();
        return -1;
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public void executeDeleteRange(@Nonnull String tableName, @Nonnull KeySet keyPrefix, @Nonnull Options options) throws SQLException {
        checkOpen();
    }
}
