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

import com.apple.foundationdb.relational.api.KeySet;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.RelationalStatement;
import com.apple.foundationdb.relational.api.RelationalStruct;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.jdbc.grpc.GrpcSQLException;
import com.apple.foundationdb.relational.jdbc.grpc.v1.GetRequest;
import com.apple.foundationdb.relational.jdbc.grpc.v1.GetResponse;
import com.apple.foundationdb.relational.jdbc.grpc.v1.InsertRequest;
import com.apple.foundationdb.relational.jdbc.grpc.v1.InsertResponse;
import com.apple.foundationdb.relational.jdbc.grpc.v1.Parameter;
import com.apple.foundationdb.relational.jdbc.grpc.v1.Parameters;
import com.apple.foundationdb.relational.jdbc.grpc.v1.ScanRequest;
import com.apple.foundationdb.relational.jdbc.grpc.v1.ScanResponse;
import com.apple.foundationdb.relational.jdbc.grpc.v1.StatementRequest;
import com.apple.foundationdb.relational.jdbc.grpc.v1.StatementResponse;
import com.apple.foundationdb.relational.util.ExcludeFromJacocoGeneratedReport;
import com.apple.foundationdb.relational.util.SpotBugsSuppressWarnings;

import io.grpc.StatusRuntimeException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.Collection;
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
     * Special value that is used to indicate that a statement returned a {@link java.sql.ResultSet}. The
     * method #getUpdateCount() will return this value if the previous statement that
     * was executed with {@link Statement#execute(String)} returned a {@link java.sql.ResultSet}.
     */
    public static final int STATEMENT_RESULT_SET = -1;

    /**
     * Special value that is used to indicate that a statement had no result. The method #getUpdateCount() will return
     * this value if the previous statement that was executed
     * with #execute(String), such as DDL statements.
     */
    public static final int STATEMENT_NO_RESULT = -2;
    private Options options;

    @SpotBugsSuppressWarnings(value = "CT_CONSTRUCTOR_THROW", justification = "Should consider refactoring but throwing exceptions for now")
    JDBCRelationalStatement(@Nonnull final JDBCRelationalConnection connection) throws SQLException {
        this.connection = connection;
        this.options = connection.getOptions().withChild(Options.NONE);
    }

    private void checkOpen() throws SQLException {
        if (isClosed()) {
            throw new RelationalException("Statement closed", ErrorCode.STATEMENT_CLOSED).toSqlException();
        }
    }

    @Override
    public int getUpdateCount() throws SQLException {
        checkOpen();
        // "Retrieves the current result as an update count; if the result is a ResultSet object or there are no more
        // results, -1 is returned. This method should be called only once per result."
        return this.updateCount;
    }

    @Override
    public RelationalResultSet executeQuery(@Nonnull String sql) throws SQLException {
        if (execute(sql)) {
            return this.currentResultSet;
        } else {
            throw new SQLException("Cannot call executeQuery with an update statement", ErrorCode.INVALID_PARAMETER.getErrorCode());
        }
    }

    /**
     * Package private method for use by this class but also by {@link JDBCRelationalPreparedStatement}.
     * @param parameters Parameters for <code>sql</code> SORTED by input order or null if this is a Statement execute
     *                   (and non-null if preparedstatement).
     * @return ResultSet object that contains the data produced by the given query; never null
     * @throws SQLException if a database access error occurs or this method is called on a closed Statement
     */
    RelationalResultSet executeQuery(@Nonnull String sql, Collection<Parameter> parameters) throws SQLException {
        if (execute(sql, parameters)) {
            return currentResultSet;
        } else {
            throw new SQLException(String.format("query '%s' does not return result set, use JDBC executeUpdate method instead", sql), ErrorCode.NO_RESULT_SET.getErrorCode());
        }
    }

    @Override
    public int executeUpdate(@Nonnull String sql) throws SQLException {
        return executeUpdate(sql, (Collection<Parameter>) null);
    }

    /**
     * Package private method for use by this class but also by {@link JDBCRelationalPreparedStatement}.
     * @param parameters Parameters for <code>sql</code> SORTED by input order or null if this is a Statement execute
     *                   (and non-null if preparedstatement).
     * @return either (1) the row count for SQL Data Manipulation Language (DML) statements or (2) 0 for SQL statements
     *  that return nothing
     * @throws SQLException if a database access error occurs or this method is called on a closed Statement
     */
    int executeUpdate(@Nonnull String sql, Collection<Parameter> parameters) throws SQLException {
        if (execute(sql, parameters)) {
            throw new SQLException(String.format("query '%s' returns a result set, use JDBC executeQuery method instead", sql), ErrorCode.EXECUTE_UPDATE_RETURNED_RESULT_SET.getErrorCode());
        }
        return this.updateCount;
    }

    /**
     * Package private method for use by this class but also by {@link JDBCRelationalPreparedStatement}.
     * @param parameters Parameters for <code>sql</code> SORTED by input order or null if this is a Statement execute
     *                   (and non-null if preparedstatement).
     * @return true if the execution produced a result set, false otherwise
     * @throws SQLException if a database access error occurs or this method is called on a closed Statement
     */
    boolean execute(@Nonnull String sql, Collection<Parameter> parameters) throws SQLException {
        StatementResponse response = execute(sql, this.options, parameters);
        this.currentResultSet = response.hasResultSet() ?
                new RelationalResultSetFacade(response.getResultSet()) : RelationalResultSetFacade.EMPTY;
        this.updateCount = response.getRowCount();
        return response.hasResultSet();
    }

    /**
     * Engine to run sql for both updates and executes.
     * @param sql SQL to execute.
     * @param options Options to use executing <code>sql</code>
     * @param parameters Parameters for <code>sql</code> SORTED by input order or null if this is a Statement execute
     *                   (and non-null if preparedstatement).
     * @return StatementResponse.
     * @throws SQLException if a database access error occurs or this method is called on a closed Statement
     */
    @SuppressWarnings({"PMD.UnusedFormalParameter"}) // Will use it later.
    private StatementResponse execute(@Nonnull String sql, @Nonnull Options options, Collection<Parameter> parameters)
            throws SQLException {
        checkOpen();
        // Punt on transaction/autocommit consideration for now (autocommit==true).
        StatementResponse statementResponse = null;
        try {
            StatementRequest.Builder builder = StatementRequest.newBuilder()
                    .setSql(sql).setDatabase(this.connection.getDatabase()).setSchema(this.connection.getSchema()).setOptions(optionsAsProto());
            if (parameters != null) {
                builder.setParameters(Parameters.newBuilder().addAllParameter(parameters).build());
            }
            statementResponse = this.connection.getStub().execute(builder.build());
        } catch (StatusRuntimeException statusRuntimeException) {
            // Is this incoming statusRuntimeException carrying a SQLException?
            SQLException sqlException = GrpcSQLException.map(statusRuntimeException);
            if (sqlException == null) {
                throw statusRuntimeException;
            }
            throw sqlException;
        }
        return statementResponse;
    }

    @Override
    public void close() throws SQLException {
        this.closed = true;
    }

    @Override
    public int getMaxRows() throws SQLException {
        int pageSize = options.getOption(Options.Name.MAX_ROWS);
        if (pageSize == Integer.MAX_VALUE) {
            return 0;
        }
        return pageSize;
    }

    @Override
    public void setMaxRows(int max) throws SQLException {
        if (max == 0) {
            max = Integer.MAX_VALUE;
        }
        this.options = Options.builder().fromOptions(options).withOption(Options.Name.MAX_ROWS, max).build();
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
        return execute(sql, (Collection<Parameter>) null);
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
    @SuppressWarnings({"PMD.UnusedFormalParameter"}) // Will use it later.
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        return execute(sql);
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

    private com.apple.foundationdb.relational.jdbc.grpc.v1.Options optionsAsProto() {
        final var builder = com.apple.foundationdb.relational.jdbc.grpc.v1.Options.newBuilder();
        int maxRows = this.options.getOption(Options.Name.MAX_ROWS);
        if (maxRows != (int) Options.defaultOptions().get(Options.Name.MAX_ROWS)) {
            builder.setMaxRows(maxRows);
        }
        return builder.build();
    }
}
