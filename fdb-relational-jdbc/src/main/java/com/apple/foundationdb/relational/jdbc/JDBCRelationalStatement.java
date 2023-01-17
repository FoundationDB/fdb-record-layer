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
import com.apple.foundationdb.relational.grpc.GrpcSQLException;
import com.apple.foundationdb.relational.grpc.jdbc.v1.GetRequest;
import com.apple.foundationdb.relational.grpc.jdbc.v1.GetResponse;
import com.apple.foundationdb.relational.grpc.jdbc.v1.InsertRequest;
import com.apple.foundationdb.relational.grpc.jdbc.v1.InsertResponse;
import com.apple.foundationdb.relational.grpc.jdbc.v1.KeySetValue;
import com.apple.foundationdb.relational.grpc.jdbc.v1.ScanRequest;
import com.apple.foundationdb.relational.grpc.jdbc.v1.ScanResponse;
import com.apple.foundationdb.relational.grpc.jdbc.v1.StatementRequest;
import com.apple.foundationdb.relational.grpc.jdbc.v1.StatementResponse;
import com.apple.foundationdb.relational.util.ExcludeFromJacocoGeneratedReport;
import com.apple.foundationdb.relational.util.SpotBugsSuppressWarnings;

import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import io.grpc.StatusRuntimeException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;


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
            throws SQLException {
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
        return statementResponse.hasResultSet() ? new JDBCRelationalResultSet(statementResponse.getResultSet()) : null;
    }

    @SuppressWarnings({"PMD.UnusedFormalParameter"}) // Will use it later.
    private int update(@Nonnull String sql, @Nonnull Options options)
            throws SQLException {
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
        return statementResponse.getRowCount();
    }

    @Override
    public RelationalResultSet executeQuery(String sql) throws SQLException {
        this.currentResultSet = execute(sql, Options.NONE);
        return this.currentResultSet;
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
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
        this.currentResultSet = this.execute(sql, Options.NONE);
        return this.currentResultSet != null;
    }

    @Override
    public RelationalResultSet getResultSet() throws SQLException {
        if (this.currentResultSet != null /*&& !currentResultSet.isClosed()  todo implement this*/) {
            return this.currentResultSet;
        }
        throw new SQLException("No open result set available");
    }

    @Override
    public Connection getConnection() throws SQLException {
        return this.connection;
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
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
    public RelationalResultSet executeGet(@Nonnull String tableName, @Nonnull KeySet key, @Nonnull Options options) throws SQLException {
        GetResponse getResponse = null;
        try {
            com.apple.foundationdb.relational.grpc.jdbc.v1.KeySet.Builder keySetBuilder =
                    com.apple.foundationdb.relational.grpc.jdbc.v1.KeySet.newBuilder();
            for (Map.Entry<String, Object> entry : key.toMap().entrySet()) {
                KeySetValue keySetValue = null;
                // Currently we support a few types only.
                if (entry.getValue() instanceof String) {
                    keySetValue = KeySetValue.newBuilder().setStringValue((String) entry.getValue()).build();
                } else if (entry.getValue() instanceof byte[]) {
                    keySetValue =
                            KeySetValue.newBuilder().setBytesValue(ByteString.copyFrom((byte[]) entry.getValue())).build();
                } else if (entry.getValue() instanceof Long) {
                    keySetValue = KeySetValue.newBuilder().setLongValue((long) entry.getValue()).build();
                } else {
                    throw new UnsupportedOperationException("Unsupported type " + entry.getValue());
                }
                keySetBuilder.putFields(entry.getKey(), keySetValue);
            }
            getResponse = this.connection.getStub().get(GetRequest.newBuilder()
                    .setKeySet(keySetBuilder.build())
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
        return getResponse == null ? null : new JDBCRelationalResultSet(getResponse.getResultSet());
    }

    @Nonnull
    @Override
    @SpotBugsSuppressWarnings(value = "NP_NONNULL_RETURN_VIOLATION", justification = "Temporary until implemented.")
    @ExcludeFromJacocoGeneratedReport
    public RelationalResultSet executeScan(@Nonnull String tableName, @Nonnull KeySet key, @Nonnull Options options)
            throws SQLException {
        ScanResponse response = null;
        try {
            com.apple.foundationdb.relational.grpc.jdbc.v1.KeySet.Builder keySetBuilder =
                    com.apple.foundationdb.relational.grpc.jdbc.v1.KeySet.newBuilder();
            for (Map.Entry<String, Object> entry : key.toMap().entrySet()) {
                KeySetValue keySetValue = null;
                // Currently we support a few types only.
                if (entry.getValue() instanceof String) {
                    keySetValue = KeySetValue.newBuilder().setStringValue((String) entry.getValue()).build();
                } else if (entry.getValue() instanceof byte[]) {
                    keySetValue =
                            KeySetValue.newBuilder().setBytesValue(ByteString.copyFrom((byte[]) entry.getValue())).build();
                } else if (entry.getValue() instanceof Long) {
                    keySetValue = KeySetValue.newBuilder().setLongValue((long) entry.getValue()).build();
                } else {
                    throw new UnsupportedOperationException("Unsupported type " + entry.getValue());
                }
                keySetBuilder.putFields(entry.getKey(), keySetValue);
            }
            response = this.connection.getStub().scan(ScanRequest.newBuilder().setKeySet(keySetBuilder.build())
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
        return response == null ? null : new JDBCRelationalResultSet(response.getResultSet());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public int executeInsert(@Nonnull String tableName, @Nonnull Iterator<? extends Message> data, @Nonnull Options options) throws SQLException {
        InsertResponse insertResponse = null;
        try {
            // Add the messages as opaque ByteStrings. On the other side, it uses context to figure how to parse the
            // bytes back up into a protobuf Message again.
            List<ByteString> byteStrings = new ArrayList<>();
            while (data.hasNext()) {
                byteStrings.add(data.next().toByteString());
            }
            insertResponse = this.connection.getStub().insert(InsertRequest.newBuilder()
                    .addAllData(byteStrings)
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
        return insertResponse == null ? -1 : insertResponse.getRowCount();
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    @SpotBugsSuppressWarnings(value = "NP_NONNULL_RETURN_VIOLATION", justification = "Temporary until implemented.")
    public DynamicMessageBuilder getDataBuilder(@Nonnull String tableName) throws SQLException {
        return null;
    }

    @Override
    @SpotBugsSuppressWarnings(value = "NP_NONNULL_RETURN_VIOLATION", justification = "Temporary until implemented.")
    @ExcludeFromJacocoGeneratedReport
    public DynamicMessageBuilder getDataBuilder(@Nonnull String maybeQualifiedTableName, @Nonnull final List<String> nestedFields) throws SQLException {
        return null;
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public int executeDelete(@Nonnull String tableName, @Nonnull Iterator<KeySet> keys, @Nonnull Options options) throws SQLException {
        return -1;
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public void executeDeleteRange(@Nonnull String tableName, @Nonnull KeySet keyPrefix, @Nonnull Options options) throws SQLException {
    }
}
