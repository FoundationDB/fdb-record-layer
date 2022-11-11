/*
 * JDBCRelationalConnection.java
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

import com.apple.foundationdb.relational.grpc.GrpcConstants;
import com.apple.foundationdb.relational.grpc.jdbc.v1.DatabaseMetaDataRequest;
import com.apple.foundationdb.relational.grpc.jdbc.v1.JDBCServiceGrpc;
import com.apple.foundationdb.relational.util.ExcludeFromJacocoGeneratedReport;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.net.URI;
import java.net.URISyntaxException;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

/**
 * Connect to a Relational Database Server.
 * JDBC URL says where to connect -- host and port -- and to which database.
 * Optionally, pass schema and database options.
 * TODO: Formal specification of the JDBC Connection URL.
 */
// JDBC natively wants to do a Connection per database.
// Flipping between databases should be lightweight in Relational (there may even be cases where a transaction
// spans databases). This argues that this Connection be able to do 'switch' between databases and that when
// we do, it would be better if we didn't have to set up a whole new rpc and stub. For now we are a Database
// per Connection. TODO.
class JDBCRelationalConnection implements Connection {
    private volatile boolean closed;
    private final ManagedChannel managedChannel;
    private final String database;
    private String schema;
    private final JDBCServiceGrpc.JDBCServiceBlockingStub blockingStub;

    JDBCRelationalConnection(String url) {
        if (!url.startsWith(JDBCConstants.JDBC_URL_PREFIX)) {
            throw new IllegalArgumentException("Missing 'jdbc:' prefix: " + url);
        }
        // Parse the url String as a URI; makes it easy to pull out the pieces.
        URI uri;
        try {
            uri = new URI(url.substring(JDBCConstants.JDBC_URL_PREFIX.length()));
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException(e);
        }
        if (!uri.getScheme().equals(JDBCConstants.JDBC_URL_SCHEME)) {
            throw new IllegalArgumentException("Not a relational jdbc url: " + url);
        }
        int port = uri.getPort();
        if (port == -1) {
            // Port is -1 if not defined in the JDBC URL.
            port = GrpcConstants.DEFAULT_SERVER_PORT;
        }
        this.database = uri.getPath();
        // TODO: Better parsing here. Query may have 'options=', etc.
        String schemaPrefix = "schema=";
        if (uri.getQuery() != null && uri.getQuery().contains(schemaPrefix)) {
            this.schema = uri.getQuery().substring(schemaPrefix.length());
        }
        this.managedChannel = ManagedChannelBuilder
                .forAddress(uri.getHost(), port)
                .usePlaintext().build();
        this.blockingStub = JDBCServiceGrpc.newBlockingStub(managedChannel);
    }

    JDBCServiceGrpc.JDBCServiceBlockingStub getStub() {
        return this.blockingStub;
    }

    /**
     * The database we have a connection too.
     * @return The database we have a connection too (TODO: Can we fix the javadoc rule so I only  have to add the
     * '@return' here and not have to have the line above too?).
     */
    String getDatabase() {
        return this.database;
    }

    @Override
    public Statement createStatement() throws SQLException {
        return new JDBCRelationalStatement(this);
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public CallableStatement prepareCall(String sql) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public String nativeSQL(String sql) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        // https://www.baeldung.com/java-jdbc-auto-commit
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public boolean getAutoCommit() throws SQLException {
        // https://www.baeldung.com/java-jdbc-auto-commit
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public void commit() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public void rollback() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public synchronized void close() throws SQLException {
        if (this.closed) {
            return;
        }
        this.closed = true;
        try {
            managedChannel.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public synchronized boolean isClosed() throws SQLException {
        return this.closed;
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        DatabaseMetaDataRequest request = DatabaseMetaDataRequest.newBuilder().build();
        return new JDBCRelationalDatabaseMetaData(this, getStub().getMetaData(request));
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public void setReadOnly(boolean readOnly) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return getMetaData().isReadOnly();
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public void setCatalog(String catalog) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public String getCatalog() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public void setTransactionIsolation(int level) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public int getTransactionIsolation() throws SQLException {
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
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public void setHoldability(int holdability) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public int getHoldability() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public Savepoint setSavepoint() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public Savepoint setSavepoint(String name) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public void rollback(Savepoint savepoint) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public Clob createClob() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public Blob createBlob() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public NClob createNClob() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public SQLXML createSQLXML() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public boolean isValid(int timeout) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        throw new SQLClientInfoException();
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        throw new SQLClientInfoException();
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public String getClientInfo(String name) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public Properties getClientInfo() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public void setSchema(String schema) throws SQLException {
        this.schema = schema;
    }

    @Override
    public String getSchema() throws SQLException {
        return this.schema;
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public void abort(Executor executor) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public int getNetworkTimeout() throws SQLException {
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
}
