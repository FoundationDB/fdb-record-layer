/*
 * RelationalJDBCConnection.java
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

import com.apple.foundationdb.relational.grpc.jdbc.v1.DatabaseMetaDataRequest;
import com.apple.foundationdb.relational.grpc.jdbc.v1.JDBCServiceGrpc;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.apple.relational.grpc.GrpcConstants;

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
 * Delegate for the DatabaseMetaData protobuf message.
 */
class RelationalJDBCConnection implements Connection {
    private volatile boolean closed;
    private final ManagedChannel managedChannel;
    private final JDBCServiceGrpc.JDBCServiceBlockingStub blockingStub;

    RelationalJDBCConnection(String url, Properties info) {
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
        // TODO: RESTORE: this.properties => (info == null) ? new Properties() : info;
        // Meantime do the below nonesense to get rid of the PMD warning...; i.e. use the passed
        // 'info' param.
        if (info != null) {
            info.elements();
        }

        String hostname = uri.getHost();
        int port = uri.getPort();
        if (port == -1) {
            // Port is -1 if not defined in the JDBC URL.
            port = GrpcConstants.DEFAULT_SERVER_PORT;
        }
        this.managedChannel = ManagedChannelBuilder
                .forAddress(hostname, port)
                .usePlaintext().build();
        this.blockingStub = JDBCServiceGrpc.newBlockingStub(managedChannel);
    }

    @Override
    public Statement createStatement() throws SQLException {
        return new RelationalJDBCStatement(this);
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public String nativeSQL(String sql) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        // https://www.baeldung.com/java-jdbc-auto-commit
        throw new SQLException("Not implemented");
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        // https://www.baeldung.com/java-jdbc-auto-commit
        throw new SQLException("Not implemented");
    }

    @Override
    public void commit() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
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
        return new RelationalDatabaseMetaData(this.blockingStub.getMetaData(request));
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return getMetaData().isReadOnly();
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public String getCatalog() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public void clearWarnings() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public void setHoldability(int holdability) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public int getHoldability() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public Clob createClob() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public Blob createBlob() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public NClob createNClob() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        throw new SQLClientInfoException();
    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        throw new SQLClientInfoException();
    }

    @Override
    public String getClientInfo(String name) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public void setSchema(String schema) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public String getSchema() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public void abort(Executor executor) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw new SQLException("Not implemented");
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        throw new SQLException("Not implemented");
    }
}
