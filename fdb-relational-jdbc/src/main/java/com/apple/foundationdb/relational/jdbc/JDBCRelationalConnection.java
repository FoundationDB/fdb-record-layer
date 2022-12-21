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

import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.RelationalConnection;
import com.apple.foundationdb.relational.api.RelationalStatement;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.grpc.GrpcConstants;
import com.apple.foundationdb.relational.grpc.jdbc.v1.DatabaseMetaDataRequest;
import com.apple.foundationdb.relational.grpc.jdbc.v1.JDBCServiceGrpc;
import com.apple.foundationdb.relational.util.ExcludeFromJacocoGeneratedReport;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import javax.annotation.Nonnull;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
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
class JDBCRelationalConnection implements RelationalConnection {
    /**
     * This is a lie for now. TODO: Fix.
     * Choosing dbeaver default for now until properly implemented.
     * See https://docs.oracle.com/javadb/10.8.3.0/devguide/cdevconcepts15366.html
     */
    private volatile int transactionIsolationLevel = Connection.TRANSACTION_REPEATABLE_READ;
    /**
     * TODO: implement.
     */
    private volatile boolean readOnly;
    /**
     * TODO: implement.
     */
    private volatile boolean autoCommit = true;
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

    /**
     * Create a {@link RelationalStatement}.
     * @return A RelationalStatement instead of a plain JDBC Statement (Here we deviate form pure JDBC for the
     * user's convenience; users will be interested in the Relational facility and will be annoyed having to go through
     * extra steps to extract Relational types from base JDBC).
     * @throws SQLException Exception on failed create.
     */
    @Override
    public RelationalStatement createStatement() throws SQLException {
        return new JDBCRelationalStatement(this);
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        // https://www.baeldung.com/java-jdbc-auto-commit
        throw new SQLException("Not implemented " + Thread.currentThread() .getStackTrace()[1] .getMethodName());
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        return this.autoCommit;
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        this.readOnly = readOnly;
    }
 
    @Override
    public boolean isReadOnly() throws SQLException {
        return this.readOnly;
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        return this.transactionIsolationLevel;
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        // TODO: For now just return null
        return null;
    }
 
    @Override
    public void clearWarnings() throws SQLException {
        // TODO: Implement
    }

    @Override
    public synchronized void close() throws SQLException {
        if (this.closed) {
            return;
        }
        this.closed = true;
        try {
            managedChannel.shutdown();
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
    public void beginTransaction() throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Nonnull
    @Override
    public Options getOptions() {
        return Options.NONE;
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public void setOption(Options.Name name, Object value) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public URI getPath() {
        return null;
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
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return iface.cast(this);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isInstance(this);
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public void commit() throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented",
                ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    public void rollback() throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented",
                ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }
}
