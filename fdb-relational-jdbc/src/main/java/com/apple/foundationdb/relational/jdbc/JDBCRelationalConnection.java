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
import com.apple.foundationdb.relational.api.RelationalPreparedStatement;
import com.apple.foundationdb.relational.api.RelationalStatement;
import com.apple.foundationdb.relational.api.SqlTypeNamesSupport;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.jdbc.grpc.GrpcConstants;
import com.apple.foundationdb.relational.jdbc.grpc.GrpcSQLExceptionUtil;
import com.apple.foundationdb.relational.jdbc.grpc.v1.CommitRequest;
import com.apple.foundationdb.relational.jdbc.grpc.v1.DatabaseMetaDataRequest;
import com.apple.foundationdb.relational.jdbc.grpc.v1.JDBCServiceGrpc;
import com.apple.foundationdb.relational.jdbc.grpc.v1.Parameter;
import com.apple.foundationdb.relational.jdbc.grpc.v1.Parameters;
import com.apple.foundationdb.relational.jdbc.grpc.v1.RollbackRequest;
import com.apple.foundationdb.relational.jdbc.grpc.v1.StatementRequest;
import com.apple.foundationdb.relational.jdbc.grpc.v1.StatementResponse;
import com.apple.foundationdb.relational.jdbc.grpc.v1.TransactionalRequest;
import com.apple.foundationdb.relational.util.ExcludeFromJacocoGeneratedReport;
import com.apple.foundationdb.relational.util.SpotBugsSuppressWarnings;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.stub.StreamObserver;

import javax.annotation.Nonnull;
import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URI;
import java.sql.Array;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.Struct;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Connect to a Relational Database Server.
 * JDBC URL says where to connect -- host + port -- and to which database.
 * Optionally, pass schema and database options.
 * TODO: Formal specification of the JDBC Connection URL.
 */
// JDBC natively wants to do a Connection per database.
// Flipping between databases should be lightweight in Relational (there may even be cases where a transaction
// spans databases). This argues that this Connection be able to do 'switch' between databases and that when
// we do, it would be better if we didn't have to set up a whole new rpc and stub. For now we are a Database
// per Connection. TODO.
// TODO: Currently, we start up an inprocess server whenever a direct access: e.g. jdbc:relational:///__SYS. This is
// least troublesome but also expensive and in tests we may want to have the client connect to an existing
// inprocess server -- a singleton started at the head of the test for example. Let's see. For now, you can
// start a server before test and then pass the inprocess server name with 'server=SERVER_NAME' appended to connection
// URI query string to have the client connect to the existing inprocess server. E.g:
// jdbc:relational:///__SYS?schema=CATALOG&server=123e4567-e89b-12d3-a456-42661417400
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
    private final JDBCServiceGrpc.JDBCServiceStub asyncStub;
    /**
     * stream used to send transactional requests to the server.
     * Only active when autocommit is OFF
      */
    private StreamObserver<TransactionalRequest> requestSender;
    /**
     * Handler for server transactional responses.
     * Used to receive and buffer the results, synchronizing response after
     * requests have been sent.
     */
    private ServerResponseHandler serverResponses;

    /**
     * If inprocess, this will be set and needs to be called on close.
     * Its an in-process server instance started by us.
     */
    private Closeable closeable;

    @SpotBugsSuppressWarnings(value = "CT_CONSTRUCTOR_THROW", justification = "Should consider refactoring but throwing exceptions for now")
    JDBCRelationalConnection(URI uri) throws SQLException {
        this.database = uri.getPath();
        Map<String, List<String>> queryParams = JDBCURI.splitQuery(uri);
        this.schema = JDBCURI.getFirstValue("schema", queryParams);
        if (uri.getHost() != null && uri.getHost().length() > 0) {
            int port = uri.getPort();
            if (port == -1) {
                // Port is -1 if not defined in the JDBC URL.
                port = GrpcConstants.DEFAULT_SERVER_PORT;
            }
            this.managedChannel = ManagedChannelBuilder.forAddress(uri.getHost(), port).usePlaintext().build();
        } else {
            // No host specified. Presume specifying inprocess server (the way a client refers to inprocess server is to
            // leave out host and port in JDBC connection URL as in
            // jdbc:relational:///__SYS?schema=CATALOG. The server name of the inprocess server instance to connect too
            // can be on the query string with a key of 'server' and a value of server name (usually a UUID). If
            // there is no 'server' specified, we will try to start one and connect to it (It gets shutdown on
            // connection close). The latter may not work. See #startInProcessRelationalServer.
            String server = JDBCURI.getFirstValue("server", queryParams);
            if (server == null) {
                server = startInProcessRelationalServer();
            }
            this.managedChannel = InProcessChannelBuilder.forName(server).directExecutor().build();
        }
        this.blockingStub = JDBCServiceGrpc.newBlockingStub(managedChannel);
        this.asyncStub = JDBCServiceGrpc.newStub(managedChannel);

        this.setAutoCommit(false);
    }

    /**
     * To start an InProcessRelationalServer server, InProcessRelationalServer needs to be on the CLASSPATH which may not
     * be the case. If a pure JDBC driver free of fdb-relational-server and fdb-relational-core dependencies (the usual deploy
     * type), it won't be present and the below attempt at loading an inprocess server via reflection will fail.
     * @return The name of the server we started (SIDE-EFFECT; our server is saved to 'this.closeable' so that on
     * connection close, it gets cleaned up too).
     */
    private String startInProcessRelationalServer() {
        String serverName;
        try {
            Object obj = Class.forName("com.apple.foundationdb.relational.server.InProcessRelationalServer")
                    .getDeclaredConstructor().newInstance();
            // Call 'start' on our InProcessRelationalServer.
            Method start = obj.getClass().getDeclaredMethod("start");
            start.invoke(obj);
            // Make sure we get cleaned-up on close.
            this.closeable = (Closeable) obj;
            // toString on InProcessRelationalServer returns the name of the inprocess Server.
            serverName = obj.toString();
        } catch (InstantiationException e) {
            throw new IllegalArgumentException("Failed instantiation of " +
                    "com.apple.foundationdb.relational.server.InProcessRelationalServer.class", e);
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new IllegalArgumentException(e);
        } catch (NoSuchMethodException e) {
            throw new IllegalArgumentException("No such method 'start' on " +
                    "com.apple.foundationdb.relational.server.InProcessRelationalServer.class", e);
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException("No 'host' specified and " +
                    "com.apple.foundationdb.relational.server.InProcessRelationalServer.class not found", e);
        }
        return serverName;
    }

    // TODO: This needs to go away as it breaks the abstraction of the connection (and prevents it from internally managing transactions)
    JDBCServiceGrpc.JDBCServiceBlockingStub getStub() {
        return this.blockingStub;
    }

    public StatementResponse execute(String sql, com.apple.foundationdb.relational.jdbc.grpc.v1.Options options, Collection<Parameter> parameters) throws SQLException {
        try {
            StatementRequest.Builder builder = StatementRequest.newBuilder()
                    .setSql(sql)
                    .setDatabase(getDatabase()) // TODO: for transactional execution these are not required
                    .setSchema(getSchema())
                    .setOptions(options);
            if (parameters != null) {
                builder.setParameters(Parameters.newBuilder().addAllParameter(parameters).build());
            }

            if (getAutoCommit()) {
                // execute using synchronous RPC call using a single transaction
                return getStub().execute(builder.build());
            } else {
                // Use the stateful sender to send the requests as part of a transaction
                TransactionalRequest.Builder transactionRequest = TransactionalRequest.newBuilder()
                                .setExecuteRequest(builder);
                requestSender.onNext(transactionRequest.build());
                // TODO: verify the correct response type
                // Wait here until a response arrives
                return serverResponses.getResponse().getExecuteResponse();
            }
        } catch (StatusRuntimeException statusRuntimeException) {
            // TODO: If we get DQL exception we should throw and continue, right?
            // TODO: On other errors, should we close the connection?

            // Is this incoming statusRuntimeException carrying a SQLException?
            SQLException sqlException = GrpcSQLExceptionUtil.map(statusRuntimeException);
            if (sqlException == null) {
                throw statusRuntimeException;
            }
            throw sqlException;
        }
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
    public RelationalPreparedStatement prepareStatement(String sql) throws SQLException {
        return new JDBCRelationalPreparedStatement(sql, this);
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        if (autoCommit == getAutoCommit()) {
            return;
        }

        if (!autoCommit) {
            // Create handlers for requests and responses (as local state, they will stay around)
            serverResponses = new ServerResponseHandler();
            requestSender = asyncStub.handleAutoCommitOff(serverResponses);
            this.autoCommit = false;
        } else {
            if (requestSender != null) {
                // Commit any remaining requests
                commit();
                // finalize and close existing connection
                requestSender.onCompleted();
            }
            this.autoCommit = true;
            serverResponses = null;
            requestSender = null;
        }
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
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        int elementType = SqlTypeNamesSupport.getSqlTypeCode(typeName);
        final com.apple.foundationdb.relational.jdbc.grpc.v1.column.Array.Builder builder = com.apple.foundationdb.relational.jdbc.grpc.v1.column.Array.newBuilder();
        builder.setElementType(elementType);
        for (Object element: elements) {
            builder.addElement(TypeConversion.toColumn(elementType, element));
        }
        return new JDBCArrayImpl(builder.build());
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        // TODO: Implement
        return null;
    }

    @Override
    public synchronized void close() throws SQLException {
        if (this.closed) {
            return;
        }
        this.closed = true;
        try {
            if (requestSender != null) {
                // rollback uncommitted changes if closed with some remaining work
                // todo: this should probably move to the server side
                // todo: probably add "dirty" flag to avoid commit/rollback if no work pending
                rollback();
                requestSender.onCompleted();
            }
            serverResponses = null;
            requestSender = null;

            managedChannel.shutdown();
            managedChannel.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            throw new SQLException(e);
        }
        if (this.closeable != null) {
            try {
                this.closeable.close();
            } catch (IOException e) {
                throw new SQLException(e);
            }
            this.closeable = null;
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
        try {
            if (getAutoCommit()) {
                throw new SQLException("Commit cannot be called when auto commit is ON");
            } else {
                TransactionalRequest.Builder transactionRequest = TransactionalRequest.newBuilder()
                        .setCommitRequest(CommitRequest.newBuilder().build());
                requestSender.onNext(transactionRequest.build());
                // TODO: verify the correct response type
                serverResponses.getResponse();
            }
        } catch (StatusRuntimeException statusRuntimeException) {
            // TODO: Close connection on unrecoverable error?

            // Is this incoming statusRuntimeException carrying a SQLException?
            SQLException sqlException = GrpcSQLExceptionUtil.map(statusRuntimeException);
            if (sqlException == null) {
                throw statusRuntimeException;
            }
            throw sqlException;
        }
    }

    @Override
    public void rollback() throws SQLException {
        try {
            if (getAutoCommit()) {
                throw new SQLException("Rollback cannot be called when auto commit is ON");
            } else {
                TransactionalRequest.Builder transactionRequest = TransactionalRequest.newBuilder()
                        .setRollbackRequest(RollbackRequest.newBuilder().build());
                requestSender.onNext(transactionRequest.build());
                // TODO: verify the correct response type
                serverResponses.getResponse();
            }
        } catch (StatusRuntimeException statusRuntimeException) {
            // TODO: Close connection on unrecoverable error?

            // Is this incoming statusRuntimeException carrying a SQLException?
            SQLException sqlException = GrpcSQLExceptionUtil.map(statusRuntimeException);
            if (sqlException == null) {
                throw statusRuntimeException;
            }
            throw sqlException;
        }
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        // TODO: Set but not implemented. SQLLine does this on startup.
        this.transactionIsolationLevel = level;
    }
}
