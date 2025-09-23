/*
 * FRL.java
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

package com.apple.foundationdb.relational.server;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabase;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabaseFactory;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpace;
import com.apple.foundationdb.relational.api.EmbeddedRelationalDriver;
import com.apple.foundationdb.relational.api.KeySet;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.RelationalConnection;
import com.apple.foundationdb.relational.api.RelationalDriver;
import com.apple.foundationdb.relational.api.RelationalPreparedStatement;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.RelationalStatement;
import com.apple.foundationdb.relational.api.RelationalStruct;
import com.apple.foundationdb.relational.api.SqlTypeNamesSupport;
import com.apple.foundationdb.relational.api.Transaction;
import com.apple.foundationdb.relational.api.catalog.StoreCatalog;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.api.metrics.NoOpMetricRegistry;
import com.apple.foundationdb.relational.jdbc.TypeConversion;
import com.apple.foundationdb.relational.jdbc.grpc.v1.Parameter;
import com.apple.foundationdb.relational.jdbc.grpc.v1.ResultSet;
import com.apple.foundationdb.relational.recordlayer.DirectFdbConnection;
import com.apple.foundationdb.relational.recordlayer.FdbConnection;
import com.apple.foundationdb.relational.recordlayer.RecordLayerConfig;
import com.apple.foundationdb.relational.recordlayer.RecordLayerEngine;
import com.apple.foundationdb.relational.recordlayer.RelationalKeyspaceProvider;
import com.apple.foundationdb.relational.recordlayer.catalog.StoreCatalogProvider;
import com.apple.foundationdb.relational.recordlayer.ddl.RecordLayerMetadataOperationsFactory;
import com.apple.foundationdb.relational.recordlayer.query.cache.RelationalPlanCache;
import com.apple.foundationdb.relational.recordlayer.util.ExceptionUtil;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.net.URI;
import java.sql.Array;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Temporary class. "The Relational Database".
 * Facade over RelationalDatabase. Mostly copied from cli DbState but cut-down.
 * TODO: Remove having to go via embedded JDBC.... It is unnecessary overhead.
 * TODO: What can be shared in here? I want to do concurrent query handling. What is unsafe?
 * TODO: How does {@link com.apple.foundationdb.relational.api.catalog.RelationalDatabase} relate?
 * TODO: Let this be front door to the relational DB used by Server? Hide the driver
 * and connection stuff behind here?
 */
// Needs to be public so can be used by sub-packages; i.e. the JDBCService
@API(API.Status.EXPERIMENTAL)
public class FRL implements AutoCloseable {
    private final FdbConnection fdbDatabase;
    private final RelationalDriver registeredDriver;
    private boolean registeredJDBCEmbedDriver;

    public FRL() throws RelationalException {
        this(Options.NONE, null);
    }

    public FRL(@Nonnull Options options) throws RelationalException {
        this(options, null);
    }

    public FRL(@Nonnull Options options, @Nullable String clusterFile) throws RelationalException {
        final FDBDatabase fdbDb = FDBDatabaseFactory.instance().getDatabase(clusterFile);
        final Long asyncToSyncTimeout = options.getOption(Options.Name.ASYNC_OPERATIONS_TIMEOUT_MILLIS);
        if (asyncToSyncTimeout > 0) {
            fdbDb.setAsyncToSyncTimeout(asyncToSyncTimeout, TimeUnit.MILLISECONDS);
        }
        this.fdbDatabase = new DirectFdbConnection(fdbDb, NoOpMetricRegistry.INSTANCE);

        final RelationalKeyspaceProvider keyspaceProvider = RelationalKeyspaceProvider.instance();
        keyspaceProvider.registerDomainIfNotExists("FRL");
        KeySpace keySpace = keyspaceProvider.getKeySpace();
        StoreCatalog storeCatalog;
        try (Transaction txn = fdbDatabase.getTransactionManager().createTransaction(Options.NONE)) {
            storeCatalog = StoreCatalogProvider.getCatalog(txn, keySpace);
            txn.commit();
        }

        RecordLayerConfig rlConfig = RecordLayerConfig.getDefault();
        RecordLayerMetadataOperationsFactory ddlFactory = new RecordLayerMetadataOperationsFactory.Builder()
                .setRlConfig(rlConfig)
                .setBaseKeySpace(keySpace)
                .setStoreCatalog(storeCatalog).build();

        try {
            this.registeredDriver = new EmbeddedRelationalDriver(RecordLayerEngine.makeEngine(
                    rlConfig,
                    Collections.singletonList(fdbDb),
                    keySpace,
                    storeCatalog,
                    null,
                    ddlFactory,
                    RelationalPlanCache.newRelationalCacheBuilder()
                            .setTtl(options.getOption(Options.Name.PLAN_CACHE_PRIMARY_TIME_TO_LIVE_MILLIS))
                            .setSize(options.getOption(Options.Name.PLAN_CACHE_PRIMARY_MAX_ENTRIES))
                            .setSecondaryTtl(options.getOption(Options.Name.PLAN_CACHE_SECONDARY_TIME_TO_LIVE_MILLIS))
                            .setSecondarySize(options.getOption(Options.Name.PLAN_CACHE_SECONDARY_MAX_ENTRIES))
                            .setTertiaryTtl(options.getOption(Options.Name.PLAN_CACHE_TERTIARY_TIME_TO_LIVE_MILLIS))
                            .setTertiarySize(options.getOption(Options.Name.PLAN_CACHE_TERTIARY_MAX_ENTRIES))
                            .build()));

            DriverManager.registerDriver(this.registeredDriver);
            this.registeredJDBCEmbedDriver = true;
        } catch (SQLException ve) {
            throw new RelationalException(ve);
        }
    }

    @SuppressWarnings("AbbreviationAsWordInName") // allow JDBCURI, though perhaps we should update this to make it clearer
    private static String createEmbeddedJDBCURI(String database, String schema)  {
        return EmbeddedRelationalDriver.JDBC_URL_PREFIX + database + (schema != null ? "?schema=" + schema : "");
    }

    public static final class Response {
        private final Optional<ResultSet> resultSet;
        //will be -1 if the query is set
        private final int rowCount;

        private Response(@Nullable ResultSet resultSet, int rowCount) {
            this.resultSet = Optional.ofNullable(resultSet);
            this.rowCount = rowCount;
        }

        public static Response query(@Nonnull ResultSet resultSet) {
            return new Response(resultSet, -1);
        }

        public static Response mutation(int rowCount) {
            return new Response(null, rowCount);
        }

        public boolean isQuery() {
            return resultSet.isPresent();
        }

        public boolean isMutation() {
            return resultSet.isEmpty();
        }

        @SuppressWarnings("OptionalGetWithoutIsPresent") //intentional
        public ResultSet getResultSet() {
            return resultSet.get();
        }

        public int getRowCount() {
            return rowCount;
        }
    }

    /**
     * Execute <code>sql</code>.
     * @param database Database to run the <code>sql</code> against.
     * @param schema Schema to use on <code>database</code>
     * @param sql SQL to execute.
     * @param parameters If non-null, then these are parameters and 'sql' is text of a prepared statement.
     * @return Returns A Response with either a ResultSet or a Row count, depending on the type of query issued
     * @throws SQLException For all sorts of reasons.
     */
    @Nonnull
    public Response execute(String database, String schema, String sql, List<Parameter> parameters, Options options)
            throws SQLException {
        // Down inside connect, it calls RecordLayerStorageCluster.loadDatabase which internally creates a Transaction
        // to RecordLayerStorageCluster.loadDatabase and which, internal to loadDatabase, it then closes.
        // We used to explicitly set schema up here but this was provoking a new, separate, transaction; just let
        // embedded JDBC driver do its thing on connect with database and schema.
        // Third transaction is then created to run the sql. Transaction closes when connection closes so do all our
        // work inside here including reading all out of the ResultSet while under transaction else callers who try
        // to read the ResultSet after the transaction has closed will get a 'transactions is not active'.
        try (var connection = connect(database, schema, options)) {
            // Options are given to the connection, don't override them in the statement
            return executeInternal(connection, sql, parameters, null);
        }
    }

    private RelationalConnection connect(String database, String schema, Options options) throws SQLException {
        final var driver = (RelationalDriver) DriverManager.getDriver(createEmbeddedJDBCURI(database, schema));
        return driver.connect(URI.create(createEmbeddedJDBCURI(database, schema)), options);
    }

    private Response executeInternal(@Nonnull RelationalConnection connection,
                                     @Nonnull String sql,
                                     @Nullable List<Parameter> parameters,
                                     @Nullable Options options) throws SQLException {
        ResultSet resultSet;
        if (parameters == null) {
            try (Statement statement = connection.createStatement()) {
                try (RelationalStatement relationalStatement = statement.unwrap(RelationalStatement.class)) {
                    setStatementOptions(options, statement);
                    if (relationalStatement.execute(sql)) {
                        try (RelationalResultSet rs = relationalStatement.getResultSet()) {
                            resultSet = TypeConversion.toProtobuf(rs);
                            return Response.query(resultSet);
                        }
                    } else {
                        return Response.mutation(relationalStatement.getUpdateCount());
                    }
                }
            }
        }
        // If parameters, it's a prepared statement.
        try (RelationalPreparedStatement statement = connection.prepareStatement(sql)) {
            int index = 1; // Parameter position is one-based.
            for (Parameter parameter : parameters) {
                addPreparedStatementParameter(statement, parameter, index++);
            }
            setStatementOptions(options, statement);
            if (statement.execute()) {
                try (RelationalResultSet rs = statement.getResultSet()) {
                    resultSet = TypeConversion.toProtobuf(rs);
                    return Response.query(resultSet);
                }
            } else {
                return Response.mutation(statement.getUpdateCount());
            }
        }
    }

    private static void setStatementOptions(final @Nullable Options options, final Statement statement) throws SQLException {
        if (options != null) {
            statement.setMaxRows(options.getOption(Options.Name.MAX_ROWS));
        }
    }

    private static void addPreparedStatementParameter(@Nonnull RelationalPreparedStatement relationalPreparedStatement,
                                                      @Nonnull Parameter parameter, int index) throws SQLException {
        final var oneOfValue = parameter.getParameter();
        if (oneOfValue.hasString()) {
            relationalPreparedStatement.setString(index, oneOfValue.getString());
        } else if (oneOfValue.hasLong()) {
            relationalPreparedStatement.setLong(index, oneOfValue.getLong());
        } else if (oneOfValue.hasInteger()) {
            relationalPreparedStatement.setInt(index, oneOfValue.getInteger());
        } else if (oneOfValue.hasFloat()) {
            relationalPreparedStatement.setFloat(index, oneOfValue.getFloat());
        } else if (oneOfValue.hasDouble()) {
            relationalPreparedStatement.setDouble(index, oneOfValue.getDouble());
        } else if (oneOfValue.hasBoolean()) {
            relationalPreparedStatement.setBoolean(index, oneOfValue.getBoolean());
        } else if (oneOfValue.hasBinary()) {
            relationalPreparedStatement.setBytes(index, oneOfValue.getBinary().toByteArray());
        } else if (oneOfValue.hasNullType()) {
            relationalPreparedStatement.setNull(index, oneOfValue.getNullType());
        } else if (oneOfValue.hasUuid()) {
            relationalPreparedStatement.setUUID(index, new UUID(oneOfValue.getUuid().getMostSignificantBits(), oneOfValue.getUuid().getLeastSignificantBits()));
        } else if (oneOfValue.hasArray()) {
            final com.apple.foundationdb.relational.jdbc.grpc.v1.column.Array arrayProto = parameter.getParameter().getArray();
            final Array relationalArray = relationalPreparedStatement.getConnection().createArrayOf(
                    SqlTypeNamesSupport.getSqlTypeName(arrayProto.getElementType()),
                    TypeConversion.fromArray(arrayProto));
            relationalPreparedStatement.setArray(index, relationalArray);
        } else {
            throw new SQLException("Unsupported value: " + parameter.getParameter());
        }
    }

    public int update(String database, String schema, String sql, Options options) throws SQLException {
        try (var connection = connect(database, schema, options)) {
            try (Statement statement = connection.createStatement()) {
                return statement.executeUpdate(sql);
            }
        }
    }

    public int insert(String database, String schema, String tableName, List<RelationalStruct> data, Options options)
            throws SQLException {
        try (var connection = connect(database, schema, options)) {
            try (Statement statement = connection.createStatement()) {
                try (RelationalStatement relationalStatement = statement.unwrap(RelationalStatement.class)) {
                    return relationalStatement.executeInsert(tableName, data, Options.NONE);
                }
            }
        }
    }

    public RelationalResultSet get(String database, String schema, String tableName, KeySet keySet, Options options)
            throws SQLException {
        try (var connection = connect(database, schema, options)) {
            try (Statement statement = connection.createStatement()) {
                try (RelationalStatement relationalStatement = statement.unwrap(RelationalStatement.class)) {
                    return relationalStatement.executeGet(tableName, keySet, Options.NONE);
                }
            }
        }
    }

    public RelationalResultSet scan(String database, String schema, String tableName, KeySet keySet, Options options)
            throws SQLException {
        try (var connection = connect(database, schema, options)) {
            try (Statement statement = connection.createStatement()) {
                try (RelationalStatement relationalStatement = statement.unwrap(RelationalStatement.class)) {
                    return relationalStatement.executeScan(tableName, keySet, Options.NONE);
                }
            }
        }
    }

    public TransactionalToken createTransactionalToken(String database, String schema, Options options) throws SQLException {
        final var driver = (RelationalDriver) DriverManager.getDriver(createEmbeddedJDBCURI(database, schema));
        RelationalConnection transactionalConnection = driver.connect(URI.create(createEmbeddedJDBCURI(database, schema)), options);
        transactionalConnection.setAutoCommit(false);
        return new TransactionalToken(transactionalConnection);
    }

    @Nonnull
    public Response transactionalExecute(TransactionalToken token, String sql, List<Parameter> parameters, @Nullable Options options)
            throws SQLException {
        assertValidToken(token);
        return executeInternal(token.getConnection(), sql, parameters, options);
    }

    public int transactionalInsert(TransactionalToken token, String tableName, List<RelationalStruct> data)
            throws SQLException {
        assertValidToken(token);
        try (Statement statement = token.getConnection().createStatement()) {
            try (RelationalStatement relationalStatement = statement.unwrap(RelationalStatement.class)) {
                return relationalStatement.executeInsert(tableName, data, Options.NONE);
            }
        }
    }

    public void transactionalCommit(TransactionalToken token) throws SQLException {
        assertValidToken(token);
        token.getConnection().commit();
    }

    public void transactionalRollback(TransactionalToken token) throws SQLException {
        assertValidToken(token);
        token.getConnection().rollback();
    }

    public void enableAutoCommit(TransactionalToken token) throws SQLException {
        assertValidToken(token);
        token.getConnection().setAutoCommit(true);
    }

    public void transactionalClose(TransactionalToken token) throws SQLException {
        if (token != null && !token.expired()) {
            token.close();
        }

    }

    private void assertValidToken(TransactionalToken token) throws SQLException {
        if (token == null) {
            // TODO: non SQLException exception?
            throw new SQLException("Transaction was not initialized");
        }
        if (token.expired()) {
            throw new SQLException("Transaction had expired");
        }
    }

    @Override
    public void close() throws SQLException, RelationalException {
        try {
            fdbDatabase.close();
        } catch (RecordCoreException ex) {
            throw ExceptionUtil.toRelationalException(ex);
        }
        // We registered the Relational embed driver... cleanup.
        if (this.registeredJDBCEmbedDriver) {
            DriverManager.deregisterDriver(registeredDriver);
        }
    }
}
