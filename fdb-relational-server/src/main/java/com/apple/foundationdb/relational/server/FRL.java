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

import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabase;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabaseFactory;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpace;
import com.apple.foundationdb.relational.api.DynamicMessageBuilder;
import com.apple.foundationdb.relational.api.EmbeddedRelationalEngine;
import com.apple.foundationdb.relational.api.KeySet;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.Transaction;
import com.apple.foundationdb.relational.api.Relational;
import com.apple.foundationdb.relational.api.RelationalConnection;
import com.apple.foundationdb.relational.api.RelationalPreparedStatement;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.RelationalStatement;
import com.apple.foundationdb.relational.api.RelationalStruct;
import com.apple.foundationdb.relational.api.catalog.StoreCatalog;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
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
import com.apple.foundationdb.relational.recordlayer.query.QueryLogger;
import com.apple.foundationdb.relational.recordlayer.query.cache.RelationalPlanCache;
import com.apple.foundationdb.relational.recordlayer.util.ExceptionUtil;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.Parser;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.net.URI;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

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
public class FRL implements AutoCloseable {
    private final FdbConnection fdbDatabase;
    private final EmbeddedRelationalEngine engine;
    private static final String JDBC_EMBED_PREFIX = "jdbc:embed:";
    private boolean registeredJDBCEmbedDriver;

    FRL() throws RelationalException {
        final FDBDatabase fdbDb = FDBDatabaseFactory.instance().getDatabase();
        this.fdbDatabase = new DirectFdbConnection(fdbDb, NoOpMetricRegistry.INSTANCE);

        RelationalKeyspaceProvider.registerDomainIfNotExists("FRL");
        KeySpace keySpace = RelationalKeyspaceProvider.getKeySpace();
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

        //TODO(bfines) configuration here
        QueryLogger.configure(Options.NONE);
        this.engine = RecordLayerEngine.makeEngine(
                rlConfig,
                Collections.singletonList(fdbDb),
                keySpace,
                storeCatalog,
                null,
                ddlFactory,
                RelationalPlanCache.buildWithDefaults());

        // Throws ErrorCode.PROTOCOL_VIOLATION if driver already registered.
        // TODO: Clean up driver registration/get registered driver. Should it register w/ DriverManager?
        try {
            this.engine.registerDriver();
            this.registeredJDBCEmbedDriver = true;
        } catch (RelationalException ve) {
            if (!ve.getErrorCode().equals(ErrorCode.PROTOCOL_VIOLATION)) {
                throw ve;
            }
        }
    }

    private static URI createEmbeddedJDBCURI(String database, String schema)  {
        return URI.create(JDBC_EMBED_PREFIX + database + (schema != null ? "?schema=" + schema : ""));
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
    public Response execute(String database, String schema, String sql, List<Parameter> parameters)
            throws SQLException {
        // Down inside connect, it calls RecordLayerStorageCluster.loadDatabase which internally creates a Transaction
        // to RecordLayerStorageCluster.loadDatabase and which, internal to loadDatabase, it then closes.
        // We used to explicitly set schema up here but this was provoking a new, separate, transaction; just let
        // embedded JDBC driver do its thing on connect with database and schema.
        // Third transaction is then created to run the sql. Transaction closes when connection closes so do all our
        // work inside here including reading all out of the ResultSet while under transaction else callers who try
        // to read the ResultSet after the transaction has closed will get a 'transactions is not active'.
        // TODO: Transaction handling.
        try (RelationalConnection connection = Relational.connect(createEmbeddedJDBCURI(database, schema), Options.NONE)) {
            ResultSet resultSet = null;
            if (parameters != null) {
                // If parameters, it's a prepared statement.
                try (PreparedStatement statement = connection.prepareStatement(sql)) {
                    try (RelationalPreparedStatement relationalStatement =
                            statement.unwrap(RelationalPreparedStatement.class)) {
                        int index = 1; // Parameter position is one-based.
                        for (Parameter parameter : parameters) {
                            addPreparedStatementParameter(relationalStatement, parameter, index++);
                        }
                        if (relationalStatement.execute()) {
                            try (RelationalResultSet rs = relationalStatement.getResultSet()) {
                                resultSet = TypeConversion.toProtobuf(rs);
                                return Response.query(resultSet);
                            }
                        } else {
                            return Response.mutation(relationalStatement.getUpdateCount());
                        }
                    }
                }
            } else {
                try (Statement statement = connection.createStatement()) {
                    try (RelationalStatement relationalStatement = statement.unwrap(RelationalStatement.class)) {
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
        } catch (RelationalException e) {
            throw e.toSqlException();
        }
    }

    private static void addPreparedStatementParameter(RelationalPreparedStatement relationalPreparedStatement,
                                                      Parameter parameter, int index) throws SQLException {
        int type = parameter.getJavaSqlTypesCode();
        switch (type) {
            case Types.VARCHAR:
                relationalPreparedStatement.setString(index, parameter.getParameter().getString());
                break;
            case Types.BIGINT:
                relationalPreparedStatement.setInt(index, parameter.getParameter().getInteger());
                break;
            case Types.INTEGER:
                relationalPreparedStatement.setInt(index, parameter.getParameter().getInteger());
                break;
            case Types.DOUBLE:
                relationalPreparedStatement.setDouble(index, parameter.getParameter().getDouble());
                break;
            case Types.BOOLEAN:
                relationalPreparedStatement.setBoolean(index, parameter.getParameter().getBoolean());
                break;
            case Types.BINARY:
                relationalPreparedStatement.setBytes(index, parameter.getParameter().getBinary().toByteArray());
                break;
            default:
                throw new SQLException("Unsupported type " + type);
        }
    }

    public int update(String database, String schema, String sql) throws SQLException {
        try (RelationalConnection connection = Relational.connect(createEmbeddedJDBCURI(database, schema), Options.NONE)) {
            try (Statement statement = connection.createStatement()) {
                try (RelationalStatement relationalStatement = statement.unwrap(RelationalStatement.class)) {
                    return relationalStatement.executeUpdate(sql);
                }
            }
        } catch (RelationalException ve) {
            throw ve.toSqlException();
        }
    }

    /**
     * Insert.
     * @param data List of ByteStrings with serialized Messages to insert.
     * @param database Which database to insert into.
     * @param schema Which schema to use.
     * @param tableName Table to insert into.
     * @return Count of rows inserted.
     * @throws SQLException If error inserting.
     * @deprecated Since 01/24/2023. Replaced by {@link #insert(String, String, String, List)}.
     */
    // Order of parameters here is intentionally different from insert List<RelationalStruct> to avoid clash of
    // List erasure (ByteString vs RelationalStruct).
    @Deprecated
    public int insert(List<ByteString> data, String database, String schema, String tableName)
            throws SQLException {
        try (RelationalConnection connection = Relational.connect(createEmbeddedJDBCURI(database, schema), Options.NONE)) {
            try (Statement statement = connection.createStatement()) {
                try (RelationalStatement relationalStatement = statement.unwrap(RelationalStatement.class)) {
                    // Get a parser to use deserializing ByteStrings.
                    DynamicMessageBuilder dynamicMessageBuilder = relationalStatement.getDataBuilder(tableName);
                    Parser<? extends Message> parser = dynamicMessageBuilder.build().getParserForType();
                    List<Message> messages = new ArrayList<>();
                    for (ByteString bytes : data) {
                        messages.add(parser.parseFrom(bytes));
                    }
                    return relationalStatement.executeInsert(tableName, messages.iterator(), Options.NONE);
                } catch (InvalidProtocolBufferException e) {
                    throw new RuntimeException(e);
                }
            }
        } catch (RelationalException ve) {
            throw ve.toSqlException();
        }
    }

    public int insert(String database, String schema, String tableName, List<RelationalStruct> data)
            throws SQLException {
        try (RelationalConnection connection = Relational.connect(createEmbeddedJDBCURI(database, schema), Options.NONE)) {
            try (Statement statement = connection.createStatement()) {
                try (RelationalStatement relationalStatement = statement.unwrap(RelationalStatement.class)) {
                    return relationalStatement.executeInsert(tableName, data, Options.NONE);
                }
            }
        } catch (RelationalException ve) {
            throw ve.toSqlException();
        }
    }

    public RelationalResultSet get(String database, String schema, String tableName, KeySet keySet)
            throws SQLException {
        try (RelationalConnection connection = Relational.connect(createEmbeddedJDBCURI(database, schema), Options.NONE)) {
            try (Statement statement = connection.createStatement()) {
                try (RelationalStatement relationalStatement = statement.unwrap(RelationalStatement.class)) {
                    return relationalStatement.executeGet(tableName, keySet, Options.NONE);
                }
            }
        } catch (RelationalException ve) {
            throw ve.toSqlException();
        }
    }

    public RelationalResultSet scan(String database, String schema, String tableName, KeySet keySet)
            throws SQLException {
        try (RelationalConnection connection = Relational.connect(createEmbeddedJDBCURI(database, schema), Options.NONE)) {
            try (Statement statement = connection.createStatement()) {
                try (RelationalStatement relationalStatement = statement.unwrap(RelationalStatement.class)) {
                    return relationalStatement.executeScan(tableName, keySet, Options.NONE);
                }
            }
        } catch (RelationalException ve) {
            throw ve.toSqlException();
        }
    }

    @Override
    public void close() throws Exception {
        try {
            fdbDatabase.close();
        } catch (RecordCoreException ex) {
            throw ExceptionUtil.toRelationalException(ex);
        }
        // We registered the Relational embed driver... cleanup.
        if (this.registeredJDBCEmbedDriver) {
            this.engine.deregisterDriver();
        }
    }
}
