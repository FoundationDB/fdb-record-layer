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
import com.apple.foundationdb.record.provider.common.DynamicMessageRecordSerializer;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabase;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabaseFactory;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpace;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpaceDirectory;
import com.apple.foundationdb.relational.api.DynamicMessageBuilder;
import com.apple.foundationdb.relational.api.EmbeddedRelationalEngine;
import com.apple.foundationdb.relational.api.KeySet;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.Transaction;
import com.apple.foundationdb.relational.api.Relational;
import com.apple.foundationdb.relational.api.RelationalConnection;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.RelationalStatement;
import com.apple.foundationdb.relational.api.RelationalStruct;
import com.apple.foundationdb.relational.api.catalog.InMemorySchemaTemplateCatalog;
import com.apple.foundationdb.relational.api.catalog.SchemaTemplateCatalog;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.api.metrics.NoOpMetricRegistry;
import com.apple.foundationdb.relational.recordlayer.DirectFdbConnection;
import com.apple.foundationdb.relational.recordlayer.FdbConnection;
import com.apple.foundationdb.relational.recordlayer.RecordLayerConfig;
import com.apple.foundationdb.relational.recordlayer.RecordLayerEngine;
import com.apple.foundationdb.relational.recordlayer.catalog.RecordLayerStoreCatalogImpl;
import com.apple.foundationdb.relational.recordlayer.ddl.RecordLayerMetadataOperationsFactory;
import com.apple.foundationdb.relational.recordlayer.util.ExceptionUtil;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.Parser;

import javax.annotation.Nullable;
import java.net.URI;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

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

        KeySpace keySpace = getKeySpaceForSetup();
        RecordLayerConfig rlConfig = new RecordLayerConfig(
                (oldUserVersion, oldMetaDataVersion, metaData) -> CompletableFuture.completedFuture(oldUserVersion),
                storePath -> DynamicMessageRecordSerializer.instance(),
                1);
        SchemaTemplateCatalog templateCatalog = new InMemorySchemaTemplateCatalog();
        RecordLayerStoreCatalogImpl catalog = new RecordLayerStoreCatalogImpl(keySpace, templateCatalog);
        try (Transaction txn = fdbDatabase.getTransactionManager().createTransaction(Options.NONE)) {
            catalog.initialize(txn);
            txn.commit();
        }

        RecordLayerStoreCatalogImpl schemaCatalog = new RecordLayerStoreCatalogImpl(keySpace, templateCatalog);
        RecordLayerMetadataOperationsFactory ddlFactory = new RecordLayerMetadataOperationsFactory.Builder()
                .setRlConfig(rlConfig)
                .setBaseKeySpace(keySpace)
                .setTemplateCatalog(templateCatalog)
                .setStoreCatalog(schemaCatalog).build();

        this.engine = RecordLayerEngine.makeEngine(
                rlConfig,
                Collections.singletonList(fdbDb),
                keySpace,
                schemaCatalog,
                templateCatalog,
                null,
                ddlFactory);

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

    private KeySpace getKeySpaceForSetup() {
        KeySpaceDirectory dbDirectory = new KeySpaceDirectory("dbid", KeySpaceDirectory.KeyType.STRING);
        dbDirectory.addSubdirectory(new KeySpaceDirectory("schema", KeySpaceDirectory.KeyType.STRING));
        KeySpaceDirectory catalogDir = new KeySpaceDirectory("CATALOG", KeySpaceDirectory.KeyType.NULL);
        return new KeySpace(dbDirectory, catalogDir);
    }

    @Nullable
    public RelationalResultSet execute(String database, String schema, String sql) throws SQLException {
        try (RelationalConnection connection = Relational.connect(URI.create(JDBC_EMBED_PREFIX + database), Options.NONE)) {
            connection.setSchema(schema);
            try (Statement statement = connection.createStatement()) {
                try (RelationalStatement relationalStatement = statement.unwrap(RelationalStatement.class)) {
                    return relationalStatement.execute(sql) ? relationalStatement.getResultSet() : null;
                }
            }
        } catch (RelationalException e) {
            throw e.toSqlException();
        }
    }

    public int update(String database, String schema, String sql) throws SQLException {
        try (RelationalConnection connection = Relational.connect(URI.create(JDBC_EMBED_PREFIX + database), Options.NONE)) {
            connection.setSchema(schema);
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
        try (RelationalConnection connection = Relational.connect(URI.create(JDBC_EMBED_PREFIX + database), Options.NONE)) {
            connection.setSchema(schema);
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
        try (RelationalConnection connection = Relational.connect(URI.create(JDBC_EMBED_PREFIX + database), Options.NONE)) {
            connection.setSchema(schema);
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
        try (RelationalConnection connection = Relational.connect(URI.create(JDBC_EMBED_PREFIX + database), Options.NONE)) {
            connection.setSchema(schema);
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
        try (RelationalConnection connection = Relational.connect(URI.create(JDBC_EMBED_PREFIX + database), Options.NONE)) {
            connection.setSchema(schema);
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
