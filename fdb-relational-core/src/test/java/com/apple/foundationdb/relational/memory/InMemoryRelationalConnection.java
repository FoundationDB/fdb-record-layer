/*
 * InMemoryRelationalConnection.java
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

package com.apple.foundationdb.relational.memory;

import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.relational.api.Continuation;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.RelationalConnection;
import com.apple.foundationdb.relational.api.RelationalDatabaseMetaData;
import com.apple.foundationdb.relational.api.RelationalPreparedStatement;
import com.apple.foundationdb.relational.api.RelationalStatement;
import com.apple.foundationdb.relational.api.ddl.ConstantAction;
import com.apple.foundationdb.relational.api.ddl.DdlQueryFactory;
import com.apple.foundationdb.relational.api.ddl.MetadataOperationsFactory;
import com.apple.foundationdb.relational.api.ddl.NoOpQueryFactory;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.api.metadata.Schema;
import com.apple.foundationdb.relational.api.metadata.SchemaTemplate;
import com.apple.foundationdb.relational.recordlayer.RecordLayerConfig;
import com.apple.foundationdb.relational.recordlayer.RelationalKeyspaceProvider;
import com.apple.foundationdb.relational.recordlayer.catalog.systables.SystemTableRegistry;
import com.apple.foundationdb.relational.recordlayer.ddl.RecordLayerMetadataOperationsFactory;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerSchemaTemplate;

import javax.annotation.Nonnull;
import java.net.URI;
import java.sql.SQLException;

public class InMemoryRelationalConnection implements RelationalConnection {
    final InMemoryCatalog catalog;

    private final URI databaseUri;
    private final RecordMetaData recordMetaData;

    public InMemoryRelationalConnection(InMemoryCatalog catalog, URI databaseUri) throws RelationalException {
        this.databaseUri = databaseUri;
        this.catalog = catalog;
        this.recordMetaData = createRecordMetaData();
    }

    @Override
    public RelationalStatement createStatement() throws SQLException {
        return new InMemoryRelationalStatement(this);
    }

    @Override
    public RelationalPreparedStatement prepareStatement(String sql) throws SQLException {
        throw new RelationalException("InMemoryRelationalConnection does not support prepared statements", ErrorCode.UNSUPPORTED_OPERATION).toSqlException();
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {

    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        return false;
    }

    @Override
    public void commit() throws SQLException {

    }

    @Override
    public void rollback() throws SQLException {

    }

    @Override
    public void close() throws SQLException {

    }

    @Override
    public boolean isClosed() throws SQLException {
        return false;
    }

    @Nonnull
    @Override
    public RelationalDatabaseMetaData getMetaData() throws SQLException {
        throw new RelationalException("InMemoryRelationalConnection does not support getMetaData", ErrorCode.UNSUPPORTED_OPERATION).toSqlException();
    }

    @Override
    public void setSchema(String schema) throws SQLException {

    }

    @Override
    public String getSchema() throws SQLException {
        return null;
    }

    @Override
    public void beginTransaction() throws SQLException {

    }

    @Override
    @Nonnull
    public Options getOptions() {
        return Options.NONE;
    }

    public void setOption(Options.Name name, Object value) {
    }

    @Override
    public URI getPath() {
        return null;
    }

    InMemoryTable loadTable(String tableName) throws RelationalException {
        String[] schemaAndTable = tableName.split("\\.");
        String schema;
        String table;
        try {
            if (schemaAndTable.length == 1) {
                schema = this.getSchema();
                table = schemaAndTable[0];
            } else {
                schema = schemaAndTable[0];
                table = schemaAndTable[1];
            }
        } catch (SQLException se) {
            throw new RelationalException(se);
        }

        if (schema == null) {
            throw new RelationalException("Unknown Schema ", ErrorCode.UNDEFINED_SCHEMA);
        }
        return catalog.loadTable(databaseUri, schema, table);
    }

    public MetadataOperationsFactory getConstantActionFactory() {
        RecordLayerConfig rlCfg = RecordLayerConfig.getDefault();
        return new RecordLayerMetadataOperationsFactory(rlCfg, catalog, RelationalKeyspaceProvider.getKeySpace()) {
            @Nonnull
            @Override
            public ConstantAction getCreateSchemaConstantAction(@Nonnull URI dbUri, @Nonnull String schemaName, @Nonnull String templateId, Options constantActionOptions) {
                return txn -> {
                    final SchemaTemplate schemaTemplate = catalog.getSchemaTemplateCatalog().loadSchemaTemplate(txn, templateId);

                    //map the schema to the template
                    Schema schema = schemaTemplate.generateSchema(dbUri.getPath(), schemaName);

                    //insert the schema into the catalog
                    catalog.saveSchema(txn, schema);
                };
            }

            @Nonnull
            @Override
            public ConstantAction getCreateDatabaseConstantAction(@Nonnull URI dbPath, @Nonnull Options constantActionOptions) {
                return super.getCreateDatabaseConstantAction(dbPath, constantActionOptions);
            }

            @Nonnull
            @Override
            public ConstantAction getDropDatabaseConstantAction(@Nonnull URI dbUrl, @Nonnull Options options) {
                return txn -> catalog.deleteDatabase(txn, dbUrl, Continuation.BEGIN);
            }
        };
    }

    public DdlQueryFactory getDdlQueryFactory() {
        return NoOpQueryFactory.INSTANCE;
    }

    public URI getDatabaseUri() {
        return databaseUri;
    }

    public RecordMetaData getRecordMetaData() {
        return recordMetaData;
    }

    private RecordMetaData createRecordMetaData() throws RelationalException {
        final var schemaBuilder = RecordLayerSchemaTemplate.newBuilder();
        SystemTableRegistry.getSystemTable(SystemTableRegistry.SCHEMAS_TABLE_NAME).addDefinition(schemaBuilder);
        SystemTableRegistry.getSystemTable(SystemTableRegistry.DATABASE_TABLE_NAME).addDefinition(schemaBuilder);
        final var schemaTemplate = schemaBuilder.setName("CATALOG_TEMPLATE").setVersion(1L).build();
        return schemaTemplate.toRecordMetadata();
    }
}
