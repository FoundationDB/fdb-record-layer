/*
 * RecordLayerStoreCatalog.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2025 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.recordlayer.catalog;

import com.apple.foundationdb.record.EndpointType;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCoreStorageException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorResult;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataOptionsProto;
import com.apple.foundationdb.record.RecordMetaDataProvider;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoredRecord;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpace;
import com.apple.foundationdb.relational.api.Continuation;
import com.apple.foundationdb.relational.api.ProtobufDataBuilder;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.RelationalStructMetaData;
import com.apple.foundationdb.relational.api.Row;
import com.apple.foundationdb.relational.api.StructMetaData;
import com.apple.foundationdb.relational.api.Transaction;
import com.apple.foundationdb.relational.api.catalog.CatalogValidator;
import com.apple.foundationdb.relational.api.catalog.SchemaTemplateCatalog;
import com.apple.foundationdb.relational.api.catalog.StoreCatalog;
import com.apple.foundationdb.relational.api.ddl.ProtobufDdlUtil;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.api.metadata.DataType;
import com.apple.foundationdb.relational.api.metadata.Schema;
import com.apple.foundationdb.relational.api.metadata.SchemaTemplate;
import com.apple.foundationdb.relational.recordlayer.ArrayRow;
import com.apple.foundationdb.relational.recordlayer.ContinuationImpl;
import com.apple.foundationdb.relational.recordlayer.MessageTuple;
import com.apple.foundationdb.relational.recordlayer.RecordLayerIterator;
import com.apple.foundationdb.relational.recordlayer.RecordLayerResultSet;
import com.apple.foundationdb.relational.recordlayer.RelationalKeyspaceProvider;
import com.apple.foundationdb.relational.recordlayer.catalog.systables.SystemTableRegistry;
import com.apple.foundationdb.relational.recordlayer.metadata.DataTypeUtils;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerSchema;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerSchemaTemplate;
import com.apple.foundationdb.relational.recordlayer.util.ExceptionUtil;
import com.apple.foundationdb.relational.util.Assert;
import com.apple.foundationdb.relational.util.SpotBugsSuppressWarnings;
import com.apple.foundationdb.tuple.Tuple;
import com.google.protobuf.Descriptors;
import com.google.protobuf.ExtensionRegistry;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.net.URI;
import java.sql.SQLException;
import java.util.Locale;
import java.util.Objects;

/**
 * This class defines and constructs the record store that holds and manipulates the catalog metadata.
 * <p>
 * We currently have three tables in this catalog: Schema, DatabaseInfo, and SchemaTemplate.
 * You can find the definition of these tables either in {@code catalog_schema_data.proto}, or
 * in the respective system tables classes: e.g.
 * {@link com.apple.foundationdb.relational.recordlayer.catalog.systables.SchemaSystemTable},
 * {@link com.apple.foundationdb.relational.recordlayer.catalog.systables.DatabaseInfoSystemTable}, and
 * {@link com.apple.foundationdb.relational.recordlayer.catalog.systables.SchemaTemplateSystemTable}.
 * <p>
 * Internally, here is how this is stored:
 * <pre>
 * - We use a prefix "/__SYS/catalog".
 * - For {@code Schema}, we use '0' (zero) as a record type key.
 * - For {@code DatabaseInfo}, we use '1' (one) as a record type key.
 * - For {@code SchemaTemplates}, we use '2' (two) as a record type key.
 * ...
 * </pre>
 * Here is an example on how the records are laid out:
 * <pre>
 * __SYS
 * catalog
 * {0, /DB1, SCh1} Schema { name='SCh1', tables... }
 * {0, /__SYS, catalog} Schema { name='catalog', tables={Schema, DatabaseInfo}}
 * {1, /DB1}   DatabaseInfo { name=/DB1}
 * {1, /__SYS} DatabaseInfo {name=__SYS }
 * ...
 * </pre>
 */
class RecordLayerStoreCatalog implements StoreCatalog {
    private static final URI DASH_DASH_SYS = URI.create("/" + RelationalKeyspaceProvider.SYS);
    private static final String CATALOG_TEMPLATE = RelationalKeyspaceProvider.CATALOG + "_TEMPLATE";
    private static final int CATALOG_TEMPLATE_VERSION = 1;

    static {
        ExtensionRegistry defaultExtensionRegistry = ExtensionRegistry.newInstance();
        RecordMetaDataOptionsProto.registerAllExtensions(defaultExtensionRegistry);
    }

    private final RelationalKeyspaceProvider.RelationalSchemaPath catalogSchemaPath;

    private final RecordMetaDataProvider catalogRecordMetaDataProvider;
    private final KeySpace keySpace;

    private SchemaTemplateCatalog schemaTemplateCatalog;

    private final RecordLayerSchemaTemplate catalogSchemaTemplate;

    private final RecordLayerSchema catalogSchema;

    @SpotBugsSuppressWarnings(value = "CT_CONSTRUCTOR_THROW", justification = "Hard to remove exception with current inheritance")
    RecordLayerStoreCatalog(@Nonnull final KeySpace keySpace) throws RelationalException {
        this.keySpace = keySpace;
        this.catalogSchemaPath = RelationalKeyspaceProvider.toDatabasePath(DASH_DASH_SYS, keySpace)
                .schemaPath(RelationalKeyspaceProvider.CATALOG);
        final var schemaBuilder = RecordLayerSchemaTemplate.newBuilder();
        SystemTableRegistry.getSystemTable(SystemTableRegistry.SCHEMAS_TABLE_NAME).addDefinition(schemaBuilder);
        SystemTableRegistry.getSystemTable(SystemTableRegistry.DATABASE_TABLE_NAME).addDefinition(schemaBuilder);
        SystemTableRegistry.getSystemTable(SystemTableRegistry.SCHEMA_TEMPLATE_TABLE_NAME).addDefinition(schemaBuilder);
        this.catalogSchemaTemplate = schemaBuilder.setName(CATALOG_TEMPLATE).setVersion(CATALOG_TEMPLATE_VERSION).build();
        this.catalogSchema = this.catalogSchemaTemplate.generateSchema(DASH_DASH_SYS.getPath(),
                RelationalKeyspaceProvider.CATALOG);
        this.catalogRecordMetaDataProvider = RecordMetaData.build(
                catalogSchema.getSchemaTemplate().unwrap(RecordLayerSchemaTemplate.class).toRecordMetadata().toProto());
    }

    /**
     * Initializes the {@link StoreCatalog} after construction. This method must be called before the catalog can be used.
     *
     * @param createTxn the transaction used for catalog setup and initialization
     * @return {@code this} StoreCatalog instance for method chaining
     * @throws RelationalException if catalog initialization fails
     */
    StoreCatalog initialize(@Nonnull final Transaction createTxn) throws RelationalException {
        return initialize(createTxn, new RecordLayerStoreSchemaTemplateCatalog(catalogSchema, catalogSchemaPath));
    }

    /**
     * Initializes the {@link StoreCatalog} after construction with a custom schema template catalog.
     * This method must be called before the catalog can be used.
     *
     * @param createTxn the transaction used for catalog setup and initialization
     * @param schemaTemplateCatalog the custom schema template catalog to use instead of the default
     * @return this StoreCatalog instance for method chaining
     * @throws RelationalException if catalog initialization fails
     */
    StoreCatalog initialize(@Nonnull final Transaction createTxn, @Nonnull final SchemaTemplateCatalog schemaTemplateCatalog)
            throws RelationalException {
        try {
            // Set Catalog store's state cacheability to be true to make frequent opening of store a light operation.
            final var store = FDBRecordStore.newBuilder()
                    .setKeySpacePath(catalogSchemaPath)
                    .setContext(createTxn.unwrap(FDBRecordContext.class))
                    .setMetaDataProvider(catalogRecordMetaDataProvider)
                    .createOrOpen(FDBRecordStoreBase.StoreExistenceCheck.NONE);
            store.setStateCacheability(true);
            // Make sure the catalog's schematemplate is in place. It won't be if this is initial start up or if the
            // schemaTemplateCatalog is the non-persisting in-memory implementation.
            if (!schemaTemplateCatalog.doesSchemaTemplateExist(createTxn, this.catalogSchemaTemplate.getName())) {
                schemaTemplateCatalog.createTemplate(createTxn, this.catalogSchemaTemplate);
            }
            this.schemaTemplateCatalog = schemaTemplateCatalog;
            // Persist our hard-coded catalog schema.
            saveSchema(createTxn, this.catalogSchema, true);
        } catch (RecordCoreStorageException ex) {
            throw ExceptionUtil.toRelationalException(ex);
        }
        return this;
    }

    @Override
    public SchemaTemplateCatalog getSchemaTemplateCatalog() {
        return schemaTemplateCatalog;
    }

    @Nonnull
    @Override
    public RecordLayerSchema loadSchema(@Nonnull Transaction txn, @Nonnull URI databaseId, @Nonnull String schemaName) throws RelationalException {
        var recordStore = RecordLayerStoreUtils.openRecordStore(txn, this.catalogSchemaPath,
                this.catalogRecordMetaDataProvider);
        Assert.notNull(recordStore);
        final Tuple primaryKey = Tuple.from(SystemTableRegistry.SCHEMA_RECORD_TYPE_KEY, databaseId.getPath(), schemaName);
        try {
            final FDBStoredRecord<Message> record = recordStore.loadRecord(primaryKey);
            if (record == null) {
                throw new RelationalException("Schema <" + databaseId.getPath() + "/" + schemaName + "> does not exist in the catalog!", ErrorCode.UNDEFINED_SCHEMA);
            }
            Message m = record.getRecord();
            return parseSchemaTable(m, txn);
        } catch (RecordCoreException ex) {
            throw ExceptionUtil.toRelationalException(ex);
        }
    }

    @Override
    public void saveSchema(@Nonnull final Transaction txn, @Nonnull final Schema schema,
                           boolean createDatabaseIfNecessary) throws RelationalException {
        var recordStore = RecordLayerStoreUtils.openRecordStore(txn, this.catalogSchemaPath,
                this.catalogRecordMetaDataProvider);
        CatalogValidator.validateSchema(schema);
        if (!doesDatabaseExist(recordStore, URI.create(schema.getDatabaseName()))) {
            if (createDatabaseIfNecessary) {
                createDatabase(recordStore, URI.create(schema.getDatabaseName()));
            } else {
                throw new RelationalException(String.format(Locale.ROOT, "Cannot create schema %s because database %s does not exist.", schema.getName(), schema.getDatabaseName()),
                        ErrorCode.UNDEFINED_DATABASE);
            }
        }
        Assert.that(schema instanceof RecordLayerSchema, ErrorCode.UNDEFINED_SCHEMA, "Unexpected schema type %s", schema.getClass());
        Assert.that(schemaTemplateCatalog.doesSchemaTemplateExist(txn, schema.getSchemaTemplate().getName(),
                        schema.getSchemaTemplate().getVersion()),
                ErrorCode.UNKNOWN_SCHEMA_TEMPLATE,
                () -> String.format(Locale.ROOT, "Cannot create schema %s because schema template %s version %d does not exist.",
                        schema.getName(), schema.getSchemaTemplate().getName(),
                        schema.getSchemaTemplate().getVersion()));
        try {
            putSchema((RecordLayerSchema) schema, recordStore);
        } catch (RecordCoreException ex) {
            throw ExceptionUtil.toRelationalException(ex);
        }
    }

    @Override
    public void repairSchema(@Nonnull Transaction txn, @Nonnull String databaseId, @Nonnull String schemaName) throws RelationalException {
        // a read-modify-write loop, done in 1 transaction
        final RecordLayerSchema schema = loadSchema(txn, URI.create(databaseId), schemaName);
        // load latest schema template
        final SchemaTemplate template = schemaTemplateCatalog.loadSchemaTemplate(txn, schema.getSchemaTemplate().getName());
        final Schema newSchema = template.generateSchema(databaseId, schemaName);
        saveSchema(txn, newSchema, false);
    }

    @Override
    public void createDatabase(@Nonnull Transaction txn, URI dbUri) throws RelationalException {
        var recordStore = RecordLayerStoreUtils.openRecordStore(txn, this.catalogSchemaPath,
                this.catalogRecordMetaDataProvider);
        try {
            createDatabase(recordStore, dbUri);
        } catch (RecordCoreException ex) {
            throw ExceptionUtil.toRelationalException(ex);
        }
    }

    @SuppressWarnings("deprecation") // need to replace protobuf data builder
    private void createDatabase(@Nonnull FDBRecordStoreBase<Message> recordStore, URI dbUri) throws RelationalException {
        try {
            ProtobufDataBuilder pmd = new ProtobufDataBuilder(catalogRecordMetaDataProvider.getRecordMetaData().getRecordType(SystemTableRegistry.DATABASE_TABLE_NAME).getDescriptor());
            Message m = pmd.setField("DATABASE_ID", dbUri.getPath()).build();
            recordStore.saveRecord(m);
        } catch (RecordCoreException | SQLException ex) {
            throw ExceptionUtil.toRelationalException(ex);
        }
    }

    @SuppressWarnings("PMD.CloseResource") // cursor lifetime extends into lifetime of returned result set
    @Override
    public RelationalResultSet listDatabases(@Nonnull Transaction txn, @Nonnull Continuation continuation) throws RelationalException {
        var recordStore = RecordLayerStoreUtils.openRecordStore(txn, this.catalogSchemaPath,
                this.catalogRecordMetaDataProvider);
        Tuple key = Tuple.from(SystemTableRegistry.DATABASE_INFO_RECORD_TYPE_KEY);
        RecordCursor<FDBStoredRecord<Message>> cursor = recordStore.scanRecords(new TupleRange(key, key, EndpointType.RANGE_INCLUSIVE, EndpointType.RANGE_INCLUSIVE), continuation.getExecutionState(), ScanProperties.FORWARD_SCAN);
        Descriptors.Descriptor d = recordStore.getRecordMetaData().getRecordMetaData().getRecordType(SystemTableRegistry.DATABASE_TABLE_NAME).getDescriptor();
        return new RecordLayerResultSet(getMetaData(d), RecordLayerIterator.create(cursor, this::transformDatabaseInfo), null /* caller is responsible for managing tx state */);
    }

    @SuppressWarnings("PMD.CloseResource") // cursor lifetime extends into lifetime of returned result set
    @Override
    public RelationalResultSet listSchemas(@Nonnull Transaction txn, @Nonnull Continuation continuation) throws RelationalException {
        var recordStore = RecordLayerStoreUtils.openRecordStore(txn, this.catalogSchemaPath,
                this.catalogRecordMetaDataProvider);
        Tuple key = Tuple.from(SystemTableRegistry.SCHEMA_RECORD_TYPE_KEY);
        RecordCursor<FDBStoredRecord<Message>> cursor = recordStore.scanRecords(new TupleRange(key, key, EndpointType.RANGE_INCLUSIVE, EndpointType.RANGE_INCLUSIVE), continuation.getExecutionState(), ScanProperties.FORWARD_SCAN);
        Descriptors.Descriptor schemaDesc = recordStore.getRecordMetaData().getRecordMetaData().getRecordType(SystemTableRegistry.SCHEMAS_TABLE_NAME).getDescriptor();
        return new RecordLayerResultSet(getMetaData(schemaDesc),
                RecordLayerIterator.create(cursor, this::transformSchema), null /* caller is responsible for managing tx state */);
    }

    @SuppressWarnings("PMD.CloseResource") // cursor lifetime extends into lifetime of returned result set
    @Override
    public RelationalResultSet listSchemas(@Nonnull Transaction txn, @Nonnull URI databaseId, @Nonnull Continuation continuation) throws RelationalException {
        var recordStore = RecordLayerStoreUtils.openRecordStore(txn, this.catalogSchemaPath,
                this.catalogRecordMetaDataProvider);
        Tuple key = Tuple.from(SystemTableRegistry.SCHEMA_RECORD_TYPE_KEY, databaseId.getPath());
        RecordCursor<FDBStoredRecord<Message>> cursor = recordStore.scanRecords(new TupleRange(key, key, EndpointType.RANGE_INCLUSIVE, EndpointType.RANGE_INCLUSIVE), continuation.getExecutionState(), ScanProperties.FORWARD_SCAN);
        Descriptors.Descriptor schemaDesc = recordStore.getRecordMetaData().getRecordMetaData().getRecordType(SystemTableRegistry.SCHEMAS_TABLE_NAME).getDescriptor();
        return new RecordLayerResultSet(getMetaData(schemaDesc), RecordLayerIterator.create(cursor, this::transformSchema), null /* caller is responsible for managing tx state */);
    }

    @Override
    public void deleteSchema(@Nonnull Transaction txn, @Nonnull URI dbUri, @Nonnull String schemaName) throws RelationalException {
        var recordStore = RecordLayerStoreUtils.openRecordStore(txn, this.catalogSchemaPath,
                this.catalogRecordMetaDataProvider);
        try {
            final var primaryKey = getSchemaKey(dbUri, schemaName);
            Assert.that(recordStore.deleteRecord(primaryKey), ErrorCode.UNDEFINED_SCHEMA, "Schema " + dbUri.getPath() + "/" + schemaName + " does not exist");
        } catch (RecordCoreException rce) {
            throw ExceptionUtil.toRelationalException(rce);
        }
    }

    @Override
    public boolean doesDatabaseExist(@Nonnull Transaction txn, @Nonnull URI databaseId) throws RelationalException {
        var recordStore = RecordLayerStoreUtils.openRecordStore(txn, this.catalogSchemaPath,
                this.catalogRecordMetaDataProvider);
        return doesDatabaseExist(recordStore, databaseId);
    }

    private boolean doesDatabaseExist(@Nonnull FDBRecordStoreBase<Message> recordStore, @Nonnull URI databaseId) throws RelationalException {
        try {
            String dbId = databaseId.getPath();
            return recordStore.loadRecord(Tuple.from(SystemTableRegistry.DATABASE_INFO_RECORD_TYPE_KEY, dbId)) != null;
        } catch (RecordCoreException rce) {
            throw ExceptionUtil.toRelationalException(rce);
        }
    }

    @Override
    public boolean doesSchemaExist(@Nonnull Transaction txn, @Nonnull URI dbUri, @Nonnull String schemaName) throws RelationalException {
        var recordStore = RecordLayerStoreUtils.openRecordStore(txn, this.catalogSchemaPath,
                this.catalogRecordMetaDataProvider);
        try {
            Tuple primaryKey = getSchemaKey(dbUri, schemaName);
            return recordStore.loadRecord(primaryKey) != null;
        } catch (RecordCoreException rce) {
            throw ExceptionUtil.toRelationalException(rce);
        }
    }

    @Override
    public boolean deleteDatabase(@Nonnull Transaction txn, @Nonnull URI dbUrl, boolean throwIfDoesNotExist) throws RelationalException {
        var recordStore = RecordLayerStoreUtils.openRecordStore(txn, this.catalogSchemaPath,
                this.catalogRecordMetaDataProvider);
        try {
            String dbId = dbUrl.getPath();
            final var allSchemasDeleted = deleteSchemas(recordStore, URI.create(dbId));
            if (allSchemasDeleted) {
                // when all schemas are deleted, delete the databaseId from DATABASE_INFO table
                if (!recordStore.deleteRecord(Tuple.from(SystemTableRegistry.DATABASE_INFO_RECORD_TYPE_KEY, dbId)) && throwIfDoesNotExist) {
                    throw new RelationalException("Cannot delete unknown database: " + dbUrl, ErrorCode.UNKNOWN_DATABASE);
                }
            } else {
                return false;
            }
        } catch (RecordCoreException rce) {
            final var relationalException = ExceptionUtil.toRelationalException(rce);
            if (relationalException.getErrorCode() == ErrorCode.TRANSACTION_INACTIVE || relationalException.getErrorCode() == ErrorCode.TRANSACTION_TIMEOUT) {
                return false;
            }
            throw ExceptionUtil.toRelationalException(rce);
        }
        return true;
    }

    @Nonnull
    @Override
    public KeySpace getKeySpace() throws RelationalException {
        return keySpace;
    }

    // delete schemas for the matching dbUri.
    // returns true if the operation completes, false when the operation cannot complete because of txn timeout
    // throws exception otherwise.
    private boolean deleteSchemas(@Nonnull FDBRecordStoreBase<Message> recordStore, @Nonnull URI dbUri) throws RelationalException {
        Tuple key = Tuple.from(SystemTableRegistry.SCHEMA_RECORD_TYPE_KEY, dbUri.getPath());
        try (RecordCursor<FDBStoredRecord<Message>> cursor =
                recordStore.scanRecords(new TupleRange(key, key, EndpointType.RANGE_INCLUSIVE,
                                EndpointType.RANGE_INCLUSIVE), ContinuationImpl.BEGIN.getExecutionState(),
                        ScanProperties.FORWARD_SCAN);) {
            RecordCursorResult<FDBStoredRecord<Message>> cursorResult;
            do {
                cursorResult = cursor.getNext();
                if (cursorResult.getContinuation().isEnd()) {
                    break;
                }
                Tuple primaryKey = Objects.requireNonNull(cursorResult.get()).getPrimaryKey();
                recordStore.deleteRecord(primaryKey);
            } while (cursorResult.hasNext());
        } catch (RecordCoreStorageException ex) {
            final var relationalException = ExceptionUtil.toRelationalException(ex);
            if (relationalException.getErrorCode() == ErrorCode.TRANSACTION_INACTIVE || relationalException.getErrorCode() == ErrorCode.TRANSACTION_TIMEOUT) {
                return false;
            }
            throw ExceptionUtil.toRelationalException(ex);
        }
        return true;
    }

    @SuppressWarnings("deprecation") // need to replace protobuf data builder
    private void putSchema(@Nonnull final RecordLayerSchema schema, @Nonnull final FDBRecordStoreBase<Message> recordStore) throws RelationalException {
        try {
            @Nonnull final ProtobufDataBuilder pmd = new ProtobufDataBuilder(catalogRecordMetaDataProvider.getRecordMetaData().getRecordType(SystemTableRegistry.SCHEMAS_TABLE_NAME).getDescriptor());
            @Nonnull final Message m = pmd.setField("DATABASE_ID", schema.getDatabaseName())
                    .setField("SCHEMA_NAME", schema.getName())
                    .setField("TEMPLATE_NAME", schema.getSchemaTemplate().getName())
                    .setField("TEMPLATE_VERSION", schema.getSchemaTemplate().getVersion())
                    .build();
            recordStore.saveRecord(m);
        } catch (RecordCoreException | SQLException e) {
            throw ExceptionUtil.toRelationalException(e);
        }
    }

    private static StructMetaData getMetaData(Descriptors.Descriptor descriptor) throws RelationalException {
        return RelationalStructMetaData.of((DataType.StructType) DataTypeUtils.toRelationalType(ProtobufDdlUtil.recordFromDescriptor(descriptor)));
    }

    @Nonnull
    private Tuple getSchemaKey(@Nonnull URI databaseId, @Nonnull String schemaName) {
        return Tuple.from(SystemTableRegistry.SCHEMA_RECORD_TYPE_KEY, databaseId.getPath(), schemaName);
    }

    @Nullable
    private Row transformSchema(@Nullable FDBStoredRecord<Message> record) {
        if (record == null) {
            return null;
        }
        Message m = record.getRecord();
        final RecordMetaData recordMetaData = catalogRecordMetaDataProvider.getRecordMetaData();
        final RecordType schemaTableMD = recordMetaData.getRecordType(SystemTableRegistry.SCHEMAS_TABLE_NAME);
        final Descriptors.Descriptor descriptor = schemaTableMD.getDescriptor();
        String dbId = (String) m.getField(descriptor.findFieldByName("DATABASE_ID"));
        String schemaName = (String) m.getField(descriptor.findFieldByName("SCHEMA_NAME"));
        String templateName = (String) m.getField(descriptor.findFieldByName("TEMPLATE_NAME"));
        final var version = (Integer) m.getField(descriptor.findFieldByName("TEMPLATE_VERSION"));
        return new ArrayRow(dbId, schemaName, templateName, version);
    }

    private Row transformDatabaseInfo(FDBStoredRecord<Message> record) {
        return new MessageTuple(record.getRecord());
    }

    private RecordLayerSchema parseSchemaTable(Message m, Transaction txn) throws RelationalException {
        final RecordMetaData recordMetaData = catalogRecordMetaDataProvider.getRecordMetaData();
        final RecordType schemaTableMD = recordMetaData.getRecordType(SystemTableRegistry.SCHEMAS_TABLE_NAME);
        final Descriptors.Descriptor descriptor = schemaTableMD.getDescriptor();
        String dbId = (String) m.getField(descriptor.findFieldByName("DATABASE_ID"));
        String schemaName = (String) m.getField(descriptor.findFieldByName("SCHEMA_NAME"));
        // load metadata from template table
        String templateName = (String) m.getField(descriptor.findFieldByName("TEMPLATE_NAME"));
        final var version = (Integer) m.getField(descriptor.findFieldByName("TEMPLATE_VERSION"));
        SchemaTemplate template = schemaTemplateCatalog.loadSchemaTemplate(txn, templateName, version);
        return (RecordLayerSchema) template.generateSchema(dbId, schemaName);
    }
}
