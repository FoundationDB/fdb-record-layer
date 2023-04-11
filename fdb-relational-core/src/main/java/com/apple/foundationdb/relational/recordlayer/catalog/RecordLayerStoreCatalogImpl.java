/*
 * RecordLayerStoreCatalogImpl.java
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

package com.apple.foundationdb.relational.recordlayer.catalog;

import com.apple.foundationdb.record.EndpointType;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCoreStorageException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorResult;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataOptionsProto;
import com.apple.foundationdb.record.RecordMetaDataProto;
import com.apple.foundationdb.record.RecordMetaDataProvider;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoredRecord;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpace;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.relational.api.Continuation;
import com.apple.foundationdb.relational.api.ProtobufDataBuilder;
import com.apple.foundationdb.relational.api.Row;
import com.apple.foundationdb.relational.api.SqlTypeSupport;
import com.apple.foundationdb.relational.api.StructMetaData;
import com.apple.foundationdb.relational.api.Transaction;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.catalog.CatalogValidator;
import com.apple.foundationdb.relational.api.catalog.InMemorySchemaTemplateCatalog;
import com.apple.foundationdb.relational.api.catalog.SchemaTemplateCatalog;
import com.apple.foundationdb.relational.api.catalog.StoreCatalog;
import com.apple.foundationdb.relational.api.ddl.ProtobufDdlUtil;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
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
import com.apple.foundationdb.relational.recordlayer.util.Assert;
import com.apple.foundationdb.relational.recordlayer.util.ExceptionUtil;

import com.google.protobuf.Descriptors;
import com.google.protobuf.ExtensionRegistry;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.net.URI;
import java.sql.SQLException;
import java.util.Objects;

/**
 * This class constructs the record store that holds the catalog metadata.
 * <p>
 * We currently have two tables in this catalog: Schema, and DatabaseInfo.
 * You can find the definition of these two tables either in {@code catalog_schema_data.proto}, or
 * in the respective system tables classes {@link com.apple.foundationdb.relational.recordlayer.catalog.systables.SchemaSystemTable},
 * resp. {@link com.apple.foundationdb.relational.recordlayer.catalog.systables.DatabaseInfoSystemTable}.
 * <p>
 * Internally, here is how the is stored:
 * - We use a prefix "/__SYS/catalog".
 * - For {@code Schema}, we use '0' (zero) as a record type key.
 * - For {@code DatabaseInfo}, we use '1' (one) as a record type key.
 * Here is an example on how the records are laid out:
 * <p>
 * __SYS
 * catalog
 * {0, /DB1, SCh1} Schema { name='SCh1', tables... }
 * {0, /__SYS, catalog} Schema { name='catalog', tables= {Schema, DatabaseInfo}}
 * {1, /DB1}   DatabaseInfo { name=/DB1}
 * {1, /__SYS} DatabaseInfo {name=__SYS }
 */
public class RecordLayerStoreCatalogImpl implements StoreCatalog {

    public static final String TEMPLATE = RelationalKeyspaceProvider.CATALOG + "_TEMPLATE";
    public static final int TEMPLATE_VERSION = 1;
    private StructMetaData dbTableMetaData;

    static {
        ExtensionRegistry defaultExtensionRegistry = ExtensionRegistry.newInstance();
        RecordMetaDataOptionsProto.registerAllExtensions(defaultExtensionRegistry);
    }

    private final RelationalKeyspaceProvider.RelationalSchemaPath schemaPath;

    private final RecordMetaDataProvider metaDataProvider;

    private final SchemaTemplateCatalog schemaTemplateCatalog;

    public RecordLayerStoreCatalogImpl(@Nonnull final KeySpace keySpace, @Nonnull SchemaTemplateCatalog schemaTemplateCatalog) throws RelationalException {
        schemaPath = RelationalKeyspaceProvider.toDatabasePath(URI.create("/" + RelationalKeyspaceProvider.SYS), keySpace).schemaPath(RelationalKeyspaceProvider.CATALOG);
        metaDataProvider = setupMetadataProvider();
        this.schemaTemplateCatalog = schemaTemplateCatalog;
    }

    @Nonnull
    private RecordMetaDataProvider setupMetadataProvider() throws RelationalException {
        final var schema = getCatalogSchemaTemplate().generateSchema("/" + RelationalKeyspaceProvider.SYS, RelationalKeyspaceProvider.CATALOG);
        final RecordMetaDataProto.MetaData proto;
        proto = schema.getSchemaTemplate().unwrap(RecordLayerSchemaTemplate.class).toRecordMetadata().toProto();
        return RecordMetaData.build(proto);
    }

    /**
     * Bootstraps the {@code __SYS} database if not already. Bootstrapping involves populating the corresponding
     * entry in Relational catalog with information about the available tables of this database.
     *
     * @param transaction The transaction used to load the {@code /__SYS?schema=catalog} schema and write to Relational catalog.
     * @throws RelationalException in case of schema parsing error.
     */
    private void bootstrapSystemDatabase(Transaction transaction) throws RelationalException {
        // TODO this needs careful design to solve a spectrum of issues pertaining concurrent bootstrapping.
        URI dbUri = URI.create("/" + RelationalKeyspaceProvider.SYS);
        final Schema schema = getCatalogSchemaTemplate().generateSchema(dbUri.getPath(), RelationalKeyspaceProvider.CATALOG);
        if (!schemaTemplateCatalog.doesSchemaTemplateExist(transaction, TEMPLATE)) {
            schemaTemplateCatalog.updateTemplate(transaction, getCatalogSchemaTemplate());
        }
        saveSchema(transaction, schema, true);
    }

    public void initialize(@Nonnull final Transaction createTxn) throws RelationalException {
        try {
            final var store = FDBRecordStore.newBuilder()
                    .setKeySpacePath(schemaPath)
                    .setContext(createTxn.unwrap(FDBRecordContext.class))
                    .setMetaDataProvider(metaDataProvider)
                    .createOrOpen(FDBRecordStoreBase.StoreExistenceCheck.NONE);

            // Set the Catalog store's state cacheability to be true to make frequent opening of the store a light
            // operation.
            store.setStateCacheability(true);

            // Update the schema template without any existence checks for now because we depend on the in-memory schema
            // template store that does not persist. Hence, even though the entry remains in the schema table (that is
            // backed by the RecordStore) the catalog schema template needs to be `put`
            if (getSchemaTemplateCatalog() instanceof InMemorySchemaTemplateCatalog) {
                schemaTemplateCatalog.updateTemplate(createTxn, getCatalogSchemaTemplate());
            }

            bootstrapSystemDatabase(createTxn);
        } catch (RecordCoreStorageException ex) {
            throw ExceptionUtil.toRelationalException(ex);
        }
    }

    @Override
    public SchemaTemplateCatalog getSchemaTemplateCatalog() {
        return schemaTemplateCatalog;
    }

    @Override
    @Nonnull
    public RecordLayerSchema loadSchema(@Nonnull Transaction txn, @Nonnull URI databaseId, @Nonnull String schemaName) throws RelationalException {
        final FDBRecordStoreBase<Message> recordStore = openFDBRecordStore(txn);
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
    public void saveSchema(@Nonnull final Transaction txn, @Nonnull final Schema schema, boolean createDatabaseIfNecessary) throws RelationalException {
        final var recordStore = openFDBRecordStore(txn);
        CatalogValidator.validateSchema(schema);
        if (!doesDatabaseExist(recordStore, URI.create(schema.getDatabaseName()))) {
            if (createDatabaseIfNecessary) {
                createDatabase(recordStore, URI.create(schema.getDatabaseName()));
            } else {
                throw new RelationalException(String.format("Cannot create schema %s because database %s does not exist.", schema.getName(), schema.getDatabaseName()),
                        ErrorCode.UNDEFINED_DATABASE);
            }
        }
        Assert.that(schema instanceof RecordLayerSchema,
                String.format("Unexpected schema type %s", schema.getClass()),
                ErrorCode.INTERNAL_ERROR);
        Assert.that(schemaTemplateCatalog.doesSchemaTemplateExist(txn, schema.getSchemaTemplate().getName(), schema.getSchemaTemplate().getVersion()),
                String.format("Cannot create schema %s because schema template %s version %d does not exist.", schema.getName(), schema.getSchemaTemplate().getName(), schema.getSchemaTemplate().getVersion()),
                ErrorCode.UNKNOWN_SCHEMA_TEMPLATE);
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
        try {
            FDBRecordStoreBase<Message> recordStore = openFDBRecordStore(txn);
            ProtobufDataBuilder pmd = new ProtobufDataBuilder(metaDataProvider.getRecordMetaData().getRecordType(SystemTableRegistry.DATABASE_TABLE_NAME).getDescriptor());
            Message m = pmd.setField("DATABASE_ID", dbUri.getPath()).build();
            recordStore.saveRecord(m);
        } catch (RecordCoreException | SQLException ex) {
            throw ExceptionUtil.toRelationalException(ex);
        }
    }

    private void createDatabase(@Nonnull FDBRecordStoreBase<Message> recordStore, URI dbUri) throws RelationalException {
        try {
            ProtobufDataBuilder pmd = new ProtobufDataBuilder(metaDataProvider.getRecordMetaData().getRecordType(SystemTableRegistry.DATABASE_TABLE_NAME).getDescriptor());
            Message m = pmd.setField("DATABASE_ID", dbUri.getPath()).build();
            recordStore.saveRecord(m);
        } catch (RecordCoreException | SQLException ex) {
            throw ExceptionUtil.toRelationalException(ex);
        }
    }

    @Override
    public RelationalResultSet listDatabases(@Nonnull Transaction txn, @Nonnull Continuation continuation) throws RelationalException {
        FDBRecordStoreBase<Message> recordStore = openFDBRecordStore(txn);
        Tuple key = Tuple.from(SystemTableRegistry.DATABASE_INFO_RECORD_TYPE_KEY);
        RecordCursor<FDBStoredRecord<Message>> cursor = recordStore.scanRecords(new TupleRange(key, key, EndpointType.RANGE_INCLUSIVE, EndpointType.RANGE_INCLUSIVE), continuation.getUnderlyingBytes(), ScanProperties.FORWARD_SCAN);
        return new RecordLayerResultSet(dbTableMetaData, RecordLayerIterator.create(cursor, this::transformDatabaseInfo), null /* caller is responsible for managing tx state */);
    }

    @Override
    public RelationalResultSet listSchemas(@Nonnull Transaction txn, @Nonnull Continuation continuation) throws RelationalException {
        FDBRecordStoreBase<Message> recordStore = openFDBRecordStore(txn);
        Tuple key = Tuple.from(SystemTableRegistry.SCHEMA_RECORD_TYPE_KEY);
        RecordCursor<FDBStoredRecord<Message>> cursor = recordStore.scanRecords(new TupleRange(key, key, EndpointType.RANGE_INCLUSIVE, EndpointType.RANGE_INCLUSIVE), continuation.getUnderlyingBytes(), ScanProperties.FORWARD_SCAN);
        Descriptors.Descriptor schemaDesc = recordStore.getRecordMetaData().getRecordMetaData().getRecordType(SystemTableRegistry.SCHEMAS_TABLE_NAME).getDescriptor();
        return new RecordLayerResultSet(getMetaData(schemaDesc),
                RecordLayerIterator.create(cursor, this::transformSchema), null /* caller is responsible for managing tx state */);
    }

    @Override
    public RelationalResultSet listSchemas(@Nonnull Transaction txn, @Nonnull URI databaseId, @Nonnull Continuation continuation) throws RelationalException {
        FDBRecordStoreBase<Message> recordStore = openFDBRecordStore(txn);
        Tuple key = Tuple.from(SystemTableRegistry.SCHEMA_RECORD_TYPE_KEY, databaseId.getPath());
        RecordCursor<FDBStoredRecord<Message>> cursor = recordStore.scanRecords(new TupleRange(key, key, EndpointType.RANGE_INCLUSIVE, EndpointType.RANGE_INCLUSIVE), continuation.getUnderlyingBytes(), ScanProperties.FORWARD_SCAN);
        Descriptors.Descriptor schemaDesc = recordStore.getRecordMetaData().getRecordMetaData().getRecordType(SystemTableRegistry.SCHEMAS_TABLE_NAME).getDescriptor();
        return new RecordLayerResultSet(getMetaData(schemaDesc), RecordLayerIterator.create(cursor, this::transformSchema), null /* caller is responsible for managing tx state */);
    }

    @Override
    public void deleteSchema(@Nonnull Transaction txn, @Nonnull URI dbUri, @Nonnull String schemaName) throws RelationalException {
        try {
            FDBRecordStoreBase<Message> recordStore = openFDBRecordStore(txn);
            final var primaryKey = getSchemaKey(dbUri, schemaName);
            Assert.that(recordStore.deleteRecord(primaryKey), "Schema " + dbUri.getPath() + "/" + schemaName + " does not exist", ErrorCode.UNDEFINED_SCHEMA);
        } catch (RecordCoreException rce) {
            throw ExceptionUtil.toRelationalException(rce);
        }
    }

    @Override
    public boolean doesDatabaseExist(@Nonnull Transaction txn, @Nonnull URI databaseId) throws RelationalException {
        FDBRecordStoreBase<Message> recordStore = openFDBRecordStore(txn);
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
        FDBRecordStoreBase<Message> recordStore = openFDBRecordStore(txn);
        try {
            Tuple primaryKey = getSchemaKey(dbUri, schemaName);
            return recordStore.loadRecord(primaryKey) != null;
        } catch (RecordCoreException rce) {
            throw ExceptionUtil.toRelationalException(rce);
        }
    }

    @Override
    public boolean deleteDatabase(@Nonnull Transaction txn, @Nonnull URI dbUrl) throws RelationalException {
        FDBRecordStoreBase<Message> recordStore = openFDBRecordStore(txn);
        try {
            String dbId = dbUrl.getPath();
            final var allSchemasDeleted = deleteSchemas(recordStore, URI.create(dbId));
            if (allSchemasDeleted) {
                // when all schemas are deleted, delete the databaseId from DATABASE_INFO table
                recordStore.deleteRecord(Tuple.from(SystemTableRegistry.DATABASE_INFO_RECORD_TYPE_KEY, dbId));
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

    private FDBRecordStoreBase<Message> openFDBRecordStore(@Nonnull Transaction txn) throws RelationalException {
        try {
            return FDBRecordStore.newBuilder().setKeySpacePath(schemaPath).setContext(txn.unwrap(FDBRecordContext.class)).setMetaDataProvider(metaDataProvider).open();
        } catch (RecordCoreException ex) {
            throw ExceptionUtil.toRelationalException(ex);
        }
    }

    // delete schemas for the matching dbUri.
    // returns true if the operation completes, false when the operation cannot complete because of txn timeout
    // throws exception otherwise.
    private boolean deleteSchemas(@Nonnull FDBRecordStoreBase<Message> recordStore, @Nonnull URI dbUri) throws RelationalException {
        Tuple key = Tuple.from(SystemTableRegistry.SCHEMA_RECORD_TYPE_KEY, dbUri.getPath());
        RecordCursor<FDBStoredRecord<Message>> cursor = recordStore.scanRecords(new TupleRange(key, key, EndpointType.RANGE_INCLUSIVE, EndpointType.RANGE_INCLUSIVE), ContinuationImpl.BEGIN.getUnderlyingBytes(), ScanProperties.FORWARD_SCAN);
        RecordCursorResult<FDBStoredRecord<Message>> cursorResult = null;
        try {
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

    private void putSchema(@Nonnull final RecordLayerSchema schema, @Nonnull final FDBRecordStoreBase<Message> recordStore) throws RelationalException {
        try {
            @Nonnull final ProtobufDataBuilder pmd = new ProtobufDataBuilder(metaDataProvider.getRecordMetaData().getRecordType(SystemTableRegistry.SCHEMAS_TABLE_NAME).getDescriptor());
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

    private StructMetaData getMetaData(Descriptors.Descriptor descriptor) throws RelationalException {
        return SqlTypeSupport.recordToMetaData(ProtobufDdlUtil.recordFromDescriptor(descriptor));
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
        final RecordMetaData recordMetaData = metaDataProvider.getRecordMetaData();
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
        final RecordMetaData recordMetaData = metaDataProvider.getRecordMetaData();
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

    private RecordLayerSchemaTemplate getCatalogSchemaTemplate() throws RelationalException {
        final var schemaBuilder = RecordLayerSchemaTemplate.newBuilder();

        SystemTableRegistry.getSystemTable(SystemTableRegistry.SCHEMAS_TABLE_NAME).addDefinition(schemaBuilder);
        SystemTableRegistry.getSystemTable(SystemTableRegistry.DATABASE_TABLE_NAME).addDefinition(schemaBuilder);

        //TODO(bfines) unfortunate side effect--can we do this differently?
        dbTableMetaData = SqlTypeSupport.typeToMetaData(DataTypeUtils.toRecordLayerType(schemaBuilder.findType(SystemTableRegistry.DATABASE_TABLE_NAME).orElseThrow()));
        return schemaBuilder
                .setName(TEMPLATE)
                .setVersion(TEMPLATE_VERSION)
                .build();
    }
}
