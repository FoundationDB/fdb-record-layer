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
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpacePath;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.relational.api.Continuation;
import com.apple.foundationdb.relational.api.ProtobufDataBuilder;
import com.apple.foundationdb.relational.api.Row;
import com.apple.foundationdb.relational.api.SqlTypeSupport;
import com.apple.foundationdb.relational.api.StructMetaData;
import com.apple.foundationdb.relational.api.Transaction;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.catalog.CatalogValidator;
import com.apple.foundationdb.relational.api.ddl.ProtobufDdlUtil;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.recordlayer.ArrayRow;
import com.apple.foundationdb.relational.recordlayer.KeySpaceUtils;
import com.apple.foundationdb.relational.recordlayer.MessageTuple;
import com.apple.foundationdb.relational.recordlayer.RecordLayerIterator;
import com.apple.foundationdb.relational.recordlayer.RecordLayerResultSet;
import com.apple.foundationdb.relational.recordlayer.catalog.systables.SystemTableRegistry;
import com.apple.foundationdb.relational.recordlayer.ddl.SchemaTemplateDescriptor;
import com.apple.foundationdb.relational.recordlayer.query.TypingContext;
import com.apple.foundationdb.relational.recordlayer.util.Assert;
import com.apple.foundationdb.relational.recordlayer.util.ExceptionUtil;

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.ExtensionRegistry;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;

import java.net.URI;
import java.sql.SQLException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

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

    public static final String SCHEMA = "CATALOG";
    public static final String SYS_DB = "/__SYS";

    private static final long MAX_SCHEMA_TEMPLATE_VERSION = 1000L;
    private static final ExtensionRegistry EXTENSION_REGISTRY;
    private StructMetaData dbTableMetaData;

    static {
        ExtensionRegistry defaultExtensionRegistry = ExtensionRegistry.newInstance();
        RecordMetaDataOptionsProto.registerAllExtensions(defaultExtensionRegistry);
        EXTENSION_REGISTRY = defaultExtensionRegistry.getUnmodifiable();
    }

    private final KeySpacePath keySpacePath;

    private final RecordMetaDataProvider metaDataProvider;

    public RecordLayerStoreCatalogImpl(@Nonnull final KeySpace keySpace) throws RelationalException {
        keySpacePath = KeySpaceUtils.uriToPath(URI.create(SYS_DB + "/" + SCHEMA), keySpace);
        metaDataProvider = setupMetadataProvider();
    }

    @Nonnull
    private RecordMetaDataProvider setupMetadataProvider() throws RelationalException {
        final Schema schema = getCatalogSchemaTemplate().generateSchema("__SYS", SCHEMA);
        RecordMetaDataProto.MetaData proto = schema.getMetaData();
        return RecordMetaData.build(proto);
    }

    /**
     * Bootstraps the {@code __SYS} database if not already. Bootstrapping involves populating the corresponding
     * entry in Relational catalog with information about the available tables of this database.
     *
     * @param transaction The transaction used to load the {@code /__SYS/catalog} schema and write to Relational catalog.
     * @throws RelationalException in case of schema parsing error.
     */
    private void bootstrapSystemDatabase(Transaction transaction) throws RelationalException {

        // poor man's approach for checking SYS schema existence
        // TODO this needs careful design to solve a spectrum of issues pertaining concurrent bootstrapping.
        URI dbUri = URI.create(SYS_DB);
        try {
            loadSchema(transaction, dbUri, SCHEMA);
        } catch (RelationalException ve) {
            if (ve.getErrorCode() != ErrorCode.UNDEFINED_SCHEMA) {
                return;
            }
        }

        if (!doesDatabaseExist(transaction, dbUri)) {
            createDatabase(transaction, dbUri);
        }

        if (!doesSchemaExist(transaction, dbUri, SCHEMA)) {
            final SchemaTemplate schemaTemplate = getCatalogSchemaTemplate();

            //map the schema to the template
            final Schema schema = schemaTemplate.generateSchema(dbUri.getPath(), SCHEMA);

            //insert the schema into the catalog
            updateSchema(transaction, schema);
        }
    }

    public void initialize(@Nonnull final Transaction createTxn) throws RelationalException {
        try {
            FDBRecordStore.newBuilder()
                    .setKeySpacePath(keySpacePath)
                    .setContext(createTxn.unwrap(FDBRecordContext.class))
                    .setMetaDataProvider(metaDataProvider)
                    .createOrOpen(FDBRecordStoreBase.StoreExistenceCheck.NONE);
            bootstrapSystemDatabase(createTxn);
        } catch (RecordCoreStorageException ex) {
            throw ExceptionUtil.toRelationalException(ex);
        }
    }

    @Override
    @Nonnull
    public Schema loadSchema(@Nonnull Transaction txn, @Nonnull URI databaseId, @Nonnull String schemaName) throws RelationalException {
        final FDBRecordStore recordStore = openFDBRecordStore(txn);
        Assert.notNull(recordStore);
        final Tuple primaryKey = Tuple.from(SystemTableRegistry.SCHEMA_RECORD_TYPE_KEY, databaseId.getPath(), schemaName);
        try {
            final FDBStoredRecord<Message> record = recordStore.loadRecord(primaryKey);
            if (record == null) {
                throw new RelationalException("Schema <" + databaseId.getPath() + "/" + schemaName + "> does not exist in the catalog!", ErrorCode.UNDEFINED_SCHEMA);
            }
            Message m = record.getRecord();
            return parseSchemaTable(m);
        } catch (RecordCoreException ex) {
            throw ExceptionUtil.toRelationalException(ex);
        }
    }

    @Override
    public boolean updateSchema(@Nonnull Transaction txn, @Nonnull Schema dataToWrite) throws RelationalException {
        CatalogValidator.validateSchema(dataToWrite);
        Assert.that(doesDatabaseExist(txn, URI.create(dataToWrite.getDatabaseId())),
                String.format("Cannot create schema %s because database %s does not exist.", dataToWrite.getSchemaName(), dataToWrite.getDatabaseId()),
                ErrorCode.UNDEFINED_DATABASE);
        try {
            // open FDBRecordStore
            FDBRecordStore recordStore = openFDBRecordStore(txn);
            updateSchemaData(dataToWrite, recordStore);
            return true;
        } catch (RecordCoreException ex) {
            throw ExceptionUtil.toRelationalException(ex);
        }
    }

    @Override
    public void repairSchema(@Nonnull Transaction txn, @Nonnull String databaseId, @Nonnull String schemaName) throws RelationalException {
        // a read-modify-write loop, done in 1 transaction
        Schema schema = loadSchema(txn, URI.create(databaseId), schemaName);
        // load latest schema template
        SchemaTemplate template = loadSchemaTemplate(txn, schema.getSchemaTemplateName());
        Schema newSchema = new Schema(databaseId, schemaName, template.getMetaData(), schema.getSchemaTemplateName(), template.getVersion());
        updateSchema(txn, newSchema);
    }

    @Override
    @Nonnull
    public SchemaTemplate loadSchemaTemplate(@Nonnull Transaction txn, @Nonnull String templateName, long version) throws RelationalException {
        final FDBRecordStore recordStore = openFDBRecordStore(txn);
        Assert.notNull(recordStore);
        final Tuple primaryKey = Tuple.from(SystemTableRegistry.SCHEMA_TEMPLATE_RECORD_TYPE_KEY, templateName, version);
        try {
            final FDBStoredRecord<Message> record = recordStore.loadRecord(primaryKey);
            if (record == null) {
                throw new RelationalException("Schema Template " + templateName + " version " + version + " does not exist in the catalog!", ErrorCode.UNKNOWN_SCHEMA_TEMPLATE);
            }
            Message m = record.getRecord();
            return parseSchemaTemplateTable(m);
        } catch (RecordCoreException ex) {
            throw ExceptionUtil.toRelationalException(ex);
        }
    }

    @Override
    public SchemaTemplate loadSchemaTemplate(@Nonnull Transaction txn, @Nonnull String templateName) throws RelationalException {
        final FDBRecordStore recordStore = openFDBRecordStore(txn);
        Assert.notNull(recordStore);
        // reverse scan primary key, return the first record
        TupleRange scanRange = new TupleRange(Tuple.from(SystemTableRegistry.SCHEMA_TEMPLATE_RECORD_TYPE_KEY, templateName, 1L), Tuple.from(SystemTableRegistry.SCHEMA_TEMPLATE_RECORD_TYPE_KEY, templateName, MAX_SCHEMA_TEMPLATE_VERSION), EndpointType.RANGE_INCLUSIVE, EndpointType.RANGE_EXCLUSIVE);
        try (RecordCursor<FDBStoredRecord<Message>> cursor = recordStore.scanRecords(scanRange, null, ScanProperties.REVERSE_SCAN)) {
            RecordCursorResult<FDBStoredRecord<Message>> cursorResult = cursor.getNext();
            if (!cursorResult.hasNext()) {
                throw new RelationalException("Schema Template " + templateName + " does not exist in the catalog!", ErrorCode.UNKNOWN_SCHEMA_TEMPLATE);
            }
            Message m = cursorResult.get().getRecord();
            return parseSchemaTemplateTable(m);
        } catch (RecordCoreException ex) {
            throw ExceptionUtil.toRelationalException(ex);
        }
    }

    @Override
    public boolean doesSchemaTemplateExist(@Nonnull Transaction txn, @Nonnull String templateName) throws RelationalException {
        try {
            loadSchemaTemplate(txn, templateName);
            return true;
        } catch (RelationalException ex) {
            if (ex.getErrorCode() == ErrorCode.UNKNOWN_SCHEMA_TEMPLATE) {
                return false;
            }
            throw ex;
        }
    }

    @Override
    public boolean updateSchemaTemplate(@Nonnull Transaction txn, @Nonnull SchemaTemplate dataToWrite) throws RelationalException {
        try {
            // open FDBRecordStore
            FDBRecordStore recordStore = openFDBRecordStore(txn);
            long lastVersion = 0L;
            try {
                SchemaTemplate lastTemplate = loadSchemaTemplate(txn, dataToWrite.getUniqueId());
                lastVersion = lastTemplate.getVersion();
            } catch (RelationalException ex) {
                if (ex.getErrorCode() != ErrorCode.UNKNOWN_SCHEMA_TEMPLATE) {
                    throw ex;
                }
            }
            // if version is unset (0L), set it to lastVersion + 1
            if (dataToWrite.getVersion() == 0L) {
                SchemaTemplate dataWithUpdatedVersion = new SchemaTemplateDescriptor(dataToWrite.getUniqueId(), dataToWrite.getMetaData(), lastVersion + 1);
                updateSchemaTemplateData(dataWithUpdatedVersion, recordStore);
            } else {
                updateSchemaTemplateData(dataToWrite, recordStore);
            }
            return true;
        } catch (RecordCoreException ex) {
            throw ExceptionUtil.toRelationalException(ex);
        }
    }

    @Override
    public void createDatabase(@Nonnull Transaction txn, URI dbUri) throws RelationalException {
        try {
            FDBRecordStore recordStore = openFDBRecordStore(txn);
            ProtobufDataBuilder pmd = new ProtobufDataBuilder(metaDataProvider.getRecordMetaData().getRecordType(SystemTableRegistry.DATABASE_TABLE_NAME).getDescriptor());
            Message m = pmd.setField("DATABASE_ID", dbUri.getPath()).build();
            recordStore.saveRecord(m);
        } catch (RecordCoreException | SQLException ex) {
            throw ExceptionUtil.toRelationalException(ex);
        }
    }

    @Override
    public RelationalResultSet listDatabases(@Nonnull Transaction txn, @Nonnull Continuation continuation) throws RelationalException {
        FDBRecordStore recordStore = openFDBRecordStore(txn);
        Tuple key = Tuple.from(SystemTableRegistry.DATABASE_INFO_RECORD_TYPE_KEY);
        RecordCursor<FDBStoredRecord<Message>> cursor = recordStore.scanRecords(new TupleRange(key, key, EndpointType.RANGE_INCLUSIVE, EndpointType.RANGE_INCLUSIVE), continuation.getBytes(), ScanProperties.FORWARD_SCAN);
        return new RecordLayerResultSet(dbTableMetaData, RecordLayerIterator.create(cursor, this::transformDatabaseInfo), null /* caller is responsible for managing tx state */);
    }

    @Override
    public RelationalResultSet listSchemas(@Nonnull Transaction txn, @Nonnull Continuation continuation) throws RelationalException {
        FDBRecordStore recordStore = openFDBRecordStore(txn);
        Tuple key = Tuple.from(SystemTableRegistry.SCHEMA_RECORD_TYPE_KEY);
        RecordCursor<FDBStoredRecord<Message>> cursor = recordStore.scanRecords(new TupleRange(key, key, EndpointType.RANGE_INCLUSIVE, EndpointType.RANGE_INCLUSIVE), continuation.getBytes(), ScanProperties.FORWARD_SCAN);
        Descriptors.Descriptor schemaDesc = recordStore.getRecordMetaData().getRecordMetaData().getRecordType(SystemTableRegistry.SCHEMAS_TABLE_NAME).getDescriptor();
        return new RecordLayerResultSet(getMetaData(schemaDesc),
                RecordLayerIterator.create(cursor, this::transformSchema), null /* caller is responsible for managing tx state */);
    }

    @Override
    public RelationalResultSet listSchemas(@Nonnull Transaction txn, @Nonnull URI databaseId, @Nonnull Continuation continuation) throws RelationalException {
        FDBRecordStore recordStore = openFDBRecordStore(txn);
        Tuple key = Tuple.from(SystemTableRegistry.SCHEMA_RECORD_TYPE_KEY, databaseId.getPath());
        RecordCursor<FDBStoredRecord<Message>> cursor = recordStore.scanRecords(new TupleRange(key, key, EndpointType.RANGE_INCLUSIVE, EndpointType.RANGE_INCLUSIVE), continuation.getBytes(), ScanProperties.FORWARD_SCAN);
        Descriptors.Descriptor schemaDesc = recordStore.getRecordMetaData().getRecordMetaData().getRecordType(SystemTableRegistry.SCHEMAS_TABLE_NAME).getDescriptor();
        return new RecordLayerResultSet(getMetaData(schemaDesc), RecordLayerIterator.create(cursor, this::transformSchema), null /* caller is responsible for managing tx state */);
    }

    @Override
    public void deleteSchema(Transaction txn, URI dbUri, String schemaName) throws RelationalException {
        try {
            FDBRecordStore recordStore = openFDBRecordStore(txn);
            Tuple primaryKey = getSchemaKey(dbUri, schemaName);
            Assert.that(recordStore.deleteRecord(primaryKey), "Schema " + dbUri.getPath() + "/" + schemaName + " does not exist", ErrorCode.UNDEFINED_SCHEMA);
        } catch (RecordCoreException rce) {
            throw ExceptionUtil.toRelationalException(rce);
        }
    }

    @Override
    public boolean doesDatabaseExist(Transaction txn, URI databaseId) throws RelationalException {
        FDBRecordStore recordStore = openFDBRecordStore(txn);
        try {
            String dbId = databaseId.getPath();
            return recordStore.loadRecord(Tuple.from(SystemTableRegistry.DATABASE_INFO_RECORD_TYPE_KEY, dbId)) != null;
        } catch (RecordCoreException rce) {
            throw ExceptionUtil.toRelationalException(rce);
        }
    }

    @Override
    public boolean doesSchemaExist(Transaction txn, URI dbUri, String schemaName) throws RelationalException {
        FDBRecordStore recordStore = openFDBRecordStore(txn);
        try {
            Tuple primaryKey = getSchemaKey(dbUri, schemaName);
            return recordStore.loadRecord(primaryKey) != null;
        } catch (RecordCoreException rce) {
            throw ExceptionUtil.toRelationalException(rce);
        }
    }

    @Override
    public void deleteDatabase(Transaction txn, URI dbUrl) throws RelationalException {
        FDBRecordStore recordStore = openFDBRecordStore(txn);
        try {
            String dbId = dbUrl.getPath();
            recordStore.deleteRecord(Tuple.from(SystemTableRegistry.DATABASE_INFO_RECORD_TYPE_KEY, dbId));
        } catch (RecordCoreException rce) {
            throw ExceptionUtil.toRelationalException(rce);
        }

    }

    private FDBRecordStore openFDBRecordStore(@Nonnull Transaction txn) throws RelationalException {
        try {
            return FDBRecordStore.newBuilder().setKeySpacePath(keySpacePath).setContext(txn.unwrap(FDBRecordContext.class)).setMetaDataProvider(metaDataProvider).open();
        } catch (RecordCoreException ex) {
            throw ExceptionUtil.toRelationalException(ex);
        }
    }

    private void updateSchemaData(Schema schema, FDBRecordStore recordStore) throws RelationalException {
        try {
            ProtobufDataBuilder pmd = new ProtobufDataBuilder(metaDataProvider.getRecordMetaData().getRecordType(SystemTableRegistry.SCHEMAS_TABLE_NAME).getDescriptor());
            Message m = pmd.setField("DATABASE_ID", schema.getDatabaseId())
                    .setField("SCHEMA_NAME", schema.getSchemaName())
                    .setField("TEMPLATE_NAME", schema.getSchemaTemplateName())
                    .setField("TEMPLATE_VERSION", schema.getTemplateVersion())
                    .setField("META_DATA", schema.getMetaData().toByteString())
                    .build();
            recordStore.saveRecord(m);
        } catch (RecordCoreException e) {
            if (e.getMessage().contains("Record is too long")) {
                throw new RelationalException("Too many columns in schema", ErrorCode.TOO_MANY_COLUMNS, e);
            } else {
                throw ExceptionUtil.toRelationalException(e);
            }
        } catch (SQLException e) {
            throw ExceptionUtil.toRelationalException(e);
        }
    }

    private void updateSchemaTemplateData(SchemaTemplate schemaTemplate, FDBRecordStore recordStore) throws RelationalException {
        try {
            ProtobufDataBuilder pmd = new ProtobufDataBuilder(metaDataProvider.getRecordMetaData().getRecordType(SystemTableRegistry.SCHEMA_TEMPLATE_TABLE_NAME).getDescriptor());
            Message m = pmd.setField("TEMPLATE_NAME", schemaTemplate.getUniqueId())
                    .setField("TEMPLATE_VERSION", schemaTemplate.getVersion())
                    .setField("META_DATA", schemaTemplate.getMetaData().toByteString())
                    .build();
            recordStore.saveRecord(m);
        } catch (RecordCoreException e) {
            if (e.getMessage().contains("Record is too long")) {
                throw new RelationalException("Too many columns in schema template", ErrorCode.TOO_MANY_COLUMNS, e);
            } else {
                throw ExceptionUtil.toRelationalException(e);
            }
        } catch (SQLException e) {
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
        long version = (Long) m.getField(descriptor.findFieldByName("TEMPLATE_VERSION"));
        ByteString tableDescBytes = (ByteString) m.getField(descriptor.findFieldByName("META_DATA"));

        RecordMetaDataProto.MetaData tableDescriptor;
        try {
            tableDescriptor = RecordMetaDataProto.MetaData.parseFrom(tableDescBytes, EXTENSION_REGISTRY);
        } catch (InvalidProtocolBufferException e) {
            throw new RelationalException("Corrupt Catalog: Message <" + m + "> cannot be parsed into a schema", ErrorCode.INTERNAL_ERROR, e).toUncheckedWrappedException();
        }
        return new ArrayRow(new Object[]{
                dbId,
                schemaName,
                templateName,
                version,
                tableDescriptor
        });
    }

    private Row transformDatabaseInfo(FDBStoredRecord<Message> record) {
        return new MessageTuple(record.getRecord());
    }

    private Schema parseSchemaTable(Message m) throws RelationalException {
        final RecordMetaData recordMetaData = metaDataProvider.getRecordMetaData();
        final RecordType schemaTableMD = recordMetaData.getRecordType(SystemTableRegistry.SCHEMAS_TABLE_NAME);
        final Descriptors.Descriptor descriptor = schemaTableMD.getDescriptor();
        String dbId = (String) m.getField(descriptor.findFieldByName("DATABASE_ID"));
        String schemaName = (String) m.getField(descriptor.findFieldByName("SCHEMA_NAME"));
        String templateName = (String) m.getField(descriptor.findFieldByName("TEMPLATE_NAME"));
        long version = (Long) m.getField(descriptor.findFieldByName("TEMPLATE_VERSION"));
        ByteString tableDescBytes = (ByteString) m.getField(descriptor.findFieldByName("META_DATA"));
        RecordMetaDataProto.MetaData tableDescriptor;
        try {
            tableDescriptor = RecordMetaDataProto.MetaData.parseFrom(tableDescBytes, EXTENSION_REGISTRY);
        } catch (InvalidProtocolBufferException e) {
            throw new RelationalException("Corrupt Catalog: Message <" + m + "> cannot be parsed into a schema", ErrorCode.INTERNAL_ERROR, e);
        }

        return new Schema(dbId, schemaName, tableDescriptor, templateName, version);
    }

    private SchemaTemplate parseSchemaTemplateTable(Message m) throws RelationalException {
        final RecordMetaData recordMetaData = metaDataProvider.getRecordMetaData();
        final RecordType schemaTemplateTable = recordMetaData.getRecordType(SystemTableRegistry.SCHEMA_TEMPLATE_TABLE_NAME);
        final Descriptors.Descriptor descriptor = schemaTemplateTable.getDescriptor();
        String templateName = (String) m.getField(descriptor.findFieldByName("TEMPLATE_NAME"));
        long version = (Long) m.getField(descriptor.findFieldByName("TEMPLATE_VERSION"));
        ByteString tableDescBytes = (ByteString) m.getField(descriptor.findFieldByName("META_DATA"));
        RecordMetaDataProto.MetaData tableDescriptor;
        try {
            tableDescriptor = RecordMetaDataProto.MetaData.parseFrom(tableDescBytes, EXTENSION_REGISTRY);
        } catch (InvalidProtocolBufferException e) {
            throw new RelationalException("Corrupt Catalog: Message <" + m + "> cannot be parsed into a schema template", ErrorCode.INTERNAL_ERROR, e);
        }

        return new SchemaTemplateDescriptor(templateName, tableDescriptor, version);
    }

    private SchemaTemplate getCatalogSchemaTemplate() throws RelationalException {
        TypingContext ctx = TypingContext.create();

        SystemTableRegistry.getSystemTable(SystemTableRegistry.SCHEMAS_TABLE_NAME).addDefinition(ctx);
        SystemTableRegistry.getSystemTable(SystemTableRegistry.DATABASE_TABLE_NAME).addDefinition(ctx);
        SystemTableRegistry.getSystemTable(SystemTableRegistry.SCHEMA_TEMPLATE_TABLE_NAME).addDefinition(ctx);

        ctx.addAllToTypeRepository();
        //TODO(bfines) unfortunate side effect--can we do this differently?
        dbTableMetaData = SqlTypeSupport.typeToMetaData(ctx.getType(SystemTableRegistry.DATABASE_TABLE_NAME));
        return ctx.generateSchemaTemplate("CATALOG_TEMPLATE", 1L);
    }

}
