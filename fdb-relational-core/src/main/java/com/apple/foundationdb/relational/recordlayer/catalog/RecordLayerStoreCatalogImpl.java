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
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.RecordMetaDataProvider;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoredRecord;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpace;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpacePath;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.relational.api.Continuation;
import com.apple.foundationdb.relational.api.Row;
import com.apple.foundationdb.relational.api.Transaction;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.catalog.CatalogValidator;
import com.apple.foundationdb.relational.api.catalog.SchemaTemplate;
import com.apple.foundationdb.relational.api.catalog.StoreCatalog;
import com.apple.foundationdb.relational.api.ddl.DdlListener;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.InternalErrorException;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.api.generated.CatalogData;
import com.apple.foundationdb.relational.recordlayer.KeySpaceUtils;
import com.apple.foundationdb.relational.recordlayer.MessageTuple;
import com.apple.foundationdb.relational.recordlayer.RecordLayerIterator;
import com.apple.foundationdb.relational.recordlayer.RecordLayerResultSet;
import com.apple.foundationdb.relational.recordlayer.catalog.systables.SystemTable;
import com.apple.foundationdb.relational.recordlayer.catalog.systables.SystemTableRegistry;
import com.apple.foundationdb.relational.recordlayer.util.ExceptionUtil;
import com.apple.foundationdb.relational.recordlayer.utils.Assert;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;

import java.net.URI;

import javax.annotation.Nonnull;

/**
 * This class constructs the record store that holds the catalog metadata.
 *
 * We currently have two tables in this catalog: Schema, and DatabaseInfo.
 * You can find the definition of these two tables either in {@code catalog_schema_data.proto}, or
 * in the respective system tables classes {@link com.apple.foundationdb.relational.recordlayer.catalog.systables.SchemaSystemTable},
 * resp. {@link com.apple.foundationdb.relational.recordlayer.catalog.systables.DatabaseInfoSystemTable}.
 *
 * Internally, here is how the is stored:
 *   - We use a prefix "/__SYS/catalog".
 *   - For {@code Schema}, we use '0' (zero) as a record type key.
 *   - For {@code DatabaseInfo}, we use '1' (one) as a record type key.
 * Here is an example on how the records are laid out:
 *
 * __SYS
 *   catalog
 *       {0, /DB1, SCh1} Schema { name='SCh1', tables... }
 *       {0, /__SYS, catalog} Schema { name='catalog', tables= {Schema, DatabaseInfo}}
 *       {1, /DB1}   DatabaseInfo { name=/DB1}
 *       {1, /__SYS} DatabaseInfo {name=__SYS }
 */
public class RecordLayerStoreCatalogImpl implements StoreCatalog {

    public static final String SCHEMA = "catalog";
    public static final String SYS_DB = "/__SYS";
    private final KeySpacePath keySpacePath;

    private final RecordMetaDataProvider metaDataProvider;

    public RecordLayerStoreCatalogImpl(@Nonnull final KeySpace keySpace) throws RelationalException {
        keySpacePath = KeySpaceUtils.uriToPath(URI.create(SYS_DB + "/" + SCHEMA), keySpace);
        metaDataProvider = setupMetadataProvider();
    }

    @Nonnull
    private RecordMetaDataProvider setupMetadataProvider() {
        final RecordMetaDataBuilder builder = RecordMetaData.newBuilder();
        builder.setRecords(CatalogData.getDescriptor());
        SystemTableRegistry.getAllTables().forEach(table ->
                builder.getRecordType(table.getName()).setRecordTypeKey(table.getRecordTypeKey()).setPrimaryKey(table.getPrimaryKeyDefinition()));
        return builder.build();
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
        try {
            loadSchema(transaction, URI.create(SYS_DB), SCHEMA);
        } catch (RelationalException ve) {
            if (ve.getErrorCode() != ErrorCode.SCHEMA_NOT_FOUND) {
                return;
            }
        }

        // inform the catalog about the tables under the /__SYS/catalog schema.
        final CatalogData.Schema schema = getSysCatalogSchema();
        updateSchema(transaction, schema);
    }

    /**
     * Retrieves the schema of the system catalog.
     * @return The schema of the system catalog.
     * @throws RelationalException in case of schema parsing error.
     */
    @Nonnull
    private static CatalogData.Schema getSysCatalogSchema() throws RelationalException {
        final DdlListener.SchemaTemplateBuilder builder = new DdlListener.SchemaTemplateBuilder("catalog_template");
        for (final SystemTable table : SystemTableRegistry.getAllTables()) {
            builder.registerTable(table.getName(), table.getDefinition(builder));
        }
        final SchemaTemplate schemaTemplate = builder.build();
        //map the schema to the template
        return schemaTemplate.generateSchema(SYS_DB, SCHEMA);
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
    public CatalogData.Schema loadSchema(@Nonnull Transaction txn, @Nonnull URI databaseId, @Nonnull String schemaName) throws RelationalException {
        final FDBRecordStore recordStore = openFDBRecordStore(txn);
        Assert.notNull(recordStore);
        final Tuple primaryKey = Tuple.from(SystemTableRegistry.SCHEMA_RECORD_TYPE_KEY, databaseId.toString(), schemaName);
        try {
            final FDBStoredRecord<Message> record = recordStore.loadRecord(primaryKey);
            Assert.notNull(record, String.format("Primary key %s not existed in Catalog!", primaryKey), ErrorCode.SCHEMA_NOT_FOUND);
            return CatalogData.Schema.newBuilder().mergeFrom(record.getRecord()).build();
        } catch (RecordCoreException ex) {
            throw ExceptionUtil.toRelationalException(ex);
        }
    }

    @Override
    public boolean updateSchema(@Nonnull Transaction txn, @Nonnull CatalogData.Schema dataToWrite) throws RelationalException {
        CatalogValidator.validateSchema(dataToWrite);
        try {
            // open FDBRecordStore
            FDBRecordStore recordStore = openFDBRecordStore(txn);
            updateDatabaseInfo(dataToWrite, recordStore);
            recordStore.saveRecord(dataToWrite);
            return true;
        } catch (InternalErrorException ex) {
            // log error here?
            return false;
        } catch (RecordCoreException ex) {
            throw ExceptionUtil.toRelationalException(ex);
        }
    }

    @Override
    public RelationalResultSet listDatabases(@Nonnull Transaction txn, @Nonnull Continuation continuation) throws RelationalException {
        // RecordQuery query = RecordQuery.newBuilder().setRecordType("DatabaseInfo").build();
        FDBRecordStore recordStore = openFDBRecordStore(txn);
        Tuple key = Tuple.from(SystemTableRegistry.DATABASE_INFO_RECORD_TYPE_KEY);
        RecordCursor<FDBStoredRecord<Message>> cursor = recordStore.scanRecords(new TupleRange(key, key, EndpointType.RANGE_INCLUSIVE, EndpointType.RANGE_INCLUSIVE), continuation.getBytes(), ScanProperties.FORWARD_SCAN);
        return new RecordLayerResultSet(getFieldNames(CatalogData.DatabaseInfo.getDescriptor()), RecordLayerIterator.create(cursor, this::transformDatabaseInfo), null /* caller is responsible for managing tx state */);
    }

    @Override
    public RelationalResultSet listSchemas(@Nonnull Transaction txn, @Nonnull Continuation continuation) throws RelationalException {
        FDBRecordStore recordStore = openFDBRecordStore(txn);
        Tuple key = Tuple.from(SystemTableRegistry.SCHEMA_RECORD_TYPE_KEY);
        RecordCursor<FDBStoredRecord<Message>> cursor = recordStore.scanRecords(new TupleRange(key, key, EndpointType.RANGE_INCLUSIVE, EndpointType.RANGE_INCLUSIVE), continuation.getBytes(), ScanProperties.FORWARD_SCAN);
        return new RecordLayerResultSet(getFieldNames(CatalogData.Schema.getDescriptor()), RecordLayerIterator.create(cursor, this::transformSchema), null /* caller is responsible for managing tx state */);
    }

    @Override
    public RelationalResultSet listSchemas(@Nonnull Transaction txn, @Nonnull URI databaseId, @Nonnull Continuation continuation) throws RelationalException {
        FDBRecordStore recordStore = openFDBRecordStore(txn);
        Tuple key = Tuple.from(SystemTableRegistry.SCHEMA_RECORD_TYPE_KEY, databaseId.getPath());
        RecordCursor<FDBStoredRecord<Message>> cursor = recordStore.scanRecords(new TupleRange(key, key, EndpointType.RANGE_INCLUSIVE, EndpointType.RANGE_INCLUSIVE), continuation.getBytes(), ScanProperties.FORWARD_SCAN);
        return new RecordLayerResultSet(getFieldNames(CatalogData.Schema.getDescriptor()), RecordLayerIterator.create(cursor, this::transformSchema), null /* caller is responsible for managing tx state */);
    }

    private Row transformSchema(FDBStoredRecord<Message> record) {
        CatalogData.Schema schema = CatalogData.Schema.newBuilder().mergeFrom(record.getRecord()).build();
        return new MessageTuple(schema);
    }

    private Row transformDatabaseInfo(FDBStoredRecord<Message> record) {
        CatalogData.DatabaseInfo databaseInfo = CatalogData.DatabaseInfo.newBuilder().mergeFrom(record.getRecord()).build();
        return new MessageTuple(databaseInfo);
    }

    @Override
    public void deleteSchema(Transaction txn, URI dbUri, String schemaName) throws RelationalException {
        try {
            FDBRecordStore recordStore = openFDBRecordStore(txn);
            Tuple primaryKey = getSchemaKey(dbUri, schemaName);
            Assert.that(recordStore.deleteRecord(primaryKey), "Schema " + dbUri.getPath() + "/" + schemaName + " does not exist", ErrorCode.SCHEMA_NOT_FOUND);
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
        } catch (RecordCoreStorageException ex) {
            // there maybe other types of RecordCoreStorageException?
            throw new RelationalException(ErrorCode.TRANSACTION_INACTIVE, ex);
        } catch (RecordCoreException ex) {
            throw ExceptionUtil.toRelationalException(ex);
        }
    }

    private void updateDatabaseInfo(CatalogData.Schema schema, FDBRecordStore recordStore) {
        CatalogData.DatabaseInfo databaseInfo = CatalogData.DatabaseInfo.newBuilder().setDatabaseId(schema.getDatabaseId()).build();
        recordStore.saveRecord(databaseInfo);
    }

    private String[] getFieldNames(Descriptors.Descriptor descriptor) {
        return descriptor.getFields().stream().map(Descriptors.FieldDescriptor::getName).toArray(String[]::new);
    }

    @Nonnull
    private Tuple getSchemaKey(@Nonnull URI databaseId, @Nonnull String schemaName) {
        return Tuple.from(SystemTableRegistry.SCHEMA_RECORD_TYPE_KEY, databaseId.getPath(), schemaName);
    }

}
