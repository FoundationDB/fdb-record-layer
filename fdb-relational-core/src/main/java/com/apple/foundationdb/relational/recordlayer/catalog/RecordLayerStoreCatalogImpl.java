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
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoredRecord;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpace;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpacePath;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.relational.api.Continuation;
import com.apple.foundationdb.relational.api.QueryProperties;
import com.apple.foundationdb.relational.api.Row;
import com.apple.foundationdb.relational.api.Transaction;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.catalog.CatalogValidator;
import com.apple.foundationdb.relational.api.catalog.StoreCatalog;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.InternalErrorException;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.api.generated.CatalogData;
import com.apple.foundationdb.relational.recordlayer.MessageTuple;
import com.apple.foundationdb.relational.recordlayer.RecordContextTransaction;
import com.apple.foundationdb.relational.recordlayer.RecordLayerIterator;
import com.apple.foundationdb.relational.recordlayer.RecordLayerResultSet;
import com.apple.foundationdb.relational.recordlayer.Scannable;
import com.apple.foundationdb.relational.recordlayer.SuppliedScannable;
import com.apple.foundationdb.relational.recordlayer.util.ExceptionUtil;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;

import java.net.URI;

import javax.annotation.Nonnull;

public class RecordLayerStoreCatalogImpl implements StoreCatalog {
    private final KeySpacePath keySpacePath;
    private final RecordMetaDataBuilder metaDataBuilder;

    public RecordLayerStoreCatalogImpl(KeySpace keySpace) {
        this.keySpacePath = keySpace.path("catalog");
        this.metaDataBuilder = RecordMetaData.newBuilder().setRecords(CatalogData.getDescriptor());
        this.metaDataBuilder.getRecordType("Schema").setRecordTypeKey("Schema").setPrimaryKey(Key.Expressions.concat(Key.Expressions.recordType(), Key.Expressions.concatenateFields("database_id", "schema_name")));
        this.metaDataBuilder.getRecordType("DatabaseInfo").setRecordTypeKey("DatabaseInfo").setPrimaryKey(Key.Expressions.concat(Key.Expressions.recordType(), Key.Expressions.field("database_id")));
    }

    @Override
    public CatalogData.Schema loadSchema(@Nonnull Transaction txn, @Nonnull URI databaseId, @Nonnull String schemaName) throws RelationalException {
        // open FDBRecordStore
        FDBRecordStore recordStore = openFDBRecordStore(txn);
        Tuple primaryKey = Tuple.from("Schema", databaseId.toString(), schemaName);
        try {
            FDBStoredRecord<Message> record = recordStore.loadRecord(primaryKey);
            if (record == null) {
                throw new RelationalException(String.format("Primary key %s not existed in Catalog!", primaryKey), ErrorCode.SCHEMA_NOT_FOUND);
            }
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
        Tuple key = Tuple.from("DatabaseInfo");
        RecordCursor<FDBStoredRecord<Message>> cursor = recordStore.scanRecords(new TupleRange(key, key, EndpointType.RANGE_INCLUSIVE, EndpointType.RANGE_INCLUSIVE), continuation.getBytes(), ScanProperties.FORWARD_SCAN);
        Scannable scannable = new SuppliedScannable(() -> RecordLayerIterator.create(cursor, this::transformDatabaseInfo), new String[]{"a"}, getFieldNames(CatalogData.DatabaseInfo.getDescriptor()));
        return new RecordLayerResultSet(scannable, null, null, new RecordContextTransaction(txn.unwrap(FDBRecordContext.class)), QueryProperties.DEFAULT, continuation);
    }

    @Override
    public RelationalResultSet listSchemas(@Nonnull Transaction txn, @Nonnull Continuation continuation) throws RelationalException {
        FDBRecordStore recordStore = openFDBRecordStore(txn);
        Tuple key = Tuple.from("Schema");
        RecordCursor<FDBStoredRecord<Message>> cursor = recordStore.scanRecords(new TupleRange(key, key, EndpointType.RANGE_INCLUSIVE, EndpointType.RANGE_INCLUSIVE), continuation.getBytes(), ScanProperties.FORWARD_SCAN);
        Scannable scannable = new SuppliedScannable(() -> RecordLayerIterator.create(cursor, this::transformSchema), new String[]{"a"}, getFieldNames(CatalogData.Schema.getDescriptor()));
        return new RecordLayerResultSet(scannable, null, null, new RecordContextTransaction(txn.unwrap(FDBRecordContext.class)), QueryProperties.DEFAULT, continuation);
    }

    @Override
    public RelationalResultSet listSchemas(@Nonnull Transaction txn, @Nonnull URI databaseId, @Nonnull Continuation continuation) throws RelationalException {
        FDBRecordStore recordStore = openFDBRecordStore(txn);
        Tuple key = Tuple.from("Schema", databaseId.getPath());
        RecordCursor<FDBStoredRecord<Message>> cursor = recordStore.scanRecords(new TupleRange(key, key, EndpointType.RANGE_INCLUSIVE, EndpointType.RANGE_INCLUSIVE), continuation.getBytes(), ScanProperties.FORWARD_SCAN);
        Scannable scannable = new SuppliedScannable(() -> RecordLayerIterator.create(cursor, this::transformSchema), new String[]{"a"}, getFieldNames(CatalogData.Schema.getDescriptor()));
        return new RecordLayerResultSet(scannable, null, null, new RecordContextTransaction(txn.unwrap(FDBRecordContext.class)), QueryProperties.DEFAULT, continuation);
    }

    private Row transformSchema(FDBStoredRecord<Message> record) {
        CatalogData.Schema schema = CatalogData.Schema.newBuilder().mergeFrom(record.getRecord()).build();
        return new MessageTuple(schema);
    }

    private Row transformDatabaseInfo(FDBStoredRecord<Message> record) {
        CatalogData.DatabaseInfo databaseInfo = CatalogData.DatabaseInfo.newBuilder().mergeFrom(record.getRecord()).build();
        return new MessageTuple(databaseInfo);
    }

    private FDBRecordStore openFDBRecordStore(@Nonnull Transaction txn) throws RelationalException {
        try {
            return FDBRecordStore.newBuilder().setKeySpacePath(keySpacePath).setContext(txn.unwrap(FDBRecordContext.class)).setMetaDataProvider(metaDataBuilder).open();
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

}
