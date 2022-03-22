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

import com.apple.foundationdb.record.RecordCoreStorageException;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoredRecord;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpace;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpacePath;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.relational.api.Transaction;
import com.apple.foundationdb.relational.api.catalog.CatalogValidator;
import com.apple.foundationdb.relational.api.catalog.StoreCatalog;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.InternalErrorException;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.api.generated.CatalogData;

import com.google.protobuf.Message;

import java.net.URI;

import javax.annotation.Nonnull;

public class RecordLayerStoreCatalogImpl implements StoreCatalog {
    private final KeySpacePath keySpacePath;
    private final RecordMetaDataBuilder metaDataBuilder;

    public RecordLayerStoreCatalogImpl(KeySpace keySpace) {
        this.keySpacePath = keySpace.path("catalog");
        this.metaDataBuilder = RecordMetaData.newBuilder().setRecords(CatalogData.getDescriptor());
        this.metaDataBuilder.getRecordType("Schema").setPrimaryKey(Key.Expressions.concatenateFields("database_id", "schema_name"));
    }

    @Override
    public CatalogData.Schema loadSchema(@Nonnull Transaction txn, @Nonnull URI databaseId, @Nonnull String schemaName) throws RelationalException {
        // open FDBRecordStore
        FDBRecordStore recordStore = openFDBRecordStore(txn);
        // arbitrarily define primary key as a combination of databaseId and schemaName here
        Tuple primaryKey = Tuple.from(databaseId.toString(), schemaName);
        FDBStoredRecord<Message> record = recordStore.loadRecord(primaryKey);
        if (record == null) {
            throw new RelationalException(String.format("Primary key %s not existed in Catalog!", primaryKey), ErrorCode.SCHEMA_NOT_FOUND);
        }
        return CatalogData.Schema.newBuilder().mergeFrom(record.getRecord()).build();
    }

    @Override
    public boolean updateSchema(@Nonnull Transaction txn, @Nonnull CatalogData.Schema dataToWrite) throws RelationalException {
        CatalogValidator.validateSchema(dataToWrite);
        try {
            // open FDBRecordStore
            FDBRecordStore recordStore = openFDBRecordStore(txn);
            recordStore.saveRecord(dataToWrite);
            return true;
        } catch (InternalErrorException ex) {
            // log error here?
            return false;
        }
    }

    private FDBRecordStore openFDBRecordStore(@Nonnull Transaction txn) throws RelationalException {
        try {
            return FDBRecordStore.newBuilder().setKeySpacePath(keySpacePath).setContext(txn.unwrap(FDBRecordContext.class)).setMetaDataProvider(metaDataBuilder).open();
        } catch (RecordCoreStorageException ex) {
            // there maybe other types of RecordCoreStorageException?
            throw new RelationalException(ErrorCode.TRANSACTION_INACTIVE, ex);
        }
    }
}
