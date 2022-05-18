/*
 * RecordLayerSchema.java
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

package com.apple.foundationdb.relational.recordlayer;

import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.relational.api.ConnectionScoped;
import com.apple.foundationdb.relational.api.DynamicMessageBuilder;
import com.apple.foundationdb.relational.api.OperationOption;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.ProtobufDataBuilder;
import com.apple.foundationdb.relational.api.catalog.DatabaseSchema;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;

import com.google.protobuf.Descriptors;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;

@NotThreadSafe
@ConnectionScoped
public class RecordLayerSchema implements DatabaseSchema {
    private final AbstractDatabase db;
    //could be accessed through the database, but this seems convenient
    final EmbeddedRelationalConnection conn;
    @Nonnull
    private final String schemaName;

    private final FDBRecordStoreBase.StoreExistenceCheck existenceCheck;

    //TODO(bfines) destroy this when the connection's transaction ends
    private FDBRecordStore currentStore;

    /*
     * Used for reference tracking to make sure that we close all the tables that we open.
     */
    private final Map<String, RecordTypeTable> loadedTables = new HashMap<>();

    public RecordLayerSchema(@Nonnull String schemaName, AbstractDatabase database, EmbeddedRelationalConnection connection, @Nonnull Options options) throws RelationalException {
        this.schemaName = schemaName;
        this.db = database;
        this.conn = connection;
        this.existenceCheck = getExistenceCheckGivenOptions(options);
    }

    @Override
    @Nonnull
    public String getSchemaName() {
        return schemaName;
    }

    @Nonnull public Table loadTable(@Nonnull String tableName, @Nonnull Options options) throws RelationalException {
        //TODO(bfines) load the record type index, rather than just the generic type, then
        // return an index object instead
        RecordTypeTable t = loadedTables.get(tableName.toUpperCase(Locale.ROOT));
        boolean putBack = false;
        if (t == null) {
            t = new RecordTypeTable(this, tableName);
            putBack = true;
        }
        if (putBack) {
            loadedTables.put(tableName.toUpperCase(Locale.ROOT), t);
        }
        return t;
    }

    @Override
    public void close() throws RelationalException {
        currentStore = null;
        for (RecordTypeTable table : loadedTables.values()) {
            table.close();
        }
        loadedTables.clear();
    }

    @Nonnull
    public FDBRecordStore loadStore() throws RelationalException {
        if (!this.conn.inActiveTransaction()) {
            if (this.conn.getAutoCommit()) {
                this.conn.beginTransaction();
            } else {
                throw new RelationalException("cannot load schema without an active transaction",
                        ErrorCode.TRANSACTION_INACTIVE);
            }
        }

        if (currentStore != null) {
            return currentStore;
        }
        currentStore = db.loadRecordStore(schemaName, existenceCheck);
        conn.addCloseListener(() -> {
            currentStore = null;
        });
        return currentStore;
    }

    private FDBRecordStoreBase.StoreExistenceCheck getExistenceCheckGivenOptions(@Nonnull Options options) throws RelationalException {
        final OperationOption.SchemaExistenceCheck existenceCheck = options.getOption(OperationOption.SCHEMA_EXISTENCE_CHECK,
                OperationOption.SchemaExistenceCheck.ERROR_IF_NOT_EXISTS);
        switch (existenceCheck) {
            case NONE:
                return FDBRecordStoreBase.StoreExistenceCheck.NONE;
            case ERROR_IF_NO_INFO_AND_HAS_RECORDS_OR_INDEXES:
                return FDBRecordStoreBase.StoreExistenceCheck.ERROR_IF_NO_INFO_AND_HAS_RECORDS_OR_INDEXES;
            case ERROR_IF_NO_INFO_AND_NOT_EMPTY:
                return FDBRecordStoreBase.StoreExistenceCheck.ERROR_IF_NO_INFO_AND_NOT_EMPTY;
            case ERROR_IF_EXISTS:
                return FDBRecordStoreBase.StoreExistenceCheck.ERROR_IF_EXISTS;
            case ERROR_IF_NOT_EXISTS:
                return FDBRecordStoreBase.StoreExistenceCheck.ERROR_IF_NOT_EXISTS;
            default:
                throw new RelationalException("Invalid StoreExistenceCheck in options: <" + existenceCheck + ">",
                        ErrorCode.INVALID_PARAMETER);
        }
    }

    public DynamicMessageBuilder getDataBuilder(String typeName) throws RelationalException {
        final Descriptors.FileDescriptor recordsDescriptor = loadStore().getRecordMetaData().getRecordsDescriptor();
        for (Descriptors.Descriptor typeDescriptor : recordsDescriptor.getMessageTypes()) {
            if (typeDescriptor.getName().equalsIgnoreCase(typeName)) {
                return new ProtobufDataBuilder(typeDescriptor);
            }
        }
        throw new RelationalException("Unknown type: <" + typeName + ">", ErrorCode.UNKNOWN_TYPE);
    }
}
