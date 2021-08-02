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

import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.relational.api.ConnectionScoped;
import com.apple.foundationdb.relational.api.OperationOption;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.Table;
import com.apple.foundationdb.relational.api.RelationalException;
import com.apple.foundationdb.relational.api.catalog.DatabaseSchema;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@NotThreadSafe
@ConnectionScoped
public class RecordLayerSchema implements DatabaseSchema {
    private final RecordLayerDatabase db;
    //could be accessed through the database, but this seems convenient
    final RecordStoreConnection conn;
    @Nonnull
    private final String schemaName;

    //TODO(bfines) destroy this when the connection's transaction ends
    private FDBRecordStore currentStore;

    /*
     * Used for reference tracking to make sure that we close all the tables that we open.
     */
    private final Map<String,RecordTypeTable> loadedTables = new HashMap<>();

    public RecordLayerSchema(@Nonnull String schemaName,RecordLayerDatabase recordLayerDatabase,RecordStoreConnection connection) {
        this.schemaName = schemaName;
        this.db = recordLayerDatabase;
        this.conn = connection;
    }

    @Override
    @Nonnull
    public String getSchemaName() {
        return schemaName;
    }

    @Override
    public int getSchemaVersion() {
        FDBRecordStore store = loadStore();
        return store.getUserVersion();
    }

    @Override
    public Set<String> listTables() {
        FDBRecordStore store = loadStore();

        final Map<String, RecordType> recordTypes = store.getRecordMetaData().getRecordTypes();
        return recordTypes.values().stream().map(RecordType::getName).collect(Collectors.toSet());
    }

    @Override
    @Nonnull public Table loadTable(@Nonnull String tableName, @Nonnull Options options) throws RelationalException {
        //TODO(bfines) load the record type index, rather than just the generic type, then
        // return an index object instead
        RecordTypeTable t = loadedTables.get(tableName);
        boolean putBack = false;
        if(t==null){
            t= new RecordTypeTable(this,tableName);
            putBack = true;
        }
        if(options.hasOption(OperationOption.FORCE_VERIFY_DDL)){
            t.validate();
        }
        if(putBack) {
            loadedTables.put(tableName.toUpperCase(Locale.ROOT), t);
        }
        return t;
    }

    @Override
    public void close() throws RelationalException {
        currentStore = null;
        for(RecordTypeTable table : loadedTables.values()){
            table.close();
        }
        loadedTables.clear();
    }

    /* ****************************************************************************************************************/
    /*package-private helper methods*/
    FDBRecordStore loadStore(){
        if(!this.conn.inActiveTransaction()){
            if(this.conn.isAutoCommitEnabled()){
                this.conn.beginTransaction();
            }else{
                throw new RelationalException("cannot load schema without an active transaction",
                        RelationalException.ErrorCode.TRANSACTION_INACTIVE);
            }
        }

        if(currentStore!=null){
            return currentStore;
        }
        currentStore = db.loadRecordStore(schemaName);
        conn.transaction.addTerminationListener(() -> currentStore = null);
        return currentStore;
    }
}
