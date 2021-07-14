/*
 * RecordTypeTable.java
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

import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.metadata.MetaDataException;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoredRecord;
import com.apple.foundationdb.relational.api.ImmutableKeyValue;
import com.apple.foundationdb.relational.api.Index;
import com.apple.foundationdb.relational.api.KeyValue;
import com.apple.foundationdb.relational.api.NestableTuple;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.Scanner;
import com.apple.foundationdb.relational.api.Table;
import com.apple.foundationdb.relational.api.Transaction;
import com.apple.foundationdb.relational.api.RelationalException;
import com.apple.foundationdb.relational.api.catalog.DatabaseSchema;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * A table implementation based on a specific record type.
 */
public class RecordTypeTable implements Table {
    private final RecordLayerSchema schema;
    private final String tableName;
    private final RecordLayerDatabase recordLayerDatabase;
    private final RecordStoreConnection conn;

    private RecordType currentTypeRef;
    public RecordTypeTable(RecordLayerSchema schema,
                           String tableName,
                           RecordLayerDatabase recordLayerDatabase) {
        this.schema = schema;
        this.tableName = tableName;
        this.recordLayerDatabase = recordLayerDatabase;
        this.conn = schema.conn;
    }

    @Override
    public DatabaseSchema getSchema() {
        return schema;
    }

    @Override
    public Scanner<KeyValue> openScan(@Nonnull Transaction t,
                                      @Nullable NestableTuple startKey,
                                      @Nullable NestableTuple endKey,
                                      @Nonnull Options scanOptions) throws RelationalException {

        //TODO(bfines) this will need to be rewired to support continuations
        TupleRange range;
        if(startKey==null){
            if(endKey==null){
                range = TupleRange.ALL; //this is almost certainly incorrect
            }else{
                range = TupleRange.between(null,TupleUtils.toFDBTuple(endKey));
            }
        }else if(endKey==null){
            range = TupleRange.between(TupleUtils.toFDBTuple(startKey),null);
        }else {
            range = TupleRange.between(TupleUtils.toFDBTuple(startKey),TupleUtils.toFDBTuple(endKey));
        }

        FDBRecordStore store = schema.loadStore();
        RecordType type = loadRecordType();
        //TODO(bfines) get the type index for this
        ScanProperties props = optionsToProperties(scanOptions);
        final RecordCursor<FDBStoredRecord<Message>> cursor = store.scanRecords(range, null, props);
        return CursorScanner.create(cursor, record -> new ImmutableKeyValue(new EmptyTuple(), new ValueTuple(record.getRecord())),true);
    }

    private ScanProperties optionsToProperties(Options scanOptions) {
        //TODO(bfines) implement fully
        return new ScanProperties(ExecuteProperties.newBuilder().build());
    }

    @Override
    public String[] getFieldNames() {
        RecordType type = loadRecordType();
        final Descriptors.Descriptor descriptor = type.getDescriptor();
        return descriptor.getFields().stream().map(Descriptors.FieldDescriptor::getName).toArray(String[]::new);
    }

    @Override
    public String[] getKeyFieldNames() {
        RecordType type = loadRecordType();
        final Descriptors.Descriptor descriptor = type.getDescriptor();
        return type.getPrimaryKey().validate(descriptor).stream().map(Descriptors.FieldDescriptor::getName).toArray(String[]::new);
    }

    @Override
    public boolean deleteRecord(@Nonnull NestableTuple key) throws RelationalException {
        FDBRecordStore store = schema.loadStore();
        return store.deleteRecord(TupleUtils.toFDBTuple(key));
    }

    @Override
    public boolean insertRecord(@Nonnull Message message) throws RelationalException {
        FDBRecordStore store = schema.loadStore();
        //TODO(bfines) maybe this should return something other than boolean?
        store.insertRecord(message);
        return true;
    }

    @Override
    public Set<Index> getAvailableIndexes() {
        return loadRecordType().getIndexes().stream().map((Function<com.apple.foundationdb.record.metadata.Index, Index>) index -> new RecordStoreIndex(index,conn)).collect(Collectors.toSet());
    }

    @Override
    public void close() throws RelationalException {
        currentTypeRef = null;
    }

    void validate() {
        if(!this.conn.inActiveTransaction()){
            this.conn.beginTransaction();
            try{
                loadRecordType();
            } finally{
                this.conn.rollback();
            }
        }else{
            loadRecordType();
        }
    }

    @Override
    public String[] getPrimaryKeys() {
        RecordType type = loadRecordType();
        return type.getPrimaryKey().validate(type.getDescriptor()).stream().map(Descriptors.FieldDescriptor::getName).toArray(String[]::new);
    }

    RecordType loadRecordType() {
        if(currentTypeRef==null) {
            FDBRecordStore store = schema.loadStore();
            try {
                //just try to load the store, and see if it fails. If it fails, it's not there
                currentTypeRef = store.getRecordMetaData().getRecordType(tableName);
                //make sure to clear our state if the transaction ends
                this.conn.transaction.addTerminationListener(() -> currentTypeRef = null);
            } catch (MetaDataException mde) {
                throw new RelationalException(mde.getMessage(), RelationalException.ErrorCode.UNKNOWN_SCHEMA);
            }
        }
        return currentTypeRef;
    }
}
