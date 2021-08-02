/*
 * RecordStoreIndex.java
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
import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorIterator;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.record.provider.foundationdb.FDBIndexedRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.IndexOrphanBehavior;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.relational.api.ImmutableKeyValue;
import com.apple.foundationdb.relational.api.Index;
import com.apple.foundationdb.relational.api.KeyValue;
import com.apple.foundationdb.relational.api.NestableTuple;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.Scanner;
import com.apple.foundationdb.relational.api.Table;
import com.apple.foundationdb.relational.api.Transaction;
import com.apple.foundationdb.relational.api.RelationalException;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

public class RecordStoreIndex extends RecordTypeScannable<IndexEntry> implements Index {
    private final com.apple.foundationdb.record.metadata.Index index;
    private final RecordStoreConnection conn;
    private final RecordTypeTable table;

    public RecordStoreIndex(com.apple.foundationdb.record.metadata.Index index, RecordTypeTable table,RecordStoreConnection conn) {
        this.index = index;
        this.conn = conn;
        this.table = table;
    }

    @Nonnull
    @Override
    public String getName() {
        return index.getName();
    }

    @Override
    public Table getTable() {
        return table;
    }

    @Override
    public void close() throws RelationalException {
        //TODO(bfines) implement
    }

    @Override
    public String[] getKeyNames() {
        final Descriptors.Descriptor descriptor = table.loadRecordType().getDescriptor();
        return index.getRootExpression().validate(descriptor).stream().map(Descriptors.FieldDescriptor::getName).toArray(String[]::new);
    }

    @Override
    public KeyValue get(@Nonnull Transaction t, @Nonnull NestableTuple key, @Nonnull Options scanOptions) throws RelationalException {
        FDBRecordStore store = getSchema().loadStore();
        final ScanProperties scanProperties = optionsToProperties(scanOptions);
        scanProperties.getExecuteProperties().setReturnedRowLimit(1);
        final RecordCursorIterator<IndexEntry> indexEntryRecordCursor = store.scanIndex(index, IndexScanType.BY_VALUE, TupleRange.allOf(TupleUtils.toFDBTuple(key)), null, scanProperties).asIterator();
        IndexEntry entry;
        if(!indexEntryRecordCursor.hasNext()){
            return null;
        }
        entry = indexEntryRecordCursor.next();

        //TODO(bfines) pull the orphan behavior from the options
        final CompletableFuture<FDBIndexedRecord<Message>> indexRecord = store.loadIndexEntryRecord(entry, IndexOrphanBehavior.ERROR);
        //TODO(bfines): add in store timing
        final FDBIndexedRecord<Message> storedRecord = t.unwrap(FDBRecordContext.class).asyncToSync(null, indexRecord);
        if(storedRecord==null){
            return null;
        }
        return new ImmutableKeyValue(TupleUtils.toRelationalTuple(storedRecord.getPrimaryKey()), new MessageTuple(storedRecord.getRecord()));
    }

    @Override
    public String[] getFieldNames() {
        //TODO(bfines) this probably isn't quite right
        return getKeyNames();
    }

    @Override
    public String[] getKeyFieldNames() {
        return getKeyNames();
    }

    @Override
    protected RecordLayerSchema getSchema() {
        return (RecordLayerSchema)table.getSchema();
    }

    @Override
    protected RecordCursor<IndexEntry> openScan(FDBRecordStore store, TupleRange range, ScanProperties props) {
        //TODO(bfines) get scan type from Options and/or ScanProperties
        return store.scanIndex(index,IndexScanType.BY_VALUE,range,null,props);
    }

    @Override
    protected Function<IndexEntry, KeyValue> keyValueTransform() {
        return indexEntry -> new ImmutableKeyValue(TupleUtils.toRelationalTuple(indexEntry.getKey()),TupleUtils.toRelationalTuple(indexEntry.getValue()));
    }

    @Override
    protected boolean supportsMessageParsing() {
        return false;
    }
}
