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

import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorIterator;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.metadata.expressions.FieldKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyWithValueExpression;
import com.apple.foundationdb.record.metadata.expressions.RecordTypeKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.ThenKeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBIndexedRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.IndexOrphanBehavior;
import com.apple.foundationdb.relational.api.ImmutableKeyValue;
import com.apple.foundationdb.relational.api.KeyValue;
import com.apple.foundationdb.relational.api.NestableTuple;
import com.apple.foundationdb.relational.api.QueryProperties;
import com.apple.foundationdb.relational.api.Transaction;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

public class RecordStoreIndex extends RecordTypeScannable<IndexEntry> implements Index {
    private final com.apple.foundationdb.record.metadata.Index index;
    private final RecordStoreConnection conn;
    private final RecordTypeTable table;

    public RecordStoreIndex(com.apple.foundationdb.record.metadata.Index index, RecordTypeTable table, RecordStoreConnection conn) {
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
    public KeyValue get(@Nonnull Transaction t, @Nonnull NestableTuple key, @Nonnull QueryProperties queryProperties) throws RelationalException {
        FDBRecordStore store = getSchema().loadStore();
        final ScanProperties scanProperties = QueryPropertiesUtils.getScanProperties(queryProperties);
        scanProperties.getExecuteProperties().setReturnedRowLimit(1);
        final RecordCursorIterator<IndexEntry> indexEntryRecordCursor = store.scanIndex(index, IndexScanType.BY_VALUE, TupleRange.allOf(TupleUtils.toFDBTuple(key)), null, scanProperties).asIterator();
        IndexEntry entry;
        if (!indexEntryRecordCursor.hasNext()) {
            return null;
        }
        entry = indexEntryRecordCursor.next();

        //TODO(bfines) pull the orphan behavior from the options
        final CompletableFuture<FDBIndexedRecord<Message>> indexRecord = store.loadIndexEntryRecord(entry, IndexOrphanBehavior.ERROR);
        //TODO(bfines): add in store timing
        final FDBIndexedRecord<Message> storedRecord = t.unwrap(FDBRecordContext.class).asyncToSync(null, indexRecord);
        if (storedRecord == null) {
            return null;
        }
        return new ImmutableKeyValue(TupleUtils.toRelationalTuple(storedRecord.getPrimaryKey()), new MessageTuple(storedRecord.getRecord()));
    }

    @Override
    public KeyBuilder getKeyBuilder() {
        return new KeyBuilder(table.loadRecordType(), index.getRootExpression(), "index: <" + index.getName() + ">");
    }

    @Override
    public String[] getFieldNames() {
        KeyExpression re = index.getRootExpression();
        if (re instanceof KeyWithValueExpression) {
            KeyWithValueExpression kve = (KeyWithValueExpression)re;
            final List<KeyExpression> keyExpressions = kve.normalizeKeyForPositions();
            String[] fields = new String[keyExpressions.size()];
            int pos = 0;
            for(KeyExpression ke: keyExpressions){
                if(ke instanceof FieldKeyExpression){
                    fields[pos] = ((FieldKeyExpression)ke).getFieldName();
                }
                pos++;
            }
            return fields;
        }else{
            return getKeyFieldNames();
        }
    }

    @Override
    public String[] getKeyFieldNames() {
        KeyExpression rootExpression = index.getRootExpression();
        return getFields(rootExpression);
    }

    private String[] getFields(KeyExpression expression){
        Descriptors.Descriptor descriptor = table.loadRecordType().getDescriptor();
        if(expression instanceof KeyWithValueExpression){
            expression = ((KeyWithValueExpression)expression).getKeyExpression();
        }

        if(expression instanceof ThenKeyExpression) {
            String[] fields = new String[expression.getColumnSize()];
            //TODO(bfines) deal with more complicated KeyExpressions also
            List<KeyExpression> children = ((ThenKeyExpression) expression).getChildren();
            int pos = 0;
            for (KeyExpression ke : children) {
                List<Descriptors.FieldDescriptor> childDescriptors = ke.validate(descriptor);
                if (childDescriptors.isEmpty()) {
                    pos++;
                    continue; //it doesn't actually have a field
                }
                for (Descriptors.FieldDescriptor childDescriptor : childDescriptors) {
                    fields[pos] = childDescriptor.getName();
                    pos++;
                }
            }
            return fields;
        }else{
            final List<Descriptors.FieldDescriptor> indexedFields = expression.validate(descriptor);
            return indexedFields.stream().map(Descriptors.FieldDescriptor::getName).toArray(String[]::new);
        }
    }

    @Override
    protected RecordLayerSchema getSchema() {
        return (RecordLayerSchema) table.getSchema();
    }

    @Override
    protected RecordCursor<IndexEntry> openScan(FDBRecordStore store, TupleRange range, ScanProperties props) {
        //TODO(bfines) get scan type from Options and/or ScanProperties
        return store.scanIndex(index, IndexScanType.BY_VALUE, range, null, props);
    }

    @Override
    protected Function<IndexEntry, KeyValue> keyValueTransform() {
        return indexEntry -> new ImmutableKeyValue(TupleUtils.toRelationalTuple(indexEntry.getKey()), TupleUtils.toRelationalTuple(indexEntry.getValue()));
    }

    @Override
    protected boolean supportsMessageParsing() {
        return false;
    }

    @Override
    protected boolean hasConstantValueForPrimaryKey() {
        return index.getRootExpression() instanceof RecordTypeKeyExpression;
    }
}
