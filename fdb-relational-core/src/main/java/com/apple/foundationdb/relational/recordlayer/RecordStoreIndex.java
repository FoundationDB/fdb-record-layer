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
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorIterator;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.metadata.expressions.RecordTypeKeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBIndexedRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.IndexOrphanBehavior;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.relational.api.Continuation;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.Row;
import com.apple.foundationdb.relational.api.SqlTypeSupport;
import com.apple.foundationdb.relational.api.StructMetaData;
import com.apple.foundationdb.relational.api.Transaction;
import com.apple.foundationdb.relational.api.ddl.ProtobufDdlUtil;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.recordlayer.util.ExceptionUtil;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class RecordStoreIndex extends RecordTypeScannable<IndexEntry> implements Index {
    private final com.apple.foundationdb.record.metadata.Index index;
    private final RecordTypeTable table;

    public RecordStoreIndex(com.apple.foundationdb.record.metadata.Index index,
                            RecordTypeTable table) {
        this.index = index;
        this.table = table;
    }

    @Nonnull
    @Override
    public String getName() {
        return index.getName();
    }

    @Override
    public StructMetaData getMetaData() throws RelationalException {
        final KeyExpression indexStruct = index.getRootExpression();
        RecordType recType = table.loadRecordType(Options.NONE);
        final List<Descriptors.FieldDescriptor> fields = indexStruct.validate(recType.getDescriptor());
        Type.Record record = ProtobufDdlUtil.recordFromFieldDescriptors(fields);
        /*
         * If the index include a record type key, then this logic doesn't work--the returned tuples
         * are off by one (wherever the record type key is in the key order). So we have to deal with that situation.
         */
        if (indexStruct.hasRecordTypeKey()) {
            int typeKeyPos = -1;
            final List<KeyExpression> keyExpressions = indexStruct.normalizeKeyForPositions();
            int p = 0;
            for (KeyExpression ke : keyExpressions) {
                if (ke instanceof RecordTypeKeyExpression) {
                    typeKeyPos = p;
                    break;
                }
                p++;
            }
            Type.Record.Field typeKeyField = Type.Record.Field.of(Type.primitiveType(Type.TypeCode.LONG, false), Optional.of("__TYPE_KEY"), Optional.of(p + 1));
            List<Type.Record.Field> recFields = new ArrayList<>(record.getFields());
            if (typeKeyPos >= recFields.size()) {
                recFields.add(typeKeyField);
            } else {
                recFields.add(typeKeyPos, typeKeyField);
            }
            record = Type.Record.fromFields(recFields);
        }
        return SqlTypeSupport.recordToMetaData(record);
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
    public Row get(@Nonnull Transaction t, @Nonnull Row key, @Nonnull Options options) throws RelationalException {
        FDBRecordStore store = getSchema().loadStore();
        ScanProperties scanProperties = QueryPropertiesUtils.getScanProperties(options);
        scanProperties = new ScanProperties(scanProperties.getExecuteProperties().setReturnedRowLimit(1), scanProperties.isReverse());
        try {
            final RecordCursorIterator<IndexEntry> indexEntryRecordCursor = store.scanIndex(index, IndexScanType.BY_VALUE, TupleRange.allOf(TupleUtils.toFDBTuple(key)), null, scanProperties).asIterator();
            IndexEntry entry;
            if (!indexEntryRecordCursor.hasNext()) {
                return null;
            }
            entry = indexEntryRecordCursor.next();

            //TODO(bfines) pull the orphan behavior from the options
            final CompletableFuture<FDBIndexedRecord<Message>> indexRecord = store.loadIndexEntryRecord(entry, IndexOrphanBehavior.ERROR);
            //TODO(bfines): add in store timing
            final FDBIndexedRecord<Message> storedRecord = t.unwrap(FDBRecordContext.class).asyncToSync(FDBStoreTimer.Waits.WAIT_LOAD_RECORD, indexRecord);
            if (storedRecord == null) {
                return null;
            }
            return new MessageTuple(storedRecord.getRecord());
        } catch (RecordCoreException ex) {
            throw ExceptionUtil.toRelationalException(ex);
        }
    }

    @Override
    public KeyBuilder getKeyBuilder() throws RelationalException {
        return new KeyBuilder(table.loadRecordType(Options.NONE), index.getRootExpression(), "index: <" + index.getName() + ">");
    }

    @Override
    protected RecordLayerSchema getSchema() {
        return table.getSchema();
    }

    @Override
    protected RecordCursor<IndexEntry> openScan(FDBRecordStore store, TupleRange range,
                                                @Nullable Continuation continuation, Options options) throws RelationalException {
        //TODO(bfines) get scan type from Options and/or ScanProperties
        assert continuation == null || continuation instanceof ContinuationImpl;
        try {
            return store.scanIndex(index, IndexScanType.BY_VALUE, range,
                    continuation == null ? null : continuation.getBytes(), QueryPropertiesUtils.getScanProperties(options));
        } catch (RecordCoreException ex) {
            throw ExceptionUtil.toRelationalException(ex);
        }
    }

    @Override
    protected Function<IndexEntry, Row> keyValueTransform() {
        return indexEntry -> new ImmutableKeyValue(TupleUtils.toRelationalTuple(indexEntry.getKey()), TupleUtils.toRelationalTuple(indexEntry.getValue()));
    }

    @Override
    protected boolean supportsMessageParsing() {
        return false;
    }

    @Override
    protected boolean hasConstantValueForPrimaryKey(Options options) {
        return index.getRootExpression() instanceof RecordTypeKeyExpression;
    }
}
