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
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.metadata.expressions.RecordTypeKeyExpression;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.relational.api.Continuation;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.Row;
import com.apple.foundationdb.relational.api.SqlTypeSupport;
import com.apple.foundationdb.relational.api.StructMetaData;
import com.apple.foundationdb.relational.api.Transaction;
import com.apple.foundationdb.relational.api.ddl.ProtobufDdlUtil;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.recordlayer.storage.BackingStore;

import com.google.protobuf.Descriptors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.function.Function;

public class RecordStoreIndex extends RecordTypeScannable<IndexEntry> implements Index {
    private final com.apple.foundationdb.record.metadata.Index index;
    private final RecordTypeTable table;

    public RecordStoreIndex(com.apple.foundationdb.record.metadata.Index index,
                            RecordTypeTable table) {
        this.index = index;
        this.table = table;
    }

    @Override
    public void validate(Options scanOptions) throws RelationalException {
        table.loadRecordType(scanOptions);
    }

    @Nonnull
    @Override
    public String getName() {
        return index.getName();
    }

    @Override
    public StructMetaData getMetaData() throws RelationalException {
        final KeyExpression indexStruct = index.getRootExpression();
        final RecordType recType = table.loadRecordType(Options.NONE);
        final List<Descriptors.FieldDescriptor> fields = indexStruct.validate(recType.getDescriptor());
        final Type.Record record = ProtobufDdlUtil.recordFromFieldDescriptors(fields);
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
        BackingStore store = getSchema().loadStore();
        return store.getFromIndex(index, key, options);
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
    protected RecordCursor<IndexEntry> openScan(BackingStore store, TupleRange range,
                                                @Nullable Continuation continuation, Options options) throws RelationalException {
        return store.scanIndex(index, range, continuation, options);
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
