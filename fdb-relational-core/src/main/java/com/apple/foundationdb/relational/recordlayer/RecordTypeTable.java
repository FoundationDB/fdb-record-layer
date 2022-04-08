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

import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.metadata.MetaDataException;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.record.metadata.expressions.RecordTypeKeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoredRecord;
import com.apple.foundationdb.relational.api.Continuation;
import com.apple.foundationdb.relational.api.QueryProperties;
import com.apple.foundationdb.relational.api.Row;
import com.apple.foundationdb.relational.api.Transaction;
import com.apple.foundationdb.relational.api.catalog.TableMetaData;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.recordlayer.util.ExceptionUtil;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;

import java.sql.SQLException;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * A table implementation based on a specific record type.
 */
public class RecordTypeTable extends RecordTypeScannable<FDBStoredRecord<Message>> implements Table {
    private final RecordLayerSchema schema;
    private final String tableName;
    private final RecordStoreConnection conn;

    private RecordType currentTypeRef;

    public RecordTypeTable(@Nonnull RecordLayerSchema schema,
                           @Nonnull String tableName) {
        this.schema = schema;
        this.tableName = tableName;
        this.conn = schema.conn;
    }

    @Override
    public @Nonnull
    RecordLayerSchema getSchema() {
        return schema;
    }

    @Override
    public Row get(@Nonnull Transaction t, @Nonnull Row key, @Nonnull QueryProperties queryProperties) throws RelationalException {
        FDBRecordStore store = schema.loadStore();
        try {
            final FDBStoredRecord<Message> storedRecord = store.loadRecord(TupleUtils.toFDBTuple(key));
            if (storedRecord == null) {
                return null;
            }
            return new MessageTuple(storedRecord.getRecord());
        } catch (RecordCoreException ex) {
            throw ExceptionUtil.toRelationalException(ex);
        }
    }

    @Override
    public String[] getFieldNames() throws RelationalException {
        RecordType type = loadRecordType();
        final Descriptors.Descriptor descriptor = type.getDescriptor();
        return descriptor.getFields().stream().map(Descriptors.FieldDescriptor::getName).toArray(String[]::new);
    }

    @Override
    public String[] getKeyFieldNames() throws RelationalException {
        try {
            RecordType type = loadRecordType();
            final Descriptors.Descriptor descriptor = type.getDescriptor();
            return type.getPrimaryKey().validate(descriptor).stream().map(Descriptors.FieldDescriptor::getName).toArray(String[]::new);
        } catch (RecordCoreException ex) {
            throw ExceptionUtil.toRelationalException(ex);
        }
    }

    @Override
    public KeyBuilder getKeyBuilder() throws RelationalException {
        final RecordType typeForKey = loadRecordType();
        return new KeyBuilder(typeForKey, typeForKey.getPrimaryKey(), "primary key of <" + tableName + ">");
    }

    @Override
    public boolean deleteRecord(@Nonnull Row key) throws RelationalException {
        FDBRecordStore store = schema.loadStore();
        try {
            return store.deleteRecord(TupleUtils.toFDBTuple(key));
        } catch (RecordCoreException ex) {
            throw ExceptionUtil.toRelationalException(ex);
        }
    }

    @Override
    public boolean insertRecord(@Nonnull Message message) throws RelationalException {
        FDBRecordStore store = schema.loadStore();
        try {
            if (!store.getRecordMetaData().getRecordType(this.tableName).getDescriptor().equals(message.getDescriptorForType())) {
                throw new RelationalException("type of message <" + message.getClass() + "> does not match the required type for table <" + getName() + ">", ErrorCode.INVALID_PARAMETER);
            }
            //TODO(bfines) maybe this should return something other than boolean?
            store.insertRecord(message);
        } catch (MetaDataException mde) {
            throw new RelationalException("type of message <" + message.getClass() + "> does not match the required type for table <" + getName() + ">", ErrorCode.INVALID_PARAMETER, mde);
        } catch (RecordCoreException ex) {
            throw ExceptionUtil.toRelationalException(ex);
        }
        return true;
    }

    @Override
    public Set<Index> getAvailableIndexes() throws RelationalException {
        return loadRecordType().getIndexes().stream().map((Function<com.apple.foundationdb.record.metadata.Index, Index>) index -> new RecordStoreIndex(index, this)).collect(Collectors.toSet());
    }

    @Override
    public void close() throws RelationalException {
        currentTypeRef = null;
    }

    void validate() throws RelationalException {
        try {
            if (!this.conn.inActiveTransaction()) {
                this.conn.beginTransaction();
                try {
                    loadRecordType();
                } finally {
                    this.conn.rollback();
                }
            } else {
                loadRecordType();
            }
        } catch (SQLException e) {
            throw ExceptionUtil.toRelationalException(e);
        }
    }

    @Nonnull
    @Override
    public String getName() {
        return tableName;
    }

    @Nonnull
    @Override
    public TableMetaData getMetaData() throws RelationalException {
        return new RecordTypeMetaData(loadRecordType());
    }

    @Override
    protected RecordCursor<FDBStoredRecord<Message>> openScan(FDBRecordStore store, TupleRange range, @Nullable Continuation continuation, ScanProperties props) throws RelationalException {
        RecordType type = loadRecordType();
        try {
            return store.scanRecords(range, continuation == null ? null : continuation.getBytes(), props)
                    .filter(record -> type.equals(record.getRecordType()));
        } catch (RecordCoreException ex) {
            throw ExceptionUtil.toRelationalException(ex);
        }
    }

    @Override
    protected Function<FDBStoredRecord<Message>, Row> keyValueTransform() {
        return record -> new MessageTuple(record.getRecord());
    }

    @Override
    protected boolean supportsMessageParsing() {
        return true;
    }

    @Override
    protected boolean hasConstantValueForPrimaryKey() throws RelationalException {
        return loadRecordType().getPrimaryKey() instanceof RecordTypeKeyExpression;
    }

    RecordType loadRecordType() throws RelationalException {
        if (currentTypeRef == null) {
            FDBRecordStore store = schema.loadStore();
            try {
                //just try to load the store, and see if it fails. If it fails, it's not there
                currentTypeRef = store.getRecordMetaData().getRecordType(tableName);
                //make sure to clear our state if the transaction ends
                this.conn.transaction.addTerminationListener(() -> currentTypeRef = null);
            } catch (MetaDataException mde) {
                throw new RelationalException(mde.getMessage(), ErrorCode.UNKNOWN_SCHEMA, mde);
            }
        }
        return currentTypeRef;
    }
}
