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
import com.apple.foundationdb.record.provider.foundationdb.RecordAlreadyExistsException;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.relational.api.Continuation;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.Row;
import com.apple.foundationdb.relational.api.SqlTypeSupport;
import com.apple.foundationdb.relational.api.StructMetaData;
import com.apple.foundationdb.relational.api.Transaction;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.recordlayer.util.ExceptionUtil;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;

import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
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
    private final EmbeddedRelationalConnection conn;

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
    public Row get(@Nonnull Transaction t, @Nonnull Row key, @Nonnull Options options) throws RelationalException {
        loadRecordType(options);
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
    @Nonnull
    public StructMetaData getMetaData() throws RelationalException {
        RecordType type = loadRecordType(Options.NONE);

        Map<String, Descriptors.FieldDescriptor> descriptorLookupMap = type.getDescriptor().getFields().stream()
                .collect(Collectors.toMap(Descriptors.FieldDescriptor::getName, Function.identity()));

        TreeMap<String, Descriptors.FieldDescriptor> orderedFieldMap = new TreeMap<>((o1, o2) -> {
            if (o1 == null) {
                if (o2 == null) {
                    return 0;
                } else {
                    return -1;
                } //sort nulls first; shouldn't happen here but it's a good habit
            } else if (o2 == null) {
                return 1;
            } else {
                Descriptors.FieldDescriptor field1 = descriptorLookupMap.get(o1);
                Descriptors.FieldDescriptor field2 = descriptorLookupMap.get(o2);
                return Integer.compare(field1.getIndex(), field2.getIndex());
            }
        });
        orderedFieldMap.putAll(descriptorLookupMap);
        final Type.Record record = Type.Record.fromFieldDescriptorsMap(orderedFieldMap);
        return SqlTypeSupport.recordToMetaData(record);
    }

    @Override
    public KeyBuilder getKeyBuilder() throws RelationalException {
        final RecordType typeForKey = loadRecordType(Options.NONE);
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
    @SuppressWarnings("PMD.PreserveStackTrace") //we are intentionally destroying the stack trace here
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
        } catch (RecordAlreadyExistsException raee) {
            throw new RelationalException("Duplicate primary key for message (" + message + ") on table <" + tableName + ">", ErrorCode.UNIQUE_CONSTRAINT_VIOLATION);
        } catch (RecordCoreException ex) {
            throw ExceptionUtil.toRelationalException(ex);
        }
        return true;
    }

    @Override
    public Set<Index> getAvailableIndexes() throws RelationalException {
        return loadRecordType(Options.NONE).getIndexes().stream()
                .map((Function<com.apple.foundationdb.record.metadata.Index, Index>) index -> new RecordStoreIndex(index, this))
                .collect(Collectors.toSet());
    }

    @Override
    public void close() throws RelationalException {
        currentTypeRef = null;
    }

    @Nonnull
    @Override
    public String getName() {
        return tableName;
    }

    @Override
    public void validateTable(@Nonnull Options options) throws RelationalException {
        loadRecordType(options);
    }

    @Override
    protected RecordCursor<FDBStoredRecord<Message>> openScan(FDBRecordStore store, TupleRange range,
                                                              @Nullable Continuation continuation,
                                                              Options options) throws RelationalException {
        RecordType type = loadRecordType(options);
        try {
            final ScanProperties scanProps = QueryPropertiesUtils.getScanProperties(options);
            return store.scanRecords(range, continuation == null ? null : continuation.getBytes(), scanProps)
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
    protected boolean hasConstantValueForPrimaryKey(Options options) throws RelationalException {
        return loadRecordType(options).getPrimaryKey() instanceof RecordTypeKeyExpression;
    }

    RecordType loadRecordType(Options options) throws RelationalException {
        FDBRecordStore store = schema.loadStore();
        if (currentTypeRef == null) {
            try {
                //just try to load the store, and see if it fails. If it fails, it's not there
                currentTypeRef = store.getRecordMetaData().getRecordType(tableName);
                validateRecordType(currentTypeRef, store, options);
                //make sure to clear our state if the transaction ends
                this.conn.addCloseListener(() -> currentTypeRef = null);
            } catch (MetaDataException mde) {
                throw new RelationalException(mde.getMessage(), ErrorCode.UNKNOWN_SCHEMA, mde);
            }
        } else {
            //make sure that this record type is valid _for the operation we are doing now_.
            validateRecordType(currentTypeRef, store, options);
        }

        return currentTypeRef;
    }

    private void validateRecordType(RecordType recordType, FDBRecordStore store, Options options) throws RelationalException {
        Integer requiredVersion = options.getOption(Options.Name.REQUIRED_METADATA_TABLE_VERSION);
        if (requiredVersion != null) {
            /*
             * We need to check the created version stamp for the table, and ensure that it was
             * created after the specified value.
             *
             * For a weird twist, sometimes we don't care what the specific value is, only that
             * a value exists (this is done for strange historical reasons). To signify that,
             * we use the -1 option value.
             *
             * The context fields here are carried over from prior implementations.
             */
            final Integer tableMetaDataVersion = recordType.getSinceVersion();
            if (tableMetaDataVersion == null) {
                final String errMsg = String.format("table <%s> is not available, creation version is missing from metadata(version <%s>)",
                        recordType.getName(), store.getRecordMetaData().getVersion());
                throw new RelationalException(errMsg, ErrorCode.INCORRECT_METADATA_TABLE_VERSION)
                        .addContext("metadataVersion", store.getRecordMetaData().getVersion())
                        .addContext("recordType", recordType.getName());
            }
            if (requiredVersion != -1 && requiredVersion < tableMetaDataVersion) {
                final String errMsg = String.format("table <%s> is not available, creation version is invalid for metadata(version <%s>); Required creation version <%s>",
                        recordType.getName(), store.getRecordMetaData().getVersion(), requiredVersion);
                throw new RelationalException(errMsg, ErrorCode.INCORRECT_METADATA_TABLE_VERSION)
                        .addContext("metadataVersion", store.getRecordMetaData().getVersion())
                        .addContext("recordType", recordType.getName());
            }
        }
    }

}
