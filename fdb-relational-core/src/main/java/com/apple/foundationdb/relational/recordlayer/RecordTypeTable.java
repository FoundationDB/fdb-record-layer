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
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.metadata.MetaDataException;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.record.metadata.expressions.RecordTypeKeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoredRecord;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.relational.api.Continuation;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.Row;
import com.apple.foundationdb.relational.api.SqlTypeSupport;
import com.apple.foundationdb.relational.api.StructMetaData;
import com.apple.foundationdb.relational.api.Transaction;
import com.apple.foundationdb.relational.api.RelationalStruct;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.recordlayer.storage.BackingStore;
import com.apple.foundationdb.relational.recordlayer.util.ExceptionUtil;

import com.apple.foundationdb.relational.util.Assert;
import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;

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
    public void validate(Options scanOptions) throws RelationalException {
        //loading the record type should force the validation
        loadRecordType(scanOptions);
    }

    @Override
    public @Nonnull
    RecordLayerSchema getSchema() {
        return schema;
    }

    @Override
    public Row get(@Nonnull Transaction t, @Nonnull Row key, @Nonnull Options options) throws RelationalException {
        loadRecordType(options);
        BackingStore store = schema.loadStore();
        return store.get(key, options);
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
        BackingStore store = schema.loadStore();
        return store.delete(key);
    }

    @Override
    public void deleteRange(Map<String, Object> prefix) throws RelationalException {
        String tableNameForDelete;
        if (loadRecordType(Options.NONE).primaryKeyHasRecordTypePrefix()) {
            tableNameForDelete = tableName;
        } else {
            tableNameForDelete = null;
        }
        BackingStore store = schema.loadStore();
        store.deleteRange(prefix, tableNameForDelete);
    }

    @Override
    public boolean insertRecord(@Nonnull Message message, boolean replaceOnDuplicate) throws RelationalException {
        BackingStore store = schema.loadStore();
        //TODO(bfines) maybe this should return something other than boolean?
        return store.insert(tableName, message, replaceOnDuplicate);
    }

    @Override
    @SuppressWarnings("PMD.PreserveStackTrace") //we are intentionally destroying the stack trace here
    public boolean insertRecord(@Nonnull RelationalStruct insert, boolean replaceOnDuplicate) throws RelationalException {
        BackingStore store = schema.loadStore();
        try {
            final RecordType recordType = store.getRecordMetaData().getRecordType(this.tableName);
            Message message = toDynamicMessage(insert, recordType.getDescriptor());
            return insertRecord(message, replaceOnDuplicate);
        } catch (RecordCoreException ex) {
            throw ExceptionUtil.toRelationalException(ex);
        }
    }

    /**
     * Convert {@link RelationalStruct} to Record Layer {@link Message}.
     */
    @VisibleForTesting
    static Message toDynamicMessage(RelationalStruct struct, Descriptors.Descriptor descriptor) throws RelationalException {
        Type.Record record = Type.Record.fromDescriptor(descriptor);
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(descriptor);
        Map<String, Type.Record.Field> fieldMap = record.getFieldNameFieldMap();
        try {
            StructMetaData structMetaData = struct.getMetaData();
            for (int i = 1; i <= structMetaData.getColumnCount(); i++) {
                String columnName = structMetaData.getColumnName(i);
                Type.Record.Field field = fieldMap.get(columnName);
                if (field == null) {
                    throw new RelationalException(String.format("Column <%s> does not exist on table/struct <%s>",
                            columnName, record.getName()), ErrorCode.UNDEFINED_COLUMN);
                }
                Descriptors.FieldDescriptor fd = builder.getDescriptorForType().findFieldByName(field.getFieldName());
                // java.sql.Types, not cascades types.
                // TODO: Add type checking, nudging, and coercion at this point! Per bfines, good place to do it!
                //  TODO (Add type checking, nudging, and coercion)
                switch (structMetaData.getColumnType(i)) {
                    case Types.BOOLEAN:
                        builder.setField(fd, struct.getBoolean(i));
                        break;
                    case Types.BINARY:
                        builder.setField(fd, struct.getBytes(i));
                        break;
                    case Types.DOUBLE:
                        builder.setField(fd, struct.getDouble(i));
                        break;
                    case Types.FLOAT:
                        builder.setField(fd, struct.getFloat(i));
                        break;
                    case Types.INTEGER:
                        builder.setField(fd, struct.getInt(i));
                        break;
                    case Types.BIGINT:
                        builder.setField(fd, struct.getLong(i));
                        break;
                    case Types.VARCHAR:
                        builder.setField(fd, struct.getString(i));
                        break;
                    case Types.STRUCT:
                        var subStruct = struct.getStruct(i);
                        builder.setField(fd, toDynamicMessage(subStruct, fd.getMessageType()));
                        break;
                    case Types.ARRAY:
                        var subArray = struct.getArray(i);
                        var arrayBuilder = DynamicMessage.newBuilder(fd.getMessageType());
                        final var arrayItems = (Object[]) subArray.getArray();
                        List<Descriptors.FieldDescriptor> innerFields = fd.getMessageType().getFields();
                        if (innerFields != null && !innerFields.isEmpty()) {
                            Descriptors.FieldDescriptor structFieldDescriptor = innerFields.get(0);
                            for (Object arrayItem : arrayItems) {
                                Assert.thatUnchecked(arrayItem instanceof RelationalStruct, ErrorCode.INTERNAL_ERROR, "Direct Insertions using STRUCT do not support primitive arrays.");
                                arrayBuilder.addRepeatedField(structFieldDescriptor,
                                        toDynamicMessage((RelationalStruct) arrayItem, structFieldDescriptor.getMessageType()));
                            }
                        }
                        builder.setField(fd, arrayBuilder.build());
                        break;
                    default:
                        // No handling of ENUMS.
                        //programmer error
                        throw new RelationalException(String.format("Internal Error:Unexpected Column type <%s> for column <%s>",
                                structMetaData.getColumnType(i), field.getFieldName()), ErrorCode.INTERNAL_ERROR);
                }
            }
        } catch (SQLException sqle) {
            throw new RelationalException(sqle);
        }

        return builder.build();
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
    protected RecordCursor<FDBStoredRecord<Message>> openScan(BackingStore store, TupleRange range,
                                                              @Nullable Continuation continuation,
                                                              Options options) throws RelationalException {
        RecordType type = loadRecordType(options);
        return store.scanType(type, range, continuation, options);
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
        BackingStore store = schema.loadStore();
        RecordMetaData metaData = store.getRecordMetaData();
        if (currentTypeRef == null) {
            try {
                //just try to load the store, and see if it fails. If it fails, it's not there
                currentTypeRef = store.getRecordMetaData().getRecordType(tableName);
                validateRecordType(currentTypeRef, metaData, options);
                //make sure to clear our state if the transaction ends
                this.conn.addCloseListener(() -> currentTypeRef = null);
            } catch (MetaDataException mde) {
                throw new RelationalException(mde.getMessage(), ErrorCode.UNDEFINED_SCHEMA, mde);
            }
        } else {
            //make sure that this record type is valid _for the operation we are doing now_.
            validateRecordType(currentTypeRef, metaData, options);
        }

        return currentTypeRef;
    }

    private void validateRecordType(RecordType recordType, RecordMetaData metaData, Options options) throws RelationalException {
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
            final int metaDataVersion = metaData.getVersion();
            if (tableMetaDataVersion == null) {
                final String errMsg = String.format("table <%s> is not available, creation version is missing from metadata(version <%s>)",
                        recordType.getName(), metaDataVersion);
                throw new RelationalException(errMsg, ErrorCode.INCORRECT_METADATA_TABLE_VERSION)
                        .addContext("metadataVersion", metaDataVersion)
                        .addContext("recordType", recordType.getName());
            }
            if (requiredVersion != -1 && requiredVersion < tableMetaDataVersion) {
                final String errMsg = String.format("table <%s> is not available, creation version is invalid for metadata(version <%s>); Required creation version <%s>",
                        recordType.getName(), metaDataVersion, requiredVersion);
                throw new RelationalException(errMsg, ErrorCode.INCORRECT_METADATA_TABLE_VERSION)
                        .addContext("metadataVersion", metaDataVersion)
                        .addContext("recordType", recordType.getName());
            }
        }
    }

}
