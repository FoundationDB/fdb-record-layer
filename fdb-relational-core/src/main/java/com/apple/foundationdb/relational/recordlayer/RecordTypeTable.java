/*
 * RecordTypeTable.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2025 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.linear.AbstractRealVector;
import com.apple.foundationdb.linear.DoubleRealVector;
import com.apple.foundationdb.linear.FloatRealVector;
import com.apple.foundationdb.linear.HalfRealVector;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataOptionsProto;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.metadata.MetaDataException;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.record.metadata.expressions.RecordTypeKeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoredRecord;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.relational.api.Continuation;
import com.apple.foundationdb.relational.api.ImmutableRowStruct;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.RelationalStruct;
import com.apple.foundationdb.relational.api.RelationalStructMetaData;
import com.apple.foundationdb.relational.api.Row;
import com.apple.foundationdb.relational.api.StructMetaData;
import com.apple.foundationdb.relational.api.Transaction;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.api.metadata.DataType;
import com.apple.foundationdb.relational.recordlayer.metadata.DataTypeUtils;
import com.apple.foundationdb.relational.recordlayer.storage.BackingStore;
import com.apple.foundationdb.relational.recordlayer.util.ExceptionUtil;
import com.apple.foundationdb.relational.util.Assert;
import com.apple.foundationdb.relational.util.NullableArrayUtils;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.sql.SQLException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * A table implementation based on a specific record type.
 */
@API(API.Status.EXPERIMENTAL)
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

    @Nonnull
    @Override
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
        return RelationalStructMetaData.of((DataType.StructType) DataTypeUtils.toRelationalType(record));
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
    @Deprecated
    public boolean insertRecord(@Nonnull Message message, boolean replaceOnDuplicate) throws RelationalException {
        BackingStore store = schema.loadStore();
        //TODO(bfines) maybe this should return something other than boolean?
        return store.insert(tableName, message, replaceOnDuplicate);
    }

    @Override
    @Deprecated
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
     * This is used to support {@link com.apple.foundationdb.relational.api.RelationalDirectAccessStatement#executeInsert} operation and
     * should not be used for any other general purpose.
     */
    @Nonnull
    public static Message toDynamicMessage(RelationalStruct struct, Descriptors.Descriptor descriptor) throws RelationalException {
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(descriptor);
        try {
            final var type = struct.getMetaData().getRelationalDataType();
            final var fields = type.getFields();
            for (int i = 0; i < fields.size(); i++) {
                final var field = fields.get(i);
                final var columnName = fields.get(i).getName();
                Descriptors.FieldDescriptor fd = builder.getDescriptorForType().findFieldByName(columnName);
                Assert.thatUnchecked(fd != null, ErrorCode.INVALID_PARAMETER, "Cannot find column name: " + columnName);
                // TODO: Add type checking, nudging, and coercion at this point! Per bfines, good place to do it!
                //  TODO (Add type checking, nudging, and coercion)
                if (fd.getType() == Descriptors.FieldDescriptor.Type.ENUM) {
                    final var maybeEnumValue = struct.getString(i + 1);
                    if (maybeEnumValue != null) {
                        final var valueDescriptor = fd.getEnumType().findValueByName(maybeEnumValue);
                        Assert.that(valueDescriptor != null, ErrorCode.CANNOT_CONVERT_TYPE, "Invalid enum value: %s", maybeEnumValue);
                        builder.setField(fd, valueDescriptor);
                    }
                    continue;
                }
                switch (field.getType().getCode()) {
                    case BOOLEAN:
                    case DOUBLE:
                    case FLOAT:
                    case INTEGER:
                    case LONG:
                    case STRING:
                        final var obj = struct.getObject(i + 1);
                        if (obj != null) {
                            try {
                                builder.setField(fd, obj);
                            } catch (IllegalArgumentException ex) {
                                throw new RelationalException("Unexpected Column type " + struct.getMetaData().getColumnTypeName(i) + " for column " + columnName, ErrorCode.CANNOT_CONVERT_TYPE, ex);
                            }
                        }
                        break;
                    case BYTES:
                        final var bytes = struct.getBytes(i + 1);
                        if (bytes != null) {
                            builder.setField(fd, ByteString.copyFrom(bytes));
                        }
                        break;
                    case STRUCT:
                        var subStruct = struct.getStruct(i + 1);
                        if (subStruct != null) {
                            builder.setField(fd, toDynamicMessage(subStruct, fd.getMessageType()));
                        }
                        break;
                    case ARRAY:
                        var array = struct.getArray(i + 1);
                        // if the fieldDescriptor is repeatable, then it's an unwrapped array
                        if (fd.isRepeated()) {
                            final var arrayItems = (Object[]) (array == null ? new Object[]{} : array.getArray());
                            for (Object arrayItem : arrayItems) {
                                if (arrayItem instanceof RelationalStruct) {
                                    builder.addRepeatedField(fd, toDynamicMessage((RelationalStruct) arrayItem, fd.getMessageType()));
                                } else {
                                    builder.addRepeatedField(fd, arrayItem);
                                }
                            }
                        } else {
                            Assert.that(fd.getType() == Descriptors.FieldDescriptor.Type.MESSAGE, ErrorCode.CANNOT_CONVERT_TYPE,
                                    "Field Type expected to be of Type ARRAY but is actually " + fd.getType());
                            Assert.that(NullableArrayUtils.isWrappedArrayDescriptor(fd.getMessageType()));
                            // wrap array in a struct and call toDynamicMessage again
                            final var wrapper = new ImmutableRowStruct(new ArrayRow(array), RelationalStructMetaData.of(
                                    DataType.StructType.from("STRUCT", List.of(
                                            DataType.StructType.Field.from(NullableArrayUtils.REPEATED_FIELD_NAME, array.getMetaData().asRelationalType(), 0)
                                    ), true)));
                            builder.setField(fd, toDynamicMessage(wrapper, fd.getMessageType()));
                        }
                        break;
                    case VECTOR:
                        final var vector = struct.getObject(i + 1);
                        if (vector != null) {
                            Assert.that(vector instanceof AbstractRealVector, ErrorCode.CANNOT_CONVERT_TYPE,
                                    "Field Type expected to be of Type VECTOR but is actually " + fd.getType());
                            final var fieldOptionMaybe = Optional.ofNullable(fd.getOptions()).map(f -> f.getExtension(RecordMetaDataOptionsProto.field));
                            Assert.that(fieldOptionMaybe.isPresent() && fieldOptionMaybe.get().hasVectorOptions(), ErrorCode.CANNOT_CONVERT_TYPE, "Cannot insert non vector type into vector column");
                            final var vectorOptions = fieldOptionMaybe.get().getVectorOptions();
                            Assert.that(vectorOptions.getDimensions() == ((AbstractRealVector)vector).getNumDimensions(), ErrorCode.CANNOT_CONVERT_TYPE, "Wrong number of dimension for vector");
                            final int precision = vectorOptions.getPrecision();
                            switch (precision) {
                                case 16:
                                    Assert.that(vector instanceof HalfRealVector, ErrorCode.CANNOT_CONVERT_TYPE, "Wrong precision for vector");
                                    break;
                                case 32:
                                    Assert.that(vector instanceof FloatRealVector, ErrorCode.CANNOT_CONVERT_TYPE, "Wrong precision for vector");
                                    break;
                                case 64:
                                    Assert.that(vector instanceof DoubleRealVector, ErrorCode.CANNOT_CONVERT_TYPE, "Wrong precision for vector");
                                    break;
                                default:
                                    Assert.fail(ErrorCode.INTERNAL_ERROR, "Unknown precision for vector");
                                    break;
                            }
                            builder.setField(fd, ((AbstractRealVector) vector).getRawData());
                        }
                        break;
                    case NULL:
                        break;
                    default:
                        Assert.fail(ErrorCode.INTERNAL_ERROR, (String.format(Locale.ROOT, "Unexpected Column type <%s> for column <%s>",
                                struct.getMetaData().getColumnTypeName(i), columnName)));
                        break;
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
                final String errMsg = String.format(Locale.ROOT, "table <%s> is not available, creation version is missing from metadata(version <%s>)",
                        recordType.getName(), metaDataVersion);
                throw new RelationalException(errMsg, ErrorCode.INCORRECT_METADATA_TABLE_VERSION)
                        .addContext("metadataVersion", metaDataVersion)
                        .addContext("recordType", recordType.getName());
            }
            if (requiredVersion != -1 && requiredVersion < tableMetaDataVersion) {
                final String errMsg = String.format(Locale.ROOT, "table <%s> is not available, creation version is invalid for metadata(version <%s>); Required creation version <%s>",
                        recordType.getName(), metaDataVersion, requiredVersion);
                throw new RelationalException(errMsg, ErrorCode.INCORRECT_METADATA_TABLE_VERSION)
                        .addContext("metadataVersion", metaDataVersion)
                        .addContext("recordType", recordType.getName());
            }
        }
    }

}
