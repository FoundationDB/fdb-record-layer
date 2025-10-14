/*
 * TypeConversion.java
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

package com.apple.foundationdb.relational.jdbc;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.relational.api.ArrayMetaData;
import com.apple.foundationdb.relational.api.Continuation;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.RelationalArray;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.RelationalStruct;
import com.apple.foundationdb.relational.api.StructMetaData;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.api.metadata.DataType;
import com.apple.foundationdb.relational.jdbc.grpc.v1.KeySet;
import com.apple.foundationdb.relational.jdbc.grpc.v1.KeySetValue;
import com.apple.foundationdb.relational.jdbc.grpc.v1.ResultSet;
import com.apple.foundationdb.relational.jdbc.grpc.v1.ResultSetMetadata;
import com.apple.foundationdb.relational.jdbc.grpc.v1.RpcContinuation;
import com.apple.foundationdb.relational.jdbc.grpc.v1.RpcContinuationReason;
import com.apple.foundationdb.relational.jdbc.grpc.v1.column.Array;
import com.apple.foundationdb.relational.jdbc.grpc.v1.column.Column;
import com.apple.foundationdb.relational.jdbc.grpc.v1.column.ColumnMetadata;
import com.apple.foundationdb.relational.jdbc.grpc.v1.column.EnumMetadata;
import com.apple.foundationdb.relational.jdbc.grpc.v1.column.ListColumn;
import com.apple.foundationdb.relational.jdbc.grpc.v1.column.ListColumnMetadata;
import com.apple.foundationdb.relational.jdbc.grpc.v1.column.Struct;
import com.apple.foundationdb.relational.jdbc.grpc.v1.column.Type;
import com.apple.foundationdb.relational.jdbc.grpc.v1.column.Uuid;
import com.apple.foundationdb.relational.util.PositionalIndex;
import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.ByteString;

import javax.annotation.Nonnull;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.BiFunction;

/**
 * Utility for converting types used by JDBC from Relational and FDB such as KeySet, RelationalStruct and RelationalArray.
 * Utility is mostly into and out-of generated protobufs and type convertions such as a list of RelationalStructs to
 * ResultSet. Used in client and server.
 */
@API(API.Status.EXPERIMENTAL)
public class TypeConversion {
    /**
     * Return {@link RelationalStruct} instance found at <code>rowIndex</code> and <code>oneBasedColumn</code> offsets.
     * Manipulation of protobufs. Exploits package-private internal attributes of {@link RelationalStructFacade}.
     * @param resultSet Protobuf ResultSet to fetch {@link RelationalStruct} from.
     * @param rowIndex Current 'row' offset into <code>resultSet</code>
     * @param oneBasedColumn One-based column index where we'll find the {@link RelationalStruct} instance.
     * @return {@link RelationalStruct} instance pulled from <code>resultSet</code>
     * @throws SQLException If failed get of <code>resultSet</code> metadata.
     */
    static RelationalStruct getStruct(ResultSet resultSet, int rowIndex, int oneBasedColumn) throws SQLException {
        int index = PositionalIndex.toProtobuf(oneBasedColumn);
        var metadata =
                resultSet.getMetadata().getColumnMetadata().getColumnMetadata(index).getStructMetadata();
        Column column = resultSet.getRow(rowIndex).getColumns().getColumn(index);
        return column.hasStruct() ? new RelationalStructFacade(metadata, column.getStruct()) : null;
    }

    static UUID getUUID(ResultSet resultSet, int rowIndex, int oneBasedColumn) throws SQLException {
        int index = PositionalIndex.toProtobuf(oneBasedColumn);
        Column column = resultSet.getRow(rowIndex).getColumns().getColumn(index);
        return column.hasUuid() ? new UUID(column.getUuid().getMostSignificantBits(), column.getUuid().getLeastSignificantBits()) : null;
    }

    /**
     * Convert list of {@link RelationalStruct} to {@link ResultSet}.
     * @param structs List of {@link RelationalStruct} which are expected to be of type {@link RelationalStructFacade}.
     * @return Populated {@link ResultSet}.
     * @throws SQLException If failed get of metadata or if unwrap to {@link RelationalStructFacade} fails.
     * @see #fromResultSetProtobuf(ResultSet)
     */
    static ResultSet toResultSetProtobuf(List<RelationalStruct> structs) throws SQLException {
        var resultSetBuilder = ResultSet.newBuilder();
        for (RelationalStruct struct : structs) {
            RelationalStructFacade relationalStruct = struct.unwrap(RelationalStructFacade.class);
            // This is 'dirty'... reaching into jdbcRelationalStruct to get its backing protobuf metadata and data
            // but serialization is nasty generally needing access to private fields so in part this is excusable.
            // metadata and data are kept in distinct protobufs for now; more amenable to processing in the
            // different contexts -- at least for now.
            if (!resultSetBuilder.hasMetadata()) {
                var resultSetMetadataBuilder = ResultSetMetadata.newBuilder();
                // Only need to do this first time through; there is only one instance of metadata.
                resultSetBuilder.setMetadata(resultSetMetadataBuilder
                        .setColumnMetadata(relationalStruct.getDelegateMetadata()).build());
            }
            resultSetBuilder.addRow(relationalStruct.getDelegate());
        }
        return resultSetBuilder.build();
    }

    /**
     * Convert a {@link ResultSet} to a List of {@link RelationalStruct}.
     * @param data ResultSet to deserialize as a List of {@link RelationalStruct}.
     * @return List of {@link RelationalStruct}.
     * @see #toResultSetProtobuf(List)
     */
    @SuppressWarnings("PMD.AvoidReassigningLoopVariables") // TODO: This may be mis-handling the variable. Needs follow-up/testing
    public static List<RelationalStruct> fromResultSetProtobuf(ResultSet data) {
        int rowCount = data.getRowCount();
        List<RelationalStruct> structs = new ArrayList<>(rowCount);
        for (int i = 0; i < rowCount; i++) {
            // Suspicious update of i here. This is skipping every other row. May be intentional or not
            structs.add(new RelationalStructFacade(data.getMetadata().getColumnMetadata(), data.getRow(i++)));
        }
        return structs;
    }

    static KeySet toProtobuf(com.apple.foundationdb.relational.api.KeySet keySet) {
        KeySet.Builder keySetBuilder = KeySet.newBuilder();
        for (Map.Entry<String, Object> entry : keySet.toMap().entrySet()) {
            KeySetValue keySetValue;
            // Currently we support a few types only.
            if (entry.getValue() instanceof String) {
                keySetValue = KeySetValue.newBuilder().setStringValue((String) entry.getValue()).build();
            } else if (entry.getValue() instanceof byte[]) {
                keySetValue =
                        KeySetValue.newBuilder().setBytesValue(ByteString.copyFrom((byte[]) entry.getValue())).build();
            } else if (entry.getValue() instanceof Long) {
                keySetValue = KeySetValue.newBuilder().setLongValue((long) entry.getValue()).build();
            } else {
                throw new UnsupportedOperationException("Unsupported type " + entry.getValue());
            }
            keySetBuilder.putFields(entry.getKey(), keySetValue);
        }
        return keySetBuilder.build();
    }

    public static com.apple.foundationdb.relational.api.KeySet fromProtobuf(KeySet protobufKeySet) throws SQLException {
        com.apple.foundationdb.relational.api.KeySet keySet = new com.apple.foundationdb.relational.api.KeySet();
        for (Map.Entry<String, KeySetValue> entry : protobufKeySet.getFieldsMap().entrySet()) {
            keySet.setKeyColumn(entry.getKey(),
                    entry.getValue().hasBytesValue() ? entry.getValue().getBytesValue() :
                            entry.getValue().hasLongValue() ? entry.getValue().getLongValue() :
                                    entry.getValue().hasStringValue() ? entry.getValue().getStringValue() : null);
        }
        return keySet;
    }

    private static ColumnMetadata toColumnMetadata(StructMetaData metadata, int oneBasedIndex, int fieldIndex)
            throws SQLException {
        final var type = metadata.getRelationalDataType().getFields().get(fieldIndex).getType();
        final var protobufType = toProtobufType(type);
        var columnMetadataBuilder = ColumnMetadata.newBuilder()
                .setName(metadata.getColumnName(oneBasedIndex))
                .setJavaSqlTypesCode(metadata.getColumnType(oneBasedIndex))
                .setNullable(metadata.isNullable(oneBasedIndex) == DatabaseMetaData.columnNullable)
                .setType(protobufType);
        // TODO phantom.
        // TODO: label
        // One-offs
        switch (protobufType) {
            case STRUCT:
                var listColumnMetadata = toListColumnMetadataProtobuf(metadata.getStructMetaData(oneBasedIndex));
                columnMetadataBuilder.setStructMetadata(listColumnMetadata);
                break;
            case ARRAY:
                var columnMetadata = toColumnMetadata(metadata.getArrayMetaData(oneBasedIndex));
                columnMetadataBuilder.setArrayMetadata(columnMetadata);
                break;
            case ENUM:
                var enumMetadata = toEnumMetadata((DataType.EnumType) type);
                columnMetadataBuilder.setEnumMetadata(enumMetadata);
                break;
            default:
                break;
        }
        return columnMetadataBuilder.build();
    }

    private static EnumMetadata toEnumMetadata(@Nonnull DataType.EnumType enumType) {
        final var builder = EnumMetadata.newBuilder().setName(enumType.getName());
        enumType.getValues().forEach(v -> builder.addValues(v.getName()));
        return builder.build();
    }

    private static Type toProtobufType(@Nonnull DataType type) {
        switch (type.getCode()) {
            case LONG:
                return Type.LONG;
            case INTEGER:
                return Type.INTEGER;
            case BOOLEAN:
                return Type.BOOLEAN;
            case BYTES:
                return Type.BYTES;
            case DOUBLE:
                return Type.DOUBLE;
            case FLOAT:
                return Type.FLOAT;
            case STRING:
                return Type.STRING;
            case STRUCT:
                return Type.STRUCT;
            case ARRAY:
                return Type.ARRAY;
            case VERSION:
                return Type.VERSION;
            case ENUM:
                return Type.ENUM;
            case UUID:
                return Type.UUID;
            default:
                throw new RelationalException("not supported in toProtobuf: " + type, ErrorCode.INTERNAL_ERROR).toUncheckedWrappedException();
        }
    }

    /**
     * The below is about making an Array.
     */
    private static ColumnMetadata toColumnMetadata(@Nonnull ArrayMetaData metadata)
            throws SQLException {
        var columnMetadataBuilder = ColumnMetadata.newBuilder()
                .setName(metadata.getElementName())
                .setJavaSqlTypesCode(metadata.getElementType())
                .setType(toProtobufType(metadata.asRelationalType().getElementType()))
                .setNullable(metadata.isElementNullable() == DatabaseMetaData.columnNullable);
        // TODO phantom.
        // TODO: label
        // One-offs
        switch (metadata.getElementType()) {
            case Types.STRUCT:
                var listColumnMetadata = toListColumnMetadataProtobuf(metadata.getElementStructMetaData());
                columnMetadataBuilder.setStructMetadata(listColumnMetadata);
                break;
            case Types.ARRAY:
                var columnMetadata = toColumnMetadata(metadata.getElementArrayMetaData());
                columnMetadataBuilder.setArrayMetadata(columnMetadata);
                break;
            default:
                break;
        }
        return columnMetadataBuilder.build();
    }

    private static ListColumnMetadata toListColumnMetadataProtobuf(@Nonnull StructMetaData metadata) throws SQLException {
        var listColumnMetadataBuilder = ListColumnMetadata.newBuilder();
        for (int oneBasedIndex = 1; oneBasedIndex <= metadata.getColumnCount(); oneBasedIndex++) {
            var columnMetadata = toColumnMetadata(metadata, oneBasedIndex, oneBasedIndex - 1 + metadata.getLeadingPhantomColumnCount());
            listColumnMetadataBuilder.addColumnMetadata(columnMetadata);
        }
        return listColumnMetadataBuilder.build();
    }

    private static ResultSetMetadata toResultSetMetaData(RelationalResultSet resultSet, int columnCount) throws SQLException {
        var listColumnMetadataBuilder = ListColumnMetadata.newBuilder();
        for (int oneBasedIndex = 1; oneBasedIndex <= columnCount; oneBasedIndex++) {
            listColumnMetadataBuilder.addColumnMetadata(toColumnMetadata(resultSet.getMetaData(), oneBasedIndex, oneBasedIndex - 1 + resultSet.getMetaData().getLeadingPhantomColumnCount()));
        }
        return ResultSetMetadata.newBuilder().setColumnMetadata(toListColumnMetadataProtobuf(resultSet.getMetaData())).build();
    }

    private static Array toArray(RelationalArray relationalArray) throws SQLException {
        var arrayBuilder = Array.newBuilder();
        if (relationalArray != null) {
            var relationalResultSet = relationalArray.getResultSet();
            while (relationalResultSet.next()) {
                final var value = relationalResultSet.getObject(2);
                arrayBuilder.addElement(toColumn(relationalResultSet.getMetaData().getRelationalDataType().getFields().get(1), value, relationalResultSet.wasNull()));
            }
        }
        return arrayBuilder.build();
    }

    private static Struct toStruct(RelationalStruct relationalStruct) throws SQLException {
        // TODO: The call to get metadata below is expensive? And all we want is column count. Revisit.
        var listColumnBuilder = ListColumn.newBuilder();
        final var leadingPhantomCount = relationalStruct.getMetaData().getLeadingPhantomColumnCount();
        final var fields = relationalStruct.getMetaData().getRelationalDataType().getFields();
        for (int i = 0; i < fields.size(); i++) {
            if (i >= leadingPhantomCount) {
                final var value = relationalStruct.getObject(i + 1);
                listColumnBuilder.addColumn(toColumn(fields.get(i), value, relationalStruct.wasNull()));
            }
        }
        return Struct.newBuilder().setColumns(listColumnBuilder.build()).build();
    }

    /**
     * Return the Java object stored within the proto.
     * @param columnType the type of object in the column
     * @param column the column to process
     * @return the Java object from the Column representation
     * @throws SQLException in case of an error
     */
    public static Object fromColumn(int columnType, Column column) throws SQLException {
        switch (columnType) {
            case Types.ARRAY:
                checkColumnType(columnType, column.hasArray());
                return fromArray(column.getArray());
            case Types.BIGINT:
                checkColumnType(columnType, column.hasLong());
                return column.getLong();
            case Types.INTEGER:
                checkColumnType(columnType, column.hasInteger());
                return column.getInteger();
            case Types.BOOLEAN:
                checkColumnType(columnType, column.hasBoolean());
                return column.getBoolean();
            case Types.VARCHAR:
                checkColumnType(columnType, column.hasString());
                return column.getString();
            case Types.BINARY:
                checkColumnType(columnType, column.hasBinary());
                return column.getBinary().toByteArray();
            case Types.DOUBLE:
                checkColumnType(columnType, column.hasDouble());
                return column.getDouble();
            default:
                // NULL (java.sql.Types value 0) is not a valid column type for an array and is likely the result of a default value for the
                // (optional) array.getElementType() protobuf field.
                throw new SQLException("java.sql.Type=" + columnType + " not supported", ErrorCode.ARRAY_ELEMENT_ERROR.getErrorCode());
        }
    }

    private static void checkColumnType(final int expectedColumnType, final boolean columnHasType) throws SQLException {
        if (!columnHasType) {
            throw new SQLException("Column has wrong type (expected " + expectedColumnType + ")", ErrorCode.WRONG_OBJECT_TYPE.getErrorCode());
        }
    }

    /**
     * Return the Java array stored within the proto.
     * @param array the array to process
     * @return the Java array from the proto representation
     * @throws SQLException in case of an error
     */
    public static Object[] fromArray(Array array) throws SQLException {
        Object[] result = new Object[array.getElementCount()];
        final List<Column> elements = array.getElementList();
        for (int i = 0 ; i < elements.size() ; i++) {
            result[i] = fromColumn(array.getElementType(), elements.get(i));
        }
        return result;
    }

    /**
     * Return the protobuf {@link Array} for a SQL {@link java.sql.Array}.
     * @param array the SQL array
     * @return the resulting protobuf array
     */
    public static Array toArray(@Nonnull java.sql.Array array) throws SQLException {
        Array.Builder builder = Array.newBuilder();
        builder.setElementType(array.getBaseType());
        for (Object o: (Object[])array.getArray()) {
            builder.addElement(toColumn(array.getBaseType(), o));
        }
        return builder.build();
    }

    /**
     * Create {@link Column} from a Java object.
     * Note: In case the column is of a composite type (array) then the actual type has to be a SQL flavor
     * ({@link java.sql.Array}.
     * Note: In case {@code columnType} is of value {@link Types#NULL}, the {@code obj} parameter is expected to be the
     * type of null. That is, the {@code obj} will represent the {@link Types} constant for the type of variable whose
     * value is null.
     * @param columnType the SQL type to create (from {@link Types})
     * @param obj the value to use for the column
     * @return the created column
     * @throws SQLException in case of error
     */
    public static Column toColumn(int columnType, @Nonnull Object obj) throws SQLException {
        if (columnType != DataType.getDataTypeFromObject(obj).getJdbcSqlCode()) {
            throw new SQLException("Column element type does not match object type: " + columnType + " / " + obj.getClass().getSimpleName(),
                    ErrorCode.WRONG_OBJECT_TYPE.getErrorCode());
        }

        Column.Builder builder = Column.newBuilder();
        switch (columnType) {
            case Types.BIGINT:
                builder = builder.setLong((Long)obj);
                break;
            case Types.INTEGER:
                builder = builder.setInteger((Integer)obj);
                break;
            case Types.BOOLEAN:
                builder = builder.setBoolean((Boolean)obj);
                break;
            case Types.VARCHAR:
                builder = builder.setString((String)obj);
                break;
            case Types.BINARY:
                builder = builder.setBinary((ByteString)obj);
                break;
            case Types.DOUBLE:
                builder = builder.setDouble((Double)obj);
                break;
            case Types.ARRAY:
                builder = builder.setArray(toArray((java.sql.Array)obj));
                break;
            case Types.NULL:
                builder = builder.setNullType((Integer)obj);
                break;
            default:
                throw new SQLException("java.sql.Type=" + columnType + " not supported",
                        ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
        }
        return builder.build();
    }

    private static Column toColumn(@Nonnull DataType.StructType.Field field, @Nonnull Object value, boolean wasNull) throws SQLException {
        Column column;
        switch (field.getType().getCode()) {
            case STRUCT:
                column = toColumn(wasNull ? null : toStruct((RelationalStruct) value),
                        (a, b) -> a == null ? b.clearStruct() : b.setStruct(a));
                break;
            case ARRAY:
                column = toColumn(wasNull ? null : toArray((RelationalArray) value),
                        (a, b) -> a == null ? b.clearArray() : b.setArray(a));
                break;
            case LONG:
                column = toColumn(wasNull ? null : (Long) value,
                        (a, b) -> a == null ? b.clearLong() : b.setLong(a));
                break;
            case INTEGER:
                column = toColumn(wasNull ? null : (Integer) value,
                        (a, b) -> a == null ? b.clearInteger() : b.setInteger(a));
                break;
            case BOOLEAN:
                column = toColumn(wasNull ? null : (Boolean) value,
                        (a, b) -> a == null ? b.clearBoolean() : b.setBoolean(a));
                break;
            case STRING:
            case ENUM:
                column = toColumn(wasNull ? null : (String) value,
                        (a, b) -> a == null ? b.clearString() : b.setString(a));
                break;
            case BYTES:
            case VERSION:
                column = toColumn(wasNull ? null : (byte[]) value,
                        (a, b) -> a == null ? b.clearBinary() : b.setBinary(ByteString.copyFrom(a)));
                break;
            case DOUBLE:
                column = toColumn(wasNull ? null : (Double) value,
                        (a, b) -> a == null ? b.clearDouble() : b.setDouble(a));
                break;
            case UUID:
                column = toColumn(wasNull ? null : (UUID) value,
                        (a, b) -> a == null ? b.clearUuid() : b.setUuid(Uuid.newBuilder()
                                .setMostSignificantBits(a.getMostSignificantBits())
                                .setLeastSignificantBits(a.getLeastSignificantBits())
                                .build()));
                break;
            default:
                throw new SQLException("DataType: " + field.getType() + " not supported",
                        ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
        }
        return column;
    }

    static DataType.StructType getStructDataType(@Nonnull List<ColumnMetadata> columnMetadataList, boolean nullable) {
        final var structFields = new ArrayList<DataType.StructType.Field>();
        for (int i = 0; i < columnMetadataList.size(); i++) {
            final var colMetadata = columnMetadataList.get(i);
            final var dataType = getDataType(colMetadata.getType(), colMetadata, colMetadata.getNullable());
            structFields.add(DataType.StructType.Field.from(colMetadata.getName(), dataType, i));
        }
        // we do not preserve struct name
        return DataType.StructType.from("ANONYMOUS_STRUCT", structFields, nullable);
    }

    /**
     * Build a Column from <code>p</code>.
     * @param p Particular of the generic to build the Column with (can be null).
     * @param f BiFunction to build column.
     * @param <P> Type we use building Column.
     * @return Column instance made from <code>p</code> whether null or not.
     */
    @VisibleForTesting
    static <P> Column toColumn(P p, BiFunction<P, Column.Builder, Column.Builder> f) {
        return f.apply(p, Column.newBuilder()).build();
    }

    public static ResultSet toProtobuf(RelationalResultSet relationalResultSet) throws SQLException {
        if (relationalResultSet == null) {
            return null;
        }
        var resultSetBuilder = ResultSet.newBuilder();
        while (relationalResultSet.next()) {
            if (!resultSetBuilder.hasMetadata()) {
                resultSetBuilder.setMetadata(toResultSetMetaData(relationalResultSet, relationalResultSet.getMetaData().getColumnCount()));
            }
            resultSetBuilder.addRow(toStruct(relationalResultSet));
        }
        // Set the continuation after all the rows have been traversed
        Continuation existingContinuation = relationalResultSet.getContinuation();
        RpcContinuation rpcContinuation = toContinuation(existingContinuation);
        resultSetBuilder.setContinuation(rpcContinuation);

        return resultSetBuilder.build();
    }

    private static RpcContinuation toContinuation(@Nonnull Continuation existingContinuation) {
        RpcContinuation.Builder builder = RpcContinuation.newBuilder()
                .setVersion(RelationalRpcContinuation.CURRENT_VERSION)
                .setAtBeginning(existingContinuation.atBeginning())
                .setAtEnd(existingContinuation.atEnd());
        // Here, we serialize the entire continuation - this will make it easier to recreate the original once
        // we send it back
        byte[] state = existingContinuation.serialize();
        if (state != null) {
            builder.setInternalState(ByteString.copyFrom(state));
        }
        Continuation.Reason reason = existingContinuation.getReason();
        if (reason != null) {
            builder.setReason(toReason(reason));
        }
        return builder.build();
    }

    public static RpcContinuationReason toReason(Continuation.Reason reason) {
        if (reason == null) {
            return null;
        }
        switch (reason) {
            case TRANSACTION_LIMIT_REACHED:
                return RpcContinuationReason.TRANSACTION_LIMIT_REACHED;
            case QUERY_EXECUTION_LIMIT_REACHED:
                return RpcContinuationReason.QUERY_EXECUTION_LIMIT_REACHED;
            case CURSOR_AFTER_LAST:
                return RpcContinuationReason.CURSOR_AFTER_LAST;
            default:
                throw new IllegalStateException("Unrecognized continuation reason: " + reason);
        }
    }

    public static Continuation.Reason toReason(RpcContinuationReason reason) {
        if (reason == null) {
            return null;
        }
        switch (reason) {
            case TRANSACTION_LIMIT_REACHED:
                return Continuation.Reason.TRANSACTION_LIMIT_REACHED;
            case QUERY_EXECUTION_LIMIT_REACHED:
                return Continuation.Reason.QUERY_EXECUTION_LIMIT_REACHED;
            case CURSOR_AFTER_LAST:
                return Continuation.Reason.CURSOR_AFTER_LAST;
            default:
                throw new IllegalStateException("Unrecognized continuation reason: " + reason);
        }
    }

    @VisibleForTesting
    @SuppressWarnings("unchecked")
    static com.apple.foundationdb.relational.jdbc.grpc.v1.Options.Builder toProtobuf(@Nonnull Options options) throws SQLException {
        final var builder = com.apple.foundationdb.relational.jdbc.grpc.v1.Options.newBuilder();
        // Switched-on by default on JDBC driver until the option is deprecated and removed.
        builder.setContinuationsContainCompiledStatements(true);
        for (Map.Entry<Options.Name, ?> entry : options.entries()) {
            switch (entry.getKey()) {
                case MAX_ROWS:
                    builder.setMaxRows((Integer)entry.getValue());
                    break;
                case CONTINUATION:
                    builder.setContinuation(ByteString.copyFrom(((Continuation)entry.getValue()).serialize()));
                    break;
                case INDEX_HINT:
                    builder.setIndexHint((String)entry.getValue());
                    break;
                case REQUIRED_METADATA_TABLE_VERSION:
                    builder.setRequiredMetadataTableVersion((Integer)entry.getValue());
                    break;
                case TRANSACTION_TIMEOUT:
                    builder.setTransactionTimeout((Long)entry.getValue());
                    break;
                case REPLACE_ON_DUPLICATE_PK:
                    builder.setReplaceOnDuplicatePk((Boolean)entry.getValue());
                    break;
                case PLAN_CACHE_PRIMARY_MAX_ENTRIES:
                    builder.setPlanCachePrimaryMaxEntries((Integer)entry.getValue());
                    break;
                case PLAN_CACHE_SECONDARY_MAX_ENTRIES:
                    builder.setPlanCacheSecondaryMaxEntries((Integer)entry.getValue());
                    break;
                case PLAN_CACHE_TERTIARY_MAX_ENTRIES:
                    builder.setPlanCacheTertiaryMaxEntries((Integer)entry.getValue());
                    break;
                case PLAN_CACHE_PRIMARY_TIME_TO_LIVE_MILLIS:
                    builder.setPlanCachePrimaryTimeToLiveMillis((Long)entry.getValue());
                    break;
                case PLAN_CACHE_SECONDARY_TIME_TO_LIVE_MILLIS:
                    builder.setPlanCacheSecondaryTimeToLiveMillis((Long)entry.getValue());
                    break;
                case PLAN_CACHE_TERTIARY_TIME_TO_LIVE_MILLIS:
                    builder.setPlanCacheTertiaryTimeToLiveMillis((Long)entry.getValue());
                    break;
                case INDEX_FETCH_METHOD:
                    switch ((Options.IndexFetchMethod)entry.getValue()) {
                        case SCAN_AND_FETCH:
                            builder.setIndexFetchMethod(com.apple.foundationdb.relational.jdbc.grpc.v1.Options.IndexFetchMethod.SCAN_AND_FETCH);
                            break;
                        case USE_REMOTE_FETCH:
                            builder.setIndexFetchMethod(com.apple.foundationdb.relational.jdbc.grpc.v1.Options.IndexFetchMethod.USE_REMOTE_FETCH);
                            break;
                        case USE_REMOTE_FETCH_WITH_FALLBACK:
                            builder.setIndexFetchMethod(com.apple.foundationdb.relational.jdbc.grpc.v1.Options.IndexFetchMethod.USE_REMOTE_FETCH_WITH_FALLBACK);
                            break;
                        default:
                            throw new SQLException("Unknown fetch method");
                    }
                    break;
                case DISABLED_PLANNER_RULES:
                    for (String rule : (Collection<String>)entry.getValue()) {
                        builder.addDisabledPlannerRules(rule);
                    }
                    break;
                case DISABLE_PLANNER_REWRITING:
                    builder.setDisablePlannerRewriting((Boolean)entry.getValue());
                    break;
                case LOG_QUERY:
                    builder.setLogQuery((Boolean)entry.getValue());
                    break;
                case LOG_SLOW_QUERY_THRESHOLD_MICROS:
                    builder.setLogSlowQueryThresholdMicros((Long)entry.getValue());
                    break;
                case EXECUTION_TIME_LIMIT:
                    builder.setExecutionTimeLimit((Long)entry.getValue());
                    break;
                case EXECUTION_SCANNED_BYTES_LIMIT:
                    builder.setExecutionScannedBytesLimit((Long)entry.getValue());
                    break;
                case EXECUTION_SCANNED_ROWS_LIMIT:
                    builder.setExecutionScannedRowsLimit((Integer)entry.getValue());
                    break;
                case DRY_RUN:
                    builder.setDryRun((Boolean)entry.getValue());
                    break;
                case CASE_SENSITIVE_IDENTIFIERS:
                    builder.setCaseSensitiveIdentifiers((Boolean)entry.getValue());
                    break;
                case CURRENT_PLAN_HASH_MODE:
                    builder.setCurrentPlanHashMode((String)entry.getValue());
                    break;
                case VALID_PLAN_HASH_MODES:
                    builder.setValidPlanHashModes((String)entry.getValue());
                    break;
                case ASYNC_OPERATIONS_TIMEOUT_MILLIS:
                    builder.setAsyncOperationsTimeoutMillis((Long)entry.getValue());
                    break;
                case ENCRYPT_WHEN_SERIALIZING:
                    builder.setEncryptWhenSerializing((Boolean)entry.getValue());
                    break;
                case ENCRYPTION_KEY_STORE:
                    if (Options.isNull(entry.getValue())) {
                        builder.clearEncryptionKeyStore();
                    } else {
                        builder.setEncryptionKeyStore((String)entry.getValue());
                    }
                    break;
                case ENCRYPTION_KEY_ENTRY:
                    if (Options.isNull(entry.getValue())) {
                        builder.clearEncryptionKeyEntry();
                    } else {
                        builder.setEncryptionKeyEntry((String)entry.getValue());
                    }
                    break;
                case ENCRYPTION_KEY_ENTRY_LIST:
                    for (String rule : (List<String>)entry.getValue()) {
                        builder.addEncryptionKeyEntryList(rule);
                    }
                    break;
                case ENCRYPTION_KEY_PASSWORD:
                    if (Options.isNull(entry.getValue())) {
                        builder.clearEncryptionKeyPassword();
                    } else {
                        builder.setEncryptionKeyPassword((String)entry.getValue());
                    }
                    break;
                case COMPRESS_WHEN_SERIALIZING:
                    builder.setCompressWhenSerializing((Boolean)entry.getValue());
                    break;
                default:
                    throw new SQLException("Cannot encode option in protobuf");
            }
        }
        return builder;
    }

    public static Options fromProtobuf(com.apple.foundationdb.relational.jdbc.grpc.v1.Options protoOptions) throws SQLException {
        final Options.Builder builder = Options.builder();
        if (protoOptions.hasMaxRows()) {
            builder.withOption(Options.Name.MAX_ROWS, protoOptions.getMaxRows());
        }
        if (protoOptions.hasContinuation()) {
            // TODO: The Impl class lives in relational-core.
            builder.withOption(Options.Name.CONTINUATION, protoOptions.getContinuation().toByteArray());
        }
        if (protoOptions.hasIndexHint()) {
            builder.withOption(Options.Name.INDEX_HINT, protoOptions.getIndexHint());
        }
        if (protoOptions.hasRequiredMetadataTableVersion()) {
            builder.withOption(Options.Name.REQUIRED_METADATA_TABLE_VERSION, protoOptions.getRequiredMetadataTableVersion());
        }
        if (protoOptions.hasTransactionTimeout()) {
            builder.withOption(Options.Name.TRANSACTION_TIMEOUT, protoOptions.getTransactionTimeout());
        }
        if (protoOptions.hasReplaceOnDuplicatePk()) {
            builder.withOption(Options.Name.REPLACE_ON_DUPLICATE_PK, protoOptions.getReplaceOnDuplicatePk());
        }
        if (protoOptions.hasPlanCachePrimaryMaxEntries()) {
            builder.withOption(Options.Name.PLAN_CACHE_PRIMARY_MAX_ENTRIES, protoOptions.getPlanCachePrimaryMaxEntries());
        }
        if (protoOptions.hasPlanCacheSecondaryMaxEntries()) {
            builder.withOption(Options.Name.PLAN_CACHE_SECONDARY_MAX_ENTRIES, protoOptions.getPlanCacheSecondaryMaxEntries());
        }
        if (protoOptions.hasPlanCacheTertiaryMaxEntries()) {
            builder.withOption(Options.Name.PLAN_CACHE_TERTIARY_MAX_ENTRIES, protoOptions.getPlanCacheTertiaryMaxEntries());
        }
        if (protoOptions.hasPlanCachePrimaryTimeToLiveMillis()) {
            builder.withOption(Options.Name.PLAN_CACHE_PRIMARY_TIME_TO_LIVE_MILLIS, protoOptions.getPlanCachePrimaryTimeToLiveMillis());
        }
        if (protoOptions.hasPlanCacheSecondaryTimeToLiveMillis()) {
            builder.withOption(Options.Name.PLAN_CACHE_SECONDARY_TIME_TO_LIVE_MILLIS, protoOptions.getPlanCacheSecondaryTimeToLiveMillis());
        }
        if (protoOptions.hasPlanCacheTertiaryTimeToLiveMillis()) {
            builder.withOption(Options.Name.PLAN_CACHE_TERTIARY_TIME_TO_LIVE_MILLIS, protoOptions.getPlanCacheTertiaryTimeToLiveMillis());
        }
        if (protoOptions.hasIndexFetchMethod()) {
            Options.IndexFetchMethod indexFetchMethod;
            switch (protoOptions.getIndexFetchMethod()) {
                case SCAN_AND_FETCH:
                    indexFetchMethod = Options.IndexFetchMethod.SCAN_AND_FETCH;
                    break;
                case USE_REMOTE_FETCH:
                    indexFetchMethod = Options.IndexFetchMethod.USE_REMOTE_FETCH;
                    break;
                case USE_REMOTE_FETCH_WITH_FALLBACK:
                    indexFetchMethod = Options.IndexFetchMethod.USE_REMOTE_FETCH_WITH_FALLBACK;
                    break;
                default:
                    throw new SQLException("Unknown fetch method");
            }
            builder.withOption(Options.Name.INDEX_FETCH_METHOD, indexFetchMethod);
        }
        if (protoOptions.getDisabledPlannerRulesCount() > 0) {
            builder.withOption(Options.Name.DISABLED_PLANNER_RULES, protoOptions.getDisabledPlannerRulesList());
        }
        if (protoOptions.hasDisablePlannerRewriting()) {
            builder.withOption(Options.Name.DISABLE_PLANNER_REWRITING, protoOptions.getDisablePlannerRewriting());
        }
        if (protoOptions.hasLogQuery()) {
            builder.withOption(Options.Name.LOG_QUERY, protoOptions.getLogQuery());
        }
        if (protoOptions.hasLogSlowQueryThresholdMicros()) {
            builder.withOption(Options.Name.LOG_SLOW_QUERY_THRESHOLD_MICROS, protoOptions.getLogSlowQueryThresholdMicros());
        }
        if (protoOptions.hasExecutionTimeLimit()) {
            builder.withOption(Options.Name.EXECUTION_TIME_LIMIT, protoOptions.getExecutionTimeLimit());
        }
        if (protoOptions.hasExecutionScannedBytesLimit()) {
            builder.withOption(Options.Name.EXECUTION_SCANNED_BYTES_LIMIT, protoOptions.getExecutionScannedBytesLimit());
        }
        if (protoOptions.hasExecutionScannedRowsLimit()) {
            builder.withOption(Options.Name.EXECUTION_SCANNED_ROWS_LIMIT, protoOptions.getExecutionScannedRowsLimit());
        }
        if (protoOptions.hasDryRun()) {
            builder.withOption(Options.Name.DRY_RUN, protoOptions.getDryRun());
        }
        if (protoOptions.hasCaseSensitiveIdentifiers()) {
            builder.withOption(Options.Name.CASE_SENSITIVE_IDENTIFIERS, protoOptions.getCaseSensitiveIdentifiers());
        }
        if (protoOptions.hasCurrentPlanHashMode()) {
            builder.withOption(Options.Name.CURRENT_PLAN_HASH_MODE, protoOptions.getCurrentPlanHashMode());
        }
        if (protoOptions.hasValidPlanHashModes()) {
            builder.withOption(Options.Name.VALID_PLAN_HASH_MODES, protoOptions.getValidPlanHashModes());
        }
        if (protoOptions.hasContinuationsContainCompiledStatements() && !protoOptions.getContinuationsContainCompiledStatements()) {
            throw new RelationalException("Option CONTINUATIONS_CONTAIN_COMPILED_STATEMENTS=false not supported anymore!", ErrorCode.UNSUPPORTED_OPERATION).toUncheckedWrappedException();
        }
        if (protoOptions.hasAsyncOperationsTimeoutMillis()) {
            builder.withOption(Options.Name.ASYNC_OPERATIONS_TIMEOUT_MILLIS, protoOptions.getAsyncOperationsTimeoutMillis());
        }
        if (protoOptions.hasEncryptWhenSerializing()) {
            builder.withOption(Options.Name.ENCRYPT_WHEN_SERIALIZING, protoOptions.getEncryptWhenSerializing());
        }
        if (protoOptions.hasEncryptionKeyStore()) {
            builder.withOption(Options.Name.ENCRYPTION_KEY_STORE, protoOptions.getEncryptionKeyStore());
        }
        if (protoOptions.hasEncryptionKeyEntry()) {
            builder.withOption(Options.Name.ENCRYPTION_KEY_ENTRY, protoOptions.getEncryptionKeyEntry());
        }
        if (protoOptions.getEncryptionKeyEntryListCount() > 0) {
            builder.withOption(Options.Name.ENCRYPTION_KEY_ENTRY_LIST, protoOptions.getEncryptionKeyEntryListList());
        }
        if (protoOptions.hasEncryptionKeyPassword()) {
            builder.withOption(Options.Name.ENCRYPTION_KEY_PASSWORD, protoOptions.getEncryptionKeyPassword());
        }
        if (protoOptions.hasCompressWhenSerializing()) {
            builder.withOption(Options.Name.COMPRESS_WHEN_SERIALIZING, protoOptions.getCompressWhenSerializing());
        }
        return builder.build();
    }

    static int toSqlType(Type type) throws SQLException {
        switch (type) {
            case INTEGER:
                return Types.INTEGER;
            case LONG:
                return Types.BIGINT;
            case STRING:
                return Types.VARCHAR;
            case ENUM:
            case UUID:
                return Types.OTHER;
            case BOOLEAN:
                return Types.BOOLEAN;
            case ARRAY:
                return Types.ARRAY;
            case STRUCT:
                return Types.STRUCT;
            case BYTES:
                return Types.BINARY;
            case FLOAT:
                return Types.FLOAT;
            case DOUBLE:
                return Types.DOUBLE;
            default:
                throw new SQLException("JDBC Type: " + type + " not supported");
        }
    }

    private static DataType.EnumType getEnumDataType(@Nonnull EnumMetadata enumMetadata, boolean nullable) {
        final var enumValues = new ArrayList<DataType.EnumType.EnumValue>();
        int i = 1;
        for (var value: enumMetadata.getValuesList()) {
            enumValues.add(DataType.EnumType.EnumValue.of(value, i++));
        }
        return DataType.EnumType.from(enumMetadata.getName(), enumValues, nullable);
    }

    static DataType getDataType(@Nonnull Type type, @Nonnull ColumnMetadata columnMetadata, boolean nullable) {
        switch (type) {
            case LONG:
                return nullable ? DataType.Primitives.NULLABLE_LONG.type() : DataType.Primitives.LONG.type();
            case INTEGER:
                return nullable ? DataType.Primitives.NULLABLE_INTEGER.type() : DataType.Primitives.INTEGER.type();
            case DOUBLE:
                return nullable ? DataType.Primitives.NULLABLE_DOUBLE.type() : DataType.Primitives.DOUBLE.type();
            case FLOAT:
                return nullable ? DataType.Primitives.NULLABLE_FLOAT.type() : DataType.Primitives.FLOAT.type();
            case BOOLEAN:
                return nullable ? DataType.Primitives.NULLABLE_BOOLEAN.type() : DataType.Primitives.BOOLEAN.type();
            case BYTES:
                return nullable ? DataType.Primitives.NULLABLE_BYTES.type() : DataType.Primitives.BYTES.type();
            case UUID:
                return nullable ? DataType.Primitives.NULLABLE_UUID.type() : DataType.Primitives.UUID.type();
            case STRING:
                return nullable ? DataType.Primitives.NULLABLE_STRING.type() : DataType.Primitives.STRING.type();
            case VERSION:
                return nullable ? DataType.Primitives.NULLABLE_VERSION.type() : DataType.Primitives.VERSION.type();
            case STRUCT:
                return getStructDataType(columnMetadata.getStructMetadata().getColumnMetadataList(), nullable);
            case ENUM:
                return getEnumDataType(columnMetadata.getEnumMetadata(), nullable);
            case ARRAY:
                final var arrayMetadata = columnMetadata.getArrayMetadata();
                return DataType.ArrayType.from(getDataType(arrayMetadata.getType(), arrayMetadata, arrayMetadata.getNullable()), nullable);
            default:
                throw new RelationalException("Not implemeneted: " + type.name(), ErrorCode.INTERNAL_ERROR).toUncheckedWrappedException();
        }
    }
}
