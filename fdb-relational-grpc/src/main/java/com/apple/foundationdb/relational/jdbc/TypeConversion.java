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
import com.apple.foundationdb.relational.api.SqlTypeNamesSupport;
import com.apple.foundationdb.relational.api.StructMetaData;
import com.apple.foundationdb.relational.api.RelationalArray;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.RelationalResultSetMetaData;
import com.apple.foundationdb.relational.api.RelationalStruct;
import com.apple.foundationdb.relational.api.RelationalStructMetaData;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.jdbc.grpc.v1.KeySet;
import com.apple.foundationdb.relational.jdbc.grpc.v1.KeySetValue;
import com.apple.foundationdb.relational.jdbc.grpc.v1.ResultSet;
import com.apple.foundationdb.relational.jdbc.grpc.v1.RpcContinuationReason;
import com.apple.foundationdb.relational.jdbc.grpc.v1.ResultSetMetadata;
import com.apple.foundationdb.relational.jdbc.grpc.v1.RpcContinuation;
import com.apple.foundationdb.relational.jdbc.grpc.v1.column.Array;
import com.apple.foundationdb.relational.jdbc.grpc.v1.column.Column;
import com.apple.foundationdb.relational.jdbc.grpc.v1.column.ColumnMetadata;
import com.apple.foundationdb.relational.jdbc.grpc.v1.column.ListColumn;
import com.apple.foundationdb.relational.jdbc.grpc.v1.column.ListColumnMetadata;
import com.apple.foundationdb.relational.jdbc.grpc.v1.column.Struct;
import com.apple.foundationdb.relational.util.PositionalIndex;
import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.ByteString;

import javax.annotation.Nonnull;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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
     * @param resultSet Protobuf ResultSet to fetch {@link RelationalStruct} from.
     * @param rowIndex Current 'row' offset into <code>resultSet</code>
     * @param oneBasedColumn One-based column index where we'll find the {@link RelationalStruct} instance.
     * @return {@link RelationalStruct} instance pulled from <code>resultSet</code>
     * @throws SQLException If failed get of <code>resultSet</code> metadata.
     */
    // Manipulation of protobufs. Exploits package-private internal ottributes of {@link RelationalStructFacade}.
    static RelationalStruct getStruct(ResultSet resultSet, int rowIndex, int oneBasedColumn) throws SQLException {
        int index = PositionalIndex.toProtobuf(oneBasedColumn);
        var metadata =
                resultSet.getMetadata().getColumnMetadata().getColumnMetadata(index).getStructMetadata();
        Column column = resultSet.getRow(rowIndex).getColumns().getColumn(index);
        return column.hasStruct() ? new RelationalStructFacade(metadata, column.getStruct()) : null;
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

    private static ColumnMetadata toColumnMetadata(StructMetaData metadata, int oneBasedIndex)
            throws SQLException {
        var columnMetadataBuilder = ColumnMetadata.newBuilder()
                .setName(metadata.getColumnName(oneBasedIndex))
                .setJavaSqlTypesCode(metadata.getColumnType(oneBasedIndex));
        // TODO nullable.
        // TODO phantom.
        // TODO: label
        // One-offs
        switch (metadata.getColumnType(oneBasedIndex)) {
            case Types.STRUCT:
                var listColumnMetadata = toListColumnMetadataProtobuf((RelationalStructMetaData) metadata.getStructMetaData(oneBasedIndex));
                columnMetadataBuilder.setStructMetadata(listColumnMetadata);
                break;
            case Types.ARRAY:
                var columnMetadata = toColumnMetadata(metadata.getArrayMetaData(oneBasedIndex));
                columnMetadataBuilder.setArrayMetadata(columnMetadata);
                break;
            default:
                break;
        }
        return columnMetadataBuilder.build();
    }

    /**
     * The below is about making an Array.
     */
    private static ColumnMetadata toColumnMetadata(@Nonnull ArrayMetaData metadata)
            throws SQLException {
        var columnMetadataBuilder = ColumnMetadata.newBuilder()
                .setName(metadata.getElementName())
                .setJavaSqlTypesCode(metadata.getElementType());
        // TODO nullable.
        // TODO phantom.
        // TODO: label
        // One-offs
        switch (metadata.getElementType()) {
            case Types.STRUCT:
                var listColumnMetadata = toListColumnMetadataProtobuf((RelationalStructMetaData) metadata.getElementStructMetaData());
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

    private static ListColumnMetadata toListColumnMetadataProtobuf(@Nonnull RelationalStructMetaData metadata) throws SQLException {
        var listColumnMetadataBuilder = ListColumnMetadata.newBuilder();
        for (int oneBasedIndex = 1; oneBasedIndex <= metadata.getColumnCount(); oneBasedIndex++) {
            var columnMetadata = toColumnMetadata(metadata, oneBasedIndex);
            listColumnMetadataBuilder.addColumnMetadata(columnMetadata);
        }
        return listColumnMetadataBuilder.build();
    }

    private static ResultSetMetadata toResultSetMetaData(RelationalResultSetMetaData metadata, int columnCount) throws SQLException {
        var listColumnMetadataBuilder = ListColumnMetadata.newBuilder();
        for (int oneBasedIndex = 1; oneBasedIndex <= columnCount; oneBasedIndex++) {
            listColumnMetadataBuilder.addColumnMetadata(toColumnMetadata(metadata, oneBasedIndex));
        }
        return ResultSetMetadata.newBuilder().setColumnMetadata(listColumnMetadataBuilder.build()).build();
    }

    private static Array toArray(RelationalArray relationalArray) throws SQLException {
        var arrayBuilder = Array.newBuilder();
        if (relationalArray != null) {
            var relationalResultSet = relationalArray.getResultSet();
            while (relationalResultSet.next()) {
                arrayBuilder.addElement(toColumn(relationalResultSet, 2));
            }
        }
        return arrayBuilder.build();
    }

    private static Struct toStruct(RelationalStruct relationalStruct) throws SQLException {
        // TODO: The call to get metadata below is expensive? And all we want is column count. Revisit.
        var listColumnBuilder = ListColumn.newBuilder();
        for (int oneBasedIndex = 1; oneBasedIndex <= relationalStruct.getMetaData().getColumnCount(); oneBasedIndex++) {
            listColumnBuilder.addColumn(toColumn(relationalStruct, oneBasedIndex));
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
                // TODO: NULL?
                throw new SQLException("java.sql.Type=" + columnType + " not supported",
                        ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
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
        if (columnType != SqlTypeNamesSupport.getSqlTypeCodeFromObject(obj)) {
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

    private static Column toColumn(RelationalStruct relationalStruct, int oneBasedIndex) throws SQLException {
        int columnType = relationalStruct.getMetaData().getColumnType(oneBasedIndex);
        Column column;
        switch (columnType) {
            case Types.STRUCT:
                RelationalStruct struct = relationalStruct.getStruct(oneBasedIndex);
                column = toColumn(struct == null ? null : toStruct(struct),
                        (a, b) -> a == null ? b.clearStruct() : b.setStruct(a));
                break;
            case Types.ARRAY:
                RelationalArray array = relationalStruct.getArray(oneBasedIndex);
                column = toColumn(array == null ? null : toArray(array),
                        (a, b) -> a == null ? b.clearArray() : b.setArray(a));
                break;
            case Types.BIGINT:
                long l = relationalStruct.getLong(oneBasedIndex);
                column = toColumn(relationalStruct.wasNull() ? null : l,
                        (a, b) -> a == null ? b.clearLong() : b.setLong(a));
                break;
            case Types.INTEGER:
                int i = relationalStruct.getInt(oneBasedIndex);
                column = toColumn(relationalStruct.wasNull() ? null : i,
                        (a, b) -> a == null ? b.clearInteger() : b.setInteger(a));
                break;
            case Types.BOOLEAN:
                boolean bool = relationalStruct.getBoolean(oneBasedIndex);
                column = toColumn(relationalStruct.wasNull() ? null : bool,
                        (a, b) -> a == null ? b.clearBoolean() : b.setBoolean(a));
                break;
            case Types.VARCHAR:
                column = toColumn(relationalStruct.getString(oneBasedIndex),
                        (a, b) -> a == null ? b.clearString() : b.setString(a));
                break;
            case Types.BINARY:
                column = toColumn(relationalStruct.getBytes(oneBasedIndex),
                        (a, b) -> a == null ? b.clearBinary() : b.setBinary(ByteString.copyFrom(a)));
                break;
            case Types.DOUBLE:
                double d = relationalStruct.getDouble(oneBasedIndex);
                column = toColumn(relationalStruct.wasNull() ? null : d,
                        (a, b) -> a == null ? b.clearDouble() : b.setDouble(a));
                break;
            default:
                throw new SQLException("java.sql.Type=" + columnType + " not supported",
                        ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
        }
        return column;
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

    /**
     * Map a Relational-Core ResultSet row to a protobuf Struct.
     */
    private static Struct toRow(RelationalResultSet relationalResultSet) throws SQLException {
        var listColumnBuilder = ListColumn.newBuilder();
        for (int oneBasedIndex = 1; oneBasedIndex <= relationalResultSet.getMetaData().getColumnCount();
                oneBasedIndex++) {
            listColumnBuilder.addColumn(toColumn(relationalResultSet, oneBasedIndex));
        }
        return Struct.newBuilder().setColumns(listColumnBuilder.build()).build();
    }

    public static ResultSet toProtobuf(RelationalResultSet relationalResultSet) throws SQLException {
        if (relationalResultSet == null) {
            return null;
        }
        var resultSetBuilder = ResultSet.newBuilder();
        var metadata = relationalResultSet.getMetaData();
        while (relationalResultSet.next()) {
            if (!resultSetBuilder.hasMetadata()) {
                resultSetBuilder.setMetadata(toResultSetMetaData(relationalResultSet.getMetaData(), metadata.getColumnCount()));
            }
            resultSetBuilder.addRow(toRow(relationalResultSet));
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
}
