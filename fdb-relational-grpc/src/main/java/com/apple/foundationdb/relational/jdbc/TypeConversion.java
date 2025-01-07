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
import com.apple.foundationdb.relational.jdbc.grpc.v1.ResultSetContinuation;
import com.apple.foundationdb.relational.jdbc.grpc.v1.ResultSetContinuationReason;
import com.apple.foundationdb.relational.jdbc.grpc.v1.ResultSetMetadata;
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
        // TODO: we may go over the cursor by 1 here?
        Continuation existingContinuation = relationalResultSet.getContinuation();
        ResultSetContinuation rpcContinuation = toContinuation(existingContinuation);
        if (rpcContinuation != null) {
            resultSetBuilder.setContinuation(rpcContinuation);
        }

        return resultSetBuilder.build();
    }

    private static ResultSetContinuation toContinuation(Continuation existingContinuation) {
        if (existingContinuation == null) {
            return null;
        } else if (existingContinuation.atBeginning()) {
            return RelationalGrpcContinuation.BEGIN.getProto();
        } else if (existingContinuation.atEnd()) {
            return RelationalGrpcContinuation.END.getProto();
        } else {
            return ResultSetContinuation.newBuilder()
                    .setVersion(RelationalGrpcContinuation.CURRENT_VERSION)
                    // Here, we serialize the entire continuation - this will make it easier to recreate the original once
                    // we get it back
                    .setInternalState(ByteString.copyFrom(existingContinuation.serialize()))
                    .setReason(toReason(existingContinuation.getReason()))
                    .build();
        }
    }

    public static ResultSetContinuationReason toReason(Continuation.Reason reason) {
        if (reason == null) {
            return null;
        }
        switch (reason) {
            case TRANSACTION_LIMIT_REACHED:
                return ResultSetContinuationReason.TRANSACTION_LIMIT_REACHED;
            case QUERY_EXECUTION_LIMIT_REACHED:
                return ResultSetContinuationReason.QUERY_EXECUTION_LIMIT_REACHED;
            case CURSOR_AFTER_LAST:
                return ResultSetContinuationReason.CURSOR_AFTER_LAST;
            default:
                throw new IllegalStateException("Unrecognized continuation reason: " + reason);
        }
    }

    public static Continuation.Reason toReason(ResultSetContinuationReason reason) {
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
