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

import com.apple.foundationdb.relational.api.RelationalArray;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.RelationalStruct;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.jdbc.grpc.v1.KeySet;
import com.apple.foundationdb.relational.jdbc.grpc.v1.KeySetValue;
import com.apple.foundationdb.relational.jdbc.grpc.v1.ResultSet;
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
    public static List<RelationalStruct> fromResultSetProtobuf(ResultSet data) {
        int rowCount = data.getRowCount();
        List<RelationalStruct> structs = new ArrayList<>(rowCount);
        for (int i = 0; i < rowCount; i++) {
            structs.add(new RelationalStructFacade(data.getMetadata().getColumnMetadata(), data.getRow(i++)));
        }
        return structs;
    }

    static KeySet toProtobuf(com.apple.foundationdb.relational.api.KeySet keySet) {
        KeySet.Builder keySetBuilder = KeySet.newBuilder();
        for (Map.Entry<String, Object> entry : keySet.toMap().entrySet()) {
            KeySetValue keySetValue = null;
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

    private static ColumnMetadata toColumnMetadata(RelationalStruct relationalStruct, int oneBasedIndex)
            throws SQLException {
        var metadata = relationalStruct.getMetaData();
        var columnMetadataBuilder = ColumnMetadata.newBuilder()
                .setName(metadata.getColumnName(oneBasedIndex))
                .setJavaSqlTypesCode(metadata.getColumnType(oneBasedIndex));
        // TODO nullable.
        // TODO phantom.
        // TODO: label
        // One-offs
        switch (metadata.getColumnType(oneBasedIndex)) {
            case Types.STRUCT:
                var listColumnMetadata = toListColumnMetadataProtobuf(relationalStruct.getStruct(oneBasedIndex));
                columnMetadataBuilder.setStructMetadata(listColumnMetadata);
                break;
            case Types.ARRAY:
                var columnMetadata = toColumnMetadata(relationalStruct.getArray(oneBasedIndex),
                        metadata.getColumnName(oneBasedIndex));
                columnMetadataBuilder.setArrayMetadata(columnMetadata);
                break;
            default:
                break;
        }
        return columnMetadataBuilder.build();
    }

    /**
     * The below is about making an Array of Structs.
     * From RowArray#getBaseType: "...In Relational, the contents of an Array is _always_ a Struct..."
     */
    private static ColumnMetadata toColumnMetadata(RelationalArray relationalArray, String name)
            throws SQLException {
        var columnMetadataBuilder = ColumnMetadata.newBuilder()
                .setName(name)
                .setJavaSqlTypesCode(Types.STRUCT);
        // TODO nullable.
        // TODO phantom.
        // TODO: label
        // The passed in array can be null. Maybe because accounting of metadata is off. Lets check for now.
        if (relationalArray != null) {
            RelationalResultSet relationalResultSet = relationalArray.getResultSet();
            var metadata = relationalResultSet.getMetaData();
            var listColumnMetadataBuilder = ListColumnMetadata.newBuilder();
            for (int oneBasedIndex = 1; oneBasedIndex <= metadata.getColumnCount(); oneBasedIndex++) {
                var columnMetadata = toColumnMetadata(relationalResultSet, oneBasedIndex);
                listColumnMetadataBuilder.addColumnMetadata(columnMetadata);
            }
            columnMetadataBuilder.setStructMetadata(listColumnMetadataBuilder.build());
        }
        return columnMetadataBuilder.build();
    }

    private static ListColumnMetadata toListColumnMetadataProtobuf(RelationalStruct relationalStruct) throws SQLException {
        var listColumnMetadataBuilder = ListColumnMetadata.newBuilder();
        if (relationalStruct != null) {
            var metadata = relationalStruct.getMetaData();
            for (int oneBasedIndex = 1; oneBasedIndex <= metadata.getColumnCount(); oneBasedIndex++) {
                var columnMetadata = toColumnMetadata(relationalStruct, oneBasedIndex);
                listColumnMetadataBuilder.addColumnMetadata(columnMetadata);
            }
        }
        return listColumnMetadataBuilder.build();
    }

    private static ResultSetMetadata toResultSetMetaData(RelationalResultSet relationalResultSet, int columnCount)
            throws SQLException {
        var listColumnMetadataBuilder = ListColumnMetadata.newBuilder();
        for (int oneBasedIndex = 1; oneBasedIndex <= columnCount; oneBasedIndex++) {
            listColumnMetadataBuilder.addColumnMetadata(toColumnMetadata(relationalResultSet, oneBasedIndex));
        }
        return ResultSetMetadata.newBuilder().setColumnMetadata(listColumnMetadataBuilder.build()).build();
    }

    private static Array toArray(RelationalArray relationalArray) throws SQLException {
        var arrayBuilder = Array.newBuilder();
        if (relationalArray != null) {
            var relationalResultSet = relationalArray.getResultSet();
            while (relationalResultSet.next()) {
                arrayBuilder.addElement(toStruct(relationalResultSet));
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
        Column column = null;
        switch (columnType) {
            case Types.STRUCT:
                RelationalStruct struct = relationalStruct.getStruct(oneBasedIndex);
                column = toColumn(struct == null ? null : toStruct(struct),
                        (a, b) -> a == null ? b.clearStruct() : b.setStruct((Struct) a));
                break;
            case Types.ARRAY:
                RelationalArray array = relationalStruct.getArray(oneBasedIndex);
                column = toColumn(array == null ? null : toArray(array),
                        (a, b) -> a == null ? b.clearArray() : b.setArray((Array) a));
                break;
            case Types.BIGINT:
                long l = relationalStruct.getLong(oneBasedIndex);
                column = toColumn(relationalStruct.wasNull() ? null : l,
                        (a, b) -> a == null ? b.clearLong() : b.setLong((Long) a));
                break;
            case Types.INTEGER:
                int i = relationalStruct.getInt(oneBasedIndex);
                column = toColumn(relationalStruct.wasNull() ? null : i,
                        (a, b) -> a == null ? b.clearInteger() : b.setInteger((Integer) a));
                break;
            case Types.BOOLEAN:
                boolean bool = relationalStruct.getBoolean(oneBasedIndex);
                column = toColumn(relationalStruct.wasNull() ? null : bool,
                        (a, b) -> a == null ? b.clearBoolean() : b.setBoolean((Boolean) a));
                break;
            case Types.VARCHAR:
                column = toColumn(relationalStruct.getString(oneBasedIndex),
                        (a, b) -> a == null ? b.clearString() : b.setString(a));
                break;
            case Types.BINARY:
                column = toColumn(relationalStruct.getBytes(oneBasedIndex),
                        (a, b) -> a == null ? b.clearBinary() : b.setBinary(ByteString.copyFrom((byte[]) a)));
                break;
            case Types.DOUBLE:
                double d = relationalStruct.getDouble(oneBasedIndex);
                column = toColumn(relationalStruct.wasNull() ? null : d,
                        (a, b) -> a == null ? b.clearDouble() : b.setDouble((Double) a));
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
     * @return Column instance made from <code>p</code> whether null or not.
     * @param <P> Type we use building Column.
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
                resultSetBuilder.setMetadata(toResultSetMetaData(relationalResultSet, metadata.getColumnCount()));
            }
            resultSetBuilder.addRow(toRow(relationalResultSet));
        }
        return resultSetBuilder.build();
    }
}
