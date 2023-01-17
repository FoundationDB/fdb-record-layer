/*
 * ResultSetProtobuf.java
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

package com.apple.foundationdb.relational.server.jdbc.v1;

import com.apple.foundationdb.relational.api.RelationalArray;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.RelationalStruct;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.grpc.jdbc.v1.ResultSet;
import com.apple.foundationdb.relational.grpc.jdbc.v1.ResultSetMetadata;
import com.apple.foundationdb.relational.grpc.jdbc.v1.column.Array;
import com.apple.foundationdb.relational.grpc.jdbc.v1.column.Column;
import com.apple.foundationdb.relational.grpc.jdbc.v1.column.ColumnMetadata;
import com.apple.foundationdb.relational.grpc.jdbc.v1.column.ListColumn;
import com.apple.foundationdb.relational.grpc.jdbc.v1.column.ListColumnMetadata;
import com.apple.foundationdb.relational.grpc.jdbc.v1.column.Struct;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.ByteString;

import java.sql.SQLException;
import java.sql.Types;
import java.util.function.BiFunction;

/**
 * Package-private ResultSet Protobuf conversions.
 * High-level, all metadata is stored to ResultSetMetaData -- even the metadata on Structs and Arrays: i.e. metadata
 * and data are kept separately when we protobuf ResultSets (We'll save not repeating metadata per Struct/Array
 * instance).
 */
class ResultSetProtobuf {
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
                var listColumnMetadata = toListColumnMetadata(relationalStruct.getStruct(oneBasedIndex));
                columnMetadataBuilder.setStruct(listColumnMetadata);
                break;
            case Types.ARRAY:
                var columnMetadata = toColumnMetadata(relationalStruct.getArray(oneBasedIndex),
                        metadata.getColumnName(oneBasedIndex));
                columnMetadataBuilder.setArray(columnMetadata);
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
    private static ColumnMetadata toColumnMetadata(RelationalArray relationalArray, String name) throws SQLException {
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
                listColumnMetadataBuilder.addColumns(columnMetadata);
            }
            columnMetadataBuilder.setStruct(listColumnMetadataBuilder.build());
        }
        return columnMetadataBuilder.build();
    }

    private static ListColumnMetadata toListColumnMetadata(RelationalStruct relationalStruct) throws SQLException {
        var metadata = relationalStruct.getMetaData();
        var listColumnMetadataBuilder = ListColumnMetadata.newBuilder();
        for (int oneBasedIndex = 1; oneBasedIndex <= metadata.getColumnCount(); oneBasedIndex++) {
            var columnMetadata = toColumnMetadata(relationalStruct, oneBasedIndex);
            listColumnMetadataBuilder.addColumns(columnMetadata);
        }
        return listColumnMetadataBuilder.build();
    }

    private static ResultSetMetadata toResultSetMetaData(RelationalResultSet relationalResultSet) throws SQLException {
        var metadata = relationalResultSet.getMetaData();
        var resultSetMetadataBuilder = ResultSetMetadata.newBuilder();
        for (int oneBasedIndex = 1; oneBasedIndex <= metadata.getColumnCount(); oneBasedIndex++) {
            var columnMetadata = toColumnMetadata(relationalResultSet, oneBasedIndex);
            resultSetMetadataBuilder.addColumns(columnMetadata);
        }
        return resultSetMetadataBuilder.build();
    }

    private static Array toArray(RelationalArray relationalArray) throws SQLException {
        var arrayBuilder = Array.newBuilder();
        if (relationalArray != null) {
            var relationalResultSet = relationalArray.getResultSet();
            var relationalResultSetMetaData = relationalResultSet.getMetaData();
            while (relationalResultSet.next()) {
                var stuctBuilder = Struct.newBuilder();
                for (int oneBasedColumn = 1; oneBasedColumn <= relationalResultSetMetaData.getColumnCount(); oneBasedColumn++) {
                    var column = toColumn(relationalResultSet, oneBasedColumn);
                    if (column != null) {
                        stuctBuilder.addColumns(column);
                    }
                }
                arrayBuilder.addElements(stuctBuilder.build());
            }
        }
        return arrayBuilder.build();
    }

    private static Struct toStruct(RelationalStruct relationalStruct) throws SQLException {
        var metaData = relationalStruct.getMetaData();
        var structBuilder = Struct.newBuilder();
        for (int oneBasedIndex = 1; oneBasedIndex <= metaData.getColumnCount(); oneBasedIndex++) {
            var column = toColumn(relationalStruct, oneBasedIndex);
            if (column != null) {
                structBuilder.addColumns(column);
            }
        }
        return structBuilder.build();
    }

    private static Column toColumn(RelationalStruct relationalStruct, int oneBasedIndex) throws SQLException {
        var metaData = relationalStruct.getMetaData();
        int columnType = metaData.getColumnType(oneBasedIndex);
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
                column = toColumn(relationalStruct.getLong(oneBasedIndex),
                        (a, b) -> a == null ? b.clearLong() : b.setLong((Long) a));
                break;
            case Types.BOOLEAN:
                column = toColumn(relationalStruct.getBoolean(oneBasedIndex),
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

    private static ListColumn mapRow(RelationalResultSet relationalResultSet) throws SQLException {
        var metaData = relationalResultSet.getMetaData();
        var listColumnBuilder = ListColumn.newBuilder();
        for (int oneBasedIndex = 1; oneBasedIndex <= metaData.getColumnCount(); oneBasedIndex++) {
            var column = toColumn(relationalResultSet, oneBasedIndex);
            if (column != null) {
                listColumnBuilder.addColumns(column);
            }

        }
        return listColumnBuilder.build();
    }

    /**
     * Map RelationalResultSet to protobuf ResultSet.
     */
    static ResultSet map(RelationalResultSet relationalResultSet) throws SQLException {
        var resultSetBuilder = ResultSet.newBuilder();
        if (relationalResultSet.next()) {
            // Build up the metadata for this ResultSet in protobuf.
            resultSetBuilder.setMetadata(toResultSetMetaData(relationalResultSet));

            // Fill out the row data row for current resultset.
            resultSetBuilder.addRows(mapRow(relationalResultSet));

            // Add the data from the remaining rows if any.
            while (relationalResultSet.next()) {
                resultSetBuilder.addRows(mapRow(relationalResultSet));
            }
        }
        return resultSetBuilder.build();
    }
}
