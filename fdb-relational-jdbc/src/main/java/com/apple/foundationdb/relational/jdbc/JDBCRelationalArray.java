/*
 * JDBCRelationalArray.java
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

import com.apple.foundationdb.relational.api.Continuation;
import com.apple.foundationdb.relational.api.SqlTypeNamesSupport;
import com.apple.foundationdb.relational.api.StructMetaData;
import com.apple.foundationdb.relational.api.RelationalArray;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.RelationalResultSetMetaData;
import com.apple.foundationdb.relational.api.RelationalStruct;
import com.apple.foundationdb.relational.grpc.jdbc.v1.column.Array;
import com.apple.foundationdb.relational.grpc.jdbc.v1.column.Column;
import com.apple.foundationdb.relational.grpc.jdbc.v1.column.ColumnMetadata;
import com.apple.foundationdb.relational.util.SpotBugsSuppressWarnings;

import javax.annotation.Nonnull;
import java.sql.SQLException;
import java.sql.Types;

/**
 * Facade over protobufs to deliver a {@link RelationalArray} view.
 */
public class JDBCRelationalArray extends RelationalArray {
    /**
     * Column metadata for this Array.
     */
    private final ColumnMetadata metadata;

    /**
     * The Array in protobuf format.
     */
    private final Array delegate;

    JDBCRelationalArray(ColumnMetadata columnMetadata, Array delegate) {
        this.metadata = columnMetadata;
        this.delegate = delegate;
    }

    @Override
    public String getBaseTypeName() throws SQLException {
        return SqlTypeNamesSupport.getSqlTypeName(getBaseType());
    }

    @Override
    public int getBaseType() throws SQLException {
        // From RowArray#getBaseType: "...In Relational, the contents of an Array is _always_ a Struct..."
        return Types.STRUCT;
    }

    @Override
    public Object getArray(long index, int count) throws SQLException {
        throw new SQLException("Not implemented " + Thread.currentThread() .getStackTrace()[1] .getMethodName());
    }

    @Override
    public RelationalResultSet getResultSet(long index, int count) throws SQLException {
        // Provide a RelationalResultSet view of backing protobuf metadata.
        return new RelationalResultSet() {
            /**
             * The ResultSet index starts before '1'... you have to call 'next' to get to first ResultSet.
             */
            private int rowIndex = -1;

            @Override
            public RelationalResultSetMetaData getMetaData() throws SQLException {
                return new RelationalResultSetMetaData() {

                    @Override
                    public int getColumnCount() throws SQLException {
                        int count = 0;
                        if (metadata.hasArray()) {
                            if (metadata.getArray().hasStruct()) {
                                count = metadata.getArray().getStruct().getColumnsCount();
                            }
                        }
                        return count;
                    }

                    @Override
                    public String getColumnName(int oneBasedColumn) throws SQLException {
                        String columnName = null;
                        if (metadata.hasArray()) {
                            if (metadata.getArray().hasStruct()) {
                                columnName = metadata.getArray().getStruct()
                                        .getColumns(JDBCProtobuf.toProtobufIndex(oneBasedColumn)).getName();
                            }
                        }
                        return columnName;
                    }

                    @Override
                    public int getColumnType(int oneBasedColumn) throws SQLException {
                        int columnType = -1;
                        if (metadata.hasArray()) {
                            if (metadata.getArray().hasStruct()) {
                                columnType = metadata.getArray().getStruct()
                                        .getColumns(JDBCProtobuf.toProtobufIndex(oneBasedColumn)).getJavaSqlTypesCode();
                            }
                        }
                        return columnType;
                    }

                    @Override
                    public StructMetaData getArrayMetaData(int oneBasedColumn) throws SQLException {
                        throw new SQLException("Not implemented " + Thread.currentThread() .getStackTrace()[1] .getMethodName());
                    }

                    @Override
                    public StructMetaData getStructMetaData(int oneBasedColumn) throws SQLException {
                        throw new SQLException("Not implemented " + Thread.currentThread() .getStackTrace()[1] .getMethodName());
                    }
                };
            }

            @Nonnull
            @Override
            public Continuation getContinuation() throws SQLException {
                throw new SQLException("Not implemented " + Thread.currentThread() .getStackTrace()[1] .getMethodName());
            }

            @Override
            public RelationalStruct getStruct(String columnLabel) throws SQLException {
                throw new SQLException("Not implemented " + Thread.currentThread() .getStackTrace()[1] .getMethodName());
            }

            @Override
            public RelationalStruct getStruct(int oneBasedColumn) throws SQLException {
                throw new SQLException("Not implemented " + Thread.currentThread() .getStackTrace()[1] .getMethodName());
            }

            @Override
            public RelationalArray getArray(String columnLabel) throws SQLException {
                throw new SQLException("Not implemented " + Thread.currentThread() .getStackTrace()[1] .getMethodName());
            }

            @Override
            public RelationalArray getArray(int oneBasedColumn) throws SQLException {
                throw new SQLException("Not implemented " + Thread.currentThread() .getStackTrace()[1] .getMethodName());
            }

            @Override
            public boolean next() throws SQLException {
                return ++rowIndex < delegate.getElementsCount();
            }

            @Override
            public void close() throws SQLException {
            }

            @Override
            public String getString(int oneBasedColumn) throws SQLException {
                Column column = delegate.getElements(rowIndex).getColumns(JDBCProtobuf.toProtobufIndex(oneBasedColumn));
                return column == null || !column.hasString() ? null : column.getString();
            }

            @Override
            public boolean getBoolean(int columnIndex) throws SQLException {
                throw new SQLException("Not implemented " + Thread.currentThread() .getStackTrace()[1] .getMethodName());
            }

            @SpotBugsSuppressWarnings(value = {"NP_NULL_ON_SOME_PATH_FROM_RETURN_VALUE", "NP_NULL_ON_SOME_PATH"},
                    justification = "We should be good in here.")
            @Override
            public long getLong(int oneBasedColumn) throws SQLException {
                Column column = delegate.getElements(rowIndex).getColumns(JDBCProtobuf.toProtobufIndex(oneBasedColumn));
                return (column == null || !column.hasLong()) ? null : column.getLong();
            }

            @Override
            public float getFloat(int columnIndex) throws SQLException {
                throw new SQLException("Not implemented " + Thread.currentThread() .getStackTrace()[1] .getMethodName());
            }

            @Override
            public double getDouble(int columnIndex) throws SQLException {
                throw new SQLException("Not implemented " + Thread.currentThread() .getStackTrace()[1] .getMethodName());
            }

            @Override
            public byte[] getBytes(int oneBasedColumn) throws SQLException {
                Column column = delegate.getElements(rowIndex).getColumns(JDBCProtobuf.toProtobufIndex(oneBasedColumn));
                return column == null || !column.hasBinary() ? null : column.getBinary().toByteArray();
            }

            @Override
            public String getString(String columnLabel) throws SQLException {
                throw new SQLException("Not implemented " + Thread.currentThread() .getStackTrace()[1] .getMethodName());
            }

            @Override
            public boolean getBoolean(String columnLabel) throws SQLException {
                throw new SQLException("Not implemented " + Thread.currentThread() .getStackTrace()[1] .getMethodName());
            }

            @Override
            public long getLong(String columnLabel) throws SQLException {
                throw new SQLException("Not implemented " + Thread.currentThread() .getStackTrace()[1] .getMethodName());
            }

            @Override
            public float getFloat(String columnLabel) throws SQLException {
                throw new SQLException("Not implemented " + Thread.currentThread() .getStackTrace()[1] .getMethodName());
            }

            @Override
            public double getDouble(String columnLabel) throws SQLException {
                throw new SQLException("Not implemented " + Thread.currentThread() .getStackTrace()[1] .getMethodName());
            }

            @Override
            public byte[] getBytes(String columnLabel) throws SQLException {
                throw new SQLException("Not implemented " + Thread.currentThread() .getStackTrace()[1] .getMethodName());
            }

            @Override
            public Object getObject(int oneBasedColumn) throws SQLException {
                int type = getMetaData().getColumnType(oneBasedColumn);
                switch (type) {
                    case Types.BIGINT:
                        return getLong(oneBasedColumn);
                    case Types.VARCHAR:
                        return getString(oneBasedColumn);
                    default:
                        throw new SQLException("Unsupported java.sql.Types=" + type);
                }
            }

            @Override
            public Object getObject(String columnLabel) throws SQLException {
                throw new SQLException("Not implemented " + Thread.currentThread() .getStackTrace()[1] .getMethodName());
            }
        };
    }
}
