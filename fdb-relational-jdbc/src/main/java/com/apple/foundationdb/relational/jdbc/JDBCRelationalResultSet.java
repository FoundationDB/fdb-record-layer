/*
 * JDBCRelationalResultSet.java
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
import com.apple.foundationdb.relational.api.RelationalArray;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.RelationalResultSetMetaData;
import com.apple.foundationdb.relational.api.RelationalStruct;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.grpc.jdbc.v1.ResultSet;
import com.apple.foundationdb.relational.grpc.jdbc.v1.column.Column;
import com.apple.foundationdb.relational.grpc.jdbc.v1.column.ColumnMetadata;
import com.apple.foundationdb.relational.util.ExcludeFromJacocoGeneratedReport;

import javax.annotation.Nonnull;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.Types;

class JDBCRelationalResultSet implements RelationalResultSet {
    private final ResultSet delegate;
    private final int rows;
    /**
     * The ResultSet index starts before '1'... you have to call 'next' to get to first ResultSet.
     */
    private int rowIndex = -1;

    JDBCRelationalResultSet(ResultSet delegate) {
        this.delegate = delegate;
        this.rows = delegate.getRowsCount();
    }

    @Override
    public boolean next() throws SQLException {
        return ++rowIndex < rows;
    }

    @Override
    public void close() throws SQLException {
    }

    @Override
    public String getString(int oneBasedColumn) throws SQLException {
        return this.delegate.getRows(rowIndex).getColumns(JDBCProtobuf.toProtobufIndex(oneBasedColumn)).getString();
    }

    @Override
    public boolean getBoolean(int oneBasedColumn) throws SQLException {
        return this.delegate.getRows(rowIndex).getColumns(JDBCProtobuf.toProtobufIndex(oneBasedColumn)).getBoolean();
    }

    @Override
    public int getInt(int oneBasedColumn) throws SQLException {
        // Presume column is JDBC 1-based index.
        // TODO: This needs work.
        return this.delegate.getRows(rowIndex).getColumns(JDBCProtobuf.toProtobufIndex(oneBasedColumn)).getInteger();
    }

    @Override
    public long getLong(int oneBasedColumn) throws SQLException {
        // TODO: This needs work.
        return this.delegate.getRows(rowIndex).getColumns(JDBCProtobuf.toProtobufIndex(oneBasedColumn)).getLong();
    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    public float getFloat(int oneBasedColumn) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    public double getDouble(int oneBasedColumn) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    public byte[] getBytes(int oneBasedColumn) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    private static int columnLabelToColumnOneBasedIndex(java.util.List<Column> columns, String columnLabel) {
        // TODO: make this better; cache a map.
        int index = 0;
        for (Column column : columns) {
            if (column.getString().equals(columnLabel)) {
                return index + 1;/*1-based*/
            }
            index++;
        }
        return -1;
    }

    @Override
    public String getString(String columnLabel) throws SQLException {
        // TOOD: Do getName for now.
        return getString(columnLabelToColumnOneBasedIndex(this.delegate.getRows(rowIndex).getColumnsList(),
                columnLabel));
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public boolean getBoolean(String columnLabel) throws SQLException {
        throw new SQLException("Not implemented " + Thread.currentThread() .getStackTrace()[1] .getMethodName());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public byte getByte(String columnLabel) throws SQLException {
        throw new SQLException("Not implemented " + Thread.currentThread() .getStackTrace()[1] .getMethodName());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public long getLong(String columnLabel) throws SQLException {
        throw new SQLException("Not implemented " + Thread.currentThread() .getStackTrace()[1] .getMethodName());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public float getFloat(String columnLabel) throws SQLException {
        throw new SQLException("Not implemented " + Thread.currentThread() .getStackTrace()[1] .getMethodName());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public double getDouble(String columnLabel) throws SQLException {
        throw new SQLException("Not implemented " + Thread.currentThread() .getStackTrace()[1] .getMethodName());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public byte[] getBytes(String columnLabel) throws SQLException {
        throw new SQLException("Not implemented " + Thread.currentThread() .getStackTrace()[1] .getMethodName());
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        // TODO: Does nothing for now.
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {
        // TODO: Does nothing for now.
    }

    @Override
    public RelationalResultSetMetaData getMetaData() throws SQLException {
        return new JDBCRelationalResultSetMetaData(this.delegate.getMetadata());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    @Nonnull
    public Continuation getContinuation() throws SQLException {
        throw new SQLException("Not implemented " + Thread.currentThread() .getStackTrace()[1] .getMethodName());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public RelationalStruct getStruct(String columnLabel) throws SQLException {
        throw new SQLException("Not implemented " + Thread.currentThread() .getStackTrace()[1] .getMethodName());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public RelationalStruct getStruct(int oneBasedColumn) throws SQLException {
        int index = JDBCProtobuf.toProtobufIndex(oneBasedColumn);
        ColumnMetadata columnMetadata = this.delegate.getMetadata().getColumns(index);
        return new JDBCRelationalStruct(columnMetadata, this.delegate.getRows(rowIndex).getColumns(index).getStruct());
    }

    @Override
    public Object getObject(int oneBasedColumn) throws SQLException {
        int type = getMetaData().getColumnType(oneBasedColumn);
        switch (type) {
            case Types.VARCHAR:
                return getString(oneBasedColumn);
            case Types.BIGINT:
                return getLong(oneBasedColumn);
            case Types.STRUCT:
                return getStruct(oneBasedColumn);
            case Types.ARRAY:
                return getArray(oneBasedColumn);
            case Types.BINARY:
                Column column =
                        this.delegate.getRows(rowIndex).getColumns(JDBCProtobuf.toProtobufIndex(oneBasedColumn));
                return column == null || !column.hasBinary() ? null : column.getBinary().toByteArray();
            default:
                throw new SQLException("Unsupported type " + type);
        }
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public Object getObject(String columnLabel) throws SQLException {
        throw new SQLException("Not implemented " + Thread.currentThread() .getStackTrace()[1] .getMethodName());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public RelationalArray getArray(int oneBasedColumn) throws SQLException {
        int index = JDBCProtobuf.toProtobufIndex(oneBasedColumn);
        ColumnMetadata columnMetadata = this.delegate.getMetadata().getColumns(index);
        Column column = this.delegate.getRows(rowIndex).getColumns(index);
        return column == null || !column.hasArray() ? null : new JDBCRelationalArray(columnMetadata, column.getArray());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public RelationalArray getArray(String columnLabel) throws SQLException {
        throw new SQLException("Not implemented " + Thread.currentThread() .getStackTrace()[1] .getMethodName());
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return iface.cast(this);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isInstance(this);
    }
}
