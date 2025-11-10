/*
 * RelationalResultSetFacade.java
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
import com.apple.foundationdb.relational.api.RelationalStructMetaData;
import com.apple.foundationdb.relational.api.StructResultSetMetaData;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.metadata.DataType;
import com.apple.foundationdb.relational.jdbc.grpc.v1.ResultSet;
import com.apple.foundationdb.relational.jdbc.grpc.v1.column.Column;
import com.apple.foundationdb.relational.jdbc.grpc.v1.column.ColumnMetadata;
import com.apple.foundationdb.relational.util.ExcludeFromJacocoGeneratedReport;
import com.apple.foundationdb.relational.util.PositionalIndex;
import com.apple.foundationdb.relational.util.SpotBugsSuppressWarnings;
import com.google.common.base.Suppliers;

import javax.annotation.Nonnull;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.Types;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Facade over grpc protobuf objects that offers a {@link RelationalResultSet} view.
 */
class RelationalResultSetFacade implements RelationalResultSet {
    private final Supplier<DataType.StructType> type;

    private final ResultSet delegate;
    private final int rows;
    /**
     * The ResultSet index starts before '1'... you have to call 'next' to get to first ResultSet.
     */
    private int rowIndex = -1;

    private volatile boolean closed;
    private boolean wasNull = true;

    /**
     * When column is primitive numeric and null, we have to return something -- can't return null when primitive type.
     * This is what we return.
     */
    private static final int NUMERIC_VALUE_WHEN_NULL = -1;

    static final RelationalResultSet EMPTY = new RelationalResultSetFacade(ResultSet.newBuilder().build());

    RelationalResultSetFacade(ResultSet delegate) {
        this.delegate = delegate;
        this.type = Suppliers.memoize(() -> TypeConversion.getStructDataType(delegate.getMetadata().getColumnMetadata().getColumnMetadataList(), false));
        this.rows = delegate.getRowCount();
    }

    @Override
    public boolean next() throws SQLException {
        return ++rowIndex < rows;
    }

    public boolean hasNext() {
        return rowIndex < (rows - 1);
    }

    @Override
    public boolean isClosed() throws SQLException {
        return this.closed;
    }

    @Override
    public void close() throws SQLException {
        this.closed = true;
    }

    @Override
    public boolean wasNull() throws SQLException {
        // "...is for checking if the last primitive value read from the result set was null (as primitives don't
        // support null, while a database column does)"
        // "Reports whether the last column read had a value of SQL NULL. Note that you must first call one of the
        // getter methods on a column to try to read its value and then call the method wasNull to see if the value
        // read was SQL NULL."
        return this.wasNull;
    }

    private <R> R get(int oneBasedIndex, Function<Column, R> s) {
        int index = PositionalIndex.toProtobuf(oneBasedIndex);
        Column column = this.delegate.getRow(rowIndex).getColumns().getColumn(index);
        return s.apply(column);
    }

    @Override
    public String getString(final int oneBasedColumn) throws SQLException {
        return get(oneBasedColumn, column -> {
            if (column.hasString()) {
                // Do I need to update lastColumnReadWasNull for String type?
                // For primitives only?
                this.wasNull = false;
                return column.getString();
            }
            this.wasNull = true;
            return null;
        });
    }

    @Override
    public boolean getBoolean(int oneBasedColumn) throws SQLException {
        return get(oneBasedColumn, column -> {
            if (column.hasBoolean()) {
                this.wasNull = false;
                return column.getBoolean();
            }
            this.wasNull = true;
            return false;
        });
    }

    @Override
    public int getInt(int oneBasedColumn) throws SQLException {
        return get(oneBasedColumn, column -> {
            if (column.hasInteger()) {
                this.wasNull = false;
                return column.getInteger();
            }
            this.wasNull = true;
            return NUMERIC_VALUE_WHEN_NULL;
        });
    }

    @Override
    public long getLong(int oneBasedColumn) throws SQLException {
        return get(oneBasedColumn, column -> {
            if (column.hasLong()) {
                this.wasNull = false;
                return column.getLong();
            }
            this.wasNull = true;
            return Long.valueOf(NUMERIC_VALUE_WHEN_NULL);
        });
    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    public float getFloat(int oneBasedColumn) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    public double getDouble(int oneBasedColumn) throws SQLException {
        return get(oneBasedColumn, column -> {
            if (column.hasDouble()) {
                this.wasNull = false;
                return column.getDouble();
            }
            this.wasNull = true;
            return Double.valueOf(NUMERIC_VALUE_WHEN_NULL);
        });
    }

    @Override
    public byte[] getBytes(int oneBasedColumn) throws SQLException {
        return get(oneBasedColumn, column -> {
            if (column.hasBinary()) {
                this.wasNull = false;
                return column.getBinary().toByteArray();
            }
            this.wasNull = true;
            return null;
        });
    }

    private static int columnLabelToColumnOneBasedIndex(java.util.List<ColumnMetadata> metadatas, String columnLabel)
            throws SQLException {
        // TODO: make this better; cache a map.
        int index = 0;
        for (ColumnMetadata metadata : metadatas) {
            if (metadata.getName().equalsIgnoreCase(columnLabel)) {
                return index + 1; // 1-based
            }
            index++;
        }
        throw new SQLException("Unknown " + columnLabel + " in " +
                metadatas.stream().map(n -> n.getName()).collect(Collectors.toList()));
    }

    @Override
    public String getString(String columnLabel) throws SQLException {
        // TOOD: Do getName for now.
        return getString(
                columnLabelToColumnOneBasedIndex(
                        this.delegate.getMetadata().getColumnMetadata().getColumnMetadataList(),
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
    public int getInt(String columnLabel) throws SQLException {
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
        return type.get() != null ? new StructResultSetMetaData(RelationalStructMetaData.of(type.get())) : new RelationalResultSetMetaDataFacade(this.delegate.getMetadata());
    }

    @Override
    @Nonnull
    @SpotBugsSuppressWarnings("NP") // TODO: Will need to fix null handling
    public Continuation getContinuation() throws SQLException {
        if (hasNext()) {
            throw new SQLException("Continuation can only be returned for the last row");
        }
        return new RelationalRpcContinuation(delegate.getContinuation());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public RelationalStruct getStruct(String columnLabel) throws SQLException {
        // Not implemented
        throw new SQLException("Not implemented getStruct");
    }

    @Override
    public RelationalStruct getStruct(int oneBasedColumn) throws SQLException {
        RelationalStruct s = TypeConversion.getStruct(this.delegate, this.rowIndex, oneBasedColumn);
        wasNull = s == null;
        return s;
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public UUID getUUID(String columnLabel) throws SQLException {
        // Not implemented
        throw new SQLException("Not implemented getUUID");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public UUID getUUID(int oneBasedColumn) throws SQLException {
        UUID s = TypeConversion.getUUID(this.delegate, this.rowIndex, oneBasedColumn);
        wasNull = s == null;
        return s;
    }

    @Override
    public Object getObject(int oneBasedColumn) throws SQLException {
        int type = getMetaData().getColumnType(oneBasedColumn);
        final Object o;
        switch (type) {
            case Types.VARCHAR:
                o = getString(oneBasedColumn);
                break;
            case Types.INTEGER:
                o = getInt(oneBasedColumn);
                break;
            case Types.BIGINT:
                o = getLong(oneBasedColumn);
                break;
            case Types.STRUCT:
                o = getStruct(oneBasedColumn);
                break;
            case Types.ARRAY:
                o = getArray(oneBasedColumn);
                break;
            case Types.DOUBLE:
                o = getDouble(oneBasedColumn);
                break;
            case Types.BOOLEAN:
                o = getBoolean(oneBasedColumn);
                break;
            case Types.BINARY:
                o = getBytes(oneBasedColumn);
                break;
            case Types.OTHER:
                int index = PositionalIndex.toProtobuf(oneBasedColumn);
                final var relationalType = getMetaData().getRelationalDataType().getFields().get(index).getType();
                final var typeCode = relationalType.getCode();
                switch (typeCode) {
                    case UUID:
                        o = getUUID(oneBasedColumn);
                        break;
                    case ENUM:
                        o = getString(oneBasedColumn);
                        break;
                    case VECTOR: {
                        final var bytes = getBytes(oneBasedColumn);
                        if (wasNull()) {
                            return null;
                        }
                        o = TypeConversion.parseVector(bytes, ((DataType.VectorType)relationalType).getPrecision());
                    }
                        break;
                    default:
                        throw new SQLException("Unsupported type " + type);
                }
                break;
            default:
                throw new SQLException("Unsupported type " + type);
        }
        if (wasNull()) {
            return null;
        } else {
            return o;
        }
    }

    @Override
    public Object getObject(String columnLabel) throws SQLException {
        return getObject(RelationalStruct.getOneBasedPosition(columnLabel, this));
    }

    @Override
    public RelationalArray getArray(int oneBasedColumn) throws SQLException {
        int index = PositionalIndex.toProtobuf(oneBasedColumn);
        ColumnMetadata columnMetadata = this.delegate.getMetadata().getColumnMetadata().getColumnMetadata(index);
        Column column = this.delegate.getRow(rowIndex).getColumns().getColumn(index);
        RelationalArrayFacade array = column == null || !column.hasArray() ? null :
                new RelationalArrayFacade(columnMetadata.getArrayMetadata(), column.getArray());
        wasNull = array == null;
        return array;
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
