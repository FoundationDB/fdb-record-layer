/*
 * JDBCRelationalStruct.java
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

import com.apple.foundationdb.relational.api.StructMetaData;
import com.apple.foundationdb.relational.api.RelationalArray;
import com.apple.foundationdb.relational.api.RelationalStruct;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.grpc.jdbc.v1.column.Column;
import com.apple.foundationdb.relational.grpc.jdbc.v1.column.ColumnMetadata;
import com.apple.foundationdb.relational.grpc.jdbc.v1.column.Struct;

import java.sql.SQLException;

/**
 * Facade over protobuf objects to provide a {@link RelationalStruct} implementation.
 */
public class JDBCRelationalStruct implements RelationalStruct {
    /**
     * Column metadata for this Struct.
     */
    private final ColumnMetadata metadata;
    /**
     * The Struct in protobuf format.
     */
    private final Struct delegate;

    JDBCRelationalStruct(ColumnMetadata columnMetadata, Struct delegate) {
        this.metadata = columnMetadata;
        this.delegate = delegate;
    }

    /**
     * Facade over protobuf metadata to provide a {@link StructMetaData} view.
     */
    @Override
    public StructMetaData getMetaData() {
        return new StructMetaData() {
            @Override
            public int getColumnCount() throws SQLException {
                return metadata.getStruct().getColumnsCount();
            }

            @Override
            public int isNullable(int oneBasedColumn) throws SQLException {
                throw new SQLException("Not implemented", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
            }

            @Override
            public String getColumnLabel(int oneBasedColumn) throws SQLException {
                throw new SQLException("Not implemented", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
            }

            @Override
            public String getColumnName(int oneBasedColumn) throws SQLException {
                return metadata.getStruct().getColumns(JDBCProtobuf.toProtobufIndex(oneBasedColumn)).getName();
            }

            @Override
            public String getSchemaName(int oneBasedColumn) throws SQLException {
                throw new SQLException("Not implemented", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
            }

            @Override
            public String getTableName(int oneBasedColumn) throws SQLException {
                throw new SQLException("Not implemented", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
            }

            @Override
            public String getCatalogName(int oneBasedColumn) throws SQLException {
                throw new SQLException("Not implemented", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
            }

            @Override
            public int getColumnType(int oneBasedColumn) throws SQLException {
                return metadata.getStruct().getColumns(JDBCProtobuf.toProtobufIndex(oneBasedColumn))
                        .getJavaSqlTypesCode();
            }

            @Override
            public String getColumnTypeName(int oneBasedColumn) throws SQLException {
                throw new SQLException("Not implemented", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
            }

            @Override
            public StructMetaData getNestedMetaData(int oneBasedColumn) throws SQLException {
                throw new SQLException("Not implemented", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
            }

            @Override
            public StructMetaData getArrayMetaData(int oneBasedColumn) throws SQLException {
                throw new SQLException("Not implemented", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
            }

            @Override
            public int getLeadingPhantomColumnCount() {
                return -1000;
            }

            @Override
            public <T> T unwrap(Class<T> iface) throws SQLException {
                throw new SQLException("Not implemented", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
            }

            @Override
            public boolean isWrapperFor(Class<?> iface) throws SQLException {
                throw new SQLException("Not implemented", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
            }
        };
    }

    @Override
    public boolean getBoolean(int oneBasedColumn) throws SQLException {
        throw new SQLException("Not implemented", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    public boolean getBoolean(String fieldName) throws SQLException {
        throw new SQLException("Not implemented", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    public long getLong(int oneBasedColumn) throws SQLException {
        return this.delegate.getColumns(JDBCProtobuf.toProtobufIndex(oneBasedColumn)).getLong();
    }

    @Override
    public long getLong(String fieldName) throws SQLException {
        throw new SQLException("Not implemented", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    public float getFloat(int oneBasedColumn) throws SQLException {
        throw new SQLException("Not implemented", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    public float getFloat(String fieldName) throws SQLException {
        throw new SQLException("Not implemented", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    public double getDouble(int oneBasedColumn) throws SQLException {
        throw new SQLException("Not implemented", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    public double getDouble(String fieldName) throws SQLException {
        throw new SQLException("Not implemented", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    public byte[] getBytes(int oneBasedColumn) throws SQLException {
        Column column = this.delegate.getColumns(JDBCProtobuf.toProtobufIndex(oneBasedColumn));
        return column == null || !column.hasBinary() ? null : column.getBinary().toByteArray();
    }

    @Override
    public byte[] getBytes(String fieldName) throws SQLException {
        throw new SQLException("Not implemented", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    public String getString(int oneBasedColumn) throws SQLException {
        Column column = this.delegate.getColumns(JDBCProtobuf.toProtobufIndex(oneBasedColumn));
        return column == null || !column.hasString() ? null : column.getString();
    }

    @Override
    public String getString(String fieldName) throws SQLException {
        throw new SQLException("Not implemented", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    public Object getObject(int oneBasedColumn) throws SQLException {
        throw new SQLException("Not implemented", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    public Object getObject(String fieldName) throws SQLException {
        throw new SQLException("Not implemented", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    public RelationalStruct getStruct(int oneBasedColumn) throws SQLException {
        throw new SQLException("Not implemented", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    public RelationalStruct getStruct(String fieldName) throws SQLException {
        throw new SQLException("Not implemented", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    public RelationalArray getArray(int oneBasedColumn) throws SQLException {
        throw new SQLException("Not implemented", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    public RelationalArray getArray(String fieldName) throws SQLException {
        throw new SQLException("Not implemented", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    public String getSQLTypeName() throws SQLException {
        throw new SQLException("Not implemented", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }
}
