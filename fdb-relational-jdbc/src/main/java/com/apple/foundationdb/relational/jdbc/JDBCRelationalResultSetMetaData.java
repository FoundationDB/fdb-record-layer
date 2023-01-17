/*
 * JDBCRelationalResultSetMetaData.java
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
import com.apple.foundationdb.relational.api.RelationalResultSetMetaData;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.grpc.jdbc.v1.ResultSetMetadata;
import com.apple.foundationdb.relational.util.ExcludeFromJacocoGeneratedReport;

import java.sql.SQLException;

class JDBCRelationalResultSetMetaData implements RelationalResultSetMetaData  {
    private final ResultSetMetadata delegate;

    JDBCRelationalResultSetMetaData(ResultSetMetadata delegate) {
        this.delegate = delegate;
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public StructMetaData getArrayMetaData(int oneBasedColumn) throws SQLException {
        throw new SQLException("Not implemented", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public StructMetaData getStructMetaData(int oneBasedColumn) throws SQLException {
        throw new SQLException("Not implemented", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    public int getColumnCount() throws SQLException {
        return this.delegate.getColumnsCount();
    }

    @Override
    public String getColumnName(int oneBasedColumn) throws SQLException {
        return this.delegate.getColumns(JDBCProtobuf.toProtobufIndex(oneBasedColumn)).getName();
    }

    @Override
    public String getColumnLabel(int column) throws SQLException {
        // TODO: For now, return name.
        return getColumnName(column);
    }

    @Override
    public int getColumnType(int oneBasedColumn) throws SQLException {
        return this.delegate.getColumns(JDBCProtobuf.toProtobufIndex(oneBasedColumn)).getJavaSqlTypesCode();
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isAssignableFrom(StructMetaData.class)) {
            return iface.cast(new StructMetaDataFacade(this));
        }
        return iface.cast(this);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isInstance(this) || iface.isAssignableFrom(StructMetaData.class);
    }

    // TODO: This could be removed if RelationalResultSetMetaData implemented StructMetaData
    private static class StructMetaDataFacade implements StructMetaData {
        private final JDBCRelationalResultSetMetaData delegate;

        StructMetaDataFacade(JDBCRelationalResultSetMetaData jdbcRelationalResultSetMetaData) {
            this.delegate = jdbcRelationalResultSetMetaData;
        }

        @Override
        public int getColumnCount() throws SQLException {
            return this.delegate.getColumnCount();
        }

        @Override
        public int isNullable(int oneBasedColumn) throws SQLException {
            throw new SQLException("Not implemented in the relational layer",
                    ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
        }

        @Override
        public String getColumnLabel(int oneBasedColumn) throws SQLException {
            return this.delegate.getColumnLabel(oneBasedColumn);
        }

        @Override
        public String getColumnName(int oneBasedColumn) throws SQLException {
            return this.delegate.getColumnName(oneBasedColumn);
        }

        @Override
        public String getSchemaName(int oneBasedColumn) throws SQLException {
            return this.delegate.getSchemaName(oneBasedColumn);
        }

        @Override
        public String getTableName(int oneBasedColumn) throws SQLException {
            return this.delegate.getTableName(oneBasedColumn);
        }

        @Override
        public String getCatalogName(int oneBasedColumn) throws SQLException {
            return this.delegate.getCatalogName(oneBasedColumn);
        }

        @Override
        public int getColumnType(int oneBasedColumn) throws SQLException {
            return this.delegate.getColumnType(oneBasedColumn);
        }

        @Override
        public String getColumnTypeName(int oneBasedColumn) throws SQLException {
            return this.delegate.getColumnTypeName(oneBasedColumn);
        }

        @Override
        public StructMetaData getNestedMetaData(int oneBasedColumn) throws SQLException {
            throw new SQLException("Not implemented in the relational layer",
                    ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
        }

        @Override
        public StructMetaData getArrayMetaData(int oneBasedColumn) throws SQLException {
            return this.delegate.getArrayMetaData(oneBasedColumn);
        }

        @Override
        public int getLeadingPhantomColumnCount() {
            return 0;
        }

        @Override
        public <T> T unwrap(Class<T> iface) throws SQLException {
            throw new SQLException("Not implemented in the relational layer",
                    ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
        }

        @Override
        public boolean isWrapperFor(Class<?> iface) throws SQLException {
            throw new SQLException("Not implemented in the relational layer",
                    ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
        }
    }
}
