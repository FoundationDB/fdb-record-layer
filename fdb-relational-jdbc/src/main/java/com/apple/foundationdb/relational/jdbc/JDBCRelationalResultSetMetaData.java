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
import com.apple.foundationdb.relational.util.ExcludeFromJacocoGeneratedReport;

import com.google.spanner.v1.ResultSetMetadata;
import com.google.spanner.v1.TypeCode;

import java.sql.SQLException;
import java.sql.Types;

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
        return this.delegate.getRowType().getFieldsCount();
    }

    @Override
    public String getColumnName(int column) throws SQLException {
        // Presume column is JDBC 1-based index.
        return this.delegate.getRowType().getFields(column - 1).getName();
    }

    @Override
    public String getColumnLabel(int column) throws SQLException {
        // TODO: For now, return name.
        return getColumnName(column);
    }

    @Override
    public int getColumnType(int column) throws SQLException {
        // Presume column is JDBC 1-based index.
        return toType(this.delegate.getRowType().getFields(column - 1).getType().getCode());
    }

    private static int toType(TypeCode typeCode) throws SQLException {
        int type;
        switch (typeCode) {
            case BOOL:
                type = Types.BOOLEAN;
                break;
            case INT64:
                type = Types.INTEGER;
                break;
            case STRING:
                type = Types.VARCHAR;
                break;
            default:
                throw new SQLException("TypeCode " + typeCode + " not implemented ",
                        ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
        }
        return type;
    }
}
