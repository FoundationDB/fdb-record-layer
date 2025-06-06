/*
 * RelationalResultSetMetaData.java
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

package com.apple.foundationdb.relational.api;

import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.util.ExcludeFromJacocoGeneratedReport;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

/**
 * Represents metadata about a given ResultSet.
 */
// Why are indices in the below '1-based' (oneBasedColumn)?
// Because JDBC is 1-based: "The columns are numbered from left to right, as they appear in the select list of the
//query, starting at 1." See 15.2.3 Retrieving Values in https://download.oracle.com/otndocs/jcp/jdbc-4_2-mrel2-spec/
public interface RelationalResultSetMetaData extends ResultSetMetaData, StructMetaData {

    @Override
    @ExcludeFromJacocoGeneratedReport
    default boolean isAutoIncrement(int column) throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default boolean isCaseSensitive(int column) throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default boolean isSearchable(int column) throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default boolean isCurrency(int column) throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default int isNullable(int column) throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default boolean isSigned(int column) throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default int getColumnDisplaySize(int column) throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default String getColumnLabel(int column) throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default String getSchemaName(int column) throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default int getPrecision(int column) throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default int getScale(int column) throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    default String getTableName(int column) throws SQLException {
        // Throwing an exception here messes up sqlline drawing results. Just do what Oracle apparently does here
        // and return an empty String for now until we implement ResultSet context attributes like table name.
        return "";
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default String getCatalogName(int column) throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default String getColumnTypeName(int column) throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default boolean isReadOnly(int column) throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default boolean isWritable(int column) throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default boolean isDefinitelyWritable(int column) throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default String getColumnClassName(int column) throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default <T> T unwrap(Class<T> iface) throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    default boolean isWrapperFor(Class<?> iface) throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }
}
