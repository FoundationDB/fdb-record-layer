/*
 * StructMetaData.java
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

import java.sql.SQLException;
import java.sql.Wrapper;

/**
 * Metadata describing what is in a {@link RelationalStruct}.
 * As {@link java.sql.ResultSetMetaData} is for {@link java.sql.ResultSet}, so is this Interface for instances of
 * {@link java.sql.Struct}. The below is modeled on -- and a subset of -- {@link java.sql.ResultSetMetaData} with a
 * few extras such as {@link #getNestedMetaData} and {@link #getLeadingPhantomColumnCount()}.
 */
public interface StructMetaData extends Wrapper {

    int getColumnCount() throws SQLException;

    //not super relevant yet, but it will be
    int isNullable(int oneBasedColumn) throws SQLException;

    // the output of an AS clause, o.w. the same as getColumnName()
    String getColumnLabel(int oneBasedColumn) throws SQLException;

    String getColumnName(int oneBasedColumn) throws SQLException;

    String getSchemaName(int oneBasedColumn) throws SQLException;

    String getTableName(int oneBasedColumn) throws SQLException;

    //the database where the column came from
    String getCatalogName(int oneBasedColumn) throws SQLException;

    int getColumnType(int oneBasedColumn) throws SQLException;

    String getColumnTypeName(int oneBasedColumn) throws SQLException;

    /**
     * Get the Metadata for a nested struct type.
     *
     * If the column is not a struct type, this will throw an error.
     *
     * @param oneBasedColumn the position of the column, indexed at 1
     * @return the metadata for the struct at column {@code oneBasedColumn}
     * @throws SQLException if the type of the column is not a struct, or if something else goes wrong.
     */
    default StructMetaData getNestedMetaData(int oneBasedColumn) throws SQLException {
        throw new SQLException("Not implemented in the relational layer", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    /**
     * Get the Metadata for an array type.
     *
     * If the column is not an array type, this will throw an error.
     *
     * @param oneBasedColumn the position of the column, indexed at 1
     * @return the metadata for the array at column {@code oneBasedColumn}
     * @throws SQLException if the type of the column is not an array, or if something else goes wrong.
     */
    StructMetaData getArrayMetaData(int oneBasedColumn) throws SQLException;

    default int getLeadingPhantomColumnCount() {
        return 0;
    }
}
