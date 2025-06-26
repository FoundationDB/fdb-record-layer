/*
 * MockResultSetMetadata.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2025 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.relational.api.ArrayMetaData;
import com.apple.foundationdb.relational.api.RelationalResultSetMetaData;
import com.apple.foundationdb.relational.api.StructMetaData;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.metadata.DataType;

<<<<<<< Updated upstream
import java.sql.ResultSetMetaData;
=======
import javax.annotation.Nonnull;
>>>>>>> Stashed changes
import java.sql.SQLException;

/**
 * A testing implementation of an result set metadata. The columns are all assumed to be integers.
 */
public class MockResultSetMetadata implements RelationalResultSetMetaData {
    private final DataType.StructType type;

<<<<<<< Updated upstream
    public MockResultSetMetadata() {
        this.type = DataType.StructType.from("testType", List.of(
                DataType.StructType.Field.from("f1", DataType.Primitives.INTEGER.type(), 0),
                DataType.StructType.Field.from("f2", DataType.Primitives.INTEGER.type(), 1),
                DataType.StructType.Field.from("f2", DataType.Primitives.INTEGER.type(), 2)
        ), false);
=======
    public MockResultSetMetadata(@Nonnull DataType.StructType type) {
        this.type = type;
>>>>>>> Stashed changes
    }

    @Override
    public String getTypeName() throws SQLException {
<<<<<<< Updated upstream
        return this.type.getName();
=======
        return type.getName();
>>>>>>> Stashed changes
    }

    @Override
    public StructMetaData getStructMetaData(int oneBasedColumn) throws SQLException {
        throw new SQLException("Unsupported operation", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    public ArrayMetaData getArrayMetaData(int oneBasedColumn) throws SQLException {
        throw new SQLException("Unsupported operation", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    public DataType.StructType getRelationalDataType() throws SQLException {
        return type;
    }

    @Override
    public int getColumnCount() throws SQLException {
<<<<<<< Updated upstream
        return this.type.getFields().size();
=======
        return type.getFields().size();
>>>>>>> Stashed changes
    }

    @Override
    public String getColumnName(int column) throws SQLException {
        return "Column " + column;
    }

    @Override
    public int getColumnType(int column) throws SQLException {
<<<<<<< Updated upstream
        return this.type.getFields().get(column - 1).getType().getJdbcSqlCode();
    }

    @Override
    public int isNullable(int column) {
        return this.type.getFields().get(column - 1).getType().isNullable() ? ResultSetMetaData.columnNullable : ResultSetMetaData.columnNoNulls;
=======
        return type.getFields().get(column - 1).getType().getJdbcSqlCode();
>>>>>>> Stashed changes
    }
}
