/*
 * MockResultSetMetadata.java
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

import com.apple.foundationdb.relational.api.ArrayMetaData;
import com.apple.foundationdb.relational.api.StructMetaData;
import com.apple.foundationdb.relational.api.RelationalResultSetMetaData;

import java.sql.SQLException;
import java.util.List;

/**
 * A testing implementation of an result set metadata. The columns are all assumed to be integers.
 */
public class MockResultSetMetadata implements RelationalResultSetMetaData {
    private final String typeName;
    private final List<Integer> columnTypes;

    public MockResultSetMetadata(String typeName, List<Integer> columnTypes) {
        this.typeName = typeName;
        this.columnTypes = columnTypes;
    }

    @Override
    public String getTypeName() throws SQLException {
        return typeName;
    }

    @Override
    public StructMetaData getStructMetaData(int oneBasedColumn) throws SQLException {
        return null;
    }

    @Override
    public ArrayMetaData getArrayMetaData(int oneBasedColumn) throws SQLException {
        return null;
    }

    @Override
    public int getColumnCount() throws SQLException {
        return columnTypes.size();
    }

    @Override
    public String getColumnName(int column) throws SQLException {
        return "";
    }

    @Override
    public int getColumnType(int column) throws SQLException {
        return columnTypes.get(column - 1);
    }
}
