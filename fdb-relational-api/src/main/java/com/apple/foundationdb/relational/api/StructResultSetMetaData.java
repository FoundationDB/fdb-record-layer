/*
 * StructResultSetMetaData.java
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

import java.sql.SQLException;

public class StructResultSetMetaData implements RelationalResultSetMetaData {
    private final StructMetaData metaData;

    public StructResultSetMetaData(StructMetaData metaData) {
        this.metaData = metaData;
    }

    @Override
    public String getTypeName() throws SQLException {
        return metaData.getTypeName();
    }

    @Override
    public int getColumnCount() throws SQLException {
        return metaData.getColumnCount();
    }

    @Override
    public String getColumnName(int column) throws SQLException {
        return metaData.getColumnName(column);
    }

    @Override
    public String getColumnLabel(int column) throws SQLException {
        // For now, return column name until we implement column labels.
        // Throwing an exception breaks all processing.
        return getColumnName(column);
    }

    @Override
    public int getColumnType(int column) throws SQLException {
        return metaData.getColumnType(column);
    }

    @Override
    public String getColumnTypeName(int column) throws SQLException {
        return metaData.getColumnTypeName(column);
    }

    @Override
    public ArrayMetaData getArrayMetaData(int oneBasedColumn) throws SQLException {
        return metaData.getArrayMetaData(oneBasedColumn);
    }

    @Override
    public StructMetaData getStructMetaData(int oneBasedColumn) throws SQLException {
        return metaData.getStructMetaData(oneBasedColumn);
    }

    @Override
    public int isNullable(int oneBasedColumn) throws SQLException {
        return metaData.isNullable(oneBasedColumn);
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isAssignableFrom(StructResultSetMetaData.class)) {
            return iface.cast(metaData);
        } else if (iface.isAssignableFrom(StructMetaData.class)) {
            return iface.cast(metaData);
        }
        return RelationalResultSetMetaData.super.unwrap(iface);
    }
}
