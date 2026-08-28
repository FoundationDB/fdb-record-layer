/*
 * AbstractAggregateResultSet.java
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

package com.apple.foundationdb.relational.yamltests;

import com.apple.foundationdb.relational.api.RelationalArray;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.RelationalResultSetMetaData;
import com.apple.foundationdb.relational.api.RelationalStruct;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;

import org.jspecify.annotations.Nullable;
import java.sql.SQLException;
import java.util.UUID;

/**
 * A result set implementation that aggregates many result sets into one, where each internal result set acts as a row
 * in the aggregate.
 * TODO: This is a copy of the AbstractMockResultSet. We should clear that tech debt by integrating the types
 * TODO: of testing results sets into more reusable code.
 */
public abstract class AbstractAggregateResultSet implements RelationalResultSet {
    private boolean isClosed = false;
    private final RelationalResultSetMetaData metadata;
    @Nullable
    protected RelationalResultSet currentRow;

    protected AbstractAggregateResultSet(RelationalResultSetMetaData metadata) {
        this.metadata = metadata;
    }

    protected abstract boolean hasNext();

    @Nullable
    protected abstract RelationalResultSet advanceRow() throws SQLException;

    @Override
    public boolean next() throws SQLException {
        currentRow = advanceRow();
        return currentRow != null;
    }

    @Override
    public void close() throws SQLException {
        isClosed = true;
    }

    @Override
    public boolean wasNull() throws SQLException {
        return checkCurrentRow().wasNull();
    }

    @Override
    public String getString(int columnIndex) throws SQLException {
        return checkCurrentRow().getString(columnIndex);
    }

    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {
        return checkCurrentRow().getBoolean(columnIndex);
    }

    @Override
    public int getInt(int columnIndex) throws SQLException {
        return checkCurrentRow().getInt(columnIndex);
    }

    @Override
    public long getLong(int columnIndex) throws SQLException {
        return checkCurrentRow().getLong(columnIndex);
    }

    @Override
    public float getFloat(int columnIndex) throws SQLException {
        return checkCurrentRow().getFloat(columnIndex);
    }

    @Override
    public double getDouble(int columnIndex) throws SQLException {
        return checkCurrentRow().getDouble(columnIndex);
    }

    @Override
    public byte[] getBytes(int columnIndex) throws SQLException {
        return checkCurrentRow().getBytes(columnIndex);
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {
        return checkCurrentRow().getObject(columnIndex);
    }

    @Override
    public RelationalStruct getStruct(int oneBasedPosition) throws SQLException {
        return checkCurrentRow().getStruct(oneBasedPosition);
    }

    @Override
    public RelationalArray getArray(int oneBasedPosition) throws SQLException {
        return checkCurrentRow().getArray(oneBasedPosition);
    }

    @Override
    public UUID getUUID(int oneBasedPosition) throws SQLException {
        return checkCurrentRow().getUUID(oneBasedPosition);
    }

    @Override
    public String getString(String columnLabel) throws SQLException {
        return checkCurrentRow().getString(columnLabel);
    }

    @Override
    public boolean getBoolean(String columnLabel) throws SQLException {
        return checkCurrentRow().getBoolean(columnLabel);
    }

    @Override
    public int getInt(String columnLabel) throws SQLException {
        return checkCurrentRow().getInt(columnLabel);
    }

    @Override
    public long getLong(String columnLabel) throws SQLException {
        return checkCurrentRow().getLong(columnLabel);
    }

    @Override
    public float getFloat(String columnLabel) throws SQLException {
        return checkCurrentRow().getFloat(columnLabel);
    }

    @Override
    public double getDouble(String columnLabel) throws SQLException {
        return checkCurrentRow().getDouble(columnLabel);
    }

    @Override
    public byte[] getBytes(String columnLabel) throws SQLException {
        return checkCurrentRow().getBytes(columnLabel);
    }

    @Override
    public Object getObject(String columnLabel) throws SQLException {
        return checkCurrentRow().getObject(columnLabel);
    }

    @Override
    public RelationalStruct getStruct(String fieldName) throws SQLException {
        return checkCurrentRow().getStruct(fieldName);
    }

    @Override
    public RelationalArray getArray(String fieldName) throws SQLException {
        return checkCurrentRow().getArray(fieldName);
    }

    @Override
    public UUID getUUID(String fieldName) throws SQLException {
        return checkCurrentRow().getUUID(fieldName);
    }

    @Override
    public RelationalResultSetMetaData getMetaData() throws SQLException {
        return metadata;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return isClosed;
    }

    private RelationalResultSet checkCurrentRow() throws SQLException {
        if ((currentRow == null) || isClosed()) {
            throw new SQLException("ResultSet exhausted", ErrorCode.INVALID_CURSOR_STATE.getErrorCode());
        }
        return currentRow;
    }
}
