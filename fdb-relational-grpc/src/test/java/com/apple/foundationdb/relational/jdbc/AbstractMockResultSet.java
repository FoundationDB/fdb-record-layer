/*
 * AbstractMockResultSet.java
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

import com.apple.foundationdb.relational.api.RelationalArray;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.RelationalResultSetMetaData;
import com.apple.foundationdb.relational.api.RelationalStruct;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;

import org.jspecify.annotations.Nullable;

import java.sql.SQLException;
import java.util.UUID;

/**
 * Mock ResultSet base class for testing purposes. This class handles the basic accessors of the result set and can be
 * extended to different kinds of concrete classes (similar to AbstractRecordLayerResultSet)
 */
public abstract class AbstractMockResultSet implements RelationalResultSet {
    private final RelationalResultSetMetaData metadata;
    @Nullable
    private MockResultSetRow currentRow;

    protected AbstractMockResultSet(RelationalResultSetMetaData metadata) {
        this.metadata = metadata;
    }

    protected abstract boolean hasNext();

    @Nullable
    protected abstract MockResultSetRow advanceRow() throws RelationalException;

    @Override
    public boolean next() throws SQLException {
        try {
            currentRow = advanceRow();
            return currentRow != null;
        } catch (RelationalException e) {
            throw e.toSqlException();
        }
    }

    @Override
    public void close() throws SQLException {
    }

    @Override
    public boolean wasNull() throws SQLException {
        return requireCurrentRow().wasNull();
    }

    @Override
    public String getString(int columnIndex) throws SQLException {
        return requireCurrentRow().getString(columnIndex);
    }

    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {
        return requireCurrentRow().getBoolean(columnIndex);
    }

    @Override
    public int getInt(int columnIndex) throws SQLException {
        return requireCurrentRow().getInt(columnIndex);
    }

    @Override
    public long getLong(int columnIndex) throws SQLException {
        return requireCurrentRow().getLong(columnIndex);
    }

    @Override
    public float getFloat(int columnIndex) throws SQLException {
        return requireCurrentRow().getFloat(columnIndex);
    }

    @Override
    public double getDouble(int columnIndex) throws SQLException {
        return requireCurrentRow().getDouble(columnIndex);
    }

    @Override
    public byte[] getBytes(int columnIndex) throws SQLException {
        return requireCurrentRow().getBytes(columnIndex);
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {
        return requireCurrentRow().getObject(columnIndex);
    }

    @Override
    public RelationalStruct getStruct(int oneBasedPosition) throws SQLException {
        return requireCurrentRow().getStruct(oneBasedPosition);
    }

    @Override
    public RelationalArray getArray(int oneBasedPosition) throws SQLException {
        return requireCurrentRow().getArray(oneBasedPosition);
    }

    @Override
    public UUID getUUID(int oneBasedPosition) throws SQLException {
        return requireCurrentRow().getUUID(oneBasedPosition);
    }

    @Override
    public String getString(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException("Label operations not supported in AbstractTestResultSet");
    }

    @Override
    public boolean getBoolean(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException("Label operations not supported in AbstractTestResultSet");
    }

    @Override
    public int getInt(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException("Label operations not supported in AbstractTestResultSet");
    }

    @Override
    public long getLong(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException("Label operations not supported in AbstractTestResultSet");
    }

    @Override
    public float getFloat(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException("Label operations not supported in AbstractTestResultSet");
    }

    @Override
    public double getDouble(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException("Label operations not supported in AbstractTestResultSet");
    }

    @Override
    public byte[] getBytes(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException("Label operations not supported in AbstractTestResultSet");
    }

    @Override
    public Object getObject(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException("Label operations not supported in AbstractTestResultSet");
    }

    @Override
    public RelationalStruct getStruct(String fieldName) throws SQLException {
        throw new UnsupportedOperationException("Label operations not supported in AbstractTestResultSet");
    }

    @Override
    public RelationalArray getArray(String fieldName) throws SQLException {
        throw new UnsupportedOperationException("Label operations not supported in AbstractTestResultSet");
    }

    @Override
    public UUID getUUID(String fieldName) throws SQLException {
        throw new UnsupportedOperationException("Label operations not supported in AbstractTestResultSet");
    }

    @Override
    public RelationalResultSetMetaData getMetaData() throws SQLException {
        return metadata;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return false;
    }

    private MockResultSetRow requireCurrentRow() throws SQLException {
        if (currentRow == null) {
            throw new SQLException("ResultSet exhausted", ErrorCode.INVALID_CURSOR_STATE.getErrorCode());
        }
        return currentRow;
    }
}
