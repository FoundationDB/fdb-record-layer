/*
 * AbstractRecordLayerResultSet.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2024 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.recordlayer;

import com.apple.foundationdb.relational.api.MutableRowStruct;
import com.apple.foundationdb.relational.api.Row;
import com.apple.foundationdb.relational.api.StructMetaData;
import com.apple.foundationdb.relational.api.StructResultSetMetaData;
import com.apple.foundationdb.relational.api.RelationalArray;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.RelationalResultSetMetaData;
import com.apple.foundationdb.relational.api.RelationalStruct;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;

import java.sql.SQLException;

public abstract class AbstractRecordLayerResultSet implements RelationalResultSet {

    protected final StructMetaData metaData;

    private final MutableRowStruct currentRow;

    public AbstractRecordLayerResultSet(StructMetaData metaData) {
        this.metaData = metaData;
        this.currentRow = new MutableRowStruct(metaData);
    }

    protected abstract boolean hasNext();

    protected abstract Row advanceRow() throws RelationalException;

    @Override
    public boolean next() throws SQLException {
        try {
            Row next = advanceRow();
            currentRow.setRow(next);
            return next != null;
        } catch (RelationalException ve) {
            throw ve.toSqlException();
        }
    }

    @Override
    public boolean wasNull() {
        return currentRow.wasNull();
    }

    @Override
    public boolean getBoolean(int oneBasedPosition) throws SQLException {
        if (!currentRow.hasRow()) {
            throw new SQLException("ResultSet exhausted", ErrorCode.INVALID_CURSOR_STATE.getErrorCode());
        }
        return currentRow.getBoolean(oneBasedPosition);
    }

    @Override
    public boolean getBoolean(String columnLabel) throws SQLException {
        if (!currentRow.hasRow()) {
            throw new SQLException("ResultSet exhausted", ErrorCode.INVALID_CURSOR_STATE.getErrorCode());
        }
        return currentRow.getBoolean(columnLabel);
    }

    @Override
    public byte[] getBytes(int oneBasedPosition) throws SQLException {
        if (!currentRow.hasRow()) {
            throw new SQLException("ResultSet exhausted", ErrorCode.INVALID_CURSOR_STATE.getErrorCode());
        }
        return currentRow.getBytes(oneBasedPosition);
    }

    @Override
    public byte[] getBytes(String columnLabel) throws SQLException {
        if (!currentRow.hasRow()) {
            throw new SQLException("ResultSet exhausted", ErrorCode.INVALID_CURSOR_STATE.getErrorCode());
        }
        return currentRow.getBytes(columnLabel);
    }

    @Override
    public int getInt(int oneBasedPosition) throws SQLException {
        if (!currentRow.hasRow()) {
            throw new SQLException("ResultSet exhausted", ErrorCode.INVALID_CURSOR_STATE.getErrorCode());
        }
        return currentRow.getInt(oneBasedPosition);
    }

    @Override
    public int getInt(String columnLabel) throws SQLException {
        if (!currentRow.hasRow()) {
            throw new SQLException("ResultSet exhausted", ErrorCode.INVALID_CURSOR_STATE.getErrorCode());
        }
        return currentRow.getInt(columnLabel);
    }

    @Override
    public long getLong(int oneBasedPosition) throws SQLException {
        if (!currentRow.hasRow()) {
            throw new SQLException("ResultSet exhausted", ErrorCode.INVALID_CURSOR_STATE.getErrorCode());
        }
        return currentRow.getLong(oneBasedPosition);
    }

    @Override
    public long getLong(String columnLabel) throws SQLException {
        if (!currentRow.hasRow()) {
            throw new SQLException("ResultSet exhausted", ErrorCode.INVALID_CURSOR_STATE.getErrorCode());
        }
        return currentRow.getLong(columnLabel);
    }

    @Override
    public float getFloat(int oneBasedPosition) throws SQLException {
        if (!currentRow.hasRow()) {
            throw new SQLException("ResultSet exhausted", ErrorCode.INVALID_CURSOR_STATE.getErrorCode());
        }
        return currentRow.getFloat(oneBasedPosition);
    }

    @Override
    public float getFloat(String columnLabel) throws SQLException {
        if (!currentRow.hasRow()) {
            throw new SQLException("ResultSet exhausted", ErrorCode.INVALID_CURSOR_STATE.getErrorCode());
        }
        return currentRow.getFloat(columnLabel);
    }

    @Override
    public double getDouble(int oneBasedPosition) throws SQLException {
        if (!currentRow.hasRow()) {
            throw new SQLException("ResultSet exhausted", ErrorCode.INVALID_CURSOR_STATE.getErrorCode());
        }
        return currentRow.getDouble(oneBasedPosition);
    }

    @Override
    public double getDouble(String columnLabel) throws SQLException {
        if (!currentRow.hasRow()) {
            throw new SQLException("ResultSet exhausted", ErrorCode.INVALID_CURSOR_STATE.getErrorCode());
        }
        return currentRow.getDouble(columnLabel);
    }

    @Override
    public Object getObject(String columnLabel) throws SQLException {
        if (!currentRow.hasRow()) {
            throw new SQLException("ResultSet exhausted", ErrorCode.INVALID_CURSOR_STATE.getErrorCode());
        }
        return currentRow.getObject(columnLabel);
    }

    @Override
    public Object getObject(int oneBasedPosition) throws SQLException {
        if (!currentRow.hasRow()) {
            throw new SQLException("ResultSet exhausted", ErrorCode.INVALID_CURSOR_STATE.getErrorCode());
        }
        return currentRow.getObject(oneBasedPosition);
    }

    @Override
    public String getString(int oneBasedPosition) throws SQLException {
        if (!currentRow.hasRow()) {
            throw new SQLException("ResultSet exhausted", ErrorCode.INVALID_CURSOR_STATE.getErrorCode());
        }
        return currentRow.getString(oneBasedPosition);
    }

    @Override
    public String getString(String columnLabel) throws SQLException {
        if (!currentRow.hasRow()) {
            throw new SQLException("ResultSet exhausted", ErrorCode.INVALID_CURSOR_STATE.getErrorCode());
        }
        return currentRow.getString(columnLabel);
    }

    @Override
    public RelationalArray getArray(int oneBasedPosition) throws SQLException {
        if (!currentRow.hasRow()) {
            throw new SQLException("ResultSet exhausted", ErrorCode.INVALID_CURSOR_STATE.getErrorCode());
        }
        return currentRow.getArray(oneBasedPosition);
    }

    @Override
    public RelationalArray getArray(String columnLabel) throws SQLException {
        if (!currentRow.hasRow()) {
            throw new SQLException("ResultSet exhausted", ErrorCode.INVALID_CURSOR_STATE.getErrorCode());
        }
        return currentRow.getArray(columnLabel);
    }

    @Override
    public RelationalStruct getStruct(int oneBasedColumn) throws SQLException {
        if (!currentRow.hasRow()) {
            throw new SQLException("ResultSet exhausted", ErrorCode.INVALID_CURSOR_STATE.getErrorCode());
        }
        return currentRow.getStruct(oneBasedColumn);
    }

    @Override
    public RelationalStruct getStruct(String columnLabel) throws SQLException {
        if (!currentRow.hasRow()) {
            throw new SQLException("ResultSet exhausted", ErrorCode.INVALID_CURSOR_STATE.getErrorCode());
        }
        return currentRow.getStruct(columnLabel);
    }

    @Override
    public RelationalResultSetMetaData getMetaData() {
        return new StructResultSetMetaData(metaData);
    }
}
