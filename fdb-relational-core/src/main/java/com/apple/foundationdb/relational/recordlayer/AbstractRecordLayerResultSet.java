/*
 * AbstractRecordLayerResultSet.java
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

package com.apple.foundationdb.relational.recordlayer;

import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.InvalidColumnReferenceException;
import com.apple.foundationdb.relational.api.exceptions.OperationUnsupportedException;

import com.google.protobuf.Message;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public abstract class AbstractRecordLayerResultSet implements RelationalResultSet {

    @Override
    public boolean getBoolean(int oneBasedPosition) throws SQLException {
        Object o = getObject(oneBasedPosition);
        if (o == null) {
            return false;
        }
        if (!(o instanceof Boolean)) {
            throw new SQLException("Boolean", ErrorCode.CANNOT_CONVERT_TYPE.getErrorCode());
        }

        return (Boolean) o;
    }

    @Override
    public boolean getBoolean(String columnLabel) throws SQLException {
        try {
            return getBoolean(getOneBasedPosition(columnLabel));
        } catch (InvalidColumnReferenceException e) {
            throw e.toSqlException();
        }
    }

    @Override
    public long getLong(int oneBasedPosition) throws SQLException {
        Object o = getObject(oneBasedPosition);
        if (o == null) {
            return 0L;
        }
        if (!(o instanceof Number)) {
            throw new SQLException("Long", ErrorCode.CANNOT_CONVERT_TYPE.getErrorCode());
        }

        return ((Number) o).longValue();
    }

    @Override
    public long getLong(String columnLabel) throws SQLException {
        try {
            return getLong(getOneBasedPosition(columnLabel));
        } catch (InvalidColumnReferenceException e) {
            throw e.toSqlException();
        }
    }

    @Override
    public float getFloat(int oneBasedPosition) throws SQLException {
        Object o = getObject(oneBasedPosition);
        if (o == null) {
            return 0L;
        }
        if (!(o instanceof Number)) {
            throw new SQLException("Float", ErrorCode.CANNOT_CONVERT_TYPE.getErrorCode());
        }

        return ((Number) o).floatValue();
    }

    @Override
    public float getFloat(String columnLabel) throws SQLException {
        try {
            return getFloat(getOneBasedPosition(columnLabel));
        } catch (InvalidColumnReferenceException e) {
            throw e.toSqlException();
        }
    }

    @Override
    public double getDouble(int oneBasedPosition) throws SQLException {
        Object o = getObject(oneBasedPosition);
        if (o == null) {
            return 0L;
        }
        if (!(o instanceof Number)) {
            throw new SQLException("Double", ErrorCode.CANNOT_CONVERT_TYPE.getErrorCode());
        }

        return ((Number) o).doubleValue();
    }

    @Override
    public double getDouble(String columnLabel) throws SQLException {
        try {
            return getDouble(getOneBasedPosition(columnLabel));
        } catch (InvalidColumnReferenceException e) {
            throw e.toSqlException();
        }
    }

    @Override
    public Object getObject(String columnLabel) throws SQLException {
        try {
            return getObject(getOneBasedPosition(columnLabel));
        } catch (InvalidColumnReferenceException e) {
            throw e.toSqlException();
        }
    }

    @Override
    public String getString(int oneBasedPosition) throws SQLException {
        Object o = getObject(oneBasedPosition);
        if (o == null) {
            return null;
        }
        if (!(o instanceof String)) {
            throw new SQLException("String", ErrorCode.CANNOT_CONVERT_TYPE.getErrorCode());
        }

        return (String) o;
    }

    @Override
    public String getString(String columnLabel) throws SQLException {
        try {
            return getString(getOneBasedPosition(columnLabel));
        } catch (InvalidColumnReferenceException e) {
            throw e.toSqlException();
        }
    }

    @Override
    public Iterable<?> getRepeated(int oneBasedPosition) throws SQLException {
        Object o = getObject(oneBasedPosition);
        if (o == null) {
            return null;
        }
        if (!(o instanceof Iterable)) {
            throw new SQLException("Iterable", ErrorCode.CANNOT_CONVERT_TYPE.getErrorCode());
        }

        return (Iterable<?>) o;
    }

    @Override
    public Iterable<?> getRepeated(String columnLabel) throws SQLException {
        try {
            return getRepeated(getOneBasedPosition(columnLabel));
        } catch (InvalidColumnReferenceException e) {
            throw e.toSqlException();
        }
    }

    @Override
    public boolean supportsMessageParsing() {
        return false;
    }

    @Override
    public <M extends Message> M parseMessage() throws SQLException {
        throw new OperationUnsupportedException("Does not support message parsing").toSqlException();
    }

    @Override
    public ResultSetMetaData getMetaData() {
        return new RecordResultSetMetaData(getFieldNames());
    }

    /**
     * Returns a 0-based position number for this column label.
     * @param columnLabel the name of the column
     * @return The 0-based position of the field in this result set
     * @throws SQLException if parsing the underlying message fails
     * @throws InvalidColumnReferenceException if this field name does not exist
     */
    protected abstract int getZeroBasedPosition(String columnLabel) throws SQLException, InvalidColumnReferenceException;

    /**
     * Returns a 1-based position number for this column label.
     * @param columnLabel the name of the column
     * @return The 1-based position of the column in this result set
     * @throws SQLException if parsing the underlying message fails
     * @throws InvalidColumnReferenceException if this field name does not exist
     */
    protected int getOneBasedPosition(String columnLabel) throws SQLException, InvalidColumnReferenceException {
        return getZeroBasedPosition(columnLabel) + 1;
    }

    protected abstract String[] getFieldNames();
}
