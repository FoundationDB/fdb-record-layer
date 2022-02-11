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

import com.apple.foundationdb.relational.api.IsolationLevel;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.InvalidColumnReferenceException;
import com.apple.foundationdb.relational.api.exceptions.OperationUnsupportedException;

import com.google.protobuf.Message;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public abstract class AbstractRecordLayerResultSet implements RelationalResultSet {
    @Override
    public IsolationLevel getActualIsolationLevel() throws SQLException {
        throw new OperationUnsupportedException("Not Implemented in the Relational layer").toSqlException();
    }

    @Override
    public IsolationLevel getRequestedIsolationLevel() throws SQLException {
        throw new OperationUnsupportedException("Not Implemented in the Relational layer").toSqlException();
    }

    @Override
    public boolean getBoolean(int position) throws SQLException {
        Object o = getObject(position);
        if (o == null) {
            return false; //TODO(bfines) return a default value here
        }
        if (!(o instanceof Boolean)) {
            throw new SQLException("Boolean", ErrorCode.CANNOT_CONVERT_TYPE.getErrorCode());
        }

        return (Boolean) o;
    }

    @Override
    public boolean getBoolean(String fieldName) throws SQLException {
        throw new OperationUnsupportedException("Not Implemented in the Relational layer").toSqlException();
    }

    @Override
    public long getLong(int position) throws SQLException {
        Object o = getObject(position);
        if (o == null) {
            return 0L;
        }
        if (!(o instanceof Number)) {
            throw new SQLException("Long", ErrorCode.CANNOT_CONVERT_TYPE.getErrorCode());
        }

        return ((Number) o).longValue();
    }

    @Override
    public long getLong(String fieldName) throws SQLException {
        try {
            return getLong(getPosition(fieldName));
        } catch (InvalidColumnReferenceException e) {
            throw e.toSqlException();
        }
    }

    @Override
    public float getFloat(int position) throws SQLException {
        Object o = getObject(position);
        if (o == null) {
            return 0L;
        }
        if (!(o instanceof Number)) {
            throw new SQLException("Long", ErrorCode.CANNOT_CONVERT_TYPE.getErrorCode());
        }

        return ((Number) o).floatValue();
    }

    @Override
    public float getFloat(String fieldName) throws SQLException {
        try {
            return getFloat(getPosition(fieldName));
        } catch (InvalidColumnReferenceException e) {
            throw e.toSqlException();
        }
    }

    @Override
    public double getDouble(int position) throws SQLException {
        Object o = getObject(position);
        if (o == null) {
            return 0L;
        }
        if (!(o instanceof Number)) {
            throw new SQLException("Long", ErrorCode.CANNOT_CONVERT_TYPE.getErrorCode());
        }

        return ((Number) o).doubleValue();
    }

    @Override
    public double getDouble(String fieldName) throws SQLException {
        try {
            return getDouble(getPosition(fieldName));
        } catch (InvalidColumnReferenceException e) {
            throw e.toSqlException();
        }
    }

    @Override
    public Object getObject(String fieldName) throws SQLException {
        try {
            return getObject(getPosition(fieldName));
        } catch (InvalidColumnReferenceException e) {
            throw e.toSqlException();
        }
    }

    @Override
    public String getString(int position) throws SQLException {
        Object o = getObject(position);
        if (o == null) {
            return null;
        }
        if (!(o instanceof String)) {
            throw new SQLException("String", ErrorCode.CANNOT_CONVERT_TYPE.getErrorCode());
        }

        return (String) o;
    }

    @Override
    public String getString(String fieldName) throws SQLException {
        try {
            return getString(getPosition(fieldName));
        } catch (InvalidColumnReferenceException e) {
            throw e.toSqlException();
        }
    }

    @Override
    public Message getMessage(int position) throws SQLException {
        Object o = getObject(position);
        if (o == null) {
            return null;
        }
        if (!(o instanceof Message)) {
            throw new SQLException("Message", ErrorCode.CANNOT_CONVERT_TYPE.getErrorCode());
        }

        return (Message) o;
    }

    @Override
    public Message getMessage(String fieldName) throws SQLException {
        try {
            return getMessage(getPosition(fieldName));
        } catch (InvalidColumnReferenceException e) {
            throw e.toSqlException();
        }
    }

    @Override
    public Iterable<?> getRepeated(int position) throws SQLException {
        Object o = getObject(position);
        if (o == null) {
            return null;
        }
        if (!(o instanceof Iterable)) {
            throw new SQLException("Iterable", ErrorCode.CANNOT_CONVERT_TYPE.getErrorCode());
        }

        return (Iterable<?>) o;
    }

    @Override
    public Iterable<?> getRepeated(String fieldName) throws SQLException {
        try {
            return getRepeated(getPosition(fieldName));
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

    protected abstract int getPosition(String fieldName) throws SQLException, InvalidColumnReferenceException;

    protected abstract String[] getFieldNames();
}
