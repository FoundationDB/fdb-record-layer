/*
 * RecordLayerResultSet.java
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

import com.apple.foundationdb.relational.api.Continuation;
import com.apple.foundationdb.relational.api.Row;
import com.apple.foundationdb.relational.api.exceptions.InvalidColumnReferenceException;
import com.apple.foundationdb.relational.api.exceptions.InvalidCursorStateException;
import com.apple.foundationdb.relational.api.exceptions.OperationUnsupportedException;
import com.apple.foundationdb.relational.api.exceptions.UncheckedRelationalException;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.util.SpotBugsSuppressWarnings;

import com.google.protobuf.Message;

import java.sql.SQLException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class RecordLayerResultSet extends AbstractRecordLayerResultSet {

    @Nonnull
    private final ResumableIterator<Row> currentCursor;

    // needed until TODO is fixed
    @Nullable
    private final RecordStoreConnection connection;

    @Nonnull
    private final String[] fieldNames;

    private Row currentRow;

    @SpotBugsSuppressWarnings(value = "EI_EXPOSE_REP2", justification = "internal implementation should have proper usage")
    public RecordLayerResultSet(@Nonnull String[] fieldNames,
                                @Nonnull final ResumableIterator<Row> iterator,
                                @Nullable final RecordStoreConnection connection) throws RelationalException {
        this.fieldNames = fieldNames;
        this.currentCursor = iterator;
        this.connection = connection;
    }

    @Override
    public boolean next() throws SQLException {
        currentRow = null;

        if (!currentCursor.hasNext()) {
            return false;
        }

        try {
            currentRow = currentCursor.next();
        } catch (UncheckedRelationalException e) {
            throw e.unwrap().toSqlException();
        }
        return true;
    }

    @Override
    public void close() throws SQLException {
        try {
            currentCursor.close();
        } catch (RelationalException e) {
            throw e.toSqlException();
        }
        try {
            if (connection != null && connection.getAutoCommit() && connection.transaction != null) {
                connection.transaction.commit();
                connection.transaction.close();
                connection.transaction = null;
            }
        } catch (RelationalException ve) {
            throw ve.toSqlException();
        }
    }

    @Override
    public Object getObject(int position) throws SQLException {
        try {
            if (currentRow == null) {
                throw new InvalidCursorStateException("Cursor was not advanced, or has been exhausted");
            }
            if (position < 1 || position > (currentRow.getNumFields())) {
                throw InvalidColumnReferenceException.getExceptionForInvalidPositionNumber(position);
            }
            position -= 1; // Switch to 0 based index
            return currentRow.getObject(position);
        } catch (RelationalException e) {
            throw e.toSqlException();
        }
    }

    @Override
    protected int getZeroBasedPosition(String fieldName) throws InvalidColumnReferenceException {
        for (int pos = 0; pos < fieldNames.length; pos++) {
            if (fieldNames[pos] != null && fieldNames[pos].equalsIgnoreCase(fieldName)) {
                return pos;
            }
        }
        throw new InvalidColumnReferenceException(fieldName);
    }

    @Override
    protected String[] getFieldNames() {
        return fieldNames;
    }

    @Override
    public boolean supportsMessageParsing() {
        return currentRow != null && currentRow instanceof MessageTuple;
    }

    @Override
    public <M extends Message> M parseMessage() throws SQLException {
        if (!supportsMessageParsing()) {
            throw new OperationUnsupportedException("This ResultSet does not support Message Parsing").toSqlException();
        }
        return ((MessageTuple) currentRow).parseMessage();
    }

    @Override
    public Continuation getContinuation() throws RelationalException {
        return currentCursor.getContinuation();
    }
}
