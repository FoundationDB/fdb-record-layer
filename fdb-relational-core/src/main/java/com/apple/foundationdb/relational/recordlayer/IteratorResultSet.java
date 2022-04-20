/*
 * IteratorResultSet.java
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
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.util.SpotBugsSuppressWarnings;

import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.util.Iterator;

import javax.annotation.Nullable;

/**
 * Placeholder result set until better generic abstractions come along.
 */
@SpotBugsSuppressWarnings(value = "EI_EXPOSE_REP2", justification = "Intentionally exposed for performance reasons")
public class IteratorResultSet extends AbstractRecordLayerResultSet {
    private final String[] fieldNames;
    private final Iterator<Row> rowIter;

    private int currentRowPosition;
    private Row currentRow;

    public IteratorResultSet(String[] fieldNames, Iterator<Row> rowIter, int initialRowPosition) {
        this.fieldNames = fieldNames;
        this.rowIter = rowIter;
        this.currentRowPosition = initialRowPosition;
    }

    @Override
    protected int getZeroBasedPosition(String fieldName) throws SQLException, InvalidColumnReferenceException {
        for (int i = 0; i < fieldNames.length; i++) {
            if (fieldNames[i].equalsIgnoreCase(fieldName)) {
                return i;
            }
        }
        throw new InvalidColumnReferenceException("No column for value <" + fieldName + ">");
    }

    @Override
    public Continuation getContinuation() throws RelationalException {
        boolean hasNext = rowIter.hasNext();
        boolean beginning = currentRowPosition == 0;
        int currPos = currentRowPosition + 1;
        return new Continuation() {
            @Nullable
            @Override
            public byte[] getBytes() {
                if (beginning) {
                    return null;
                } else if (hasNext) {
                    ByteBuffer buffer = ByteBuffer.allocate(4);
                    buffer.putInt(currPos);
                    buffer.flip();
                    return buffer.array();
                } else {
                    return new byte[]{};
                }
            }

            @Override
            public boolean atBeginning() {
                return beginning;
            }

            @Override
            public boolean atEnd() {
                return !hasNext;
            }
        };
    }

    @Override
    protected String[] getFieldNames() {
        return fieldNames;
    }

    @Override
    public boolean next() throws SQLException {
        if (!rowIter.hasNext()) {
            return false;
        }
        currentRow = rowIter.next();
        currentRowPosition++;
        return true;
    }

    @Override
    public void close() throws SQLException {
        //no-op at the moment
    }

    @Override
    public Object getObject(int oneBasedPosition) throws SQLException {
        try {
            return currentRow.getObject(oneBasedPosition - 1);
        } catch (InvalidColumnReferenceException e) {
            throw e.toSqlException();
        }
    }
}
