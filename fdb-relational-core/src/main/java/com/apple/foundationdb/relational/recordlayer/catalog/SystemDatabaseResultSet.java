/*
 * SystemDatabaseResultSet.java
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

package com.apple.foundationdb.relational.recordlayer.catalog;

import com.apple.foundationdb.relational.api.Continuation;
import com.apple.foundationdb.relational.api.KeyValue;
import com.apple.foundationdb.relational.api.QueryProperties;
import com.apple.foundationdb.relational.api.Transaction;
import com.apple.foundationdb.relational.api.exceptions.InvalidCursorStateException;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.recordlayer.AbstractRecordLayerResultSet;
import com.apple.foundationdb.relational.recordlayer.ResumableIterator;
import com.apple.foundationdb.relational.recordlayer.Scannable;

import java.sql.SQLException;

public class SystemDatabaseResultSet extends AbstractRecordLayerResultSet {
    private final Scannable systemScannable;
    private final QueryProperties options;
    private final Transaction txn;
    private final boolean commitOnClose;
    private final String[] fields;

    private boolean nextCalled;
    private ResumableIterator<KeyValue> currentScanner;
    private Continuation currentContinuation;
    private KeyValue nextKeyValue;

    public SystemDatabaseResultSet(Transaction txn,
                                   Scannable directoryScannable,
                                   QueryProperties options,
                                   boolean commitOnClose) throws RelationalException {
        this.txn = txn;
        this.systemScannable = directoryScannable;
        this.options = options;
        this.fields = directoryScannable.getFieldNames();
        this.commitOnClose = commitOnClose;
    }

    @Override
    public boolean next() throws SQLException {
        nextCalled = true;
        if (currentScanner == null) {
            // TODO (yhatem) discuss how to implement metadata query continuations
            try {
                currentScanner = systemScannable.openScan(txn, null, null, null, options);
            } catch (RelationalException e) {
                throw e.toSqlException();
            }
        }
        boolean hasNext =  currentScanner.hasNext();
        if (hasNext) {
            nextKeyValue = currentScanner.next();
        } else {
            try {
                currentContinuation = currentScanner.getContinuation();
            } catch (RelationalException e) {
                throw e.toSqlException();
            }
        }
        return hasNext;

    }

    @Override
    public void close() throws SQLException {
        try {
            if (currentScanner != null) {
                currentScanner.close();
            }
            if (commitOnClose) {
                txn.close();
            }
        } catch (RelationalException e) {
            throw e.toSqlException();
        }
    }

    @Override
    public Object getObject(int position) throws SQLException {
        if (!nextCalled) {
            throw new InvalidCursorStateException("Iterator was not advanced or has terminated").toSqlException();
        }
        if (nextKeyValue.keyColumnCount() > position) {
            return nextKeyValue.key().getObject(position);
        } else {
            return nextKeyValue.value().getObject(position - nextKeyValue.keyColumnCount());
        }
    }

    @Override
    public int getNumFields() throws SQLException {
        if (nextKeyValue == null) {
            throw new InvalidCursorStateException("Iterator was not advanced or has terminated").toSqlException();
        }
        return nextKeyValue.keyColumnCount() + nextKeyValue.value().getNumFields();
    }

    @Override
    public boolean terminatedEarly() {
        return currentScanner.terminatedEarly();
    }

    @Override
    public Continuation getContinuation() {
        return currentContinuation;
    }

    @Override
    protected int getPosition(String fieldName) {
        for (int i = 0; i < fields.length; i++) {
            if (fields[i] != null && fields[i].equalsIgnoreCase(fieldName)) {
                return i;
            }
        }
        return -1;
    }

    @Override
    protected String[] getFieldNames() {
        return fields;
    }
}
