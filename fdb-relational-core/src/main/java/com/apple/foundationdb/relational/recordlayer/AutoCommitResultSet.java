/*
 * AutoCommitResultSet.java
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
import com.apple.foundationdb.relational.api.exceptions.OperationUnsupportedException;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.google.protobuf.Message;

/**
 * A delegating result set which performs the auto-commit logic necessary for the transaction
 * when the result set is exhausted (or explicitly closed).
 *
 *
 * TODO(bfines) make each of these methods automatically rollback on the correct kind of error
 */
public class AutoCommitResultSet implements RelationalResultSet {
    private final RecordStoreConnection conn;
    private final RelationalResultSet delegate;

    public AutoCommitResultSet(RecordStoreConnection conn, RelationalResultSet delegate) {
        this.conn = conn;
        this.delegate = delegate;
    }

    @Override
    public boolean next() throws RelationalException {
        boolean hasNext = delegate.next();
        if (!hasNext) {
            conn.commit();
        }
        return hasNext;
    }

    @Override
    public void close() throws RelationalException {
        try {
            conn.commit();
        } finally {
            delegate.close();
        }
    }

    @Override
    public boolean getBoolean(int position) throws RelationalException {
        return delegate.getBoolean(position);
    }

    @Override
    public boolean getBoolean(String fieldName) throws RelationalException {
        return delegate.getBoolean(fieldName);
    }

    @Override
    public long getLong(int position) throws RelationalException {
        return delegate.getLong(position);
    }

    @Override
    public long getLong(String fieldName) throws RelationalException {
        return delegate.getLong(fieldName);
    }

    @Override
    public float getFloat(int position) throws RelationalException {
        return delegate.getFloat(position);
    }

    @Override
    public float getFloat(String fieldName) throws RelationalException {
        return delegate.getFloat(fieldName);
    }

    @Override
    public double getDouble(int position) throws RelationalException {
        return delegate.getDouble(position);
    }

    @Override
    public double getDouble(String fieldName) throws RelationalException {
        return delegate.getDouble(fieldName);
    }

    @Override
    public Object getObject(int position) throws RelationalException {
        return delegate.getObject(position);
    }

    @Override
    public Object getObject(String fieldName) throws RelationalException {
        return delegate.getObject(fieldName);
    }

    @Override
    public String getString(int position) throws RelationalException {
        return delegate.getString(position);
    }

    @Override
    public String getString(String fieldName) throws RelationalException {
        return delegate.getString(fieldName);
    }

    @Override
    public Message getMessage(int position) throws RelationalException {
        return delegate.getMessage(position);
    }

    @Override
    public Message getMessage(String fieldName) throws RelationalException {
        return delegate.getMessage(fieldName);
    }

    @Override
    public Iterable<?> getRepeated(int position) throws RelationalException {
        return delegate.getRepeated(position);
    }

    @Override
    public Iterable<?> getRepeated(String fieldName) throws RelationalException {
        return delegate.getRepeated(fieldName);
    }

    @Override
    public boolean supportsMessageParsing() {
        return delegate.supportsMessageParsing();
    }

    @Override
    public <M extends Message> M parseMessage() throws OperationUnsupportedException {
        return delegate.parseMessage();
    }

    @Override
    public IsolationLevel getActualIsolationLevel() {
        return delegate.getActualIsolationLevel();
    }

    @Override
    public IsolationLevel getRequestedIsolationLevel() {
        return delegate.getRequestedIsolationLevel();
    }
}
