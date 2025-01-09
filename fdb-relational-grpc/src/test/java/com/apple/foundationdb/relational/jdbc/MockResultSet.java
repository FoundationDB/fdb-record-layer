/*
 * MockResultSet.java
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

package com.apple.foundationdb.relational.jdbc;

import com.apple.foundationdb.relational.api.Continuation;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;

import javax.annotation.Nonnull;
import java.sql.SQLException;
import java.util.Iterator;

/**
 * A Concrete implementation of a testing result set that can iterate over a collection of rows.
 * This is similar to the IteratorResultSet.
 */
public class MockResultSet extends AbstractMockResultSet {
    private final Iterator<? extends MockResultSetRow> rowIterator;
    private final Continuation continuation;
    private int currentRowPosition;

    public MockResultSet(MockResultSetMetadata metadata, Iterator<MockResultSetRow> rowIterator, Continuation continuation) {
        super(metadata);
        this.rowIterator = rowIterator;
        this.continuation = continuation;
        this.currentRowPosition = 0;
    }

    @Override
    protected boolean hasNext() {
        return rowIterator.hasNext();
    }

    @Override
    protected MockResultSetRow advanceRow() throws RelationalException {
        if (!hasNext()) {
            return null;
        }
        currentRowPosition++;
        return rowIterator.next();
    }

    @Nonnull
    @Override
    public Continuation getContinuation() throws SQLException {
        return continuation;
    }
}
