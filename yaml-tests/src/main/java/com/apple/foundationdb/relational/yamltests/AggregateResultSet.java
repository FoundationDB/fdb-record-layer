/*
 * AggregateResultSet.java
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

import com.apple.foundationdb.relational.api.Continuation;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.RelationalResultSetMetaData;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;

import javax.annotation.Nonnull;
import java.sql.SQLException;
import java.util.Iterator;

/**
 * An implementation of a result set that is made up of a set of inner result sets.
 * This result set delegates all data get* calls to the inner delegates.
 * At this time, each inner result set if assumed to represent a single row.
 * TODO: It may be beneficial to create a more sophisticated aggregate that can take inner result sets that have more than
 * TODO: one row each.
 */
public class AggregateResultSet extends AbstractAggregateResultSet {
    private final Continuation continuation;
    private final Iterator<? extends RelationalResultSet> rowIterator;

    public AggregateResultSet(RelationalResultSetMetaData metadata, Continuation continuation, Iterator<RelationalResultSet> rowIterator) {
        super(metadata);
        this.continuation = continuation;
        this.rowIterator = rowIterator;
    }

    @Override
    protected boolean hasNext() {
        return rowIterator.hasNext();
    }

    @Override
    protected RelationalResultSet advanceRow() throws SQLException {
        if (!hasNext()) {
            return null;
        }
        return rowIterator.next();
    }

    @Nonnull
    @Override
    public Continuation getContinuation() throws SQLException {
        if (hasNext()) {
            throw new SQLException("Continuation can only be returned once the result set has been exhausted", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
        }
        return continuation;
    }
}
