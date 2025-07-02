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

import com.apple.foundationdb.annotation.API;

import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.relational.api.Continuation;
import com.apple.foundationdb.relational.api.Row;
import com.apple.foundationdb.relational.api.StructMetaData;
import com.apple.foundationdb.relational.api.exceptions.UncheckedRelationalException;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.sql.SQLException;

import static com.apple.foundationdb.relational.api.exceptions.ErrorCode.UNSUPPORTED_OPERATION;

@API(API.Status.EXPERIMENTAL)
public class RecordLayerResultSet extends AbstractRecordLayerResultSet {

    @Nonnull
    private final ResumableIterator<Row> currentCursor;

    // needed until TODO is fixed
    @Nullable
    private final EmbeddedRelationalConnection connection;

    private Row currentRow;

    private volatile boolean closed;

    @Nonnull
    private final EnrichContinuationFunction enrichContinuationFunction;

    public RecordLayerResultSet(@Nonnull StructMetaData metaData,
                                @Nonnull final ResumableIterator<Row> iterator,
                                @Nullable final EmbeddedRelationalConnection connection) {
        this(metaData, iterator, connection, EnrichContinuationFunction.identity());
    }

    public RecordLayerResultSet(@Nonnull StructMetaData metaData,
                                @Nonnull final ResumableIterator<Row> iterator,
                                @Nullable final EmbeddedRelationalConnection connection,
                                @Nonnull final EnrichContinuationFunction enrichContinuationFunction) {
        super(metaData);
        this.currentCursor = iterator;
        this.connection = connection;
        this.enrichContinuationFunction = enrichContinuationFunction;
    }

    @Override
    protected boolean hasNext() {
        return currentCursor.hasNext();
    }

    @Override
    @SuppressWarnings("PMD.PreserveStackTrace")
    protected Row advanceRow() throws RelationalException {
        currentRow = null;
        if (currentCursor.hasNext()) {
            try {
                currentRow = currentCursor.next();
            } catch (UncheckedRelationalException e) {
                throw e.unwrap();
            }
        }
        return currentRow;
    }

    @Override
    public void close() throws SQLException {
        try {
            currentCursor.close();
        } catch (RelationalException e) {
            throw e.toSqlException();
        }
        if (connection != null && connection.shouldCommit() && connection.inActiveTransaction()) {
            connection.commitInternal();
        }
        this.closed = true;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return this.closed;
    }

    @Nonnull
    @Override
    public Continuation getContinuation() throws SQLException {
        if (hasNext()) {
            throw new SQLException("Continuation can only be returned once the result set has been exhausted", UNSUPPORTED_OPERATION.getErrorCode());
        }
        try {
            return enrichContinuationFunction.apply(currentCursor.getContinuation(), continuationReason());
        } catch (RelationalException e) {
            throw e.toSqlException();
        }
    }

    private Continuation.Reason continuationReason() {
        if (currentRow != null) {
            return Continuation.Reason.USER_REQUESTED_CONTINUATION;
        }
        if (currentCursor.hasNext()) {
            return Continuation.Reason.USER_REQUESTED_CONTINUATION;
        }
        if (currentCursor.terminatedEarly()) {
            return Continuation.Reason.TRANSACTION_LIMIT_REACHED;
        } else if (currentCursor.getNoNextReason().equals(RecordCursor.NoNextReason.RETURN_LIMIT_REACHED)) {
            return Continuation.Reason.QUERY_EXECUTION_LIMIT_REACHED;
        } else {
            return Continuation.Reason.CURSOR_AFTER_LAST;
        }
    }

    @FunctionalInterface
    public interface EnrichContinuationFunction {
        @Nonnull
        Continuation apply(@Nonnull Continuation continuation, Continuation.Reason reason) throws RelationalException;

        static EnrichContinuationFunction identity() {
            return (continuation, reason) -> continuation;
        }
    }
}
