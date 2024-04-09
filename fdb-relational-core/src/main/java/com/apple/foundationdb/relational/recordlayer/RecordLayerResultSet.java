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
import com.apple.foundationdb.relational.api.StructMetaData;
import com.apple.foundationdb.relational.api.exceptions.UncheckedRelationalException;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.sql.SQLException;
import java.util.Objects;

public class RecordLayerResultSet extends AbstractRecordLayerResultSet {

    @Nonnull
    private final ResumableIterator<Row> currentCursor;

    // needed until TODO is fixed
    @Nullable
    private final EmbeddedRelationalConnection connection;

    private Row currentRow;

    @Nullable
    private final Integer planHash;

    private volatile boolean closed;

    @Nonnull
    private final EnrichContinuationFunction enrichContinuationFunction;

    public RecordLayerResultSet(@Nonnull StructMetaData metaData,
                                @Nonnull final ResumableIterator<Row> iterator,
                                @Nullable final EmbeddedRelationalConnection connection) {
        this(metaData, iterator, connection, EnrichContinuationFunction.identity(), null);
    }

    public RecordLayerResultSet(@Nonnull StructMetaData metaData,
                                @Nonnull final ResumableIterator<Row> iterator,
                                @Nullable final EmbeddedRelationalConnection connection,
                                @Nonnull final EnrichContinuationFunction enrichContinuationFunction,
                                @Nullable Integer planHash) {
        super(metaData);
        this.currentCursor = iterator;
        this.connection = connection;
        this.enrichContinuationFunction = enrichContinuationFunction;
        this.planHash = planHash;
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

    @Nullable
    @Override
    public NoNextRowReason noNextRowReason() {
        if (currentRow != null) {
            return null;
        }
        if (currentCursor.hasNext()) {
            return null;
        }
        if (currentCursor.terminatedEarly()) {
            return NoNextRowReason.EXEC_LIMIT_REACHED;
        } else {
            return NoNextRowReason.NO_MORE_ROWS;
        }
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
        this.closed = true;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return this.closed;
    }

    @Override
    @Nonnull
    public Continuation getContinuation() throws SQLException {
        try {
            return enrichContinuationFunction.apply(currentCursor.getContinuation());
        } catch (RelationalException e) {
            throw e.toSqlException();
        }
    }

    /**
     * Get rid of this method. This is only used for the YAML driver to check for the correct plan hash.
     * We should rather do that through a command like {@code PLANHASH query} a la {@code EXPLAIN query}. Or maybe
     * have explain return two columns.
     * @return the plan hash
     */
    @Override
    public int getPlanHash() {
        return Objects.requireNonNull(planHash);
    }

    @FunctionalInterface
    public interface EnrichContinuationFunction {
        @Nonnull
        Continuation apply(@Nonnull Continuation continuation) throws RelationalException;

        static EnrichContinuationFunction identity() {
            return continuation -> continuation;
        }
    }
}
