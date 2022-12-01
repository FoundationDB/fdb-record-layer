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

import java.sql.SQLException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class RecordLayerResultSet extends AbstractRecordLayerResultSet {

    @Nonnull
    private final ResumableIterator<Row> currentCursor;

    // needed until TODO is fixed
    @Nullable
    private final EmbeddedRelationalConnection connection;

    private Row currentRow;

    public RecordLayerResultSet(@Nonnull StructMetaData metaData,
                                @Nonnull final ResumableIterator<Row> iterator,
                                @Nullable final EmbeddedRelationalConnection connection) {
        super(metaData);
        this.currentCursor = iterator;
        this.connection = connection;
    }

    @Override
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
    @Nonnull
    public Continuation getContinuation() throws SQLException {
        try {
            return currentCursor.getContinuation();
        } catch (RelationalException e) {
            throw e.toSqlException();
        }
    }
}
