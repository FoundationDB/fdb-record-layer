/*
 * CommittingIteratorResultSet.java
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

package com.apple.foundationdb.relational.recordlayer;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.relational.api.Row;
import com.apple.foundationdb.relational.api.StructMetaData;

import javax.annotation.Nonnull;
import java.sql.SQLException;
import java.util.Iterator;

/**
 * A version of {@link IteratorResultSet} that commits the connection when closed, if appropriate.
 * Note: This might misalign with the actual spec for JDBC, but is needed for operations that return a result set
 * that isn't tied to a {@link RecordLayerResultSet}, and also have modifications that need to be committed.
 * It would be good to align better with the JDBC spec, but that may be hard if we have updates with continuations.
 */
@API(API.Status.INTERNAL)
public class CommittingIteratorResultSet extends IteratorResultSet {
    private boolean closed;
    private final EmbeddedRelationalConnection connection;

    public CommittingIteratorResultSet(@Nonnull final StructMetaData metaData,
                                       @Nonnull final Iterator<? extends Row> rowIter,
                                       final int initialRowPosition,
                                       @Nonnull final EmbeddedRelationalConnection connection) {
        super(metaData, rowIter, initialRowPosition);
        this.connection = connection;
    }

    @Override
    public void close() throws SQLException {
        super.close();
        if (connection != null && connection.canCommit() && connection.inActiveTransaction()) {
            connection.commitInternal();
        }
    }
}
