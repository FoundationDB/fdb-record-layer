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
import com.apple.foundationdb.relational.api.StructMetaData;
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

    private final Iterator<? extends Row> rowIter;

    private int currentRowPosition;

    public IteratorResultSet(StructMetaData metaData, Iterator<? extends Row> rowIter, int initialRowPosition) {
        super(metaData);
        this.rowIter = rowIter;
        this.currentRowPosition = initialRowPosition;
    }

    @Override
    public Continuation getContinuation() throws SQLException {
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
    public void close() throws SQLException {
        //no-op at the moment
    }

    @Override
    protected Row advanceRow() throws RelationalException {
        if (!rowIter.hasNext()) {
            return null;
        }
        currentRowPosition++;
        return rowIter.next();
    }
}
