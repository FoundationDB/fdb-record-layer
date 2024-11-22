/*
 * IndexScrubbingTools.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2024 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.provider.foundationdb;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorResult;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.tuple.Tuple;
import com.google.protobuf.Message;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;

/**
 * Index Scrubbing Toolbox for a specific index maintainer.
 * @param <T> the iteration type
 */
public interface IndexScrubbingTools<T> {

    /**
     * The specific type of scrubbing.
     */
    enum ScrubbingType {
        DANGLING,
        MISSING
    }

    /**
     * A single issue reported by {@link #handleOneItem}.
     */
    class Issue {
        final KeyValueLogMessage logMessage;
        final FDBStoreTimer.Counts timerCounter;
        final FDBStoredRecord<Message> recordToIndex;

        public Issue(final KeyValueLogMessage logMessage, final FDBStoreTimer.Counts timerCounter, final FDBStoredRecord<Message> recordToIndex) {
            this.logMessage = logMessage;
            this.timerCounter = timerCounter;
            this.recordToIndex = recordToIndex;
        }
    }

    String getName();

    RecordCursor<T> getIterator(TupleRange range, FDBRecordStore store, int limit);

    Tuple getContinuation(RecordCursorResult<T> result);

    CompletableFuture<Issue> handleOneItem(FDBRecordStore store, Transaction transaction, RecordCursorResult<T> result);

    default void presetCommonParams(Index index, boolean allowRepair, boolean isSynthetic, Collection<RecordType> types) {
        // no-op
    }
}
