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
import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorResult;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.record.provider.common.StoreTimer;
import com.apple.foundationdb.tuple.Tuple;
import com.google.protobuf.Message;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;

/**
 * Index Scrubbing Toolbox for a specific index maintainer.
 * @param <T> the iteration type
 */
@API(API.Status.EXPERIMENTAL)
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
        /**
         * If non-null, log this message after the transaction is successfully completed.
         */
        final KeyValueLogMessage logMessage;
        /**
         * If non-null, update this counter after the transaction is successfully completed.
         */
        final StoreTimer.Count timerCounter;
        /**
         * If non-null, let the indexer index this record. This makes sense if repair is allowed and the scrubber did not fix
         * the issue on its own.
         */
        final FDBStoredRecord<Message> recordToIndex;

        public Issue(final KeyValueLogMessage logMessage, final FDBStoreTimer.Counts timerCounter, final FDBStoredRecord<Message> recordToIndex) {
            this.logMessage = logMessage;
            this.timerCounter = timerCounter;
            this.recordToIndex = recordToIndex;
        }
    }

    /**
     * Return a cursor that can be iterated by the indexing process.
     * Note 1: for scrubbing, the cursor will always move forward.
     * Note 2: instead of using a filtered cursor, skip irrelevant items in {@link #handleOneItem}. This will make the code indexer throttle friendly.
     * @param range range to iterate on. Low value is inclusive, high value is exclusive
     * @param store record store
     * @param limit max items to iterate (used for throttling)
     * @return a cursor
     */
    RecordCursor<T> getCursor(TupleRange range, FDBRecordStore store, int limit);

    /**
     * Return a continuation for a given result. This continuation is used to save the scrubbed range in the record store, and
     * will become a low value in future range to scrub.
     * @param result a result that was returned by a cursor provided by {@link #getCursor}
     * @return continuation tuple
     */
    Tuple getKeyFromCursorResult(RecordCursorResult<T> result);

    /**
     * Check the validity of a given a specific cursor item. If applicable, return a {@link Issue} to report
     * a problem.
     * @param store record store
     * @param transaction an open transaction
     * @param result an item that was returned by a cursor provided by {@link #getCursor}
     * @return null if the result valid, an {@link Issue} if not.
     */
    CompletableFuture<Issue> handleOneItem(FDBRecordStore store, Transaction transaction, RecordCursorResult<T> result);

    /**
     * This function is called prior to any other operation to set common parameters. For a given scrubbing tool
     * implementation, any parameter (or all) may be ignored.
     * @param index the index to operate
     * @param allowRepair true if repair is allowed
     * @param isSynthetic true if this is a synthetic index
     * @param types list of record types that are associated with this index
     */
    default void presetCommonParams(Index index, boolean allowRepair, boolean isSynthetic, Collection<RecordType> types) {
        // no-op
    }
}
