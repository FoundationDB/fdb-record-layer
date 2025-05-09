/*
 * CursorFactory.java
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

package com.apple.foundationdb.record.provider.foundationdb.cursors.throttled;

import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorResult;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Create a cursor with the given store and last result.
 * @param <T> the type of item the cursor iterates over.
 * This factory method is used by the {@link ThrottledRetryingIterator} to create inner cursors when needed.
 * The iterator creates transactions based off of the constraints given, and for each such transaction, a new inner
 * cursor gets created.
 */
@FunctionalInterface
public interface CursorFactory<T> {
    /**
     * Create a new inner cursor for the {@link ThrottledRetryingIterator}.
     * @param store the record store to use
     * @param lastResult the last result from the previous cursor (use for continuation). Null is none.
     * @param rowLimit the adjusted row limit to use
     * @return a newly created cursor with the given continuation and limit
     */
    RecordCursor<T> createCursor(@Nonnull FDBRecordStore store, @Nullable RecordCursorResult<T> lastResult, int rowLimit);
}
