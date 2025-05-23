/*
 * ItemHandler.java
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

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.RecordCursorResult;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;

import javax.annotation.Nonnull;
import java.util.concurrent.CompletableFuture;

/**
 * A handler of an item during the iteration of a {@link ThrottledRetryingIterator}.
 * @param <T> the type of element in the iteration
 */
@API(API.Status.EXPERIMENTAL)
@FunctionalInterface
public interface ItemHandler<T> {
    /**
     * Process an item.
     * Once done processing, return a future that controls whether to continue the iteration or stop.
     * The quota manager holds the current state of the iteration (per the current transaction). The handler can
     * change the state via {@link ThrottledRetryingIterator.QuotaManager#deleteCountAdd(int)},
     * {@link ThrottledRetryingIterator.QuotaManager#deleteCountInc()} and
     * {@link ThrottledRetryingIterator.QuotaManager#markExhausted()}.
     * @param store the record store to use
     * @param lastResult the result to process
     * @param quotaManager the current quota manager state
     * @return Future (Void) for when the operation is complete
     */
    @Nonnull
    CompletableFuture<Void> handleOneItem(@Nonnull FDBRecordStore store, @Nonnull RecordCursorResult<T> lastResult, @Nonnull ThrottledRetryingIterator.QuotaManager quotaManager);
}
