/*
 * ExecuteState.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2018 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record;


import com.apple.foundationdb.API;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * An encapsulation of the mutable state of query or scan execution. In general, parameters that affect
 * a query/scan execution (as opposed to a query plan) belong here if they involve mutable state and in
 * {@link ExecuteProperties} otherwise.
 * In general, the state object should be constructed by as part of a "root" <code>ExecuteProperties</code> rather than
 * directly by the client.
 */
@API(API.Status.MAINTAINED)
public class ExecuteState {
    /**
     * An empty execute state with no record scan limit.
     */
    public static final ExecuteState NO_SCANNED_RECORDS_LIMIT = new ExecuteState();

    @Nullable
    private final RecordScanLimiter recordScanLimiter;

    public ExecuteState(@Nullable RecordScanLimiter recordScanLimiter) {
        this.recordScanLimiter = recordScanLimiter;
    }

    public ExecuteState() {
        this(null);
    }

    /**
     * Create a new {@code ExecuteState} that represents the same properties of the execution that this state represented
     * when it was first created, but with an independent set of mutable objects. For example, the {@link RecordScanLimiter}
     * of the returned {@code ExecuteState} has a limit equal to the original limit that was used to create this state's
     * {@code RecordScanLimiter}. It is up to the implementor to ensure that all components of the state are reset in
     * a meaningful way, since this might vary depending on that piece of state.
     * @return a new state that represents the same properties but does not share mutable state with this {@code ExecuteState}
     */
    @Nonnull
    public ExecuteState reset() {
        if (recordScanLimiter == null) {
            return NO_SCANNED_RECORDS_LIMIT;
        }
        return new ExecuteState(recordScanLimiter.reset());
    }
    
    /**
     * Get a limiter for the maximum number of records that can be retrieved from the database.
     * Note that this limit is not strictly enforced, depending on the underlying {@link com.apple.foundationdb.record.cursors.BaseCursor}
     * implementation.
     * All base cursors are always permitted to load at least one key-value entry before it is stopped by the record scan
     * limit to ensure that cursors with multiple child cursors (such as {@link com.apple.foundationdb.record.provider.foundationdb.cursors.UnionCursor})
     * can always make progress. Thus, a query execution might overrun its scanned records limit by up to the number of
     * base cursors in the cursor tree.
     * Particular base cursors may exceed the record scan limit in other ways, which are documented in their Javadocs.
     * @return the record scan limiter or <code>null</code> if no limit is set
     */
    @Nullable
    public RecordScanLimiter getRecordScanLimiter() {
        return recordScanLimiter;
    }

    @Override
    public String toString() {
        if (recordScanLimiter == null) {
            return "State()";
        }
        return "State(" + recordScanLimiter + ")";
    }
}
