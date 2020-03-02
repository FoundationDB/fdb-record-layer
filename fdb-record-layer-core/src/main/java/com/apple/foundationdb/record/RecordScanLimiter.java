/*
 * RecordScanLimiter.java
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

import com.apple.foundationdb.annotation.API;

import javax.annotation.Nonnull;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Track number of records scanned up to some limit, after which record scans should not be allowed.
 *
 * @see ExecuteState#getRecordScanLimiter
 */
@API(API.Status.MAINTAINED)
public class RecordScanLimiter {
    /**
     * Value that indicates that the scan limiter should effectively be used purely for tracking the number of
     * records scanned without actually enforcing a limit.
     */
    public static final int UNLIMITED = Integer.MAX_VALUE;

    private final int originalLimit;
    private final AtomicInteger allowedRecordScansRemaining;

    public RecordScanLimiter(int limit) {
        originalLimit = limit;
        allowedRecordScansRemaining = new AtomicInteger(limit);
    }

    /**
     * Create a new {@code RecordScanLimiter} with this limiter's original limit, ignoring any calls to {@link #tryRecordScan()}.
     * @return a new limiter with the same original scan limit as this limiter
     */
    @Nonnull
    public RecordScanLimiter reset() {
        return new RecordScanLimiter(originalLimit);
    }

    /**
     * Return whether or not this limiter has an actual limit.
     *
     * @return {@code true} if the limiter is enforcing a limit.
     */
    public boolean isUnlimited() {
        return this.originalLimit == UNLIMITED;
    }

    /**
     * Atomically decrement the counter and return false if falls below 0.
     * @return <code>true</code> if the remaining count is at least 0, and <code>false</code> if it is less than 0
     */
    public boolean tryRecordScan() {
        int remaining = allowedRecordScansRemaining.getAndDecrement();

        // Realistically we should never able to scan MAX_INT records in a single transaction, but you never know...
        return remaining > 0 || originalLimit == UNLIMITED;
    }

    /**
     * Get the record scan limit. In particular, this will return the target
     * number of records that this limiter is being used to enforce.
     *
     * @return the record scan limit being enforced
     */
    public int getLimit() {
        return originalLimit;
    }

    /**
     * Returns the number of records that have been scanned thus far.
     *
     * @return the number of records that have been scanned
     */
    public int getRecordsScanned() {
        return originalLimit - allowedRecordScansRemaining.get();
    }

    @Override
    public String toString() {
        return String.format("RecordScanLimiter(%d limit, %d left)", originalLimit, allowedRecordScansRemaining.get());
    }

    /**
     * A non-tracking, non-enforcing limiter.
     */
    protected static class Untracked extends RecordScanLimiter {
        public static final Untracked INSTANCE = new Untracked();

        private Untracked() {
            super(UNLIMITED);
        }

        @Nonnull
        @Override
        public RecordScanLimiter reset() {
            return this;
        }

        @Override
        public boolean tryRecordScan() {
            return true;
        }

        @Override
        public int getRecordsScanned() {
            return 0;
        }
    }
}

