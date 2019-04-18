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
     * Atomically decrement the counter and return false if falls below 0.
     * @return <code>true</code> if the remaining count is at least 0, and <code>false</code> if it is less than 0
     */
    public boolean tryRecordScan() {
        return allowedRecordScansRemaining.getAndDecrement() > 0;
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

    @Override
    public String toString() {
        return String.format("RecordScanLimiter(%d limit, %d left)", originalLimit, allowedRecordScansRemaining.get());
    }
}

