/*
 * ByteScanLimiter.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2019 Apple Inc. and the FoundationDB project authors
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
import java.util.concurrent.atomic.AtomicLong;

/**
 * Track the number of bytes scanned up to some limit, after which record scans should not be allowed.
 *
 * @see ExecuteState#getByteScanLimiter()
 */
@API(API.Status.INTERNAL)
public class ByteScanLimiter {
    /**
     * Value that indicates that the scan limiter should effectively be used purely for tracking the number of
     * bytes scanned without actually enforcing a limit.
     */
    public static final long UNLIMITED = Long.MAX_VALUE;

    private final long originalLimit;
    private final AtomicLong bytesRemaining;

    public ByteScanLimiter(long limit) {
        originalLimit = limit;
        bytesRemaining = new AtomicLong(limit);
    }

    /**
     * Create a new {@code ByteScanLimiter} with this limiter's original limit, ignoring any calls to
     * {@link #hasBytesRemaining()} and {@link #registerScannedBytes(long)}.
     * @return a new {@code ByteScanLimiter} with this limiter's original limit
     */
    @Nonnull
    public ByteScanLimiter reset() {
        return new ByteScanLimiter(originalLimit);
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
     * Atomically check whether the number of remaining bytes is at least 0.
     * @return {@code true} if the remaining count is at least 0 and {@code false} if it is less than 0
     */
    public boolean hasBytesRemaining() {
        return bytesRemaining.get() > 0 || originalLimit == UNLIMITED;
    }

    /**
     * Atomically decrement the number of remaining bytes by the given number of bytes.
     * @param bytes the number of bytes to register
     */
    public void registerScannedBytes(long bytes) {
        bytesRemaining.addAndGet(-bytes);
    }

    /**
     * Get the byte scan limit. In particular, this will return the target
     * number of bytes that this limiter is being used to enforce.
     *
     * @return the byte scan limit being enforced
     */
    public long getLimit() {
        return originalLimit;
    }

    /**
     * Returns the number of bytes that have been scanned thus far.
     *
     * @return the number of bytes that have been scanned
     */
    public long getBytesScanned() {
        return originalLimit - bytesRemaining.get();
    }

    @Override
    public String toString() {
        return String.format("ByteScanLimiter(%d limit, %d left)", originalLimit, bytesRemaining.get());
    }

    /**
     * A non-tracking, non-enforcing limiter.
     */
    protected static class Untracked extends ByteScanLimiter {
        public static final Untracked INSTANCE = new Untracked();

        private Untracked() {
            super(UNLIMITED);
        }

        @Nonnull
        @Override
        public ByteScanLimiter reset() {
            return this;
        }

        @Override
        public boolean hasBytesRemaining() {
            return true;
        }

        @Override
        public void registerScannedBytes(long bytes) {
            // IGNORED
        }

        @Override
        public long getLimit() {
            return UNLIMITED;
        }

        @Override
        public long getBytesScanned() {
            return 0L;
        }
    }
}
