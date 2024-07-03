/*
 * ByteScanLimiterFactory.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2020 Apple Inc. and the FoundationDB project authors
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
 * A factory that produces implementations of {@link ByteScanLimiter}s.
 */
@API(API.Status.INTERNAL)
public class ByteScanLimiterFactory
{
    private static final Untracked UNTRACKED = new Untracked();

    private ByteScanLimiterFactory() {
        // Cannot instantiate the factory
    }

    /**
     * Creates a limiter that enforces a maximum number of bytes that can be processed in a single scan.
     *
     * @param limit the maximum number of bytes that can be processed in a single scan
     * @return an enforcing limiter
     */
    public static ByteScanLimiter enforce(long limit) {
        return new Enforcing(limit);
    }

    /**
     * Creates a limiter that tracks the number of bytes that has been scanned, but does not impose a limit.
     * @return a tracking limiter
     */
    public static ByteScanLimiter tracking() {
        return new Tracking();
    }

    /**
     * Creates a limiter that neither enforces nor tracks the number of bytes scanned.
     *
     * @return an untracked limiter
     */
    public static ByteScanLimiter untracked() {
        return UNTRACKED;
    }

    /**
     * An implementation of {@link ByteScanLimiter} that tracks and enforces byte scan resource consumption.
     */
    private static class Enforcing implements ByteScanLimiter {

        private final long originalLimit;
        private final AtomicLong bytesRemaining;

        private Enforcing(long limit) {
            originalLimit = limit;
            bytesRemaining = new AtomicLong(limit);
        }

        @Override
        @Nonnull
        public ByteScanLimiter reset() {
            return new Enforcing(originalLimit);
        }

        @Override
        public boolean isEnforcing() {
            return true;
        }

        @Override
        public boolean hasBytesRemaining() {
            return bytesRemaining.get() > 0;
        }

        @Override
        public void registerScannedBytes(long bytes) {
            bytesRemaining.addAndGet(-bytes);
        }

        @Override
        public long getLimit() {
            return originalLimit;
        }

        @Override
        public long getBytesScanned() {
            return originalLimit - bytesRemaining.get();
        }

        @Override
        public String toString() {
            return "ByteScanLimiter(" + originalLimit + " limit, " + bytesRemaining.get() + " left)";
        }
    }

    /**
     * An implementation of {@link ByteScanLimiter} that tracks but does not enforce any limit.
     */
    private static class Tracking implements ByteScanLimiter {

        private final AtomicLong bytesScanned = new AtomicLong();

        @Override
        @Nonnull
        public ByteScanLimiter reset() {
            return new Tracking();
        }

        @Override
        public boolean isEnforcing() {
            return false;
        }

        @Override
        public boolean hasBytesRemaining() {
            return true;
        }

        @Override
        public void registerScannedBytes(long bytes) {
            bytesScanned.addAndGet(bytes);
        }

        @Override
        public long getLimit() {
            return Long.MAX_VALUE;
        }

        @Override
        public long getBytesScanned() {
            return bytesScanned.get();
        }

        @Override
        public String toString() {
            return "ByteScanLimiter(UNLIMITED, " + bytesScanned.get() + " scanned)";
        }
    }

    /**
     * An implementation of {@link ByteScanLimiter} that tracks but does not enforce any limit.
     */
    private static class Untracked implements ByteScanLimiter {
        @Override
        @Nonnull
        public ByteScanLimiter reset() {
            return this;
        }

        @Override
        public boolean isEnforcing() {
            return false;
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
            return Long.MAX_VALUE;
        }

        @Override
        public long getBytesScanned() {
            return 0;
        }

        @Override
        public String toString() {
            return "ByteScanLimiter(NO_LIMIT)";
        }
    }
}
