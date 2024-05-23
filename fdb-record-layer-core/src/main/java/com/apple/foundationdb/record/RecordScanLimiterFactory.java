/*
 * RecordScanLimiterFactory.java
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
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A factory that produces implementations of {@link RecordScanLimiter}s.
 */
@API(API.Status.INTERNAL)
public class RecordScanLimiterFactory {

    private static final RecordScanLimiter UNTRACKED = new Untracked();

    /**
     * Creates a limiter that enforces a maximum number of records that can be processed in a single scan.
     *
     * @param limit the maximum number of records that can be processed in a single scan
     * @return an enforcing limiter
     */
    public static RecordScanLimiter enforce(int limit) {
        return new Enforcing(limit);
    }

    /**
     * Creates a limiter that tracks the number of records that has been scanned, but does not impose a limit.
     * @return a tracking limiter
     */
    public static RecordScanLimiter tracking() {
        return new Tracking();
    }

    /**
     * Creates a limiter that neither enforces nor tracks the number of records scanned.
     *
     * @return an untracked limiter
     */
    public static RecordScanLimiter untracked() {
        return UNTRACKED;
    }

    /**
     * Implementation of a {@link RecordScanLimiter} that tracks records scanned and enforces a limit.
     */
    private static class Enforcing implements RecordScanLimiter {
        private final int originalLimit;
        private final AtomicInteger allowedRecordScansRemaining;

        public Enforcing(int limit) {
            originalLimit = limit;
            allowedRecordScansRemaining = new AtomicInteger(limit);
        }

        @Override
        @Nonnull
        public RecordScanLimiter reset() {
            return new Enforcing(originalLimit);
        }

        @Override
        public boolean isEnforcing() {
            return true;
        }

        @Override
        public boolean tryRecordScan() {
            return allowedRecordScansRemaining.getAndDecrement() > 0;
        }

        @Override
        public int getLimit() {
            return originalLimit;
        }

        @Override
        public int getRecordsScanned() {
            return originalLimit - allowedRecordScansRemaining.get();
        }

        @Override
        public String toString() {
            return "RecordScanLimiter(" + originalLimit + " limit, " + allowedRecordScansRemaining.get() + " left)";
        }
    }

    /**
     * Implementation of a {@link RecordScanLimiter} that tracks records scanned but does not enforce a limit.
     */
    private static class Tracking implements RecordScanLimiter {
        private final AtomicInteger recordsScanned = new AtomicInteger();

        @Override
        @Nonnull
        public RecordScanLimiter reset() {
            return new Tracking();
        }

        @Override
        public boolean isEnforcing() {
            return false;
        }

        @Override
        public boolean tryRecordScan() {
            recordsScanned.incrementAndGet();
            return true;
        }

        @Override
        public int getLimit() {
            return Integer.MAX_VALUE;
        }

        @Override
        public int getRecordsScanned() {
            return recordsScanned.get();
        }

        @Override
        public String toString() {
            return "RecordScanLimiter(UNLIMITED, " + recordsScanned.get() + " scanned)";
        }
    }

    /**
     * Implementation of a {@link RecordScanLimiter} that does not track or enforce any limits.
     */
    private static class Untracked implements RecordScanLimiter {
        @Override
        @Nonnull
        public RecordScanLimiter reset() {
            return this;
        }

        @Override
        public boolean isEnforcing() {
            return false;
        }

        @Override
        public boolean tryRecordScan() {
            return true;
        }

        @Override
        public int getLimit() {
            return Integer.MAX_VALUE;
        }

        @Override
        public int getRecordsScanned() {
            return 0;
        }

        @Override
        public String toString() {
            return "RecordScanLimiter(NO_LIMIT)";
        }
    }
}
