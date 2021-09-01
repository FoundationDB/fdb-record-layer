/*
 * QueryProperties.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2024 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.api;

public class QueryProperties {
    // the isolation level at which the scan takes place -- false: serializable level, true: snapshot level
    private final boolean isSnapshotIsolation;
    // number of records to skip; skipping happens before any rowLimit is applied
    private final int skip;
    // limit the maximum number of records to return
    private final int rowLimit;
    // a limit on the length of time that the cursor will run for
    private final long timeLimit;
    // the limit on the number of records that may be scanned
    private final int scannedRecordsLimit;
    // the limit on the bytes that may be scanned
    private final long scannedBytesLimit;
    // how record scan limit reached is handled -- false: return early with continuation, true: throw exception
    private final boolean failOnScanLimitReached;
    // the streaming mode to use when opening the record cursor -- false: the client will process records one-at-a-time, true: the client will load all records immediately
    private final boolean loadAllRecordsImmediately;
    // Whether to read the entries in reverse order
    private final boolean reverse;

    public static final QueryProperties DEFAULT = newBuilder().build();

    QueryProperties(boolean isSnapshotIsolation, int skip, int rowLimit, long timeLimit, int scannedRecordsLimit,
                    long scannedBytesLimit, boolean failOnScanLimitReached, boolean loadAllRecordsImmediately, boolean reverse) {
        this.isSnapshotIsolation = isSnapshotIsolation;
        this.skip = skip;
        this.rowLimit = rowLimit;
        this.timeLimit = timeLimit;
        this.scannedRecordsLimit = scannedRecordsLimit;
        this.scannedBytesLimit = scannedBytesLimit;
        this.failOnScanLimitReached = failOnScanLimitReached;
        this.loadAllRecordsImmediately = loadAllRecordsImmediately;
        this.reverse = reverse;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * @return whether the query uses snapshot or serializable isolation level
     */
    public boolean isSnapshotIsolation() {
        return isSnapshotIsolation;
    }

    /**
     * @return the number of records to skip in the query before any rowLimit is applied
     */
    public int getSkip() {
        return skip;
    }

    /**
     * @return the limit on the the maximum number of records to return
     */
    public int getRowLimit() {
        return rowLimit;
    }

    /**
     * @return the limit on the length of time that the cursor will run for
     */
    public long getTimeLimit() {
        return timeLimit;
    }

    /**
     * @return the limit on the number of records that may be scanned
     */
    public int getScannedRecordsLimit() {
        return scannedRecordsLimit;
    }

    /**
     * @return the limit on the bytes that may be scanned
     */
    public long getScannedBytesLimit() {
        return scannedBytesLimit;
    }

    /**
     * @return whether it throws exception if the record scan limit is reached. It returns early with continuation if this is false
     */
    public boolean failOnScanLimitReached() {
        return failOnScanLimitReached;
    }

    /**
     * @return whether it loads all records immediately or processes records one-at-a-time, when opening the record cursor
     */
    public boolean loadAllRecordsImmediately() {
        return loadAllRecordsImmediately;
    }

    /**
     * @return whether it reads the entries in reverse order
     */
    public boolean isReverse() {
        return reverse;
    }

    public static class Builder {
        private boolean isSnapshotIsolation = false;
        private int skip = 0;
        private int rowLimit = 0;
        private long timeLimit = 0L;
        private int scannedRecordsLimit = Integer.MAX_VALUE;
        private long scannedBytesLimit = Long.MAX_VALUE;
        private boolean failOnScanLimitReached = false;
        private boolean loadAllRecordsImmediately = false;
        private boolean reverse = false;

        public Builder setIsSnapshotIsolation(boolean isSnapshotIsolation) {
            this.isSnapshotIsolation = isSnapshotIsolation;
            return this;
        }

        public Builder setSkip(int skip) {
            this.skip = skip;
            return this;
        }

        public Builder setRowLimit(int rowLimit) {
            this.rowLimit = rowLimit;
            return this;
        }

        public Builder setTimeLimit(long timeLimit) {
            this.timeLimit = timeLimit;
            return this;
        }

        public Builder setScannedRecordsLimit(int scannedRecordsLimit) {
            this.scannedRecordsLimit = scannedRecordsLimit;
            return this;
        }

        public Builder setScannedBytesLimit(long scannedBytesLimit) {
            this.scannedBytesLimit = scannedBytesLimit;
            return this;
        }

        public Builder setFailOnScanLimitReached(boolean failOnScanLimitReached) {
            this.failOnScanLimitReached = failOnScanLimitReached;
            return this;
        }

        public Builder setLoadAllRecordsImmediately(boolean loadAllRecordsImmediately) {
            this.loadAllRecordsImmediately = loadAllRecordsImmediately;
            return this;
        }

        public Builder setReverse(boolean reverse) {
            this.reverse = reverse;
            return this;
        }

        public QueryProperties build() {
            return new QueryProperties(isSnapshotIsolation, skip, rowLimit, timeLimit, scannedRecordsLimit,
                    scannedBytesLimit, failOnScanLimitReached, loadAllRecordsImmediately, reverse);
        }
    }
}
