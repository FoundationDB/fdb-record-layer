/*
 * ExecuteProperties.java
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

import com.apple.foundationdb.ReadTransaction;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

/**
 * Limits on the execution of a query.
 * <ul>
 * <li>number of records to skip</li>
 * <li>limit on number of records returned</li>
 * <li>time limit for execution</li>
 * <li>limit on number of key-value pairs scanned</li>
 * </ul>
 */
public class ExecuteProperties {
    /**
     * A constant representing that no time limit is set.
     */
    public static final long UNLIMITED_TIME = 0L;
    /**
     * A basic set of properties for an unlimited query/scan execution with serializable isolation.
     */
    public static final ExecuteProperties SERIAL_EXECUTE = ExecuteProperties.newBuilder()
            .setIsolationLevel(IsolationLevel.SERIALIZABLE)
            .build();

    // the isolation level at which the scan takes place
    @Nonnull
    protected final IsolationLevel isolationLevel;

    // number of records to skip; skipping happens before any rowLimit is applied.
    protected final int skip;

    // limit the maximum number of records to return
    protected final int rowLimit;

    // a limit on the length of time that the cursor will run for.
    private final long timeLimit;

    // A wrapper that encapsulates all of the mutable state associated with the execution, such as the record scan limit.
    // In general, the state should be preserved under all transformations except for explicit mutations of the state member.
    @Nonnull
    private final ExecuteState state;

    // how record scan limit reached is handled -- false: return early with continuation, true: throw exception
    private final boolean failOnScanLimitReached;

    private ExecuteProperties(int skip, int rowLimit, @Nonnull IsolationLevel isolationLevel, long timeLimit,
                              @Nonnull ExecuteState state, boolean failOnScanLimitReached) {
        this.skip = skip;
        this.rowLimit = rowLimit;
        this.isolationLevel = isolationLevel;
        this.timeLimit = timeLimit;
        this.state = state;
        this.failOnScanLimitReached = failOnScanLimitReached;
    }

    @Nonnull
    public IsolationLevel getIsolationLevel() {
        return isolationLevel;
    }

    public int getSkip() {
        return skip;
    }

    @Nonnull
    public ExecuteProperties setSkip(final int skip) {
        if (skip == this.skip) {
            return this;
        }
        return copy(skip, rowLimit, timeLimit, isolationLevel, state, failOnScanLimitReached);
    }

    /**
     * Get the limit on the number of rows that will be returned as it would be passed to FDB.
     * @return the limit or {@link ReadTransaction#ROW_LIMIT_UNLIMITED} if there is no limit.
     */
    public int getReturnedRowLimit() {
        return rowLimit;
    }

    /**
     * Set the limit on the number of rows that will be returned.
     * @param rowLimit the limit or {@link ReadTransaction#ROW_LIMIT_UNLIMITED} or {@link Integer#MAX_VALUE} for no limit
     * @return a new <code>ExecuteProperties</code> with the given limit
     */
    @Nonnull
    public ExecuteProperties setReturnedRowLimit(final int rowLimit) {
        final int newLimit = validateAndNormalizeRowLimit(rowLimit);
        if (newLimit == this.rowLimit) {
            return this;
        }
        return copy(skip, newLimit, timeLimit, isolationLevel, state, failOnScanLimitReached);
    }

    public long getTimeLimit() {
        return timeLimit;
    }

    @Nonnull
    public ExecuteState getState() {
        return state;
    }

    /**
     * Build a new <code>ExecuteProperties</code> with the given <code>ExecuteState</code>.
     * @param newState the new state
     * @return a new properties object with the new state
     */
    @Nonnull
    public ExecuteProperties setState(@Nonnull ExecuteState newState) {
        return copy(skip, rowLimit, timeLimit, isolationLevel, newState, failOnScanLimitReached);
    }

    /**
     * Build a new <code>ExecuteProperties</code> with an empty state.
     * @return a new properties object with an empty state
     */
    @Nonnull
    public ExecuteProperties clearState() {
        return copy(skip, rowLimit, timeLimit, isolationLevel, new ExecuteState(), failOnScanLimitReached);
    }

    /**
     * Get whether reaching the scan limit throws an exception.
     * @return {@code true} if the scan limit throws an exception when reached,
     * {@code false} if the scan returns early with {@link com.apple.foundationdb.record.RecordCursor.NoNextReason#SCAN_LIMIT_REACHED}
     */
    public boolean isFailOnScanLimitReached() {
        return failOnScanLimitReached;
    }

    public ExecuteProperties setFailOnScanLimitReached(boolean failOnScanLimitReached) {
        if (failOnScanLimitReached == this.failOnScanLimitReached) {
            return this;
        }
        return copy(skip, rowLimit, timeLimit, isolationLevel, state, failOnScanLimitReached);
    }

    @Nonnull
    public ExecuteProperties clearReturnedRowLimit() {
        if (getReturnedRowLimit() == ReadTransaction.ROW_LIMIT_UNLIMITED) {
            return this;
        }
        return copy(skip, ReadTransaction.ROW_LIMIT_UNLIMITED, timeLimit, isolationLevel, state, failOnScanLimitReached);
    }

    /**
     * Clear the returned row limit and time limit. Does not clear the skip.
     * @return a new <code>ExecuteProperties</code> without the returned row and time limits
     */
    @Nonnull
    public ExecuteProperties clearRowAndTimeLimits() {
        if (getTimeLimit() == UNLIMITED_TIME && getReturnedRowLimit() == ReadTransaction.ROW_LIMIT_UNLIMITED ) {
            return this;
        }
        return copy(skip, ReadTransaction.ROW_LIMIT_UNLIMITED, UNLIMITED_TIME, isolationLevel, state, failOnScanLimitReached);
    }

    /**
     * Clear the skip and returned row limit, but no other limits.
     * @return a new <code>ExecuteProperties</code> without the skip and returned row limit
     */
    @Nonnull
    public ExecuteProperties clearSkipAndLimit() {
        if (skip == 0 && rowLimit == ReadTransaction.ROW_LIMIT_UNLIMITED) {
            return this;
        }
        return copy(0, ReadTransaction.ROW_LIMIT_UNLIMITED, timeLimit, isolationLevel, state, failOnScanLimitReached);
    }

    /**
     * Remove any skip count and adjust the limit to include enough rows that we can skip those and then apply the current limit.
     * @return a new properties without skip and with an adjusted limit
     */
    @Nonnull
    public ExecuteProperties clearSkipAndAdjustLimit() {
        if (skip == 0) {
            return this;
        }
        return copy(0, rowLimit == ReadTransaction.ROW_LIMIT_UNLIMITED ? ReadTransaction.ROW_LIMIT_UNLIMITED : rowLimit + skip,
                timeLimit, isolationLevel, state, failOnScanLimitReached);
    }

    /**
     * Get the limit on the number of rows that will be returned as could be used for counting.
     * @return the limit or {@link Integer#MAX_VALUE} if there is no limit.
     */
    public int getReturnedRowLimitOrMax() {
        return rowLimit == ReadTransaction.ROW_LIMIT_UNLIMITED ? Integer.MAX_VALUE : rowLimit;
    }

    /**
     * Merge these limits with the ones specified in <code>other</code>, using the limit specified by <code>other</code>
     * except where it is unlimited, in which case the limit from this <code>ExecuteProperties</code> is used instead.
     * @param other the <code>ExecuteProperties</code> to the take the limits from
     * @return an <code>ExecuteProperties</code> with limits merged as described above
     */
    @Nonnull
    public ExecuteProperties setLimitsFrom(@Nonnull ExecuteProperties other) {
        ExecuteProperties.Builder builder = toBuilder();
        if (other.rowLimit != ReadTransaction.ROW_LIMIT_UNLIMITED) {
            builder.setReturnedRowLimit(other.rowLimit);
        }
        if (other.timeLimit != UNLIMITED_TIME) {
            builder.setTimeLimit(other.timeLimit);
        }
        if (other.state.getRecordScanLimiter() != null) {
            builder.setState(other.state);
        }
        return builder.build();
    }

    /**
     * Reset the stateful parts of the properties to their "original" values, creating an independent mutable state.
     * @see ExecuteState#reset()
     * @return an {@code ExecuteProperties} with an independent mutable state
     */
    @Nonnull
    public ExecuteProperties resetState() {
        return copy(skip, rowLimit, timeLimit, isolationLevel, state.reset(), failOnScanLimitReached);
    }

    /**
     * Create a new instance with these fields, copying any additional fields from subclasses.
     * @param skip skip count
     * @param rowLimit returned row limit
     * @param timeLimit time limit
     * @param isolationLevel isolation level
     * @param state execute state
     * @param failOnScanLimitReached fail on scan limit reached
     * @return a new properties with the given fields changed and other fields copied from this properties
     */
    @Nonnull
    protected ExecuteProperties copy(int skip, int rowLimit, long timeLimit, @Nonnull IsolationLevel isolationLevel,
                                     @Nonnull ExecuteState state, boolean failOnScanLimitReached) {
        return new ExecuteProperties(skip, rowLimit, isolationLevel, timeLimit, state, failOnScanLimitReached);
    }

    @Nonnull
    public ScanProperties asScanProperties(boolean reverse) {
        return new ScanProperties(this, reverse);
    }

    private static int validateAndNormalizeRowLimit(final int rowLimit) {
        if (rowLimit < 0) {
            throw new RecordCoreException("Invalid returned row limit specified: " + rowLimit);
        }
        return rowLimit == Integer.MAX_VALUE ? ReadTransaction.ROW_LIMIT_UNLIMITED : rowLimit;
    }

    private static long validateAndNormalizeTimeLimit(final long timeLimit) {
        if (timeLimit < 0L) {
            throw new RecordCoreException("Invalid time limit specified: " + timeLimit);
        }
        if (timeLimit == Long.MAX_VALUE) {
            return UNLIMITED_TIME;
        }
        return timeLimit;
    }

    @Nonnull
    public static Builder newBuilder() {
        return new Builder();
    }

    @Nonnull
    public Builder toBuilder() {
        return new Builder(this);
    }

    @Nonnull
    @Override
    public String toString() {
        final List<String> components = new ArrayList<>();
        if (!isolationLevel.equals(IsolationLevel.SERIALIZABLE)) {
            components.add(isolationLevel.toString());
        }
        if (skip != 0) {
            components.add(String.format("skip %d", skip));
        }
        if (rowLimit != ReadTransaction.ROW_LIMIT_UNLIMITED) {
            components.add(String.format("rowLimit %d", rowLimit));
        }
        if (timeLimit != UNLIMITED_TIME) {
            components.add(String.format("timeLimit %d ms,", timeLimit));
        }
        if (failOnScanLimitReached) {
            components.add("fail on scan limit,");
        }
        components.add(state.toString());
        return String.format("ExecuteProperties(%s)", String.join(", ", components));
    }

    /**
     * A builder for {@link ExecuteProperties}.
     * <pre><code>
     * ExecuteProperties.newBuilder().setSkip(s).setReturnedRowLimit(l).build()
     * </code></pre>
     */
    public static class Builder {
        private IsolationLevel isolationLevel = IsolationLevel.SERIALIZABLE;
        private int skip = 0;
        private int rowLimit = ReadTransaction.ROW_LIMIT_UNLIMITED;
        private long timeLimit = UNLIMITED_TIME;
        private int scannedRecordsLimit = Integer.MAX_VALUE;
        private ExecuteState executeState = null;
        private boolean failOnScanLimitReached = false;

        private Builder() {
        }

        private Builder(ExecuteProperties executeProperties) {
            this.isolationLevel = executeProperties.isolationLevel;
            this.skip = executeProperties.skip;
            this.rowLimit = executeProperties.rowLimit;
            this.timeLimit = executeProperties.timeLimit;
            this.executeState = executeProperties.state;
            this.failOnScanLimitReached = executeProperties.failOnScanLimitReached;
        }

        @Nonnull
        public Builder setIsolationLevel(@Nonnull IsolationLevel isolationLevel) {
            this.isolationLevel = isolationLevel;
            return this;
        }

        @Nonnull
        public Builder setSkip(int skip) {
            this.skip = skip;
            return this;
        }

        @Nonnull
        public Builder setReturnedRowLimit(int rowLimit) {
            this.rowLimit = validateAndNormalizeRowLimit(rowLimit);
            return this;
        }

        @Nonnull
        public Builder setTimeLimit(long timeLimit) {
            this.timeLimit = validateAndNormalizeTimeLimit(timeLimit);
            return this;
        }

        /**
         * Set the limit on the number of records that may be scanned.
         * Note that at most one of {@link #scannedRecordsLimit} and {@link #executeState} may be set at the same time,
         * since the {@link ExecuteState} contains a shared {@link RecordScanLimiter}.
         * @param limit the maximum number of records to scan
         * @return an updated builder
         */
        @Nonnull
        public Builder setScannedRecordsLimit(int limit) {
            if (executeState != null) {
                throw new RecordCoreException("Tried to set a record scan limit on a builder with an ExecuteState");
            }
            this.scannedRecordsLimit = validateAndNormalizeRecordScanLimit(limit);
            return this;
        }

        private static int validateAndNormalizeRecordScanLimit(final int scanLimit) {
            if (scanLimit < 0) {
                throw new RecordCoreException("Invalid record scan limit specified: " + scanLimit);
            }
            return scanLimit;
        }

        @Nonnull
        public Builder clearScannedRecordsLimit() {
            return setScannedRecordsLimit(Integer.MAX_VALUE);
        }

        @Nonnull
        public Builder setState(@Nullable ExecuteState state) {
            if (scannedRecordsLimit != Integer.MAX_VALUE) {
                throw new RecordCoreException("Tried to set a state on a builder with a record scan limit");
            }
            this.executeState = state;
            return this;
        }

        @Nonnull
        public Builder clearState() {
            return setState(null);
        }

        /**
         * Set how scan limit reached is handled.
         * This setting has no effect if {@link #setScannedRecordsLimit(int)} is not also set.
         * @param failOnScanLimitReached {@code true} to throw an exception, {@code false} to return early
         * @return an updated builder
         */
        public Builder setFailOnScanLimitReached(boolean failOnScanLimitReached) {
            this.failOnScanLimitReached = failOnScanLimitReached;
            return this;
        }

        @Nonnull
        public ExecuteProperties build() {
            final ExecuteState state;
            if (executeState != null) {
                state = executeState;
            } else if (scannedRecordsLimit == Integer.MAX_VALUE) {
                state = ExecuteState.NO_SCANNED_RECORDS_LIMIT;
            } else {
                state = new ExecuteState(new RecordScanLimiter(scannedRecordsLimit));
            }
            return new ExecuteProperties(skip, rowLimit, isolationLevel, timeLimit, state, failOnScanLimitReached);
        }
    }
}
