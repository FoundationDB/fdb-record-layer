/*
 * StoreTimerSnapshot.java
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

package com.apple.foundationdb.record.provider.common;

import com.apple.foundationdb.annotation.API;
import com.google.common.collect.ImmutableMap;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Map;
import java.util.UUID;

/**
 * An immutable snapshot of a {@link StoreTimer}.
 * <p>
 * Subtracting a snapshot of a timer from the original timer after subsequent operations have
 * been performed and timed can provide useful metrics as to the cost of those subsequent operations.
 * See {@link StoreTimer#getDifference(StoreTimer, StoreTimerSnapshot)}.
 */
@API(API.Status.UNSTABLE)
public final class StoreTimerSnapshot {
    @Nonnull
    private final ImmutableMap<StoreTimer.Event, CounterSnapshot> counters;
    @Nonnull
    private final ImmutableMap<StoreTimer.Event, CounterSnapshot> timeoutCounters;
    @Nonnull
    private final UUID fromUUID;
    private final long createTime;

    private StoreTimerSnapshot(@Nonnull StoreTimer timer) {
        ImmutableMap.Builder<StoreTimer.Event, CounterSnapshot> counters = new ImmutableMap.Builder<>();
        ImmutableMap.Builder<StoreTimer.Event, CounterSnapshot> timeoutCounters = new ImmutableMap.Builder<>();
        timer.counters.forEach((key, value) -> counters.put(key, CounterSnapshot.from(value)));
        timer.timeoutCounters.forEach((key, value) -> timeoutCounters.put(key, CounterSnapshot.from(value)));
        this.counters = counters.build();
        this.timeoutCounters = timeoutCounters.build();
        fromUUID = timer.geUUID();
        createTime = System.nanoTime();
    }

    /**
     * Creates an immutable snapshot of a {@link StoreTimer}.
     *
     * @param timer to create the snapshot from
     *
     * @return immutable snapshot of the provided timer
     */
    @Nonnull
    public static StoreTimerSnapshot from(@Nonnull StoreTimer timer) {
        return new StoreTimerSnapshot(timer);
    }

    /**
     * Returns the counters taken at the time of the snapshot.
     *
     * @return the counters taken at the time of the snapshot.
     */
    public Map<StoreTimer.Event, CounterSnapshot> getCounters() {
        return counters;
    }

    /**
     * Returns the timeout counters taken at the time of the snapshot.
     *
     * @return the timeout counters taken at the time of the snapshot.
     */
    public Map<StoreTimer.Event, CounterSnapshot> getTimeoutCounters() {
        return timeoutCounters;
    }


    /**
     * Get the counter for a given event.
     *
     * @param event the event of interest
     *
     * @return immutable counter if it exists or null otherwise
     */
    @Nullable
    public CounterSnapshot getCounterSnapshot(@Nonnull StoreTimer.Event event) {
        return counters.get(event);
    }

    /**
     * Determine if there is a counter recorded for a given event.
     *
     * @param event the event of interest
     *
     * @return true if a counter exists for the event of interest
     */
    public boolean containsCounter(@Nonnull StoreTimer.Event event) {
        return counters.containsKey(event);
    }

    /**
     * Get the timeout counter for a given event.
     *
     * @param event the event of interest
     *
     * @return immutable timeout counter if it exists or null otherwise
     */
    @Nullable
    public CounterSnapshot getTimeoutCounterSnapshot(@Nonnull StoreTimer.Event event) {
        return timeoutCounters.get(event);
    }

    /**
     * Determine if there is a timeout counter recorded for a given event.
     *
     * @param event the event of interest
     *
     * @return true if a timeout counter exists for the event of interest
     */
    public boolean containsTimeoutCounter(@Nonnull StoreTimer.Event event) {
        return timeoutCounters.containsKey(event);
    }

    /**
     * Determine if this snapshot is derived from the provided timer.
     *
     * @param timer the provided timer
     *
     * @return true if it is valid to diff this snapshot from the provided timer
     */
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    public boolean derivedFrom(StoreTimer timer) {
        if (timer.geUUID() != this.fromUUID) {
            return false;
        }
        return true;
    }

    /**
     * Determine if this snapshot was taken after the provided timer was last reset.
     *
     * @param timer the provided timer
     *
     * @return true if this snapshot was taken after the provided timer was last reset
     */
    public boolean takenAfterReset(StoreTimer timer) {
        if (timer.lastReset > this.createTime) {
            return false;
        }
        return true;
    }

    /**
     * Set the reset time on the provided timer to the create time of this snapshot.
     *
     * @param timer the provided timer to srt
     */
    public void setResetTime(StoreTimer timer) {
        timer.lastReset = this.createTime;
    }

    /**
     * An immutable snapshot of a {@link StoreTimer.Counter}.
     * <p>
     * Contains the number of occurrences and cummulative time spent on an associated event.
     */
    public static class CounterSnapshot {
        private final long cumulativeValue;
        private final int count;

        private CounterSnapshot(@Nonnull StoreTimer.Counter c) {
            cumulativeValue = c.getCumulativeValue();
            count = c.getCount();
        }

        /**
         * Creates an immutable snapshot of a {@link StoreTimer.Counter}.
         *
         * @param counter to create the snapshot from
         *
         * @return immutable snapshot of the provided counter
         */
        @Nonnull
        public static CounterSnapshot from(@Nonnull StoreTimer.Counter counter) {
            return new CounterSnapshot(counter);
        }

        /**
         * Get the number of occurrences of the associated event.
         *
         * @return the number of occurrences of the associated event
         */
        public int getCount() {
            return count;
        }

        /**
         * Get the cumulative time spent on the associated event.
         *
         * @return the cumulative time spent on the associated event
         */
        public long getTimeNanos() {
            return getCumulativeValue();
        }

        /**
         * Get the cumulative size of the associated event.
         *
         * @return the cumulative size of the associated event
         */
        public long getCumulativeValue() {
            return cumulativeValue;
        }
    }
}
