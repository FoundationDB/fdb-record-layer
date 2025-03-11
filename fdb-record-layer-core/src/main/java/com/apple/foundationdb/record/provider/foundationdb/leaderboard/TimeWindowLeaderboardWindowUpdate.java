/*
 * TimeWindowLeaderboardWindowUpdate.java
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

package com.apple.foundationdb.record.provider.foundationdb.leaderboard;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.async.RankedSet;
import com.apple.foundationdb.record.provider.foundationdb.IndexOperation;

/**
 * Maintain active set of time windows.
 */
@API(API.Status.EXPERIMENTAL)
public class TimeWindowLeaderboardWindowUpdate extends IndexOperation {
    /**
     * When to completely rebuild an index.
     */
    public enum Rebuild {
        ALWAYS, NEVER, IF_OVERLAPPING_CHANGED
    }

    private final long updateTimestamp;
    private final boolean highScoreFirst;
    private final long deleteBefore;
    private final boolean allTime;
    private final Iterable<TimeWindowSpec> specs;
    private final int nlevels;
    private final Rebuild rebuild;

    /**
     * Create a time window update operation.
     * @param updateTimestamp a timestamp to be recorded if any changes are made
     * @param highScoreFirst if <code>true</code>, numerically higher scores come first in the index
     * @param deleteBefore delete any time windows ending at this time or before
     * @param allTime include an all-time leaderboard
     * @param specs specifications for time windows to create
     * @param nlevels number of skip list levels to maintain
     * @param rebuild completely rebuild the index using the new time windows by scanning all existing records
     *
     */
    public TimeWindowLeaderboardWindowUpdate(long updateTimestamp,
                                             boolean highScoreFirst,
                                             long deleteBefore,
                                             boolean allTime, Iterable<TimeWindowSpec> specs,
                                             int nlevels, Rebuild rebuild) {
        this.updateTimestamp = updateTimestamp;
        this.highScoreFirst = highScoreFirst;
        this.deleteBefore = deleteBefore;
        this.allTime = allTime;
        this.specs = specs;
        this.nlevels = nlevels;
        this.rebuild = rebuild;
    }

    /**
     * Create a time window update operation.
     * @param updateTimestamp a timestamp to be recorded if any changes are made
     * @param highScoreFirst if <code>true</code>, numerically higher scores come first in the index
     * @param deleteBefore delete any time windows ending at this time or before;
     * this will not delete the all-time leaderboard, if there is one
     * @param allTime include an all-time leaderboard
     * @param specs specifications for time windows to create
     * @param rebuild completely rebuild the index using the new time windows by scanning all existing records
     *
     */
    public TimeWindowLeaderboardWindowUpdate(long updateTimestamp,
                                             boolean highScoreFirst,
                                             long deleteBefore,
                                             boolean allTime, Iterable<TimeWindowSpec> specs,
                                             Rebuild rebuild) {
        this(updateTimestamp, highScoreFirst, deleteBefore, allTime, specs, RankedSet.DEFAULT_LEVELS, rebuild);
    }

    public long getUpdateTimestamp() {
        return updateTimestamp;
    }

    public boolean isHighScoreFirst() {
        return highScoreFirst;
    }

    public long getDeleteBefore() {
        return deleteBefore;
    }

    public boolean isAllTime() {
        return allTime;
    }

    public Iterable<TimeWindowSpec> getSpecs() {
        return specs;
    }

    public int getNlevels() {
        return nlevels;
    }

    public Rebuild getRebuild() {
        return rebuild;
    }

    /**
     * A specification of a set of sliding time windows.
     * The time windows are all of type <code>type</code>.
     * There are <code>count</code> of them.
     * The earliest one starts at <code>baseTimestamp</code>.
     * The following ones are spaced <code>startIncrement</code> apart.
     * They are <code>duration</code> wide.
     *
     * So, for example, to have daily time windows accurate to six hours, and assuming that time units are seconds,
     * <code>new TimeWindowSpec(DAILY, midnight, 6 * 60 * 60, 24 * 60 * 60, 4)</code>
     */
    public static class TimeWindowSpec {
        private final int type;
        private final long baseTimestamp;
        private final long startIncrement;
        private final long duration;
        private final int count;

        public TimeWindowSpec(int type, long baseTimestamp, long startIncrement, long duration, int count) {
            this.type = type;
            this.baseTimestamp = baseTimestamp;
            this.startIncrement = startIncrement;
            this.duration = duration;
            this.count = count;
        }

        public int getType() {
            return type;
        }

        public long getBaseTimestamp() {
            return baseTimestamp;
        }

        public long getStartIncrement() {
            return startIncrement;
        }

        public long getDuration() {
            return duration;
        }

        public long getCount() {
            return count;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            TimeWindowSpec that = (TimeWindowSpec) o;

            if (this.type != that.type) {
                return false;
            }
            if (this.baseTimestamp != that.baseTimestamp) {
                return false;
            }
            if (this.startIncrement != that.startIncrement) {
                return false;
            }
            if (this.duration != that.duration) {
                return false;
            }
            return count == that.count;
        }

        @Override
        public int hashCode() {
            int result = type;
            result = 31 * result + (int) (baseTimestamp ^ (baseTimestamp >>> 32));
            result = 31 * result + (int) (startIncrement ^ (startIncrement >>> 32));
            result = 31 * result + (int) (duration ^ (duration >>> 32));
            result = 31 * result + count;
            return result;
        }
    }
}
