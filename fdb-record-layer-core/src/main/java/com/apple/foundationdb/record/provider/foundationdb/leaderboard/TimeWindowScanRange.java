/*
 * TimeWindowScanRange.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2022 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.provider.foundationdb.IndexScanRange;

/**
 * Extend {@link IndexScanRange} to have time window.
 */
@API(API.Status.EXPERIMENTAL)
public class TimeWindowScanRange extends IndexScanRange {
    private final int leaderboardType;
    private final long leaderboardTimestamp;

    public TimeWindowScanRange(int leaderboardType, long leaderboardTimestamp, TupleRange range) {
        super(IndexScanType.BY_TIME_WINDOW, range);
        this.leaderboardType = leaderboardType;
        this.leaderboardTimestamp = leaderboardTimestamp;
    }

    public int getLeaderboardType() {
        return leaderboardType;
    }

    public long getLeaderboardTimestamp() {
        return leaderboardTimestamp;
    }

    @Override
    public String toString() {
        return leaderboardType + "," + leaderboardTimestamp;
    }
}
