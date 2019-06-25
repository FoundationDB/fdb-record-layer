/*
 * TimeWindowLeaderboardSubDirectory.java
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
import com.apple.foundationdb.record.TimeWindowLeaderboardProto;
import com.apple.foundationdb.tuple.Tuple;

import javax.annotation.Nonnull;

/**
 * Persisted per-group information for leaderboard ranked sets.
 */
@API(API.Status.EXPERIMENTAL)
public class TimeWindowLeaderboardSubDirectory {
    @Nonnull
    private final Tuple group;
    private final boolean highScoreFirst;

    public TimeWindowLeaderboardSubDirectory(@Nonnull Tuple group, boolean highScoreFirst) {
        this.group = group;
        this.highScoreFirst = highScoreFirst;
    }

    protected TimeWindowLeaderboardSubDirectory(@Nonnull Tuple group, @Nonnull TimeWindowLeaderboardProto.TimeWindowLeaderboardSubDirectory proto) {
        this.group = group;
        highScoreFirst = proto.getHighScoreFirst();
    }

    @Nonnull
    public Tuple getGroup() {
        return group;
    }

    public boolean isHighScoreFirst() {
        return highScoreFirst;
    }

    @Nonnull
    public TimeWindowLeaderboardProto.TimeWindowLeaderboardSubDirectory toProto() {
        TimeWindowLeaderboardProto.TimeWindowLeaderboardSubDirectory.Builder builder = TimeWindowLeaderboardProto.TimeWindowLeaderboardSubDirectory.newBuilder();
        builder.setHighScoreFirst(highScoreFirst);
        return builder.build();
    }

}
