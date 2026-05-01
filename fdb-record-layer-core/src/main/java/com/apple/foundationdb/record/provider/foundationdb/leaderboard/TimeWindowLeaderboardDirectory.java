/*
 * TimeWindowLeaderboardDirectory.java
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
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The persisted set of active leaderboard ranked sets.
 */
@API(API.Status.EXPERIMENTAL)
public final class TimeWindowLeaderboardDirectory {
    private final boolean highScoreFirst;
    private long updateTimestamp;
    private int nextKey;
    private Map<Integer, Collection<TimeWindowLeaderboard>> leaderboards = new TreeMap<>();
    private Map<Tuple, TimeWindowLeaderboardSubDirectory> subdirectories = new ConcurrentHashMap<>();

    public TimeWindowLeaderboardDirectory(boolean highScoreFirst) {
        this.highScoreFirst = highScoreFirst;
    }

    TimeWindowLeaderboardDirectory(TimeWindowLeaderboardProto.TimeWindowLeaderboardDirectory proto) {
        highScoreFirst = proto.getHighScoreFirst();
        updateTimestamp = proto.getUpdateTimestamp();
        nextKey = proto.getNextKey();
        for (TimeWindowLeaderboardProto.TimeWindowLeaderboard leaderboardProto : proto.getLeaderboardsList()) {
            final TimeWindowLeaderboard leaderboard = new TimeWindowLeaderboard(this, leaderboardProto);
            addLeaderboard(leaderboard);
        }
    }

    public long getUpdateTimestamp() {
        return updateTimestamp;
    }

    public void setUpdateTimestamp(long updateTimestamp) {
        this.updateTimestamp = updateTimestamp;
    }

    public boolean isHighScoreFirst() {
        return highScoreFirst;
    }

    public Map<Integer, Collection<TimeWindowLeaderboard>> getLeaderboards() {
        return leaderboards;
    }

    @Nullable
    public TimeWindowLeaderboard oldestLeaderboardMatching(int type, long timestamp) {
        Iterable<TimeWindowLeaderboard> entry = leaderboards.get(type);
        if (entry == null) {
            return null;
        }
        for (TimeWindowLeaderboard leaderboard : entry) {
            if (leaderboard.containsTimestamp(timestamp)) {
                return leaderboard;
            }
        }
        return null;
    }

    @Nullable
    public TimeWindowLeaderboard findLeaderboard(int type, long startTimestamp, long endTimestamp) {
        Iterable<TimeWindowLeaderboard> entry = leaderboards.get(type);
        if (entry == null) {
            return null;
        }
        for (TimeWindowLeaderboard leaderboard : entry) {
            if (leaderboard.getStartTimestamp() == startTimestamp && leaderboard.getEndTimestamp() == endTimestamp) {
                return leaderboard;
            }
        }
        return null;
    }

    public void addLeaderboard(int type, long startTimestamp, long endTimestamp, int nlevels) {
        addLeaderboard(new TimeWindowLeaderboard(this, type, startTimestamp, endTimestamp, Tuple.from(nextKey++), nlevels));
    }

    private void addLeaderboard(TimeWindowLeaderboard leaderboard) {
        leaderboards.compute(leaderboard.getType(), (iignore, collection) -> {
            if (collection == null) {
                collection = new TreeSet<>();
            }
            collection.add(leaderboard);
            return collection;
        });
    }

    @Nullable
    public TimeWindowLeaderboardSubDirectory getSubDirectory(@Nonnull Tuple subdir) {
        return subdirectories.get(subdir);
    }

    public void addSubDirectory(@Nonnull TimeWindowLeaderboardSubDirectory subdir) {
        subdirectories.put(subdir.getGroup(), subdir);
    }

    @Nonnull
    public TimeWindowLeaderboardProto.TimeWindowLeaderboardDirectory toProto() {
        TimeWindowLeaderboardProto.TimeWindowLeaderboardDirectory.Builder builder = TimeWindowLeaderboardProto.TimeWindowLeaderboardDirectory.newBuilder();
        builder.setUpdateTimestamp(updateTimestamp);
        builder.setHighScoreFirst(highScoreFirst);
        builder.setNextKey(nextKey);
        for (Collection<TimeWindowLeaderboard> entry : leaderboards.values()) {
            for (TimeWindowLeaderboard leaderboard : entry) {
                builder.addLeaderboards(leaderboard.toProto());
            }
        }
        return builder.build();
    }
}
