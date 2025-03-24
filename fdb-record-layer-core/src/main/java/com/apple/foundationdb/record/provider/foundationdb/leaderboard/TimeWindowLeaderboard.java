/*
 * TimeWindowLeaderboard.java
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
import com.apple.foundationdb.record.TimeWindowLeaderboardProto;
import com.apple.foundationdb.tuple.Tuple;
import com.google.protobuf.ZeroCopyByteString;

import javax.annotation.Nonnull;

/**
 * A single leaderboard, representing ranks within a time window.
 */
@API(API.Status.EXPERIMENTAL)
public class TimeWindowLeaderboard implements Comparable<TimeWindowLeaderboard> {
    public static final int ALL_TIME_LEADERBOARD_TYPE = 0;

    @Nonnull
    private final TimeWindowLeaderboardDirectory directory;
    private final int type;
    private final long startTimestamp;
    private final long endTimestamp;
    @Nonnull
    private final Tuple subspaceKey;
    private final int nlevels;

    public TimeWindowLeaderboard(@Nonnull TimeWindowLeaderboardDirectory directory,
                                 int type, long startTimestamp, long endTimestamp,
                                 @Nonnull Tuple subspaceKey, int nlevels) {
        this.directory = directory;
        this.type = type;
        this.startTimestamp = startTimestamp;
        this.endTimestamp = endTimestamp;
        this.subspaceKey = subspaceKey;
        this.nlevels = nlevels;
    }

    protected TimeWindowLeaderboard(@Nonnull TimeWindowLeaderboardDirectory directory,
                                    @Nonnull TimeWindowLeaderboardProto.TimeWindowLeaderboard proto) {
        this(directory,
                proto.getType(), proto.getStartTimestamp(), proto.getEndTimestamp(),
                Tuple.fromBytes(proto.getSubspaceKey().toByteArray()),
                proto.hasNlevels() ? proto.getNlevels() : RankedSet.DEFAULT_LEVELS);
    }

    public TimeWindowLeaderboardDirectory getDirectory() {
        return directory;
    }

    public int getType() {
        return type;
    }

    public long getStartTimestamp() {
        return startTimestamp;
    }

    public long getEndTimestamp() {
        return endTimestamp;
    }

    public boolean containsTimestamp(long timestamp) {
        return startTimestamp <= timestamp && timestamp < endTimestamp;
    }

    @Nonnull
    public Tuple getSubspaceKey() {
        return subspaceKey;
    }

    public int getNLevels() {
        return nlevels;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        TimeWindowLeaderboard that = (TimeWindowLeaderboard) o;

        if (type != that.type) {
            return false;
        }
        if (startTimestamp != that.startTimestamp) {
            return false;
        }
        if (endTimestamp != that.endTimestamp) {
            return false;
        }
        if (!subspaceKey.equals(that.subspaceKey)) {
            return false;
        }
        return nlevels == that.nlevels;
    }

    @Override
    public int hashCode() {
        int result = type;
        result = 31 * result + (int) (startTimestamp ^ (startTimestamp >>> 32));
        result = 31 * result + (int) (endTimestamp ^ (endTimestamp >>> 32));
        result = 31 * result + subspaceKey.hashCode();
        result = 31 * result + nlevels;
        return result;
    }

    @Override
    public int compareTo(@Nonnull TimeWindowLeaderboard that) {
        if (this.type != that.type) {
            return this.type < that.type ? -1 : +1;
        }
        if (this.startTimestamp != that.startTimestamp) {
            return this.startTimestamp < that.startTimestamp ? -1 : +1;
        }
        if (this.endTimestamp != that.endTimestamp) {
            return this.endTimestamp < that.endTimestamp ? +1 : -1; // Wider comes first.
        }
        if (!this.subspaceKey.equals(that.subspaceKey)) {
            return this.subspaceKey.compareTo(that.subspaceKey);
        }
        return this.nlevels < that.nlevels ? -1 : +1;
    }

    @Nonnull
    protected TimeWindowLeaderboardProto.TimeWindowLeaderboard.Builder toProto() {
        return TimeWindowLeaderboardProto.TimeWindowLeaderboard.newBuilder()
                .setType(type)
                .setStartTimestamp(startTimestamp)
                .setEndTimestamp(endTimestamp)
                .setSubspaceKey(ZeroCopyByteString.wrap(subspaceKey.pack()))
                .setNlevels(nlevels);
    }

    @Override
    public String toString() {
        return "TimeWindowLeaderboard{" +
                "type=" + type +
                ", startTimestamp=" + startTimestamp +
                ", endTimestamp=" + endTimestamp +
                ", subspaceKey=" + subspaceKey +
                ", nlevels=" + nlevels +
                '}';
    }
}
