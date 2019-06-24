/*
 * TimeWindowLeaderboardScoreTrim.java
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
import com.apple.foundationdb.record.provider.foundationdb.IndexOperation;
import com.apple.foundationdb.tuple.Tuple;

import java.util.Collection;

/**
 * Retain only scores that would be indexed by active time windows.
 *
 * Scores are specified as a collection of {@link Tuple}. A sub-collection is returned in {@link TimeWindowLeaderboardScoreTrimResult}.
 *
 * Score tuples can include their group key as well. If they do not, then all groups must have the default setting for whether high scores come first, which is needed
 * to keep only the best score for a given time window.
 */
@API(API.Status.EXPERIMENTAL)
public class TimeWindowLeaderboardScoreTrim extends IndexOperation {
    private final Collection<Tuple> scores;
    private final boolean includesGroup;

    public TimeWindowLeaderboardScoreTrim(Collection<Tuple> scores, boolean includesGroup) {
        this.scores = scores;
        this.includesGroup = includesGroup;
    }

    public Collection<Tuple> getScores() {
        return scores;
    }

    public boolean isIncludesGroup() {
        return includesGroup;
    }
}
