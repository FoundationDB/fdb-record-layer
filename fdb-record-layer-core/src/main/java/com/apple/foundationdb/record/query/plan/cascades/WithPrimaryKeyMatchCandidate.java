/*
 * WithPrimaryKeyMatchCandidate.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2021 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.cascades;

import com.apple.foundationdb.record.query.plan.cascades.values.Value;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Optional;

/**
 * Interface to represent a candidate that uses a primary key to identify a record.
 */
public interface WithPrimaryKeyMatchCandidate extends MatchCandidate {
    @Nonnull
    Optional<List<Value>> getPrimaryKeyValuesMaybe();

    @Nonnull
    static Optional<List<Value>> commonRecordKeyValuesMaybe(@Nonnull Iterable<? extends MatchCandidate> matchCandidates) {
        List<Value> common = null;
        var first = true;
        for (final var matchCandidate : matchCandidates) {
            final List<Value> key;
            if (matchCandidate instanceof WithPrimaryKeyMatchCandidate) {
                final var withPrimaryKeyMatchCandidate = (WithPrimaryKeyMatchCandidate)matchCandidate;
                final var keyMaybe = withPrimaryKeyMatchCandidate.getPrimaryKeyValuesMaybe();
                if (keyMaybe.isEmpty()) {
                    return Optional.empty();
                }
                key = keyMaybe.get();
            } else if (matchCandidate instanceof AggregateIndexMatchCandidate) {
                final var aggregateIndexMatchCandidate = (AggregateIndexMatchCandidate)matchCandidate;
                key = aggregateIndexMatchCandidate.getGroupByValues();
            } else {
                return Optional.empty();
            }
            if (first) {
                common = key;
                first = false;
            } else if (!common.equals(key)) {
                return Optional.empty();
            }
        }
        return Optional.ofNullable(common); // common can only be null if we didn't have any match candidates to start with
    }
}
