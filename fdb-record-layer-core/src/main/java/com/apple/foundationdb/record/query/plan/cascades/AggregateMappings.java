/*
 * AggregateMappings.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2025 Apple Inc. and the FoundationDB project authors
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
import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableMap;

import javax.annotation.Nonnull;
import java.util.Map;

public class AggregateMappings {
    @Nonnull
    private final Map<Value, Value> matchedAggregateMap;

    @Nonnull
    private final BiMap<CorrelationIdentifier, Value> unmatchedAggregateMap;

    private AggregateMappings(@Nonnull final Map<Value, Value> matchedAggregateMap,
                              @Nonnull final BiMap<CorrelationIdentifier, Value> unmatchedAggregateMap) {
        this.matchedAggregateMap = matchedAggregateMap;
        this.unmatchedAggregateMap = unmatchedAggregateMap;
    }

    @Nonnull
    public Map<Value, Value> getMatchedAggregateMap() {
        return matchedAggregateMap;
    }

    @Nonnull
    public BiMap<CorrelationIdentifier, Value> getUnmatchedAggregateMap() {
        return unmatchedAggregateMap;
    }

    public static AggregateMappings empty() {
        return of(ImmutableBiMap.of(), ImmutableBiMap.of());
    }

    @Nonnull
    public static AggregateMappings of(@Nonnull final Map<Value, Value> matchedAggregateMap,
                                       @Nonnull final BiMap<CorrelationIdentifier, Value> unmatchedAggregateMap) {
        return new AggregateMappings(ImmutableMap.copyOf(matchedAggregateMap), ImmutableBiMap.copyOf(unmatchedAggregateMap));
    }
}
