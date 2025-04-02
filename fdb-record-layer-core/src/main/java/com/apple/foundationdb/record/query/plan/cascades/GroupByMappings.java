/*
 * GroupByMappings.java
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

import javax.annotation.Nonnull;

public class GroupByMappings {
    @Nonnull
    private final BiMap<Value, Value> matchedGroupingsMap;
    @Nonnull
    private final BiMap<Value, Value> matchedAggregatesMap;

    @Nonnull
    private final BiMap<CorrelationIdentifier, Value> unmatchedAggregatesMap;

    private GroupByMappings(@Nonnull final BiMap<Value, Value> matchedGroupingsMap,
                            @Nonnull final BiMap<Value, Value> matchedAggregatesMap,
                            @Nonnull final BiMap<CorrelationIdentifier, Value> unmatchedAggregatesMap) {
        this.matchedGroupingsMap = matchedGroupingsMap;
        this.matchedAggregatesMap = matchedAggregatesMap;
        this.unmatchedAggregatesMap = unmatchedAggregatesMap;
    }

    @Nonnull
    public BiMap<Value, Value> getMatchedGroupingsMap() {
        return matchedGroupingsMap;
    }

    @Nonnull
    public BiMap<Value, Value> getMatchedAggregatesMap() {
        return matchedAggregatesMap;
    }

    @Nonnull
    public BiMap<CorrelationIdentifier, Value> getUnmatchedAggregatesMap() {
        return unmatchedAggregatesMap;
    }

    public static GroupByMappings empty() {
        return of(ImmutableBiMap.of(), ImmutableBiMap.of(), ImmutableBiMap.of());
    }

    @Nonnull
    public static GroupByMappings of(@Nonnull final BiMap<Value, Value> matchedGroupingsMap,
                                     @Nonnull final BiMap<Value, Value> matchedAggregateMap,
                                     @Nonnull final BiMap<CorrelationIdentifier, Value> unmatchedAggregatesMap) {
        return new GroupByMappings(ImmutableBiMap.copyOf(matchedGroupingsMap),
                ImmutableBiMap.copyOf(matchedAggregateMap),
                ImmutableBiMap.copyOf(unmatchedAggregatesMap));
    }
}
