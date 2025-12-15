/*
 * PlannerEventStats.java
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

package com.apple.foundationdb.record.query.plan.cascades.events;

import com.google.common.collect.ImmutableMap;

import javax.annotation.Nonnull;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

public class PlannerEventStats {
    @Nonnull
    protected final Map<PlannerEvent.Location, Long> locationCountMap;

    protected long totalTimeInNs;
    protected long ownTimeInNs;

    protected PlannerEventStats(@Nonnull final Map<PlannerEvent.Location, Long> locationCountMap,
                                final long totalTimeInNs,
                                final long ownTimeInNs) {
        this.locationCountMap = locationCountMap;
        this.totalTimeInNs = totalTimeInNs;
        this.ownTimeInNs = ownTimeInNs;
    }

    @Nonnull
    public Map<PlannerEvent.Location, Long> getLocationCountMap() {
        return locationCountMap;
    }

    public long getCount(@Nonnull final PlannerEvent.Location location) {
        return locationCountMap.getOrDefault(location, 0L);
    }

    public long getTotalTimeInNs() {
        return totalTimeInNs;
    }

    public long getOwnTimeInNs() {
        return ownTimeInNs;
    }

    @Nonnull
    public PlannerEventStats toImmutable() {
        return new PlannerEventStats(ImmutableMap.copyOf(locationCountMap), totalTimeInNs, ownTimeInNs);
    }

    public static PlannerEventStats merge(final PlannerEventStats first, final PlannerEventStats second) {
        return new PlannerEventStats(
                Stream.of(first.getLocationCountMap(), second.getLocationCountMap())
                        .map(Map::entrySet)
                        .flatMap(Set::stream)
                        .collect(ImmutableMap.toImmutableMap(Map.Entry::getKey, Map.Entry::getValue, Long::sum)),
                first.getTotalTimeInNs() + second.getTotalTimeInNs(),
                first.getOwnTimeInNs() + second.getOwnTimeInNs());
    }
}
