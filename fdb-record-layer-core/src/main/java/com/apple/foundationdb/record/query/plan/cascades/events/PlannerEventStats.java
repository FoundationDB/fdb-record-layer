/*
 * PlannerEventStats.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2026 Apple Inc. and the FoundationDB project authors
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

/**
 * <p>
 * Aggregated statistics for a single {@link PlannerEvent} class across the planning of a single query.
 * </p>
 * <p>
 * An instance of this class should be used to track a single {@link PlannerEvent} class.
 * </p>
 */
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

    /**
     * Get a {@link Map} of {@link PlannerEvent.Location} to the number of times instances of the {@link PlannerEvent}
     * class tracked by this instance were emitted with the provided {@link PlannerEvent.Location}.
     * @return a {@link Map} of {@link PlannerEvent.Location} to a {@link Long}.
     */
    @Nonnull
    public Map<PlannerEvent.Location, Long> getLocationCountMap() {
        return locationCountMap;
    }

    /**
     * Get the number of times instances of the {@link PlannerEvent} class tracked by this instance
     * were emitted with the provided {@link PlannerEvent.Location}.
     * @return {@code long} representing the number of events emitted with {@code location}.
     */
    public long getCount(@Nonnull final PlannerEvent.Location location) {
        return locationCountMap.getOrDefault(location, 0L);
    }

    /**
     * <p>
     * Returns the total time spent executing instances of the {@link PlannerEvent} class this instance of
     * {@link PlannerEventStats} tracks, measured from {@link PlannerEvent.Location#BEGIN} to
     * {@link PlannerEvent.Location#END}. This includes time spent in any nested planner events that occurred within the
     * execution of this event type.
     * </p>
     * <p>
     * For example, if Event A runs for 10ns and Event B (nested within A) runs for 3ns,
     * then Event A's total time is 10ns and Event B's total time is 3ns.
     * </p>
     *
     * @return the total execution time in nanoseconds for this event type
     */
    public long getTotalTimeInNs() {
        return totalTimeInNs;
    }

    /**
     * <p>
     * Returns the total time spent executing instances of the {@link PlannerEvent} class this instance of
     * {@link PlannerEventStats} tracks, which excludes time spent in any nested planner events that occurred within the
     * execution of this event type. This represents the time directly attributable to this event type's processing.
     * </p>
     * <p>
     * For example, if Event A runs for 10ms total and Event B (nested within A) runs for 3ms,
     * then Event A's own time is 7ms (10ms - 3ms) and Event B's own time is 3ms.
     * </p>
     * @return the own exclusive execution time in nanoseconds for this event type
     */
    public long getOwnTimeInNs() {
        return ownTimeInNs;
    }

    /**
     * Return an immutable copy of this instance.
     * @return an immutable instance of {@link PlannerEventStats}.
     */
    @Nonnull
    public PlannerEventStats toImmutable() {
        return new PlannerEventStats(ImmutableMap.copyOf(locationCountMap), totalTimeInNs, ownTimeInNs);
    }

    /**
     * Merge two instances of {@link PlannerEventStats}.
     * @param first the first instance of {@link PlannerEventStats}
     * @param second the second instance of {@link PlannerEventStats}
     * @return a new instance of {@link PlannerEventStats} containing the combined statistics for the {@link PlannerEvent} class
     *         that {@code first} and {@code second} tracked.
     */
    @Nonnull
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
