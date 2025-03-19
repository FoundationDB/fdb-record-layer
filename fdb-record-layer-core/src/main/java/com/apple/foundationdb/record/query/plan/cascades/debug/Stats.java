/*
 * Stats.java
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

package com.apple.foundationdb.record.query.plan.cascades.debug;

import com.google.common.collect.ImmutableMap;

import javax.annotation.Nonnull;
import java.util.Map;

public class Stats {
    @Nonnull
    protected final Map<Debugger.Location, Long> locationCountMap;

    protected long totalTimeInNs;
    protected long ownTimeInNs;

    protected Stats(@Nonnull final Map<Debugger.Location, Long> locationCountMap,
                    final long totalTimeInNs,
                    final long ownTimeInNs) {
        this.locationCountMap = locationCountMap;
        this.totalTimeInNs = totalTimeInNs;
        this.ownTimeInNs = ownTimeInNs;
    }

    @Nonnull
    public Map<Debugger.Location, Long> getLocationCountMap() {
        return locationCountMap;
    }

    public long getCount(@Nonnull final Debugger.Location location) {
        return locationCountMap.getOrDefault(location, 0L);
    }

    public long getTotalTimeInNs() {
        return totalTimeInNs;
    }

    public long getOwnTimeInNs() {
        return ownTimeInNs;
    }

    @Nonnull
    public Stats toImmutable() {
        return new Stats(ImmutableMap.copyOf(locationCountMap), totalTimeInNs, ownTimeInNs);
    }
}
