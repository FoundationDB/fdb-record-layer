/*
 * SizeStatisticsGroupedResults.java
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

package com.apple.foundationdb.record.provider.foundationdb.cursors;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.tuple.Tuple;

import javax.annotation.Nonnull;

/**
 * The result of a grouped size calculation.
 * The result is aggregated for a subspace whose key suffix is specified below, within the containing subspace.
 */
@API(API.Status.EXPERIMENTAL)
public class SizeStatisticsGroupedResults {
    private final Tuple aggregationKey;
    private final SizeStatisticsResults stats;

    public SizeStatisticsGroupedResults(@Nonnull final Tuple aggregationKey, @Nonnull SizeStatisticsResults stats) {
        this.aggregationKey = aggregationKey;
        this.stats = stats;
    }

    /**
     * The Tuple that represents the key whose stats are aggregated.
     * @return the Tuple representing the subspace key aggregated
     */
    @Nonnull
    public Tuple getAggregationKey() {
        return aggregationKey;
    }

    /**
     * The aggregated stats.
     * @return the statistics collected for the subspace
     */
    @Nonnull
    public SizeStatisticsResults getStats() {
        return stats;
    }
}
