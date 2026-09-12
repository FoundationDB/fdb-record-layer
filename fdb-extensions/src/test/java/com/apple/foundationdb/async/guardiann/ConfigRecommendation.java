/*
 * ConfigRecommendation.java
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

package com.apple.foundationdb.async.guardiann;

import com.apple.foundationdb.linear.Metric;

import javax.annotation.Nonnull;

/**
 * Derives a whole Guardiann {@link Config} from the two decisions a caller actually has to make: the distance
 * {@link Metric} and how large a cluster may grow before it splits.
 * <p>
 * Most of {@link Config}'s knobs are not independent — they only behave sensibly in certain ratios to
 * {@code primaryClusterMax}, and getting one of those ratios wrong is not obvious from reading the value. This class
 * states each ratio once, with the reason, so a caller changing the cluster size gets a coherent set rather than a
 * mix of scaled and unscaled numbers. {@code ConfigRecommendationTest} pins the relationships that must hold.
 * <p>
 * Returns a builder rather than a {@code Config} for two reasons: the number of dimensions is a property of the data
 * rather than of the tuning, and a caller with dataset-specific knowledge (SIFT's density, say, which wants far more
 * replication than the generic ratio gives) should be able to override individual knobs on top.
 *
 * <h2>Where the ratios come from</h2>
 * <ul>
 *   <li>{@code primaryClusterMin = max / 10} — measured. A drain of 50k SIFT vectors at {@code max = 512} behaves
 *       well at 50 and badly at a much lower floor: clusters whose lifetime peak is small get a merge threshold
 *       pinned to the floor, so setting it far below {@code max / 10} leaves a population of small clusters that
 *       never consolidate and accumulate references to deleted vectors.</li>
 *   <li>{@code primaryClusterHardMax = 2 * max} — the existing convention, and the back-pressure valve: an insert
 *       that would exceed it is refused rather than queued behind a split backlog.</li>
 *   <li>{@code minChildFraction = min / max} — a split fires at no fewer than {@code max} vectors, so a child of
 *       this fraction holds at least {@code primaryClusterMin} and is therefore not born already wanting to
 *       merge.</li>
 *   <li>{@code replicatedClusterTarget = max / 10} and {@code replicatedClusterMaxWrites = 3 * max / 10} — the
 *       existing conventions, kept so replication scales with cluster size.</li>
 *   <li>{@code underreplicatedPrimaryClusterMax = max / 20} and {@code collapseMinDuplicates = max / 10} — the
 *       ratios the shipped defaults already imply at {@code max = 1000}.</li>
 * </ul>
 *
 * <h2>What is deliberately not scaled</h2>
 * {@code mergeMaxEverFraction}, {@code maxRelativeImbalance} and {@code splitImbalancePenalty} are all expressed
 * relative to a cluster's own size or to a perfectly balanced partitioning, so they carry no absolute scale and are
 * left at their defaults. Quantization ({@code useRaBitQ}, {@code raBitQNumExBits}) is an encoding decision driven by
 * dimensionality and recall target rather than by cluster size, so it is left to the caller too.
 */
final class ConfigRecommendation {
    /** Cluster sizes below this leave no room for the ratios below to be meaningful. */
    private static final int MIN_SUPPORTED_CLUSTER_MAX = 50;

    private ConfigRecommendation() {
    }

    /**
     * Recommends a configuration for the given metric and cluster size ceiling.
     *
     * @param metric the distance metric the data is compared under
     * @param primaryClusterMax the number of primary vectors at which a cluster splits
     * @return a builder carrying the recommendation, ready for {@code build(numDimensions)} and for any
     *         dataset-specific overrides the caller wants to layer on
     */
    @Nonnull
    static Config.ConfigBuilder forClusterMax(@Nonnull final Metric metric, final int primaryClusterMax) {
        if (primaryClusterMax < MIN_SUPPORTED_CLUSTER_MAX) {
            throw new IllegalArgumentException("primaryClusterMax must be at least " + MIN_SUPPORTED_CLUSTER_MAX
                    + " for the recommended ratios to be meaningful; got " + primaryClusterMax);
        }
        final int primaryClusterMin = primaryClusterMax / 10;
        return Guardiann.newConfigBuilder()
                .setMetric(metric)
                // cluster shape
                .setPrimaryClusterMax(primaryClusterMax)
                .setPrimaryClusterMin(primaryClusterMin)
                .setPrimaryClusterHardMax(2 * primaryClusterMax)
                .setMinChildFraction(primaryClusterMin / (double)primaryClusterMax)
                // replication, scaled with cluster size
                .setReplicatedClusterTarget(primaryClusterMax / 10)
                .setReplicatedClusterMaxWrites(3 * primaryClusterMax / 10)
                .setUnderreplicatedPrimaryClusterMax(primaryClusterMax / 20)
                // deduplication
                .setCollapseMinDuplicates(primaryClusterMax / 10);
    }
}
