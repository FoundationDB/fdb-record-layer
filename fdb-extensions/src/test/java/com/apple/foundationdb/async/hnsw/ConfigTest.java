/*
 * ConfigTest.java
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

package com.apple.foundationdb.async.hnsw;

import com.apple.foundationdb.linear.Metric;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

class ConfigTest {
    @Test
    void testConfig() {
        final Config defaultConfig = HNSW.defaultConfig(768);

        Assertions.assertThat(HNSW.newConfigBuilder().build(768)).isEqualTo(defaultConfig);
        Assertions.assertThat(defaultConfig.toBuilder().build(768)).isEqualTo(defaultConfig);

        final Metric metric = Metric.COSINE_METRIC;
        final boolean useInlining = true;
        final int m = Config.DEFAULT_M + 1;
        final int mMax = Config.DEFAULT_M_MAX + 1;
        final int mMax0 = Config.DEFAULT_M_MAX_0 + 1;
        final int efConstruction = Config.DEFAULT_EF_CONSTRUCTION + 1;
        final int efRepair = Config.DEFAULT_EF_REPAIR + 1;
        final boolean extendCandidates = true;
        final boolean keepPrunedConnections = true;
        final int statsThreshold = 5000;
        final double sampleVectorStatsProbability = 0.000001d;
        final double maintainStatsProbability = 0.000002d;

        final boolean useRaBitQ = true;
        final int raBitQNumExBits = Config.DEFAULT_RABITQ_NUM_EX_BITS + 1;

        final int maxNumConcurrentNodeFetches = 1;
        final int maxNumConcurrentNeighborhoodFetches = 2;
        final int maxNumConcurrentDeleteFromLayer = Config.DEFAULT_MAX_NUM_CONCURRENT_DELETE_FROM_LAYER + 1;

        Assertions.assertThat(defaultConfig.metric()).isNotSameAs(metric);
        Assertions.assertThat(defaultConfig.useInlining()).isNotEqualTo(useInlining);
        Assertions.assertThat(defaultConfig.m()).isNotEqualTo(m);
        Assertions.assertThat(defaultConfig.mMax()).isNotEqualTo(mMax);
        Assertions.assertThat(defaultConfig.mMax0()).isNotEqualTo(mMax0);
        Assertions.assertThat(defaultConfig.efConstruction()).isNotEqualTo(efConstruction);
        Assertions.assertThat(defaultConfig.efRepair()).isNotEqualTo(efRepair);
        Assertions.assertThat(defaultConfig.extendCandidates()).isNotEqualTo(extendCandidates);
        Assertions.assertThat(defaultConfig.keepPrunedConnections()).isNotEqualTo(keepPrunedConnections);

        Assertions.assertThat(defaultConfig.sampleVectorStatsProbability()).isNotEqualTo(sampleVectorStatsProbability);
        Assertions.assertThat(defaultConfig.maintainStatsProbability()).isNotEqualTo(maintainStatsProbability);
        Assertions.assertThat(defaultConfig.statsThreshold()).isNotEqualTo(statsThreshold);

        Assertions.assertThat(defaultConfig.useRaBitQ()).isNotEqualTo(useRaBitQ);
        Assertions.assertThat(defaultConfig.raBitQNumExBits()).isNotEqualTo(raBitQNumExBits);

        Assertions.assertThat(defaultConfig.maxNumConcurrentNodeFetches()).isNotEqualTo(maxNumConcurrentNodeFetches);
        Assertions.assertThat(defaultConfig.maxNumConcurrentNeighborhoodFetches()).isNotEqualTo(maxNumConcurrentNeighborhoodFetches);
        Assertions.assertThat(defaultConfig.maxNumConcurrentDeleteFromLayer()).isNotEqualTo(maxNumConcurrentDeleteFromLayer);

        final Config newConfig =
                defaultConfig.toBuilder()
                        .setMetric(metric)
                        .setUseInlining(useInlining)
                        .setM(m)
                        .setMMax(mMax)
                        .setMMax0(mMax0)
                        .setEfConstruction(efConstruction)
                        .setEfRepair(efRepair)
                        .setExtendCandidates(extendCandidates)
                        .setKeepPrunedConnections(keepPrunedConnections)
                        .setSampleVectorStatsProbability(sampleVectorStatsProbability)
                        .setMaintainStatsProbability(maintainStatsProbability)
                        .setStatsThreshold(statsThreshold)
                        .setUseRaBitQ(useRaBitQ)
                        .setRaBitQNumExBits(raBitQNumExBits)
                        .setMaxNumConcurrentNodeFetches(maxNumConcurrentNodeFetches)
                        .setMaxNumConcurrentNeighborhoodFetches(maxNumConcurrentNeighborhoodFetches)
                        .setMaxNumConcurrentDeleteFromLayer(maxNumConcurrentDeleteFromLayer)
                        .build(768);

        Assertions.assertThat(newConfig.metric()).isSameAs(metric);
        Assertions.assertThat(newConfig.useInlining()).isEqualTo(useInlining);
        Assertions.assertThat(newConfig.m()).isEqualTo(m);
        Assertions.assertThat(newConfig.mMax()).isEqualTo(mMax);
        Assertions.assertThat(newConfig.mMax0()).isEqualTo(mMax0);
        Assertions.assertThat(newConfig.efConstruction()).isEqualTo(efConstruction);
        Assertions.assertThat(newConfig.efRepair()).isEqualTo(efRepair);
        Assertions.assertThat(newConfig.extendCandidates()).isEqualTo(extendCandidates);
        Assertions.assertThat(newConfig.keepPrunedConnections()).isEqualTo(keepPrunedConnections);

        Assertions.assertThat(newConfig.sampleVectorStatsProbability()).isEqualTo(sampleVectorStatsProbability);
        Assertions.assertThat(newConfig.maintainStatsProbability()).isEqualTo(maintainStatsProbability);
        Assertions.assertThat(newConfig.statsThreshold()).isEqualTo(statsThreshold);

        Assertions.assertThat(newConfig.useRaBitQ()).isEqualTo(useRaBitQ);
        Assertions.assertThat(newConfig.raBitQNumExBits()).isEqualTo(raBitQNumExBits);

        Assertions.assertThat(newConfig.maxNumConcurrentNodeFetches()).isEqualTo(maxNumConcurrentNodeFetches);
        Assertions.assertThat(newConfig.maxNumConcurrentNeighborhoodFetches()).isEqualTo(maxNumConcurrentNeighborhoodFetches);
        Assertions.assertThat(newConfig.maxNumConcurrentDeleteFromLayer()).isEqualTo(maxNumConcurrentDeleteFromLayer);
    }

    @Test
    void testEqualsHashCodeAndToString() {
        final Config config1 = HNSW.newConfigBuilder().build(768);
        final Config config2 = HNSW.newConfigBuilder().build(768);
        final Config config3 = HNSW.newConfigBuilder().setM(4).build(768);

        Assertions.assertThat(config1.hashCode()).isEqualTo(config2.hashCode());
        Assertions.assertThat(config1).isEqualTo(config2);
        Assertions.assertThat(config3).isNotEqualTo(config1);

        Assertions.assertThat(config1.toString()).isEqualTo(config2.toString());
        Assertions.assertThat(config1.toString()).isNotEqualTo(config3.toString());
    }
}
