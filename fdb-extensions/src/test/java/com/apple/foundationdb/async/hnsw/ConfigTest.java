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

        final long randomSeed = 1L;
        final Metric metric = Metric.COSINE_METRIC;
        final boolean useInlining = true;
        final int m = Config.DEFAULT_M + 1;
        final int mMax = Config.DEFAULT_M_MAX + 1;
        final int mMax0 = Config.DEFAULT_M_MAX_0 + 1;
        final int efConstruction = Config.DEFAULT_EF_CONSTRUCTION + 1;
        final boolean extendCandidates = true;
        final boolean keepPrunedConnections = true;
        final int statsThreshold = 1;
        final double sampleVectorStatsProbability = 0.000001d;
        final double maintainStatsProbability = 0.000001d;

        final boolean useRaBitQ = true;
        final int raBitQNumExBits = Config.DEFAULT_RABITQ_NUM_EX_BITS + 1;

        Assertions.assertThat(defaultConfig.getRandomSeed()).isNotEqualTo(randomSeed);
        Assertions.assertThat(defaultConfig.getMetric()).isNotSameAs(metric);
        Assertions.assertThat(defaultConfig.isUseInlining()).isNotEqualTo(useInlining);
        Assertions.assertThat(defaultConfig.getM()).isNotEqualTo(m);
        Assertions.assertThat(defaultConfig.getMMax()).isNotEqualTo(mMax);
        Assertions.assertThat(defaultConfig.getMMax0()).isNotEqualTo(mMax0);
        Assertions.assertThat(defaultConfig.getEfConstruction()).isNotEqualTo(efConstruction);
        Assertions.assertThat(defaultConfig.isExtendCandidates()).isNotEqualTo(extendCandidates);
        Assertions.assertThat(defaultConfig.isKeepPrunedConnections()).isNotEqualTo(keepPrunedConnections);

        Assertions.assertThat(defaultConfig.getSampleVectorStatsProbability()).isNotEqualTo(sampleVectorStatsProbability);
        Assertions.assertThat(defaultConfig.getMaintainStatsProbability()).isNotEqualTo(maintainStatsProbability);
        Assertions.assertThat(defaultConfig.getStatsThreshold()).isNotEqualTo(statsThreshold);

        Assertions.assertThat(defaultConfig.isUseRaBitQ()).isNotEqualTo(useRaBitQ);
        Assertions.assertThat(defaultConfig.getRaBitQNumExBits()).isNotEqualTo(raBitQNumExBits);

        final Config newConfig =
                defaultConfig.toBuilder()
                        .setRandomSeed(randomSeed)
                        .setMetric(metric)
                        .setUseInlining(useInlining)
                        .setM(m)
                        .setMMax(mMax)
                        .setMMax0(mMax0)
                        .setEfConstruction(efConstruction)
                        .setExtendCandidates(extendCandidates)
                        .setKeepPrunedConnections(keepPrunedConnections)
                        .setSampleVectorStatsProbability(sampleVectorStatsProbability)
                        .setMaintainStatsProbability(maintainStatsProbability)
                        .setStatsThreshold(statsThreshold)
                        .setUseRaBitQ(useRaBitQ)
                        .setRaBitQNumExBits(raBitQNumExBits)
                        .build(768);

        Assertions.assertThat(newConfig.getRandomSeed()).isEqualTo(randomSeed);
        Assertions.assertThat(newConfig.getMetric()).isSameAs(metric);
        Assertions.assertThat(newConfig.isUseInlining()).isEqualTo(useInlining);
        Assertions.assertThat(newConfig.getM()).isEqualTo(m);
        Assertions.assertThat(newConfig.getMMax()).isEqualTo(mMax);
        Assertions.assertThat(newConfig.getMMax0()).isEqualTo(mMax0);
        Assertions.assertThat(newConfig.getEfConstruction()).isEqualTo(efConstruction);
        Assertions.assertThat(newConfig.isExtendCandidates()).isEqualTo(extendCandidates);
        Assertions.assertThat(newConfig.isKeepPrunedConnections()).isEqualTo(keepPrunedConnections);

        Assertions.assertThat(newConfig.getSampleVectorStatsProbability()).isEqualTo(sampleVectorStatsProbability);
        Assertions.assertThat(newConfig.getMaintainStatsProbability()).isEqualTo(maintainStatsProbability);
        Assertions.assertThat(newConfig.getStatsThreshold()).isEqualTo(statsThreshold);

        Assertions.assertThat(newConfig.isUseRaBitQ()).isEqualTo(useRaBitQ);
        Assertions.assertThat(newConfig.getRaBitQNumExBits()).isEqualTo(raBitQNumExBits);
    }

    @Test
    void testEqualsHashCodeAndToString() {
        final Config config1 = HNSW.newConfigBuilder().build(768);
        final Config config2 = HNSW.newConfigBuilder().build(768);
        final Config config3 = HNSW.newConfigBuilder().setM(1).build(768);

        Assertions.assertThat(config1.hashCode()).isEqualTo(config2.hashCode());
        Assertions.assertThat(config1).isEqualTo(config2);
        Assertions.assertThat(config3).isNotEqualTo(config1);

        Assertions.assertThat(config1.toString()).isEqualTo(config2.toString());
        Assertions.assertThat(config1.toString()).isNotEqualTo(config3.toString());
    }
}
