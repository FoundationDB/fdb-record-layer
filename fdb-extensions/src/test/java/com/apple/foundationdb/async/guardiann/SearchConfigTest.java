/*
 * SearchConfigTest.java
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

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

class SearchConfigTest {

    @Test
    void testSearchConfig() {
        final SearchConfig defaultConfig = new SearchConfig.SearchConfigBuilder().build();

        Assertions.assertThat(new SearchConfig.SearchConfigBuilder().build()).isEqualTo(defaultConfig);
        Assertions.assertThat(defaultConfig.toBuilder().build()).isEqualTo(defaultConfig);

        final double candidatePoolFactor = SearchConfig.DEFAULT_CANDIDATE_POOL_FACTOR + 1.0d;
        final int searchMaxClusters = SearchConfig.DEFAULT_SEARCH_MAX_CLUSTERS + 1;
        final int searchMinClustersBeforePruning = SearchConfig.DEFAULT_SEARCH_MIN_CLUSTERS_BEFORE_PRUNING + 1;
        final double searchDistanceRatioCutoff = SearchConfig.DEFAULT_SEARCH_DISTANCE_RATIO_CUTOFF + 1.0d;
        final int centroidEfRingSearch = SearchConfig.DEFAULT_CENTROID_EF_RING_SEARCH + 1;
        final int centroidEfOutwardSearch = SearchConfig.DEFAULT_CENTROID_EF_OUTWARD_SEARCH + 1;
        final int searchConcurrency = SearchConfig.DEFAULT_SEARCH_CONCURRENCY + 1;

        Assertions.assertThat(defaultConfig.candidatePoolFactor()).isNotEqualTo(candidatePoolFactor);
        Assertions.assertThat(defaultConfig.searchMaxClusters()).isNotEqualTo(searchMaxClusters);
        Assertions.assertThat(defaultConfig.searchMinClustersBeforePruning()).isNotEqualTo(searchMinClustersBeforePruning);
        Assertions.assertThat(defaultConfig.searchDistanceRatioCutoff()).isNotEqualTo(searchDistanceRatioCutoff);
        Assertions.assertThat(defaultConfig.centroidEfRingSearch()).isNotEqualTo(centroidEfRingSearch);
        Assertions.assertThat(defaultConfig.centroidEfOutwardSearch()).isNotEqualTo(centroidEfOutwardSearch);
        Assertions.assertThat(defaultConfig.searchConcurrency()).isNotEqualTo(searchConcurrency);

        final SearchConfig newConfig =
                defaultConfig.toBuilder()
                        .setCandidatePoolFactor(candidatePoolFactor)
                        .setSearchMaxClusters(searchMaxClusters)
                        .setSearchMinClustersBeforePruning(searchMinClustersBeforePruning)
                        .setSearchDistanceRatioCutoff(searchDistanceRatioCutoff)
                        .setCentroidEfRingSearch(centroidEfRingSearch)
                        .setCentroidEfOutwardSearch(centroidEfOutwardSearch)
                        .setSearchConcurrency(searchConcurrency)
                        .build();

        Assertions.assertThat(newConfig.candidatePoolFactor()).isEqualTo(candidatePoolFactor);
        Assertions.assertThat(newConfig.searchMaxClusters()).isEqualTo(searchMaxClusters);
        Assertions.assertThat(newConfig.searchMinClustersBeforePruning()).isEqualTo(searchMinClustersBeforePruning);
        Assertions.assertThat(newConfig.searchDistanceRatioCutoff()).isEqualTo(searchDistanceRatioCutoff);
        Assertions.assertThat(newConfig.centroidEfRingSearch()).isEqualTo(centroidEfRingSearch);
        Assertions.assertThat(newConfig.centroidEfOutwardSearch()).isEqualTo(centroidEfOutwardSearch);
        Assertions.assertThat(newConfig.searchConcurrency()).isEqualTo(searchConcurrency);
    }

    @Test
    void testCandidatePoolSizeAppliesFactorAndFloorsAtK() {
        // Factor of exactly 1.0: the pool equals k (never smaller).
        final SearchConfig unitFactor = new SearchConfig.SearchConfigBuilder().setCandidatePoolFactor(1.0d).build();
        Assertions.assertThat(unitFactor.candidatePoolSize(10)).isEqualTo(10);
        Assertions.assertThat(unitFactor.candidatePoolSize(1)).isEqualTo(1);

        // A fractional factor rounds up so the pool is strictly larger than k.
        final SearchConfig oversampling = new SearchConfig.SearchConfigBuilder().setCandidatePoolFactor(1.15d).build();
        Assertions.assertThat(oversampling.candidatePoolSize(100)).isEqualTo(115);
        Assertions.assertThat(oversampling.candidatePoolSize(10)).isEqualTo(12); // ceil(11.5)
        Assertions.assertThat(oversampling.candidatePoolSize(1)).isEqualTo(2);   // ceil(1.15)

        final SearchConfig doubling = new SearchConfig.SearchConfigBuilder().setCandidatePoolFactor(2.0d).build();
        Assertions.assertThat(doubling.candidatePoolSize(50)).isEqualTo(100);
    }

    @Test
    void testSearchConfigValidatesArguments() {
        Assertions.assertThatThrownBy(() ->
                        new SearchConfig.SearchConfigBuilder().setCandidatePoolFactor(0.99d).build())
                .isInstanceOf(IllegalArgumentException.class);
        Assertions.assertThatThrownBy(() -> new SearchConfig.SearchConfigBuilder().setSearchMaxClusters(0).build())
                .isInstanceOf(IllegalArgumentException.class);
        Assertions.assertThatThrownBy(() ->
                        new SearchConfig.SearchConfigBuilder().setSearchDistanceRatioCutoff(0.5d).build())
                .isInstanceOf(IllegalArgumentException.class);
        Assertions.assertThatThrownBy(() -> new SearchConfig.SearchConfigBuilder().setSearchConcurrency(0).build())
                .isInstanceOf(IllegalArgumentException.class);
    }
}
