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

        final int candidatePoolSize = SearchConfig.DEFAULT_CANDIDATE_POOL_SIZE + 1;
        final int searchMaxClusters = SearchConfig.DEFAULT_SEARCH_MAX_CLUSTERS + 1;
        final int searchMinClustersBeforePruning = SearchConfig.DEFAULT_SEARCH_MIN_CLUSTERS_BEFORE_PRUNING + 1;
        final double searchDistanceRatioCutoff = SearchConfig.DEFAULT_SEARCH_DISTANCE_RATIO_CUTOFF + 1.0d;
        final int centroidEfRingSearch = SearchConfig.DEFAULT_CENTROID_EF_RING_SEARCH + 1;
        final int centroidEfOutwardSearch = SearchConfig.DEFAULT_CENTROID_EF_OUTWARD_SEARCH + 1;
        final int searchConcurrency = SearchConfig.DEFAULT_SEARCH_CONCURRENCY + 1;

        Assertions.assertThat(defaultConfig.candidatePoolSize()).isNotEqualTo(candidatePoolSize);
        Assertions.assertThat(defaultConfig.searchMaxClusters()).isNotEqualTo(searchMaxClusters);
        Assertions.assertThat(defaultConfig.searchMinClustersBeforePruning()).isNotEqualTo(searchMinClustersBeforePruning);
        Assertions.assertThat(defaultConfig.searchDistanceRatioCutoff()).isNotEqualTo(searchDistanceRatioCutoff);
        Assertions.assertThat(defaultConfig.centroidEfRingSearch()).isNotEqualTo(centroidEfRingSearch);
        Assertions.assertThat(defaultConfig.centroidEfOutwardSearch()).isNotEqualTo(centroidEfOutwardSearch);
        Assertions.assertThat(defaultConfig.searchConcurrency()).isNotEqualTo(searchConcurrency);

        final SearchConfig newConfig =
                defaultConfig.toBuilder()
                        .setCandidatePoolSize(candidatePoolSize)
                        .setSearchMaxClusters(searchMaxClusters)
                        .setSearchMinClustersBeforePruning(searchMinClustersBeforePruning)
                        .setSearchDistanceRatioCutoff(searchDistanceRatioCutoff)
                        .setCentroidEfRingSearch(centroidEfRingSearch)
                        .setCentroidEfOutwardSearch(centroidEfOutwardSearch)
                        .setSearchConcurrency(searchConcurrency)
                        .build();

        Assertions.assertThat(newConfig.candidatePoolSize()).isEqualTo(candidatePoolSize);
        Assertions.assertThat(newConfig.searchMaxClusters()).isEqualTo(searchMaxClusters);
        Assertions.assertThat(newConfig.searchMinClustersBeforePruning()).isEqualTo(searchMinClustersBeforePruning);
        Assertions.assertThat(newConfig.searchDistanceRatioCutoff()).isEqualTo(searchDistanceRatioCutoff);
        Assertions.assertThat(newConfig.centroidEfRingSearch()).isEqualTo(centroidEfRingSearch);
        Assertions.assertThat(newConfig.centroidEfOutwardSearch()).isEqualTo(centroidEfOutwardSearch);
        Assertions.assertThat(newConfig.searchConcurrency()).isEqualTo(searchConcurrency);
    }

    @Test
    void testSearchConfigValidatesArguments() {
        Assertions.assertThatThrownBy(() -> new SearchConfig.SearchConfigBuilder().setCandidatePoolSize(0).build())
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
