/*
 * SearchConfig.java
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

import com.google.common.base.Preconditions;
import com.google.errorprone.annotations.CanIgnoreReturnValue;

import javax.annotation.Nonnull;

/**
 * The performance/recall tuning knobs for a single Guardiann search. These are the parameters that change how hard the
 * search works — how many candidates it buffers, how many clusters it probes, how aggressively it prunes — without
 * changing <em>what</em> a correct answer is. The semantic inputs ({@code k}, the query vector, {@code includeVectors},
 * and the streaming pagination cursor) stay direct arguments of the search methods; everything that only trades latency
 * against recall lives here so it can travel as one value and grow without widening every signature.
 *
 * <p>
 * There is deliberately no built-in "default" instance: good values depend on the dataset, its size, the index
 * topology, and the recall target, so picking them is the caller's (or the test harness's) responsibility. The
 * {@code DEFAULT_*} constants exist only as the {@link SearchConfigBuilder}'s per-field fallbacks, so a caller can set
 * just the knobs it cares about and leave the rest at a reasonable starting point.
 *
 * @param candidatePoolSize how many nearest candidates to keep in flight before trimming to {@code k}. In the pruned
 *        {@link Search#kNearestNeighborsSearch} this is the size of the {@code DistinctTopK} candidate pool; in the
 *        streaming {@link Search#searchOrderedByDistance} it is the reorder-window size of the almost-sorted iterator.
 *        Larger values improve recall/ordering at the cost of latency. (This replaces the old, misleadingly named
 *        {@code efSearch} — it is unrelated to HNSW's {@code efSearch}.)
 * @param searchMaxClusters the maximum number of cluster centroids to probe around the query
 * @param searchMinClustersBeforePruning the number of nearest clusters always retained before distance-ratio pruning
 *        is allowed to drop any (pruned search only; ignored by the streaming search)
 * @param searchDistanceRatioCutoff clusters whose centroid distance exceeds this multiple of the nearest centroid's
 *        distance are pruned, once {@code searchMinClustersBeforePruning} clusters are retained (pruned search only;
 *        ignored by the streaming search)
 * @param centroidEfRingSearch the ring-search exploration factor for the centroid HNSW walk that selects candidate
 *        clusters (passed through to {@link com.apple.foundationdb.async.hnsw.HNSW#orderByDistance})
 * @param centroidEfOutwardSearch the outward-search exploration factor (candidate-queue size) for that same centroid
 *        HNSW walk
 * @param searchConcurrency the executor parallelism for the fan-out metadata/reference reads a search issues
 */
public record SearchConfig(int candidatePoolSize,
                           int searchMaxClusters,
                           int searchMinClustersBeforePruning,
                           double searchDistanceRatioCutoff,
                           int centroidEfRingSearch,
                           int centroidEfOutwardSearch,
                           int searchConcurrency) {

    public static final int DEFAULT_CANDIDATE_POOL_SIZE = 128;
    public static final int DEFAULT_SEARCH_MAX_CLUSTERS = 48;
    public static final int DEFAULT_SEARCH_MIN_CLUSTERS_BEFORE_PRUNING = 16;
    public static final double DEFAULT_SEARCH_DISTANCE_RATIO_CUTOFF = 1.5d;
    public static final int DEFAULT_CENTROID_EF_RING_SEARCH = 100;
    public static final int DEFAULT_CENTROID_EF_OUTWARD_SEARCH = 400;
    public static final int DEFAULT_SEARCH_CONCURRENCY = 10;

    public SearchConfig {
        Preconditions.checkArgument(candidatePoolSize >= 1, "candidatePoolSize must be >= 1");
        Preconditions.checkArgument(searchMaxClusters >= 1, "searchMaxClusters must be >= 1");
        Preconditions.checkArgument(searchMinClustersBeforePruning >= 0,
                "searchMinClustersBeforePruning must be >= 0");
        Preconditions.checkArgument(searchDistanceRatioCutoff >= 1.0d,
                "searchDistanceRatioCutoff must be >= 1.0");
        Preconditions.checkArgument(centroidEfRingSearch >= 1, "centroidEfRingSearch must be >= 1");
        Preconditions.checkArgument(centroidEfOutwardSearch >= 1, "centroidEfOutwardSearch must be >= 1");
        Preconditions.checkArgument(searchConcurrency >= 1, "searchConcurrency must be >= 1");
    }

    @Nonnull
    public SearchConfigBuilder toBuilder() {
        return new SearchConfigBuilder(candidatePoolSize(), searchMaxClusters(), searchMinClustersBeforePruning(),
                searchDistanceRatioCutoff(), centroidEfRingSearch(), centroidEfOutwardSearch(), searchConcurrency());
    }

    @Override
    @Nonnull
    public String toString() {
        return "SearchConfig[candidatePoolSize=" + candidatePoolSize() +
                ", searchMaxClusters=" + searchMaxClusters() +
                ", searchMinClustersBeforePruning=" + searchMinClustersBeforePruning() +
                ", searchDistanceRatioCutoff=" + searchDistanceRatioCutoff() +
                ", centroidEfRingSearch=" + centroidEfRingSearch() +
                ", centroidEfOutwardSearch=" + centroidEfOutwardSearch() +
                ", searchConcurrency=" + searchConcurrency() +
                "]";
    }

    /**
     * Builder for {@link SearchConfig}. Each field defaults to its {@code DEFAULT_*} constant, so a caller can set only
     * the knobs it wants to tune.
     */
    @CanIgnoreReturnValue
    public static class SearchConfigBuilder {
        private int candidatePoolSize = DEFAULT_CANDIDATE_POOL_SIZE;
        private int searchMaxClusters = DEFAULT_SEARCH_MAX_CLUSTERS;
        private int searchMinClustersBeforePruning = DEFAULT_SEARCH_MIN_CLUSTERS_BEFORE_PRUNING;
        private double searchDistanceRatioCutoff = DEFAULT_SEARCH_DISTANCE_RATIO_CUTOFF;
        private int centroidEfRingSearch = DEFAULT_CENTROID_EF_RING_SEARCH;
        private int centroidEfOutwardSearch = DEFAULT_CENTROID_EF_OUTWARD_SEARCH;
        private int searchConcurrency = DEFAULT_SEARCH_CONCURRENCY;

        public SearchConfigBuilder() {
        }

        public SearchConfigBuilder(final int candidatePoolSize, final int searchMaxClusters,
                                   final int searchMinClustersBeforePruning, final double searchDistanceRatioCutoff,
                                   final int centroidEfRingSearch, final int centroidEfOutwardSearch,
                                   final int searchConcurrency) {
            this.candidatePoolSize = candidatePoolSize;
            this.searchMaxClusters = searchMaxClusters;
            this.searchMinClustersBeforePruning = searchMinClustersBeforePruning;
            this.searchDistanceRatioCutoff = searchDistanceRatioCutoff;
            this.centroidEfRingSearch = centroidEfRingSearch;
            this.centroidEfOutwardSearch = centroidEfOutwardSearch;
            this.searchConcurrency = searchConcurrency;
        }

        public int getCandidatePoolSize() {
            return candidatePoolSize;
        }

        @Nonnull
        public SearchConfigBuilder setCandidatePoolSize(final int candidatePoolSize) {
            this.candidatePoolSize = candidatePoolSize;
            return this;
        }

        public int getSearchMaxClusters() {
            return searchMaxClusters;
        }

        @Nonnull
        public SearchConfigBuilder setSearchMaxClusters(final int searchMaxClusters) {
            this.searchMaxClusters = searchMaxClusters;
            return this;
        }

        public int getSearchMinClustersBeforePruning() {
            return searchMinClustersBeforePruning;
        }

        @Nonnull
        public SearchConfigBuilder setSearchMinClustersBeforePruning(final int searchMinClustersBeforePruning) {
            this.searchMinClustersBeforePruning = searchMinClustersBeforePruning;
            return this;
        }

        public double getSearchDistanceRatioCutoff() {
            return searchDistanceRatioCutoff;
        }

        @Nonnull
        public SearchConfigBuilder setSearchDistanceRatioCutoff(final double searchDistanceRatioCutoff) {
            this.searchDistanceRatioCutoff = searchDistanceRatioCutoff;
            return this;
        }

        public int getCentroidEfRingSearch() {
            return centroidEfRingSearch;
        }

        @Nonnull
        public SearchConfigBuilder setCentroidEfRingSearch(final int centroidEfRingSearch) {
            this.centroidEfRingSearch = centroidEfRingSearch;
            return this;
        }

        public int getCentroidEfOutwardSearch() {
            return centroidEfOutwardSearch;
        }

        @Nonnull
        public SearchConfigBuilder setCentroidEfOutwardSearch(final int centroidEfOutwardSearch) {
            this.centroidEfOutwardSearch = centroidEfOutwardSearch;
            return this;
        }

        public int getSearchConcurrency() {
            return searchConcurrency;
        }

        @Nonnull
        public SearchConfigBuilder setSearchConcurrency(final int searchConcurrency) {
            this.searchConcurrency = searchConcurrency;
            return this;
        }

        @Nonnull
        public SearchConfig build() {
            return new SearchConfig(getCandidatePoolSize(), getSearchMaxClusters(),
                    getSearchMinClustersBeforePruning(), getSearchDistanceRatioCutoff(), getCentroidEfRingSearch(),
                    getCentroidEfOutwardSearch(), getSearchConcurrency());
        }
    }
}
