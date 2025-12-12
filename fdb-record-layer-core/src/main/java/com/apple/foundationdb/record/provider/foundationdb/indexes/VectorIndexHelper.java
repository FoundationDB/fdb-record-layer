/*
 * VectorIndexHelper.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2023 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.provider.foundationdb.indexes;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.async.hnsw.Config;
import com.apple.foundationdb.async.hnsw.Config.ConfigBuilder;
import com.apple.foundationdb.async.hnsw.HNSW;
import com.apple.foundationdb.linear.Metric;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexOptions;
import com.apple.foundationdb.record.metadata.MetaDataException;
import com.apple.foundationdb.record.provider.common.StoreTimer;

import javax.annotation.Nonnull;

/**
 * Helper functions for index maintainers that use a {@link HNSW}.
 */
@API(API.Status.EXPERIMENTAL)
public final class VectorIndexHelper {
    private VectorIndexHelper() {
    }

    /**
     * Parse standard options into {@link Config}.
     * @param index the index definition to get options from
     * @return parsed config options
     */
    @Nonnull
    public static Config getConfig(@Nonnull final Index index) {
        final ConfigBuilder builder = HNSW.newConfigBuilder();
        final String hnswMetricOption = index.getOption(IndexOptions.HNSW_METRIC);
        if (hnswMetricOption != null) {
            builder.setMetric(Metric.valueOf(hnswMetricOption));
        }
        final String hnswNumDimensionsOption = index.getOption(IndexOptions.HNSW_NUM_DIMENSIONS);
        if (hnswNumDimensionsOption == null) {
            throw new MetaDataException("need to specify the number of dimensions",
                    LogMessageKeys.INDEX_NAME, index.getName());
        }
        final int numDimensions = Integer.parseInt(hnswNumDimensionsOption);

        final String hnswUseInliningOption = index.getOption(IndexOptions.HNSW_USE_INLINING);
        if (hnswUseInliningOption != null) {
            builder.setUseInlining(Boolean.parseBoolean(hnswUseInliningOption));
        }
        final String hnswMOption = index.getOption(IndexOptions.HNSW_M);
        if (hnswMOption != null) {
            builder.setM(Integer.parseInt(hnswMOption));
        }
        final String hnswMMaxOption = index.getOption(IndexOptions.HNSW_M_MAX);
        if (hnswMMaxOption != null) {
            builder.setMMax(Integer.parseInt(hnswMMaxOption));
        }
        final String hnswMMax0Option = index.getOption(IndexOptions.HNSW_M_MAX_0);
        if (hnswMMax0Option != null) {
            builder.setMMax0(Integer.parseInt(hnswMMax0Option));
        }
        final String hnswEfConstructionOption = index.getOption(IndexOptions.HNSW_EF_CONSTRUCTION);
        if (hnswEfConstructionOption != null) {
            builder.setEfConstruction(Integer.parseInt(hnswEfConstructionOption));
        }
        final String hnswExtendCandidatesOption = index.getOption(IndexOptions.HNSW_EXTEND_CANDIDATES);
        if (hnswExtendCandidatesOption != null) {
            builder.setExtendCandidates(Boolean.parseBoolean(hnswExtendCandidatesOption));
        }
        final String hnswKeepPrunedConnectionsOption = index.getOption(IndexOptions.HNSW_KEEP_PRUNED_CONNECTIONS);
        if (hnswKeepPrunedConnectionsOption != null) {
            builder.setKeepPrunedConnections(Boolean.parseBoolean(hnswKeepPrunedConnectionsOption));
        }
        final String hnswSampleVectorStatsProbabilityOption = index.getOption(IndexOptions.HNSW_SAMPLE_VECTOR_STATS_PROBABILITY);
        if (hnswSampleVectorStatsProbabilityOption != null) {
            builder.setSampleVectorStatsProbability(Double.parseDouble(hnswSampleVectorStatsProbabilityOption));
        }
        final String hnswMaintainStatsProbabilityOption = index.getOption(IndexOptions.HNSW_MAINTAIN_STATS_PROBABILITY);
        if (hnswMaintainStatsProbabilityOption != null) {
            builder.setMaintainStatsProbability(Double.parseDouble(hnswMaintainStatsProbabilityOption));
        }
        final String hnswStatsThresholdOption = index.getOption(IndexOptions.HNSW_STATS_THRESHOLD);
        if (hnswStatsThresholdOption != null) {
            builder.setStatsThreshold(Integer.parseInt(hnswStatsThresholdOption));
        }
        final String hnswUseRaBitQOption = index.getOption(IndexOptions.HNSW_USE_RABITQ);
        if (hnswUseRaBitQOption != null) {
            builder.setUseRaBitQ(Boolean.parseBoolean(hnswUseRaBitQOption));
        }
        final String hnswRaBitQNumExBitsOption = index.getOption(IndexOptions.HNSW_RABITQ_NUM_EX_BITS);
        if (hnswRaBitQNumExBitsOption != null) {
            builder.setRaBitQNumExBits(Integer.parseInt(hnswRaBitQNumExBitsOption));
        }
        final String hnswMaxNumConcurrentNodeFetchesOption = index.getOption(IndexOptions.HNSW_MAX_NUM_CONCURRENT_NODE_FETCHES);
        if (hnswMaxNumConcurrentNodeFetchesOption != null) {
            builder.setMaxNumConcurrentNodeFetches(Integer.parseInt(hnswMaxNumConcurrentNodeFetchesOption));
        }
        final String hnswMaxNumConcurrentNeighborhoodFetchesOption = index.getOption(IndexOptions.HNSW_MAX_NUM_CONCURRENT_NEIGHBORHOOD_FETCHES);
        if (hnswMaxNumConcurrentNeighborhoodFetchesOption != null) {
            builder.setMaxNumConcurrentNeighborhoodFetches(Integer.parseInt(hnswMaxNumConcurrentNeighborhoodFetchesOption));
        }
        return builder.build(numDimensions);
    }

    /**
     * Instrumentation events specific to vector index maintenance.
     */
    public enum Events implements StoreTimer.DetailEvent {
        VECTOR_SCAN("scanning the partition of a vector index"),
        VECTOR_SKIP_SCAN("skip scan the prefix tuples of a vector index scan");

        private final String title;
        private final String logKey;

        Events(String title, String logKey) {
            this.title = title;
            this.logKey = (logKey != null) ? logKey : StoreTimer.DetailEvent.super.logKey();
        }

        Events(String title) {
            this(title, null);
        }

        @Override
        public String title() {
            return title;
        }

        @Override
        @Nonnull
        public String logKey() {
            return this.logKey;
        }
    }
}
