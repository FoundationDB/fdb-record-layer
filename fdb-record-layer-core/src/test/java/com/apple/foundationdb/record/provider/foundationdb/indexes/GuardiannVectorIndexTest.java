/*
 * GuardiannVectorIndexTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2025 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.linear.Metric;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexOptions;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.Map;

/**
 * Vector index tests against the Guardiann engine. The behavioral scenarios are inherited from
 * {@link VectorIndexEngineTestSuite}; this class pins the engine to Guardiann and adds the Guardiann-specific
 * option-evolution validation.
 * <p>
 * The cluster-size knobs are tuned so that each partition forms a handful of clusters (rather than one giant cluster or
 * the production-sized defaults), which exercises Guardiann's multi-cluster search/replication paths while staying well
 * within {@code searchMaxClusters} so recall stays high on these small, uniformly-random test datasets.
 */
class GuardiannVectorIndexTest extends VectorIndexEngineTestSuite {

    @Nonnull
    @Override
    protected Map<String, String> indexOptions() {
        return ImmutableMap.<String, String>builder()
                .put(IndexOptions.VECTOR_ENGINE, VectorIndexEngine.Kind.GUARDIANN.name())
                .put(IndexOptions.VECTOR_METRIC, Metric.EUCLIDEAN_METRIC.name())
                .put(IndexOptions.VECTOR_NUM_DIMENSIONS, "128")
                .put(IndexOptions.GUARDIANN_PRIMARY_CLUSTER_MAX, "200")
                .put(IndexOptions.GUARDIANN_PRIMARY_CLUSTER_MIN, "20")
                .put(IndexOptions.GUARDIANN_DETERMINISTIC_RANDOMNESS, "true")
                .build();
    }

    @Override
    protected boolean maintainIndexesInTransaction() {
        return true;
    }

    @Test
    void optionsEvolutionTest() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, this::addGroupedVectorIndex);

            final RecordMetaData metaData = recordStore.getRecordMetaData();
            final Index index = metaData.getIndex("GroupedVectorIndex");

            // re-specifying every option at its current value is always allowed, and the mutable (stats/concurrency)
            // knobs may take new values
            validateOptionsEvolution(metaData, index,
                    ImmutableMap.<String, String>builder()
                            // immutable — must be re-specified at the same value
                            .put(IndexOptions.VECTOR_ENGINE, VectorIndexEngine.Kind.GUARDIANN.name())
                            .put(IndexOptions.VECTOR_METRIC, Metric.EUCLIDEAN_METRIC.name())
                            .put(IndexOptions.VECTOR_NUM_DIMENSIONS, "128")
                            .put(IndexOptions.GUARDIANN_PRIMARY_CLUSTER_MAX, "200")
                            .put(IndexOptions.GUARDIANN_PRIMARY_CLUSTER_MIN, "20")
                            .put(IndexOptions.GUARDIANN_DETERMINISTIC_RANDOMNESS, "true")
                            // mutable — stats and concurrency knobs may change freely
                            .put(IndexOptions.VECTOR_SAMPLE_VECTOR_STATS_PROBABILITY, "0.999")
                            .put(IndexOptions.VECTOR_MAINTAIN_STATS_PROBABILITY, "0.78")
                            .put(IndexOptions.VECTOR_STATS_THRESHOLD, "500")
                            .put(IndexOptions.GUARDIANN_SAMPLE_BATCH_SIZE, "25")
                            .put(IndexOptions.GUARDIANN_DELETE_CONCURRENCY, "5")
                            .put(IndexOptions.GUARDIANN_SPLIT_MERGE_CONCURRENCY, "5")
                            .put(IndexOptions.GUARDIANN_REASSIGN_CONCURRENCY, "5")
                            .put(IndexOptions.GUARDIANN_COLLAPSE_CONCURRENCY, "5")
                            .put(IndexOptions.GUARDIANN_BOUNCE_CONCURRENCY, "5").build());

            // switching the engine is never allowed
            assertInvalidOptionsEvolution(metaData, index,
                    optionsWith(IndexOptions.VECTOR_ENGINE, VectorIndexEngine.Kind.HNSW.name()));

            // changing an immutable encoding option is not allowed
            assertInvalidOptionsEvolution(metaData, index,
                    optionsWith(IndexOptions.VECTOR_METRIC, Metric.COSINE_METRIC.name()));

            assertInvalidOptionsEvolution(metaData, index,
                    optionsWith(IndexOptions.VECTOR_NUM_DIMENSIONS, "768"));

            // changing an immutable cluster-shape knob is not allowed
            assertInvalidOptionsEvolution(metaData, index,
                    optionsWith(IndexOptions.GUARDIANN_PRIMARY_CLUSTER_MAX, "500"));

            assertInvalidOptionsEvolution(metaData, index,
                    optionsWith(IndexOptions.GUARDIANN_PRIMARY_CLUSTER_MIN, "50"));

            // changing an immutable construction-time centroid-walk knob is not allowed: an index must be built with a
            // single, fixed set of construction tuning
            assertInvalidOptionsEvolution(metaData, index,
                    optionsWith(IndexOptions.GUARDIANN_CONSTRUCTION_CENTROID_EF_RING_SEARCH, "250"));

            assertInvalidOptionsEvolution(metaData, index,
                    optionsWith(IndexOptions.GUARDIANN_CONSTRUCTION_CENTROID_EF_OUTWARD_SEARCH, "800"));

            // a mutable knob may change on its own
            validateOptionsEvolution(metaData, index,
                    optionsWith(IndexOptions.GUARDIANN_SPLIT_MERGE_CONCURRENCY, "7"));
        }
    }

    /**
     * The engine's standard {@link #indexOptions()} with a single option overridden (or added). Used to build the
     * "changed exactly one option" cases; unlike an {@link ImmutableMap.Builder}, this overwrites an existing key rather
     * than rejecting the duplicate.
     *
     * @param key the option to override
     * @param value the new value
     * @return the options with {@code key} set to {@code value}
     */
    @Nonnull
    private Map<String, String> optionsWith(@Nonnull final String key, @Nonnull final String value) {
        final Map<String, String> merged = new HashMap<>(indexOptions());
        merged.put(key, value);
        return ImmutableMap.copyOf(merged);
    }
}
