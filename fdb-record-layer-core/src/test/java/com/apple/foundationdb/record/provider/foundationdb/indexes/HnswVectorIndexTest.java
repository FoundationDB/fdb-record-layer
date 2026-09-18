/*
 * HnswVectorIndexTest.java
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

package com.apple.foundationdb.record.provider.foundationdb.indexes;

import com.apple.foundationdb.linear.Metric;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexOptions;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.subspace.Subspace;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Vector index tests against the HNSW engine. The behavioral scenarios are inherited from
 * {@link VectorIndexEngineTestSuite}; this class pins the engine to HNSW and adds the HNSW-specific option-evolution
 * validation.
 */
class HnswVectorIndexTest extends VectorIndexEngineTestSuite {

    @Nonnull
    @Override
    protected Map<String, String> indexOptions() {
        return ImmutableMap.of(IndexOptions.VECTOR_ENGINE, VectorIndexEngineKind.HNSW.name(),
                IndexOptions.VECTOR_METRIC, Metric.EUCLIDEAN_METRIC.name(),
                IndexOptions.VECTOR_NUM_DIMENSIONS, "128");
    }

    /**
     * The HNSW engine does everything inline and enqueues no deferred tasks, so it tracks no task counts
     * ({@code getTaskCounts()} is null) and the maintainer never routes a merge to it — being asked to drain is a
     * programming error, which {@code executeDeferredTasks} rejects.
     */
    @Test
    void executeDeferredTasksIsAnIllegalCall() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, this::addUngroupedVectorIndex);
            final Index index = recordStore.getRecordMetaData().getIndex("UngroupedVectorIndex");
            final HnswVectorIndexEngine engine = HnswVectorIndexEngine.fromIndex(index);
            final Subspace subspace = recordStore.indexSubspace(index);

            assertThat(engine.getTaskCounts())
                    .as("HNSW tracks no deferred work, so the maintainer never routes a merge to it").isNull();
            assertThatThrownBy(() ->
                    engine.executeDeferredTasks(context, subspace, 1, TaskEventRegister.NOOP, Long.MAX_VALUE))
                    .as("draining the inline HNSW engine is an illegal call")
                    .isInstanceOf(RecordCoreException.class)
                    .hasMessageContaining("no deferred tasks");
        }
    }

    @Test
    void optionsEvolutionTest() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, this::addGroupedVectorIndex);

            final RecordMetaData metaData = recordStore.getRecordMetaData();
            final Index index = metaData.getIndex("GroupedVectorIndex");

            // validate the allowed changes all at once
            validateOptionsEvolution(metaData, index,
                    ImmutableMap.<String, String>builder()
                            .put(IndexOptions.VECTOR_ENGINE, VectorIndexEngineKind.HNSW.name())
                            // cannot change those per se but must accept same value
                            .put(IndexOptions.VECTOR_METRIC, Metric.EUCLIDEAN_METRIC.name())
                            .put(IndexOptions.VECTOR_NUM_DIMENSIONS, "128")
                            .put(IndexOptions.HNSW_USE_INLINING, "false")
                            .put(IndexOptions.HNSW_M, "16")
                            .put(IndexOptions.HNSW_M_MAX, "16")
                            .put(IndexOptions.HNSW_M_MAX_0, "32")
                            .put(IndexOptions.HNSW_EF_CONSTRUCTION, "200")
                            .put(IndexOptions.HNSW_EF_REPAIR, "64")
                            .put(IndexOptions.HNSW_EXTEND_CANDIDATES, "false")
                            .put(IndexOptions.HNSW_KEEP_PRUNED_CONNECTIONS, "false")
                            .put(IndexOptions.VECTOR_USE_RABITQ, "false")
                            .put(IndexOptions.VECTOR_RABITQ_NUM_EX_BITS, "4")

                            // these are allowed to change in any way
                            .put(IndexOptions.VECTOR_SAMPLE_VECTOR_STATS_PROBABILITY, "0.999")
                            .put(IndexOptions.VECTOR_MAINTAIN_STATS_PROBABILITY, "0.78")
                            .put(IndexOptions.VECTOR_STATS_THRESHOLD, "500")
                            .put(IndexOptions.HNSW_MAX_NUM_CONCURRENT_NODE_FETCHES, "17")
                            .put(IndexOptions.HNSW_MAX_NUM_CONCURRENT_NEIGHBORHOOD_FETCHES, "9")
                            .put(IndexOptions.HNSW_MAX_NUM_CONCURRENT_DELETE_FROM_LAYER, "5").build());

            assertInvalidOptionsEvolution(metaData, index,
                    ImmutableMap.of(IndexOptions.VECTOR_NUM_DIMENSIONS, "128",
                            IndexOptions.VECTOR_METRIC, Metric.EUCLIDEAN_SQUARE_METRIC.name()));

            assertInvalidOptionsEvolution(metaData, index,
                    ImmutableMap.of(IndexOptions.VECTOR_NUM_DIMENSIONS, "768"));

            // switching the engine is never allowed
            assertInvalidOptionsEvolution(metaData, index,
                    ImmutableMap.of(IndexOptions.VECTOR_NUM_DIMENSIONS, "128",
                            IndexOptions.VECTOR_ENGINE, VectorIndexEngineKind.GUARDIANN.name()));

            assertInvalidOptionsEvolution(metaData, index,
                    ImmutableMap.of(IndexOptions.VECTOR_NUM_DIMENSIONS, "128",
                            IndexOptions.HNSW_USE_INLINING, "true"));

            assertInvalidOptionsEvolution(metaData, index,
                    ImmutableMap.of(IndexOptions.VECTOR_NUM_DIMENSIONS, "128",
                            IndexOptions.HNSW_M, "8"));

            assertInvalidOptionsEvolution(metaData, index,
                    ImmutableMap.of(IndexOptions.VECTOR_NUM_DIMENSIONS, "128",
                            IndexOptions.HNSW_M_MAX, "8"));

            assertInvalidOptionsEvolution(metaData, index,
                    ImmutableMap.of(IndexOptions.VECTOR_NUM_DIMENSIONS, "128",
                            IndexOptions.HNSW_M_MAX_0, "16"));

            assertInvalidOptionsEvolution(metaData, index,
                    ImmutableMap.of(IndexOptions.VECTOR_NUM_DIMENSIONS, "128",
                            IndexOptions.HNSW_EF_CONSTRUCTION, "500"));

            assertInvalidOptionsEvolution(metaData, index,
                    ImmutableMap.of(IndexOptions.VECTOR_NUM_DIMENSIONS, "128",
                            IndexOptions.HNSW_EF_REPAIR, "500"));

            assertInvalidOptionsEvolution(metaData, index,
                    ImmutableMap.of(IndexOptions.VECTOR_NUM_DIMENSIONS, "128",
                            IndexOptions.HNSW_EXTEND_CANDIDATES, "true"));

            assertInvalidOptionsEvolution(metaData, index,
                    ImmutableMap.of(IndexOptions.VECTOR_NUM_DIMENSIONS, "128",
                            IndexOptions.HNSW_KEEP_PRUNED_CONNECTIONS, "true"));

            assertInvalidOptionsEvolution(metaData, index,
                    ImmutableMap.of(IndexOptions.VECTOR_NUM_DIMENSIONS, "128",
                            IndexOptions.VECTOR_USE_RABITQ, "true"));

            assertInvalidOptionsEvolution(metaData, index,
                    ImmutableMap.of(IndexOptions.VECTOR_NUM_DIMENSIONS, "128",
                            IndexOptions.VECTOR_RABITQ_NUM_EX_BITS, "1"));
        }
    }
}
