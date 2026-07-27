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
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexOptions;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.IndexValidator;
import com.apple.foundationdb.record.metadata.MetaDataException;
import com.apple.foundationdb.record.metadata.MetaDataValidator;
import com.apple.foundationdb.record.metadata.expressions.KeyWithValueExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainerFactory;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainerFactoryRegistry;
import com.google.common.collect.ImmutableMap;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.util.Map;
import java.util.Objects;

import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;

/**
 * Vector index tests against the HNSW engine. The behavioral scenarios are inherited from
 * {@link VectorIndexEngineTestSuite}; this class pins the engine to HNSW and adds the HNSW-specific option-evolution
 * validation.
 */
class HnswVectorIndexTest extends VectorIndexEngineTestSuite {

    @Nonnull
    @Override
    protected Map<String, String> indexOptions() {
        return ImmutableMap.of(IndexOptions.VECTOR_ENGINE, VectorIndexEngine.Kind.HNSW.name(),
                IndexOptions.VECTOR_METRIC, Metric.EUCLIDEAN_METRIC.name(),
                IndexOptions.VECTOR_NUM_DIMENSIONS, "128");
    }

    @Test
    void directIndexValidatorTest() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, this::addGroupedVectorIndex);

            final Index index =
                    Objects.requireNonNull(recordStore.getMetaDataProvider())
                            .getRecordMetaData().getIndex("GroupedVectorIndex");
            final IndexMaintainerFactoryRegistry indexMaintainerRegistry = recordStore.getIndexMaintainerRegistry();
            final MetaDataValidator metaDataValidator =
                    new MetaDataValidator(recordStore.getRecordMetaData(), indexMaintainerRegistry);
            metaDataValidator.validate();

            // validate the allowed changes all at once
            validateIndexEvolution(metaDataValidator, index,
                    ImmutableMap.<String, String>builder()
                            .put(IndexOptions.VECTOR_ENGINE, VectorIndexEngine.Kind.HNSW.name())
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

            Assertions.assertThatThrownBy(() -> validateIndexEvolution(metaDataValidator, index,
                    ImmutableMap.of(IndexOptions.VECTOR_NUM_DIMENSIONS, "128",
                            IndexOptions.VECTOR_METRIC, Metric.EUCLIDEAN_SQUARE_METRIC.name())))
                    .isInstanceOf(MetaDataException.class);

            Assertions.assertThatThrownBy(() -> validateIndexEvolution(metaDataValidator, index,
                    ImmutableMap.of(IndexOptions.VECTOR_NUM_DIMENSIONS, "768")))
                    .isInstanceOf(MetaDataException.class);

            // switching the engine is never allowed
            Assertions.assertThatThrownBy(() -> validateIndexEvolution(metaDataValidator, index,
                    ImmutableMap.of(IndexOptions.VECTOR_NUM_DIMENSIONS, "128",
                            IndexOptions.VECTOR_ENGINE, VectorIndexEngine.Kind.GUARDIANN.name())))
                    .isInstanceOf(MetaDataException.class);

            Assertions.assertThatThrownBy(() -> validateIndexEvolution(metaDataValidator, index,
                    ImmutableMap.of(IndexOptions.VECTOR_NUM_DIMENSIONS, "128",
                            IndexOptions.HNSW_USE_INLINING, "true"))).isInstanceOf(MetaDataException.class);

            Assertions.assertThatThrownBy(() -> validateIndexEvolution(metaDataValidator, index,
                    ImmutableMap.of(IndexOptions.VECTOR_NUM_DIMENSIONS, "128",
                            IndexOptions.HNSW_M, "8"))).isInstanceOf(MetaDataException.class);

            Assertions.assertThatThrownBy(() -> validateIndexEvolution(metaDataValidator, index,
                    ImmutableMap.of(IndexOptions.VECTOR_NUM_DIMENSIONS, "128",
                            IndexOptions.HNSW_M_MAX, "8"))).isInstanceOf(MetaDataException.class);

            Assertions.assertThatThrownBy(() -> validateIndexEvolution(metaDataValidator, index,
                    ImmutableMap.of(IndexOptions.VECTOR_NUM_DIMENSIONS, "128",
                            IndexOptions.HNSW_M_MAX_0, "16"))).isInstanceOf(MetaDataException.class);

            Assertions.assertThatThrownBy(() -> validateIndexEvolution(metaDataValidator, index,
                    ImmutableMap.of(IndexOptions.VECTOR_NUM_DIMENSIONS, "128",
                            IndexOptions.HNSW_EF_CONSTRUCTION, "500"))).isInstanceOf(MetaDataException.class);

            Assertions.assertThatThrownBy(() -> validateIndexEvolution(metaDataValidator, index,
                    ImmutableMap.of(IndexOptions.VECTOR_NUM_DIMENSIONS, "128",
                            IndexOptions.HNSW_EF_REPAIR, "500"))).isInstanceOf(MetaDataException.class);

            Assertions.assertThatThrownBy(() -> validateIndexEvolution(metaDataValidator, index,
                    ImmutableMap.of(IndexOptions.VECTOR_NUM_DIMENSIONS, "128",
                            IndexOptions.HNSW_EXTEND_CANDIDATES, "true"))).isInstanceOf(MetaDataException.class);

            Assertions.assertThatThrownBy(() -> validateIndexEvolution(metaDataValidator, index,
                    ImmutableMap.of(IndexOptions.VECTOR_NUM_DIMENSIONS, "128",
                            IndexOptions.HNSW_KEEP_PRUNED_CONNECTIONS, "true")))
                    .isInstanceOf(MetaDataException.class);

            Assertions.assertThatThrownBy(() -> validateIndexEvolution(metaDataValidator, index,
                    ImmutableMap.of(IndexOptions.VECTOR_NUM_DIMENSIONS, "128",
                            IndexOptions.VECTOR_USE_RABITQ, "true"))).isInstanceOf(MetaDataException.class);

            Assertions.assertThatThrownBy(() -> validateIndexEvolution(metaDataValidator, index,
                    ImmutableMap.of(IndexOptions.VECTOR_NUM_DIMENSIONS, "128",
                            IndexOptions.VECTOR_RABITQ_NUM_EX_BITS, "1"))).isInstanceOf(MetaDataException.class);
        }
    }

    private void validateIndexEvolution(@Nonnull final MetaDataValidator metaDataValidator,
                                        @Nonnull final Index oldIndex, @Nonnull final Map<String, String> optionsMap) {
        final Index newIndex =
                new Index("GroupedVectorIndex",
                        new KeyWithValueExpression(concat(field("group_id"), field("vector_data")), 1),
                        IndexTypes.VECTOR,
                        optionsMap);

        final IndexMaintainerFactoryRegistry indexMaintainerRegistry = recordStore.getIndexMaintainerRegistry();
        final IndexMaintainerFactory indexMaintainerFactory =
                indexMaintainerRegistry.getIndexMaintainerFactory(oldIndex);

        final IndexValidator validatorForCompatibleNewIndex =
                indexMaintainerFactory.getIndexValidator(newIndex);
        validatorForCompatibleNewIndex.validate(metaDataValidator);
        validatorForCompatibleNewIndex.validateChangedOptions(oldIndex);
    }
}
