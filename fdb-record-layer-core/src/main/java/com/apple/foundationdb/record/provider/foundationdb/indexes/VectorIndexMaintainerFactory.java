/*
 * VectorIndexMaintainerFactory.java
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
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexOptions;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.IndexValidator;
import com.apple.foundationdb.record.metadata.MetaDataException;
import com.apple.foundationdb.record.metadata.MetaDataValidator;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainer;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainerFactory;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainerState;
import com.google.auto.service.AutoService;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.Set;

/**
 * A factory for {@link VectorIndexMaintainer} indexes.
 */
@AutoService(IndexMaintainerFactory.class)
@API(API.Status.EXPERIMENTAL)
public class VectorIndexMaintainerFactory implements IndexMaintainerFactory {
    static final String[] TYPES = { IndexTypes.VECTOR };

    @Override
    @Nonnull
    public Iterable<String> getIndexTypes() {
        return Arrays.asList(TYPES);
    }

    @Override
    @Nonnull
    public IndexValidator getIndexValidator(Index index) {
        return new IndexValidator(index) {
            @Override
            public void validate(@Nonnull MetaDataValidator metaDataValidator) {
                super.validate(metaDataValidator);
                validateNotVersion();
                validateStructure();
            }

            /**
             * TODO.
             */
            private void validateStructure() {
                //
                // There is no structural constraint on the key expression of the index. We just happen to interpret
                // things in specific ways:
                //
                // - without GroupingKeyExpression:
                //   - one HNSW for the entire table (ungrouped HNSW)
                //   - first column of the expression gives us access to the field containing the vector
                // - with GroupingKeyExpression:
                //   - one HNSW for each grouping prefix
                //   - first column in the grouped expression gives us access to the field containing the vector
                //
                // In any case, the vector is always a half-precision-encoded vector of dimensionality
                // blob.length / 2 (for now).
                //
                // TODO We do not support extraneous columns to support advanced covering index scans for now. That
                //      Will probably encoded by a KeyWithValueExpression in the root position (but not now)
                //
            }

            @Override
            @SuppressWarnings("PMD.CompareObjectsWithEquals")
            public void validateChangedOptions(@Nonnull final Index oldIndex,
                                               @Nonnull final Set<String> changedOptions) {
                if (!changedOptions.isEmpty()) {
                    // Allow changing from unspecified to the default (or vice versa), but not otherwise.
                    final Config oldOptions = VectorIndexHelper.getConfig(oldIndex);
                    final Config newOptions = VectorIndexHelper.getConfig(index);

                    if (changedOptions.contains(IndexOptions.HNSW_RANDOM_SEED)) {
                        if (oldOptions.getRandomSeed() != newOptions.getRandomSeed()) {
                            throw new MetaDataException("HNSW random seed changed",
                                    LogMessageKeys.INDEX_NAME, index.getName());
                        }
                        changedOptions.remove(IndexOptions.HNSW_RANDOM_SEED);
                    }
                    if (changedOptions.contains(IndexOptions.HNSW_METRIC)) {
                        if (oldOptions.getMetric() != newOptions.getMetric()) {
                            throw new MetaDataException("HNSW metric changed",
                                    LogMessageKeys.INDEX_NAME, index.getName());
                        }
                        changedOptions.remove(IndexOptions.HNSW_METRIC);
                    }
                    if (changedOptions.contains(IndexOptions.HNSW_NUM_DIMENSIONS)) {
                        if (oldOptions.getNumDimensions() != newOptions.getNumDimensions()) {
                            throw new MetaDataException("HNSW numDimensions changed",
                                    LogMessageKeys.INDEX_NAME, index.getName());
                        }
                        changedOptions.remove(IndexOptions.HNSW_NUM_DIMENSIONS);
                    }
                    if (changedOptions.contains(IndexOptions.HNSW_USE_INLINING)) {
                        if (oldOptions.isUseInlining() != newOptions.isUseInlining()) {
                            throw new MetaDataException("HNSW useInlining changed",
                                    LogMessageKeys.INDEX_NAME, index.getName());
                        }
                        changedOptions.remove(IndexOptions.HNSW_USE_INLINING);
                    }
                    if (changedOptions.contains(IndexOptions.HNSW_M)) {
                        if (oldOptions.getM() != newOptions.getM()) {
                            throw new MetaDataException("HNSW M changed",
                                    LogMessageKeys.INDEX_NAME, index.getName());
                        }
                        changedOptions.remove(IndexOptions.HNSW_M);
                    }
                    if (changedOptions.contains(IndexOptions.HNSW_M_MAX)) {
                        if (oldOptions.getMMax() != newOptions.getMMax()) {
                            throw new MetaDataException("HNSW mMax changed",
                                    LogMessageKeys.INDEX_NAME, index.getName());
                        }
                        changedOptions.remove(IndexOptions.HNSW_M_MAX);
                    }
                    if (changedOptions.contains(IndexOptions.HNSW_M_MAX_0)) {
                        if (oldOptions.getMMax0() != newOptions.getMMax0()) {
                            throw new MetaDataException("HNSW mMax0 changed",
                                    LogMessageKeys.INDEX_NAME, index.getName());
                        }
                        changedOptions.remove(IndexOptions.HNSW_M_MAX_0);
                    }
                    if (changedOptions.contains(IndexOptions.HNSW_EF_CONSTRUCTION)) {
                        if (oldOptions.getEfConstruction() != newOptions.getEfConstruction()) {
                            throw new MetaDataException("HNSW efConstruction changed",
                                    LogMessageKeys.INDEX_NAME, index.getName());
                        }
                        changedOptions.remove(IndexOptions.HNSW_EF_CONSTRUCTION);
                    }
                    if (changedOptions.contains(IndexOptions.HNSW_EXTEND_CANDIDATES)) {
                        if (oldOptions.isExtendCandidates() != newOptions.isExtendCandidates()) {
                            throw new MetaDataException("HNSW extendCandidates changed",
                                    LogMessageKeys.INDEX_NAME, index.getName());
                        }
                        changedOptions.remove(IndexOptions.HNSW_EXTEND_CANDIDATES);
                    }
                    if (changedOptions.contains(IndexOptions.HNSW_KEEP_PRUNED_CONNECTIONS)) {
                        if (oldOptions.isKeepPrunedConnections() != newOptions.isKeepPrunedConnections()) {
                            throw new MetaDataException("HNSW keepPrunedConnections changed",
                                    LogMessageKeys.INDEX_NAME, index.getName());
                        }
                        changedOptions.remove(IndexOptions.HNSW_KEEP_PRUNED_CONNECTIONS);
                    }
                    if (changedOptions.contains(IndexOptions.HNSW_USE_RABITQ)) {
                        if (oldOptions.isUseRaBitQ() != newOptions.isUseRaBitQ()) {
                            throw new MetaDataException("HNSW useRaBitQ changed",
                                    LogMessageKeys.INDEX_NAME, index.getName());
                        }
                        changedOptions.remove(IndexOptions.HNSW_USE_RABITQ);
                    }
                    if (changedOptions.contains(IndexOptions.HNSW_RABITQ_NUM_EX_BITS)) {
                        if (oldOptions.getRaBitQNumExBits() != newOptions.getRaBitQNumExBits()) {
                            throw new MetaDataException("HNSW RaBitQ numExBits changed",
                                    LogMessageKeys.INDEX_NAME, index.getName());
                        }
                        changedOptions.remove(IndexOptions.HNSW_RABITQ_NUM_EX_BITS);
                    }

                    // The following index options can be changed.
                    changedOptions.remove(IndexOptions.HNSW_SAMPLE_VECTOR_STATS_PROBABILITY);
                    changedOptions.remove(IndexOptions.HNSW_MAINTAIN_STATS_PROBABILITY);
                    changedOptions.remove(IndexOptions.HNSW_STATS_THRESHOLD);
                    changedOptions.remove(IndexOptions.HNSW_MAX_NUM_CONCURRENT_NODE_FETCHES);
                    changedOptions.remove(IndexOptions.HNSW_MAX_NUM_CONCURRENT_NEIGHBORHOOD_FETCHES);
                }
                super.validateChangedOptions(oldIndex, changedOptions);
            }
        };
    }

    @Override
    @Nonnull
    public IndexMaintainer getIndexMaintainer(@Nonnull final IndexMaintainerState state) {
        return new VectorIndexMaintainer(state);
    }
}
