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
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyWithValueExpression;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainer;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainerFactory;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainerState;
import com.google.auto.service.AutoService;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.Set;
import java.util.function.Function;

/**
 * A factory for {@link VectorIndexMaintainer} index maintainers.
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
        return new VectorIndexValidator(index);
    }

    @Override
    @Nonnull
    public IndexMaintainer getIndexMaintainer(@Nonnull final IndexMaintainerState state) {
        return new VectorIndexMaintainer(state);
    }

    /**
     * Index validator for HNSW-based vector indexes.
     */
    private static class VectorIndexValidator extends IndexValidator {
        public VectorIndexValidator(final Index index) {
            super(index);
        }

        @Override
        public void validate(@Nonnull MetaDataValidator metaDataValidator) {
            super.validate(metaDataValidator);
            validateStructure();

            try {
                VectorIndexHelper.getConfig(index);
            } catch (final IllegalArgumentException illegalArgumentException) {
                throw new MetaDataException("incorrect index options", illegalArgumentException);
            }
        }

        /**
         * Validates the key expression structure of a vector index.
         * <p>
         * The root expression must be a {@link KeyWithValueExpression}. Its split point divides the columns:
         * <ul>
         * <li>columns before the split point: index prefix (for partitioning)</li>
         * <li>columns after the split point: vector column followed by optional covering columns</li>
         * </ul>
         * The first column after the split point is always the vector column, so at least one column
         * is required after the split point. There are some other structural requirements to the root key
         * expression of a vector index and some general requirements to the index itself:
         * <ul>
         * <li>the root key expression must not contain a grouping key expression</li>
         * <li>the index must not be unique</li>
         * <li>the index must not contain any version columns</li>
         * </ul>
         * <p>
         * TODO: Currently only exactly one column after the split point is supported (no covering columns yet).
         */
        private void validateStructure() {
            validateNotGrouping();
            validateNotUnique();
            validateNotVersion();

            final KeyExpression key = index.getRootExpression();
            if (!(key instanceof KeyWithValueExpression)) {
                throw new KeyExpression.InvalidExpressionException(
                        "vector index type must use top key with value expression",
                        LogMessageKeys.INDEX_TYPE, index.getType(),
                        LogMessageKeys.INDEX_NAME, index.getName(),
                        LogMessageKeys.INDEX_KEY, index.getRootExpression());
            }
            if (key.createsDuplicates()) {
                throw new KeyExpression.InvalidExpressionException(
                        "fan outs not supported in index type",
                        LogMessageKeys.INDEX_TYPE, index.getType(),
                        LogMessageKeys.INDEX_NAME, index.getName(),
                        LogMessageKeys.INDEX_KEY, index.getRootExpression());
            }
        }

        @Override
        @SuppressWarnings("PMD.CompareObjectsWithEquals")
        public void validateChangedOptions(@Nonnull final Index oldIndex,
                                           @Nonnull final Set<String> changedOptions) {
            if (!changedOptions.isEmpty()) {
                final Config oldOptions = VectorIndexHelper.getConfig(oldIndex);
                final Config newOptions = VectorIndexHelper.getConfig(index);

                // do not allow changing any of the following
                disallowChange(changedOptions, IndexOptions.HNSW_DETERMINISTIC_SEEDING,
                        oldOptions, newOptions, Config::isDeterministicSeeding);
                disallowChange(changedOptions, IndexOptions.HNSW_METRIC,
                        oldOptions, newOptions, Config::getMetric);
                disallowChange(changedOptions, IndexOptions.HNSW_NUM_DIMENSIONS,
                        oldOptions, newOptions, Config::getNumDimensions);
                disallowChange(changedOptions, IndexOptions.HNSW_USE_INLINING,
                        oldOptions, newOptions, Config::isUseInlining);
                disallowChange(changedOptions, IndexOptions.HNSW_M,
                        oldOptions, newOptions, Config::getM);
                disallowChange(changedOptions, IndexOptions.HNSW_M_MAX,
                        oldOptions, newOptions, Config::getMMax);
                disallowChange(changedOptions, IndexOptions.HNSW_M_MAX_0,
                        oldOptions, newOptions, Config::getMMax0);
                disallowChange(changedOptions, IndexOptions.HNSW_EF_CONSTRUCTION,
                        oldOptions, newOptions, Config::getEfConstruction);
                disallowChange(changedOptions, IndexOptions.HNSW_EXTEND_CANDIDATES,
                        oldOptions, newOptions, Config::isExtendCandidates);
                disallowChange(changedOptions, IndexOptions.HNSW_KEEP_PRUNED_CONNECTIONS,
                        oldOptions, newOptions, Config::isKeepPrunedConnections);
                disallowChange(changedOptions, IndexOptions.HNSW_USE_RABITQ,
                        oldOptions, newOptions, Config::isUseRaBitQ);
                disallowChange(changedOptions, IndexOptions.HNSW_RABITQ_NUM_EX_BITS,
                        oldOptions, newOptions, Config::getRaBitQNumExBits);

                // The following index options can be changed.
                changedOptions.remove(IndexOptions.HNSW_SAMPLE_VECTOR_STATS_PROBABILITY);
                changedOptions.remove(IndexOptions.HNSW_MAINTAIN_STATS_PROBABILITY);
                changedOptions.remove(IndexOptions.HNSW_STATS_THRESHOLD);
                changedOptions.remove(IndexOptions.HNSW_MAX_NUM_CONCURRENT_NODE_FETCHES);
                changedOptions.remove(IndexOptions.HNSW_MAX_NUM_CONCURRENT_NEIGHBORHOOD_FETCHES);
            }
            super.validateChangedOptions(oldIndex, changedOptions);
        }

        private <T> void disallowChange(@Nonnull final Set<String> changedOptions,
                                        @Nonnull final String optionName,
                                        @Nonnull final Config oldConfig, @Nonnull final Config newConfig,
                                        Function<Config, T> extractorFunction) {
            if (changedOptions.contains(optionName)) {
                final T oldValue = extractorFunction.apply(oldConfig);
                final T newValue = extractorFunction.apply(newConfig);
                if (!oldValue.equals(newValue)) {
                    throw new MetaDataException("attempted to change " + optionName +
                            " from " + oldValue + " to " + newValue,
                            LogMessageKeys.INDEX_NAME, index.getName());
                }
                changedOptions.remove(optionName);
            }
        }
    }
}
