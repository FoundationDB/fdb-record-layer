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
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexOptions;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.IndexValidator;
import com.apple.foundationdb.record.metadata.MetaDataException;
import com.apple.foundationdb.record.metadata.MetaDataValidator;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyWithValueExpression;
import com.apple.foundationdb.record.provider.foundationdb.IndexGeneralAttributes;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainer;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainerFactory;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainerState;
import com.apple.foundationdb.record.query.plan.cascades.ExpansionVisitor;
import com.apple.foundationdb.record.query.plan.cascades.IndexExpansionInfo;
import com.apple.foundationdb.record.query.plan.cascades.MatchCandidate;
import com.apple.foundationdb.record.query.plan.cascades.MatchCandidateExpansion;
import com.apple.foundationdb.record.query.plan.cascades.VectorIndexExpansionVisitor;
import com.google.auto.service.AutoService;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.Set;

/**
 * A factory for {@link VectorIndexMaintainer} index maintainers.
 */
@AutoService(IndexMaintainerFactory.class)
@API(API.Status.EXPERIMENTAL)
public class VectorIndexMaintainerFactory implements IndexMaintainerFactory {
    static final String[] TYPES = { IndexTypes.VECTOR };
    private static final IndexGeneralAttributes GENERAL_ATTRIBUTES = new IndexGeneralAttributes(false);

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

    @Nonnull
    @Override
    public Iterable<MatchCandidate> createMatchCandidates(@Nonnull final RecordMetaData metaData, @Nonnull final Index index, final boolean reverse) {
        final IndexExpansionInfo info = IndexExpansionInfo.createInfo(metaData, index, reverse);
        final ExpansionVisitor<?> expansionVisitor = new VectorIndexExpansionVisitor(info.getIndex(), info.getIndexedRecordTypes());
        return MatchCandidateExpansion.optionalToIterable(
                MatchCandidateExpansion.expandIndexMatchCandidate(info, false, info.getCommonPrimaryKeyForTypes(), expansionVisitor));
    }

    @Nonnull
    @Override
    public IndexGeneralAttributes getIndexGeneralAttributes(@Nonnull final Index index) {
        return GENERAL_ATTRIBUTES;
    }

    /**
     * Index validator for vector indexes, engine-agnostic. Structural validation is shared; config validation and the
     * rules for which options may change on an existing index are delegated to the {@link VectorIndexEngine} selected by
     * the index's {@link IndexOptions#VECTOR_ENGINE} option.
     */
    private static class VectorIndexValidator extends IndexValidator {
        public VectorIndexValidator(final Index index) {
            super(index);
        }

        @Override
        public void validate(@Nonnull final MetaDataValidator metaDataValidator) {
            super.validate(metaDataValidator);
            validateStructure();

            try {
                VectorIndexHelper.validate(index);
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
        public void validateChangedOptions(@Nonnull final Index oldIndex,
                                           @Nonnull final Set<String> changedOptions) {
            if (!changedOptions.isEmpty()) {
                // Let the engine handle its own options (removing the ones it accepts); anything it leaves in the set
                // is rejected by the super implementation.
                VectorIndexEngine.validateChangedOptions(oldIndex, index, changedOptions);
            }
            super.validateChangedOptions(oldIndex, changedOptions);
        }
    }
}
