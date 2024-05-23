/*
 * RankIndexMaintainerFactory.java
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
import com.apple.foundationdb.async.rtree.RTree;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexOptions;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.IndexValidator;
import com.apple.foundationdb.record.metadata.MetaDataException;
import com.apple.foundationdb.record.metadata.MetaDataValidator;
import com.apple.foundationdb.record.metadata.expressions.DimensionsKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyWithValueExpression;
import com.apple.foundationdb.record.metadata.expressions.ThenKeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainer;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainerFactory;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainerState;
import com.google.auto.service.AutoService;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.Objects;
import java.util.Set;

/**
 * A factory for {@link MultidimensionalIndexMaintainer} indexes.
 */
@AutoService(IndexMaintainerFactory.class)
@API(API.Status.EXPERIMENTAL)
public class MultidimensionalIndexMaintainerFactory implements IndexMaintainerFactory {
    static final String[] TYPES = { IndexTypes.MULTIDIMENSIONAL};

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
                validateNotGrouping();
                validateNotVersion();
                validateStructure();
            }

            /**
             * Validate the structure of the {@link KeyExpression} associated with a multidimensional index.
             * A multidimensional index consists of prefix, dimensions, and suffix key parts which are declared
             * by a {@link DimensionsKeyExpression}. A {@link DimensionsKeyExpression} in turn holds a prefix size and
             * a dimensions size integer which function as split points of its one child key expression (much like
             * the split point in {@link KeyWithValueExpression}).
             * Neither prefix, dimensions, nor suffix can contain any value parts. Thus, if covering value-only parts
             * are needed, they can be added outside the {@link DimensionsKeyExpression} using a top
             * {@link KeyWithValueExpression}:
             * {@code KeyWithValue(ThenKey(DimensionsKey(ThenKey(...), 1, 2), valueExpr), 4)}.
             * Note that when a {@link KeyWithValueExpression} is used, the column size of the
             * {@link DimensionsKeyExpression} must be equal to the split point of the {@link KeyWithValueExpression}.
             */
            private void validateStructure() {
                // We allow for an optional KeyWithValueExpression
                KeyExpression key = index.getRootExpression();
                if (key instanceof KeyWithValueExpression) {
                    final KeyWithValueExpression keyWithValueExpression = (KeyWithValueExpression)key;

                    //
                    // Find the DimensionsKeyExpression that must cover the entire key part.
                    //
                    key = keyWithValueExpression.getInnerKey();
                    while (key instanceof ThenKeyExpression)  {
                        key = ((ThenKeyExpression)key).getChildren().get(0);
                    }

                    if (!(key instanceof DimensionsKeyExpression)) {
                        throw new KeyExpression.InvalidExpressionException(
                                "no dimensions key expression or at incorrect place in index",
                                LogMessageKeys.INDEX_TYPE, index.getType(),
                                LogMessageKeys.INDEX_NAME, index.getName(),
                                LogMessageKeys.INDEX_KEY, index.getRootExpression());
                    }

                    final DimensionsKeyExpression dimensionsKeyExpression = (DimensionsKeyExpression)key;
                    validateDimensions(dimensionsKeyExpression);

                    if (dimensionsKeyExpression.getColumnSize() != keyWithValueExpression.getSplitPoint()) {
                        throw new KeyExpression.InvalidExpressionException(
                                "dimensions key expression must cover exactly all key parts in index",
                                LogMessageKeys.INDEX_TYPE, index.getType(),
                                LogMessageKeys.INDEX_NAME, index.getName(),
                                LogMessageKeys.INDEX_KEY, index.getRootExpression());
                    }
                } else {
                    // If there is not KeyWithValueExpression, DimensionsKeyExpression is the only other option.
                    if (!(key instanceof DimensionsKeyExpression)) {
                        throw new KeyExpression.InvalidExpressionException(
                                "no dimensions key expression or at incorrect place in index",
                                LogMessageKeys.INDEX_TYPE, index.getType(),
                                LogMessageKeys.INDEX_NAME, index.getName(),
                                LogMessageKeys.INDEX_KEY, index.getRootExpression());
                    }
                    validateDimensions((DimensionsKeyExpression)key);
                }
            }

            private void validateDimensions(@Nonnull final DimensionsKeyExpression dimensionsKeyExpression) {
                if (dimensionsKeyExpression.getPrefixSize() + dimensionsKeyExpression.getDimensionsSize() >
                        dimensionsKeyExpression.getColumnSize()) {
                    throw new KeyExpression.InvalidExpressionException(
                            "dimensions key expression declares wider prefix/dimensions than it covers in index",
                            LogMessageKeys.INDEX_TYPE, index.getType(),
                            LogMessageKeys.INDEX_NAME, index.getName(),
                            LogMessageKeys.INDEX_KEY, index.getRootExpression());
                }
            }

            @Override
            public void validateChangedOptions(@Nonnull Index oldIndex, @Nonnull Set<String> changedOptions) {
                if (!changedOptions.isEmpty()) {
                    // Allow changing from unspecified to the default (or vice versa), but not otherwise.
                    final RTree.Config oldOptions = MultiDimensionalIndexHelper.getConfig(oldIndex);
                    final RTree.Config newOptions = MultiDimensionalIndexHelper.getConfig(index);
                    if (changedOptions.contains(IndexOptions.RTREE_MIN_M)) {
                        if (oldOptions.getMinM() != newOptions.getMinM()) {
                            throw new MetaDataException("rtree minM changed",
                                    LogMessageKeys.INDEX_NAME, index.getName());
                        }
                        changedOptions.remove(IndexOptions.RTREE_MIN_M);
                    }
                    if (changedOptions.contains(IndexOptions.RTREE_MAX_M)) {
                        if (oldOptions.getMaxM() != newOptions.getMaxM()) {
                            throw new MetaDataException("rtree minM changed",
                                    LogMessageKeys.INDEX_NAME, index.getName());
                        }
                        changedOptions.remove(IndexOptions.RTREE_MAX_M);
                    }
                    if (changedOptions.contains(IndexOptions.RTREE_SPLIT_S)) {
                        if (oldOptions.getSplitS() != newOptions.getSplitS()) {
                            throw new MetaDataException("rtree splitS changed",
                                    LogMessageKeys.INDEX_NAME, index.getName());
                        }
                        changedOptions.remove(IndexOptions.RTREE_SPLIT_S);
                    }
                    if (changedOptions.contains(IndexOptions.RTREE_STORAGE)) {
                        if (!Objects.equals(oldOptions.getStorage(), newOptions.getStorage())) {
                            throw new MetaDataException("rtree storage changed",
                                    LogMessageKeys.INDEX_NAME, index.getName());
                        }
                        changedOptions.remove(IndexOptions.RTREE_STORAGE);
                    }
                    if (changedOptions.contains(IndexOptions.RTREE_STORE_HILBERT_VALUES)) {
                        if (oldOptions.isStoreHilbertValues() != newOptions.isStoreHilbertValues()) {
                            throw new MetaDataException("rtree store Hilbert values changed",
                                    LogMessageKeys.INDEX_NAME, index.getName());
                        }
                        changedOptions.remove(IndexOptions.RTREE_STORE_HILBERT_VALUES);
                    }
                    if (changedOptions.contains(IndexOptions.RTREE_USE_NODE_SLOT_INDEX)) {
                        if (oldOptions.isUseNodeSlotIndex() != newOptions.isUseNodeSlotIndex()) {
                            throw new MetaDataException("rtree use node slot index changed",
                                    LogMessageKeys.INDEX_NAME, index.getName());
                        }
                        changedOptions.remove(IndexOptions.RTREE_USE_NODE_SLOT_INDEX);
                    }
                }
                super.validateChangedOptions(oldIndex, changedOptions);
            }
        };
    }

    @Override
    @Nonnull
    public IndexMaintainer getIndexMaintainer(@Nonnull final IndexMaintainerState state) {
        return new MultidimensionalIndexMaintainer(state);
    }
}
