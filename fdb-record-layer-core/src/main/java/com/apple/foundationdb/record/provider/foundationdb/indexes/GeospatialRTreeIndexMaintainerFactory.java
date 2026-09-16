/*
 * GeospatialRTreeIndexMaintainerFactory.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2026 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.IndexGeneralAttributes;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainer;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainerFactory;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainerState;
import com.google.auto.service.AutoService;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.Objects;
import java.util.Set;

/**
 * A factory for {@link GeospatialRTreeIndexMaintainer} indexes.
 */
@AutoService(IndexMaintainerFactory.class)
@API(API.Status.EXPERIMENTAL)
public class GeospatialRTreeIndexMaintainerFactory implements IndexMaintainerFactory {
    static final String[] TYPES = { IndexTypes.GEOSPATIAL_RTREE };
    private static final IndexGeneralAttributes GENERAL_ATTRIBUTES = new IndexGeneralAttributes(false);

    @Override
    @Nonnull
    public Iterable<String> getIndexTypes() {
        return Arrays.asList(TYPES);
    }

    @Override
    @Nonnull
    public IndexValidator getIndexValidator(final Index index) {
        return new IndexValidator(index) {
            @Override
            public void validate(@Nonnull final MetaDataValidator metaDataValidator) {
                super.validate(metaDataValidator);
                validateNotVersion();
                validateNotUnique();
                validateStructure();
            }

            // The columns not consumed by grouping must be exactly the two coordinate columns (latitude, longitude);
            // grouping columns, when present, partition the index into a separate R-tree per grouping tuple.
            private void validateStructure() {
                final KeyExpression root = index.getRootExpression();
                final int coordinateColumns = root.getColumnSize() - GeospatialRTreeIndexHelper.getGroupingCount(root);
                if (coordinateColumns != GeospatialRTreeIndexHelper.COORDINATE_DIMENSIONS) {
                    throw new KeyExpression.InvalidExpressionException(
                            "geospatial R-tree index requires exactly two coordinate columns following any grouping",
                            LogMessageKeys.INDEX_TYPE, index.getType(),
                            LogMessageKeys.INDEX_NAME, index.getName(),
                            LogMessageKeys.INDEX_KEY, root);
                }
            }

            @Override
            public void validateChangedOptions(@Nonnull final Index oldIndex, @Nonnull final Set<String> changedOptions) {
                if (!changedOptions.isEmpty()) {
                    // These options are baked into the stored data; changing them requires a rebuild, so reject a change
                    // that would alter the effective value. Allow toggling between unspecified and the default.
                    final RTree.Config oldConfig = GeospatialRTreeIndexHelper.getConfig(oldIndex);
                    final RTree.Config newConfig = GeospatialRTreeIndexHelper.getConfig(index);
                    checkUnchanged(changedOptions, IndexOptions.RTREE_MIN_M, oldConfig.getMinM() == newConfig.getMinM());
                    checkUnchanged(changedOptions, IndexOptions.RTREE_MAX_M, oldConfig.getMaxM() == newConfig.getMaxM());
                    checkUnchanged(changedOptions, IndexOptions.RTREE_SPLIT_S, oldConfig.getSplitS() == newConfig.getSplitS());
                    checkUnchanged(changedOptions, IndexOptions.RTREE_STORAGE,
                            Objects.equals(oldConfig.getStorage(), newConfig.getStorage()));
                    checkUnchanged(changedOptions, IndexOptions.RTREE_STORE_HILBERT_VALUES,
                            oldConfig.isStoreHilbertValues() == newConfig.isStoreHilbertValues());
                    checkUnchanged(changedOptions, IndexOptions.RTREE_USE_NODE_SLOT_INDEX,
                            oldConfig.isUseNodeSlotIndex() == newConfig.isUseNodeSlotIndex());
                    checkUnchanged(changedOptions, IndexOptions.GEOSPATIAL_RTREE_PRECISION_DIGITS,
                            GeospatialRTreeIndexHelper.getScale(oldIndex) == GeospatialRTreeIndexHelper.getScale(index));
                }
                super.validateChangedOptions(oldIndex, changedOptions);
            }

            private void checkUnchanged(@Nonnull final Set<String> changedOptions, @Nonnull final String option,
                                        final boolean unchanged) {
                if (changedOptions.contains(option)) {
                    if (!unchanged) {
                        throw new MetaDataException("geospatial R-tree index option changed",
                                LogMessageKeys.INDEX_NAME, index.getName(),
                                LogMessageKeys.INDEX_OPTION, option);
                    }
                    changedOptions.remove(option);
                }
            }
        };
    }

    @Override
    @Nonnull
    public IndexMaintainer getIndexMaintainer(@Nonnull final IndexMaintainerState state) {
        return new GeospatialRTreeIndexMaintainer(state);
    }

    @Nonnull
    @Override
    public IndexGeneralAttributes getIndexGeneralAttributes(@Nonnull final Index index) {
        return GENERAL_ATTRIBUTES;
    }
}
