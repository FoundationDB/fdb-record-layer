/*
 * RankIndexMaintainerFactory.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2018 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.async.RTree;
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
