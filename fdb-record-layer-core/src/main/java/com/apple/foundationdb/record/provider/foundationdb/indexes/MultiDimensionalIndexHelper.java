/*
 * MultiDimensionalIndexHelper.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2023 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexOptions;
import com.apple.foundationdb.record.provider.common.StoreTimer;

import javax.annotation.Nonnull;

/**
 * Helper functions for index maintainers that use a {@link RTree}.
 */
@API(API.Status.EXPERIMENTAL)
public class MultiDimensionalIndexHelper {
    private MultiDimensionalIndexHelper() {
    }

    /**
     * Parse standard options into {@link RTree.Config}.
     * @param index the index definition to get options from
     * @return parsed config options
     */
    public static RTree.Config getConfig(@Nonnull final Index index) {
        final RTree.ConfigBuilder builder = RTree.newConfigBuilder();
        final String rtreeMinMOption = index.getOption(IndexOptions.RTREE_MIN_M);
        if (rtreeMinMOption != null) {
            builder.setMinM(Integer.parseInt(rtreeMinMOption));
        }
        final String rtreeMaxMOption = index.getOption(IndexOptions.RTREE_MAX_M);
        if (rtreeMaxMOption != null) {
            builder.setMaxM(Integer.parseInt(rtreeMaxMOption));
        }
        final String rtreeSplitS = index.getOption(IndexOptions.RTREE_SPLIT_S);
        if (rtreeSplitS != null) {
            builder.setSplitS(Integer.parseInt(rtreeSplitS));
        }
        final String rtreeStorage = index.getOption(IndexOptions.RTREE_STORAGE);
        if (rtreeStorage != null) {
            builder.setStorage(RTree.Storage.valueOf(rtreeStorage));
        }
        final String rtreeStoreHilbertValues = index.getOption(IndexOptions.RTREE_STORE_HILBERT_VALUES);
        if (rtreeStorage != null) {
            builder.setStoreHilbertValues(Boolean.parseBoolean(rtreeStoreHilbertValues));
        }
        final String rtreeUseNodeSlotIndex = index.getOption(IndexOptions.RTREE_USE_NODE_SLOT_INDEX);
        if (rtreeUseNodeSlotIndex != null) {
            builder.setUseNodeSlotIndex(Boolean.parseBoolean(rtreeUseNodeSlotIndex));
        }

        return builder.build();
    }

    /**
     * Instrumentation events specific to R-tree index maintenance.
     */
    public enum Events implements StoreTimer.DetailEvent {
        MULTIDIMENSIONAL_SCAN("scannig the R-tree of a multidimensional index"),
        MULTIDIMENSIONAL_SKIP_SCAN("skip scan the prefix tuples of a multidimensional scan"),
        MULTIDIMENSIONAL_MODIFICATION("modifying the R-tree of a multidimensional index");

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
