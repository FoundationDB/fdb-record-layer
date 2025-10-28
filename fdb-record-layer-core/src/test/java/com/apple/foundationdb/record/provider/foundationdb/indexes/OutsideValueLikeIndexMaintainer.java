/*
 * OutsideValueLikeIndexMaintainer.java
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

import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexValidator;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainer;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainerFactory;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainerState;
import com.apple.foundationdb.record.query.plan.cascades.MatchCandidate;
import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nonnull;
import java.util.Set;

/**
 * Test class that is supposed to mock an index that behaves like a
 * {@link com.apple.foundationdb.record.metadata.IndexTypes#VALUE} index but
 * is not defined within the main code. In a "real" use case of this, there may
 * be something special that the index maintainer does that differentiates it
 * from the built-in index type (e.g., it may interface with an external system
 * or have some kind of special storage or what have you). But if it looks to
 * the query planner like a regular value index, we should nevertheless be
 * to surface it to the planners.
 */
public class OutsideValueLikeIndexMaintainer extends ValueIndexMaintainer {
    @Nonnull
    public static final String INDEX_TYPE = "outside_value";

    public OutsideValueLikeIndexMaintainer(final IndexMaintainerState state) {
        super(state);
    }

    /**
     * Factory for creating these index types.
     */
    @AutoService(IndexMaintainerFactory.class)
    public static class Factory implements IndexMaintainerFactory {
        @Nonnull
        private static final Set<String> INDEX_TYPES = ImmutableSet.of(INDEX_TYPE);
        private static final IndexMaintainerFactory underlying = new ValueIndexMaintainerFactory();

        @Nonnull
        @Override
        public Iterable<String> getIndexTypes() {
            return INDEX_TYPES;
        }

        @Nonnull
        @Override
        public IndexValidator getIndexValidator(final Index index) {
            // Delegate to the value index type.
            return underlying.getIndexValidator(index);
        }

        @Nonnull
        @Override
        public IndexMaintainer getIndexMaintainer(@Nonnull final IndexMaintainerState state) {
            // Do not delegate here. Create the new custom index maintainer type.
            // (Though as can be seen above, the methods here more-or-less all delegate
            // to the value index implementation.)
            return new OutsideValueLikeIndexMaintainer(state);
        }

        @Nonnull
        @Override
        public Iterable<MatchCandidate> createMatchCandidates(@Nonnull final RecordMetaData metaData, @Nonnull final Index index, final boolean reverse) {
            // Delegate to the value index type.
            return underlying.createMatchCandidates(metaData, index, reverse);
        }
    }
}
