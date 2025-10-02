/*
 * NonCascadesValueIndexMaintainer.java
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
import com.google.auto.service.AutoService;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.Set;

/**
 * Simulate a value-like index that does not implement {@link IndexMaintainerFactory#createMatchCandidates(RecordMetaData, Index, boolean)}
 * and so cannot be used by the {@link com.apple.foundationdb.record.query.plan.cascades.CascadesPlanner}.
 */
public class NonCascadesValueIndexMaintainer extends ValueIndexMaintainer {
    public static final String INDEX_TYPE = "non_cascades_value";

    public NonCascadesValueIndexMaintainer(final IndexMaintainerState state) {
        super(state);
    }

    @AutoService(IndexMaintainerFactory.class)
    public static class Factory implements IndexMaintainerFactory {
        private static final Set<String> INDEX_TYPES = Collections.singleton(INDEX_TYPE);
        private static final ValueIndexMaintainerFactory underlying = new ValueIndexMaintainerFactory();

        @Nonnull
        @Override
        public Iterable<String> getIndexTypes() {
            return INDEX_TYPES;
        }

        @Nonnull
        @Override
        public IndexValidator getIndexValidator(final Index index) {
            return underlying.getIndexValidator(index);
        }

        @Nonnull
        @Override
        public IndexMaintainer getIndexMaintainer(@Nonnull final IndexMaintainerState state) {
            return new NonCascadesValueIndexMaintainer(state);
        }
    }
}
