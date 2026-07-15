/*
 * VectorIndexHelper.java
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
import com.apple.foundationdb.linear.Metric;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.provider.common.StoreTimer;

import javax.annotation.Nonnull;

/**
 * Helper functions and instrumentation events for vector index maintainers. The engine-specific configuration (HNSW or
 * Guardiann) is intentionally not exposed here; it is encapsulated by {@link VectorIndexEngine} and its implementations.
 */
@API(API.Status.EXPERIMENTAL)
public final class VectorIndexHelper {
    private VectorIndexHelper() {
    }

    /**
     * Parses and validates the vector engine configuration for an index. Parsing eagerly validates the options (an
     * invalid option throws), so this doubles as the config-validation entry point used by the index validator and by
     * tests that assert an index's options are acceptable. It is engine-aware: the {@code VECTOR_ENGINE} option selects
     * which engine's configuration is parsed.
     *
     * @param index the index definition to get options from
     */
    public static void validate(@Nonnull final Index index) {
        // Reject specifying any option under more than one of its (current/legacy) names before parsing, so an
        // ambiguous options map fails fast rather than silently resolving to the canonical name.
        VectorIndexOptionsHelper.validateNoAliasConflicts(index, VectorIndexOptionKeys.ALL);
        VectorIndexEngine.fromIndex(index);
    }

    /**
     * Reads the distance metric configured for a vector index. Engine-neutral and independent of which engine backs the
     * index: query planning only needs to know which distance function results are ordered by.
     *
     * @param index the index definition
     * @return the metric of the index
     */
    @Nonnull
    public static Metric getMetric(@Nonnull final Index index) {
        return VectorIndexEngine.metricFromIndex(index);
    }

    /**
     * Instrumentation events specific to vector index maintenance.
     */
    public enum Events implements StoreTimer.DetailEvent {
        VECTOR_SCAN("scanning the partition of a vector index"),
        VECTOR_SKIP_SCAN("skip scan the prefix tuples of a vector index scan");

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
