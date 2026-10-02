/*
 * SyntheticTableGeneratorFactory.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2026 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.recordlayer.query.ddl;

import javax.annotation.Nonnull;
import java.util.Optional;

/**
 * Picks the {@link SyntheticTableGenerator} a definition needs, if it needs one. The only place that knows which kinds
 * exist, so adding one is a change here rather than in the generator that routes to it.
 */
final class SyntheticTableGeneratorFactory {

    private SyntheticTableGeneratorFactory() {
    }

    /**
     * The generator for whichever kind of synthetic table the definition needs.
     *
     * @param spec what the index is made of
     * @param indexName the name the definition gives the index
     * @param quantifierValues what the plan's quantifiers stand for
     *
     * @return the generator, empty when the index is maintained from a stored table
     */
    @Nonnull
    static Optional<? extends SyntheticTableGenerator> forDefinition(@Nonnull final IndexSpec spec,
                                                                     @Nonnull final String indexName,
                                                                     @Nonnull final QuantifierValues quantifierValues) {
        // An unnesting only needs a synthetic table when the key reads through one in a way a fan-out cannot
        // express, which is for the unnested generator to say.
        return RecordLayerUnnestedSyntheticTableGenerator.initIfNeeded(spec, indexName, quantifierValues);
    }
}
