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

import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerSchemaTemplate;
import com.apple.foundationdb.relational.util.Assert;

import javax.annotation.Nonnull;
import java.util.Optional;

/**
 * Picks the {@link SyntheticTableGenerator} a definition needs, if it needs one, and whether the schema template already
 * has the synthetic table it would generate. The only place that knows which kinds exist, so adding one is a change here
 * rather than in the generator that routes to it.
 */
final class SyntheticTableGeneratorFactory {

    private SyntheticTableGeneratorFactory() {
    }

    /**
     * The synthetic table a definition needs, if it needs one. An existing synthetic table of the schema template is
     * reused if it produces the same synthetic records, up to a renaming of their constituents.
     *
     * @param spec what the index is made of
     * @param indexName the name the definition gives the index
     * @param quantifierValues what the plan's quantifiers stand for
     * @param schemaTemplateBuilder the metadata the index is added to, searched for an existing synthetic table
     * @return the synthetic table, empty when the index is maintained from a stored table
     */
    @Nonnull
    static Optional<Resolution> resolve(@Nonnull final IndexSpec spec,
                                        @Nonnull final String indexName,
                                        @Nonnull final QuantifierValues quantifierValues,
                                        @Nonnull final RecordLayerSchemaTemplate.Builder schemaTemplateBuilder) {
        return forDefinition(spec, indexName, quantifierValues).map(generator -> {
            generator.checkSupported(spec);
            return generator.reuseMaybe(schemaTemplateBuilder)
                    .map(Resolution::ofExisting)
                    .orElseGet(() -> Resolution.ofNew(generator));
        });
    }

    /**
     * The generator for whichever kind of synthetic table the definition needs.
     *
     * @param spec what the index is made of
     * @param indexName the name the definition gives the index
     * @param quantifierValues what the plan's quantifiers stand for
     * @return the generator, empty when the index is maintained from a stored table
     */
    @Nonnull
    private static Optional<? extends SyntheticTableGenerator<?>> forDefinition(@Nonnull final IndexSpec spec,
                                                                     @Nonnull final String indexName,
                                                                     @Nonnull final QuantifierValues quantifierValues) {
        final var scanCount = spec.scanCount();
        final var hasUnnesting = !quantifierValues.getExplodes().isEmpty();
        if (scanCount > 1 && hasUnnesting) {
            // The two kinds do not compose yet, and no one kind can serve a definition needing both.
            throw Assert.failUnchecked(ErrorCode.UNSUPPORTED_OPERATION,
                    "Unsupported index definition, an unnesting cannot be combined with a join");
        } else if (scanCount > 1) {
            return RecordLayerJoinedSyntheticTableGenerator.initIfNeeded(spec, indexName, quantifierValues);
        } else if (scanCount == 1 && hasUnnesting) {
            return RecordLayerUnnestedSyntheticTableGenerator.initIfNeeded(spec, indexName, quantifierValues);
        } else {
            return Optional.empty();
        }
    }

    /**
     * The synthetic table an index is defined on.
     *
     * @param generator rewrites the definition onto the synthetic table
     * @param addsSyntheticTable whether the synthetic table is one of the definition's own, which the caller adds to the
     * schema template, built by {@link SyntheticTableGenerator#generate}, once the index is known to be valid; false if
     * the schema template already has it
     */
    record Resolution(@Nonnull SyntheticTableGenerator<?> generator, boolean addsSyntheticTable) {

        static Resolution ofExisting(@Nonnull SyntheticTableGenerator<?> generator) {
            return new Resolution(generator, false);
        }

        static Resolution ofNew(@Nonnull SyntheticTableGenerator<?> generator) {
            return new Resolution(generator, true);
        }
    }
}
