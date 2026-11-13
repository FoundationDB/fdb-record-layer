/*
 * SyntheticTableGenerator.java
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

import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerSchemaTemplate;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerSyntheticTable;

import javax.annotation.Nonnull;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

/**
 * Generator for synthetic table an index is defined on - for the index definitions that need one.
 *
 * <p>Each kind decides for itself whether a definition needs it, what it cannot express, and how the index key reads
 * against it. {@link SyntheticTableGeneratorFactory} is the one place that picks between them, and they are mutually
 * exclusive: unnesting within a join does not compose yet.
 *
 * <p>An index need not be defined on a synthetic table of its own. If the schema template already has one producing the
 * same synthetic records as the table {@link #generate} would build, up to a renaming of their constituents,
 * {@link #reuseMaybe} yields the same generator defined on that one instead, without building the table.
 *
 * @param <T> the kind of synthetic table this generator builds
 */
interface SyntheticTableGenerator<T extends RecordLayerSyntheticTable> {

    /**
     * Rejects what this kind of synthetic table generator cannot work with, separately from the definitions
     * {@link IndexSpec#checkValidity} rejects for any index at all.
     *
     * @param spec what the index is made of
     */
    void checkSupported(@Nonnull IndexSpec spec);

    /**
     * The same index, made of the synthetic table rather than the stored records: the projection and ordering are
     * re-expressed in the synthetic table's coordinates, so that they translate as ordinary field paths.
     *
     * @param spec what the index is made of, resolved against the stored records
     * @return the {@link IndexSpec}, in the synthetic table's coordinates
     */
    @Nonnull
    IndexSpec rewrite(@Nonnull IndexSpec spec);

    @Nonnull
    Type.Record getType();

    /**
     * {@return the synthetic table, without any index} Every call builds the same table, named and typed as
     * {@link #getType()} is. Only called once the definition is known to need a synthetic table of its own.
     */
    @Nonnull
    RecordLayerSyntheticTable.Builder generate();

    /**
     * {@return the fingerprint of the synthetic table {@link #generate} would build}
     */
    @Nonnull
    RecordLayerSyntheticTable.Fingerprint<T> getFingerprint();

    /**
     * {@return how the synthetic table {@link #generate} would build corresponds to an existing one of the same
     * fingerprint} The function yields, for each of this generator's constituents, the constituent of the existing table
     * it corresponds to, or empty if the existing table does not produce the same synthetic records.
     */
    @Nonnull
    Function<T, Optional<Map<String, String>>> matcher();

    /**
     * The same generator, defined on an existing synthetic table rather than on one of its own: its constituents are
     * renamed to those of the existing table, and the existing table's name and record type are used, so that
     * {@link #rewrite} expresses the index in the existing table's terms.
     *
     * @param existing a synthetic table producing the same synthetic records as the one {@link #generate} would build
     * @param aliasMapping for each of this generator's constituents, the constituent of {@code existing} it corresponds
     * to
     * @return the generator defined on {@code existing}
     */
    @Nonnull
    SyntheticTableGenerator<T> adopt(@Nonnull T existing, @Nonnull Map<String, String> aliasMapping);

    /**
     * The same generator, defined on the first synthetic table of the schema template producing the same synthetic
     * records as the one {@link #generate} would build, up to a renaming of their constituents. The constituents of two
     * such tables read the same stored tables through the same fields, so they are of the same types.
     *
     * @param schemaTemplateBuilder the schema template to find the existing synthetic table in
     * @return the generator defined on the existing table, or empty if there is none
     */
    @Nonnull
    default Optional<SyntheticTableGenerator<T>> reuseMaybe(@Nonnull final RecordLayerSchemaTemplate.Builder schemaTemplateBuilder) {
        final var candidates = schemaTemplateBuilder.getSyntheticTables(getFingerprint());
        if (candidates.isEmpty()) {
            return Optional.empty();
        }
        final var matcher = matcher();
        for (final T existing : candidates) {
            final var aliasMapping = matcher.apply(existing);
            if (aliasMapping.isPresent()) {
                return Optional.of(adopt(existing, aliasMapping.get()));
            }
        }
        return Optional.empty();
    }
}
