/*
 * VectorIndexOptionsHelper.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2025 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.MetaDataException;
import com.apple.foundationdb.record.provider.foundationdb.VectorIndexScanBounds;
import com.apple.foundationdb.record.provider.foundationdb.VectorIndexScanOptions;

import javax.annotation.Nonnull;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.DoubleConsumer;
import java.util.function.IntConsumer;

/**
 * Helpers for reading and validating vector index options through the {@link VectorIndexOptionKeys} catalog. Each
 * option's (canonical, legacy) name pairing and value type is declared once as a {@link VectorOptionKey}; the reads and
 * change validators here operate on those keys, so the pairing is never restated at a call site.
 */
final class VectorIndexOptionsHelper {
    private VectorIndexOptionsHelper() {
    }

    /**
     * Reads the number of dimensions of a vector index. This option is mandatory as there is no meaningful default.
     *
     * @param index the index definition
     * @return the number of dimensions
     * @throws MetaDataException if the dimensions option is set under none of its names
     */
    static int getNumDimensions(@Nonnull final Index index) {
        final Integer numDimensions = VectorIndexOptionKeys.NUM_DIMENSIONS.read(index);
        if (numDimensions == null) {
            throw new MetaDataException("need to specify the number of dimensions",
                    LogMessageKeys.INDEX_NAME, index.getName());
        }
        return numDimensions;
    }

    /**
     * Verifies that no option is specified under more than one of its names. An option that carries both a current name
     * and a legacy alias (e.g. {@code vectorMetric} and {@code hnswMetric}) must be given under at most one of them —
     * setting both is rejected even if the values agree, as that case is considered indicative of a logic bug.
     *
     * @param index the index definition to check
     * @param keys the option keys to check (each key's {@link VectorOptionKey#allNames() names} are inspected)
     * @throws MetaDataException if any key has more than one of its names set on the index
     */
    static void validateNoAliasConflicts(@Nonnull final Index index,
                                         @Nonnull final Iterable<VectorOptionKey<?>> keys) {
        for (final VectorOptionKey<?> key : keys) {
            boolean seen = false;
            for (final String name : key.allNames()) {
                if (index.getOption(name) != null) {
                    if (seen) {
                        throw new MetaDataException("vector index option specified under more than one name",
                                LogMessageKeys.INDEX_NAME, index.getName(),
                                LogMessageKeys.INDEX_OPTION, key.getCanonicalName());
                    }
                    seen = true;
                }
            }
        }
    }

    /**
     * Reads an optional integer option and, when it is set under any of the key's names, passes it to {@code setter}
     * (typically a config-builder method); otherwise leaves the builder at its default. The typed counterparts
     * ({@link #applyDouble}, {@link #applyBoolean}) follow the same contract.
     *
     * @param key the option to read
     * @param index the index definition
     * @param setter the sink for the value when present
     */
    static void applyInteger(@Nonnull final VectorOptionKey<Integer> key, @Nonnull final Index index,
                             @Nonnull final IntConsumer setter) {
        final Integer value = key.read(index);
        if (value != null) {
            setter.accept(value);
        }
    }

    static void applyDouble(@Nonnull final VectorOptionKey<Double> key, @Nonnull final Index index,
                            @Nonnull final DoubleConsumer setter) {
        final Double value = key.read(index);
        if (value != null) {
            setter.accept(value);
        }
    }

    static void applyBoolean(@Nonnull final VectorOptionKey<Boolean> key, @Nonnull final Index index,
                             @Nonnull final Consumer<Boolean> setter) {
        final Boolean value = key.read(index);
        if (value != null) {
            setter.accept(value);
        }
    }

    /**
     * Disallows a change to an immutable option (by wire name) by comparing its <em>effective</em> (parsed and
     * defaulted) value between the old and new index. If the values differ a {@link MetaDataException} is thrown;
     * if they are equal, the name is removed from {@code changedOptions} so the caller knows it has been handled.
     * Names absent from {@code changedOptions} are ignored.
     * <p>
     * The comparison uses effective values, not raw option strings: setting an option to a value that equals the
     * built-in default counts as no change even when the other index omits the option entirely. Callers therefore pass
     * the values extracted from the parsed engine config, not the raw option strings.
     *
     * @param changedOptions the mutable set of changed option names; the handled name is removed from it
     * @param optionName the wire name to guard
     * @param oldValue the effective value on the pre-change index
     * @param newValue the effective value on the post-change index
     * @param indexName the name of the index (for error reporting)
     * @param <T> the value type
     */
    static <T> void disallowChange(@Nonnull final Set<String> changedOptions, @Nonnull final String optionName,
                                   @Nonnull final T oldValue, @Nonnull final T newValue,
                                   @Nonnull final String indexName) {
        if (changedOptions.contains(optionName)) {
            if (!Objects.equals(oldValue, newValue)) {
                throw new MetaDataException("attempted to change immutable vector index option",
                        LogMessageKeys.INDEX_NAME, indexName,
                        LogMessageKeys.INDEX_OPTION, optionName);
            }
            changedOptions.remove(optionName);
        }
    }

    /**
     * Disallows a change to an immutable option, guarding every wire name of the key (canonical and legacy aliases) so
     * a change expressed through any of them is caught. Fans {@link #disallowChange(Set, String, Object, Object, String)}
     * out over {@link VectorOptionKey#allNames()}.
     *
     * @param changedOptions the mutable set of changed option names; the handled names are removed from it
     * @param key the option to guard
     * @param oldValue the effective value on the pre-change index
     * @param newValue the effective value on the post-change index
     * @param indexName the name of the index (for error reporting)
     * @param <T> the value type
     */
    static <T> void disallowChange(@Nonnull final Set<String> changedOptions, @Nonnull final VectorOptionKey<?> key,
                                   @Nonnull final T oldValue, @Nonnull final T newValue,
                                   @Nonnull final String indexName) {
        for (final String name : key.allNames()) {
            disallowChange(changedOptions, name, oldValue, newValue, indexName);
        }
    }

    /**
     * Marks an option as mutable by removing every wire name of the key (canonical and legacy aliases) from
     * {@code changedOptions} without any comparison.
     *
     * @param changedOptions the mutable set of changed option names
     * @param key the option to allow changing
     */
    static void allowChange(@Nonnull final Set<String> changedOptions, @Nonnull final VectorOptionKey<?> key) {
        for (final String name : key.allNames()) {
            changedOptions.remove(name);
        }
    }

    /**
     * Resolves whether a vector search should return the stored vectors alongside the primary keys. Honors the explicit
     * {@link VectorIndexScanOptions#VECTOR_RETURN_VECTORS} scan option when set (under either its canonical or legacy
     * name); otherwise returns vectors by default only when they are stored verbatim. If RaBitQ quantization is in use
     * the stored form must be reconstructed, so vectors are withheld by default (returning them would waste that
     * reconstruction when the caller did not ask); without RaBitQ the vectors were already fetched and are identical to
     * what was inserted, so returning them is free.
     *
     * @param scanBounds the per-query scan bounds carrying the scan options
     * @param useRaBitQ whether the engine's configuration uses RaBitQ quantization
     * @return whether the search should include vectors in its results
     */
    static boolean returnVectors(@Nonnull final VectorIndexScanBounds scanBounds, final boolean useRaBitQ) {
        final VectorIndexScanOptions scanOptions = scanBounds.getVectorIndexScanOptions();
        final Boolean returnVectorsValue = scanOptions.getOption(VectorIndexScanOptions.VECTOR_RETURN_VECTORS);
        if (returnVectorsValue != null) {
            return returnVectorsValue;
        }
        return !useRaBitQ;
    }
}
