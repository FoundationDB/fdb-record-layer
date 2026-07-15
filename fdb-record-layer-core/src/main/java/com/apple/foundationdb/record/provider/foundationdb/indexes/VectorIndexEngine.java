/*
 * VectorIndexEngine.java
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

import com.apple.foundationdb.ReadTransaction;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.async.common.ResultEntry;
import com.apple.foundationdb.linear.Metric;
import com.apple.foundationdb.linear.RealVector;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexOptions;
import com.apple.foundationdb.record.metadata.MetaDataException;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.VectorIndexScanBounds;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * The engine that actually backs a {@link com.apple.foundationdb.record.metadata.IndexTypes#VECTOR vector} index. This
 * is the adapter that lets a single {@link VectorIndexMaintainer} sit on top of more than one underlying vector
 * structure: today an {@link com.apple.foundationdb.async.hnsw.HNSW HNSW} graph
 * ({@link HnswVectorIndexEngine}) or a {@link com.apple.foundationdb.async.guardiann.Guardiann Guardiann} clustered
 * structure ({@link GuardiannVectorIndexEngine}).
 * <p>
 * The maintainer keeps everything that is engine-independent — the continuation/cursor machinery, the prefix skip-scan,
 * locking, and the translation of results to {@link com.apple.foundationdb.record.IndexEntry index entries}. This
 * interface is deliberately narrow: an engine only knows how to search, insert and delete against a concrete partition
 * {@link Subspace}, returning raw {@link ResultEntry} results the maintainer then wraps. Each engine also owns the
 * parsing and validation of its own index options (via its static {@code fromIndex} and {@code validateChangedOptions})
 * and builds its own read/write listeners so it can attribute the right work to the shared {@link FDBStoreTimer}.
 * <p>
 * The interface is {@code sealed}: the set of engines is closed and known at compile time, which lets
 * {@link #fromIndex(Index)} exhaustively dispatch on the {@link IndexOptions#VECTOR_ENGINE} option.
 */
sealed interface VectorIndexEngine permits HnswVectorIndexEngine, GuardiannVectorIndexEngine {
    /**
     * Searches a single partition for the nearest neighbors described by {@code scanBounds}. The result is the full,
     * distance-ordered page of hits the maintainer turns into index entries; pagination and continuations are handled
     * by the maintainer, not the engine.
     *
     * @param readTransaction the transaction to read under
     * @param subspace the partition subspace holding this engine's structure
     * @param executor the executor to run asynchronous work on
     * @param timer the timer to attribute read work to
     * @param scanBounds the bounds (query vector, limit, per-query scan options) of the search
     * @return a future of the distance-ordered result entries
     */
    @Nonnull
    CompletableFuture<List<? extends ResultEntry>> search(@Nonnull ReadTransaction readTransaction,
                                                          @Nonnull Subspace subspace,
                                                          @Nonnull Executor executor,
                                                          @Nonnull FDBStoreTimer timer,
                                                          @Nonnull VectorIndexScanBounds scanBounds);

    /**
     * Inserts a single vector into a partition.
     *
     * @param transaction the transaction to write under
     * @param subspace the partition subspace holding this engine's structure
     * @param executor the executor to run asynchronous work on
     * @param timer the timer to attribute write work to
     * @param primaryKey the (prefix-trimmed) primary key of the record
     * @param vector the vector to insert
     * @return a future that completes when the insert is done
     */
    @Nonnull
    CompletableFuture<Void> insert(@Nonnull Transaction transaction,
                                   @Nonnull Subspace subspace,
                                   @Nonnull Executor executor,
                                   @Nonnull FDBStoreTimer timer,
                                   @Nonnull Tuple primaryKey,
                                   @Nonnull RealVector vector);

    /**
     * Deletes a single vector from a partition. The vector is always supplied because some engines (notably Guardiann)
     * need it to locate the vector's cluster references; engines that only key on the primary key (HNSW) ignore it.
     *
     * @param transaction the transaction to write under
     * @param subspace the partition subspace holding this engine's structure
     * @param executor the executor to run asynchronous work on
     * @param timer the timer to attribute write work to
     * @param primaryKey the (prefix-trimmed) primary key of the record
     * @param vector the vector being deleted
     * @return a future that completes when the delete is done
     */
    @Nonnull
    CompletableFuture<Void> delete(@Nonnull Transaction transaction,
                                   @Nonnull Subspace subspace,
                                   @Nonnull Executor executor,
                                   @Nonnull FDBStoreTimer timer,
                                   @Nonnull Tuple primaryKey,
                                   @Nonnull RealVector vector);

    /**
     * The kinds of vector engine, as selectable through the {@link IndexOptions#VECTOR_ENGINE} index option.
     */
    enum Kind {
        HNSW,
        GUARDIANN;

        /**
         * Resolves an engine kind from its option string, accepting any letter case. When {@code value} is
         * {@code null} (the option was not set) the engine defaults to {@link #HNSW}, so vector indexes created before
         * the engine option existed continue to use HNSW.
         *
         * @param value the raw option value, or {@code null} if unset
         * @return the resolved engine kind
         */
        @Nonnull
        static Kind fromOptionValue(final String value) {
            if (value == null) {
                return HNSW;
            }
            return switch (value.toUpperCase(Locale.ROOT)) {
                case "HNSW" -> HNSW;
                case "GUARDIANN" -> GUARDIANN;
                default -> throw new MetaDataException("unknown vector index engine", LogMessageKeys.VALUE, value);
            };
        }
    }

    /**
     * Determines the engine kind an index is configured to use.
     *
     * @param index the index definition
     * @return the engine kind
     */
    @Nonnull
    static Kind kindFromIndex(@Nonnull final Index index) {
        return Kind.fromOptionValue(index.getOption(IndexOptions.VECTOR_ENGINE));
    }

    /**
     * Builds the engine an index is configured to use, parsing its engine-specific configuration from the index
     * options. Parsing eagerly validates the options, so this doubles as the config-validation entry point used by the
     * index validator.
     *
     * @param index the index definition
     * @return the engine backing this index
     */
    @Nonnull
    @SuppressWarnings("UnnecessaryDefault")
    static VectorIndexEngine fromIndex(@Nonnull final Index index) {
        return switch (kindFromIndex(index)) {
            case HNSW -> HnswVectorIndexEngine.fromIndex(index);
            case GUARDIANN -> GuardiannVectorIndexEngine.fromIndex(index);
            default -> throw new MetaDataException("unknown vector index engine");
        };
    }

    /**
     * Reads the metric configured for a vector index without having to build the whole engine. Used by planning code
     * that only needs to know which distance function the index sorts by.
     *
     * @param index the index definition
     * @return the metric of the index
     */
    @Nonnull
    static Metric metricFromIndex(@Nonnull final Index index) {
        return VectorIndexOptionsHelper.getMetric(index);
    }

    /**
     * Validates a set of changed index options against the engine's rules for which options may change on an existing
     * index. The engine itself ({@link IndexOptions#VECTOR_ENGINE}) is immutable — an index cannot switch engines — and
     * beyond that each engine decides which of its options are immutable (structural) versus mutable (tuning). Handled
     * options are removed from {@code changedOptions}; whatever remains is left for the caller's default handling.
     * <p>
     * The new index having no option specified under more than one of its names is enforced separately by the new
     * index's own {@code validate()} (which runs during metadata evolution before this method), so it is not re-checked
     * here.
     *
     * @param oldIndex the pre-change index
     * @param newIndex the post-change index
     * @param changedOptions the mutable set of changed option names
     */
    static void validateChangedOptions(@Nonnull final Index oldIndex, @Nonnull final Index newIndex,
                                       @Nonnull final Set<String> changedOptions) {
        // The engine backing an index can never change; that would reinterpret the on-disk layout.
        final Kind newIndexKind = kindFromIndex(newIndex);
        VectorIndexOptionsHelper.disallowChange(changedOptions, IndexOptions.VECTOR_ENGINE,
                kindFromIndex(oldIndex), newIndexKind, newIndex.getName());

        switch (newIndexKind) {
            case HNSW:
                HnswVectorIndexEngine.validateChangedOptions(oldIndex, newIndex, changedOptions);
                break;
            case GUARDIANN:
                GuardiannVectorIndexEngine.validateChangedOptions(oldIndex, newIndex, changedOptions);
                break;
            default:
                throw new MetaDataException("unknown vector index engine");
        }
    }
}
