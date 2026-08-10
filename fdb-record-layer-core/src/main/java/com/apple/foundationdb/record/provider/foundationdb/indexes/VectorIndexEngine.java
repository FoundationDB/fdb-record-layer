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

import com.apple.foundationdb.async.common.ResultEntry;
import com.apple.foundationdb.linear.Metric;
import com.apple.foundationdb.linear.RealVector;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexOptions;
import com.apple.foundationdb.record.metadata.MetaDataException;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.VectorIndexScanBounds;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

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
 * {@link #fromIndex(Index, Subspace)} exhaustively dispatch on the {@link IndexOptions#VECTOR_ENGINE} option.
 */
sealed interface VectorIndexEngine permits HnswVectorIndexEngine, GuardiannVectorIndexEngine {
    /**
     * Searches a single partition for the nearest neighbors described by {@code scanBounds}. The result is the full,
     * distance-ordered page of hits the maintainer turns into index entries; pagination and continuations are handled
     * by the maintainer, not the engine. The read transaction, executor and timer are all taken from {@code context};
     * {@code snapshot} selects whether the read is done at snapshot isolation (that derives from the scan's isolation
     * level, which the context alone does not carry).
     *
     * @param context the record context to read under; supplies the transaction, executor and timer
     * @param snapshot whether to read at snapshot isolation
     * @param subspace the partition subspace holding this engine's structure
     * @param scanBounds the bounds (query vector, limit, per-query scan options) of the search
     * @return a future of the distance-ordered result entries
     */
    @Nonnull
    CompletableFuture<List<? extends ResultEntry>> search(@Nonnull FDBRecordContext context,
                                                          boolean snapshot,
                                                          @Nonnull Subspace subspace,
                                                          @Nonnull VectorIndexScanBounds scanBounds);

    /**
     * Inserts a single vector into a partition. The write transaction, executor and timer are all taken from
     * {@code context}.
     *
     * @param context the record context to write under; supplies the transaction, executor and timer
     * @param subspace the partition subspace holding this engine's structure
     * @param primaryKey the (prefix-trimmed) primary key of the record
     * @param vector the vector to insert
     * @param register notified as deferred maintenance tasks are enqueued/executed during this insert (via its
     *        {@code onTaskEnqueued}/{@code onTaskExecuted} callbacks); {@link TaskEventRegister#NOOP} if there is
     *        nothing to react to
     * @param maintainInTransaction when {@code true}, the engine drains a deferred maintenance task inside this
     *        writing transaction (Guardiann); when {@code false} it lets work accumulate for a background merge.
     *        Engines that do everything inline (HNSW) ignore it. Sourced from the store's
     *        {@link com.apple.foundationdb.record.provider.foundationdb.IndexDeferredMaintenanceControl#shouldAutoMergeDuringCommit()}
     * @return a future that completes when the insert is done
     */
    @Nonnull
    CompletableFuture<Void> insert(@Nonnull FDBRecordContext context,
                                   @Nonnull Subspace subspace,
                                   @Nonnull Tuple primaryKey,
                                   @Nonnull RealVector vector,
                                   @Nonnull TaskEventRegister register,
                                   boolean maintainInTransaction);

    /**
     * Deletes a single vector from a partition. The vector is always supplied because some engines (notably Guardiann)
     * need it to locate the vector's cluster references; engines that only key on the primary key (HNSW) ignore it.
     * The write transaction, executor and timer are all taken from {@code context}.
     *
     * @param context the record context to write under; supplies the transaction, executor and timer
     * @param subspace the partition subspace holding this engine's structure
     * @param primaryKey the (prefix-trimmed) primary key of the record
     * @param vector the vector being deleted
     * @param register notified as deferred maintenance tasks are enqueued/executed during this delete (via its
     *        {@code onTaskEnqueued}/{@code onTaskExecuted} callbacks); {@link TaskEventRegister#NOOP} if there is
     *        nothing to react to
     * @param maintainInTransaction when {@code true}, the engine drains a deferred maintenance task inside this
     *        writing transaction (Guardiann); when {@code false} it lets work accumulate for a background merge.
     *        Engines that do everything inline (HNSW) ignore it
     * @return a future that completes when the delete is done
     */
    @Nonnull
    CompletableFuture<Void> delete(@Nonnull FDBRecordContext context,
                                   @Nonnull Subspace subspace,
                                   @Nonnull Tuple primaryKey,
                                   @Nonnull RealVector vector,
                                   @Nonnull TaskEventRegister register,
                                   boolean maintainInTransaction);

    /**
     * The register that tracks this engine's outstanding deferred-maintenance work, or {@code null} for an engine that
     * does everything inline (HNSW) and therefore has nothing to track. The engine owns it (built from the index's
     * secondary subspace at construction), so the maintainer can ask for it without a per-engine branch of its own.
     * @return this engine's task-count register, or {@code null} if the engine defers no work
     */
    @Nullable
    VectorIndexTaskCounts getTaskCounts();

    /**
     * Whether an insert/delete that enqueues deferred maintenance work should tell the caller — through the record
     * store's {@link com.apple.foundationdb.record.provider.foundationdb.IndexDeferredMaintenanceControl} — that a
     * background merge is needed. True only for an engine that defers work <em>and</em> is not draining it inside the
     * writing transaction for this write; {@code false} for an engine that does everything inline (HNSW) or when this
     * write self-drains in-transaction. The maintainer uses this to decide whether to compose a
     * {@link MaintenanceControlRegister} into the register it hands the engine, keeping the engine itself decoupled
     * from the store.
     * @param maintainInTransaction whether this write drains a deferred task in its own transaction (see
     *        {@link #insert}); an engine that defers work signals the caller only when this is {@code false}
     * @return whether the caller should be signalled to merge when this engine enqueues deferred work
     */
    boolean signalsMergeRequiredToCaller(boolean maintainInTransaction);

    /**
     * Drains up to {@code numTasks} of a partition's deferred maintenance tasks, running them inline in
     * {@code context}'s transaction. This is how a merge pays down the backlog that inserts and deletes only nibble at:
     * the maintainer calls it once per partition that has outstanding work. The Guardiann engine runs its queued
     * split/merge/reassign/collapse tasks; an engine that does everything inline (HNSW) never enqueues tasks and is
     * never routed here — being asked to drain is a programming error, so it throws.
     * <p>
     * As each task executes, the write listener notifies {@code register}'s {@code onTaskExecuted} callback in the same
     * transaction — e.g. so a task-count register stays in step with the queue as the merge drains it.
     *
     * @param context the record context to drain under; supplies the transaction, executor and timer
     * @param subspace the partition subspace holding this engine's structure
     * @param numTasks the maximum number of queued tasks to run in this transaction
     * @param register notified as tasks are executed during the drain (via its {@code onTaskExecuted} callback);
     *        {@link TaskEventRegister#NOOP} if there is nothing to react to
     * @param deadlineMillis an absolute wall-clock deadline (epoch millis); the engine stops before starting a task
     *        once it is reached, so a merge can bound a drain by time as well as by {@code numTasks} (at least one task
     *        still runs). Pass {@link Long#MAX_VALUE} for no time bound
     * @return a future of the number of tasks actually run — fewer than {@code numTasks} when the queue held fewer or
     *         the deadline was reached, which is how a merge learns how much of a partition it drained
     */
    @Nonnull
    CompletableFuture<Integer> executeDeferredTasks(@Nonnull FDBRecordContext context,
                                                    @Nonnull Subspace subspace,
                                                    int numTasks,
                                                    @Nonnull TaskEventRegister register,
                                                    long deadlineMillis);

    /**
     * Determines the engine kind an index is configured to use.
     *
     * @param index the index definition
     * @return the engine kind
     */
    @Nonnull
    static VectorIndexEngineKind kindFromIndex(@Nonnull final Index index) {
        return VectorIndexEngineKind.fromOptionValue(index.getOption(IndexOptions.VECTOR_ENGINE));
    }

    /**
     * Eagerly parses the engine-specific configuration for an index, purely to validate it: an invalid option throws.
     * This is the config-validation entry point used by the index validator and by option tests, neither of which has a
     * store — so it parses the config without building an engine (which would need the index's secondary subspace).
     *
     * @param index the index definition to validate
     */
    @SuppressWarnings("checkstyle:MissingSwitchDefault")
    static void validate(@Nonnull final Index index) {
        switch (kindFromIndex(index)) {
            case HNSW -> HnswVectorIndexEngine.parseConfig(index);
            case GUARDIANN -> GuardiannVectorIndexEngine.parseConfig(index);
        }
    }

    /**
     * Builds the engine an index is configured to use, giving it the index's secondary subspace so a deferring engine
     * (Guardiann) can own its {@link VectorIndexTaskCounts}. Parsing the engine's configuration eagerly validates the
     * index options (an invalid option throws).
     *
     * @param index the index definition
     * @param indexSecondarySubspace the index's secondary subspace
     * @return the engine backing this index
     */
    @Nonnull
    static VectorIndexEngine fromIndex(@Nonnull final Index index, @Nonnull final Subspace indexSecondarySubspace) {
        return switch (kindFromIndex(index)) {
            case HNSW -> HnswVectorIndexEngine.fromIndex(index);
            case GUARDIANN -> GuardiannVectorIndexEngine.fromIndex(index, indexSecondarySubspace);
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
        return VectorIndexOptionKeys.METRIC.read(index, Metric.EUCLIDEAN_METRIC);
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
        final VectorIndexEngineKind newIndexKind = kindFromIndex(newIndex);
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
