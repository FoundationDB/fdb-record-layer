/*
 * VectorIndexTestSupport.java
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

package com.apple.foundationdb.record.provider.foundationdb.indexes;

import com.apple.foundationdb.async.guardiann.Guardiann;
import com.apple.foundationdb.async.guardiann.GuardiannStructureAsserts;
import com.apple.foundationdb.async.guardiann.OnReadListener;
import com.apple.foundationdb.async.guardiann.OnWriteListener;
import com.apple.foundationdb.linear.RealVector;
import com.apple.foundationdb.record.Bindings;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.ExecuteState;
import com.apple.foundationdb.record.IndexFetchMethod;
import com.apple.foundationdb.record.IsolationLevel;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorIterator;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabase;
import com.apple.foundationdb.record.provider.foundationdb.FDBExceptions;
import com.apple.foundationdb.record.provider.foundationdb.FDBQueriedRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.OnlineIndexer;
import com.apple.foundationdb.record.provider.foundationdb.VectorIndexScanComparisons;
import com.apple.foundationdb.record.provider.foundationdb.VectorIndexScanOptions;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.QueryPlanConstraint;
import com.apple.foundationdb.record.query.plan.ScanComparisons;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.typing.TypeRepository;
import com.apple.foundationdb.record.query.plan.cascades.values.LiteralValue;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryFetchFromPartialRecordPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryIndexPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.util.VectorUtils;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static com.apple.foundationdb.record.query.plan.cascades.properties.UsedTypesProperty.usedTypes;

/**
 * Static helpers for tests of vector indexes at the record-store level: driving the index merger, building and running
 * kNN index plans, measuring recall, and checking the on-disk Guardiann structure. The helpers are independent of any
 * record type. Callers pass in how to open a context ({@code contextOpener}) and a store in it ({@code storeOpener}),
 * so every transaction gets the caller's context configuration, and {@link #createIndexPlan} takes the descriptor of
 * the record type the index is on. This class lives in the index maintainer's package so it can reach the
 * package-private {@link VectorIndexMaintainer#hasOutstandingWork()} and
 * {@link GuardiannVectorIndexEngine#parseConfig(Index)}.
 */
public class VectorIndexTestSupport {
    private static final Logger logger = LoggerFactory.getLogger(VectorIndexTestSupport.class);

    // Passes of OnlineIndexer.mergeIndex() allowed before the drain is declared failed. Deliberately tiny, and not a
    // statement about how deep a cascade of follow-up tasks can get: a pass loops internally (in IndexingMerger) until
    // the maintainer reports nothing it can do, and follow-up tasks a drain enqueues keep the partition's count
    // positive, so they are retired within that same pass. What a pass cannot do is work on a partition whose merge
    // lock record names another owner — it skips such a partition and, having nothing else to do, returns having
    // drained nothing. More passes do not help in that case, since the record is only reclaimed once it ages out, so
    // this bound stays small: one retry, then fail and let the cause be investigated.
    private static final int MERGE_DRAIN_MAX_PASSES = 2;

    private VectorIndexTestSupport() {
    }

    /**
     * Runs one pass of the real record-layer index merger over {@code indexName} via {@link OnlineIndexer#mergeIndex()}
     * — the same entry a background merger uses. The merger sets the merge session id and drives the
     * per-partition claim/drain loop internally, so tests need not hand-roll that bookkeeping.
     * @param contextOpener opens the context the store for the {@link OnlineIndexer} configuration is opened in
     * @param storeOpener opens the record store holding the index in a given context
     * @param indexName the vector index to merge
     */
    @SuppressWarnings("PMD.CloseResource") // the outer context only builds the store for OnlineIndexer config
    public static void mergeVectorIndexOnce(@Nonnull final Supplier<FDBRecordContext> contextOpener,
                                            @Nonnull final Function<FDBRecordContext, FDBRecordStore> storeOpener,
                                            @Nonnull final String indexName) {
        try (FDBRecordContext context = contextOpener.get()) {
            final FDBRecordStore store = storeOpener.apply(context);
            final Index index = store.getRecordMetaData().getIndex(indexName);
            try (OnlineIndexer indexer = OnlineIndexer.newBuilder()
                    .setRecordStore(store)
                    .setIndex(index)
                    .setTimer(new FDBStoreTimer())
                    .build()) {
                indexer.mergeIndex();
            }
        }
    }

    /**
     * Drives {@link #mergeVectorIndexOnce} until {@code indexName} has no outstanding deferred-maintenance work
     * (executing a task can enqueue follow-ups, so a few passes may be needed), failing if it does not converge.
     * @param contextOpener opens the context of each transaction
     * @param storeOpener opens the record store holding the index in a given context
     * @param indexName the vector index to drain
     */
    public static void mergeVectorIndexToCompletion(@Nonnull final Supplier<FDBRecordContext> contextOpener,
                                                    @Nonnull final Function<FDBRecordContext, FDBRecordStore> storeOpener,
                                                    @Nonnull final String indexName) throws Exception {
        for (int pass = 0; pass < MERGE_DRAIN_MAX_PASSES; pass++) {
            mergeVectorIndexOnce(contextOpener, storeOpener, indexName);
            if (!vectorIndexHasOutstandingWork(contextOpener, storeOpener, indexName)) {
                return;
            }
        }
        throw new AssertionError(String.format("merge did not drain the backlog for %s within %d passes",
                indexName, MERGE_DRAIN_MAX_PASSES));
    }

    /**
     * Whether {@code indexName} still has outstanding deferred-maintenance work, read via its
     * {@link VectorIndexMaintainer}.
     * @param contextOpener opens the context to read in
     * @param storeOpener opens the record store holding the index in a given context
     * @param indexName the vector index to check
     * @return whether any partition has outstanding tasks
     */
    public static boolean vectorIndexHasOutstandingWork(@Nonnull final Supplier<FDBRecordContext> contextOpener,
                                                        @Nonnull final Function<FDBRecordContext, FDBRecordStore> storeOpener,
                                                        @Nonnull final String indexName) throws Exception {
        try (FDBRecordContext context = contextOpener.get()) {
            final FDBRecordStore store = storeOpener.apply(context);
            final VectorIndexMaintainer maintainer =
                    (VectorIndexMaintainer)store.getIndexMaintainer(store.getRecordMetaData().getIndex(indexName));
            return maintainer.hasOutstandingWork().get();
        }
    }

    /**
     * Inserts one batch in a single transaction, retrying the whole batch on an FDB conflict (logged) and backing
     * off + retrying on a cluster hard-cap back-pressure (logged). {@code saveBatch} runs again on every attempt, so it
     * must be safe to replay; {@code saveRecord} is idempotent by primary key, so replaying a rolled-back batch is safe.
     * @param contextOpener opens the context of each attempt
     * @param storeOpener opens the record store holding the index in a given context
     * @param saveBatch saves the batch's records into the given store
     * @param batchStart identifies the batch in the retry log messages, e.g. its first record number
     * @param batchSize the number of records in the batch, added to {@code committed} once the batch commits
     * @param backPressureBackoffMillis how long to back off before retrying a back-pressured batch
     * @param committed the number of committed records
     * @param conflictRetries the number of retries caused by FDB conflicts
     * @param backPressureRetries the number of retries caused by {@link VectorIndexClusterTooLargeException}
     */
    public static void insertBatchWithRetry(@Nonnull final Supplier<FDBRecordContext> contextOpener,
                                            @Nonnull final Function<FDBRecordContext, FDBRecordStore> storeOpener,
                                            @Nonnull final Consumer<FDBRecordStore> saveBatch,
                                            final long batchStart,
                                            final int batchSize,
                                            final long backPressureBackoffMillis,
                                            @Nonnull final AtomicLong committed,
                                            @Nonnull final AtomicLong conflictRetries,
                                            @Nonnull final AtomicLong backPressureRetries) {
        while (true) {
            try (FDBRecordContext context = contextOpener.get()) {
                final FDBRecordStore store = storeOpener.apply(context);
                saveBatch.accept(store);
                context.commit();
                committed.addAndGet(batchSize);
                return;
            } catch (final RuntimeException e) {
                if (FDBExceptions.isOrHasCause(e, VectorIndexClusterTooLargeException.class)) {
                    backPressureRetries.incrementAndGet();
                    logger.info("insert back-pressured (hard cap) on batch starting {}; backing off and retrying",
                            batchStart);
                    sleepQuietly(backPressureBackoffMillis);
                } else if (FDBExceptions.isOrHasCause(e, FDBExceptions.FDBStoreTransactionConflictException.class)) {
                    conflictRetries.incrementAndGet();
                    logger.info("insert conflict on batch starting {}; retrying", batchStart);
                } else {
                    throw e;
                }
            }
        }
    }

    /**
     * A raw {@link Guardiann} over {@code index}'s subspace in {@code store} — the same keys the record-layer engine
     * writes. Reusing {@link GuardiannVectorIndexEngine#parseConfig} guarantees the reconstructed {@link Guardiann} is
     * configured exactly as the engine that wrote the data (dimensions, cluster sizes, replication thresholds).
     * @param store the record store holding the index
     * @param index the Guardiann vector index
     * @param executor the executor for the {@link Guardiann} to run on
     * @return a {@link Guardiann} over the index's data
     */
    @Nonnull
    public static Guardiann guardiannView(@Nonnull final FDBRecordStore store, @Nonnull final Index index,
                                          @Nonnull final Executor executor) {
        return new Guardiann(store.indexSubspace(index), executor, GuardiannVectorIndexEngine.parseConfig(index),
                OnWriteListener.NOOP, OnReadListener.NOOP);
    }

    /**
     * Verifies the on-disk Guardiann structure of {@code indexName} is internally consistent: rebuild a raw
     * {@link Guardiann} over the index's subspace (see {@link #guardiannView}) and run fdb-extensions'
     * structural-invariant checker. This is the no-delete variant, so it also asserts every replica references a live
     * primary. The checker first drains to quiescence, which is a no-op if {@link #mergeVectorIndexToCompletion}
     * already emptied the backlog.
     * @param fdb the database the index is in
     * @param contextOpener opens the context the index subspace and config are read in
     * @param storeOpener opens the record store holding the index in a given context
     * @param indexName the Guardiann vector index to check
     */
    public static void assertGuardiannStructureInvariants(@Nonnull final FDBDatabase fdb,
                                                          @Nonnull final Supplier<FDBRecordContext> contextOpener,
                                                          @Nonnull final Function<FDBRecordContext, FDBRecordStore> storeOpener,
                                                          @Nonnull final String indexName) {
        final Guardiann guardiann;
        try (FDBRecordContext context = contextOpener.get()) {
            final FDBRecordStore store = storeOpener.apply(context);
            guardiann = guardiannView(store, store.getRecordMetaData().getIndex(indexName), fdb.getExecutor());
        }
        GuardiannStructureAsserts.assertGuardiannInvariants(fdb.database(), guardiann);
    }

    /**
     * Mean recall@{@code k} of {@code topK} over every query vs. the provided ground truth. The whole ground-truth set
     * of a query is its recall@k reference set, so each set must carry exactly the {@code k} nearest neighbors.
     * @param queries the query vectors
     * @param groundTruth the ids of the {@code k} nearest neighbors per query, in query order
     * @param k the number of neighbors to search for
     * @param topK returns the ids of the {@code k} nearest neighbors the index finds for a query
     * @return the mean recall@k over all queries
     */
    public static double meanRecallAtK(@Nonnull final List<? extends RealVector> queries,
                                       @Nonnull final List<Set<Integer>> groundTruth,
                                       final int k,
                                       @Nonnull final BiFunction<RealVector, Integer, Set<Long>> topK) {
        double totalRecall = 0.0d;
        for (int q = 0; q < queries.size(); q++) {
            final Set<Long> expected = groundTruth.get(q).stream()
                    .map(Integer::longValue)
                    .collect(ImmutableSet.toImmutableSet());
            final Set<Long> got = topK.apply(queries.get(q), k);
            final long hits = got.stream().filter(expected::contains).count();
            totalRecall += (double) hits / k;
        }
        return totalRecall / queries.size();
    }

    /**
     * Executes a vector index kNN plan (e.g. from {@link #createIndexPlan}) to exhaustion and returns the ids of the
     * hits.
     * @param contextOpener opens the context to query in
     * @param storeOpener opens the record store holding the index in a given context
     * @param plan the plan to execute
     * @param idOf maps a hit to its id, e.g. its record number
     * @return the ids of the hits
     */
    @Nonnull
    public static Set<Long> queryTopK(@Nonnull final Supplier<FDBRecordContext> contextOpener,
                                      @Nonnull final Function<FDBRecordContext, FDBRecordStore> storeOpener,
                                      @Nonnull final RecordQueryPlan plan,
                                      @Nonnull final Function<FDBQueriedRecord<Message>, Long> idOf) {
        final Set<Long> ids = new HashSet<>();
        try (FDBRecordContext context = contextOpener.get()) {
            final FDBRecordStore store = storeOpener.apply(context);
            byte[] continuation = null;
            do {
                try (RecordCursorIterator<FDBQueriedRecord<Message>> cursor =
                             executeQuery(store, plan, continuation)) {
                    while (cursor.hasNext()) {
                        ids.add(idOf.apply(Objects.requireNonNull(cursor.next())));
                    }
                    continuation = cursor.getNoNextReason() == RecordCursor.NoNextReason.SOURCE_EXHAUSTED
                                   ? null : cursor.getContinuation();
                }
            } while (continuation != null);
        }
        return ids;
    }

    @Nonnull
    @SuppressWarnings("resource")
    private static RecordCursorIterator<FDBQueriedRecord<Message>> executeQuery(@Nonnull final FDBRecordStore store,
                                                                                @Nonnull final RecordQueryPlan plan,
                                                                                @Nullable final byte[] continuation) {
        final var usedTypes = usedTypes().evaluate(plan);
        final var typeRepository = TypeRepository.newBuilder().addAllTypes(usedTypes).build();
        final var executeProperties = ExecuteProperties.newBuilder()
                .setIsolationLevel(IsolationLevel.SERIALIZABLE)
                .setState(ExecuteState.NO_LIMITS)
                .setReturnedRowLimit(Integer.MAX_VALUE).build();
        return plan.execute(store, EvaluationContext.forBindingsAndTypeRepository(Bindings.EMPTY_BINDINGS, typeRepository),
                continuation, executeProperties).asIterator();
    }

    /**
     * An index plan that scans the {@code k} nearest neighbors of {@code queryVector} in {@code indexName} and fetches
     * their records. The plan's common primary key is a fixed placeholder ({@code recNo}) that
     * {@link IndexFetchMethod#SCAN_AND_FETCH} execution ignores.
     * @param queryVector the query vector
     * @param k the number of neighbors to return
     * @param indexName the vector index to scan
     * @param recordDescriptor the descriptor of the record type the index is on
     * @return the index plan
     */
    @Nonnull
    public static RecordQueryIndexPlan createIndexPlan(@Nonnull final RealVector queryVector, final int k,
                                                       @Nonnull final String indexName,
                                                       @Nonnull final Descriptors.Descriptor recordDescriptor) {
        final VectorIndexScanComparisons vectorIndexScanComparisons =
                createVectorIndexScanComparisons(queryVector, k, VectorIndexScanOptions.empty());

        final Type.Record baseRecordType =
                Type.Record.fromFieldDescriptorsMap(
                        Type.Record.toFieldDescriptorMap(recordDescriptor.getFields()));

        return new RecordQueryIndexPlan(indexName, field("recNo"),
                vectorIndexScanComparisons, IndexFetchMethod.SCAN_AND_FETCH,
                RecordQueryFetchFromPartialRecordPlan.FetchIndexRecords.PRIMARY_KEY, false, false,
                Optional.empty(), baseRecordType, QueryPlanConstraint.noConstraint());
    }

    /**
     * Scan comparisons for the {@code k} nearest neighbors of {@code queryVector}. The query literal's vector type
     * takes its precision and dimensions from {@code queryVector}.
     * @param queryVector the query vector
     * @param k the number of neighbors to return
     * @param vectorIndexScanOptions the scan options
     * @return the scan comparisons
     */
    @Nonnull
    public static VectorIndexScanComparisons createVectorIndexScanComparisons(@Nonnull final RealVector queryVector, final int k,
                                                                              @Nonnull final VectorIndexScanOptions vectorIndexScanOptions) {
        final Comparisons.DistanceRankValueComparison distanceRankComparison =
                new Comparisons.DistanceRankValueComparison(Comparisons.Type.DISTANCE_RANK_LESS_THAN_OR_EQUAL,
                        new LiteralValue<>(Type.Vector.of(false, VectorUtils.getVectorPrecision(queryVector),
                                queryVector.getNumDimensions()), queryVector),
                        new LiteralValue<>(k), null, null);

        return VectorIndexScanComparisons.byDistance(ScanComparisons.EMPTY,
                distanceRankComparison, vectorIndexScanOptions);
    }

    static void sleepQuietly(final long millis) {
        try {
            Thread.sleep(millis);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
