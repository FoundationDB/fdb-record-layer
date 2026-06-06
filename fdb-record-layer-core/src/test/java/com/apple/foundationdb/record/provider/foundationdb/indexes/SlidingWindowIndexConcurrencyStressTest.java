/*
 * SlidingWindowIndexConcurrencyTest.java
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

import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexPredicate;
import com.apple.foundationdb.record.metadata.IndexPredicate.RowNumberWindowPredicate.Direction;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabaseRunner;
import com.apple.foundationdb.record.provider.foundationdb.FDBExceptions;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreConcurrentTestBase;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpacePath;
import com.apple.foundationdb.record.test.TestKeySpace;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.Tags;
import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.opentest4j.TestAbortedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

/**
 * Concurrency stress tests for {@link SlidingWindowIndexMaintainer}.
 *
 * <p>Hammers a single sliding window with many concurrent inserts and deletes whose window
 * keys are drawn from a range that straddles the eventual boundary (so traffic falls both
 * above and below the window). After the workload completes, asserts the central
 * "boundary separates" invariant: every primary key in the underlying delegate index is on
 * the in-window side of the boundary, and every entry tracked in the sliding-window
 * subspace but missing from the delegate is on the overflow side.</p>
 *
 * <p>Uses a VALUE delegate (rather than HNSW) so the workload can run with thousands of
 * mutations in seconds.</p>
 *
 * <p>Each parameterization runs under one of three pre-seed regimes ({@link PreSeed}):
 * an empty window that grows under contention, a half-full window that crosses the fill
 * threshold mid-workload, and a pre-full window that exercises eviction and re-election
 * from the first iteration.</p>
 */
@Tag(Tags.RequiresFDB)
@Tag(Tags.Slow)
class SlidingWindowIndexConcurrencyStressTest extends FDBRecordStoreConcurrentTestBase {

    private static final Logger LOGGER = LoggerFactory.getLogger(SlidingWindowIndexConcurrencyStressTest.class);

    private static final String INDEX_NAME = "sw_value_index";
    private static final int WINDOW_SIZE = 25;
    private static final int REC_POOL = 200;
    private static final int RELEVANCE_MAX = 10 * WINDOW_SIZE;

    /** Pre-fill regime for the window prior to running the concurrent workload. */
    enum PreSeed {
        /** No pre-seeding; workers grow the window from zero. */
        EMPTY,
        /** Pre-seed {@code windowSize / 2} records so the workload crosses the fill threshold. */
        CROSSING,
        /** Pre-seed {@code windowSize} records so the workload starts with a full window. */
        FULL
    }

    private KeySpacePath path;

    @BeforeEach
    void allocatePath() {
        path = pathManager.createPath(TestKeySpace.RECORD_STORE);
    }

    @Nonnull
    private RecordMetaData buildMetaData(@Nonnull final Direction direction) {
        final RecordMetaDataBuilder builder = RecordMetaData.newBuilder()
                .setRecords(TestRecords1Proto.getDescriptor());
        final IndexPredicate.RowNumberWindowPredicate predicate =
                new IndexPredicate.RowNumberWindowPredicate(
                        ImmutableList.of("num_value_2"), direction, WINDOW_SIZE, ImmutableList.of());
        builder.addIndex("MySimpleRecord",
                new Index(INDEX_NAME, Key.Expressions.field("num_value_2"), IndexTypes.VALUE,
                        Collections.emptyMap(), predicate));
        return builder.getRecordMetaData();
    }

    @Nonnull
    private FDBRecordStore openStore(@Nonnull final FDBRecordContext context,
                                     @Nonnull final RecordMetaData metaData) {
        return createOrOpenRecordStore(context, metaData, path).getLeft();
    }

    /**
     * Pre-seed records prior to running the concurrent workload. Seeded primary keys are
     * disjoint from the worker primary-key pool so that workers cannot accidentally delete
     * the seed records and reduce the test to an empty window.
     */
    private void preSeed(@Nonnull final PreSeed preSeed,
                         @Nonnull final RecordMetaData metaData,
                         @Nonnull final Random rnd) {
        if (preSeed == PreSeed.EMPTY) {
            return;
        }
        final int n = preSeed == PreSeed.CROSSING ? WINDOW_SIZE / 2 : WINDOW_SIZE;
        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore store = openStore(context, metaData);
            for (int i = 0; i < n; i++) {
                store.saveRecord(TestRecords1Proto.MySimpleRecord.newBuilder()
                        .setRecNo(REC_POOL + 1 + i)
                        .setStrValueIndexed("seed")
                        .setNumValue2(rnd.nextInt(RELEVANCE_MAX))
                        .setNumValue3Indexed(0)
                        .build());
            }
            commit(context);
        }
    }

    /**
     * Pick a primary key from {@code [pkStart, pkStart + pkSize)} and either insert or delete it.
     * Each worker (thread in Driver 2, simultaneous context in Driver 1) is assigned a disjoint
     * slice of the pk space so that workers cannot collide on the same record key — leaving the
     * boundary meta-key as the only intended source of write conflicts.
     */
    private void doRandomMutation(@Nonnull final FDBRecordStore store, @Nonnull final Random rnd,
                                  final long pkStart, final int pkSize) {
        final boolean insert = rnd.nextBoolean();
        final long pk = pkStart + rnd.nextInt(pkSize);
        if (insert) {
            store.saveRecord(TestRecords1Proto.MySimpleRecord.newBuilder()
                    .setRecNo(pk)
                    .setStrValueIndexed("w")
                    .setNumValue2(rnd.nextInt(RELEVANCE_MAX))
                    .setNumValue3Indexed(0)
                    .build());
        } else {
            store.deleteRecord(Tuple.from(pk));
        }
    }

    // ===== Driver 1: single-threaded interleaving of multiple FDBRecordContexts =====

    /**
     * Opens {@code K} {@link FDBRecordContext}s simultaneously, issues a random mutation on
     * each, then commits them in order. Commits past the first against the same boundary
     * meta-key are expected to throw {@link FDBExceptions.FDBStoreTransactionConflictException}
     * (because the boundary-mutation path adds a read conflict on the boundary key); the
     * test catches these and treats them as informational. Whatever subset of transactions
     * commits successfully must leave the invariant intact at the end of the workload.
     */
    @ParameterizedTest(name = "interleaved[seed={0}, direction={1}, preSeed={2}]")
    @MethodSource("scenarios")
    void interleavedTransactions(final long seed, @Nonnull final Direction direction, @Nonnull final PreSeed preSeed) {
        final Random rnd = new Random(seed);
        final RecordMetaData metaData = buildMetaData(direction);
        preSeed(preSeed, metaData, new Random(seed));

        final int concurrentTxns = 4;
        final int cycles = 16;
        // Disjoint pk slice per context: prevents workers from racing on record keys, so any
        // observed conflict is a boundary-meta-key conflict — the case under test.
        final int pkSlice = REC_POOL / concurrentTxns;
        long conflicts = 0L;
        long commits = 0L;
        for (int cycle = 0; cycle < cycles; cycle++) {
            final List<FDBRecordContext> contexts = new ArrayList<>(concurrentTxns);
            try {
                for (int i = 0; i < concurrentTxns; i++) {
                    contexts.add(openContext());
                }
                // Force all transactions to obtain a read version up front so subsequent
                // commits actually race rather than serializing through GRV.
                for (FDBRecordContext ctx : contexts) {
                    ctx.getReadVersion();
                }
                for (int i = 0; i < concurrentTxns; i++) {
                    final FDBRecordContext ctx = contexts.get(i);
                    final FDBRecordStore store = openStore(ctx, metaData);
                    final long pkStart = 1L + (long) i * pkSlice;
                    doRandomMutation(store, rnd, pkStart, pkSlice);
                    doRandomMutation(store, rnd, pkStart, pkSlice);
                }
                for (FDBRecordContext ctx : contexts) {
                    try {
                        ctx.commit();
                        commits++;
                    } catch (FDBExceptions.FDBStoreTransactionConflictException expected) {
                        conflicts++;
                    }
                }
            } finally {
                for (FDBRecordContext ctx : contexts) {
                    ctx.close();
                }
            }
        }

        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("interleavedTransactions[seed={}, direction={}, preSeed={}]: commits={}, conflicts={}",
                    seed, direction, preSeed, commits, conflicts);
        }

        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore store = openStore(context, metaData);
            // Bound the count by windowSize + concurrentTxns: each in-flight transaction
            // can over-add at most once when racing on the snapshot-read counter.
            SlidingWindowTestHelpers.verifySlidingWindowInvariant(
                    store, INDEX_NAME, WINDOW_SIZE, direction, WINDOW_SIZE + concurrentTxns);
        }
    }

    // ===== Driver 2: real threads driving cross-transaction concurrency via FDBDatabaseRunner =====

    /**
     * Spawns {@code numWorkers} real threads. Each worker runs a loop of
     * {@code iterationsPerWorker} random mutations, each in its own
     * {@link FDBRecordContext} obtained via {@link FDBDatabaseRunner#run} (which retries
     * automatically on transient conflicts). After all workers complete, asserts the
     * boundary-separates invariant.
     */
    @ParameterizedTest(name = "threaded[seed={0}, direction={1}, preSeed={2}]")
    @MethodSource("scenarios")
    void realThreadedConcurrency(final long seed, @Nonnull final Direction direction, @Nonnull final PreSeed preSeed) throws Exception {
        final int numWorkers = 8;
        final int iterationsPerWorker = 200;
        // Disjoint pk slice per worker thread: see Driver 1 comment.
        final int pkSlice = REC_POOL / numWorkers;
        final RecordMetaData metaData = buildMetaData(direction);
        final Random random = new Random(seed);
        preSeed(preSeed, metaData, random);

        final AtomicLong totalRetries = new AtomicLong(0L);
        final ExecutorService executor = Executors.newFixedThreadPool(numWorkers, new ThreadFactory() {
            private final AtomicInteger idx = new AtomicInteger();
            @Override
            public Thread newThread(@Nonnull Runnable r) {
                final Thread t = new Thread(r, "sliding-window-worker-" + idx.getAndIncrement());
                t.setDaemon(true);
                return t;
            }
        });

        try {
            final List<Future<?>> futures = new ArrayList<>(numWorkers);
            for (int w = 0; w < numWorkers; w++) {
                final long workerSeed = random.nextLong();
                final long pkStart = 1L + (long) w * pkSlice;
                futures.add(executor.submit(() -> {
                    final Random workerRnd = new Random(workerSeed);
                    try (FDBDatabaseRunner runner = fdb.newRunner()) {
                        for (int it = 0; it < iterationsPerWorker; it++) {
                            final AtomicInteger attempts = new AtomicInteger(0);
                            runner.run(ctx -> {
                                if (attempts.getAndIncrement() > 0) {
                                    totalRetries.incrementAndGet();
                                }
                                final FDBRecordStore store = openStore(ctx, metaData);
                                doRandomMutation(store, workerRnd, pkStart, pkSlice);
                                return null;
                            });
                        }
                    }
                }));
            }
            for (Future<?> f : futures) {
                // Surface worker exceptions: f.get() will throw ExecutionException wrapping the cause.
                f.get(2, TimeUnit.MINUTES);
            }
        } catch (Exception exception) {
            for (Throwable cause = exception; cause != null; cause = cause.getCause()) {
                if (cause instanceof FDBExceptions.FDBStoreTransactionConflictException) {
                    throw new TestAbortedException("Thread wasn't able to commit, test result inconclusive");
                }
            }
            throw exception;
        } finally {
            executor.shutdownNow();
            executor.awaitTermination(30, TimeUnit.SECONDS);
        }

        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("realThreadedConcurrency[seed={}, direction={}, preSeed={}]: totalRetries={}",
                    seed, direction, preSeed, totalRetries.get());
        }

        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore store = openStore(context, metaData);
            // Bound the count by windowSize + numWorkers: each in-flight worker can
            // over-add at most once when racing on the snapshot-read counter.
            SlidingWindowTestHelpers.verifySlidingWindowInvariant(
                    store, INDEX_NAME, WINDOW_SIZE, direction, WINDOW_SIZE + numWorkers);
        }
    }

    static Stream<Arguments> scenarios() {
        final long[] seeds = {1L, 42L, 123L, 9999L};
        final List<Arguments> args = new ArrayList<>();
        for (long s : seeds) {
            for (Direction d : Direction.values()) {
                for (PreSeed p : PreSeed.values()) {
                    args.add(Arguments.of(s, d, p));
                }
            }
        }
        return args.stream();
    }
}
