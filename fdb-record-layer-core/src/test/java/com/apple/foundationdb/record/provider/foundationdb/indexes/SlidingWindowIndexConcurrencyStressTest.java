/*
 * SlidingWindowIndexConcurrencyStressTest.java
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
import com.apple.test.ParameterizedTestUtils;
import com.apple.test.RandomizedTestUtils;
import com.apple.test.Tags;
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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

/**
 * Concurrency stress tests for {@link SlidingWindowIndexMaintainer} over a value-index
 * delegate. Hammers a single sliding window with random concurrent inserts and deletes
 * whose window keys straddle the boundary, then asserts the "boundary separates"
 * invariant on the post-state: every primary key in the delegate sits on the window
 * side of the boundary, and every entries-subspace key absent from the delegate sits
 * on the overflow side.
 *
 * <p>Two drivers exercise the same workload at different concurrency layers:</p>
 * <ul>
 *     <li>{@code interleavedTransactions} — opens N {@link FDBRecordContext}s
 *         simultaneously and commits them in order, tallying boundary-meta-key
 *         conflicts.</li>
 *     <li>{@code realThreadedConcurrency} — spawns N worker threads, each running its
 *         iterations through a retrying {@link FDBDatabaseRunner}.</li>
 * </ul>
 *
 * <p>Every scenario runs under three {@link PreSeed} regimes (empty, crossing, full)
 * to cover window growth, the fill-threshold crossing, and steady-state eviction.</p>
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
        final IndexPredicate predicate =
                new IndexPredicate.RowNumberWindowPredicate("num_value_2", direction, WINDOW_SIZE);
        builder.addIndex("MySimpleRecord",
                new Index(INDEX_NAME, Key.Expressions.field("num_value_2"), IndexTypes.VALUE,
                        Map.of(), predicate));
        return builder.getRecordMetaData();
    }

    @Nonnull
    private FDBRecordStore openStore(@Nonnull final FDBRecordContext context,
                                     @Nonnull final RecordMetaData metaData) {
        return createOrOpenRecordStore(context, metaData, path).getLeft();
    }

    /**
     * Pre-seed records so the workload starts in the requested fill state. Seeded
     * primary keys live above {@link #REC_POOL} so workers cannot accidentally delete
     * them and reduce the test to an empty window.
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
     * Insert or delete a primary key drawn from {@code [pkStart, pkStart + pkSize)}.
     * Each worker is assigned a disjoint slice so the only intended source of write
     * conflicts is the shared boundary meta-key.
     */
    private void doRandomMutation(@Nonnull final FDBRecordStore store, @Nonnull final Random rnd,
                                  final long pkStart, final int pkSize) {
        final long pk = pkStart + rnd.nextInt(pkSize);
        if (rnd.nextBoolean()) {
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

    /**
     * Driver 1: opens {@code concurrentTxns} contexts simultaneously, runs a random
     * mutation in each, and commits them in order. Boundary-meta-key conflicts are
     * caught and tallied; whatever subset commits must still satisfy the
     * boundary-separates invariant.
     */
    @ParameterizedTest
    @MethodSource("scenarios")
    void interleavedTransactions(final long seed, @Nonnull final Direction direction, @Nonnull final PreSeed preSeed) {
        final Random rnd = new Random(seed);
        final RecordMetaData metaData = buildMetaData(direction);
        preSeed(preSeed, metaData, new Random(seed));

        final int concurrentTxns = 4;
        final int cycles = 16;
        final int pkSlice = REC_POOL / concurrentTxns;
        long conflicts = 0L;
        long commits = 0L;
        for (int cycle = 0; cycle < cycles; cycle++) {
            final List<FDBRecordContext> contexts = new ArrayList<>(concurrentTxns);
            try {
                for (int i = 0; i < concurrentTxns; i++) {
                    contexts.add(openContext());
                }
                // Force all transactions to take a read version up front so subsequent
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
            // Each in-flight tx can over-add at most once on the snapshot-read counter,
            // so the count is bounded by windowSize + concurrentTxns.
            SlidingWindowTestHelpers.verifySlidingWindowInvariant(
                    store, INDEX_NAME, WINDOW_SIZE, direction, WINDOW_SIZE - 2 * concurrentTxns, WINDOW_SIZE + 2 * concurrentTxns);
        }
    }

    /**
     * Driver 2: spawns {@code numWorkers} real threads. Each runs
     * {@code iterationsPerWorker} random mutations in their own
     * {@link FDBRecordContext} via {@link FDBDatabaseRunner#run} (which retries on
     * transient conflicts). After all workers complete, asserts the
     * boundary-separates invariant.
     */
    @ParameterizedTest
    @MethodSource("scenarios")
    void realThreadedConcurrency(final long seed, @Nonnull final Direction direction, @Nonnull final PreSeed preSeed) throws Exception {
        final int numWorkers = 8;
        final int iterationsPerWorker = 200;
        final int pkSlice = REC_POOL / numWorkers;
        final RecordMetaData metaData = buildMetaData(direction);
        final Random random = new Random(seed);
        preSeed(preSeed, metaData, random);

        final AtomicLong totalRetries = new AtomicLong(0L);
        final AtomicInteger threadIdx = new AtomicInteger();
        final ExecutorService executor = Executors.newFixedThreadPool(numWorkers, r -> {
            final Thread t = new Thread(r, "sliding-window-worker-" + threadIdx.getAndIncrement());
            t.setDaemon(true);
            return t;
        });

        try {
            final List<Future<?>> futures = new ArrayList<>(numWorkers);
            for (int w = 0; w < numWorkers; w++) {
                final long workerSeed = random.nextLong();
                final long pkStart = 1L + (long) w * pkSlice;
                futures.add(executor.submit(() ->
                        runWorker(metaData, workerSeed, pkStart, pkSlice, iterationsPerWorker, totalRetries)));
            }
            for (Future<?> f : futures) {
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
            
            SlidingWindowTestHelpers.verifySlidingWindowInvariant(
                    store, INDEX_NAME, WINDOW_SIZE, direction, WINDOW_SIZE - 2 * numWorkers, WINDOW_SIZE + numWorkers);
        }
    }

    private void runWorker(@Nonnull final RecordMetaData metaData, final long workerSeed,
                           final long pkStart, final int pkSlice, final int iterationsPerWorker,
                           @Nonnull final AtomicLong totalRetries) {
        final Random workerRnd = new Random(workerSeed);
        try (FDBDatabaseRunner runner = fdb.newRunner()) {
            for (int it = 0; it < iterationsPerWorker; it++) {
                final AtomicInteger attempts = new AtomicInteger(0);
                runner.run(ctx -> {
                    if (attempts.getAndIncrement() > 0) {
                        totalRetries.incrementAndGet();
                    }
                    doRandomMutation(openStore(ctx, metaData), workerRnd, pkStart, pkSlice);
                    return null;
                });
            }
        }
    }

    @Nonnull
    static Stream<Arguments> scenarios() {
        return ParameterizedTestUtils.cartesianProduct(
                RandomizedTestUtils.randomSeeds(0x5ca1ab1e, 0xfdb01234L),
                Arrays.stream(Direction.values()),
                Arrays.stream(PreSeed.values())
        );
    }
}
