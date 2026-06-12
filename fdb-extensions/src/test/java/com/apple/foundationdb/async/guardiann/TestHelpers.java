/*
 * TestHelpers.java
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

package com.apple.foundationdb.async.guardiann;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.async.MoreAsyncUtil;
import com.apple.foundationdb.async.common.PrimaryKeyAndVector;
import com.apple.foundationdb.async.common.ResultEntry;
import com.apple.foundationdb.async.common.StorageTransform;
import com.apple.foundationdb.async.hnsw.HNSW;
import com.apple.foundationdb.linear.DoubleRealVector;
import com.apple.foundationdb.linear.RealVector;
import com.apple.foundationdb.linear.StoredVecsIterator;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.Set;
import java.util.SplittableRandom;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.apple.foundationdb.async.common.CommonTestHelpers.createPrimaryKey;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test helpers for testing {@link Guardiann}s.
 */
@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
class TestHelpers {
    private static final Logger logger = LoggerFactory.getLogger(TestHelpers.class);

    @Nonnull
    static List<PrimaryKeyAndVector> basicInsertBatch(@Nonnull final Database db,
                                                      @Nonnull final Guardiann guardiann,
                                                      final int batchSize,
                                                      final long firstId,
                                                      @Nonnull final BiFunction<Transaction, Long, PrimaryKeyAndVector> insertFunction)
            throws ExecutionException, InterruptedException, TimeoutException {

        return db.runAsync(tr -> {
            final TestOnWriteListener onWriteListener = (TestOnWriteListener)guardiann.getOnWriteListener();
            onWriteListener.pushFrame();
            final TestOnReadListener onReadListener = (TestOnReadListener)guardiann.getOnReadListener();
            onReadListener.pushFrame();

            final ImmutableList.Builder<PrimaryKeyAndVector> data = ImmutableList.builder();

            final long beginTs = System.nanoTime();

            final CompletableFuture<Integer> loopFuture =
                    MoreAsyncUtil.forLoop(0, 0,
                            (i, u) ->
                                    i < batchSize && (int)u == i && onWriteListener.getSumTaskExecutedCounters() == 0,
                            i -> i + 1,
                            (i, u) -> {
                                final PrimaryKeyAndVector record = insertFunction.apply(tr, firstId + i);
                                if (record == null) {
                                    return CompletableFuture.completedFuture(i);
                                }
                                data.add(record);

                                return guardiann.insert(tr, record.getPrimaryKey(),
                                                record.getVector(), null)
                                        .thenApply(ignored -> i + 1);
                            }, guardiann.getExecutor());
            return loopFuture.thenApply(vignore -> data.build())
                    .whenComplete((result, error) -> {
                        if (error != null) {
                            logger.trace("failed to insert batchSize={}", batchSize);
                        } else {
                            final long endTs = System.nanoTime();
                            logger.trace("inserted batchSize={} records={} starting at id={} took elapsedTime={}ms, readBytes={}",
                                    batchSize, result.size(), firstId, TimeUnit.NANOSECONDS.toMillis(endTs - beginTs),
                                    onReadListener);
                        }
                        onWriteListener.popFrame();
                        onReadListener.popFrame();
                    });
        }).get(2, TimeUnit.MINUTES); // set a timeout for inserting a single batch including retries so setup won't run forever
    }

    /**
     * Symmetric to {@link #basicInsertBatch} for deletion. Deletes every record in
     * {@code recordsToDelete} from {@code guardiann} inside a single transaction, with the same
     * bail-out-on-deferred-task semantics: if a deferred maintenance task executes mid-batch, the
     * loop stops and the caller is expected to re-invoke with the un-deleted tail.
     * <p>
     * Returns the records that were actually issued for deletion (i.e. the prefix of
     * {@code recordsToDelete} processed before any bail-out). The caller is responsible for
     * advancing past those records on the next call. This mirrors the contract of
     * {@link #basicInsertBatch}.
     */
    @Nonnull
    static List<PrimaryKeyAndVector> basicDeleteBatch(@Nonnull final Database db,
                                                      @Nonnull final Guardiann guardiann,
                                                      @Nonnull final List<PrimaryKeyAndVector> recordsToDelete)
            throws ExecutionException, InterruptedException, TimeoutException {

        final int batchSize = recordsToDelete.size();
        return db.runAsync(tr -> {
            final TestOnWriteListener onWriteListener = (TestOnWriteListener) guardiann.getOnWriteListener();
            onWriteListener.pushFrame();
            final TestOnReadListener onReadListener = (TestOnReadListener) guardiann.getOnReadListener();
            onReadListener.pushFrame();

            final ImmutableList.Builder<PrimaryKeyAndVector> data = ImmutableList.builder();
            final long beginTs = System.nanoTime();

            final CompletableFuture<Integer> loopFuture =
                    MoreAsyncUtil.forLoop(0, 0,
                            (i, u) ->
                                    i < batchSize && (int) u == i && onWriteListener.getSumTaskExecutedCounters() == 0,
                            i -> i + 1,
                            (i, u) -> {
                                final PrimaryKeyAndVector record = recordsToDelete.get(i);
                                data.add(record);
                                return guardiann.delete(tr, record.getPrimaryKey(), record.getVector())
                                        .thenApply(ignored -> i + 1);
                            }, guardiann.getExecutor());
            return loopFuture.thenApply(vignore -> data.build())
                    .whenComplete((result, error) -> {
                        if (error != null) {
                            logger.trace("failed to delete batchSize={}", batchSize);
                        } else {
                            final long endTs = System.nanoTime();
                            logger.trace("deleted batchSize={} records={} took elapsedTime={}ms, readBytes={}",
                                    batchSize, result.size(), TimeUnit.NANOSECONDS.toMillis(endTs - beginTs),
                                    onReadListener);
                        }
                        onWriteListener.popFrame();
                        onReadListener.popFrame();
                    });
        }).get(2, TimeUnit.MINUTES);
    }

    static void insertSIFTSmall(@Nonnull final Database db,
                                @Nonnull final Guardiann guardiann) throws Exception {
        insertVectors(db, guardiann, SIFT_SMALL_BASE_PATH, 10_000, 50);
    }

    static void insertSIFT1m(@Nonnull final Database db,
                             @Nonnull final Guardiann guardiann,
                             final int numVectors,
                             final int batchSize) throws Exception {
        insertVectors(db, guardiann, SIFT_1M_BASE_PATH, numVectors, batchSize);
    }

    /** Path to the SIFT-small base vectors {@code .fvecs} file (10k × 128). Produced by the
     *  gradle {@code extractSiftSmall} task. */
    static final String SIFT_SMALL_BASE_PATH = ".out/extracted/siftsmall/siftsmall_base.fvecs";

    /** Path to the SIFT-small query vectors {@code .fvecs} file (100 × 128). */
    static final String SIFT_SMALL_QUERY_PATH = ".out/extracted/siftsmall/siftsmall_query.fvecs";

    /** Path to the SIFT-small ground-truth top-k indices {@code .ivecs} file. */
    static final String SIFT_SMALL_GROUNDTRUTH_PATH = ".out/extracted/siftsmall/siftsmall_groundtruth.ivecs";

    /** Path to the SIFT-1M base vectors {@code .fvecs} file (1M × 128). Downloaded by gradle to
     *  {@code .out/downloads/sift_base.fvecs}. */
    static final String SIFT_1M_BASE_PATH = ".out/downloads/sift_base.fvecs";

    /** Path to the SIFT-1M query vectors {@code .fvecs} file (10k × 128). */
    static final String SIFT_1M_QUERY_PATH = ".out/downloads/sift_query.fvecs";

    /** Path to the SIFT-1M ground-truth top-k indices {@code .ivecs} file. */
    static final String SIFT_1M_GROUNDTRUTH_PATH = ".out/downloads/sift_groundtruth.ivecs";

    /**
     * Loads the SIFT-small base vectors as a list of {@code (primaryKey, vector)} records, in
     * insertion order. Useful for tests that need to look up the original vector for a given
     * item id (since {@link #insertSIFTSmall} no longer surfaces it).
     */
    @Nonnull
    static List<PrimaryKeyAndVector> loadSiftSmall() throws Exception {
        return loadVectors(SIFT_SMALL_BASE_PATH, 10_000);
    }

    /**
     * Loads the first {@code numVectors} SIFT-1M base vectors as a list of
     * {@code (primaryKey, vector)} records, in insertion order. See {@link #loadSiftSmall} for
     * usage notes.
     * <p>
     * <b>Memory warning:</b> the entire requested slice is materialized in memory. SIFT-1M
     * vectors are 128-dim doubles, so each entry is roughly 1 KB; calling this with
     * {@code numVectors = 1_000_000} allocates about 1 GB. If the caller only needs to
     * <i>insert</i> vectors (not look them up later), use {@link #insertSIFT1m} instead — that
     * streams the file one batch at a time and never holds the whole dataset in memory.
     */
    @Nonnull
    static List<PrimaryKeyAndVector> loadSift1m(final int numVectors) throws Exception {
        return loadVectors(SIFT_1M_BASE_PATH, numVectors);
    }

    @Nonnull
    static List<PrimaryKeyAndVector> loadVectors(@Nonnull final String baseFile,
                                                 final int numVectors) throws Exception {
        final Path basePath = Paths.get(baseFile);

        final ImmutableList.Builder<PrimaryKeyAndVector> insertedDataBuilder = ImmutableList.builder();

        try (final var fileChannel = FileChannel.open(basePath, StandardOpenOption.READ)) {
            final Iterator<DoubleRealVector> vectorIterator = new StoredVecsIterator.StoredFVecsIterator(fileChannel);

            int i = 0;
            while (vectorIterator.hasNext() && i < numVectors) {
                final DoubleRealVector currentVector = vectorIterator.next();
                final Tuple currentPrimaryKey = createPrimaryKey(i++);
                insertedDataBuilder.add(new PrimaryKeyAndVector(currentPrimaryKey, currentVector));
            }
        }
        return insertedDataBuilder.build();
    }

    /**
     * Two disjoint deterministic samples drawn from the SIFT-1M base file in a single streaming
     * pass, never holding the full dataset in memory.
     *
     * @param a first sample
     * @param b second sample, disjoint from {@code a}
     */
    record Samples(@Nonnull List<PrimaryKeyAndVector> a, @Nonnull List<PrimaryKeyAndVector> b) {
    }

    /**
     * Single-pass reservoir-sampled disjoint pair of samples drawn from
     * {@link #SIFT_1M_BASE_PATH}. Streams the file once; resident memory is bounded at roughly
     * {@code (sizeA + sizeB) * 1KB} regardless of how big the source file is. The two returned
     * lists are guaranteed disjoint by primary key (since each PK is the original index in the
     * SIFT-1M stream).
     * <p>
     * The whole sampling process is deterministic given {@code seed}: same seed → same two lists,
     * in the same order. The order within each list is a fresh deterministic shuffle of the
     * combined reservoir before splitting, so callers get a reasonable insertion/deletion order
     * for free without having to reshuffle.
     *
     * @param seed seed for the reservoir's random replacement decisions and the post-pass shuffle
     * @param sizeA size of the first sample
     * @param sizeB size of the second sample
     * @return a {@link Samples} pair
     */
    @Nonnull
    static Samples loadDisjointSamplesFromSift1m(final long seed,
                                                 final int sizeA,
                                                 final int sizeB) throws IOException {
        Verify.verify(sizeA > 0 && sizeB > 0, "sample sizes must be positive (sizeA=%s, sizeB=%s)", sizeA, sizeB);
        final int totalSize = sizeA + sizeB;

        // Two parallel arrays so we don't pay an Object[] indirection per slot.
        final long[] reservoirIndices = new long[totalSize];
        final DoubleRealVector[] reservoirVectors = new DoubleRealVector[totalSize];

        // SplittableRandom for the reservoir replacement decisions; deterministic from seed.
        final SplittableRandom rnd = new SplittableRandom(seed);

        long total = 0L;
        final Path basePath = Paths.get(SIFT_1M_BASE_PATH);
        try (final var fileChannel = FileChannel.open(basePath, StandardOpenOption.READ)) {
            final Iterator<DoubleRealVector> it = new StoredVecsIterator.StoredFVecsIterator(fileChannel);
            while (it.hasNext()) {
                final DoubleRealVector v = it.next();
                if (total < totalSize) {
                    // Initial fill: items go to slot=total unconditionally.
                    reservoirIndices[(int) total] = total;
                    reservoirVectors[(int) total] = v;
                } else {
                    // Algorithm R: pick j uniformly in [0, total]; if j < totalSize, replace.
                    // total fits in long but the modulus we need is total + 1 ≤ 1M for SIFT-1M;
                    // call out the cast and let JVM verify.
                    final long bound = total + 1L;
                    final long j = rnd.nextLong(bound);
                    if (j < totalSize) {
                        reservoirIndices[(int) j] = total;
                        reservoirVectors[(int) j] = v;
                    }
                }
                total++;
            }
        }

        Verify.verify(total >= totalSize,
                "SIFT-1M file produced %s records, fewer than requested totalSize=%s", total, totalSize);

        // Build records and shuffle deterministically before splitting. The shuffle randomizes
        // which reservoir slots end up in setA vs setB and gives callers a usable iteration order.
        final List<PrimaryKeyAndVector> sampled = new ArrayList<>(totalSize);
        for (int i = 0; i < totalSize; i++) {
            sampled.add(new PrimaryKeyAndVector(
                    createPrimaryKey(reservoirIndices[i]),
                    reservoirVectors[i]));
        }
        // Mix the seed with a salt so the reservoir RNG and the shuffle RNG don't share state.
        Collections.shuffle(sampled, new Random(seed ^ 0xA5A5_5A5A_A5A5_5A5AL));

        return new Samples(
                ImmutableList.copyOf(sampled.subList(0, sizeA)),
                ImmutableList.copyOf(sampled.subList(sizeA, totalSize)));
    }

    static void insertVectors(@Nonnull final Database db,
                              @Nonnull final Guardiann guardiann,
                              @Nonnull final String baseFile,
                              final int numVectors,
                              final int desiredBatchSize) throws Exception {
        final Path siftPath = Paths.get(baseFile);

        final TestOnReadListener onReadListener = (TestOnReadListener)guardiann.getOnReadListener();
        final TestOnWriteListener onWriteListener = (TestOnWriteListener)guardiann.getOnWriteListener();
        onReadListener.pushFrame();
        onWriteListener.pushFrame();

        try (final var fileChannel = FileChannel.open(siftPath, StandardOpenOption.READ)) {
            final Iterator<DoubleRealVector> vectorIterator = new StoredVecsIterator.StoredFVecsIterator(fileChannel);

            int i = 0;
            while (vectorIterator.hasNext() && i < numVectors) {
                onReadListener.pushFrame();
                onWriteListener.pushFrame();

                final int batchSize = Math.min(desiredBatchSize, numVectors - i);
                final long beginTs = System.nanoTime();
                final List<DoubleRealVector> remainingBatch =
                        Lists.newArrayList(Iterators.limit(vectorIterator, batchSize));
                while (!remainingBatch.isEmpty()) {
                    final long currentBatchStart = i;
                    final List<PrimaryKeyAndVector> insertedInBatch =
                            basicInsertBatch(db, guardiann, remainingBatch.size(), i,
                                    (tr, nextId) -> {
                                        final int indexInBatch = Math.toIntExact(nextId - currentBatchStart);
                                        if (indexInBatch >= remainingBatch.size()) {
                                            return null;
                                        }
                                        final Tuple currentPrimaryKey = createPrimaryKey(nextId);
                                        final DoubleRealVector doubleVector = remainingBatch.get(indexInBatch);
                                        return new PrimaryKeyAndVector(currentPrimaryKey, doubleVector);
                                    });
                    final int numInsertedInBatch = insertedInBatch.size();
                    i += numInsertedInBatch;
                    remainingBatch.subList(0, numInsertedInBatch).clear();
                }
                final long endTs = System.nanoTime();
                final long bytesRead = onReadListener.getBytesRead();
                final long bytesWritten = onWriteListener.getBytesWritten();

                logger.info("inserted batchSize={} for a total of numRecords={} took elapsedTime={}ms, bytesRead={}, bytesWritten={}, taskCountByKind={}",
                        batchSize, i, TimeUnit.NANOSECONDS.toMillis(endTs - beginTs),
                        bytesRead, bytesWritten, onWriteListener.getNumTasksEnqueuedByKind());

                onWriteListener.popFrame();
                onReadListener.popFrame();
            }
            assertThat(i).isEqualTo(numVectors);
        }
        logger.info("total number of tasks enqueued by kind={}", onWriteListener.getNumTasksEnqueuedByKind());
        logger.info("total number of tasks executed by kind={}", onWriteListener.getNumTasksExecutedByKind());

        onWriteListener.popFrame();
        onReadListener.popFrame();
    }

    static void insertFirstRepeatedly(@Nonnull final Database db,
                                      @Nonnull final Guardiann guardiann,
                                      @Nonnull final String baseFile,
                                      final int numRepetitions,
                                      final int desiredBatchSize) throws Exception {
        final Path siftPath = Paths.get(baseFile);

        try (final var fileChannel = FileChannel.open(siftPath, StandardOpenOption.READ)) {
            final Iterator<DoubleRealVector> vectorIterator = new StoredVecsIterator.StoredFVecsIterator(fileChannel);

            if (!vectorIterator.hasNext()) {
                return;
            }

            int i = 0;
            final DoubleRealVector onlyVector = vectorIterator.next();

            while (i < numRepetitions) {
                final int batchSize = Math.min(desiredBatchSize, numRepetitions - i);
                final List<DoubleRealVector> remainingBatch =
                        IntStream.range(0, batchSize).mapToObj(ignored -> onlyVector)
                                .collect(Collectors.toList());
                while (!remainingBatch.isEmpty()) {
                    final long currentBatchStart = i;
                    final List<PrimaryKeyAndVector> insertedInBatch =
                            basicInsertBatch(db, guardiann, remainingBatch.size(), i,
                                    (tr, nextId) -> {
                                        final int indexInBatch = Math.toIntExact(nextId - currentBatchStart);
                                        if (indexInBatch >= remainingBatch.size()) {
                                            return null;
                                        }
                                        final Tuple currentPrimaryKey = createPrimaryKey(-1 - nextId);
                                        final DoubleRealVector doubleVector = remainingBatch.get(indexInBatch);
                                        return new PrimaryKeyAndVector(currentPrimaryKey, doubleVector);
                                    });
                    final int numInsertedInBatch = insertedInBatch.size();
                    i += numInsertedInBatch;
                    remainingBatch.subList(0, numInsertedInBatch).clear();
                }
            }
        }
    }

    static void queryVectors(@Nonnull final Database db,
                             @Nonnull final Guardiann guardiann,
                             @Nonnull final String queriesFile,
                             @Nonnull final String groundTruthFile,
                             final int k) throws IOException {
        queryVectors(db, guardiann, queriesFile, groundTruthFile, k, -1);
    }

    /**
     * Observational variant of recall validation: runs every query in {@code queriesFile}, logs
     * per-query timing/read-bytes/recall, and does not assert any threshold. Use
     * {@link #assertRecallAtKAtLeast} when a test wants to enforce a recall floor.
     * <p>
     * Recall is computed via {@link #singleQueryRecall} (set-based intersection of deduplicated
     * results against ground truth) so duplicate primary keys in the result set don't inflate
     * the per-query score.
     */
    static void queryVectors(@Nonnull final Database db,
                             @Nonnull final Guardiann guardiann,
                             @Nonnull final String queriesFile,
                             @Nonnull final String groundTruthFile,
                             final int k,
                             final int maxIndex) throws IOException {
        final List<DoubleRealVector> queries = loadSiftQueryVectors(queriesFile);
        final List<Set<Integer>> groundTruth = loadSiftGroundTruth(groundTruthFile, maxIndex);
        Verify.verify(queries.size() == groundTruth.size(),
                "queries (%s) and ground truth (%s) must align", queries.size(), groundTruth.size());

        final TestOnReadListener onReadListener = (TestOnReadListener) guardiann.getOnReadListener();
        final int efSearch = (int) ((double) k * 1.15);

        for (int i = 0; i < queries.size(); i++) {
            final Set<Integer> truth = groundTruth.get(i);
            if (truth.isEmpty()) {
                logger.info("query ground truth does not have indices that have been inserted yet");
                continue;
            }
            final DoubleRealVector queryVector = queries.get(i);
            onReadListener.pushFrame();
            final long beginTs = System.nanoTime();
            final List<? extends ResultEntry> results =
                    db.run(tr -> guardiann.kNearestNeighborsSearch(tr, k, efSearch,
                            48, 16, 1.50d, true, queryVector).join());
            final long endTs = System.nanoTime();
            logger.info("retrieved result in elapsedTimeMs={}, reading readBytes={}",
                    TimeUnit.NANOSECONDS.toMillis(endTs - beginTs), onReadListener.getBytesRead());

            final double recall = singleQueryRecall(truth, results);
            logger.info("query returned results recall={}, k={}",
                    String.format(Locale.ROOT, "%.2f", recall * 100.0d), truth.size());
            onReadListener.popFrame();
        }
    }

    /**
     * Set-based recall@k for a single query: {@code |dedup(results) ∩ groundTruth| / |groundTruth|}.
     * Deduplicating the result set by primary key matters when the search can surface multiple
     * references to the same primary (e.g. a primary and its replicas both appearing in the
     * top-k); without dedup, a single in-truth primary would otherwise be counted once per
     * duplicate.
     */
    private static double singleQueryRecall(@Nonnull final Set<Integer> groundTruthIndices,
                                            @Nonnull final List<? extends ResultEntry> results) {
        final Set<Integer> resultIndices = results.stream()
                .map(re -> (int) re.primaryKey().getLong(0))
                .collect(ImmutableSet.toImmutableSet());
        final long hits = resultIndices.stream().filter(groundTruthIndices::contains).count();
        return (double) hits / groundTruthIndices.size();
    }

    /**
     * Loads query vectors from a SIFT-style {@code .fvecs} file as {@link DoubleRealVector}s
     * (matching the representation used by the insert helpers — {@link Guardiann}'s public API
     * accepts any {@link RealVector}, so there's no need to pre-quantize to half-precision).
     */
    @Nonnull
    static List<DoubleRealVector> loadSiftQueryVectors(@Nonnull final String queriesFile) throws IOException {
        final ImmutableList.Builder<DoubleRealVector> queries = ImmutableList.builder();
        try (final var channel = FileChannel.open(Paths.get(queriesFile), StandardOpenOption.READ)) {
            final Iterator<DoubleRealVector> iterator = new StoredVecsIterator.StoredFVecsIterator(channel);
            while (iterator.hasNext()) {
                queries.add(iterator.next());
            }
        }
        return queries.build();
    }

    /**
     * Loads per-query ground-truth top-k index sets from a SIFT-style {@code .ivecs} file.
     * Indices greater than {@code maxIndex} are filtered out; pass {@code -1} to keep all.
     */
    @Nonnull
    static List<Set<Integer>> loadSiftGroundTruth(@Nonnull final String groundTruthFile,
                                                  final int maxIndex) throws IOException {
        final ImmutableList.Builder<Set<Integer>> truth = ImmutableList.builder();
        try (final var channel = FileChannel.open(Paths.get(groundTruthFile), StandardOpenOption.READ)) {
            final Iterator<List<Integer>> iterator = new StoredVecsIterator.StoredIVecsIterator(channel);
            while (iterator.hasNext()) {
                truth.add(iterator.next().stream()
                        .filter(idx -> maxIndex < 0 || idx <= maxIndex)
                        .collect(ImmutableSet.toImmutableSet()));
            }
        }
        return truth.build();
    }

    /**
     * Asserts that the mean set-based recall@k across the given queries meets or exceeds
     * {@code minMeanRecall}. Queries whose ground-truth set is empty (e.g. when {@code maxIndex}
     * excluded every truth index for that query) are skipped.
     * <p>
     * Recall is computed via {@link #singleQueryRecall} — deduplicated result-set intersection
     * with ground truth. Search parameters ({@code efSearch}, etc.) match {@link #queryVectors}
     * so the two helpers report comparable numbers.
     *
     * @param queries pre-loaded query vectors (see {@link #loadSiftQueryVectors})
     * @param groundTruth pre-loaded per-query ground-truth index sets (see
     *        {@link #loadSiftGroundTruth}); must have the same size as {@code queries}
     * @param k top-k to retrieve per query
     * @param minMeanRecall floor that the mean recall must meet or exceed
     */
    static void assertRecallAtKAtLeast(@Nonnull final Database db,
                                       @Nonnull final Guardiann guardiann,
                                       @Nonnull final List<? extends RealVector> queries,
                                       @Nonnull final List<? extends Set<Integer>> groundTruth,
                                       final int k,
                                       final double minMeanRecall) {
        Verify.verify(queries.size() == groundTruth.size(),
                "queries (%s) and groundTruth (%s) must align", queries.size(), groundTruth.size());
        final int efSearch = (int) ((double) k * 1.15);
        double sumRecall = 0.0;
        int countedQueries = 0;
        for (int i = 0; i < queries.size(); i++) {
            final Set<Integer> truth = ImmutableSet.copyOf(groundTruth.get(i));
            if (truth.isEmpty()) {
                continue;
            }
            final RealVector q = queries.get(i);
            final List<? extends ResultEntry> results =
                    db.run(tr -> guardiann.kNearestNeighborsSearch(tr, k, efSearch,
                            48, 16, 1.50d, true, q).join());
            sumRecall += singleQueryRecall(truth, results);
            countedQueries++;
        }
        assertThat(countedQueries)
                .as("at least one query must have non-empty ground truth")
                .isGreaterThan(0);
        final double meanRecall = sumRecall / countedQueries;
        logger.info("assertRecallAtKAtLeast: mean recall@{} = {} over {} queries (threshold {})",
                k, String.format(Locale.ROOT, "%.4f", meanRecall), countedQueries, minMeanRecall);
        assertThat(meanRecall)
                .as("mean recall@%d over %d queries", k, countedQueries)
                .isGreaterThanOrEqualTo(minMeanRecall);
    }

    /**
     * Like {@link #assertRecallAtKAtLeast} but computes ground truth on the fly from the given
     * {@code active} map of {@code (primaryKey, vector)} entries — i.e. brute-force squared-L2
     * top-{@code k} per query. Useful when the active set is a moving subset of the original
     * universe (insert/delete churn) so the static {@code .ivecs} ground truth no longer applies.
     * <p>
     * Implementation note: scoring uses {@link RealVector#l2SquaredDistance} for ordering — the
     * monotonic-square shortcut to skip a {@code sqrt} that the Euclidean estimator would just
     * undo. Results are compared with the kNN search via {@link #singleQueryRecall} (deduplicated
     * set intersection) so the threshold is comparable to the static-truth helper.
     *
     * @param queries query vectors to score against
     * @param active the current active set, keyed by {@link Tuple} primary key
     * @param k top-k to retrieve from both brute force and the index
     * @param minMeanRecall floor that the mean recall must meet or exceed
     * @return the observed mean recall (for logging/assertions in the caller)
     */
    static double assertRecallAtKAtLeastDynamic(@Nonnull final Database db,
                                                @Nonnull final Guardiann guardiann,
                                                @Nonnull final List<? extends RealVector> queries,
                                                @Nonnull final Map<Tuple, ? extends RealVector> active,
                                                final int k,
                                                final double minMeanRecall) {
        Verify.verify(active.size() >= k,
                "active set (%s) must have at least k=%s entries for a meaningful recall check",
                active.size(), k);
        final int efSearch = (int) ((double) k * 1.15);
        double sumRecall = 0.0d;
        int countedQueries = 0;
        for (final RealVector query : queries) {
            final Set<Integer> truth = bruteForceTopKByEuclidean(query, active, k);
            if (truth.isEmpty()) {
                continue;
            }
            final List<? extends ResultEntry> results =
                    db.run(tr -> guardiann.kNearestNeighborsSearch(tr, k, efSearch,
                            48, 16, 1.50d, true, query).join());
            sumRecall += singleQueryRecall(truth, results);
            countedQueries++;
        }
        assertThat(countedQueries)
                .as("at least one query must produce non-empty truth (active=%s, k=%s)", active.size(), k)
                .isGreaterThan(0);
        final double meanRecall = sumRecall / countedQueries;
        logger.info("assertRecallAtKAtLeastDynamic: mean recall@{} = {} over {} queries (active={}, threshold={})",
                k, String.format(Locale.ROOT, "%.4f", meanRecall),
                countedQueries, active.size(), minMeanRecall);
        assertThat(meanRecall)
                .as("dynamic mean recall@%d over %d queries (active=%d)",
                        k, countedQueries, active.size())
                .isGreaterThanOrEqualTo(minMeanRecall);
        return meanRecall;
    }

    /**
     * Brute-force top-{@code k} primary-key indices in {@code active} by squared Euclidean
     * distance to {@code query}. Maintains a max-heap of size {@code k} so the per-query work is
     * {@code O(|active| * log k)} (and {@code O(|active| * d)} for the dot products); for the
     * test workloads here that's sub-second on 10k × 128-dim vectors per query.
     * <p>
     * Returned indices are extracted as {@code (int) primaryKey.getLong(0)}, matching the
     * convention used by {@link #singleQueryRecall} so the two are directly comparable.
     */
    @Nonnull
    static Set<Integer> bruteForceTopKByEuclidean(@Nonnull final RealVector query,
                                                  @Nonnull final Map<Tuple, ? extends RealVector> active,
                                                  final int k) {
        // Local record so we don't need a top-level helper class.
        record IndexedDistance(double distance, int index) { }
        final PriorityQueue<IndexedDistance> heap = new PriorityQueue<>(Math.max(1, k),
                Comparator.comparingDouble(IndexedDistance::distance).reversed());
        for (final Map.Entry<Tuple, ? extends RealVector> e : active.entrySet()) {
            final double d = query.l2SquaredDistance(e.getValue());
            final int idx = (int) e.getKey().getLong(0);
            if (heap.size() < k) {
                heap.add(new IndexedDistance(d, idx));
            } else if (d < Objects.requireNonNull(heap.peek()).distance()) {
                heap.poll();
                heap.add(new IndexedDistance(d, idx));
            }
        }
        return heap.stream().map(IndexedDistance::index).collect(ImmutableSet.toImmutableSet());
    }

    static List<RealVector> readQueryVectors(@Nonnull final String queriesFile) throws IOException {
        final ImmutableList.Builder<RealVector> resultBuilder = ImmutableList.builder();
        final Path queryPath = Paths.get(queriesFile);

        try (final var queryChannel = FileChannel.open(queryPath, StandardOpenOption.READ)) {
            final Iterator<DoubleRealVector> queryIterator = new StoredVecsIterator.StoredFVecsIterator(queryChannel);

            while (queryIterator.hasNext()) {
                final DoubleRealVector queryVector = queryIterator.next();

                resultBuilder.add(queryVector);
            }
        }
        return resultBuilder.build();
    }

    static class TestOnWriteListener implements OnWriteListener {
        @Nonnull
        private final ArrayDeque<Frame> frames;

        public TestOnWriteListener() {
            this.frames = new ArrayDeque<>();
        }

        @Override
        public void onKeyValueWritten(@Nonnull final byte[] key, @Nonnull final byte[] value) {
            for (final Frame frame : frames) {
                frame.bytesWritten().addAndGet(key.length + value.length);
            }
        }

        @Override
        public void onTaskEnqueued(@Nonnull final AbstractDeferredTask.Kind taskKind,
                                   @Nonnull final UUID taskId, @Nonnull final Set<UUID> targetClusterIds) {
            for (final Frame frame : frames) {
                frame.numTasksEnqueuedByKind()
                        .compute(taskKind, (ignored, counter) ->
                                Objects.requireNonNullElse(counter, 0) + 1);
            }
        }

        @Override
        public void onTaskExecuted(@Nonnull final AbstractDeferredTask.Kind taskKind,
                                   @Nonnull final UUID taskId, @Nonnull final Set<UUID> targetClusterIds) {
            for (final Frame frame : frames) {
                frame.numTasksExecutedByKind().compute(taskKind, (ignored, counter) ->
                        Objects.requireNonNullElse(counter, 0) + 1);
            }
        }

        @Nonnull
        public Map<AbstractDeferredTask.Kind, Integer> getNumTasksEnqueuedByKind() {
            return Objects.requireNonNull(frames.peek()).numTasksEnqueuedByKind();
        }

        @Nonnull
        public Map<AbstractDeferredTask.Kind, Integer> getNumTasksExecutedByKind() {
            return Objects.requireNonNull(frames.peek()).numTasksExecutedByKind();
        }

        public int getSumTaskExecutedCounters() {
            return Objects.requireNonNull(frames.peek()).numTasksExecutedByKind().values().stream().mapToInt(i -> i).sum();
        }

        @Nonnull
        public Long getBytesWritten() {
            return Objects.requireNonNull(frames.peek()).bytesWritten().get();
        }

        public void pushFrame() {
            frames.push(new Frame(new AtomicLong(0L), Maps.newConcurrentMap(), Maps.newConcurrentMap()));
        }

        public void popFrame() {
            frames.pop();
        }

        private record Frame(@Nonnull AtomicLong bytesWritten,
                             @Nonnull Map<AbstractDeferredTask.Kind, Integer> numTasksEnqueuedByKind,
                             @Nonnull Map<AbstractDeferredTask.Kind, Integer> numTasksExecutedByKind) {
        }
    }

    static class TestOnReadListener implements OnReadListener {
        @Nonnull
        private final ArrayDeque<AtomicLong> frames;

        public TestOnReadListener() {
            this.frames = new ArrayDeque<>();
        }

        public long getBytesRead() {
            return Objects.requireNonNull(frames.peek()).get();
        }

        @Override
        public void onKeyValueRead(@Nonnull final byte[] key, @Nullable final byte[] value) {
            for (final AtomicLong frame : frames) {
                frame.addAndGet(key.length + (value == null ? 0 : value.length));
            }
        }

        public void pushFrame() {
            frames.push(new AtomicLong(0L));
        }

        public void popFrame() {
            frames.pop();
        }
    }

    /**
     * Safety bound for {@link #runToQuiescence}. A healthy drain finishes well under this; if it
     * doesn't, the structure is producing tasks faster than they get retired and something is
     * wrong.
     */
    private static final int MAX_DRAIN_ITERATIONS = 1_000;

    /**
     * Drains all pending deferred tasks from {@code guardiann} by repeatedly fetching one task
     * and executing it in its own transaction until the tasks subspace is empty.
     * <p>
     * Insertions and deletions piggy-back deferred-task execution onto themselves (one task per
     * op, via {@link Primitives#doSomeDeferredTasks}). Once those producer ops stop, pending
     * tasks remain in the tasks subspace until something pulls them out — this method is that
     * something. Tests typically call it before checking post-condition invariants so the
     * structure is observed at a quiescent state.
     * <p>
     * Note that {@link BounceTask} is a state machine: each invocation executes one of its
     * dependent tasks and then either re-enqueues a new BounceTask with the remaining
     * dependents OR fires the final task once the last dependent has run. Draining therefore
     * takes roughly N+1 iterations per BounceTask (N dependents + the final task), and
     * additional tasks (e.g. a split firing a follow-up) may be enqueued mid-drain. The loop
     * here simply keeps going until the tasks subspace reads empty.
     *
     * @return the number of tasks executed during the drain
     */
    static int runToQuiescence(@Nonnull final Database db, @Nonnull final Guardiann guardiann) {
        final Primitives primitives = guardiann.getLocator().primitives();
        int executed = 0;
        for (int i = 0; i < MAX_DRAIN_ITERATIONS; i++) {
            final boolean didWork = db.run(transaction -> {
                final AccessInfo accessInfo = primitives.fetchAccessInfo(transaction).join();
                if (accessInfo == null) {
                    return false;
                }
                final List<AbstractDeferredTask> pending =
                        primitives.fetchSomeDeferredTasks(transaction, accessInfo, 1).join();
                if (pending.isEmpty()) {
                    return false;
                }
                primitives.doDeferredTask(transaction, pending.get(0)).join();
                return true;
            });
            if (!didWork) {
                if (executed > 0) {
                    logger.info("runToQuiescence drained {} tasks", executed);
                }
                return executed;
            }
            executed++;
        }
        throw new IllegalStateException("runToQuiescence did not converge after "
                + MAX_DRAIN_ITERATIONS + " iterations; possible task-loop bug");
    }

    /**
     * Asserts that the tasks subspace is empty — i.e. {@link #runToQuiescence} would execute
     * zero tasks if called now. Tests typically call {@link #runToQuiescence} first, then this,
     * to confirm the drain truly settled.
     */
    static void assertQuiescence(@Nonnull final Database db, @Nonnull final Guardiann guardiann) {
        final Primitives primitives = guardiann.getLocator().primitives();
        final int pending = db.run(transaction -> {
            final AccessInfo accessInfo = primitives.fetchAccessInfo(transaction).join();
            if (accessInfo == null) {
                return 0;
            }
            return primitives.fetchSomeDeferredTasks(transaction, accessInfo, 1).join().size();
        });
        assertThat(pending).as("deferred tasks remaining at quiescence check").isEqualTo(0);
    }

    /**
     * Per-cluster view captured by {@link #snapshotStructure}. The vector-reference sets are keyed
     * by {@link VectorId} (primary key + UUID), which uniquely identifies a vector across the
     * structure and supports cross-cluster cross-referencing (e.g. linking a replica to its
     * primary).
     *
     * @param clusterId the cluster's UUID
     * @param centroid the centroid in the client (untransformed) coordinate space, taken from the
     *        centroid HNSW. Useful for stable cross-snapshot comparison
     * @param metadata the cluster's stored {@link ClusterMetadata}
     * @param primaries primary-copy vector references (whether or not underreplicated; collapsed
     *        primaries are excluded from this set and appear in {@code collapsedRefs} instead)
     * @param replicas replicated copies held in this cluster for search recall
     * @param collapsedRefs collapsed references; each carries its signature in its {@code id()}'s
     *        primary-key tuple
     */
    record ClusterView(@Nonnull UUID clusterId,
                       @Nonnull RealVector centroid,
                       @Nonnull ClusterMetadata metadata,
                       @Nonnull Set<VectorId> primaries,
                       @Nonnull Set<VectorId> replicas,
                       @Nonnull Set<VectorId> collapsedRefs) {
    }

    /**
     * Compact, immutable view of the Guardiann's cluster topology at a point in time: per-cluster
     * metadata, primary vectors, replicas, and collapsed references. Tests build one (typically
     * right after {@link #runToQuiescence}) and feed it to the {@code assert*} invariant helpers
     * below. The same snapshots are intended for diff-based scenario tests in a later phase
     * ("after this op, exactly these clusters changed"); the diff API isn't built yet.
     *
     * @param clusters per-cluster views, keyed by cluster id (immutable)
     */
    record StructureSnapshot(@Nonnull Map<UUID, ClusterView> clusters) {
        /** Returns the number of clusters in this snapshot. */
        public int numClusters() {
            return clusters.size();
        }

        /** Returns the total primary-copy count across all clusters. */
        public int totalPrimaries() {
            return clusters.values().stream().mapToInt(c -> c.primaries().size()).sum();
        }

        /** Returns the total replica count across all clusters (counts each replica copy once). */
        public int totalReplicas() {
            return clusters.values().stream().mapToInt(c -> c.replicas().size()).sum();
        }

        /** Returns the total collapsed-reference count across all clusters. */
        public int totalCollapsedRefs() {
            return clusters.values().stream().mapToInt(c -> c.collapsedRefs().size()).sum();
        }

        /**
         * Returns the reverse mapping primary {@link VectorId} → owning cluster id. The build
         * itself fails via {@link Verify} if a primary appears in more than one cluster, so the
         * returned map is guaranteed unique by construction.
         */
        @Nonnull
        public Map<VectorId, UUID> primaryOwners() {
            final Map<VectorId, UUID> owners = Maps.newHashMapWithExpectedSize(totalPrimaries());
            for (final ClusterView cv : clusters.values()) {
                for (final VectorId id : cv.primaries()) {
                    final UUID prior = owners.put(id, cv.clusterId());
                    Verify.verify(prior == null,
                            "primary %s appears in clusters %s and %s", id, prior, cv.clusterId());
                }
            }
            return owners;
        }
    }

    /**
     * Snapshots the current Guardiann cluster topology. Returns {@code null} if the structure is
     * empty (no clusters yet — common before the first insert).
     * <p>
     * Implementation: scan the centroid HNSW for cluster ids and centroids (this uses its own
     * internal transactions), then fetch each cluster's metadata and vector references in a
     * single follow-up read transaction.
     */
    @Nullable
    static StructureSnapshot snapshotStructure(@Nonnull final Database db,
                                               @Nonnull final Guardiann guardiann) {
        final Primitives primitives = guardiann.getLocator().primitives();
        final HNSW centroidsHnsw = primitives.getClusterCentroidsHnsw();

        // First, pull (clusterId, untransformedCentroid) pairs from the centroid HNSW.
        final List<ResultEntry> centroidEntries = Lists.newArrayList();
        HNSW.scanLayer(centroidsHnsw.getConfig(), centroidsHnsw.getSubspace(), db, 0, 100,
                centroidEntries::add);
        if (centroidEntries.isEmpty()) {
            return null;
        }

        // Then, in one read transaction, fetch every cluster's metadata + vector references and
        // bucketize the references into primaries / replicas / collapsed.
        final Map<UUID, ClusterView> views = db.run(transaction -> {
            final AccessInfo accessInfo = primitives.fetchAccessInfo(transaction).join();
            Verify.verifyNotNull(accessInfo,
                    "centroid HNSW had entries but AccessInfo is missing");
            final StorageTransform storageTransform = primitives.storageTransform(accessInfo);

            final Map<UUID, ClusterView> built = Maps.newHashMapWithExpectedSize(centroidEntries.size());
            for (final ResultEntry entry : centroidEntries) {
                final UUID clusterId = StorageAdapter.clusterIdFromTuple(entry.primaryKey());
                final RealVector untransformedCentroid =
                        Objects.requireNonNull(entry.vector(), "centroid HNSW must yield vectors");
                final Cluster cluster = primitives.fetchCluster(transaction, storageTransform,
                        clusterId, untransformedCentroid).join();

                final ImmutableSet.Builder<VectorId> primariesBuilder = ImmutableSet.builder();
                final ImmutableSet.Builder<VectorId> replicasBuilder = ImmutableSet.builder();
                final ImmutableSet.Builder<VectorId> collapsedBuilder = ImmutableSet.builder();
                for (final VectorReference ref : cluster.vectorReferences()) {
                    if (ref.isCollapsed()) {
                        collapsedBuilder.add(ref.id());
                    } else if (ref.isPrimaryCopy()) {
                        primariesBuilder.add(ref.id());
                    } else {
                        replicasBuilder.add(ref.id());
                    }
                }
                built.put(clusterId, new ClusterView(clusterId, untransformedCentroid,
                        cluster.clusterMetadata(),
                        primariesBuilder.build(), replicasBuilder.build(), collapsedBuilder.build()));
            }
            return built;
        });
        return new StructureSnapshot(Map.copyOf(views));
    }

    /**
     * Asserts that every primary {@link VectorId} appears in exactly one cluster's primary set —
     * no orphans, no duplicates across clusters. The construction of
     * {@link StructureSnapshot#primaryOwners()} performs this check internally via
     * {@link Verify}; this method exposes it as a named assertion for tests that want to opt in
     * explicitly. Empty snapshots (no clusters) trivially pass.
     */
    static void assertEveryPrimaryUniqueAndAccountedFor(@Nullable final StructureSnapshot snapshot) {
        if (snapshot == null) {
            return;
        }
        // primaryOwners() rebuilds the reverse map; the build asserts uniqueness via Verify.
        final Map<VectorId, UUID> owners = snapshot.primaryOwners();
        assertThat(owners.size())
                .as("primaryOwners size must match total primary count")
                .isEqualTo(snapshot.totalPrimaries());
    }

    /**
     * Asserts that every replica {@link VectorId} in any cluster has a corresponding primary copy
     * somewhere in the structure. A dangling replica (no live primary with the same VectorId)
     * indicates the primary was deleted but its replicas weren't reaped, or that a replicate-only
     * insert path slipped through.
     */
    static void assertReplicasReferenceLivePrimaries(@Nullable final StructureSnapshot snapshot) {
        if (snapshot == null) {
            return;
        }
        final Set<VectorId> livePrimaries = snapshot.primaryOwners().keySet();
        for (final ClusterView cv : snapshot.clusters().values()) {
            for (final VectorId replicaId : cv.replicas()) {
                assertThat(livePrimaries)
                        .as("replica %s in cluster %s has no live primary", replicaId, cv.clusterId())
                        .contains(replicaId);
            }
        }
    }

    /**
     * Umbrella post-condition check for any test that has just performed structural operations
     * (inserts, deletes, repartitions, etc.). Runs the deferred-task queue to quiescence,
     * verifies that nothing remains pending, then snapshots and validates every structural
     * invariant currently checkable on a quiescent snapshot.
     * <p>
     * Equivalent to the explicit sequence:
     * <pre>{@code
     * runToQuiescence(db, guardiann);
     * assertQuiescence(db, guardiann);
     * final StructureSnapshot s = snapshotStructure(db, guardiann);
     * assertEveryPrimaryUniqueAndAccountedFor(s);
     * assertReplicasReferenceLivePrimaries(s);
     * }</pre>
     * but bundled into one call so scenario tests don't have to repeat the boilerplate. Tests
     * that want finer control (e.g. snapshotting before and after for a diff) should call the
     * individual helpers instead.
     */
    static void assertGuardiannInvariants(@Nonnull final Database db,
                                          @Nonnull final Guardiann guardiann) {
        runToQuiescence(db, guardiann);
        assertQuiescence(db, guardiann);
        final StructureSnapshot snapshot = snapshotStructure(db, guardiann);
        assertEveryPrimaryUniqueAndAccountedFor(snapshot);
        assertReplicasReferenceLivePrimaries(snapshot);
    }
}
