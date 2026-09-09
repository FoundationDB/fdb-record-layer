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
import com.apple.foundationdb.async.common.TopK;
import com.apple.foundationdb.linear.DoubleRealVector;
import com.apple.foundationdb.linear.RealVector;
import com.apple.foundationdb.linear.StoredVecsIterator;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
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

import static com.apple.foundationdb.async.common.CommonTestHelpers.createPrimaryKey;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test helpers for testing {@link Guardiann}s.
 */
@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
class TestHelpers {
    private static final Logger logger = LoggerFactory.getLogger(TestHelpers.class);

    // Parameters for assertOrderedByDistanceQualityAtLeast: it asks for the entire dataset (k, the reorder
    // window, and the probed-cluster cap are all set to the index size), ordered from the start of the cursor,
    // so a "perfect" result is every live vector in (distance, primaryKey) order.
    /** "From the start" cluster cursor: skip no cluster centroid by radius. */
    private static final double ORDERED_BY_DISTANCE_FROM_START_MIN_RADIUS_CLUSTER = 0.0d;
    /** "From the start" result cursor: keep every result regardless of distance. */
    private static final double ORDERED_BY_DISTANCE_FROM_START_MIN_RADIUS = Double.NEGATIVE_INFINITY;
    /**
     * Upper bound on the live-vector count for which {@link #assertOrderedByDistanceQualityAtLeast} actually runs.
     * Unlike the pruned kNN search, {@code searchOrderedByDistance} scans every probed cluster and issues a
     * metadata point-read per candidate, all within a single read transaction; above this many vectors that one
     * transaction risks the ~5s limit, so the check self-skips. Sized with headroom over the 10k SIFT-small
     * datasets and an order of magnitude under the 100k {@code @SuperSlow} samples; calibrate from a real run if
     * needed.
     */
    private static final int MAX_ORDERED_BY_DISTANCE_INDEX_SIZE = 25_000;

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

                                return guardiann.insert(tr, record.primaryKey(),
                                                record.vector(), null, true)
                                        .thenApply(ignored -> i + 1);
                            }, guardiann.getExecutor());
            return loopFuture.thenApply(vignore -> data.build())
                    .whenComplete((result, error) -> {
                        if (error != null) {
                            logger.warn("failed to insert batchSize={}", batchSize);
                        } else {
                            final long endTs = System.nanoTime();
                            logger.debug("inserted batchSize={} records={} startingAtPrimaryKey={} took elapsedTime={}ms, readBytes={}",
                                    batchSize, result.size(),
                                    result.isEmpty() ? "<none>" : result.get(0).primaryKey(),
                                    TimeUnit.NANOSECONDS.toMillis(endTs - beginTs),
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
                                return guardiann.delete(tr, record.primaryKey(), record.vector(), true)
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

    /**
     * Deletes every record in {@code records}, re-invoking {@link #basicDeleteBatch} until the list is drained.
     * {@code basicDeleteBatch} bails out of a batch as soon as it triggers a deferred task, so a single call may
     * only delete a prefix; this loops until nothing remains.
     *
     * @param db the database
     * @param guardiann the structure to delete from
     * @param records the records to delete
     */
    static void deleteToCompletion(@Nonnull final Database db,
                                   @Nonnull final Guardiann guardiann,
                                   @Nonnull final List<PrimaryKeyAndVector> records)
            throws ExecutionException, InterruptedException, TimeoutException {
        final List<PrimaryKeyAndVector> remaining = new ArrayList<>(records);
        while (!remaining.isEmpty()) {
            final List<PrimaryKeyAndVector> done = basicDeleteBatch(db, guardiann, remaining);
            remaining.subList(0, done.size()).clear();
        }
    }

    /**
     * Sums the under-replicated primary counts across every cluster in {@code snapshot} — i.e. how many primaries
     * still owe replicas. Useful for asserting a reassign/replication pass drives that total down.
     *
     * @param snapshot the structure snapshot to aggregate over
     *
     * @return the total number of under-replicated primaries across all clusters
     */
    static int countUnderReplicatedPrimaries(@Nonnull final StructureSnapshot snapshot) {
        return snapshot.clusters().values().stream()
                .mapToInt(cv -> cv.metadata().numPrimaryUnderreplicatedVectors())
                .sum();
    }

    /**
     * Two disjoint deterministic samples drawn from an fvecs base file in a single streaming
     * pass, never holding the full dataset in memory.
     *
     * @param a first sample
     * @param b second sample, disjoint from {@code a}
     */
    record Samples(@Nonnull List<PrimaryKeyAndVector> a, @Nonnull List<PrimaryKeyAndVector> b) {
    }

    /**
     * Single-pass reservoir-sampled disjoint pair of samples drawn from the {@code baseFile} {@code .fvecs} stream.
     * Streams the file once; resident memory is bounded at roughly {@code (sizeA + sizeB) * 1KB} regardless of how
     * big the source file is. The two returned lists are guaranteed disjoint by primary key (since each PK is the
     * original index in the stream).
     * <p>
     * The whole sampling process is deterministic given {@code (baseFile, seed)}: same inputs → same two lists,
     * in the same order. The order within each list is a fresh deterministic shuffle of the
     * combined reservoir before splitting, so callers get a reasonable insertion/deletion order
     * for free without having to reshuffle.
     *
     * @param baseFile path to the {@code .fvecs} base file to sample from
     * @param seed seed for the reservoir's random replacement decisions and the post-pass shuffle
     * @param sizeA size of the first sample
     * @param sizeB size of the second sample
     * @return a {@link Samples} pair
     */
    @Nonnull
    static Samples loadDisjointSamples(@Nonnull final String baseFile, final long seed, final int sizeA, final int sizeB) throws IOException {
        Verify.verify(sizeA > 0 && sizeB > 0, "sample sizes must be positive (sizeA=%s, sizeB=%s)", sizeA, sizeB);
        final List<PrimaryKeyAndVector> sampled = reservoirSample(baseFile, seed, sizeA + sizeB);
        return new Samples(
                ImmutableList.copyOf(sampled.subList(0, sizeA)),
                ImmutableList.copyOf(sampled.subList(sizeA, sizeA + sizeB)));
    }

    /**
     * A single deterministic reservoir sample of {@code size} records drawn from the {@code baseFile} {@code .fvecs}
     * stream in one pass (the single-sample sibling of {@link #loadDisjointSamples}). Same {@code (baseFile, seed)} →
     * same sample, in the same deterministically shuffled order. Useful for insert-only workloads that don't need a
     * disjoint second set.
     *
     * @param baseFile path to the {@code .fvecs} base file to sample from
     * @param seed seed for the reservoir's replacement decisions and the post-pass shuffle
     * @param size number of records to sample
     *
     * @return the sampled records
     */
    @Nonnull
    static List<PrimaryKeyAndVector> loadSample(@Nonnull final String baseFile, final long seed, final int size) throws IOException {
        Verify.verify(size > 0, "sample size must be positive (size=%s)", size);
        return ImmutableList.copyOf(reservoirSample(baseFile, seed, size));
    }

    /**
     * Reservoir-samples {@code totalSize} records from the {@code baseFile} {@code .fvecs} stream in a single
     * streaming pass (Algorithm R), then deterministically shuffles them. Resident memory is bounded at roughly
     * {@code totalSize * 1KB} regardless of source-file size. Each record's primary key is its original index in
     * the stream, so any two disjoint slices of the result are collision-free by construction.
     */
    @Nonnull
    private static List<PrimaryKeyAndVector> reservoirSample(@Nonnull final String baseFile, final long seed, final int totalSize) throws IOException {
        // Two parallel arrays so we don't pay an Object[] indirection per slot.
        final long[] reservoirIndices = new long[totalSize];
        final DoubleRealVector[] reservoirVectors = new DoubleRealVector[totalSize];

        // SplittableRandom for the reservoir replacement decisions; deterministic from seed.
        final SplittableRandom rnd = new SplittableRandom(seed);

        long total = 0L;
        final Path basePath = Paths.get(baseFile);
        try (final FileChannel fileChannel = FileChannel.open(basePath, StandardOpenOption.READ)) {
            final Iterator<DoubleRealVector> it = new StoredVecsIterator.StoredFVecsIterator(fileChannel);
            while (it.hasNext()) {
                final DoubleRealVector v = it.next();
                if (total < totalSize) {
                    // Initial fill: items go to slot=total unconditionally.
                    reservoirIndices[(int) total] = total;
                    reservoirVectors[(int) total] = v;
                } else {
                    // Algorithm R: pick j uniformly in [0, total]; if j < totalSize, replace.
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
                "base file produced %s records, fewer than requested totalSize=%s", total, totalSize);

        // Build records and shuffle deterministically. The shuffle randomizes which reservoir slots end up where
        // (so disjoint slices are random) and gives callers a usable iteration order.
        final List<PrimaryKeyAndVector> sampled = new ArrayList<>(totalSize);
        for (int i = 0; i < totalSize; i++) {
            sampled.add(new PrimaryKeyAndVector(
                    createPrimaryKey(reservoirIndices[i]),
                    reservoirVectors[i]));
        }
        // Mix the seed with a salt so the reservoir RNG and the shuffle RNG don't share state.
        Collections.shuffle(sampled, new Random(seed ^ 0xA5A5_5A5A_A5A5_5A5AL));
        return sampled;
    }

    /**
     * Inserts {@code records} into {@code guardiann} in batches of {@code batchSize}, retrying the tail of any
     * batch that bails out because a deferred maintenance task fired mid-transaction (see
     * {@link #insertBatchWithRetry}). Each record keeps its own primary key. Callers that read vectors from a flat
     * file load them with {@link VecsDatasetLoaders#loadVectors} (or {@link #loadSample}) and pass the resulting list here.
     *
     * @param db the database
     * @param guardiann the structure to insert into
     * @param records the records to insert, in order
     * @param batchSize the number of records per insert transaction
     */
    static void insertRecords(@Nonnull final Database db,
                              @Nonnull final Guardiann guardiann,
                              @Nonnull final List<PrimaryKeyAndVector> records,
                              final int batchSize) throws Exception {
        for (int i = 0; i < records.size(); i += batchSize) {
            final int end = Math.min(i + batchSize, records.size());
            insertBatchWithRetry(db, guardiann, records.subList(i, end), i);
        }
    }

    /**
     * Inserts one fully-materialized {@code batch} (records' primary keys already set) into {@code guardiann} via
     * {@link #basicInsertBatch}, re-issuing the un-inserted tail whenever a deferred maintenance task fires
     * mid-transaction, until the whole batch is inserted. {@code firstGlobalIndex} is only the index base handed to
     * {@code basicInsertBatch}; the batch records carry their own primary keys.
     */
    private static void insertBatchWithRetry(@Nonnull final Database db,
                                             @Nonnull final Guardiann guardiann,
                                             @Nonnull final List<PrimaryKeyAndVector> batch,
                                             final long firstGlobalIndex) throws Exception {
        final List<PrimaryKeyAndVector> remaining = new ArrayList<>(batch);
        long base = firstGlobalIndex;
        while (!remaining.isEmpty()) {
            final long batchStart = base;
            final List<PrimaryKeyAndVector> inserted =
                    basicInsertBatch(db, guardiann, remaining.size(), batchStart,
                            (tr, nextId) -> {
                                final int idx = Math.toIntExact(nextId - batchStart);
                                return idx < remaining.size() ? remaining.get(idx) : null;
                            });
            base += inserted.size();
            remaining.subList(0, inserted.size()).clear();
        }
        logger.info("inserted batch; globalIndex={}", base);
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
     * Builds the {@link SearchConfig} the recall checks use: a candidate pool 15% larger than {@code k} (the
     * dataset-dependent knob), with every other knob left at its builder default (which matches what these checks
     * have always passed: 48 probed clusters, a min-of-16 prune floor, a 1.5 distance-ratio cutoff). Because the pool
     * is now expressed as a {@code k}-relative factor ({@link SearchConfig#candidatePoolFactor()}), one config serves
     * every {@code k}; {@code 1.15} happens to be the builder default, so this is just the all-defaults config, kept as
     * a named helper so the recall checks document the pool size they rely on.
     *
     * @return a search config tuned for recall@k checks
     */
    static SearchConfig recallSearchConfig() {
        return new SearchConfig.SearchConfigBuilder()
                .setCandidatePoolFactor(1.15d)
                .build();
    }

    /**
     * Asserts that the mean set-based recall@k across the given queries meets or exceeds
     * {@code minMeanRecall}. Queries whose ground-truth set is empty (e.g. when {@code maxIndex}
     * excluded every truth index for that query) are skipped.
     * <p>
     * Recall is computed via {@link #singleQueryRecall} — deduplicated result-set intersection
     * with ground truth.
     *
     * @param queries pre-loaded query vectors (see {@link VecsDatasetLoaders#loadQueryVectors})
     * @param groundTruth pre-loaded per-query ground-truth index sets (see
     *        {@link VecsDatasetLoaders#loadGroundTruth}); must have the same size as {@code queries}
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
        double sumRecall = 0.0;
        int countedQueries = 0;
        for (int i = 0; i < queries.size(); i++) {
            final Set<Integer> truth = ImmutableSet.copyOf(groundTruth.get(i));
            if (truth.isEmpty()) {
                continue;
            }
            final RealVector q = queries.get(i);
            final List<? extends ResultEntry> results =
                    db.run(tr -> guardiann.kNearestNeighborsSearch(tr, k, recallSearchConfig(),
                            true, q).join());
            final double recall = singleQueryRecall(truth, results);
            logger.debug("assertRecallAtKAtLeast: recall@{} = {} for query {}",
                    k, String.format(Locale.ROOT, "%.4f", recall), i);
            sumRecall += recall;
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
        double sumRecall = 0.0d;
        int countedQueries = 0;
        for (int i = 0; i < queries.size(); i++) {
            final RealVector query = queries.get(i);
            final Set<Integer> truth = bruteForceTopKByEuclidean(query, active, k);
            if (truth.isEmpty()) {
                continue;
            }
            final List<? extends ResultEntry> results =
                    db.run(tr -> guardiann.kNearestNeighborsSearch(tr, k, recallSearchConfig(),
                            true, query).join());
            final double recall = singleQueryRecall(truth, results);
            logger.debug("assertRecallAtKAtLeastDynamic: recall@{} = {} for query {}",
                    k, String.format(Locale.ROOT, "%.4f", recall), i);
            sumRecall += recall;
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
     * Deterministically subsamples up to {@code count} elements of {@code items}, seeded by {@code seed}: a
     * Fisher–Yates shuffle of a copy, then the first {@code count}. Returns the input unchanged when it already
     * holds {@code count} or fewer. Handy for keeping per-element checks fast without losing run-to-run variety.
     *
     * @param items the elements to sample from (left untouched)
     * @param seed the seed driving the shuffle, for reproducibility
     * @param count the maximum number of elements to return
     * @param <T> the element type
     * @return up to {@code count} elements of {@code items} in a deterministic, seed-dependent order
     */
    @Nonnull
    static <T> List<T> deterministicSample(@Nonnull final List<T> items, final long seed, final int count) {
        if (items.size() <= count) {
            return items;
        }
        final List<T> shuffled = new ArrayList<>(items);
        final SplittableRandom rng = new SplittableRandom(seed);
        for (int i = shuffled.size() - 1; i > 0; i--) {
            final int j = rng.nextInt(i + 1);
            final T tmp = shuffled.get(i);
            shuffled.set(i, shuffled.get(j));
            shuffled.set(j, tmp);
        }
        return List.copyOf(shuffled.subList(0, count));
    }

    /**
     * Runs {@link Guardiann#searchOrderedByDistance} for each query and asserts the mean result-ordering quality
     * meets or exceeds {@code minMeanQuality}. Unlike the recall helpers this needs <em>no ground truth</em> and
     * no {@code k}: it asks for the <em>entire</em> dataset ({@code k}, the reorder window, and the probed-cluster
     * cap are all set to {@code indexSize}), so a flawless result is every live vector returned in
     * {@code (distance, primaryKey)} order. Each list is scored in {@code [0, 1]} by
     * {@link #orderedByDistanceQuality}.
     * <p>
     * The whole check self-skips (returning {@link Double#NaN}, asserting nothing) when {@code indexSize} exceeds
     * {@link #MAX_ORDERED_BY_DISTANCE_INDEX_SIZE}, so it is safe to call unconditionally — including from
     * {@code @SuperSlow} workloads, where it begins running once their active set has drained under the limit.
     * The observed mean is logged so the bar can be calibrated from a real run.
     *
     * @param db the database
     * @param guardiann the structure to query
     * @param queries the query vectors to score against
     * @param indexSize the number of live vectors in the index; used both as the full result size to request and
     *        as the completeness denominator. The check is skipped (returning {@link Double#NaN}) when this
     *        exceeds {@link #MAX_ORDERED_BY_DISTANCE_INDEX_SIZE}
     * @param minMeanQuality floor that the mean quality must meet or exceed
     * @return the observed mean quality, or {@link Double#NaN} if the check was skipped
     */
    static double assertOrderedByDistanceQualityAtLeast(@Nonnull final Database db,
                                                        @Nonnull final Guardiann guardiann,
                                                        @Nonnull final List<? extends RealVector> queries,
                                                        final int indexSize,
                                                        final double minMeanQuality) {
        if (indexSize > MAX_ORDERED_BY_DISTANCE_INDEX_SIZE) {
            logger.info("skipping searchOrderedByDistance quality check: indexSize={} exceeds the "
                    + "single-transaction limit {}", indexSize, MAX_ORDERED_BY_DISTANCE_INDEX_SIZE);
            return Double.NaN;
        }
        double sumQuality = 0.0d;
        int countedQueries = 0;
        for (int i = 0; i < queries.size(); i++) {
            final RealVector query = queries.get(i);
            // Ask for the whole dataset, fully ordered: k, the reorder window, and the probed-cluster cap are all
            // the index size, so a correct method returns every live vector in (distance, primaryKey) order. A
            // candidate-pool factor of 1.0 keeps the reorder window at exactly k (== indexSize).
            final List<? extends ResultEntry> results =
                    db.run(tr -> guardiann.searchOrderedByDistance(tr, indexSize,
                            new SearchConfig.SearchConfigBuilder().setCandidatePoolFactor(1.0d)
                                    .setSearchMaxClusters(indexSize).build(),
                            ORDERED_BY_DISTANCE_FROM_START_MIN_RADIUS_CLUSTER,
                            ORDERED_BY_DISTANCE_FROM_START_MIN_RADIUS,
                            null, false, query).join());
            final double quality = orderedByDistanceQuality(results, indexSize);
            logger.debug("assertOrderedByDistanceQualityAtLeast: quality = {} (returned {}/{}) for query {}",
                    String.format(Locale.ROOT, "%.4f", quality), results.size(), indexSize, i);
            sumQuality += quality;
            countedQueries++;
        }
        assertThat(countedQueries)
                .as("there must be at least one query to score")
                .isGreaterThan(0);
        final double meanQuality = sumQuality / countedQueries;
        logger.info("assertOrderedByDistanceQualityAtLeast: mean quality = {} over {} queries (indexSize={}, threshold={})",
                String.format(Locale.ROOT, "%.4f", meanQuality), countedQueries, indexSize, minMeanQuality);
        assertThat(meanQuality)
                .as("mean searchOrderedByDistance quality over %d queries (indexSize=%d)", countedQueries, indexSize)
                .isGreaterThanOrEqualTo(minMeanQuality);
        return meanQuality;
    }

    /**
     * Scores a single {@code searchOrderedByDistance} result list in {@code [0, 1]} ({@code 1.0} = flawless),
     * combining two ground-truth-free signals:
     * <ul>
     *   <li><b>Completeness</b> — {@code returned / indexSize}, penalizing missing items: the full ordered scan
     *       should return every live vector, so returning only 9850 of 10000 docks the score.</li>
     *   <li><b>Order</b> — position-weighted inversions under the {@code (distance, primaryKey)} total order the
     *       method promises. Every out-of-order pair {@code (i < j)} whose earlier entry sorts <em>after</em> the
     *       later one is a defect weighted {@code 1/(i + 1)}, so disorder near the front of the result counts
     *       worse than near the tail; normalized against the worst case (a fully reversed sequence, where every
     *       pair is inverted). {@code 1.0} = perfectly ordered.</li>
     * </ul>
     * The two are multiplied, so both must be good to score well: a flawless result (every vector, perfectly
     * ordered) scores {@code 1.0}, and the worst case (empty, or fully reversed) scores {@code 0.0}.
     *
     * @param results the result list returned by {@code searchOrderedByDistance}, in result (rank) order
     * @param indexSize the number of live vectors the scan should have returned (completeness denominator)
     * @return the quality score in {@code [0, 1]}
     */
    private static double orderedByDistanceQuality(@Nonnull final List<? extends ResultEntry> results, final int indexSize) {
        final int numReturned = results.size();
        final double completenessScore =
                indexSize <= 0 ? 1.0d : Math.min(1.0d, (double) numReturned / (double) indexSize);

        // Position-weighted inversion count under the (distance, primaryKey) order, and the worst case it is
        // normalized against (every pair inverted, i.e. a fully reversed result).
        double weightedInversions = 0.0d;
        double worstWeightedInversions = 0.0d;
        for (int i = 0; i < numReturned; i++) {
            final double positionWeight = 1.0d / (i + 1);
            final ResultEntry earlier = results.get(i);
            for (int j = i + 1; j < numReturned; j++) {
                worstWeightedInversions += positionWeight;
                final ResultEntry later = results.get(j);
                int comparison = Double.compare(earlier.distance(), later.distance());
                if (comparison == 0) {
                    comparison = earlier.primaryKey().compareTo(later.primaryKey());
                }
                if (comparison > 0) {
                    weightedInversions += positionWeight;
                }
            }
        }
        final double orderScore = worstWeightedInversions == 0.0d
                ? 1.0d
                : 1.0d - weightedInversions / worstWeightedInversions;

        return orderScore * completenessScore;
    }

    /**
     * Brute-force top-{@code k} primary-key indices in {@code active} by squared Euclidean
     * distance to {@code query}. Maintains a max-heap of size {@code k} so the per-query work is
     * {@code O(|active| * log k)} (and {@code O(|active| * d)} for the dot products).
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

        final TopK<IndexedDistance> topK = TopK.min(
                Comparator.comparingDouble(IndexedDistance::distance).thenComparingInt(IndexedDistance::index), k);
        for (final Map.Entry<Tuple, ? extends RealVector> e : active.entrySet()) {
            topK.add(new IndexedDistance(query.l2SquaredDistance(e.getValue()), (int) e.getKey().getLong(0)));
        }
        return topK.toSortedList().stream().map(IndexedDistance::index).collect(ImmutableSet.toImmutableSet());
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
        public void onTaskEnqueued(@Nonnull final TaskKind taskKind,
                                   @Nonnull final UUID taskId, @Nonnull final Set<UUID> targetClusterIds) {
            for (final Frame frame : frames) {
                frame.numTasksEnqueuedByKind()
                        .compute(taskKind, (ignored, counter) ->
                                Objects.requireNonNullElse(counter, 0) + 1);
            }
        }

        @Override
        public void onTaskExecuted(@Nonnull final TaskKind taskKind,
                                   @Nonnull final UUID taskId, @Nonnull final Set<UUID> targetClusterIds) {
            for (final Frame frame : frames) {
                frame.numTasksExecutedByKind().compute(taskKind, (ignored, counter) ->
                        Objects.requireNonNullElse(counter, 0) + 1);
            }
        }

        @Nonnull
        public Map<TaskKind, Integer> getNumTasksEnqueuedByKind() {
            return Objects.requireNonNull(frames.peek()).numTasksEnqueuedByKind();
        }

        @Nonnull
        public Map<TaskKind, Integer> getNumTasksExecutedByKind() {
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
                             @Nonnull Map<TaskKind, Integer> numTasksEnqueuedByKind,
                             @Nonnull Map<TaskKind, Integer> numTasksExecutedByKind) {
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
}
