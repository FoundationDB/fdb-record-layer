/*
 * InsertDeleteSlowTest.java
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
import com.apple.foundationdb.async.common.BaseTest;
import com.apple.foundationdb.async.common.PrimaryKeyAndVector;
import com.apple.foundationdb.linear.DoubleRealVector;
import com.apple.foundationdb.linear.Metric;
import com.apple.foundationdb.linear.RealVector;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.test.TestDatabaseExtension;
import com.apple.foundationdb.test.TestExecutors;
import com.apple.foundationdb.test.TestSubspaceExtension;
import com.apple.foundationdb.test.RootLogLevelExtension;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.RandomizedTestUtils;
import com.apple.test.SuperSlow;
import com.google.common.collect.ImmutableSet;
import org.apache.logging.log4j.Level;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SplittableRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Insert/delete workload scenarios against SIFT-1M, each verifying that recall stays above the
 * configured floor throughout and that the structure ends in the expected state. Two scenarios:
 * <ul>
 *   <li>{@code interleavedInsertsDeletesPreserveRecallAndConverge}: two disjoint 10k reservoir
 *       samples are drawn; the first seeds the structure, the second is inserted in batches
 *       randomly interleaved with batches deleting the first, until both are exhausted. The
 *       structure must then contain <em>only</em> the second sample.</li>
 *   <li>{@code insertThenDeleteAllPreservesRecallUntilEmpty}: a single 10k sample seeds the
 *       structure, then every record is deleted in random batches (no further inserts). Recall
 *       must hold until the active set falls below {@code k}, and the structure must end
 *       <em>empty</em>. This drain (no replenishing inserts) reliably pushes clusters below
 *       {@code primaryClusterMin}, exercising the 2→1 merge path.</li>
 * </ul>
 * <p>
 * Gated under {@link SuperSlow} because the SIFT-1M base file (~516 MB) is only present in nightly
 * builds. Parameterized over a small set of seeds so the random batch schedule and reservoir
 * sample are both reproducible: a failure prints the seed and the entire run can be replayed.
 *
 * <h2>Why dynamic ground truth</h2>
 * The SIFT-1M ground-truth file is computed against the full 1 M base set. Our active set at any
 * moment is a 10k subset that is not a prefix of the file, so the static {@code .ivecs} truth no
 * longer applies. Recall is therefore evaluated against brute-force squared-L2 top-k computed
 * from the live active map on the fly (see
 * {@link TestHelpers#assertRecallAtKAtLeastDynamic}).
 */
public class InsertDeleteSlowTest implements BaseTest {
    private static final Logger logger = LoggerFactory.getLogger(InsertDeleteSlowTest.class);

    /** Size of each disjoint sample. Total inserts/deletes = 2 * SAMPLE_SIZE. */
    private static final int SAMPLE_SIZE_A = 10_000;
    private static final int SAMPLE_SIZE_B = 10_000;

    /** Fixed batch size for both insert and delete operations. Final tail per queue may be smaller. */
    private static final int BATCH_SIZE = 50;

    /** k for kNN search and recall measurement. */
    private static final int RECALL_K = 100;

    /** Number of SIFT-1M query vectors used per recall checkpoint. First N of the query file. */
    private static final int RECALL_NUM_QUERIES = 100;

    /** Recall floor enforced at every checkpoint (initial, periodic during churn, final). */
    private static final double MIN_RECALL = 0.80d;

    /** Number of churn batches between recall checkpoints. ~8 checkpoints over ~400 batches. */
    private static final int RECALL_CHECK_INTERVAL_BATCHES = 50;

    /** Salt mixed into the seed to drive the random insert/delete schedule independently of sampling. */
    private static final long INTERLEAVE_SEED_SALT = 0x5EED_4DA7_D17_C0DEL;

    @RegisterExtension
    static final TestDatabaseExtension dbExtension = new TestDatabaseExtension();

    @RegisterExtension
    final TestSubspaceExtension subspaceExtension = new TestSubspaceExtension(dbExtension);

    @RegisterExtension
    final RootLogLevelExtension rootLogLevelExtension = new RootLogLevelExtension(Level.INFO);

    @TempDir
    Path tempDir;

    private static Database db;

    @Nonnull
    @Override
    public Database getDb() {
        return Objects.requireNonNull(db);
    }

    @Nonnull
    @Override
    public Subspace getSubspace() {
        return subspaceExtension.getSubspace();
    }

    @Nonnull
    @Override
    public Path getTempDir() {
        return tempDir;
    }

    @BeforeAll
    public static void setUpDb() {
        db = dbExtension.getDatabase();
    }

    @Nonnull
    private static Stream<Long> seeds() {
        return RandomizedTestUtils.randomSeeds(0xDEAD_C0DEL, 0xFDB5_CA1EL);
    }

    @ParameterizedTest(name = "[{index}] seed={0}")
    @MethodSource("seeds")
    //@SuperSlow
    @Timeout(value = 4, unit = TimeUnit.HOURS)
    void interleavedInsertsDeletesPreserveRecallAndConverge(final long seed) throws Exception {
        // ---- Phase 0: deterministic disjoint samples + query subset ----
        logger.info("seed=0x{} sampling sizeA={}, sizeB={} from SIFT-1M",
                Long.toHexString(seed), SAMPLE_SIZE_A, SAMPLE_SIZE_B);
        final TestHelpers.Samples samples =
                TestHelpers.loadDisjointSamplesFromSift1m(seed, SAMPLE_SIZE_A, SAMPLE_SIZE_B);
        final List<PrimaryKeyAndVector> setA = samples.a();
        final List<PrimaryKeyAndVector> setB = samples.b();
        verifyDisjoint(setA, setB);

        final List<DoubleRealVector> allQueries =
                TestHelpers.loadSiftQueryVectors(TestHelpers.SIFT_1M_QUERY_PATH);
        final List<DoubleRealVector> queries = List.copyOf(
                allQueries.subList(0, Math.min(RECALL_NUM_QUERIES, allQueries.size())));

        // ---- Phase 1: build a fresh Guardiann and seed it with setA ----
        final TestHelpers.TestOnWriteListener onWriteListener = new TestHelpers.TestOnWriteListener();
        final TestHelpers.TestOnReadListener onReadListener = new TestHelpers.TestOnReadListener();
        final Guardiann guardiann = new Guardiann(getSubspace(),
                TestExecutors.defaultThreadPool(),
                buildConfig(),
                onWriteListener,
                onReadListener);

        logger.info("phase1: inserting setA ({} records) ...", setA.size());
        insertAllRecordsBatched(guardiann, setA);
        TestHelpers.runToQuiescence(getDb(), guardiann);

        // Active map mirrors the index's primary contents. Mutated alongside every churn op so we
        // always have an up-to-date "what's in there" for the recall checks.
        final Map<Tuple, RealVector> active = new HashMap<>();
        for (final PrimaryKeyAndVector r : setA) {
            active.put(r.getPrimaryKey(), r.getVector());
        }

        // ---- Phase 2: initial recall checkpoint ----
        TestHelpers.assertRecallAtKAtLeastDynamic(getDb(), guardiann, queries, active,
                RECALL_K, MIN_RECALL);

        // ---- Phase 3: interleaved churn ----
        // Per-batch random choice between "delete next 50 from A" and "insert next 50 from B".
        // No strict alternation; runs of consecutive same-type batches are expected and desired.
        final SplittableRandom interleaveRng = new SplittableRandom(seed ^ INTERLEAVE_SEED_SALT);
        final Deque<PrimaryKeyAndVector> deleteQueueA = new ArrayDeque<>(setA);
        final Deque<PrimaryKeyAndVector> insertQueueB = new ArrayDeque<>(setB);
        int batchesSinceCheck = 0;
        int totalBatches = 0;
        int totalDeleteBatches = 0;
        int totalInsertBatches = 0;
        while (!deleteQueueA.isEmpty() || !insertQueueB.isEmpty()) {
            final boolean canDelete = !deleteQueueA.isEmpty();
            final boolean canInsert = !insertQueueB.isEmpty();
            final boolean doDelete;
            if (!canDelete) {
                doDelete = false;
            } else if (!canInsert) {
                doDelete = true;
            } else {
                doDelete = interleaveRng.nextBoolean();
            }

            final Deque<PrimaryKeyAndVector> source = doDelete ? deleteQueueA : insertQueueB;
            final List<PrimaryKeyAndVector> batch = takeUpTo(source, BATCH_SIZE);
            if (doDelete) {
                deleteOneBatch(guardiann, batch);
                for (final PrimaryKeyAndVector r : batch) {
                    active.remove(r.getPrimaryKey());
                }
                totalDeleteBatches++;
            } else {
                insertOneBatch(guardiann, batch);
                for (final PrimaryKeyAndVector r : batch) {
                    active.put(r.getPrimaryKey(), r.getVector());
                }
                totalInsertBatches++;
            }
            totalBatches++;
            batchesSinceCheck++;

            if (batchesSinceCheck >= RECALL_CHECK_INTERVAL_BATCHES) {
                batchesSinceCheck = 0;
                logger.info("checkpoint after totalBatches={} (deletes={}, inserts={}): active.size={}, queues left a={}, b={}",
                        totalBatches, totalDeleteBatches, totalInsertBatches,
                        active.size(), deleteQueueA.size(), insertQueueB.size());
                // Skip the recall check if the active set has dipped below k — happens only
                // pathologically at the tail when the random schedule has done many more deletes
                // than inserts. Per Chebyshev, with two equal-sized queues this is unlikely, but
                // bail out cleanly rather than fail the @code Verify in the helper.
                if (active.size() >= RECALL_K) {
                    TestHelpers.assertRecallAtKAtLeastDynamic(getDb(), guardiann, queries, active,
                            RECALL_K, MIN_RECALL);
                } else {
                    logger.info("skipping mid-flight recall check: active.size={} < k={}",
                            active.size(), RECALL_K);
                }
            }
        }
        logger.info("phase3 complete: totalBatches={} (deletes={}, inserts={}), active.size={}",
                totalBatches, totalDeleteBatches, totalInsertBatches, active.size());

        // ---- Phase 4: drain & invariants ----
        // This run interleaves inserts and deletes, so dangling replica vectors are expected
        // (a deleted primary's replicas aren't necessarily reaped). Use the after-deletes variant,
        // which checks quiescence and primary-uniqueness but not replica/live-primary references.
        TestHelpers.assertGuardiannInvariantsAfterDeletes(getDb(), guardiann);

        // ---- Phase 5: only setB remains ----
        final TestHelpers.StructureSnapshot snap = TestHelpers.snapshotStructure(getDb(), guardiann);
        assertThat(snap)
                .as("structure snapshot must be non-null after the run")
                .isNotNull();
        final Set<Tuple> presentPks = snap.primaryOwners().keySet().stream()
                .map(VectorId::getPrimaryKey)
                .collect(ImmutableSet.toImmutableSet());
        final Set<Tuple> expectedPks = setB.stream()
                .map(PrimaryKeyAndVector::getPrimaryKey)
                .collect(ImmutableSet.toImmutableSet());
        assertThat(presentPks)
                .as("only setB primary keys must remain after the interleaved run")
                .isEqualTo(expectedPks);
        assertThat(active.keySet())
                .as("local active map must mirror the structure's primary keys")
                .isEqualTo(expectedPks);

        // ---- Phase 6: final recall checkpoint over setB ----
        final Map<Tuple, RealVector> finalActive = new HashMap<>(setB.size());
        for (final PrimaryKeyAndVector r : setB) {
            finalActive.put(r.getPrimaryKey(), r.getVector());
        }
        TestHelpers.assertRecallAtKAtLeastDynamic(getDb(), guardiann, queries, finalActive,
                RECALL_K, MIN_RECALL);
    }

    /**
     * Seeds the structure with a single 10k sample, then deletes every record in random batches
     * with no further inserts. Recall must stay above {@link #MIN_RECALL} at each checkpoint while
     * the active set is still at least {@link #RECALL_K}; once the structure is fully drained it
     * must be empty (no primaries remain).
     * <p>
     * Unlike {@code interleavedInsertsDeletesPreserveRecallAndConverge}, there are no replenishing
     * inserts, so the per-cluster primary counts only ever shrink — this is the workload that
     * reliably drives clusters below {@code primaryClusterMin} and therefore exercises the merge
     * (2→1, {@code KMeans.fit} with {@code k == 1}) path end-to-end.
     */
    @ParameterizedTest(name = "[{index}] seed={0}")
    @MethodSource("seeds")
    //@SuperSlow
    @Timeout(value = 4, unit = TimeUnit.HOURS)
    void insertThenDeleteAllPreservesRecallUntilEmpty(final long seed) throws Exception {
        // ---- Phase 0: deterministic startup sample + query subset ----
        logger.info("seed=0x{} sampling startup size={} from SIFT-1M",
                Long.toHexString(seed), SAMPLE_SIZE_A);
        final TestHelpers.Samples samples =
                TestHelpers.loadDisjointSamplesFromSift1m(seed, SAMPLE_SIZE_A, SAMPLE_SIZE_B);
        final List<PrimaryKeyAndVector> startup = samples.a();

        final List<DoubleRealVector> allQueries =
                TestHelpers.loadSiftQueryVectors(TestHelpers.SIFT_1M_QUERY_PATH);
        final List<DoubleRealVector> queries = List.copyOf(
                allQueries.subList(0, Math.min(RECALL_NUM_QUERIES, allQueries.size())));

        // ---- Phase 1: build a fresh Guardiann and seed it with the startup sample ----
        final TestHelpers.TestOnWriteListener onWriteListener = new TestHelpers.TestOnWriteListener();
        final TestHelpers.TestOnReadListener onReadListener = new TestHelpers.TestOnReadListener();
        final Guardiann guardiann = new Guardiann(getSubspace(),
                TestExecutors.defaultThreadPool(),
                buildConfig(),
                onWriteListener,
                onReadListener);

        logger.info("phase1: inserting startup sample ({} records) ...", startup.size());
        insertAllRecordsBatched(guardiann, startup);
        TestHelpers.runToQuiescence(getDb(), guardiann);

        final Map<Tuple, RealVector> active = new HashMap<>();
        for (final PrimaryKeyAndVector r : startup) {
            active.put(r.getPrimaryKey(), r.getVector());
        }

        // ---- Phase 2: initial recall checkpoint ----
        TestHelpers.assertRecallAtKAtLeastDynamic(getDb(), guardiann, queries, active,
                RECALL_K, MIN_RECALL);

        // ---- Phase 3: delete everything in random batches (no inserts) ----
        final SplittableRandom deleteRng = new SplittableRandom(seed ^ INTERLEAVE_SEED_SALT);
        final Deque<PrimaryKeyAndVector> deleteQueue = new ArrayDeque<>(shuffledCopy(startup, deleteRng));
        int batchesSinceCheck = 0;
        int totalDeleteBatches = 0;
        while (!deleteQueue.isEmpty()) {
            final List<PrimaryKeyAndVector> batch = takeUpTo(deleteQueue, BATCH_SIZE);
            deleteOneBatch(guardiann, batch);
            for (final PrimaryKeyAndVector r : batch) {
                active.remove(r.getPrimaryKey());
            }
            totalDeleteBatches++;
            batchesSinceCheck++;

            if (batchesSinceCheck >= RECALL_CHECK_INTERVAL_BATCHES) {
                batchesSinceCheck = 0;
                logger.info("checkpoint after deleteBatches={}: active.size={}, queue left={}",
                        totalDeleteBatches, active.size(), deleteQueue.size());
                // Recall@k is only measurable while at least k records remain; once the active set
                // dips below k (the tail of the drain), stop checking and just keep deleting.
                if (active.size() >= RECALL_K) {
                    TestHelpers.assertRecallAtKAtLeastDynamic(getDb(), guardiann, queries, active,
                            RECALL_K, MIN_RECALL);
                } else {
                    logger.info("skipping recall check: active.size={} < k={}", active.size(), RECALL_K);
                }
            }
        }
        logger.info("phase3 complete: deleteBatches={}, active.size={}", totalDeleteBatches, active.size());

        // ---- Phase 4: drain & invariants ----
        // Everything was deleted, so dangling replica vectors are expected (a deleted primary's
        // replicas aren't necessarily reaped). Use the after-deletes variant, which checks
        // quiescence and primary-uniqueness but not replica/live-primary references.
        TestHelpers.assertGuardiannInvariantsAfterDeletes(getDb(), guardiann);

        // ---- Phase 5: the structure must be empty ----
        assertThat(active)
                .as("local active map must be empty after deleting the entire startup sample")
                .isEmpty();
        final TestHelpers.StructureSnapshot snap = TestHelpers.snapshotStructure(getDb(), guardiann);
        final int remainingPrimaries = snap == null ? 0 : snap.totalPrimaries();
        assertThat(remainingPrimaries)
                .as("no primaries may remain after deleting every record")
                .isZero();
    }

    @Nonnull
    private static Config buildConfig() {
        return Guardiann.newConfigBuilder()
                .setUseRaBitQ(true)
                .setRaBitQNumExBits(6)
                .setMetric(Metric.EUCLIDEAN_METRIC)
                .setPrimaryClusterMax(512)
                .setPrimaryClusterMin(100)
                .setDeterministicRandomness(true)
                .setReplicationPriorityMin(0.65d)
                .setReplicatedClusterTarget(500)
                .setReplicatedClusterMaxWrites(2000)
                .build(128);
    }

    private static void verifyDisjoint(@Nonnull final List<PrimaryKeyAndVector> a,
                                       @Nonnull final List<PrimaryKeyAndVector> b) {
        final Set<Tuple> pksA = a.stream()
                .map(PrimaryKeyAndVector::getPrimaryKey)
                .collect(ImmutableSet.toImmutableSet());
        for (final PrimaryKeyAndVector r : b) {
            assertThat(pksA)
                    .as("setA and setB must be disjoint by primary key")
                    .doesNotContain(r.getPrimaryKey());
        }
    }

    /**
     * Returns a freshly shuffled copy of {@code records}, using {@code rng} to drive an in-place
     * Fisher–Yates shuffle of the copy. The input list is left untouched.
     */
    @Nonnull
    private static List<PrimaryKeyAndVector> shuffledCopy(@Nonnull final List<PrimaryKeyAndVector> records,
                                                          @Nonnull final SplittableRandom rng) {
        final List<PrimaryKeyAndVector> copy = new ArrayList<>(records);
        for (int i = copy.size() - 1; i > 0; i--) {
            final int j = rng.nextInt(i + 1);
            final PrimaryKeyAndVector tmp = copy.get(i);
            copy.set(i, copy.get(j));
            copy.set(j, tmp);
        }
        return copy;
    }

    /**
     * Removes up to {@code batchSize} records from the front of {@code queue} and returns them.
     * The final batch from each queue may be smaller than {@code batchSize}; all earlier batches
     * are exactly {@code batchSize}.
     */
    @Nonnull
    private static List<PrimaryKeyAndVector> takeUpTo(@Nonnull final Deque<PrimaryKeyAndVector> queue,
                                                      final int batchSize) {
        final int n = Math.min(batchSize, queue.size());
        final List<PrimaryKeyAndVector> batch = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            batch.add(queue.removeFirst());
        }
        return batch;
    }

    /**
     * Inserts every record in {@code records} into {@code guardiann} via {@link
     * TestHelpers#basicInsertBatch}. The outer loop slices the input into {@code BATCH_SIZE}
     * chunks; the inner loop re-issues the tail when {@code basicInsertBatch} bails out early
     * because a deferred maintenance task fired mid-transaction.
     */
    private void insertAllRecordsBatched(@Nonnull final Guardiann guardiann,
                                         @Nonnull final List<PrimaryKeyAndVector> records) throws Exception {
        int i = 0;
        while (i < records.size()) {
            final int end = Math.min(i + BATCH_SIZE, records.size());
            final List<PrimaryKeyAndVector> remaining =
                    new ArrayList<>(records.subList(i, end));
            while (!remaining.isEmpty()) {
                final List<PrimaryKeyAndVector> done =
                        TestHelpers.basicInsertBatch(getDb(), guardiann, remaining.size(), 0L,
                                (tr, nextId) -> {
                                    final int idx = Math.toIntExact(nextId);
                                    return idx < remaining.size() ? remaining.get(idx) : null;
                                });
                i += done.size();
                remaining.subList(0, done.size()).clear();
            }
        }
    }

    /** Insert a single batch (one transaction modulo the bail-out retry) of pre-selected records. */
    private void insertOneBatch(@Nonnull final Guardiann guardiann,
                                @Nonnull final List<PrimaryKeyAndVector> batch) throws Exception {
        final List<PrimaryKeyAndVector> remaining = new ArrayList<>(batch);
        while (!remaining.isEmpty()) {
            final List<PrimaryKeyAndVector> done =
                    TestHelpers.basicInsertBatch(getDb(), guardiann, remaining.size(), 0L,
                            (tr, nextId) -> {
                                final int idx = Math.toIntExact(nextId);
                                return idx < remaining.size() ? remaining.get(idx) : null;
                            });
            remaining.subList(0, done.size()).clear();
        }
    }

    /** Delete a single batch (one transaction modulo the bail-out retry) of pre-selected records. */
    private void deleteOneBatch(@Nonnull final Guardiann guardiann,
                                @Nonnull final List<PrimaryKeyAndVector> batch) throws Exception {
        final List<PrimaryKeyAndVector> remaining = new ArrayList<>(batch);
        while (!remaining.isEmpty()) {
            final List<PrimaryKeyAndVector> done =
                    TestHelpers.basicDeleteBatch(getDb(), guardiann, remaining);
            remaining.subList(0, done.size()).clear();
        }
    }
}
