/*
 * SiftTest.java
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
import com.apple.foundationdb.async.common.ResultEntry;
import com.apple.foundationdb.async.hnsw.HNSW;
import com.apple.foundationdb.linear.DoubleRealVector;
import com.apple.foundationdb.linear.Metric;
import com.apple.foundationdb.linear.RealVector;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.test.TestDatabaseExtension;
import com.apple.foundationdb.test.TestExecutors;
import com.apple.foundationdb.test.TestSubspaceExtension;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.RandomSeedSource;
import com.apple.test.SuperSlow;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.params.ParameterizedTest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.IOException;
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
import java.util.function.Consumer;

import static com.apple.foundationdb.async.guardiann.SiftTestHelpers.SIFT_1M_BASE_PATH;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * SIFT recall + invariant scenarios for {@link Guardiann}. Each test builds its own {@link Guardiann} in a
 * fresh, per-test subspace (see {@link TestSubspaceExtension}) and loads its own dataset, so the cases are
 * independent. The suite consolidates the former small-set and SIFT-1M-subsampled workloads into one class.
 *
 * <p>Three workloads, each in a fast 10k variant and a {@link SuperSlow} ~100k variant:
 * <ul>
 *   <li><b>insert-only</b> ({@link #insertSiftSmall}, {@link #insertSiftSubsampledSlow}): insert a dataset,
 *       verify recall and the structural/replication invariants.</li>
 *   <li><b>interleaved insert/delete</b> ({@link #insertDeleteSiftSubsampled},
 *       {@link #insertDeleteSiftSubsampledSlow}): seed with sample A, then randomly interleave inserting a
 *       disjoint sample B with deleting sample A until both are exhausted; only B must remain.</li>
 *   <li><b>insert then delete-all</b> ({@link #insertDeleteAllSiftSmall},
 *       {@link #insertDeleteAllSiftSubsampledSlow}): seed with a sample, then delete every record in random
 *       batches; the structure must end empty. This drain reliably drives clusters below
 *       {@code primaryClusterMin}, exercising the 2&rarr;1 merge path.</li>
 * </ul>
 *
 * <p>The 10k variants use either the dedicated SIFT-small base file or a 10k reservoir sample of SIFT-1M; the
 * {@code Slow} variants use ~100k reservoir samples of SIFT-1M (its ~516&nbsp;MB base file is only present in
 * nightly builds, hence {@link SuperSlow}). The subsampled and delete-driven tests are parameterized over seeds
 * via {@link RandomSeedSource} so the reservoir sample and the random batch schedule are reproducible.
 *
 * <h2>Why dynamic ground truth</h2>
 * Recall is evaluated everywhere via {@link TestHelpers#assertRecallAtKAtLeastDynamic}, which computes
 * brute-force squared-L2 top-k from the live active set on the fly. The static SIFT ground-truth files are
 * computed against a full base set, but our active set at any moment is a subsample (or a shrinking subset), so
 * the static {@code .ivecs} truth does not apply.
 */
@Execution(ExecutionMode.CONCURRENT)
public class SiftTest implements BaseTest {
    private static final Logger logger = LoggerFactory.getLogger(SiftTest.class);

    /** Size of the fast (non-{@link SuperSlow}) datasets. */
    private static final int SAMPLE_SIZE_SMALL = 10_000;
    /** Size of the {@link SuperSlow} datasets (per sample). */
    private static final int SAMPLE_SIZE_LARGE = 100_000;

    /** Fixed batch size for both insert and delete operations. The final tail per queue may be smaller. */
    private static final int BATCH_SIZE = 50;

    /** k for kNN search and recall measurement. */
    private static final int RECALL_K = 100;

    /** Number of SIFT query vectors used per recall checkpoint (the first N of the query file). */
    private static final int RECALL_NUM_QUERIES = 100;

    /** Recall floor enforced at every checkpoint (initial, periodic during churn, final). */
    private static final double MIN_RECALL = 0.80d;

    /**
     * Quality floor for {@link TestHelpers#assertOrderedByDistanceQualityAtLeast} (1.0 = a flawless result:
     * every live vector returned in {@code (distance, primaryKey)} order). The helper logs the observed mean so
     * this bar can be calibrated from a real run.
     */
    private static final double MIN_ORDERED_BY_DISTANCE_QUALITY = 0.90d;

    /**
     * Number of queries the {@code searchOrderedByDistance} quality check samples down to (deterministically,
     * seeded by the test) before scoring. The check scans the whole index per query, so running all
     * {@link #RECALL_NUM_QUERIES} queries per checkpoint is far too slow.
     */
    private static final int NUM_ORDERED_BY_DISTANCE_QUERIES = 10;

    /** Number of churn batches between recall checkpoints. */
    private static final int RECALL_CHECK_INTERVAL_BATCHES = 50;

    /** Salt mixed into the seed to drive the random insert/delete schedule independently of sampling. */
    private static final long INTERLEAVE_SEED_SALT = 0x5EED_4DA7_D17_C0DEL;

    @RegisterExtension
    static final TestDatabaseExtension dbExtension = new TestDatabaseExtension();

    @RegisterExtension
    final TestSubspaceExtension subspaceExtension = new TestSubspaceExtension(dbExtension);

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

    // ---------------------------------------------------------------------------------------------------------
    // Insert-only
    // ---------------------------------------------------------------------------------------------------------

    /**
     * Insert the dedicated SIFT-small 10k base set, then verify recall against the curated SIFT-small ground
     * truth (the full set is inserted, so the static {@code .ivecs} truth applies directly) plus the insert-only
     * invariants.
     */
    @Test
    @Timeout(value = 10, unit = TimeUnit.MINUTES)
    void insertSiftSmall() throws Exception {
        final List<PrimaryKeyAndVector> dataset =
                TestHelpers.loadVectors(SiftTestHelpers.SIFT_SMALL_BASE_PATH, SAMPLE_SIZE_SMALL);
        final Guardiann guardiann = newGuardiann();
        insertAndQuiesce(guardiann, dataset);

        final List<DoubleRealVector> queries = loadQueries(SiftTestHelpers.SIFT_SMALL_QUERY_PATH);
        final List<Set<Integer>> groundTruth =
                TestHelpers.loadGroundTruth(SiftTestHelpers.SIFT_SMALL_GROUNDTRUTH_PATH, -1);
        TestHelpers.assertRecallAtKAtLeast(getDb(), guardiann, queries, groundTruth, RECALL_K, MIN_RECALL);
        TestHelpers.assertOrderedByDistanceQualityAtLeast(getDb(), guardiann,
                TestHelpers.deterministicSample(queries, 0L, NUM_ORDERED_BY_DISTANCE_QUERIES), dataset.size(),
                MIN_ORDERED_BY_DISTANCE_QUALITY);

        // SIFT-small is dense/overlapping, so the split sub-clusters have borders and must carry replicas; a
        // near-zero replica count would mean replication is broken (a coarse floor, not a fraction-of-demand check).
        TestHelpers.assertGuardiannInvariants(getDb(), guardiann,
                TestHelpers.ReplicationInvariants.standard().withMinReplicatedFraction(0.1d));
    }

    /**
     * Insert a ~100k reservoir sample of SIFT-1M, then verify recall plus the insert-only invariants. The sample
     * is random (not a prefix), so the static SIFT-1M ground truth doesn't apply — recall is evaluated
     * dynamically against the inserted set.
     */
    @ParameterizedTest(name = "[{index}] seed={0}")
    @RandomSeedSource
    @SuperSlow
    @Timeout(value = 2, unit = TimeUnit.HOURS)
    void insertSiftSubsampledSlow(final long seed) throws Exception {
        final List<PrimaryKeyAndVector> dataset = TestHelpers.loadSample(SIFT_1M_BASE_PATH, seed, SAMPLE_SIZE_LARGE);
        final Guardiann guardiann = newGuardiann();
        insertAndQuiesce(guardiann, dataset);

        final List<DoubleRealVector> queries = loadQueries(SiftTestHelpers.SIFT_1M_QUERY_PATH);
        TestHelpers.assertRecallAtKAtLeastDynamic(getDb(), guardiann, queries, activeMapOf(dataset),
                RECALL_K, MIN_RECALL);
        TestHelpers.assertOrderedByDistanceQualityAtLeast(getDb(), guardiann,
                TestHelpers.deterministicSample(queries, seed, NUM_ORDERED_BY_DISTANCE_QUERIES), dataset.size(),
                MIN_ORDERED_BY_DISTANCE_QUALITY);
        // No replica floor yet for the sparse 1M subsample: assertReplicasNotTooFew logs the observed fraction so
        // it can be calibrated from a real run before being enforced.
        TestHelpers.assertGuardiannInvariants(getDb(), guardiann, TestHelpers.ReplicationInvariants.standard());
    }

    // ---------------------------------------------------------------------------------------------------------
    // Interleaved insert (sample B) / delete (sample A) — only B must remain
    // ---------------------------------------------------------------------------------------------------------

    @ParameterizedTest(name = "[{index}] seed={0}")
    @RandomSeedSource
    @Timeout(value = 10, unit = TimeUnit.MINUTES)
    void insertDeleteSiftSubsampled(final long seed) throws Exception {
        runInterleavedInsertDelete(seed, SAMPLE_SIZE_SMALL);
    }

    @ParameterizedTest(name = "[{index}] seed={0}")
    @RandomSeedSource
    @SuperSlow
    @Timeout(value = 2, unit = TimeUnit.HOURS)
    void insertDeleteSiftSubsampledSlow(final long seed) throws Exception {
        runInterleavedInsertDelete(seed, SAMPLE_SIZE_LARGE);
    }

    // ---------------------------------------------------------------------------------------------------------
    // Insert then delete every record — structure must end empty
    // ---------------------------------------------------------------------------------------------------------

    @ParameterizedTest(name = "[{index}] seed={0}")
    @RandomSeedSource
    @Timeout(value = 10, unit = TimeUnit.MINUTES)
    void insertDeleteAllSiftSmall(final long seed) throws Exception {
        final List<PrimaryKeyAndVector> startup =
                TestHelpers.loadVectors(SiftTestHelpers.SIFT_SMALL_BASE_PATH, SAMPLE_SIZE_SMALL);
        final List<DoubleRealVector> queries = loadQueries(SiftTestHelpers.SIFT_SMALL_QUERY_PATH);
        runInsertThenDeleteAll(seed, startup, queries);
    }

    @ParameterizedTest(name = "[{index}] seed={0}")
    @RandomSeedSource
    @SuperSlow
    @Timeout(value = 2, unit = TimeUnit.HOURS)
    void insertDeleteAllSiftSubsampledSlow(final long seed) throws Exception {
        final List<PrimaryKeyAndVector> startup = TestHelpers.loadSample(SIFT_1M_BASE_PATH, seed, SAMPLE_SIZE_LARGE);
        final List<DoubleRealVector> queries = loadQueries(SiftTestHelpers.SIFT_1M_QUERY_PATH);
        runInsertThenDeleteAll(seed, startup, queries);
    }

    // ---------------------------------------------------------------------------------------------------------
    // Workload bodies
    // ---------------------------------------------------------------------------------------------------------

    /**
     * Seeds the structure with sample A, then randomly interleaves inserting a disjoint sample B with deleting
     * sample A (in {@link #BATCH_SIZE} batches) until both are exhausted, checking recall periodically. The
     * structure must then contain only B.
     */
    private void runInterleavedInsertDelete(final long seed, final int sampleSize) throws Exception {
        logger.info("seed=0x{} sampling sizeA={}, sizeB={} from SIFT-1M",
                Long.toHexString(seed), sampleSize, sampleSize);
        final TestHelpers.Samples samples = TestHelpers.loadDisjointSamples(SIFT_1M_BASE_PATH, seed, sampleSize, sampleSize);
        final List<PrimaryKeyAndVector> setA = samples.a();
        final List<PrimaryKeyAndVector> setB = samples.b();
        verifyDisjoint(setA, setB);
        final List<DoubleRealVector> queries = loadQueries(SiftTestHelpers.SIFT_1M_QUERY_PATH);

        final Guardiann guardiann = newGuardiann();
        logger.info("phase1: inserting setA ({} records) ...", setA.size());
        insertAndQuiesce(guardiann, setA);

        // Active map mirrors the index's primary contents; mutated alongside every churn op so the recall checks
        // always know what's in there.
        final Map<Tuple, RealVector> active = activeMapOf(setA);

        // ---- initial recall checkpoint ----
        TestHelpers.assertRecallAtKAtLeastDynamic(getDb(), guardiann, queries, active, RECALL_K, MIN_RECALL);
        TestHelpers.assertOrderedByDistanceQualityAtLeast(getDb(), guardiann,
                TestHelpers.deterministicSample(queries, seed, NUM_ORDERED_BY_DISTANCE_QUERIES), active.size(),
                MIN_ORDERED_BY_DISTANCE_QUALITY);

        // ---- interleaved churn: per-batch random choice between delete-from-A and insert-from-B ----
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
                    active.remove(r.primaryKey());
                }
                totalDeleteBatches++;
            } else {
                TestHelpers.insertRecords(getDb(), guardiann, batch, BATCH_SIZE);
                for (final PrimaryKeyAndVector r : batch) {
                    active.put(r.primaryKey(), r.vector());
                }
                totalInsertBatches++;
            }
            totalBatches++;
            batchesSinceCheck++;

            if (batchesSinceCheck >= RECALL_CHECK_INTERVAL_BATCHES) {
                batchesSinceCheck = 0;
                logger.info("checkpoint after totalBatches={} (deletes={}, inserts={}): active.size={}, "
                                + "queues left a={}, b={}",
                        totalBatches, totalDeleteBatches, totalInsertBatches,
                        active.size(), deleteQueueA.size(), insertQueueB.size());
                // Skip the recall check if the active set has dipped below k — happens only pathologically at the
                // tail when the random schedule has done many more deletes than inserts.
                if (active.size() >= RECALL_K) {
                    TestHelpers.assertRecallAtKAtLeastDynamic(getDb(), guardiann, queries, active,
                            RECALL_K, MIN_RECALL);
                    TestHelpers.assertOrderedByDistanceQualityAtLeast(getDb(), guardiann,
                            TestHelpers.deterministicSample(queries, seed, NUM_ORDERED_BY_DISTANCE_QUERIES), active.size(),
                            MIN_ORDERED_BY_DISTANCE_QUALITY);
                } else {
                    logger.info("skipping mid-flight recall check: active.size={} < k={}", active.size(), RECALL_K);
                }
            }
        }
        logger.info("churn complete: totalBatches={} (deletes={}, inserts={}), active.size={}",
                totalBatches, totalDeleteBatches, totalInsertBatches, active.size());

        // ---- drain & invariants (after-deletes: dangling replicas of deleted primaries are expected) ----
        TestHelpers.assertGuardiannInvariantsAfterDeletes(getDb(), guardiann);

        // ---- only setB remains ----
        final StructureSnapshot snap = TestHelpers.snapshotStructure(getDb(), guardiann);
        assertThat(snap)
                .as("structure snapshot must be non-null after the run")
                .isNotNull();
        final Set<Tuple> presentPks = snap.primaryOwners().keySet().stream()
                .map(VectorId::primaryKey)
                .collect(ImmutableSet.toImmutableSet());
        final Set<Tuple> expectedPks = setB.stream()
                .map(PrimaryKeyAndVector::primaryKey)
                .collect(ImmutableSet.toImmutableSet());
        assertThat(presentPks)
                .as("only setB primary keys must remain after the interleaved run")
                .isEqualTo(expectedPks);
        assertThat(active.keySet())
                .as("local active map must mirror the structure's primary keys")
                .isEqualTo(expectedPks);

        // ---- final recall checkpoint over setB ----
        TestHelpers.assertRecallAtKAtLeastDynamic(getDb(), guardiann, queries, activeMapOf(setB),
                RECALL_K, MIN_RECALL);
        TestHelpers.assertOrderedByDistanceQualityAtLeast(getDb(), guardiann,
                TestHelpers.deterministicSample(queries, seed, NUM_ORDERED_BY_DISTANCE_QUERIES), setB.size(),
                MIN_ORDERED_BY_DISTANCE_QUALITY);
    }

    /**
     * Seeds the structure with {@code startup}, then deletes every record in random batches with no further
     * inserts. Recall must hold while the active set is still at least {@link #RECALL_K}; once fully drained the
     * structure must be empty. With no replenishing inserts the per-cluster primary counts only shrink, reliably
     * driving clusters below {@code primaryClusterMin} and exercising the merge (2&rarr;1) path.
     */
    private void runInsertThenDeleteAll(final long seed,
                                        @Nonnull final List<PrimaryKeyAndVector> startup,
                                        @Nonnull final List<DoubleRealVector> queries) throws Exception {
        final Guardiann guardiann = newGuardiann();
        logger.info("phase1: inserting startup sample ({} records) ...", startup.size());
        insertAndQuiesce(guardiann, startup);

        final Map<Tuple, RealVector> active = activeMapOf(startup);

        // ---- initial recall checkpoint ----
        TestHelpers.assertRecallAtKAtLeastDynamic(getDb(), guardiann, queries, active, RECALL_K, MIN_RECALL);
        TestHelpers.assertOrderedByDistanceQualityAtLeast(getDb(), guardiann,
                TestHelpers.deterministicSample(queries, seed, NUM_ORDERED_BY_DISTANCE_QUERIES), active.size(),
                MIN_ORDERED_BY_DISTANCE_QUALITY);

        // ---- delete everything in random batches (no inserts) ----
        final SplittableRandom deleteRng = new SplittableRandom(seed ^ INTERLEAVE_SEED_SALT);
        final Deque<PrimaryKeyAndVector> deleteQueue = new ArrayDeque<>(shuffledCopy(startup, deleteRng));
        int batchesSinceCheck = 0;
        int totalDeleteBatches = 0;
        while (!deleteQueue.isEmpty()) {
            final List<PrimaryKeyAndVector> batch = takeUpTo(deleteQueue, BATCH_SIZE);
            deleteOneBatch(guardiann, batch);
            for (final PrimaryKeyAndVector r : batch) {
                active.remove(r.primaryKey());
            }
            totalDeleteBatches++;
            batchesSinceCheck++;

            if (batchesSinceCheck >= RECALL_CHECK_INTERVAL_BATCHES) {
                batchesSinceCheck = 0;
                logger.info("checkpoint after deleteBatches={}: active.size={}, queue left={}",
                        totalDeleteBatches, active.size(), deleteQueue.size());
                // Recall@k is only measurable while at least k records remain; stop checking once the active set
                // dips below k (the tail of the drain) and just keep deleting.
                if (active.size() >= RECALL_K) {
                    TestHelpers.assertRecallAtKAtLeastDynamic(getDb(), guardiann, queries, active,
                            RECALL_K, MIN_RECALL);
                    TestHelpers.assertOrderedByDistanceQualityAtLeast(getDb(), guardiann,
                            TestHelpers.deterministicSample(queries, seed, NUM_ORDERED_BY_DISTANCE_QUERIES), active.size(),
                            MIN_ORDERED_BY_DISTANCE_QUALITY);
                } else {
                    logger.info("skipping recall check: active.size={} < k={}", active.size(), RECALL_K);
                }
            }
        }
        logger.info("drain complete: deleteBatches={}, active.size={}", totalDeleteBatches, active.size());

        // ---- drain & invariants (after-deletes) ----
        TestHelpers.assertGuardiannInvariantsAfterDeletes(getDb(), guardiann);

        // ---- the structure must be empty ----
        assertThat(active)
                .as("local active map must be empty after deleting every record")
                .isEmpty();
        final StructureSnapshot snap = TestHelpers.snapshotStructure(getDb(), guardiann);
        final int remainingPrimaries = snap == null ? 0 : snap.totalPrimaries();
        assertThat(remainingPrimaries)
                .as("no primaries may remain after deleting every record")
                .isZero();
    }

    // ---------------------------------------------------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------------------------------------------------

    @Nonnull
    private Guardiann newGuardiann() {
        return new Guardiann(getSubspace(),
                TestExecutors.defaultThreadPool(),
                buildConfig(),
                new TestHelpers.TestOnWriteListener(),
                new TestHelpers.TestOnReadListener());
    }

    /** Insert all of {@code dataset} in batches, then drain deferred tasks to quiescence. */
    private void insertAndQuiesce(@Nonnull final Guardiann guardiann,
                                  @Nonnull final List<PrimaryKeyAndVector> dataset) throws Exception {
        TestHelpers.insertRecords(getDb(), guardiann, dataset, BATCH_SIZE);
        TestHelpers.runToQuiescence(getDb(), guardiann);
    }

    /** A mutable {@code primaryKey -> vector} map over {@code records}, mirroring what is live in the index. */
    @Nonnull
    private static Map<Tuple, RealVector> activeMapOf(@Nonnull final List<PrimaryKeyAndVector> records) {
        final Map<Tuple, RealVector> active = new HashMap<>(records.size());
        for (final PrimaryKeyAndVector r : records) {
            active.put(r.primaryKey(), r.vector());
        }
        return active;
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

    @Nonnull
    private static List<DoubleRealVector> loadQueries(@Nonnull final String queryPath) throws IOException {
        final List<DoubleRealVector> all = TestHelpers.loadQueryVectors(queryPath);
        return List.copyOf(all.subList(0, Math.min(RECALL_NUM_QUERIES, all.size())));
    }

    private static void verifyDisjoint(@Nonnull final List<PrimaryKeyAndVector> a,
                                       @Nonnull final List<PrimaryKeyAndVector> b) {
        final Set<Tuple> pksA = a.stream()
                .map(PrimaryKeyAndVector::primaryKey)
                .collect(ImmutableSet.toImmutableSet());
        for (final PrimaryKeyAndVector r : b) {
            assertThat(pksA)
                    .as("setA and setB must be disjoint by primary key")
                    .doesNotContain(r.primaryKey());
        }
    }

    /**
     * Returns a freshly shuffled copy of {@code records}, using {@code rng} to drive an in-place Fisher–Yates
     * shuffle of the copy. The input list is left untouched.
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
     * Removes up to {@code batchSize} records from the front of {@code queue} and returns them. The final batch
     * from each queue may be smaller than {@code batchSize}; all earlier batches are exactly {@code batchSize}.
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

    static void scanCentroids(@Nonnull final Database db,
                              @Nonnull final Subspace subspace,
                              @Nonnull final com.apple.foundationdb.async.hnsw.Config config,
                              final int layer,
                              final int batchSize,
                              @Nonnull final Consumer<ResultEntry> consumer) {
        HNSW.scanLayer(config, subspace, db, layer, batchSize, consumer);
    }
}
