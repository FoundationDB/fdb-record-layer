/*
 * ReassignConvergenceTest.java
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
import com.apple.foundationdb.async.common.RandomHelpers;
import com.apple.foundationdb.async.common.StorageTransform;
import com.apple.foundationdb.linear.Metric;
import com.apple.foundationdb.linear.RealVector;
import com.apple.foundationdb.linear.Transformed;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.test.TestDatabaseExtension;
import com.apple.foundationdb.test.TestExecutors;
import com.apple.foundationdb.test.TestSubspaceExtension;
import com.apple.test.RandomSeedSource;
import com.apple.test.SuperSlow;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.apple.foundationdb.async.guardiann.SiftTestHelpers.SIFT_1M_BASE_PATH;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that running reassignment converges the structure: starting from the (intentionally imperfect)
 * post-insert state, a first round of reassigns pushes mis-homed primaries toward their nearest cluster (driving
 * the <em>wrong-assignment</em> count down), and a second round replicates the primaries that the first round
 * re-homed into new clusters (driving the <em>under-replication</em> count down).
 * <p>
 * Reassigns are driven directly, in memory, one cluster per transaction: for each cluster the test builds a
 * {@link ReassignTask} with a precomputed neighborhood and calls {@link ReassignTask#reassign} with
 * {@code enqueueFollowUpTasks=false}, so the two rounds are the only work — no follow-up split/reassign tasks are
 * enqueued. This is the realization of the "reassign-convergence" idea: insert, observe imperfection, then show two
 * controlled reassign rounds reduce it.
 * <p>
 * The post-insert state must actually be imperfect for the test to mean anything; it almost always is (small,
 * bordered clusters), but in the rare degenerate case the test deactivates itself via {@link Assumptions}.
 */
public class ReassignConvergenceTest implements BaseTest {
    private static final Logger logger = LoggerFactory.getLogger(ReassignConvergenceTest.class);

    /** Number of SIFT-small vectors for the fast variant (a seeded subsample of the 10k base set). */
    private static final int SAMPLE_SIZE_SMALL = 5_000;
    /** Number of SIFT-1M vectors for the {@link SuperSlow} variant (a seeded reservoir sample). */
    private static final int SAMPLE_SIZE_LARGE = 100_000;
    /** Insert batch size. */
    private static final int BATCH_SIZE = 50;

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

    @ParameterizedTest(name = "[{index}] seed={0}")
    @RandomSeedSource
    @Timeout(value = 10, unit = TimeUnit.MINUTES)
    void reassignConvergenceSiftSmall(final long seed) throws Exception {
        runConvergence(seed, newGuardiannSmall(), seededSample(seed, SAMPLE_SIZE_SMALL));
    }

    @ParameterizedTest(name = "[{index}] seed={0}")
    @RandomSeedSource
    @SuperSlow
    @Timeout(value = 2, unit = TimeUnit.HOURS)
    void reassignConvergenceSiftSubsampledSlow(final long seed) throws Exception {
        runConvergence(seed, newGuardiannBig(), TestHelpers.loadSample(SIFT_1M_BASE_PATH, seed, SAMPLE_SIZE_LARGE));
    }

    /**
     * Shared convergence flow: insert {@code sample} and run to quiescence, then drive two reassign rounds and
     * assert the wrong-assignment count drops after round 1 and the under-replication count drops after round 2.
     */
    private void runConvergence(final long seed, @Nonnull final Guardiann guardiann,
                                @Nonnull final List<PrimaryKeyAndVector> sample) throws Exception {
        // ---- Phase 0: insert the sample; let everything (including auto-enqueued reassigns) run. ----
        logger.info("seed={} inserting {} vectors", seed, sample.size());
        TestHelpers.insertRecords(getDb(), guardiann, sample, BATCH_SIZE);
        TestHelpers.runToQuiescence(getDb(), guardiann);

        // ---- Baseline: the post-insert state must be imperfect to have anything to converge. ----
        final StructureSnapshot postInsert = Objects.requireNonNull(
                TestHelpers.snapshotStructure(getDb(), guardiann), "structure after inserts");
        final int wrong0 = postInsert.computeAssignmentRanking().numWrongAssignments();
        final int under0 = countUnderReplicatedPrimaries(postInsert);
        logger.info("seed={} post-insert: clusters={}, wrongAssignments={}, underReplicated={}",
                seed, postInsert.numClusters(), wrong0, under0);
        Assumptions.assumeTrue(wrong0 > 0,
                "no incorrectly-assigned primaries post-insert; nothing to converge (rare)");
        Assumptions.assumeTrue(under0 > 0,
                "no under-replicated primaries post-insert; nothing to converge (rare)");

        // ---- Round 1: re-home mis-assigned primaries toward their nearest cluster. ----
        reassignEveryClusterOnce(guardiann, "round-1");
        final StructureSnapshot afterRound1 = Objects.requireNonNull(
                TestHelpers.snapshotStructure(getDb(), guardiann), "structure after round 1");
        final int wrong1 = afterRound1.computeAssignmentRanking().numWrongAssignments();
        final int under1 = countUnderReplicatedPrimaries(afterRound1);
        logger.info("seed={} after round 1: wrongAssignments={} (was {}), underReplicated={} (was {})",
                seed, wrong1, wrong0, under1, under0);

        // ---- Round 2: replicate the primaries that round 1 pushed into new (now-correct) clusters. ----
        reassignEveryClusterOnce(guardiann, "round-2");
        final StructureSnapshot afterRound2 = Objects.requireNonNull(
                TestHelpers.snapshotStructure(getDb(), guardiann), "structure after round 2");
        final int wrong2 = afterRound2.computeAssignmentRanking().numWrongAssignments();
        final int under2 = countUnderReplicatedPrimaries(afterRound2);
        logger.info("seed={} after round 2: wrongAssignments={} (was {}), underReplicated={} (was {})",
                seed, wrong2, wrong1, under2, under1);
        logger.info("seed={} convergence summary: wrong {} -> {} -> {}, underReplicated {} -> {} -> {}",
                seed, wrong0, wrong1, wrong2, under0, under1, under2);

        // ---- Assertions (intentionally simple for now; calibrate later from the logged progression). ----
        assertThat(wrong1)
                .as("seed=%s: round 1 of reassigns must reduce the number of incorrectly-assigned primaries", seed)
                .isLessThan(wrong0);
        assertThat(under2)
                .as("seed=%s: round 2 of reassigns must reduce the number of under-replicated primaries", seed)
                .isLessThan(under1);
    }

    // ---------------------------------------------------------------------------------------------------------
    // Reassign driver
    // ---------------------------------------------------------------------------------------------------------

    /**
     * Drives one reassign for every current cluster, each in its own transaction, building the task in memory and
     * calling {@link ReassignTask#reassign} directly with {@code enqueueFollowUpTasks=false} (so no follow-up
     * tasks are enqueued). Logs progress so a long run is visibly advancing.
     */
    private void reassignEveryClusterOnce(@Nonnull final Guardiann guardiann, @Nonnull final String roundName) {
        final StructureSnapshot snapshot = TestHelpers.snapshotStructure(getDb(), guardiann);
        if (snapshot == null) {
            return;
        }
        final List<ClusterView> clusters = List.copyOf(snapshot.clusters().values());
        final int total = clusters.size();
        logger.info("{}: reassigning {} clusters", roundName, total);
        int done = 0;
        for (final ClusterView cv : clusters) {
            reassignOneCluster(guardiann, cv.clusterId(), cv.transformedCentroid());
            done++;
            if (done % 10 == 0 || done == total) {
                logger.info("{}: reassigned {}/{} clusters", roundName, done, total);
            }
        }
    }

    /** Reassigns a single cluster in its own transaction; builds the {@link ReassignTask} in memory. */
    private void reassignOneCluster(@Nonnull final Guardiann guardiann,
                                    @Nonnull final UUID clusterId,
                                    @Nonnull final Transformed<RealVector> centroid) {
        final Locator locator = guardiann.getLocator();
        final Primitives primitives = locator.primitives();
        final int numNeighborhood = 1 + guardiann.getConfig().reassignOuterNeighborhoodSize();

        getDb().run(transaction -> {
            final AccessInfo accessInfo = primitives.fetchAccessInfo(transaction).join();
            final ClusterMetadata clusterMetadata = primitives.fetchClusterMetadata(transaction, clusterId).join();
            if (clusterMetadata == null) {
                return null;
            }
            final StorageTransform storageTransform = primitives.storageTransform(accessInfo);
            final RealVector untransformedCentroid = storageTransform.untransform(centroid);

            final List<ClusterMetadataWithDistance> neighborhoodMetadata =
                    primitives.fetchNeighborhoodClusterMetadata(transaction, clusterMetadata,
                            untransformedCentroid, storageTransform, numNeighborhood).join();
            final List<ClusterReference> neighborhood =
                    ClusterReference.fromClusterMetadataAndDistances(neighborhoodMetadata);

            // Deterministic, in-memory-only task id (drives only the internal tie-break RNG; never enqueued).
            final UUID taskId = RandomHelpers.randomUuid(clusterId, true);
            final ReassignTask reassignTask = ReassignTask.of(locator, accessInfo, taskId, clusterId,
                    centroid, Set.of(), neighborhood);
            reassignTask.reassign(transaction, clusterMetadata, untransformedCentroid, false).join();
            return null;
        });
    }

    // ---------------------------------------------------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------------------------------------------------

    /** Sum of {@link ClusterMetadata#numPrimaryUnderreplicatedVectors()} across all clusters (authoritative). */
    private static int countUnderReplicatedPrimaries(@Nonnull final StructureSnapshot snapshot) {
        return snapshot.clusters().values().stream()
                .mapToInt(cv -> cv.metadata().numPrimaryUnderreplicatedVectors())
                .sum();
    }

    /** A deterministic, seed-shuffled subsample of the SIFT-small base set (uses the always-present 10k file). */
    @Nonnull
    private static List<PrimaryKeyAndVector> seededSample(final long seed, final int size) throws Exception {
        final List<PrimaryKeyAndVector> all =
                new ArrayList<>(TestHelpers.loadVectors(SiftTestHelpers.SIFT_SMALL_BASE_PATH, 10_000));
        Collections.shuffle(all, new Random(seed));
        return List.copyOf(all.subList(0, Math.min(size, all.size())));
    }

    /**
     * Config for the fast variant (~5k SIFT-small subsample): tiny, bordered clusters (mirrors
     * {@code ReassignScenarioTest}) so the post-insert structure is rich in mis-homed and under-replicated
     * primaries for the rounds to converge.
     */
    @Nonnull
    private Guardiann newGuardiannSmall() {
        return guardiannFor(Guardiann.newConfigBuilder()
                .setUseRaBitQ(true)
                .setRaBitQNumExBits(6)
                .setMetric(Metric.EUCLIDEAN_METRIC)
                .setPrimaryClusterMax(80)
                .setPrimaryClusterMin(1)
                .setDeterministicRandomness(true)
                .setUnderreplicatedPrimaryClusterMax(3)
                .setReplicationPriorityMin(0.65d)
                .setReplicatedClusterTarget(5)
                .setReplicatedClusterMaxWrites(2000)
                .build(128));
    }

    /**
     * Config for the {@link SuperSlow} variant (~100k SIFT-1M subsample): the same character as the small config,
     * but with the cluster-size knobs scaled up so the index does not split on nearly every insert. At
     * {@code primaryClusterMax=80} a 100k-vector load yields well over a thousand clusters and a split storm;
     * raising it to 512 (matching {@code SiftTest}) keeps the cluster count in the low hundreds. The reassign
     * trigger ({@code underreplicatedPrimaryClusterMax}) is likewise raised so inserts do not reassign-storm, while
     * the replica capacity ({@code replicatedClusterTarget}) stays proportionally small (~1/10 of the primary cap,
     * the library default) so under-replication still arises for the rounds to converge.
     */
    @Nonnull
    private Guardiann newGuardiannBig() {
        return guardiannFor(Guardiann.newConfigBuilder()
                .setUseRaBitQ(true)
                .setRaBitQNumExBits(6)
                .setMetric(Metric.EUCLIDEAN_METRIC)
                .setPrimaryClusterMax(512)
                .setPrimaryClusterMin(100)
                .setDeterministicRandomness(true)
                .setUnderreplicatedPrimaryClusterMax(50)
                .setReplicationPriorityMin(0.65d)
                .setReplicatedClusterTarget(50)
                .setReplicatedClusterMaxWrites(2000)
                .build(128));
    }

    @Nonnull
    private Guardiann guardiannFor(@Nonnull final Config config) {
        return new Guardiann(getSubspace(),
                TestExecutors.defaultThreadPool(),
                config,
                new TestHelpers.TestOnWriteListener(),
                new TestHelpers.TestOnReadListener());
    }
}
