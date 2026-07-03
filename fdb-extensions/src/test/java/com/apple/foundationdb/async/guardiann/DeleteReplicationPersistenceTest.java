/*
 * DeleteReplicationPersistenceTest.java
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
import com.apple.foundationdb.async.common.CommonTestHelpers;
import com.apple.foundationdb.async.common.PrimaryKeyAndVector;
import com.apple.foundationdb.async.common.RandomHelpers;
import com.apple.foundationdb.linear.DoubleRealVector;
import com.apple.foundationdb.linear.Metric;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.test.TestDatabaseExtension;
import com.apple.foundationdb.test.TestExecutors;
import com.apple.foundationdb.test.TestSubspaceExtension;
import com.apple.test.RandomizedTestUtils;
import com.google.common.base.Verify;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.SplittableRandom;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Correctness test that deleting vectors leaves the surviving structure replicated. A delete removes only the
 * deleted vector's own copies (its primary plus its replicas in neighboring clusters), and the deferred-task
 * cascade it triggers (merges, reassigns, bounces) must re-establish replication for the vectors that remain — so a
 * bordered, multi-cluster structure stays replicated end to end.
 * <p>
 * The test inserts enough near-duplicates of a single SIFT base that the cluster overflows {@code primaryClusterMax}
 * and splits into several close sub-clusters (whose border vectors get replicas), then deletes a moderate fraction.
 * Those deletes <em>do</em> reorganize the structure (the delete-phase tasks logged below typically include merges,
 * reassigns and bounces), but because a single base keeps the surviving clusters close, borders — and therefore
 * replication — remain. The test asserts the end state still has multiple clusters with valid replicas.
 * <p>
 * This complements {@code DeterministicReplayTest}, whose heavy delete-everything workload legitimately ends with
 * zero replicas because its deletes merge the only borders away into well-separated single clusters.
 */
public class DeleteReplicationPersistenceTest implements BaseTest {
    private static final Logger logger = LoggerFactory.getLogger(DeleteReplicationPersistenceTest.class);

    /** Cluster size cap. Small so the single base's near-duplicates overflow it and split into sub-clusters. */
    private static final int CLUSTER_MAX = 50;
    /** Minimum primaries per cluster; a delete that drops a cluster below this would trigger a (border-dissolving) merge. */
    private static final int PRIMARY_CLUSTER_MIN = 10;
    /** Near-duplicates of one base to insert; {@code > CLUSTER_MAX} so the cluster splits into bordered sub-clusters. */
    private static final int NUM_INSERTS = 150;
    /** Vectors to delete — a moderate fraction that reorganizes the structure while keeping multiple close clusters. */
    private static final int NUM_DELETES = 30;
    /** Per-component Gaussian sigma for the perturbation. Small relative to SIFT's ~[0, 200] range. */
    private static final double PERTURBATION_SIGMA = 0.5d;

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

    @Nonnull
    private static Stream<Long> seeds() {
        return RandomizedTestUtils.randomSeeds(0xC0FFEEL, 0xDEADBEEFL, 0x5EED1234L);
    }

    @ParameterizedTest(name = "seed={0}")
    @MethodSource("seeds")
    void deletesLeaveSurvivingStructureReplicated(final long seed) throws Exception {
        final List<PrimaryKeyAndVector> inserts = buildInserts(seed);

        final Subspace runSubspace = getSubspace();
        final Config config = Guardiann.newConfigBuilder()
                .setUseRaBitQ(true)
                .setRaBitQNumExBits(6)
                .setMetric(Metric.EUCLIDEAN_METRIC)
                .setPrimaryClusterMax(CLUSTER_MAX)
                .setPrimaryClusterMin(PRIMARY_CLUSTER_MIN)
                .setDeterministicRandomness(true)
                .setReplicationPriorityMin(0.65d)
                .setReplicatedClusterTarget(40)
                .setReplicatedClusterMaxWrites(200)
                .build(128);
        final TestHelpers.TestOnWriteListener onWriteListener = new TestHelpers.TestOnWriteListener();
        final Guardiann guardiann = new Guardiann(runSubspace,
                TestExecutors.defaultThreadPool(),
                config,
                onWriteListener,
                new TestHelpers.TestOnReadListener());

        // ---- Phase 1: insert near-duplicates; the oversized cluster splits into bordered sub-clusters. ----
        onWriteListener.pushFrame();
        for (final PrimaryKeyAndVector op : inserts) {
            db.run(transaction -> {
                guardiann.insert(transaction, op.primaryKey(), op.vector(), null).join();
                return null;
            });
        }
        TestHelpers.runToQuiescence(db, guardiann);
        logger.info("seed={} insert-phase tasks executed by kind={}",
                String.format("%#x", seed), Map.copyOf(onWriteListener.getNumTasksExecutedByKind()));
        onWriteListener.popFrame();

        final StructureSnapshot afterInsert =
                Objects.requireNonNull(TestHelpers.snapshotStructure(db, guardiann), "structure after inserts");
        logger.info("seed={} after-insert: clusters={}, primaries={}, replicas={}",
                String.format("%#x", seed), afterInsert.numClusters(), afterInsert.totalPrimaries(),
                afterInsert.totalReplicas());
        assertThat(afterInsert.numClusters())
                .as("seed=%#x: inserting %d near-duplicates with primaryClusterMax=%d must split into bordered "
                        + "sub-clusters", seed, NUM_INSERTS, CLUSTER_MAX)
                .isGreaterThanOrEqualTo(2);
        assertThat(afterInsert.totalReplicas())
                .as("seed=%#x: border vectors of the split sub-clusters must be replicated (otherwise the test "
                        + "premise — that there are replicas to preserve — does not hold)", seed)
                .isGreaterThan(0);

        // ---- Phase 2: delete a moderate fraction; this reorganizes the structure (merges/reassigns/bounces). ----
        final List<PrimaryKeyAndVector> deletes = inserts.subList(0, NUM_DELETES);
        onWriteListener.pushFrame();
        for (final PrimaryKeyAndVector op : deletes) {
            db.run(transaction -> {
                guardiann.delete(transaction, op.primaryKey(), op.vector()).join();
                return null;
            });
        }
        TestHelpers.runToQuiescence(db, guardiann);
        final Map<TaskKind, Integer> deleteTasks =
                Map.copyOf(onWriteListener.getNumTasksExecutedByKind());
        logger.info("seed={} delete-phase tasks executed by kind={}", String.format("%#x", seed), deleteTasks);
        onWriteListener.popFrame();

        final StructureSnapshot afterDelete =
                Objects.requireNonNull(TestHelpers.snapshotStructure(db, guardiann), "structure after deletes");
        logger.info("seed={} after-delete: clusters={}, primaries={}, replicas={}",
                String.format("%#x", seed), afterDelete.numClusters(), afterDelete.totalPrimaries(),
                afterDelete.totalReplicas());

        // Deletes are allowed to reorganize the structure (see the delete-phase task kinds logged above — they
        // typically include merges, reassigns and bounces). What must hold is the end state: a surviving,
        // multi-cluster, replicated structure.
        assertThat(afterDelete.numClusters())
                .as("seed=%#x: the surviving structure must stay multi-cluster, so borders — and therefore "
                        + "replication — remain meaningful", seed)
                .isGreaterThanOrEqualTo(2);
        assertThat(afterDelete.totalReplicas())
                .as("seed=%#x: after deleting %d of %d vectors (including any merges/reassigns that fired), the "
                        + "surviving border vectors must still be replicated", seed, NUM_DELETES, NUM_INSERTS)
                .isGreaterThan(0);

        // The surviving replicas must be valid: every replica references a live primary, every primary is unique.
        TestHelpers.assertGuardiannInvariantsAfterDeletes(db, guardiann);
    }

    @Nonnull
    private List<PrimaryKeyAndVector> buildInserts(final long seed) throws Exception {
        final List<PrimaryKeyAndVector> baseLoaded =
                TestHelpers.loadVectors(SiftTestHelpers.SIFT_SMALL_BASE_PATH, 1);
        Verify.verify(!baseLoaded.isEmpty(), "SIFT-small must contain at least one vector");
        final DoubleRealVector base = (DoubleRealVector) baseLoaded.get(0).vector();

        final SplittableRandom rnd = new SplittableRandom(seed);
        final RandomHelpers.GaussianSampler sampler = new RandomHelpers.GaussianSampler(rnd);

        final List<PrimaryKeyAndVector> inserts = new ArrayList<>(NUM_INSERTS);
        for (int i = 0; i < NUM_INSERTS; i++) {
            inserts.add(new PrimaryKeyAndVector(CommonTestHelpers.createPrimaryKey(i),
                    CommonTestHelpers.perturb(base, sampler, PERTURBATION_SIGMA)));
        }
        return inserts;
    }
}
