/*
 * DeterministicReplayTest.java
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
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Range;
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
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.RandomizedTestUtils;
import com.apple.test.Tags;
import com.google.common.base.Verify;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SplittableRandom;
import java.util.TreeMap;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies guardiann's deterministic-replay contract: with {@link Config#deterministicRandomness()} enabled,
 * replaying the <em>same</em> ordered sequence of inserts and deletes against a fresh store — and draining every
 * deferred task (split/merge, reassign, collapse, bounce) — must produce identical resulting state.
 * <p>
 * The test is parameterized over random seeds (see {@link RandomizedTestUtils#randomSeeds}); the seed governs the
 * generated workload (which SIFT base each vector is perturbed from, and the perturbation noise), so each iteration
 * exercises a differently-shaped cluster structure. Within a single iteration both runs replay the <em>identical</em>
 * generated op sequence into two sibling subspaces.
 * <p>
 * Because every UUID (vector ids, cluster ids, collapsed-reference ids, sampled-vector keys, and deferred-task ids)
 * is now derived from a {@link SplittableRandom} seeded only from stable inputs — the record's primary key or the
 * task's id — rather than from a process-global counter, the two runs produce the same cluster ids, the same
 * vector-to-cluster assignments, and the same encoded bytes. Under the old global {@code SequentialUUID} counter
 * this test would fail: the second run's counter continued from where the first left off, so its cluster ids — and
 * therefore its keys — diverged.
 * <p>
 * Inserting more vectors than fit in one cluster forces splits (and border replication / reassign); deleting a
 * fraction of them reorganizes the structure via merges / reassigns / bounces, but keeps each base's close
 * sub-clusters — and therefore their border replicas — alive, so the replicated structure survives to the end.
 * <p>
 * Tagged {@link Tags#RequiresScalar} so it runs only under the {@code scalarFallbackTest} task (which sets
 * {@code fdb.vector.simd=scalar}), not the default SIMD test task. The replay assertions compare byte-identical
 * stored state, which depends on bit-exact, reproducible floating-point results; SIMD lane-wise reductions sum in
 * a different order than scalar accumulation, so that bit-exactness is only guaranteed on the scalar backend.
 */
@Tag(Tags.RequiresScalar)
public class DeterministicReplayTest implements BaseTest {
    private static final Logger logger = LoggerFactory.getLogger(DeterministicReplayTest.class);

    /** Cluster size cap. Small so each base's group of inserts overflows it and splits. */
    private static final int CLUSTER_MAX = 50;
    /** Minimum primaries per cluster; dropping below this is what triggers a merge. */
    private static final int PRIMARY_CLUSTER_MIN = 10;
    /** Number of distinct SIFT base vectors the inserts are perturbed around (gives several natural clusters). */
    private static final int NUM_BASES = 2;
    /** Total vectors to insert. Spread over {@link #NUM_BASES} bases, each group (~120) overflows {@link #CLUSTER_MAX}. */
    private static final int NUM_INSERTS = 240;
    /** Survivors after the delete phase. Kept high enough per base (~80) that the sub-cluster borders — and thus
     * the replicas — survive the merges/reassigns the deletes trigger. */
    private static final int REMAINING_AFTER_DELETE = 160;
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
    void identicalOpsReplayedInDeterministicModeProduceIdenticalState(final long seed) throws Exception {
        // Build the op sequence ONCE (from the seed) so both runs see byte-identical input vectors.
        final List<PrimaryKeyAndVector> inserts = buildInserts(seed);
        final List<PrimaryKeyAndVector> deletes =
                new ArrayList<>(inserts.subList(0, NUM_INSERTS - REMAINING_AFTER_DELETE));

        final RunResult a = replay("run-a", inserts, deletes);
        final RunResult b = replay("run-b", inserts, deletes);

        // Log exactly what is being compared, at both phases, before asserting — so a failure is diagnosable and a
        // passing run shows it compared real, non-trivial state.
        logComparison(seed, "after-insert", a.afterInsert(), b.afterInsert());
        logComparison(seed, "after-delete", a.afterDelete(), b.afterDelete());

        // --- Non-degeneracy: guard against silently comparing trivial (or empty) state. ---
        assertThat(a.afterInsert().numClusters())
                .as("seed=%#x: inserting %d vectors over %d bases with primaryClusterMax=%d must split into "
                        + ">= 2 clusters", seed, NUM_INSERTS, NUM_BASES, CLUSTER_MAX)
                .isGreaterThanOrEqualTo(2);
        assertThat(a.afterInsert().dump())
                .as("seed=%#x: the after-insert state being compared must be non-empty", seed)
                .isNotEmpty();
        assertThat(a.afterDelete().totalPrimaries() + a.afterDelete().totalCollapsedRefs())
                .as("seed=%#x: the delete phase must leave survivors — otherwise the after-delete comparison "
                        + "would be empty-to-empty", seed)
                .isGreaterThan(0);
        assertThat(a.afterDelete().dump())
                .as("seed=%#x: the surviving after-delete state being compared must be non-empty", seed)
                .isNotEmpty();
        assertThat(a.afterDelete().totalReplicas())
                .as("seed=%#x: the surviving multi-cluster structure must still carry replicas, so the "
                        + "byte-identical comparison also covers replicated vectors", seed)
                .isGreaterThan(0);

        // --- Determinism: identical state across the two replays, at both phases. ---
        assertThat(b.afterInsert().clusterFingerprint())
                .as("seed=%#x: after inserts, the two deterministic replays must produce the same clusters and "
                        + "vector-to-cluster assignments", seed)
                .isEqualTo(a.afterInsert().clusterFingerprint());
        assertThat(b.afterInsert().dump())
                .as("seed=%#x: after inserts, the two deterministic replays must produce byte-identical state", seed)
                .isEqualTo(a.afterInsert().dump());
        assertThat(b.afterDelete().clusterFingerprint())
                .as("seed=%#x: after deletes, the two deterministic replays must produce the same clusters and "
                        + "vector-to-cluster assignments", seed)
                .isEqualTo(a.afterDelete().clusterFingerprint());
        assertThat(b.afterDelete().dump())
                .as("seed=%#x: after deletes, the two deterministic replays must produce byte-identical state", seed)
                .isEqualTo(a.afterDelete().dump());
    }

    /**
     * Runs the full insert-then-delete workload against a fresh subspace in deterministic mode, draining all
     * deferred tasks after each phase, logging the tasks executed per phase by kind, and capturing the resulting
     * state at both phases.
     *
     * @param runName distinguishes this run's subspace from the other run's
     * @param inserts the vectors to insert, in order
     * @param deletes the vectors to delete afterwards, in order
     *
     * @return the captures taken after the insert phase and after the delete phase
     */
    @Nonnull
    private RunResult replay(@Nonnull final String runName,
                             @Nonnull final List<PrimaryKeyAndVector> inserts,
                             @Nonnull final List<PrimaryKeyAndVector> deletes) {
        final Subspace runSubspace = getSubspace().subspace(Tuple.from(runName));
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

        onWriteListener.pushFrame();
        for (final PrimaryKeyAndVector op : inserts) {
            db.run(transaction -> {
                guardiann.insert(transaction, op.primaryKey(), op.vector(), null).join();
                return null;
            });
        }
        TestHelpers.runToQuiescence(db, guardiann);
        logger.info("{}: insert-phase tasks executed by kind={}", runName,
                Map.copyOf(onWriteListener.getNumTasksExecutedByKind()));
        onWriteListener.popFrame();
        final Capture afterInsert = capture(guardiann, runSubspace);

        onWriteListener.pushFrame();
        for (final PrimaryKeyAndVector op : deletes) {
            db.run(transaction -> {
                guardiann.delete(transaction, op.primaryKey(), op.vector()).join();
                return null;
            });
        }
        TestHelpers.runToQuiescence(db, guardiann);
        logger.info("{}: delete-phase tasks executed by kind={}", runName,
                Map.copyOf(onWriteListener.getNumTasksExecutedByKind()));
        onWriteListener.popFrame();
        final Capture afterDelete = capture(guardiann, runSubspace);

        return new RunResult(afterInsert, afterDelete);
    }

    /** Snapshots the cluster topology and dumps the raw stored state for the run's subspace. */
    @Nonnull
    private Capture capture(@Nonnull final Guardiann guardiann, @Nonnull final Subspace runSubspace) {
        final StructureSnapshot snapshot = TestHelpers.snapshotStructure(db, guardiann);
        Verify.verifyNotNull(snapshot, "structure must be non-empty");
        return new Capture(snapshot.numClusters(), snapshot.totalPrimaries(), snapshot.totalReplicas(),
                snapshot.totalCollapsedRefs(), clusterFingerprint(snapshot), dump(runSubspace));
    }

    private static void logComparison(final long seed, @Nonnull final String phase,
                                      @Nonnull final Capture a, @Nonnull final Capture b) {
        logger.info("seed={} {}: comparing run-a/run-b — clusters={}/{}, primaries={}/{}, replicas={}/{}, "
                        + "collapsed={}/{}, storedKeys={}/{}",
                String.format("%#x", seed), phase,
                a.numClusters(), b.numClusters(),
                a.totalPrimaries(), b.totalPrimaries(),
                a.totalReplicas(), b.totalReplicas(),
                a.totalCollapsedRefs(), b.totalCollapsedRefs(),
                a.dump().size(), b.dump().size());
    }

    /**
     * Builds a canonical, floating-point-free fingerprint of the cluster topology: each cluster id mapped to its
     * integer vector counts, its states, and the sorted ids of its primary / replica / collapsed members. The
     * centroid and running statistics (both floating point) are intentionally excluded so the comparison is exact.
     */
    @Nonnull
    private static Map<UUID, String> clusterFingerprint(@Nonnull final StructureSnapshot snapshot) {
        final Map<UUID, String> fingerprint = new TreeMap<>();
        for (final ClusterView cv : snapshot.clusters().values()) {
            final ClusterMetadata metadata = cv.metadata();
            fingerprint.put(cv.clusterId(),
                    "p=" + metadata.getNumPrimaryVectors()
                            + ",pu=" + metadata.numPrimaryUnderreplicatedVectors()
                            + ",r=" + metadata.numReplicatedVectors()
                            + ",states=" + metadata.states()
                            + ",primaries=[" + sortedIds(cv.primaries()) + "]"
                            + ",replicas=[" + sortedIds(cv.replicas()) + "]"
                            + ",collapsed=[" + sortedIds(cv.collapsedRefs()) + "]");
        }
        return fingerprint;
    }

    @Nonnull
    private static String sortedIds(@Nonnull final Set<VectorId> ids) {
        return ids.stream().map(VectorId::toString).sorted().collect(Collectors.joining(","));
    }

    /**
     * Dumps every key/value stored under the run's subspace, keyed by the printable subspace-relative key (the
     * subspace prefix is stripped so the two runs' dumps are directly comparable).
     */
    @Nonnull
    private Map<String, String> dump(@Nonnull final Subspace runSubspace) {
        final byte[] prefix = runSubspace.pack();
        return db.run(transaction -> {
            final List<KeyValue> keyValues =
                    transaction.getRange(Range.startsWith(prefix)).asList().join();
            final Map<String, String> result = new TreeMap<>();
            for (final KeyValue keyValue : keyValues) {
                final byte[] key = keyValue.getKey();
                final byte[] relativeKey = Arrays.copyOfRange(key, prefix.length, key.length);
                result.put(ByteArrayUtil.printable(relativeKey), ByteArrayUtil.printable(keyValue.getValue()));
            }
            return result;
        });
    }

    /**
     * Generates the insert workload from the seed: {@link #NUM_INSERTS} vectors, each a Gaussian perturbation of one
     * of {@link #NUM_BASES} distinct SIFT base vectors chosen by the seeded random.
     */
    @Nonnull
    private List<PrimaryKeyAndVector> buildInserts(final long seed) throws Exception {
        final List<PrimaryKeyAndVector> baseLoaded =
                TestHelpers.loadVectors(SiftTestHelpers.SIFT_SMALL_BASE_PATH, NUM_BASES);
        Verify.verify(baseLoaded.size() == NUM_BASES, "SIFT-small must contain at least %s vectors", NUM_BASES);
        final List<DoubleRealVector> bases = new ArrayList<>(NUM_BASES);
        for (final PrimaryKeyAndVector pkv : baseLoaded) {
            bases.add((DoubleRealVector) pkv.vector());
        }

        final SplittableRandom rnd = new SplittableRandom(seed);
        final RandomHelpers.GaussianSampler sampler = new RandomHelpers.GaussianSampler(rnd);

        final List<PrimaryKeyAndVector> inserts = new ArrayList<>(NUM_INSERTS);
        for (int i = 0; i < NUM_INSERTS; i++) {
            final DoubleRealVector base = bases.get(rnd.nextInt(NUM_BASES));
            inserts.add(new PrimaryKeyAndVector(CommonTestHelpers.createPrimaryKey(i),
                    CommonTestHelpers.perturb(base, sampler, PERTURBATION_SIGMA)));
        }
        return inserts;
    }

    /** A point-in-time capture of one run: cluster/vector counts, an FP-free topology fingerprint, and the KV dump. */
    private record Capture(int numClusters,
                           int totalPrimaries,
                           int totalReplicas,
                           int totalCollapsedRefs,
                           @Nonnull Map<UUID, String> clusterFingerprint,
                           @Nonnull Map<String, String> dump) {
    }

    /** The two captures taken during one replay: right after the inserts drain, and after the deletes drain. */
    private record RunResult(@Nonnull Capture afterInsert,
                             @Nonnull Capture afterDelete) {
    }
}
