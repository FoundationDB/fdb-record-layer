/*
 * SplitScenarioTest.java
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
import com.apple.foundationdb.test.TestSubspaceExtension;
import com.apple.foundationdb.test.TestDatabaseExtension;
import com.apple.foundationdb.test.TestExecutors;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.SplittableRandom;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Phase 2 scenario: forces an oversized cluster to split. With {@code primaryClusterMax} tuned
 * down to a small value, inserting more near-duplicates than fit in one cluster must trigger
 * at least one {@link SplitMergeTask} execution and leave the structure with more clusters
 * than it started with. The near-duplicates are perturbed copies of one SIFT-small vector with
 * a tiny per-component Gaussian noise — enough to give each a distinct content signature (so
 * {@link CollapseTask} doesn't fire instead) but small enough that they all stay near the same
 * region of vector space.
 */
public class SplitScenarioTest implements BaseTest {
    private static final Logger logger = LoggerFactory.getLogger(SplitScenarioTest.class);

    /** Cluster size cap. Picked small so the trigger fires after a handful of inserts. */
    private static final int CLUSTER_MAX = 50;

    /** Number of near-duplicates to insert. Must be {@code > CLUSTER_MAX} so a split is forced. */
    private static final int NUM_NEAR_DUPLICATES = 100;

    /**
     * How many of a cluster's own primaries to strip of their {@link VectorMetadata}, staging the drift that
     * {@code repartitioningReportsTheStaleReferencesItDiscards} measures. Kept small so the cluster still holds enough
     * live vectors for k-means to partition.
     */
    private static final int NUM_REFERENCES_TO_ORPHAN = 3;

    /**
     * Inserts for the stale-reference test, made without draining so a split stays pending. Above {@link #CLUSTER_MAX}
     * so the target is genuinely oversized, and below {@code primaryClusterHardMax} (twice the cap) so the undrained
     * backlog does not back-pressure the inserts themselves.
     */
    private static final int NUM_INSERTS_WITHOUT_DRAINING = 60;

    /** Per-component Gaussian sigma for the perturbation. Small relative to SIFT's ~[0, 200] range. */
    private static final double PERTURBATION_SIGMA = 0.5d;

    /** Deterministic seed for the perturbation noise. */
    private static final long PERTURBATION_SEED = 0x5C12_AB17_BEEFL;

    @RegisterExtension
    static final TestDatabaseExtension dbExtension = new TestDatabaseExtension();
    // Per-test rather than per-class: each test drives a structure from empty, reusing the same primary keys, so they
    // must not share a subspace.
    @RegisterExtension
    final TestSubspaceExtension subspaceExtension = new TestSubspaceExtension(dbExtension);

    @TempDir
    Path tempDir;

    private static Database db;
    private Guardiann guardiann;
    private TestHelpers.TestOnWriteListener onWriteListener;

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

    @BeforeEach
    public void setUpGuardiann() {
        onWriteListener = new TestHelpers.TestOnWriteListener();
        final TestHelpers.TestOnReadListener onReadListener = new TestHelpers.TestOnReadListener();

        final Config config = ConfigRecommendation.forClusterBounds(Metric.EUCLIDEAN_METRIC, CLUSTER_MAX, 10)
                .setUseRaBitQ(true)
                .setRaBitQNumExBits(6)
                .setCollapseMinDuplicates(CLUSTER_MAX / 2)
                .setDeterministicRandomness(true)
                .setReplicationPriorityMin(0.65d)
                .setReplicatedClusterTarget(40)
                .setReplicatedClusterMaxWrites(200)
                .build(128);

        guardiann = new Guardiann(subspaceExtension.getSubspace(),
                TestExecutors.defaultThreadPool(),
                config,
                onWriteListener,
                onReadListener);
    }

    /**
     * A repartitioning reports the references it discards as stale, so an operator can tell a cluster whose recorded
     * count has drifted from one that is genuinely oversized.
     * <p>
     * Deleting a vector normally removes its reference too, so the drift this measures only arises when a delete fails
     * to find the reference — rare, and not something the suite produces on its own. It is staged here instead:
     * removing a vector's {@link VectorMetadata} while leaving its reference in place is exactly the state such a
     * delete leaves behind, and it is the state {@code cleanUpVectorReferences} exists to tolerate. The cluster's
     * recorded primary count is untouched by the staging, so the task still sees an oversized cluster and repartitions
     * it; the discarded references surface on the way through.
     */
    @Test
    void repartitioningReportsTheStaleReferencesItDiscards() throws Exception {
        // Insert without draining, so the cluster ends up genuinely oversized with its SplitMergeTask still pending.
        // Draining first would settle every cluster at or below the cap, and the task would then dismiss itself as a
        // false alarm before reaching the reconciliation this test is about.
        insertNearDuplicateCloud(NUM_INSERTS_WITHOUT_DRAINING, false);

        final StructureSnapshot before = GuardiannStructureAsserts.snapshotStructure(db, guardiann);
        final ClusterView target = Objects.requireNonNull(before).clusters().values().stream()
                .max(Comparator.comparingInt(cv -> cv.primaries().size()))
                .orElseThrow();
        assertThat(target.primaries().size())
                .as("the pending split's target must still be oversized, or the task dismisses itself")
                .isGreaterThan(CLUSTER_MAX);

        // Orphan a few of that cluster's own members, so the references that go stale are certain to belong to the
        // cluster about to be repartitioned.
        final List<VectorId> toOrphan = target.primaries().stream()
                .sorted(Comparator.comparing(id -> id.primaryKey().toString()))
                .limit(NUM_REFERENCES_TO_ORPHAN)
                .collect(ImmutableList.toImmutableList());
        assertThat(toOrphan).hasSize(NUM_REFERENCES_TO_ORPHAN);

        final Primitives primitives = guardiann.getLocator().primitives();
        db.run(tr -> {
            for (final VectorId vectorId : toOrphan) {
                primitives.deleteVectorMetadata(tr, vectorId.primaryKey());
            }
            return null;
        });

        onWriteListener.pushFrame();
        try {
            GuardiannStructureAsserts.runToQuiescence(db, guardiann);

            assertThat(onWriteListener.getNumVectorReferenceCleanups())
                    .as("repartitioning must report every reconciliation, so the drift below has a denominator")
                    .isPositive();
            assertThat(onWriteListener.getNumStaleVectorReferencesDropped())
                    .as("the %d references whose vectors were removed must be reported as discarded",
                            NUM_REFERENCES_TO_ORPHAN)
                    .isGreaterThanOrEqualTo(NUM_REFERENCES_TO_ORPHAN);
        } finally {
            onWriteListener.popFrame();
        }
    }

    @Test
    void oversizedClusterTriggersSplit() throws Exception {
        onWriteListener.pushFrame();
        try {
            insertNearDuplicateCloud(NUM_NEAR_DUPLICATES, true);

            GuardiannStructureAsserts.runToQuiescence(db, guardiann);

            final Map<TaskKind, Integer> executed =
                    onWriteListener.getNumTasksExecutedByKind();
            logger.info("scenario complete; tasks executed by kind={}", executed);

            assertThat(executed.getOrDefault(TaskKind.SPLIT_MERGE, 0))
                    .as("at least one SplitMergeTask must fire when a cluster exceeds primaryClusterMax=%d",
                            CLUSTER_MAX)
                    .isGreaterThanOrEqualTo(1);

            GuardiannStructureAsserts.assertGuardiannInvariants(db, guardiann);

            final StructureSnapshot snap = GuardiannStructureAsserts.snapshotStructure(db, guardiann);
            assertThat(snap)
                    .as("structure snapshot must be non-null after inserts")
                    .isNotNull();
            assertThat(snap.numClusters())
                    .as("structure must contain more than one cluster after the split")
                    .isGreaterThanOrEqualTo(2);
            assertThat(snap.totalPrimaries())
                    .as("every inserted vector must remain accounted for as a primary")
                    .isEqualTo(NUM_NEAR_DUPLICATES);
        } finally {
            onWriteListener.popFrame();
        }
    }

    /**
     * Inserts {@code count} perturbed copies of a single SIFT vector, each with a unique primary key.
     * Distinct perturbations mean distinct content signatures, so {@link CollapseTask} does not try to fold them into a
     * collapsed cluster.
     */
    private void insertNearDuplicateCloud(final int count, final boolean maintainInTransaction) throws Exception {
        // Read just the first SIFT-small vector as the "center" of the near-duplicate cloud.
        // Stream a single entry via loadVectors rather than slurping all 10k via loadSiftSmall().
        final List<PrimaryKeyAndVector> baseLoaded =
                VecsDatasetLoaders.loadVectors(SiftTestHelpers.SIFT_SMALL_BASE_PATH, 1);
        Verify.verify(!baseLoaded.isEmpty(), "SIFT-small must contain at least one vector");
        final DoubleRealVector base = (DoubleRealVector) baseLoaded.get(0).vector();

        final SplittableRandom rnd = new SplittableRandom(PERTURBATION_SEED);
        final RandomHelpers.GaussianSampler sampler = new RandomHelpers.GaussianSampler(rnd);

        for (int i = 0; i < count; i++) {
            final DoubleRealVector perturbed = CommonTestHelpers.perturb(base, sampler, PERTURBATION_SIGMA);
            final Tuple pk = CommonTestHelpers.createPrimaryKey(i);
            db.run(tr -> {
                guardiann.insert(tr, pk, perturbed, null, maintainInTransaction).join();
                return null;
            });
        }
    }

}
