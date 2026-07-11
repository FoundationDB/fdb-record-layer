/*
 * SplitMergeSplitScenarioTest.java
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
import com.apple.foundationdb.test.TestClassSubspaceExtension;
import com.apple.foundationdb.test.TestDatabaseExtension;
import com.apple.foundationdb.test.TestExecutors;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.base.Verify;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.nio.file.Path;
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
public class SplitMergeSplitScenarioTest implements BaseTest {
    private static final Logger logger = LoggerFactory.getLogger(SplitMergeSplitScenarioTest.class);

    /** Cluster size cap. Picked small so the trigger fires after a handful of inserts. */
    private static final int CLUSTER_MAX = 50;

    /** Number of near-duplicates to insert. Must be {@code > CLUSTER_MAX} so a split is forced. */
    private static final int NUM_NEAR_DUPLICATES = 100;

    /** Per-component Gaussian sigma for the perturbation. Small relative to SIFT's ~[0, 200] range. */
    private static final double PERTURBATION_SIGMA = 0.5d;

    /** Deterministic seed for the perturbation noise. */
    private static final long PERTURBATION_SEED = 0x5C12_AB17_BEEFL;

    @RegisterExtension
    static final TestDatabaseExtension dbExtension = new TestDatabaseExtension();
    @RegisterExtension
    static final TestClassSubspaceExtension subspaceExtension = new TestClassSubspaceExtension(dbExtension);

    @TempDir
    Path tempDir;

    private static Database db;
    private static Guardiann guardiann;
    private static TestHelpers.TestOnWriteListener onWriteListener;

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

        onWriteListener = new TestHelpers.TestOnWriteListener();
        final TestHelpers.TestOnReadListener onReadListener = new TestHelpers.TestOnReadListener();

        final Config config = Guardiann.newConfigBuilder()
                .setUseRaBitQ(true)
                .setRaBitQNumExBits(6)
                .setMetric(Metric.EUCLIDEAN_METRIC)
                .setPrimaryClusterMax(CLUSTER_MAX)
                .setPrimaryClusterMin(10)
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

    @Test
    void oversizedClusterTriggersSplit() throws Exception {
        // Read just the first SIFT-small vector as the "center" of the near-duplicate cloud.
        // Stream a single entry via loadVectors rather than slurping all 10k via loadSiftSmall().
        final List<PrimaryKeyAndVector> baseLoaded =
                TestHelpers.loadVectors(SiftTestHelpers.SIFT_SMALL_BASE_PATH, 1);
        Verify.verify(!baseLoaded.isEmpty(), "SIFT-small must contain at least one vector");
        final DoubleRealVector base = (DoubleRealVector) baseLoaded.get(0).vector();

        // Insert NUM_NEAR_DUPLICATES perturbed copies, each with a unique primary key. Distinct
        // perturbations mean distinct content signatures, so CollapseTask doesn't try to fold
        // them into a collapsed cluster.
        final SplittableRandom rnd = new SplittableRandom(PERTURBATION_SEED);
        final RandomHelpers.GaussianSampler sampler = new RandomHelpers.GaussianSampler(rnd);

        onWriteListener.pushFrame();
        try {
            for (int i = 0; i < NUM_NEAR_DUPLICATES; i++) {
                final DoubleRealVector perturbed = CommonTestHelpers.perturb(base, sampler, PERTURBATION_SIGMA);
                final Tuple pk = CommonTestHelpers.createPrimaryKey(i);
                db.run(tr -> {
                    guardiann.insert(tr, pk, perturbed, null).join();
                    return null;
                });
            }

            TestHelpers.runToQuiescence(db, guardiann);

            final Map<TaskKind, Integer> executed =
                    onWriteListener.getNumTasksExecutedByKind();
            logger.info("scenario complete; tasks executed by kind={}", executed);

            assertThat(executed.getOrDefault(TaskKind.SPLIT_MERGE, 0))
                    .as("at least one SplitMergeTask must fire when a cluster exceeds primaryClusterMax=%d",
                            CLUSTER_MAX)
                    .isGreaterThanOrEqualTo(1);

            TestHelpers.assertGuardiannInvariants(db, guardiann);

            final StructureSnapshot snap = TestHelpers.snapshotStructure(db, guardiann);
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

}
