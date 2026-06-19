/*
 * MergeScenarioTest.java
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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.SplittableRandom;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Scenario: forces a cluster to drop below {@code primaryClusterMin}, which must trigger a merge.
 * <p>
 * First inserts more near-duplicates than fit in one cluster (as in
 * {@link SplitMergeSplitScenarioTest}) so the structure splits into at least two clusters. Then it
 * deletes most of them, dropping the surviving clusters below {@code primaryClusterMin}; per
 * {@code Primitives}, a delete that takes a cluster under the minimum enqueues a
 * {@link SplitMergeTask} that takes the <em>merge</em> branch. The default 2→1 merge runs k-means
 * with {@code k == 1}, so this exercises the {@code KMeans.fit} k=1 path end-to-end — the path
 * that otherwise only runs in the multi-hour SIFT-1M delete workloads in {@code SiftTest}.
 */
public class MergeScenarioTest implements BaseTest {
    private static final Logger logger = LoggerFactory.getLogger(MergeScenarioTest.class);

    /** Cluster size cap. Small so a handful of inserts forces a split into multiple clusters. */
    private static final int CLUSTER_MAX = 50;

    /** Minimum primaries per cluster; dropping below this is what triggers a merge. */
    private static final int PRIMARY_CLUSTER_MIN = 10;

    /** Near-duplicates to insert. Must be {@code > CLUSTER_MAX} so a split is forced first. */
    private static final int NUM_NEAR_DUPLICATES = 100;

    /**
     * Survivors left after the delete phase. Chosen well below {@code 2 * PRIMARY_CLUSTER_MIN} so
     * the surviving clusters fall under {@code primaryClusterMin} and a merge is forced.
     */
    private static final int REMAINING_AFTER_DELETE = 12;

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
                .setPrimaryClusterMin(PRIMARY_CLUSTER_MIN)
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
    void underpopulatedClusterTriggersMerge() throws Exception {
        final List<PrimaryKeyAndVector> baseLoaded =
                TestHelpers.loadVectors(SiftTestHelpers.SIFT_SMALL_BASE_PATH, 1);
        Verify.verify(!baseLoaded.isEmpty(), "SIFT-small must contain at least one vector");
        final DoubleRealVector base = (DoubleRealVector) baseLoaded.get(0).getVector();

        final SplittableRandom rnd = new SplittableRandom(PERTURBATION_SEED);
        final RandomHelpers.GaussianSampler sampler = new RandomHelpers.GaussianSampler(rnd);

        // ---- Phase 1: insert near-duplicates and let the oversized cluster split. ----
        final List<PrimaryKeyAndVector> inserted = new ArrayList<>(NUM_NEAR_DUPLICATES);
        for (int i = 0; i < NUM_NEAR_DUPLICATES; i++) {
            final DoubleRealVector perturbed = perturb(base, sampler, PERTURBATION_SIGMA);
            final Tuple pk = CommonTestHelpers.createPrimaryKey(i);
            db.run(tr -> {
                guardiann.insert(tr, pk, perturbed, null).join();
                return null;
            });
            inserted.add(new PrimaryKeyAndVector(pk, perturbed));
        }
        TestHelpers.runToQuiescence(db, guardiann);

        final StructureSnapshot afterInsert = TestHelpers.snapshotStructure(db, guardiann);
        assertThat(afterInsert)
                .as("structure snapshot must be non-null after inserts")
                .isNotNull();
        assertThat(afterInsert.numClusters())
                .as("inserting %d near-duplicates with primaryClusterMax=%d must split into >= 2 clusters "
                        + "(precondition for a merge)", NUM_NEAR_DUPLICATES, CLUSTER_MAX)
                .isGreaterThanOrEqualTo(2);

        // ---- Phase 2: delete most records so the surviving clusters fall below primaryClusterMin. ----
        onWriteListener.pushFrame();
        try {
            deleteRecords(inserted.subList(0, NUM_NEAR_DUPLICATES - REMAINING_AFTER_DELETE));
            TestHelpers.runToQuiescence(db, guardiann);

            final Map<AbstractDeferredTask.Kind, Integer> deletePhaseTasks =
                    onWriteListener.getNumTasksExecutedByKind();
            logger.info("delete-phase tasks by kind={}", deletePhaseTasks);

            // A delete-driven SplitMergeTask can only be a merge (deletes never split). Its
            // execution means the merge -> k-means(k=1) path ran without throwing — which, before
            // KMeans.fit supported k=1, it could not.
            assertThat(deletePhaseTasks.getOrDefault(AbstractDeferredTask.Kind.SPLIT_MERGE, 0))
                    .as("dropping a cluster below primaryClusterMin=%d must fire a (merge) SplitMergeTask",
                            PRIMARY_CLUSTER_MIN)
                    .isGreaterThanOrEqualTo(1);
        } finally {
            onWriteListener.popFrame();
        }

        // Deletes can leave dangling replicas, so use the after-deletes invariant variant.
        TestHelpers.assertGuardiannInvariantsAfterDeletes(db, guardiann);

        final StructureSnapshot afterDelete = TestHelpers.snapshotStructure(db, guardiann);
        assertThat(afterDelete)
                .as("structure snapshot must be non-null after deletes")
                .isNotNull();
        assertThat(afterDelete.totalPrimaries())
                .as("every surviving vector must remain accounted for as a primary")
                .isEqualTo(REMAINING_AFTER_DELETE);
    }

    private void deleteRecords(@Nonnull final List<PrimaryKeyAndVector> records) throws Exception {
        final List<PrimaryKeyAndVector> remaining = new ArrayList<>(records);
        while (!remaining.isEmpty()) {
            final List<PrimaryKeyAndVector> done = TestHelpers.basicDeleteBatch(getDb(), guardiann, remaining);
            remaining.subList(0, done.size()).clear();
        }
    }

    @Nonnull
    private static DoubleRealVector perturb(@Nonnull final DoubleRealVector base,
                                            @Nonnull final RandomHelpers.GaussianSampler sampler,
                                            final double sigma) {
        final double[] data = base.getData().clone();
        for (int i = 0; i < data.length; i++) {
            data[i] += sigma * sampler.nextGaussian();
        }
        return new DoubleRealVector(data);
    }
}
