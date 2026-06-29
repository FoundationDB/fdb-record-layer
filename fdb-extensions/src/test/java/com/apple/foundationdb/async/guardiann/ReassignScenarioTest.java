/*
 * ReassignScenarioTest.java
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
import com.apple.foundationdb.async.AsyncIterator;
import com.apple.foundationdb.async.common.BaseTest;
import com.apple.foundationdb.async.common.PrimaryKeyAndVector;
import com.apple.foundationdb.async.common.RandomHelpers;
import com.apple.foundationdb.async.common.ResultEntry;
import com.apple.foundationdb.async.common.StorageTransform;
import com.apple.foundationdb.linear.Metric;
import com.apple.foundationdb.linear.Quantizer;
import com.apple.foundationdb.linear.RealVector;
import com.apple.foundationdb.linear.Transformed;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.test.TestClassSubspaceExtension;
import com.apple.foundationdb.test.TestDatabaseExtension;
import com.apple.foundationdb.test.TestExecutors;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.collect.ImmutableSet;
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
import java.util.Optional;
import java.util.SplittableRandom;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Phase 2 scenario: forces a {@link ReassignTask} to fire via the
 * {@code underreplicatedPrimaryClusterMax} trigger.
 * <p>
 * That trigger is a safety net the structure normally never reaches on its own through inserts.
 * Regular {@code Guardiann#insert} writes every new primary copy in the non-underreplicated state
 * — only {@link SplitMergeTask} and {@link ReassignTask} can mark a primary as under-replicated,
 * by reassigning it to a different cluster than the one it currently lives in. So exercising the
 * trigger in isolation requires hand-building the precondition:
 * <ol>
 *   <li>warm the structure up with enough varied inserts to grow a multi-cluster topology
 *       (splits and follow-up reassigns may fire during warmup — we don't care, we just need a
 *       stable post-quiescence shape to inject into),</li>
 *   <li>find the cluster a fixed query vector would naturally belong to, using the exact
 *       neighborhood-discovery logic {@code Insert} uses (stable across reseeds — cluster UUIDs
 *   <li>inject {@code underreplicatedPrimaryClusterMax + 1} synthetic under-replicated primary
 *       copies of the query vector into that cluster via {@link Primitives}, then call
 *       {@link Primitives#updateClusterMetadataAndEnqueueSplitOrReassignTaskMaybe} once with the cumulative
 *       delta. That call pushes {@code numPrimaryUnderreplicatedVectors} past the trigger and
 *       enqueues a {@link ReassignTask}.</li>
 * </ol>
 * The drain phase then executes the enqueued REASSIGN and the post-condition asserts that no
 * cluster's under-replicated count still exceeds the trigger.
 */
public class ReassignScenarioTest implements BaseTest {
    private static final Logger logger = LoggerFactory.getLogger(ReassignScenarioTest.class);

    /** Trigger threshold. Small so the post-warmup injection is cheap. */
    private static final int UNDERREPLICATED_MAX = 3;

    /**
     * Cluster size cap. Small enough that the warmup inserts force splits and we end up with
     * several clusters; large enough that adding {@code UNDERREPLICATED_MAX + 1} more primaries
     * to a post-split cluster does NOT also trip the {@code primaryClusterMax} threshold (which
     * would queue a SPLIT_MERGE instead of the REASSIGN we're targeting).
     */
    private static final int PRIMARY_CLUSTER_MAX = 80;

    /**
     * Warmup inserts. Several multiples of {@link #PRIMARY_CLUSTER_MAX} so the index splits a few
     * times and post-quiescence cluster sizes settle well below the cap (leaving headroom for the
     * injection phase).
     */
    private static final int NUM_WARMUP_INSERTS = 250;

    /**
     * Base id for the synthetic primary keys we inject. Picked well above the warmup PK range
     * (warmup uses {@code Tuple.from(0..NUM_WARMUP_INSERTS-1)}) so there's no collision.
     */
    private static final long INJECTION_PK_BASE = 1_000_000L;

    /** Deterministic seed for the random passed into {@code updateClusterMetadataAndEnqueueSplitOrReassignTaskMaybe}. */
    private static final long INJECTION_RANDOM_SEED = 0xC0FFEE_BABEL;

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
                .setPrimaryClusterMax(PRIMARY_CLUSTER_MAX)
                .setPrimaryClusterMin(1)
                .setDeterministicRandomness(true)
                .setUnderreplicatedPrimaryClusterMax(UNDERREPLICATED_MAX)
                // Keep the OTHER reassign trigger out of the way so the REASSIGN we observe is
                // unambiguously from the under-replicated count crossing the threshold.
                .setReplicatedClusterMaxWrites(10_000)
                .setReplicationPriorityMin(0.65d)
                .setReplicatedClusterTarget(5)
                .build(128);

        guardiann = new Guardiann(subspaceExtension.getSubspace(),
                TestExecutors.defaultThreadPool(),
                config,
                onWriteListener,
                onReadListener);
    }

    @Test
    void underreplicatedExcessTriggersReassign() throws Exception {
        // ---- Phase 1: warmup ----
        // Grow the structure past one cluster. Splits (and any follow-up reassigns) fire during
        // warmup; we don't track those — the only post-condition that matters here is a
        // quiescent multi-cluster shape to inject into.
        final List<PrimaryKeyAndVector> warmup =
                TestHelpers.loadVectors(SiftTestHelpers.SIFT_SMALL_BASE_PATH, NUM_WARMUP_INSERTS);
        for (final PrimaryKeyAndVector record : warmup) {
            db.run(tr -> {
                guardiann.insert(tr, record.getPrimaryKey(), record.getVector(), null).join();
                return null;
            });
        }
        TestHelpers.runToQuiescence(db, guardiann);
        TestHelpers.assertGuardiannInvariants(db, guardiann);

        // Fixed query vector for injection — first SIFT-small vector (already inserted as part
        // of warmup; we only need its coordinates here, not its primary key).
        final RealVector fixedVector = warmup.get(0).getVector();
        final int extra = UNDERREPLICATED_MAX + 1;

        // ---- Phase 2: inject under-replicated primaries directly via Primitives ----
        // Push a frame so the assertion below counts ONLY tasks executed after the injection;
        // warmup task counts (which include incidental REASSIGNs from the split cascade) are
        // accumulated on the outer (absent) frame and don't pollute this count.
        onWriteListener.pushFrame();
        try {
            db.run(tr -> {
                final Primitives primitives = guardiann.getLocator().primitives();
                final AccessInfo accessInfo =
                        Objects.requireNonNull(primitives.fetchAccessInfo(tr).join(),
                                "AccessInfo must be present after warmup");
                final StorageTransform storageTransform = primitives.storageTransform(accessInfo);
                final Quantizer quantizer = primitives.quantizer(accessInfo);
                final Transformed<RealVector> transformedFixedVector = storageTransform.transform(fixedVector);

                // Same neighborhood-discovery logic Insert uses: ask the centroid HNSW for the
                // nearest cluster to the raw (untransformed) query vector.
                final AsyncIterator<ResultEntry> centroidIter =
                        primitives.centroidsOrderedByDistance(tr, fixedVector, 0.0d, null);
                if (!centroidIter.onHasNext().join()) {
                    throw new IllegalStateException("expected at least one cluster after warmup");
                }
                final ResultEntry nearest = centroidIter.next();
                final UUID targetClusterId = StorageAdapter.clusterIdFromTuple(nearest.primaryKey());
                final Transformed<RealVector> targetCentroid =
                        storageTransform.transform(Objects.requireNonNull(nearest.vector()));
                final double distanceToTargetCentroid = nearest.distance();
                final ClusterMetadata targetMeta =
                        Objects.requireNonNull(primitives.fetchClusterMetadata(tr, targetClusterId).join());

                // The trigger we're targeting (under-replicated > max) is checked AFTER the split
                // trigger (total primaries > max) — if we tip the chosen cluster past
                // primaryClusterMax, we'd get SPLIT_MERGE instead. PRIMARY_CLUSTER_MAX and the
                // warmup load are tuned to leave plenty of headroom; this assertion just makes
                // the precondition explicit so a constants drift fails loudly.
                assertThat(targetMeta.getNumPrimaryVectors() + extra)
                        .as("chosen cluster %s has %d primaries; adding %d more would trip "
                                + "primaryClusterMax=%d and queue SPLIT_MERGE instead of REASSIGN",
                                targetClusterId, targetMeta.getNumPrimaryVectors(), extra, PRIMARY_CLUSTER_MAX)
                        .isLessThanOrEqualTo(PRIMARY_CLUSTER_MAX);

                logger.info("injecting {} under-replicated primaries into cluster {} (currently {} primaries, {} under-replicated)",
                        extra, targetClusterId,
                        targetMeta.getNumPrimaryVectors(), targetMeta.numPrimaryUnderreplicatedVectors());

                RunningStats updatedStandardDeviation = targetMeta.runningStandardDeviation();
                final SplittableRandom rnd = new SplittableRandom(INJECTION_RANDOM_SEED);
                for (int i = 0; i < extra; i++) {
                    final Tuple syntheticPk = Tuple.from(INJECTION_PK_BASE + i);
                    final UUID syntheticUuid = RandomHelpers.randomUuid(rnd, true);
                    final VectorMetadata vm = new VectorMetadata(syntheticPk, syntheticUuid, null);
                    primitives.writeVectorMetadata(tr, vm);
                    final VectorReference vr = VectorReference.primaryCopy(vm.vectorId(), transformedFixedVector,
                            true, false);
                    primitives.writeVectorReference(tr, quantizer, targetClusterId, vr);
                    updatedStandardDeviation = updatedStandardDeviation.add(distanceToTargetCentroid);
                }

                final Optional<UUID> enqueued = primitives.updateClusterMetadataAndEnqueueSplitOrReassignTaskMaybe(
                        tr, rnd, targetMeta, targetCentroid, accessInfo,
                        extra, extra, 0, updatedStandardDeviation, ImmutableSet.of());
                assertThat(enqueued)
                        .as("metadata update with %d new under-replicated primaries must enqueue "
                                + "a REASSIGN task (underreplicatedPrimaryClusterMax=%d)",
                                extra, UNDERREPLICATED_MAX)
                        .isPresent();
                return null;
            });

            // ---- Phase 3: drain & verify ----
            TestHelpers.runToQuiescence(db, guardiann);

            final Map<AbstractDeferredTask.Kind, Integer> executed =
                    onWriteListener.getNumTasksExecutedByKind();
            logger.info("post-injection: tasks executed by kind={}", executed);

            assertThat(executed.getOrDefault(AbstractDeferredTask.Kind.REASSIGN, 0))
                    .as("REASSIGN must fire after the cluster's under-replicated primary count "
                            + "exceeds underreplicatedPrimaryClusterMax=%d", UNDERREPLICATED_MAX)
                    .isGreaterThanOrEqualTo(1);

            TestHelpers.assertGuardiannInvariants(db, guardiann);

            final StructureSnapshot snap = TestHelpers.snapshotStructure(db, guardiann);
            assertThat(snap)
                    .as("structure snapshot must be non-null after warmup + injection")
                    .isNotNull();
            for (final ClusterView cv : snap.clusters().values()) {
                assertThat(cv.metadata().numPrimaryUnderreplicatedVectors())
                        .as("post-reassign: cluster %s under-replicated count must not exceed %d "
                                + "(otherwise REASSIGN would re-enqueue itself)",
                                cv.clusterId(), UNDERREPLICATED_MAX)
                        .isLessThanOrEqualTo(UNDERREPLICATED_MAX);
            }
        } finally {
            onWriteListener.popFrame();
        }
    }
}
