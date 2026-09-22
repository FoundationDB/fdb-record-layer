/*
 * GuardiannStructureAsserts.java
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
import com.apple.foundationdb.async.common.ResultEntry;
import com.apple.foundationdb.async.hnsw.HNSW;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Structural-invariant assertions for a {@link Guardiann}: drain the deferred-task queue to quiescence, then verify
 * the cluster topology is internally consistent (every primary unique and accounted for, no dangling replicas, and
 * the soft replication invariants within tolerance). Lives in {@code testFixtures} so both the fdb-extensions
 * guardiann tests and the record-layer's vector-index tests can run the same corruption checks against a shared
 * source of truth. The public {@code assertGuardiannInvariants*} umbrellas are the intended entry points; the
 * individual quiescence/snapshot/invariant helpers are exposed package-privately for tests that want finer control.
 */
public class GuardiannStructureAsserts {
    private static final Logger logger = LoggerFactory.getLogger(GuardiannStructureAsserts.class);

    private GuardiannStructureAsserts() {
    }

    /**
     * Safety bound for {@link #runToQuiescence}. A healthy drain finishes well under this; if it
     * doesn't, the structure is producing tasks faster than they get retired and something is
     * wrong.
     */
    private static final int MAX_DRAIN_ITERATIONS = 1_000;

    /**
     * Drains all pending deferred tasks from {@code guardiann} by repeatedly fetching one task
     * and executing it in its own transaction until the tasks subspace is empty.
     * <p>
     * Insertions and deletions piggy-back deferred-task execution onto themselves (one task per
     * op, via {@link Primitives#executeDeferredTasks}). Once those producer ops stop, pending
     * tasks remain in the tasks subspace until something pulls them out — this method is that
     * something. Tests typically call it before checking post-condition invariants so the
     * structure is observed at a quiescent state.
     * <p>
     * Note that {@link BounceTask} is a state machine: each invocation executes one of its
     * dependent tasks and then either re-enqueues a new BounceTask with the remaining
     * dependents OR fires the final task once the last dependent has run. Draining therefore
     * takes roughly N+1 iterations per BounceTask (N dependents + the final task), and
     * additional tasks (e.g. a split firing a follow-up) may be enqueued mid-drain. The loop
     * here simply keeps going until the tasks subspace reads empty.
     *
     * @return the number of tasks executed during the drain
     */
    static int runToQuiescence(@Nonnull final Database db, @Nonnull final Guardiann guardiann) {
        final Primitives primitives = guardiann.getLocator().primitives();
        int executed = 0;
        for (int i = 0; i < MAX_DRAIN_ITERATIONS; i++) {
            final boolean didWork = db.run(transaction -> {
                final AccessInfo accessInfo = primitives.fetchAccessInfo(transaction).join();
                if (accessInfo == null) {
                    return false;
                }
                final List<AbstractDeferredTask> pending =
                        primitives.fetchSomeDeferredTasks(transaction, accessInfo, 1).join();
                if (pending.isEmpty()) {
                    return false;
                }
                primitives.executeSingleDeferredTask(transaction, pending.get(0)).join();
                return true;
            });
            if (!didWork) {
                if (executed > 0) {
                    logger.info("runToQuiescence drained {} tasks", executed);
                }
                return executed;
            }
            executed++;
        }
        throw new IllegalStateException("runToQuiescence did not converge after "
                + MAX_DRAIN_ITERATIONS + " iterations; possible task-loop bug");
    }

    /**
     * Asserts that the tasks subspace is empty — i.e. {@link #runToQuiescence} would execute
     * zero tasks if called now. Tests typically call {@link #runToQuiescence} first, then this,
     * to confirm the drain truly settled.
     */
    static void assertQuiescence(@Nonnull final Database db, @Nonnull final Guardiann guardiann) {
        final Primitives primitives = guardiann.getLocator().primitives();
        final int pending = db.run(transaction -> {
            final AccessInfo accessInfo = primitives.fetchAccessInfo(transaction).join();
            if (accessInfo == null) {
                return 0;
            }
            return primitives.fetchSomeDeferredTasks(transaction, accessInfo, 1).join().size();
        });
        assertThat(pending).as("deferred tasks remaining at quiescence check").isEqualTo(0);
    }

    /**
     * Snapshots the current Guardiann cluster topology via {@link Search#snapshotStructure}. Returns
     * {@code null} if the structure is empty (no clusters yet — common before the first insert).
     * <p>
     * Implementation: scan the centroid HNSW for cluster ids and centroids (this uses its own internal
     * transactions), then in one follow-up read transaction delegate to {@link Search#snapshotStructure}, which
     * fetches every cluster's metadata and vector references and assembles the snapshot.
     */
    @Nullable
    static StructureSnapshot snapshotStructure(@Nonnull final Database db,
                                               @Nonnull final Guardiann guardiann) {
        final Primitives primitives = guardiann.getLocator().primitives();
        final HNSW centroidsHnsw = primitives.getClusterCentroidsHnsw();

        // Pull the (clusterId, centroid) entries from the centroid HNSW (this uses its own internal transactions).
        final List<ResultEntry> centroidEntries = Lists.newArrayList();
        HNSW.scanLayer(centroidsHnsw.getConfig(), centroidsHnsw.getSubspace(), db, 0, 100,
                centroidEntries::add);
        if (centroidEntries.isEmpty()) {
            return null;
        }
        return db.run(transaction ->
                guardiann.getLocator().search().snapshotStructure(transaction, centroidEntries,
                        new SearchConfig.SearchConfigBuilder().build()).join());
    }

    /**
     * Asserts that every primary {@link VectorId} appears in exactly one cluster's primary set —
     * no orphans, no duplicates across clusters. The construction of
     * {@link StructureSnapshot#primaryOwners()} performs this check internally via
     * {@link com.google.common.base.Verify}; this method exposes it as a named assertion for tests that want to opt in
     * explicitly. Empty snapshots (no clusters) trivially pass.
     */
    static void assertEveryPrimaryUniqueAndAccountedFor(@Nullable final StructureSnapshot snapshot) {
        if (snapshot == null) {
            return;
        }
        // primaryOwners() rebuilds the reverse map; the build asserts uniqueness via Verify.
        final Map<VectorId, UUID> owners = snapshot.primaryOwners();
        assertThat(owners.size())
                .as("primaryOwners size must match total primary count")
                .isEqualTo(snapshot.totalPrimaries());
    }

    /**
     * Asserts that every replica {@link VectorId} in any cluster has a corresponding primary copy
     * somewhere in the structure. A dangling replica (no live primary with the same VectorId)
     * indicates the primary was deleted but its replicas weren't reaped, or that a replicate-only
     * insert path slipped through.
     */
    static void assertReplicasReferenceLivePrimaries(@Nullable final StructureSnapshot snapshot) {
        if (snapshot == null) {
            return;
        }
        final Set<VectorId> livePrimaries = snapshot.primaryOwners().keySet();
        for (final ClusterView cv : snapshot.clusters().values()) {
            assertThat(cv.replicas())
                    .as("every replica in cluster %s must reference a live primary", cv.clusterId())
                    .allMatch(livePrimaries::contains, "references a live primary");
        }
    }

    /**
     * Tolerances (as fractions of the relevant vector population) and the deep-check size gate for the soft
     * replication invariants. The deep, per-vector check (invariant 3) runs only when the structure has at most
     * {@link #deepCheckMaxVectors} primaries, so large runs stay fast.
     *
     * @param maxUnderReplicatedFraction max fraction of primaries that may be flagged under-replicated (inv 1)
     * @param minReplicatedFraction min replicas, as a fraction of primaries, the structure must carry — a coarse
     *        floor that only catches globally-broken replication (inv 2). Defaults to {@code 0} (no requirement),
     *        since well-separated clusters legitimately carry no replicas; tests that know their data is bordered
     *        opt in via {@link #withMinReplicatedFraction}
     * @param maxWrongPrimaryFraction max fraction of primaries whose home is not their nearest cluster (inv 3)
     * @param maxSpuriousReplicaFraction max fraction of replicas whose stored replication priority is below the
     *        threshold (inv 4)
     * @param deepCheckMaxVectors run the deep check (inv 3) only at or below this many primaries
     */
    public record ReplicationInvariants(double maxUnderReplicatedFraction,
                                        double minReplicatedFraction,
                                        double maxWrongPrimaryFraction,
                                        double maxSpuriousReplicaFraction,
                                        int deepCheckMaxVectors) {
        /** Defaults for a quiesced structure that has not just had a delete storm. */
        @Nonnull
        public static ReplicationInvariants standard() {
            return new ReplicationInvariants(0.05d, 0.0d, 0.08d, 0.02d, 50_000);
        }

        /** Looser bounds for the post-delete state, where reassign may not yet have restored replication. */
        @Nonnull
        public static ReplicationInvariants afterDeletes() {
            return new ReplicationInvariants(0.20d, 0.0d, 0.08d, 0.10d, 50_000);
        }

        /**
         * Returns a copy with a positive {@link #minReplicatedFraction}, for tests whose inserted data is dense
         * enough that border replicas are expected (so a near-zero replica count signals broken replication).
         *
         * @param newMinReplicatedFraction the minimum replica fraction to require
         *
         * @return a copy of these invariants with the given replica floor
         */
        @Nonnull
        public ReplicationInvariants withMinReplicatedFraction(final double newMinReplicatedFraction) {
            return new ReplicationInvariants(maxUnderReplicatedFraction, newMinReplicatedFraction,
                    maxWrongPrimaryFraction, maxSpuriousReplicaFraction, deepCheckMaxVectors);
        }
    }

    /**
     * Soft invariant 1 — not an excessive number of under-replicated primary vectors. Counts primaries flagged
     * {@code isUnderreplicated} across all clusters (the record precondition guarantees only primaries carry that
     * flag) and asserts they are at most {@code maxFraction} of all primaries.
     */
    static void assertUnderReplicatedPrimariesBounded(@Nullable final StructureSnapshot snapshot,
                                                      final double maxFraction) {
        if (snapshot == null) {
            return;
        }
        int totalPrimaries = 0;
        int underReplicated = 0;
        for (final ClusterView cv : snapshot.clusters().values()) {
            for (final VectorReference ref : cv.references()) {
                if (ref.isPrimaryCopy()) {
                    totalPrimaries++;
                    if (ref.isUnderreplicated()) {
                        underReplicated++;
                    }
                }
            }
        }
        if (totalPrimaries == 0) {
            return;
        }
        final double fraction = (double) underReplicated / totalPrimaries;
        logger.info("invariant[under-replicated]: {}/{} primaries under-replicated (fraction={}, limit={})",
                underReplicated, totalPrimaries, fraction, maxFraction);
        assertThat(fraction)
                .as("under-replicated primaries (%d of %d) exceed the allowed fraction", underReplicated,
                        totalPrimaries)
                .isLessThanOrEqualTo(maxFraction);
    }

    /**
     * Soft invariant 4 — not an excessive number of vectors replicated that should not have been. Every replica is
     * created only because its replication priority reached {@link Config#replicationPriorityMin()}, so each
     * replica's <em>stored</em> priority should still be at least that threshold. Counts replicas below it and
     * asserts they are at most {@code maxFraction} of all replicas.
     */
    static void assertNoSpuriousReplicas(@Nullable final StructureSnapshot snapshot,
                                         @Nonnull final Config config,
                                         final double maxFraction) {
        if (snapshot == null) {
            return;
        }
        final double minPriority = config.replicationPriorityMin();
        int totalReplicas = 0;
        int spurious = 0;
        for (final ClusterView cv : snapshot.clusters().values()) {
            for (final VectorReference ref : cv.references()) {
                if (!ref.isPrimaryCopy() && !ref.isCollapsed()) {
                    totalReplicas++;
                    if (ref.replicationPriority() < minPriority) {
                        spurious++;
                    }
                }
            }
        }
        if (totalReplicas == 0) {
            return;
        }
        final double fraction = (double) spurious / totalReplicas;
        logger.info("invariant[spurious-replicas]: {}/{} replicas below replicationPriorityMin={} "
                        + "(fraction={}, limit={})", spurious, totalReplicas, minPriority, fraction, maxFraction);
        assertThat(fraction)
                .as("replicas stored below replicationPriorityMin=%s (%d of %d) exceed the allowed fraction",
                        minPriority, spurious, totalReplicas)
                .isLessThanOrEqualTo(maxFraction);
    }

    /**
     * Soft invariant 2 — not too few replicated vectors. A coarse, snapshot-only guard against globally-broken
     * replication: asserts the structure carries at least {@code minReplicatedFraction} of its primaries as
     * replicas. A fraction of {@code 0} (the default for {@link ReplicationInvariants#standard()} and
     * {@link ReplicationInvariants#afterDeletes()}) disables the check, because well-separated clusters legitimately
     * carry no replicas; tests whose inserted data is bordered opt in via
     * {@link ReplicationInvariants#withMinReplicatedFraction}. This is intentionally <em>not</em> a
     * fraction-of-demand check: replication is a bounded top-K-by-score per cluster (capped at
     * {@link Config#replicatedClusterTarget()}) plus occlusion, so the actual replica count sits far below a naive
     * "every pair above the priority threshold" demand and can't be reproduced cheaply post-hoc.
     */
    static void assertReplicasNotTooFew(@Nullable final StructureSnapshot snapshot,
                                        final double minReplicatedFraction) {
        if (snapshot == null) {
            return;
        }
        final int totalPrimaries = snapshot.totalPrimaries();
        if (totalPrimaries == 0) {
            return;
        }
        final int totalReplicas = snapshot.totalReplicas();
        final double fraction = (double) totalReplicas / totalPrimaries;
        // Always log the observed fraction (even at floor 0, where the check is disabled) so a not-yet-calibrated
        // test can be tuned from the logged value.
        logger.info("invariant[too-few-replicas]: {} replicas / {} primaries (fraction={}, floor={})",
                totalReplicas, totalPrimaries, fraction, minReplicatedFraction);
        if (minReplicatedFraction <= 0.0d) {
            return;
        }
        assertThat(fraction)
                .as("replicas (%d) are too few relative to primaries (%d) — replication looks broken",
                        totalReplicas, totalPrimaries)
                .isGreaterThanOrEqualTo(minReplicatedFraction);
    }

    /**
     * Soft invariant 3 — not an excessive number of vectors with a wrong primary assignment. A primary is
     * "wrong" when its owning cluster is not the cluster whose centroid is nearest to it (see
     * {@link StructureSnapshot#computeAssignmentRanking}). Some misassignment is expected — a vector inserted before
     * a later split, or one near a moving border, can end up off its nearest centroid — so this asserts only that
     * the wrong fraction stays at or below {@code maxWrongFraction}.
     */
    static void assertPrimaryAssignmentsMostlyCorrect(@Nonnull final StructureSnapshot.AssignmentRanking ranking,
                                                      final double maxWrongFraction) {
        if (ranking.numPrimaries() == 0) {
            return;
        }
        final double fraction = (double) ranking.numWrongAssignments() / ranking.numPrimaries();
        logger.info("invariant[wrong-primary]: {}/{} primaries not in their nearest cluster (fraction={}, limit={})",
                ranking.numWrongAssignments(), ranking.numPrimaries(), fraction, maxWrongFraction);
        assertThat(fraction)
                .as("primaries assigned to a non-nearest cluster (%d of %d) exceed the allowed fraction",
                        ranking.numWrongAssignments(), ranking.numPrimaries())
                .isLessThanOrEqualTo(maxWrongFraction);
    }

    /**
     * Umbrella for the four soft replication invariants, all derived from a single {@link #snapshotStructure} fetch.
     * The three cheap, snapshot-only checks always run: invariant 1 (under-replicated primaries), invariant 4
     * (spurious replicas), and invariant 2 (a coarse floor on replica count — a no-op unless the caller opts into a
     * positive {@link ReplicationInvariants#minReplicatedFraction()}). The one deep, per-vector check — invariant 3
     * (wrong primary assignment) — ranks every primary against every cluster via
     * {@link StructureSnapshot#computeAssignmentRanking}, so it runs only when the structure has at most
     * {@link ReplicationInvariants#deepCheckMaxVectors()} primaries; above that it is skipped (logged) so large
     * runs stay fast while still getting the cheap checks.
     *
     * @param guardiann the structure under test, queried only for its {@link Guardiann#getConfig()}
     * @param snapshot a snapshot of {@code guardiann}'s current topology, or {@code null} if the structure is empty
     * @param invariants the tolerances and the deep-check size gate to apply
     */
    static void assertReplicationInvariants(@Nonnull final Guardiann guardiann,
                                            @Nullable final StructureSnapshot snapshot,
                                            @Nonnull final ReplicationInvariants invariants) {
        // Cheap, snapshot-only checks — always run.
        assertUnderReplicatedPrimariesBounded(snapshot, invariants.maxUnderReplicatedFraction());
        assertNoSpuriousReplicas(snapshot, guardiann.getConfig(), invariants.maxSpuriousReplicaFraction());
        assertReplicasNotTooFew(snapshot, invariants.minReplicatedFraction());

        // Deep, per-vector check (inv 3) — gated on structure size so large runs stay fast.
        final int totalPrimaries = snapshot == null ? 0 : snapshot.totalPrimaries();
        if (totalPrimaries > invariants.deepCheckMaxVectors()) {
            logger.info("invariant[deep-checks]: skipped — {} primaries exceeds deepCheckMaxVectors={}",
                    totalPrimaries, invariants.deepCheckMaxVectors());
            return;
        }
        if (snapshot == null) {
            return;
        }
        assertPrimaryAssignmentsMostlyCorrect(snapshot.computeAssignmentRanking(),
                invariants.maxWrongPrimaryFraction());
    }

    /**
     * Umbrella post-condition check for a test that has performed structural operations
     * <em>without deletes</em> (inserts, repartitions, reassigns, etc.). Runs the deferred-task
     * queue to quiescence, verifies nothing remains pending, then snapshots and validates every
     * structural invariant — <em>including</em> {@link #assertReplicasReferenceLivePrimaries}.
     * <p>
     * Equivalent to the explicit sequence:
     * <pre>{@code
     * runToQuiescence(db, guardiann);
     * assertQuiescence(db, guardiann);
     * final StructureSnapshot s = snapshotStructure(db, guardiann);
     * assertEveryPrimaryUniqueAndAccountedFor(s);
     * assertReplicasReferenceLivePrimaries(s);
     * }</pre>
     * but bundled into one call so scenario tests don't have to repeat the boilerplate.
     * <p>
     * <b>Do not use this after deletes.</b> Deleting a primary can legitimately leave dangling
     * replica vectors behind (replicas are not necessarily reaped), so
     * {@link #assertReplicasReferenceLivePrimaries} would spuriously fail. Delete-involving tests
     * should call {@link #assertGuardiannInvariantsAfterDeletes} instead. Tests that want finer
     * control (e.g. snapshotting before and after for a diff) should call the individual helpers.
     */
    public static void assertGuardiannInvariants(@Nonnull final Database db,
                                                 @Nonnull final Guardiann guardiann) {
        assertGuardiannInvariants(db, guardiann, true, ReplicationInvariants.standard());
    }

    /**
     * As {@link #assertGuardiannInvariants(Database, Guardiann)} (no-deletes), but with caller-supplied replication
     * tolerances — e.g. a dense-data test opting into a positive
     * {@link ReplicationInvariants#minReplicatedFraction()} so that a near-zero replica count is treated as broken
     * replication.
     *
     * @param db the database
     * @param guardiann the structure under test
     * @param invariants the replication tolerances to apply
     */
    public static void assertGuardiannInvariants(@Nonnull final Database db,
                                                 @Nonnull final Guardiann guardiann,
                                                 @Nonnull final ReplicationInvariants invariants) {
        assertGuardiannInvariants(db, guardiann, true, invariants);
    }

    /**
     * Umbrella post-condition check for a test that may have performed deletes. Identical to
     * {@link #assertGuardiannInvariants}, except it omits {@link #assertReplicasReferenceLivePrimaries}:
     * after a delete it is valid for replica vectors to dangle (the primary is gone but its
     * replicas haven't been reaped), so that check does not hold. The quiescence and
     * primary-uniqueness invariants still do, and are still checked.
     */
    public static void assertGuardiannInvariantsAfterDeletes(@Nonnull final Database db,
                                                             @Nonnull final Guardiann guardiann) {
        assertGuardiannInvariants(db, guardiann, false, ReplicationInvariants.afterDeletes());
    }

    /**
     * Shared implementation behind {@link #assertGuardiannInvariants} and
     * {@link #assertGuardiannInvariantsAfterDeletes}. Runs to quiescence, asserts quiescence, then
     * validates the structural invariants on the resulting snapshot. The replica/live-primary
     * check is gated on {@code requireReplicasReferenceLivePrimaries} because it only holds in the
     * absence of deletes.
     *
     * @param db the database
     * @param guardiann the structure under test
     * @param requireReplicasReferenceLivePrimaries whether to additionally assert that every
     *        replica references a live primary (only valid when no deletes were performed)
     * @param invariants the replication tolerances to apply
     */
    private static void assertGuardiannInvariants(@Nonnull final Database db,
                                                  @Nonnull final Guardiann guardiann,
                                                  final boolean requireReplicasReferenceLivePrimaries,
                                                  @Nonnull final ReplicationInvariants invariants) {
        runToQuiescence(db, guardiann);
        assertQuiescence(db, guardiann);
        final StructureSnapshot snapshot = snapshotStructure(db, guardiann);
        assertEveryPrimaryUniqueAndAccountedFor(snapshot);
        if (requireReplicasReferenceLivePrimaries) {
            assertReplicasReferenceLivePrimaries(snapshot);
        }
        // The four soft replication invariants, reusing the snapshot already taken above so the cheap checks add
        // no extra scan.
        assertReplicationInvariants(guardiann, snapshot, invariants);
    }
}
