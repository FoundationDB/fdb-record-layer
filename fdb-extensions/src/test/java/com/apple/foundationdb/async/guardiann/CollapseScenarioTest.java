/*
 * CollapseScenarioTest.java
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
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.async.common.BaseTest;
import com.apple.foundationdb.async.common.CommonTestHelpers;
import com.apple.foundationdb.async.common.PrimaryKeyAndVector;
import com.apple.foundationdb.async.common.RandomHelpers;
import com.apple.foundationdb.async.common.ResultEntry;
import com.apple.foundationdb.async.common.StorageTransform;
import com.apple.foundationdb.linear.AffineOperator;
import com.apple.foundationdb.linear.DoubleRealVector;
import com.apple.foundationdb.linear.Metric;
import com.apple.foundationdb.linear.Quantizer;
import com.apple.foundationdb.linear.RealVector;
import com.apple.foundationdb.linear.RealVectorTest;
import com.apple.foundationdb.linear.Transformed;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.test.TestDatabaseExtension;
import com.apple.foundationdb.test.TestExecutors;
import com.apple.foundationdb.test.TestSubspaceExtension;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.RandomSeedSource;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.SplittableRandom;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * Scenarios exercising {@link CollapseTask}: a cluster filled with exactly-identical primary vectors (same
 * {@link StorageAdapter#signatureUuid signature}) folds those duplicates into a single stored "collapsed" reference,
 * recording each absorbed id so it stays resolvable at query time.
 * <p>
 * Tests 1–2 drive the natural end-to-end trigger (insert many identical vectors → a SPLIT that can't repartition
 * identical points → {@code AbstractDeferredTask.enqueueCollapseIfNecessary} → COLLAPSE). Tests 3–4 drive a
 * {@link CollapseTask} directly (config with a huge {@code primaryClusterMax} so inserts never auto-collapse), for
 * deterministic control over the no-op guards and the re-collapse aggregation branch.
 * <p>
 * Collapse removes the standalone primary references it folds, so it leaves the (other-cluster) replicas of those
 * vectors dangling — exactly like a delete. These tests therefore assert structure via
 * {@link TestHelpers#assertGuardiannInvariantsAfterDeletes}, which tolerates dangling replicas.
 */
public class CollapseScenarioTest implements BaseTest {
    private static final Logger logger = LoggerFactory.getLogger(CollapseScenarioTest.class);

    @RegisterExtension
    static final TestDatabaseExtension dbExtension = new TestDatabaseExtension();
    @RegisterExtension
    final TestSubspaceExtension subspaceExtension = new TestSubspaceExtension(dbExtension);

    @TempDir
    Path tempDir;

    private static Database db;

    /** Set by {@link #newGuardiann} so each test can read the COLLAPSE task counter. */
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

    // ---------------------------------------------------------------------------------------------------------
    // Tests
    // ---------------------------------------------------------------------------------------------------------

    //
    // Coverage for AbstractDeferredTask's task-id priority helpers is folded in here rather than in a standalone
    // unit test: they underpin the task-queue ordering that every scenario in this package relies on. Priority is
    // encoded in the id's most-significant bit (clear = high, set = normal).
    //

    @ParameterizedTest
    @RandomSeedSource({0x0fdbL, 0x5ca1eL, 123456L, 78910L, 1123581321345589L})
    void isNormalPriorityReadsTheMostSignificantBit(final long randomSeed) {
        assertThat(AbstractDeferredTask.isNormalPriority(new UUID(0x8000000000000000L, 0L))).isTrue();
        assertThat(AbstractDeferredTask.isNormalPriority(new UUID(0xffffffffffffffffL, 7L))).isTrue();
        assertThat(AbstractDeferredTask.isNormalPriority(new UUID(0x7fffffffffffffffL, -1L))).isFalse();
        assertThat(AbstractDeferredTask.isNormalPriority(new UUID(0L, 0L))).isFalse();

        // The id generators must agree with the predicate, both deterministically and (over several draws) not.
        final SplittableRandom random = new SplittableRandom(randomSeed);
        for (int i = 0; i < 8; i++) {
            assertThat(AbstractDeferredTask.isNormalPriority(
                    AbstractDeferredTask.randomNormalPriorityTaskId(random, true))).isTrue();
            assertThat(AbstractDeferredTask.isNormalPriority(
                    AbstractDeferredTask.randomHighPriorityTaskId(random, true))).isFalse();
            assertThat(AbstractDeferredTask.isNormalPriority(
                    AbstractDeferredTask.randomNormalPriorityTaskId(random, false))).isTrue();
            assertThat(AbstractDeferredTask.isNormalPriority(
                    AbstractDeferredTask.randomHighPriorityTaskId(random, false))).isFalse();
        }
    }

    @ParameterizedTest
    @RandomSeedSource({0x0fdbL, 0x5ca1eL, 123456L, 78910L, 1123581321345589L})
    void taskIdToStringPrefixesWithPriority(final long randomSeed) {
        final UUID highPriority = new UUID(0L, 5L);
        final UUID normalPriority = new UUID(0x8000000000000000L, 5L);

        assertThat(AbstractDeferredTask.taskIdToString(highPriority)).isEqualTo("HIGH:" + highPriority);
        assertThat(AbstractDeferredTask.taskIdToString(normalPriority)).isEqualTo("NORMAL:" + normalPriority);

        final SplittableRandom random = new SplittableRandom(randomSeed);
        assertThat(AbstractDeferredTask.taskIdToString(AbstractDeferredTask.randomNormalPriorityTaskId(random, true)))
                .startsWith("NORMAL:");
        assertThat(AbstractDeferredTask.taskIdToString(AbstractDeferredTask.randomHighPriorityTaskId(random, true)))
                .startsWith("HIGH:");
    }

    /**
     * Exercises the subspace/listener accessors that fan out across the guardiann facades. Each is a pure delegation
     * down to the {@link Locator}/{@link StorageAdapter}, so rather than a class per facade we assert here that every
     * one forwards to the same underlying instance the {@link Guardiann} was built with.
     */
    @Test
    void facadeAccessorsDelegateToTheLocator() {
        final Guardiann guardiann = newGuardiann(100_000, 20);
        final Locator locator = guardiann.getLocator();
        final Subspace root = locator.getSubspace();

        // getSubspace() forwards to the same root across every facade.
        assertThat(guardiann.getSubspace()).isEqualTo(root);
        assertThat(locator.insert().getSubspace()).isEqualTo(root);
        assertThat(locator.delete().getSubspace()).isEqualTo(root);
        assertThat(locator.search().getSubspace()).isEqualTo(root);

        // Listener getters return the shared instances the locator holds.
        assertThat(locator.search().getOnWriteListener()).isSameAs(locator.getOnWriteListener());
        assertThat(locator.search().getOnReadListener()).isSameAs(locator.getOnReadListener());

        // The samples subspace is the root nested under its own prefix (0x06), distinct from the root itself.
        final Subspace samples = locator.primitives().getSamplesSubspace();
        assertThat(ByteArrayUtil.startsWith(samples.pack(), root.pack())).isTrue();
        assertThat(samples.pack()).isNotEqualTo(root.pack());

        // A deferred task forwards its listener down to the same locator instance. Build it purely in memory (no FDB
        // read): AccessInfo's constructor is pure and the centroid is an identity-transformed random vector.
        final RealVector rawCentroid = RealVectorTest.createRandomDoubleVector(new Random(1L), 128);
        final AccessInfo accessInfo = new AccessInfo(0L, rawCentroid);
        final Transformed<RealVector> centroid = AffineOperator.identity().transform(rawCentroid);
        final CollapseTask task =
                CollapseTask.of(locator, accessInfo, new UUID(0L, 1L), UUID.randomUUID(), centroid);
        assertThat(task.getOnReadListener()).isSameAs(locator.getOnReadListener());
    }

    /**
     * Directly exercises the collapsed-vector-id store's full-scan and single-member delete, which back
     * {@link CollapseTask}'s bookkeeping but have no production callers of their own. Writes memberships under two
     * distinct signatures, then checks that {@code scanCollapsedVectorIdsIterable} returns the union across both
     * (unlike the signature-scoped fetch), and that {@code deleteCollapsedVectorId} removes exactly one member.
     */
    @Test
    void collapsedVectorIdStoreScanAndDelete() {
        final Guardiann guardiann = newGuardiann(100_000, 20);
        final Primitives primitives = guardiann.getLocator().primitives();

        final UUID signatureA = new UUID(0xAAAAAAAAAAAAAAAAL, 1L);
        final UUID signatureB = new UUID(0xBBBBBBBBBBBBBBBBL, 2L);
        final VectorId a1 = new VectorId(Tuple.from("a", 1), UUID.randomUUID());
        final VectorId a2 = new VectorId(Tuple.from("a", 2), UUID.randomUUID());
        final VectorId b1 = new VectorId(Tuple.from("b", 1), UUID.randomUUID());

        db.run(tr -> {
            primitives.writeCollapsedVectorId(tr, signatureA, a1);
            primitives.writeCollapsedVectorId(tr, signatureA, a2);
            primitives.writeCollapsedVectorId(tr, signatureB, b1);
            return null;
        });

        // A full scan crosses signature boundaries: it returns every membership regardless of signature.
        final List<VectorId> scanned = db.run(tr ->
                AsyncUtil.collect(primitives.scanCollapsedVectorIdsIterable(tr), TestExecutors.defaultThreadPool())
                        .join());
        assertThat(scanned).containsExactlyInAnyOrder(a1, a2, b1);

        // Deleting a single (signature, primaryKey) removes only that member; its siblings and other signatures stay.
        db.run(tr -> {
            primitives.deleteCollapsedVectorId(tr, signatureA, a1.primaryKey());
            return null;
        });
        final VectorId deletedMember =
                db.run(tr -> primitives.fetchCollapsedVectorId(tr, signatureA, a1.primaryKey()).join());
        assertThat(deletedMember).as("the deleted member must be gone").isNull();

        final List<VectorId> survivorsUnderA =
                db.run(tr -> primitives.fetchCollapsedVectorIds(tr, signatureA).join());
        assertThat(survivorsUnderA).as("the sibling under the same signature must survive").containsExactly(a2);

        final List<VectorId> afterDelete = db.run(tr ->
                AsyncUtil.collect(primitives.scanCollapsedVectorIdsIterable(tr), TestExecutors.defaultThreadPool())
                        .join());
        assertThat(afterDelete).containsExactlyInAnyOrder(a2, b1);
    }

    /**
     * Drives {@link Search#clusterOverlapDiagnostics}, a recall/probe-count diagnostic with no production callers: for
     * each query vector it counts how many clusters' spheres (radius = the cluster's {@code maxEver} member distance)
     * contain it.
     * <p>
     * To exercise <em>genuine</em> overlap (a query inside more than one sphere) rather than the trivial
     * single-sphere case, this builds a tight cloud of near-duplicates around one base vector with a small
     * {@code primaryClusterMax}, so it splits into several <em>co-located</em> sub-clusters whose spheres necessarily
     * intersect. Querying the cloud centre (and each sub-centroid) then lands inside {@code >= 2} spheres. Also pins
     * the two early-out guards (no {@code AccessInfo} yet, and empty query list).
     */
    @ParameterizedTest
    @RandomSeedSource({0x0fdbL, 0x5ca1eL, 123456L, 78910L, 1123581321345589L})
    void clusterOverlapDiagnosticsDetectsRealSphereOverlap(final long randomSeed) throws Exception {
        // Small cap forces the near-duplicate cloud to split into co-located sub-clusters; the huge
        // collapseMinDuplicates (plus distinct perturbed signatures) keeps CollapseTask from folding them away.
        final Guardiann guardiann = newGuardiann(50, 1_000_000);
        final Search search = guardiann.getLocator().search();
        final SearchConfig searchConfig = new SearchConfig.SearchConfigBuilder().build();

        final DoubleRealVector base =
                (DoubleRealVector) TestHelpers.loadVectors(SiftTestHelpers.SIFT_SMALL_BASE_PATH, 1).get(0).vector();

        // Guard 1: a brand-new structure has no AccessInfo yet, so the diagnostic short-circuits to an empty list.
        final List<Integer> noAccessInfoOverlaps = db.run(tr ->
                search.clusterOverlapDiagnostics(tr, ImmutableList.of(base), ImmutableList.of(), searchConfig).join());
        assertThat(noAccessInfoOverlaps).isEmpty();

        // Insert a tight cloud of perturbed near-duplicates around the single base; the cap splits it into sub-clusters.
        final RandomHelpers.GaussianSampler sampler = new RandomHelpers.GaussianSampler(new SplittableRandom(randomSeed));
        for (int i = 0; i < 150; i++) {
            final DoubleRealVector perturbed = CommonTestHelpers.perturb(base, sampler, 0.5d);
            final Tuple pk = Tuple.from("overlap", i);
            db.run(tr -> {
                guardiann.insert(tr, pk, perturbed, null).join();
                return null;
            });
        }
        TestHelpers.runToQuiescence(db, guardiann);

        final StructureSnapshot snapshot = Objects.requireNonNull(TestHelpers.snapshotStructure(db, guardiann));
        assertThat(snapshot.numClusters())
                .as("the near-duplicate cloud must split into multiple co-located sub-clusters for overlap to exist")
                .isGreaterThanOrEqualTo(2);

        final List<ResultEntry> centroids = snapshot.clusters().values().stream()
                .map(cv -> new ResultEntry(StorageAdapter.tupleFromClusterId(cv.clusterId()), cv.centroid(), null,
                        0.0d, 0))
                .collect(Collectors.toList());

        // Query the cloud centre plus every sub-centroid. Because the sub-clusters are co-located, at least one of
        // these queries must fall inside >= 2 spheres — real overlap, not the trivial single-sphere membership.
        final List<RealVector> queries = new ArrayList<>();
        queries.add(base);
        snapshot.clusters().values().forEach(cv -> queries.add(cv.centroid()));
        final List<Integer> overlaps =
                db.run(tr -> search.clusterOverlapDiagnostics(tr, queries, centroids, searchConfig).join());

        logger.info("overlap counts over {} clusters for {} queries: {}", centroids.size(), queries.size(), overlaps);
        assertThat(overlaps).hasSize(queries.size());
        assertThat(overlaps).allSatisfy(count -> assertThat(count).isBetween(0, centroids.size()));
        assertThat(overlaps.stream().mapToInt(Integer::intValue).max().orElse(0))
                .as("with co-located sub-clusters, at least one query must sit inside >= 2 clusters' spheres")
                .isGreaterThanOrEqualTo(2);

        // Guard 2: an empty query list yields an empty result even with real centroids present.
        final List<Integer> emptyQueryOverlaps =
                db.run(tr -> search.clusterOverlapDiagnostics(tr, ImmutableList.of(), centroids, searchConfig).join());
        assertThat(emptyQueryOverlaps).isEmpty();
    }

    /**
     * End-to-end: inserting many identical vectors trips a split that can't repartition them, which enqueues a
     * COLLAPSE that folds the duplicates. Robust structural assertions (the exact post-quiescence shape is timing
     * dependent because inserts also drain deferred tasks).
     */
    @Test
    void duplicatesTriggerCollapse() throws Exception {
        final int numDuplicates = 100;
        final Guardiann guardiann = newGuardiann(80, 20);
        final RealVector duplicate = duplicateVector();

        // Count COLLAPSE executions across both the inserts (which drain tasks via executeSomeDeferredTasks) and the
        // explicit drain. The listener only tallies onto a pushed frame, so push one before any task can run.
        onWriteListener.pushFrame();
        try {
            insertIdentical(guardiann, duplicate, 0L, numDuplicates);
            TestHelpers.runToQuiescence(db, guardiann);

            assertThat(onWriteListener.getNumTasksExecutedByKind()
                    .getOrDefault(TaskKind.COLLAPSE, 0))
                    .as("inserting %d identical vectors must trigger at least one COLLAPSE", numDuplicates)
                    .isGreaterThanOrEqualTo(1);
        } finally {
            onWriteListener.popFrame();
        }

        final StructureSnapshot snapshot = Objects.requireNonNull(TestHelpers.snapshotStructure(db, guardiann));
        assertThat(snapshot.totalCollapsedRefs())
                .as("the identical vectors must be folded into at least one collapsed reference")
                .isGreaterThanOrEqualTo(1);
        assertThat(snapshot.totalPrimaries())
                .as("most of the %d identical primaries must have been folded away", numDuplicates)
                .isLessThan(numDuplicates);
        for (final ClusterView cv : snapshot.clusters().values()) {
            assertThat(cv.metadata().states().contains(ClusterMetadata.State.COLLAPSE))
                    .as("cluster %s must not be left in COLLAPSE state", cv.clusterId())
                    .isFalse();
        }

        TestHelpers.assertGuardiannInvariantsAfterDeletes(db, guardiann);
    }

    /**
     * The critical guarantee: after collapsing, every absorbed duplicate is still returned by a nearest-neighbor
     * query (Search expands the collapsed reference back into its members).
     */
    @Test
    void collapsePreservesQueryability() throws Exception {
        final int numDuplicates = 100;
        final Guardiann guardiann = newGuardiann(80, 20);
        final RealVector duplicate = duplicateVector();

        final List<Tuple> primaryKeys = insertIdentical(guardiann, duplicate, 0L, numDuplicates);
        TestHelpers.runToQuiescence(db, guardiann);

        // precondition: collapse actually happened, otherwise this test wouldn't be exercising it
        assertThat(Objects.requireNonNull(TestHelpers.snapshotStructure(db, guardiann)).totalCollapsedRefs())
                .as("precondition: the duplicates must have collapsed")
                .isGreaterThanOrEqualTo(1);

        assertAllResolvable(guardiann, duplicate, primaryKeys);
    }

    /**
     * Direct-drive no-op guards: {@link CollapseTask#runTask} does nothing when the target cluster is not marked
     * {@link ClusterMetadata.State#COLLAPSE}, and when the target cluster does not exist.
     */
    @Test
    void collapseTaskNoOps() throws Exception {
        final Guardiann guardiann = newGuardiann(100_000, 20); // huge cap: inserts never auto-collapse
        final RealVector duplicate = duplicateVector();
        insertIdentical(guardiann, duplicate, 0L, 30);
        TestHelpers.runToQuiescence(db, guardiann);

        final StructureSnapshot before = Objects.requireNonNull(TestHelpers.snapshotStructure(db, guardiann));
        final ClusterView cluster = Iterables.getOnlyElement(before.clusters().values());

        // (a) cluster not in COLLAPSE state -> no-op
        runCollapseDirectly(guardiann, cluster.clusterId(), cluster.transformedCentroid());
        final StructureSnapshot afterUnmarked = Objects.requireNonNull(TestHelpers.snapshotStructure(db, guardiann));
        assertThat(afterUnmarked.totalCollapsedRefs())
                .as("CollapseTask must be a no-op when the cluster is not in COLLAPSE state")
                .isZero();
        assertThat(afterUnmarked.totalPrimaries()).isEqualTo(before.totalPrimaries());

        // (b) cluster does not exist -> no-op (no exception, no writes)
        final UUID missingClusterId = UUID.randomUUID();
        assertThatCode(() -> runCollapseDirectly(guardiann, missingClusterId, cluster.transformedCentroid()))
                .as("CollapseTask must be a no-op when the target cluster does not exist")
                .doesNotThrowAnyException();
        final StructureSnapshot afterMissing = Objects.requireNonNull(TestHelpers.snapshotStructure(db, guardiann));
        assertThat(afterMissing.totalCollapsedRefs()).isZero();
        assertThat(afterMissing.totalPrimaries()).isEqualTo(before.totalPrimaries());
    }

    /**
     * Direct-drive re-collapse: a first collapse folds N duplicates into one collapsed reference; inserting M more
     * identical copies and collapsing again must aggregate the new copies into the <em>same</em> collapsed reference
     * — it must not create a second one, and must not re-collapse the existing collapsed reference.
     */
    @Test
    void recollapseAggregatesIntoExistingCollapsedReference() throws Exception {
        final int firstBatch = 50;
        final int secondBatch = 50;
        final Guardiann guardiann = newGuardiann(100_000, 20); // huge cap: collapse only when we drive it
        final RealVector duplicate = duplicateVector();

        // ---- Round 1: fold the first batch ----
        final List<Tuple> firstKeys = insertIdentical(guardiann, duplicate, 0L, firstBatch);
        TestHelpers.runToQuiescence(db, guardiann);
        collapseOnlyCluster(guardiann);

        final ClusterView afterFirst =
                onlyCluster(Objects.requireNonNull(TestHelpers.snapshotStructure(db, guardiann)));
        assertThat(afterFirst.collapsedRefs())
                .as("round 1 must produce exactly one collapsed reference")
                .hasSize(1);
        final VectorId collapsedRef = Iterables.getOnlyElement(afterFirst.collapsedRefs());
        final UUID signature = collapsedRef.primaryKey().getUUID(0);
        assertThat(fetchCollapsedIds(guardiann, signature))
                .as("round 1 must absorb all %d identical vectors", firstBatch)
                .hasSize(firstBatch);
        assertThat(afterFirst.metadata().states().contains(ClusterMetadata.State.COLLAPSE)).isFalse();

        // ---- Round 2: more identical copies, collapse again ----
        final List<Tuple> secondKeys = insertIdentical(guardiann, duplicate, 1_000_000L, secondBatch);
        TestHelpers.runToQuiescence(db, guardiann);
        collapseOnlyCluster(guardiann);

        final ClusterView afterSecond =
                onlyCluster(Objects.requireNonNull(TestHelpers.snapshotStructure(db, guardiann)));
        assertThat(afterSecond.collapsedRefs())
                .as("re-collapse must not create a second collapsed reference")
                .hasSize(1);
        assertThat(Iterables.getOnlyElement(afterSecond.collapsedRefs()))
                .as("the existing collapsed reference must be retained verbatim, never re-collapsed")
                .isEqualTo(collapsedRef);
        assertThat(fetchCollapsedIds(guardiann, signature))
                .as("re-collapse must aggregate the new %d duplicates into the existing collapsed section", secondBatch)
                .hasSize(firstBatch + secondBatch);

        final List<Tuple> allKeys = new ArrayList<>(firstKeys);
        allKeys.addAll(secondKeys);
        assertAllResolvable(guardiann, duplicate, allKeys);

        TestHelpers.assertGuardiannInvariantsAfterDeletes(db, guardiann);
    }

    /**
     * Exercises the new {@link ReassignTask} collapse-replica folding. A cluster holds several dangling replicas of a
     * vector whose primaries were folded into a collapsed set in some other cluster — same signature, distinct primary
     * keys. Reassigning that cluster must replace them with a single reference to the collapsed area (keeping the
     * highest replication priority among them), leave the collapsed-id store untouched, and keep every absorbed member
     * queryable.
     * <p>
     * The dangling-replica precondition is hand-rolled exactly as a collapse-in-another-cluster would leave it: the
     * vector metadata still present (so {@code cleanUpVectorReferences} keeps the replica), an entry in the
     * collapsed-id store, and an orphaned replica reference in this cluster. Reference vectors are stored lossily
     * (RaBitQ encoding), so the signature is derived from the <em>read-back</em> reference — the same form
     * {@code foldCollapsedReplicas} sees — rather than from the original vector.
     */
    @Test
    void reassignFoldsCollapsedReplicasIntoOneReference() throws Exception {
        final int numDistinctPrimaries = 10;
        final int numDanglingReplicas = 8;
        final Guardiann guardiann = newGuardiann(1_000, 20); // huge cap: the distinct inserts stay in one cluster
        final Primitives primitives = guardiann.getLocator().primitives();

        // A real, single cluster of distinct primaries, so reassign has a genuine cluster to dissolve. Index 0 is
        // reserved for the duplicate vector (see duplicateVector()), so the primaries use indices 1..n.
        final List<PrimaryKeyAndVector> records =
                TestHelpers.loadVectors(SiftTestHelpers.SIFT_SMALL_BASE_PATH, numDistinctPrimaries + 1);
        for (int i = 1; i <= numDistinctPrimaries; i++) {
            final Tuple primaryKey = Tuple.from("distinct", i);
            final RealVector vector = records.get(i).vector();
            db.run(tr -> {
                guardiann.insert(tr, primaryKey, vector, null).join();
                return null;
            });
        }
        TestHelpers.runToQuiescence(db, guardiann);
        final ClusterView cluster = onlyCluster(Objects.requireNonNull(TestHelpers.snapshotStructure(db, guardiann)));
        final UUID clusterId = cluster.clusterId();

        // The vector whose copies were collapsed elsewhere (index 0; distinct from the primaries inserted above).
        final RealVector duplicate = duplicateVector();

        // Phase 1: write the orphaned replicas (and their still-present metadata) into the cluster.
        db.run(tr -> {
            final AccessInfo accessInfo = Objects.requireNonNull(primitives.fetchAccessInfo(tr).join());
            final Quantizer quantizer = primitives.quantizer(accessInfo);
            final Transformed<RealVector> transformedDuplicate =
                    primitives.storageTransform(accessInfo).transform(duplicate);
            for (int i = 0; i < numDanglingReplicas; i++) {
                final Tuple primaryKey = Tuple.from("collapsed-dup", i);
                final VectorMetadata vectorMetadata =
                        new VectorMetadata(primaryKey, RandomHelpers.randomUuid(primaryKey, true), null);
                primitives.writeVectorMetadata(tr, vectorMetadata);
                final double replicationPriority = 1.0d - 0.01d * i; // i == 0 is the maximum
                primitives.writeVectorReference(tr, quantizer, clusterId,
                        VectorReference.replicatedCopy(vectorMetadata.vectorId(), transformedDuplicate,
                                replicationPriority, false));
            }
            return null;
        });

        // Phase 2: derive the signature from the read-back references (lossy encoding), then record them as collapsed.
        final ClusterView staged = onlyCluster(Objects.requireNonNull(TestHelpers.snapshotStructure(db, guardiann)));
        final List<VectorReference> stagedReplicas = staged.references().stream()
                .filter(ref -> !ref.isPrimaryCopy() && !ref.isCollapsed())
                .toList();
        assertThat(stagedReplicas)
                .as("precondition: the cluster must carry the hand-rolled dangling replicas")
                .hasSize(numDanglingReplicas);
        final UUID signature = StorageAdapter.signatureUuid(stagedReplicas.get(0).vector());
        final List<Tuple> collapsedPrimaryKeys =
                stagedReplicas.stream().map(ref -> ref.id().primaryKey()).toList();
        db.run(tr -> {
            for (final VectorReference replica : stagedReplicas) {
                primitives.writeCollapsedVectorId(tr, signature, replica.id());
            }
            return null;
        });

        // Drive reassign on the cluster.
        reassignCluster(guardiann, clusterId, staged.transformedCentroid());

        // Every dangling replica of the collapsed signature must have folded into exactly one collapsed-area reference.
        final ClusterView after = onlyCluster(Objects.requireNonNull(TestHelpers.snapshotStructure(db, guardiann)));
        assertThat(after.references().stream()
                .filter(ref -> !ref.isPrimaryCopy() && !ref.isCollapsed()
                        && signature.equals(StorageAdapter.signatureUuid(ref.vector())))
                .toList())
                .as("every dangling replica of the collapsed signature must be folded away")
                .isEmpty();
        final List<VectorReference> collapsedAreaRefs = after.references().stream()
                .filter(ref -> ref.isCollapsed() && Tuple.from(signature).equals(ref.id().primaryKey()))
                .toList();
        assertThat(collapsedAreaRefs)
                .as("the dangling replicas must fold into exactly one collapsed-area reference")
                .hasSize(1);
        assertThat(collapsedAreaRefs.get(0).replicationPriority())
                .as("the folded reference must keep the highest replication priority among the members")
                .isEqualTo(1.0d);

        // Folding replicas must not touch the collapsed-id store.
        assertThat(fetchCollapsedIds(guardiann, signature))
                .as("folding replicas must not change the collapsed-id store")
                .hasSize(numDanglingReplicas);

        // Queryability: the folded collapsed-area reference still expands to every absorbed member.
        assertAllResolvable(guardiann, duplicate, collapsedPrimaryKeys);
    }

    // ---------------------------------------------------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------------------------------------------------

    @Nonnull
    private Guardiann newGuardiann(final int primaryClusterMax, final int collapseMinDuplicates) {
        onWriteListener = new TestHelpers.TestOnWriteListener();
        final Config config = Guardiann.newConfigBuilder()
                .setUseRaBitQ(true)
                .setRaBitQNumExBits(6)
                .setMetric(Metric.EUCLIDEAN_METRIC)
                .setPrimaryClusterMax(primaryClusterMax)
                .setPrimaryClusterMin(1)
                .setCollapseMinDuplicates(collapseMinDuplicates)
                .setDeterministicRandomness(true)
                .build(128);
        return new Guardiann(getSubspace(),
                TestExecutors.defaultThreadPool(),
                config,
                onWriteListener,
                new TestHelpers.TestOnReadListener());
    }

    /** The first SIFT-small base vector, used as the vector we insert many identical copies of. */
    @Nonnull
    private RealVector duplicateVector() throws Exception {
        return TestHelpers.loadVectors(SiftTestHelpers.SIFT_SMALL_BASE_PATH, 1).get(0).vector();
    }

    /** Inserts {@code count} copies of {@code vector} under distinct primary keys {@code [pkBase, pkBase+count)}. */
    @Nonnull
    private List<Tuple> insertIdentical(@Nonnull final Guardiann guardiann, @Nonnull final RealVector vector,
                                        final long pkBase, final int count) {
        final List<Tuple> primaryKeys = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            final Tuple primaryKey = Tuple.from(pkBase + i);
            primaryKeys.add(primaryKey);
            db.run(tr -> {
                guardiann.insert(tr, primaryKey, vector, null).join();
                return null;
            });
        }
        return primaryKeys;
    }

    @Nonnull
    private ClusterView onlyCluster(@Nonnull final StructureSnapshot snapshot) {
        return Iterables.getOnlyElement(snapshot.clusters().values());
    }

    /** Marks the single cluster COLLAPSE and runs a {@link CollapseTask} on it directly. */
    private void collapseOnlyCluster(@Nonnull final Guardiann guardiann) {
        final ClusterView cluster = onlyCluster(Objects.requireNonNull(TestHelpers.snapshotStructure(db, guardiann)));
        setCollapseState(guardiann, cluster.clusterId());
        runCollapseDirectly(guardiann, cluster.clusterId(), cluster.transformedCentroid());
    }

    /** Adds {@link ClusterMetadata.State#COLLAPSE} to the cluster's states (preserving any others). */
    private void setCollapseState(@Nonnull final Guardiann guardiann, @Nonnull final UUID clusterId) {
        db.run(tr -> {
            final Primitives primitives = guardiann.getLocator().primitives();
            final ClusterMetadata metadata =
                    Objects.requireNonNull(primitives.fetchClusterMetadata(tr, clusterId).join());
            final EnumSet<ClusterMetadata.State> states = EnumSet.copyOf(metadata.states());
            states.add(ClusterMetadata.State.COLLAPSE);
            primitives.writeClusterMetadata(tr, metadata.withNewStates(states));
            return null;
        });
    }

    /** Builds a {@link CollapseTask} in memory and runs it in its own transaction. */
    private void runCollapseDirectly(@Nonnull final Guardiann guardiann, @Nonnull final UUID clusterId,
                                     @Nonnull final Transformed<RealVector> transformedCentroid) {
        db.run(tr -> {
            final Primitives primitives = guardiann.getLocator().primitives();
            final AccessInfo accessInfo = Objects.requireNonNull(primitives.fetchAccessInfo(tr).join());
            final UUID taskId = RandomHelpers.randomUuid(clusterId, true);
            final CollapseTask collapseTask =
                    CollapseTask.of(guardiann.getLocator(), accessInfo, taskId, clusterId, transformedCentroid);
            collapseTask.runTask(tr).join();
            return null;
        });
    }

    @Nonnull
    private List<VectorId> fetchCollapsedIds(@Nonnull final Guardiann guardiann, @Nonnull final UUID signature) {
        return db.run(tr -> guardiann.getLocator().primitives().fetchCollapsedVectorIds(tr, signature).join());
    }

    /** Asserts a kNN query for {@code vector} returns all of {@code primaryKeys} (collapsed members included). */
    private void assertAllResolvable(@Nonnull final Guardiann guardiann, @Nonnull final RealVector vector,
                                     @Nonnull final List<Tuple> primaryKeys) {
        final int k = primaryKeys.size();
        // Keep a generous pool so every near-duplicate survives to the result: at least 2x k, but never below an
        // absolute floor of 128 for small k. Expressed as a k-relative factor, that floor becomes 128/k.
        final double candidatePoolFactor = Math.max(2.0d, 128.0d / k);
        final List<? extends ResultEntry> results = db.run(tr ->
                guardiann.kNearestNeighborsSearch(tr, k,
                        new SearchConfig.SearchConfigBuilder().setCandidatePoolFactor(candidatePoolFactor)
                                .setSearchMaxClusters(64).build(), false, vector).join());
        assertThat(results)
                .extracting(ResultEntry::primaryKey)
                .as("all %d collapsed duplicates must remain queryable after collapse", primaryKeys.size())
                .containsAll(primaryKeys);
        logger.info("query returned {} results for {} expected primary keys", results.size(), primaryKeys.size());
    }

    /** Direct-drives a {@link ReassignTask} on a single cluster in its own transaction (no follow-up tasks). */
    private void reassignCluster(@Nonnull final Guardiann guardiann, @Nonnull final UUID clusterId,
                                 @Nonnull final Transformed<RealVector> transformedCentroid) {
        final Locator locator = guardiann.getLocator();
        final Primitives primitives = locator.primitives();
        final int numNearestClusters = 1 + guardiann.getConfig().reassignNumNeighboringClusters();
        db.run(transaction -> {
            final AccessInfo accessInfo = Objects.requireNonNull(primitives.fetchAccessInfo(transaction).join());
            final ClusterMetadata clusterMetadata =
                    Objects.requireNonNull(primitives.fetchClusterMetadata(transaction, clusterId).join());
            final StorageTransform storageTransform = primitives.storageTransform(accessInfo);
            final RealVector untransformedCentroid = storageTransform.untransform(transformedCentroid);
            final List<ClusterMetadataWithDistance> nearestClusterMetadata =
                    primitives.findNearestClustersMetadata(transaction, clusterMetadata,
                            untransformedCentroid, storageTransform, numNearestClusters,
                            guardiann.getConfig().reassignConcurrency()).join();
            final List<ClusterReference> neighboringClusters =
                    ClusterReference.fromClusterMetadataAndDistances(nearestClusterMetadata);
            final UUID taskId = RandomHelpers.randomUuid(clusterId, true);
            final ReassignTask reassignTask = ReassignTask.of(locator, accessInfo, taskId, clusterId,
                    transformedCentroid, Set.of(), neighboringClusters);
            reassignTask.reassign(transaction, clusterMetadata, untransformedCentroid, false).join();
            return null;
        });
    }
}
