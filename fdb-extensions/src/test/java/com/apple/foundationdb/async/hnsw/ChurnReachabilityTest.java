/*
 * ChurnReachabilityTest.java
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

package com.apple.foundationdb.async.hnsw;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.async.common.BaseTest;
import com.apple.foundationdb.async.common.CommonTestHelpers;
import com.apple.foundationdb.async.common.PrimaryKeyAndVector;
import com.apple.foundationdb.async.common.ResultEntry;
import com.apple.foundationdb.linear.DoubleRealVector;
import com.apple.foundationdb.linear.Metric;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.test.TestDatabaseExtension;
import com.apple.foundationdb.test.TestExecutors;
import com.apple.foundationdb.test.TestSubspaceExtension;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.RandomSeedSource;
import com.apple.test.Tags;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.TimeoutException;

import static com.apple.foundationdb.async.common.CommonTestHelpers.randomVectors;
import static com.apple.foundationdb.async.common.CommonTestHelpers.runAsyncToSync;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * What deleting nodes does to the graph, under the access pattern guardiann's cluster-centroid HNSW is subjected to: a
 * small graph where a split inserts centroids and a merge deletes one, so the population churns many times over.
 * <p>
 * The delete repair adds only incoming edges, so every node that held a reference to the deleted node ends the delete
 * one outgoing edge poorer. The tests here cover what that costs and what the repair does about it: that no node runs
 * out of outgoing edges, that some node can still reach all the others, that a node reached from its own vector is
 * found, and that references naming a node an earlier delete removed are eventually reaped.
 * <p>
 * The graph is deliberately small. It is the regime the centroid graph actually runs in, and it is the one where the
 * degree caps never bind, so nothing is hidden behind {@code mMax} pruning.
 */
@Tag(Tags.RequiresFDB)
@Tag(Tags.Slow)
@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
class ChurnReachabilityTest implements BaseTest {
    @Nonnull
    private static final Logger logger = LoggerFactory.getLogger(ChurnReachabilityTest.class);

    @RegisterExtension
    static final TestDatabaseExtension dbExtension = new TestDatabaseExtension();
    @RegisterExtension
    TestSubspaceExtension subspaceExtension = new TestSubspaceExtension(dbExtension);

    @TempDir
    Path tempDir;

    private static final int NUM_DIMENSIONS = 128;

    private Database db;

    @BeforeEach
    public void setUpDb() {
        db = dbExtension.getDatabase();
    }

    @Nonnull
    @Override
    public Database getDb() {
        return db;
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

    @ParameterizedTest
    @RandomSeedSource({0x0fdbL, 0x5ca1eL, 123456L})
    void everyNodeStaysReachableUnderChurn(final long seed)
            throws ExecutionException, InterruptedException, TimeoutException {
        final Random random = new Random(seed);
        final HNSW hnsw = newCentroidStyleHnsw();

        final int population = 13;
        final int numRounds = 60;
        final List<PrimaryKeyAndVector> pool = randomVectors(random, NUM_DIMENSIONS, population + numRounds);
        final List<PrimaryKeyAndVector> live = new ArrayList<>();

        for (final PrimaryKeyAndVector record : pool.subList(0, population)) {
            runAsyncToSync(db, tr -> hnsw.insert(tr, record.primaryKey(), record.vector(), null));
            live.add(record);
        }

        for (int round = 0; round < numRounds; round++) {
            // Delete one live node, then insert a fresh one — population stays constant, but the graph churns.
            final PrimaryKeyAndVector victim =
                    CommonTestHelpers.pickRandomVectors(random, live, 1).get(0);
            runAsyncToSync(db, tr -> hnsw.delete(tr, victim.primaryKey()));
            live.remove(victim);

            final PrimaryKeyAndVector fresh = pool.get(population + round);
            runAsyncToSync(db, tr -> hnsw.insert(tr, fresh.primaryKey(), fresh.vector(), null));
            live.add(fresh);

            // Every live node must be findable by a search centered on its own vector: a node at distance zero
            // from the query is only missed if the traversal cannot reach it at all.
            for (final PrimaryKeyAndVector probe : live) {
                final List<? extends ResultEntry> results =
                        runAsyncToSync(db, tr -> hnsw.kNearestNeighborsSearch(tr, live.size(), 400, false,
                                probe.vector()));
                final Set<Tuple> reached = results.stream()
                        .map(ResultEntry::primaryKey)
                        .collect(ImmutableSet.toImmutableSet());
                if (!reached.contains(probe.primaryKey()) || reached.size() < live.size()) {
                    logger.warn("round={}: search centered on {} reached {} of {} live nodes",
                            round, probe.primaryKey(), reached.size(), live.size());
                }
                assertThat(reached)
                        .as("round %d: a search centered on a live node's own vector must reach every live node",
                                round)
                        .hasSize(live.size());
            }
        }
    }

    /**
     * As above, but the population <em>shrinks</em>: each round deletes a node without replacing it, the way a run
     * of guardiann merges takes the centroid count from 13 down to a handful.
     */
    @ParameterizedTest
    @RandomSeedSource({0x0fdbL, 0x5ca1eL, 123456L})
    void everyNodeStaysReachableWhileShrinking(final long seed)
            throws ExecutionException, InterruptedException, TimeoutException {
        final Random random = new Random(seed);
        final HNSW hnsw = newCentroidStyleHnsw();
        final List<PrimaryKeyAndVector> live = new ArrayList<>();

        for (final PrimaryKeyAndVector record : randomVectors(random, NUM_DIMENSIONS, 13)) {
            runAsyncToSync(db, tr -> hnsw.insert(tr, record.primaryKey(), record.vector(), null));
            live.add(record);
        }

        while (live.size() > 2) {
            final PrimaryKeyAndVector victim = CommonTestHelpers.pickRandomVectors(random, live, 1).get(0);
            runAsyncToSync(db, tr -> hnsw.delete(tr, victim.primaryKey()));
            live.remove(victim);
            assertEveryLiveNodeReachable(hnsw, live, "after shrinking to " + live.size());
        }
    }

    /**
     * The split/merge shape: one node is deleted and two fresh ones inserted <em>within a single transaction</em>,
     * which is what {@code SplitMergeTask} does to the centroid graph.
     */
    @ParameterizedTest
    @RandomSeedSource({0x0fdbL, 0x5ca1eL, 123456L})
    void everyNodeStaysReachableAcrossSameTransactionSplits(final long seed)
            throws ExecutionException, InterruptedException, TimeoutException {
        final Random random = new Random(seed);
        final HNSW hnsw = newCentroidStyleHnsw();
        final int numRounds = 40;
        final List<PrimaryKeyAndVector> pool = randomVectors(random, NUM_DIMENSIONS, 13 + 2 * numRounds);
        final List<PrimaryKeyAndVector> live = new ArrayList<>(pool.subList(0, 13));

        for (final PrimaryKeyAndVector record : live) {
            runAsyncToSync(db, tr -> hnsw.insert(tr, record.primaryKey(), record.vector(), null));
        }

        for (int round = 0; round < numRounds; round++) {
            final PrimaryKeyAndVector victim = CommonTestHelpers.pickRandomVectors(random, live, 1).get(0);
            final PrimaryKeyAndVector firstChild = pool.get(13 + 2 * round);
            final PrimaryKeyAndVector secondChild = pool.get(14 + 2 * round);

            runAsyncToSync(db, tr -> hnsw.delete(tr, victim.primaryKey())
                    .thenCompose(ignored ->
                            hnsw.insert(tr, firstChild.primaryKey(), firstChild.vector(), null))
                    .thenCompose(ignored ->
                            hnsw.insert(tr, secondChild.primaryKey(), secondChild.vector(), null)));

            live.remove(victim);
            live.add(firstChild);
            live.add(secondChild);

            // Keep the population small, the way merges keep the centroid count bounded.
            while (live.size() > 13) {
                final PrimaryKeyAndVector extra = CommonTestHelpers.pickRandomVectors(random, live, 1).get(0);
                runAsyncToSync(db, tr -> hnsw.delete(tr, extra.primaryKey()));
                live.remove(extra);
            }
            assertEveryLiveNodeReachable(hnsw, live, "after split round " + round);
        }
    }

    /**
     * Clustered geometry, which is what a centroid graph actually holds: the vectors sit in a few tight groups
     * rather than spread uniformly, so HNSW's diversity heuristic prunes the within-group links down to very few.
     * A split producing two children close to each other is the same shape.
     */
    @ParameterizedTest
    @RandomSeedSource({0x0fdbL, 0x5ca1eL, 123456L})
    void everyNodeStaysReachableWithClusteredGeometry(final long seed)
            throws ExecutionException, InterruptedException, TimeoutException {
        final Random random = new Random(seed);
        final HNSW hnsw = newCentroidStyleHnsw();
        final List<PrimaryKeyAndVector> live = new ArrayList<>();
        int nextKey = 0;

        for (int i = 0; i < 13; i++) {
            final PrimaryKeyAndVector record = clusteredVector(random, nextKey++);
            runAsyncToSync(db, tr -> hnsw.insert(tr, record.primaryKey(), record.vector(), null));
            live.add(record);
        }
        assertEveryLiveNodeReachable(hnsw, live, "after the initial 13 inserts");

        // Churn at constant population, then shrink — the two phases the centroid graph goes through.
        for (int round = 0; round < 40; round++) {
            final PrimaryKeyAndVector victim = CommonTestHelpers.pickRandomVectors(random, live, 1).get(0);
            runAsyncToSync(db, tr -> hnsw.delete(tr, victim.primaryKey()));
            live.remove(victim);

            final PrimaryKeyAndVector fresh = clusteredVector(random, nextKey++);
            runAsyncToSync(db, tr -> hnsw.insert(tr, fresh.primaryKey(), fresh.vector(), null));
            live.add(fresh);
            assertEveryLiveNodeReachable(hnsw, live, "after clustered churn round " + round);
        }

        while (live.size() > 2) {
            final PrimaryKeyAndVector victim = CommonTestHelpers.pickRandomVectors(random, live, 1).get(0);
            runAsyncToSync(db, tr -> hnsw.delete(tr, victim.primaryKey()));
            live.remove(victim);
            assertEveryLiveNodeReachable(hnsw, live, "after clustered shrink to " + live.size());
        }
    }

    /**
     * A vector drawn from one of a few tight groups: a random group center displaced by a small jitter, so
     * within-group distances are far smaller than between-group ones.
     */
    @Nonnull
    private static PrimaryKeyAndVector clusteredVector(@Nonnull final Random random, final int key) {
        final int numGroups = 4;
        final int group = random.nextInt(numGroups);
        final double[] components = new double[NUM_DIMENSIONS];
        for (int d = 0; d < NUM_DIMENSIONS; d++) {
            // Group centers are far apart along every dimension; the jitter is two orders of magnitude smaller.
            components[d] = (group + 1) * 10.0d + random.nextDouble() * 0.1d;
        }
        return new PrimaryKeyAndVector(Tuple.from(key), new DoubleRealVector(components));
    }

    /**
     * Checks what the repair leaves behind for the direct neighbours of a deleted node: a node that loses its edge
     * to the deleted node should come out of the repair with at least one outgoing edge of its own, as long as other
     * nodes remain for it to point at.
     */
    @ParameterizedTest
    @RandomSeedSource({0x0fdbL, 0x5ca1eL, 123456L})
    void repairGivesDirectNeighborsReplacementOutgoingEdges(final long seed)
            throws ExecutionException, InterruptedException, TimeoutException {
        final Random random = new Random(seed);
        final HNSW hnsw = newCentroidStyleHnsw();
        final List<PrimaryKeyAndVector> live = new ArrayList<>();
        int nextKey = 0;

        for (int i = 0; i < 13; i++) {
            final PrimaryKeyAndVector record = clusteredVector(random, nextKey++);
            runAsyncToSync(db, tr -> hnsw.insert(tr, record.primaryKey(), record.vector(), null));
            live.add(record);
        }

        for (int round = 0; round < 40 && live.size() > 2; round++) {
            final Map<Tuple, List<Tuple>> before = readLayerZeroAdjacency();
            final PrimaryKeyAndVector victim = CommonTestHelpers.pickRandomVectors(random, live, 1).get(0);
            final List<Tuple> outNeighborsOfVictim = before.getOrDefault(victim.primaryKey(), ImmutableList.of());
            final List<Tuple> inNeighborsOfVictim = before.entrySet().stream()
                    .filter(entry -> entry.getValue().contains(victim.primaryKey()))
                    .map(Map.Entry::getKey)
                    .collect(ImmutableList.toImmutableList());
            // Nodes that point at the victim without the victim pointing back. The repair only visits nodes it can
            // find by following the victim's outgoing edges, so these are the ones at risk of being missed.
            final List<Tuple> pointAtVictimWithoutReciprocation = inNeighborsOfVictim.stream()
                    .filter(key -> !outNeighborsOfVictim.contains(key))
                    .collect(ImmutableList.toImmutableList());

            runAsyncToSync(db, tr -> hnsw.delete(tr, victim.primaryKey()));
            live.remove(victim);

            final Map<Tuple, List<Tuple>> after = readLayerZeroAdjacency();
            final List<Tuple> lostAllOutgoingEdges = outNeighborsOfVictim.stream()
                    .filter(key -> after.containsKey(key) && after.get(key).isEmpty())
                    .collect(ImmutableList.toImmutableList());
            final List<String> stillPointAtVictim = after.entrySet().stream()
                    .filter(entry -> entry.getValue().contains(victim.primaryKey()))
                    .map(entry -> entry.getKey().toString())
                    .collect(ImmutableList.toImmutableList());
            final long asymmetricBefore = countAsymmetricEdges(before);
            final long asymmetricAfter = countAsymmetricEdges(after);

            if (logger.isDebugEnabled()) {
                logger.debug("round={}: deleted={}, itsOutNeighbors={}, itsInNeighbors={},"
                                + " pointAtItWithoutReciprocation={}, stillPointAtDeleted={},"
                                + " leftWithNoOutgoingEdges={}, oneDirectionalEdges {} -> {}",
                        round, victim.primaryKey(), outNeighborsOfVictim, inNeighborsOfVictim,
                        pointAtVictimWithoutReciprocation, stillPointAtVictim, lostAllOutgoingEdges,
                        asymmetricBefore, asymmetricAfter);
            }

            assertThat(lostAllOutgoingEdges)
                    .as("round %d: deleting %s left these of its neighbours with no outgoing edge at all,"
                            + " though %d nodes remain", round, victim.primaryKey(), after.size())
                    .isEmpty();
        }
    }

    /** Counts edges {@code p -> q} on layer 0 for which there is no matching {@code q -> p}. */
    private static long countAsymmetricEdges(@Nonnull final Map<Tuple, List<Tuple>> adjacency) {
        long count = 0;
        for (final Map.Entry<Tuple, List<Tuple>> entry : adjacency.entrySet()) {
            for (final Tuple neighbor : entry.getValue()) {
                final List<Tuple> reverse = adjacency.get(neighbor);
                if (reverse == null || !reverse.contains(entry.getKey())) {
                    count++;
                }
            }
        }
        return count;
    }

    @Nonnull
    private Map<Tuple, List<Tuple>> readLayerZeroAdjacency() {
        final Map<Tuple, List<Tuple>> adjacency = new LinkedHashMap<>();
        TestHelpers.scanLayer(getDb(), getSubspace(), centroidStyleConfig(), 0, 100,
                node -> adjacency.put(node.getPrimaryKey(),
                        node.getNeighbors().stream()
                                .map(NodeReference::getPrimaryKey)
                                .collect(ImmutableList.toImmutableList())));
        return adjacency;
    }

    /**
     * Tracks usable out-degree over a long run of churn. An edge naming a node that no longer exists is not usable,
     * so the quantity that matters for traversal is the number of a node's neighbours that still exist.
     */
    @ParameterizedTest
    @RandomSeedSource({0x0fdbL, 0x5ca1eL, 123456L})
    void usableOutDegreeDoesNotDecayToZero(final long seed)
            throws ExecutionException, InterruptedException, TimeoutException {
        final Random random = new Random(seed);
        final HNSW hnsw = newCentroidStyleHnsw();
        final List<PrimaryKeyAndVector> live = new ArrayList<>();
        int nextKey = 0;

        for (int i = 0; i < 13; i++) {
            final PrimaryKeyAndVector record = clusteredVector(random, nextKey++);
            runAsyncToSync(db, tr -> hnsw.insert(tr, record.primaryKey(), record.vector(), null));
            live.add(record);
        }

        for (int round = 0; round < 300; round++) {
            final PrimaryKeyAndVector victim = CommonTestHelpers.pickRandomVectors(random, live, 1).get(0);
            runAsyncToSync(db, tr -> hnsw.delete(tr, victim.primaryKey()));
            live.remove(victim);

            final PrimaryKeyAndVector fresh = clusteredVector(random, nextKey++);
            runAsyncToSync(db, tr -> hnsw.insert(tr, fresh.primaryKey(), fresh.vector(), null));
            live.add(fresh);

            final Map<Tuple, List<Tuple>> adjacency = readLayerZeroAdjacency();
            int deadEdges = 0;
            int totalEdges = 0;
            final List<Integer> usableOutDegrees = new ArrayList<>();
            final List<Tuple> starved = new ArrayList<>();
            for (final Map.Entry<Tuple, List<Tuple>> entry : adjacency.entrySet()) {
                int usable = 0;
                for (final Tuple neighbor : entry.getValue()) {
                    totalEdges++;
                    if (adjacency.containsKey(neighbor)) {
                        usable++;
                    } else {
                        deadEdges++;
                    }
                }
                usableOutDegrees.add(usable);
                if (usable == 0) {
                    starved.add(entry.getKey());
                }
            }
            if (!starved.isEmpty()) {
                logger.warn("round={}: nodes={}, edges={}, deadEdges={}, usableOutDegrees={}, starved={}",
                        round, adjacency.size(), totalEdges, deadEdges, usableOutDegrees, starved);
            } else if (logger.isDebugEnabled() && round % 25 == 0) {
                logger.debug("round={}: nodes={}, edges={}, deadEdges={}, usableOutDegrees={}",
                        round, adjacency.size(), totalEdges, deadEdges, usableOutDegrees);
            }
            assertThat(starved)
                    .as("round %d: nodes whose every outgoing edge names a node that no longer exists", round)
                    .isEmpty();
        }
    }

    /**
     * Deletes only, never inserts, and after each deletion asks an entry-point-independent question: is there still
     * some node from which every remaining node can be reached by following outgoing edges? If no such node exists,
     * no choice of entry node could cover the graph, which is a partition attributable to deletion alone.
     */
    @ParameterizedTest
    @RandomSeedSource({0x0fdbL, 0x5ca1eL, 123456L, 987654321L, 42L})
    void deleteOnlyLeavesSomeNodeAbleToReachAllOthers(final long seed)
            throws ExecutionException, InterruptedException, TimeoutException {
        final Random random = new Random(seed);
        final HNSW hnsw = newCentroidStyleHnsw();
        final List<PrimaryKeyAndVector> live = new ArrayList<>();
        final int population = 30;

        for (int i = 0; i < population; i++) {
            final PrimaryKeyAndVector record = clusteredVector(random, i);
            runAsyncToSync(db, tr -> hnsw.insert(tr, record.primaryKey(), record.vector(), null));
            live.add(record);
        }

        final Map<Tuple, List<Tuple>> initial = readLayerZeroAdjacency();
        assertThat(nodesThatReachEverything(initial))
                .as("a freshly built graph must have a node that reaches all others")
                .isNotEmpty();

        while (live.size() > 2) {
            final PrimaryKeyAndVector victim = CommonTestHelpers.pickRandomVectors(random, live, 1).get(0);
            runAsyncToSync(db, tr -> hnsw.delete(tr, victim.primaryKey()));
            live.remove(victim);

            final Map<Tuple, List<Tuple>> adjacency = readLayerZeroAdjacency();
            final List<Tuple> roots = nodesThatReachEverything(adjacency);
            if (roots.isEmpty()) {
                logger.warn("remaining={}: NO node reaches all others; adjacency={}", live.size(), adjacency);
            }
            assertThat(roots)
                    .as("with %d nodes left, no node can reach all the others, so no entry node could cover the"
                            + " graph; adjacency=%s", adjacency.size(), adjacency)
                    .isNotEmpty();
        }
    }

    /**
     * Returns the nodes from which every node in {@code adjacency} is reachable by following outgoing edges.
     * Neighbour entries naming absent nodes are skipped, since traversal cannot use them.
     */
    @Nonnull
    private static List<Tuple> nodesThatReachEverything(@Nonnull final Map<Tuple, List<Tuple>> adjacency) {
        final ImmutableList.Builder<Tuple> roots = ImmutableList.builder();
        for (final Tuple start : adjacency.keySet()) {
            final Set<Tuple> seen = new java.util.LinkedHashSet<>();
            final Deque<Tuple> frontier = new ArrayDeque<>();
            seen.add(start);
            frontier.add(start);
            while (!frontier.isEmpty()) {
                for (final Tuple next : adjacency.getOrDefault(frontier.pop(), ImmutableList.of())) {
                    if (adjacency.containsKey(next) && seen.add(next)) {
                        frontier.add(next);
                    }
                }
            }
            if (seen.size() == adjacency.size()) {
                roots.add(start);
            }
        }
        return roots.build();
    }

    /**
     * Long churn run asking the entry-point-independent question after every round: does some node still reach all
     * others? A failure is labelled by whether the just-inserted node came out with no edges at all, which only
     * happens when the insert took the "first node in an empty graph" path — that is the entry-node problem, not
     * deletion repair. Any other failure is attributable to deletion repair alone.
     */
    @ParameterizedTest
    @RandomSeedSource({0x0fdbL, 0x5ca1eL, 123456L})
    void churnLeavesSomeNodeAbleToReachAllOthers(final long seed)
            throws ExecutionException, InterruptedException, TimeoutException {
        final Random random = new Random(seed);
        final HNSW hnsw = newCentroidStyleHnsw();
        final List<PrimaryKeyAndVector> live = new ArrayList<>();
        int nextKey = 0;
        int roundsWithoutRoot = 0;
        int roundsWithoutRootFromEmptyGraphInsert = 0;

        for (int i = 0; i < 13; i++) {
            final PrimaryKeyAndVector record = clusteredVector(random, nextKey++);
            runAsyncToSync(db, tr -> hnsw.insert(tr, record.primaryKey(), record.vector(), null));
            live.add(record);
        }

        for (int round = 0; round < 500; round++) {
            final PrimaryKeyAndVector victim = CommonTestHelpers.pickRandomVectors(random, live, 1).get(0);
            runAsyncToSync(db, tr -> hnsw.delete(tr, victim.primaryKey()));
            live.remove(victim);

            // Check rootedness with the victim gone but before inserting, so a failure here cannot involve a new node.
            final Map<Tuple, List<Tuple>> afterDelete = readLayerZeroAdjacency();
            if (nodesThatReachEverything(afterDelete).isEmpty()) {
                logger.warn("round={}: NO root after the DELETE alone; adjacency={}", round, afterDelete);
                roundsWithoutRoot++;
            }

            final PrimaryKeyAndVector fresh = clusteredVector(random, nextKey++);
            runAsyncToSync(db, tr -> hnsw.insert(tr, fresh.primaryKey(), fresh.vector(), null));
            live.add(fresh);

            final Map<Tuple, List<Tuple>> afterInsert = readLayerZeroAdjacency();
            if (nodesThatReachEverything(afterInsert).isEmpty()) {
                final boolean freshNodeHasNoEdges =
                        afterInsert.getOrDefault(fresh.primaryKey(), ImmutableList.of()).isEmpty()
                                && afterInsert.values().stream()
                                        .noneMatch(list -> list.contains(fresh.primaryKey()));
                if (freshNodeHasNoEdges) {
                    roundsWithoutRootFromEmptyGraphInsert++;
                } else {
                    logger.warn("round={}: NO root after the INSERT, and the new node did have edges;"
                            + " adjacency={}", round, afterInsert);
                    roundsWithoutRoot++;
                }
            }
        }
        if (logger.isDebugEnabled()) {
            logger.debug("seed={}: rounds with no root attributable to deletion repair={},"
                            + " rounds with no root caused by an insert into an apparently empty graph={}",
                    seed, roundsWithoutRoot, roundsWithoutRootFromEmptyGraphInsert);
        }
        assertThat(roundsWithoutRoot)
                .as("rounds in which no node could reach all others, excluding the entry-node path")
                .isZero();
    }


    @Nonnull
    private HNSW newCentroidStyleHnsw() {
        return new HNSW(getSubspace(), TestExecutors.defaultThreadPool(), centroidStyleConfig(),
                new TestHelpers.TestOnWriteListener(), new TestHelpers.TestOnReadListener());
    }


    /**
     * guardiann's cluster-centroid configuration, verbatim, except that the delete repair's reciprocity arm can be
     * selected from the command line so a threshold sweep needs no recompile:
     * {@code -Dtest.sysProp.hnsw.replacementEdgeMaxOutDegree=2}, where {@code 0} disables replacement edges entirely.
     * With the property unset this is whatever {@link Config} defaults to.
     */
    @Nonnull
    private static Config centroidStyleConfig() {
        final Config.ConfigBuilder builder = HNSW.newConfigBuilder()
                .setMetric(Metric.EUCLIDEAN_METRIC)
                .setUseInlining(false)
                .setEfRepair(64)
                .setExtendCandidates(false)
                .setKeepPrunedConnections(false)
                .setUseRaBitQ(false)
                .setM(16)
                .setMMax(24)
                .setMMax0(32);
        final String maxOutDegree = System.getProperty("hnsw.replacementEdgeMaxOutDegree");
        if (maxOutDegree != null) {
            builder.setReplacementEdgeMaxOutDegree(Integer.parseInt(maxOutDegree));
        }
        return builder.build(NUM_DIMENSIONS);
    }

    /**
     * Asserts that a search centered on each live node's own vector reaches every live node. A node at distance
     * zero from the query is only ever missed because the traversal cannot reach it at all.
     */
    private void assertEveryLiveNodeReachable(@Nonnull final HNSW hnsw,
                                              @Nonnull final List<PrimaryKeyAndVector> live,
                                              @Nonnull final String where)
            throws ExecutionException, InterruptedException, TimeoutException {
        for (final PrimaryKeyAndVector probe : live) {
            final List<? extends ResultEntry> results =
                    runAsyncToSync(db, tr -> hnsw.kNearestNeighborsSearch(tr, live.size(), 400, false,
                            probe.vector()));
            final Set<Tuple> reached = results.stream()
                    .map(ResultEntry::primaryKey)
                    .collect(ImmutableSet.toImmutableSet());
            if (reached.size() < live.size()) {
                logger.warn("{}: search centered on {} reached {} of {} live nodes",
                        where, probe.primaryKey(), reached.size(), live.size());
            }
            assertThat(reached)
                    .as("%s: a search centered on a live node's own vector must reach every live node", where)
                    .hasSize(live.size());
        }
    }

    /**
     * A delete reaps the references of the nodes it visits that name a node an earlier delete removed.
     * <p>
     * Such a reference is left behind whenever the node holding it lies outside the repair candidate set of the delete
     * that removed its target, so this churns until one exists rather than trying to construct that position directly.
     * It then deletes a node that points at the holder, which puts the holder in the first degree of the next delete's
     * candidate set and so has the holder's own neighbors read, which is what proves the reference dead.
     */
    @ParameterizedTest
    @RandomSeedSource({0x0fdbL, 0x5ca1eL, 123456L})
    void deleteReapsReferencesNamingDeletedNodes(final long seed)
            throws ExecutionException, InterruptedException, TimeoutException {
        final Random random = new Random(seed);
        final ReapRecordingOnWriteListener onWriteListener = new ReapRecordingOnWriteListener();
        final HNSW hnsw = new HNSW(getSubspace(), TestExecutors.defaultThreadPool(), centroidStyleConfig(),
                onWriteListener, new TestHelpers.TestOnReadListener());
        final List<PrimaryKeyAndVector> live = new ArrayList<>();
        int nextKey = 0;

        for (int i = 0; i < 13; i++) {
            final PrimaryKeyAndVector record = clusteredVector(random, nextKey++);
            runAsyncToSync(db, tr -> hnsw.insert(tr, record.primaryKey(), record.vector(), null));
            live.add(record);
        }

        // churn until some node holds a reference naming a node that is no longer there
        Map<Tuple, List<Tuple>> adjacency = readLayerZeroAdjacency();
        Tuple holder = null;
        Tuple deadKey = null;
        for (int round = 0; round < 200 && holder == null; round++) {
            final PrimaryKeyAndVector victim = CommonTestHelpers.pickRandomVectors(random, live, 1).get(0);
            runAsyncToSync(db, tr -> hnsw.delete(tr, victim.primaryKey()));
            live.remove(victim);

            final PrimaryKeyAndVector fresh = clusteredVector(random, nextKey++);
            runAsyncToSync(db, tr -> hnsw.insert(tr, fresh.primaryKey(), fresh.vector(), null));
            live.add(fresh);

            adjacency = readLayerZeroAdjacency();
            for (final Map.Entry<Tuple, List<Tuple>> entry : adjacency.entrySet()) {
                for (final Tuple neighbor : entry.getValue()) {
                    if (!adjacency.containsKey(neighbor)) {
                        holder = entry.getKey();
                        deadKey = neighbor;
                        break;
                    }
                }
                if (holder != null) {
                    break;
                }
            }
        }
        // cast to Object because Tuple is both Iterable and Comparable, which makes the assertThat overloads ambiguous
        assertThat((Object)holder)
                .as("churn did not produce a reference naming a deleted node, so there is nothing to reap here")
                .isNotNull();
        if (logger.isDebugEnabled()) {
            logger.debug("holder={} holds a reference to the deleted node {}", holder, deadKey);
        }

        // a node that points at the holder, so that deleting it reads the holder's own neighbors
        final Tuple holderKey = holder;
        final Tuple pointsAtHolder = adjacency.entrySet().stream()
                .filter(entry -> !entry.getKey().equals(holderKey) && entry.getValue().contains(holderKey))
                .map(Map.Entry::getKey)
                .findFirst()
                .orElse(null);
        assertThat((Object)pointsAtHolder)
                .as("no node points at %s, so no delete would put it in the first degree of a candidate set", holder)
                .isNotNull();

        final int reapedBefore = onWriteListener.numReferencesReaped.get();
        runAsyncToSync(db, tr -> hnsw.delete(tr, pointsAtHolder));

        final Map<Tuple, List<Tuple>> afterAdjacency = readLayerZeroAdjacency();
        assertThat(afterAdjacency).as("the holder itself must survive this delete").containsKey(holder);
        assertThat(afterAdjacency.get(holder))
                .as("the reference to the deleted node %s must be gone from %s", deadKey, holder)
                .doesNotContain(deadKey);
        assertThat(onWriteListener.numReferencesReaped.get() - reapedBefore)
                .as("the delete must report what it reaped")
                .isPositive();
    }

    /**
     * The reap callback is invoked for every delete, whether or not it reaped anything, so that a roll-up can tell a
     * delete that found nothing from a delete that never looked.
     */
    @Test
    void theReapCallbackIsInvokedEvenWhenNothingIsReaped()
            throws ExecutionException, InterruptedException, TimeoutException {
        final Random random = new Random(0x0fdbL);
        final ReapRecordingOnWriteListener onWriteListener = new ReapRecordingOnWriteListener();
        final HNSW hnsw = new HNSW(getSubspace(), TestExecutors.defaultThreadPool(), centroidStyleConfig(),
                onWriteListener, new TestHelpers.TestOnReadListener());

        // a freshly built graph holds no reference to a deleted node, so the first delete can have nothing to reap
        final List<PrimaryKeyAndVector> live = new ArrayList<>();
        for (int i = 0; i < 6; i++) {
            final PrimaryKeyAndVector record = clusteredVector(random, i);
            runAsyncToSync(db, tr -> hnsw.insert(tr, record.primaryKey(), record.vector(), null));
            live.add(record);
        }
        assertThat(onWriteListener.numCalls.get()).as("no delete has run yet").isZero();

        runAsyncToSync(db, tr -> hnsw.delete(tr, live.get(0).primaryKey()));

        assertThat(onWriteListener.numCalls.get())
                .as("the delete must invoke the callback once per layer it deleted from")
                .isPositive();
        assertThat(onWriteListener.numReferencesReaped.get())
                .as("a graph with no references to deleted nodes has nothing to reap")
                .isZero();
        assertThat(onWriteListener.numCallsReportingZero.get())
                .as("those invocations must be the ones reporting zero")
                .isEqualTo(onWriteListener.numCalls.get());
    }

    /** Records what {@link OnWriteListener#onNeighborReferencesReaped} reports, including the calls reporting zero. */
    private static final class ReapRecordingOnWriteListener extends TestHelpers.TestOnWriteListener {
        private final AtomicInteger numCalls = new AtomicInteger();
        private final AtomicInteger numCallsReportingZero = new AtomicInteger();
        private final AtomicInteger numReferencesReaped = new AtomicInteger();

        @Override
        public void onNeighborReferencesReaped(final int layer, final int numReferences) {
            numCalls.incrementAndGet();
            if (numReferences == 0) {
                numCallsReportingZero.incrementAndGet();
            }
            numReferencesReaped.addAndGet(numReferences);
        }
    }
}
