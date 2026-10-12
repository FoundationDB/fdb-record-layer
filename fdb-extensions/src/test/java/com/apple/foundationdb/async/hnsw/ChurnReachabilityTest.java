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
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

import static com.apple.foundationdb.async.common.CommonTestHelpers.randomVectors;
import static com.apple.foundationdb.async.common.CommonTestHelpers.runAsyncToSync;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Reachability of every node under interleaved inserts and deletes on a small graph — the access pattern
 * guardiann's cluster-centroid HNSW is subjected to, where a split inserts centroids and a merge deletes one.
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

            logger.warn("round={}: deleted={}, itsOutNeighbors={}, itsInNeighbors={},"
                            + " pointAtItWithoutReciprocation={}, STILL_POINT_AT_DELETED={},"
                            + " leftWithNoOutgoingEdges={}, oneDirectionalEdges {} -> {}",
                    round, victim.primaryKey(), outNeighborsOfVictim, inNeighborsOfVictim,
                    pointAtVictimWithoutReciprocation, stillPointAtVictim, lostAllOutgoingEdges,
                    asymmetricBefore, asymmetricAfter);

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
            if (round % 25 == 0 || !starved.isEmpty()) {
                logger.warn("round={}: nodes={}, edges={}, deadEdges={}, usableOutDegrees={}, starved={}",
                        round, adjacency.size(), totalEdges, deadEdges, usableOutDegrees, starved);
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
        logger.warn("seed={}: rounds with no root attributable to deletion repair={},"
                        + " rounds with no root caused by an insert into an apparently empty graph={}",
                seed, roundsWithoutRoot, roundsWithoutRootFromEmptyGraphInsert);
        assertThat(roundsWithoutRoot)
                .as("rounds in which no node could reach all others, excluding the entry-node path")
                .isZero();
    }

    /**
     * Classifies, at every delete, the nodes that lose their edge to the deleted node {@code D} against the repair's
     * candidate set {@code Hop2(D)} — {@code D}'s existing out-neighbours plus <em>their</em> out-neighbours. When a
     * node decays to usable out-degree zero it was either a candidate the diversity heuristic scored and declined to
     * select, or a node the candidate set never contained at all; only the first case could be fixed by changing how
     * candidates are scored. Collects the whole distribution rather than failing at the first decay.
     */
    @ParameterizedTest
    @RandomSeedSource({0x0fdbL, 0x5ca1eL, 123456L})
    void decayingNodesMeasuredAgainstRepairCandidateSet(final long seed)
            throws ExecutionException, InterruptedException, TimeoutException {
        final RepairCandidateTally churnTally = new RepairCandidateTally("churn at population 13, 500 rounds");
        tallyChurn(seed, 13, 500, churnTally);
        churnTally.log(seed);

        // Second phase on a freshly emptied subspace: deletes only, taking the population from 30 down to 2.
        db.run(transaction -> {
            transaction.clear(getSubspace().range());
            return null;
        });
        final RepairCandidateTally shrinkTally = new RepairCandidateTally("delete only, population 30 down to 2");
        tallyDeleteOnly(seed, 30, shrinkTally);
        shrinkTally.log(seed);

        assertThat(churnTally.deletes).as("the churn phase must have performed deletes").isPositive();
        assertThat(shrinkTally.deletes).as("the delete-only phase must have performed deletes").isPositive();
    }

    /** Holds the population constant, tallying each delete against the candidate set it should have used. */
    private void tallyChurn(final long seed, final int population, final int numRounds,
                            @Nonnull final RepairCandidateTally tally)
            throws ExecutionException, InterruptedException, TimeoutException {
        final Random random = new Random(seed);
        final HNSW hnsw = newCentroidStyleHnsw();
        final List<PrimaryKeyAndVector> live = new ArrayList<>();
        int nextKey = 0;

        for (int i = 0; i < population; i++) {
            final PrimaryKeyAndVector record = clusteredVector(random, nextKey++);
            runAsyncToSync(db, tr -> hnsw.insert(tr, record.primaryKey(), record.vector(), null));
            live.add(record);
        }

        for (int round = 0; round < numRounds; round++) {
            final Map<Tuple, List<Tuple>> before = readLayerZeroAdjacency();
            final PrimaryKeyAndVector victim = CommonTestHelpers.pickRandomVectors(random, live, 1).get(0);
            runAsyncToSync(db, tr -> hnsw.delete(tr, victim.primaryKey()));
            live.remove(victim);
            // Snapshot before the insert, so what the repair did is not confused with what the insert did.
            tally.observe(round, victim.primaryKey(), before, readLayerZeroAdjacency());

            final PrimaryKeyAndVector fresh = clusteredVector(random, nextKey++);
            runAsyncToSync(db, tr -> hnsw.insert(tr, fresh.primaryKey(), fresh.vector(), null));
            live.add(fresh);
        }
    }

    /** Never inserts after the initial build, so every change to the graph is attributable to deletion repair. */
    private void tallyDeleteOnly(final long seed, final int population, @Nonnull final RepairCandidateTally tally)
            throws ExecutionException, InterruptedException, TimeoutException {
        final Random random = new Random(seed);
        final HNSW hnsw = newCentroidStyleHnsw();
        final List<PrimaryKeyAndVector> live = new ArrayList<>();

        for (int i = 0; i < population; i++) {
            final PrimaryKeyAndVector record = clusteredVector(random, i);
            runAsyncToSync(db, tr -> hnsw.insert(tr, record.primaryKey(), record.vector(), null));
            live.add(record);
        }

        int round = 0;
        while (live.size() > 2) {
            final Map<Tuple, List<Tuple>> before = readLayerZeroAdjacency();
            final PrimaryKeyAndVector victim = CommonTestHelpers.pickRandomVectors(random, live, 1).get(0);
            runAsyncToSync(db, tr -> hnsw.delete(tr, victim.primaryKey()));
            live.remove(victim);
            tally.observe(round, victim.primaryKey(), before, readLayerZeroAdjacency());
            round++;
        }
    }

    /**
     * Accumulates, over a run of deletes, how the nodes that lose an edge to the deleted node relate to the repair's
     * candidate set, and what the repair handed back to them. Purely a function of the layer-0 adjacency before and
     * after each delete, so it needs no instrumentation of the delete path itself.
     */
    private static final class RepairCandidateTally {
        @Nonnull
        private final String label;
        private int deletes;
        private int sumPOut;
        private int sumPIn;
        private int sumPInInHop2;
        private int sumPInNotInHop2;
        private int sumHop2;
        private int sumHop2GivenNewOutEdge;
        private int deletesWithSomePInOutsideHop2;
        private int keptDanglingInPInNotInHop2;
        private int keptDanglingInPInInHop2;
        private int usableOutDegreeDrops;
        private int dropsWhileInHop2;
        private int dropsWhileNotInHop2;
        private int dropsToZero;
        private int zeroReachedFromOne;
        private int zeroReachedFromAboveOne;
        private int zeroWhileInPOut;
        private int zeroWhileInPIn;
        private int zeroWhileInHop2;
        private int zeroWhileNotInHop2;
        private int zeroWithNewOutEdge;
        private int zeroWithNewInEdge;
        private int nodesAlreadyAtZero;
        private int nodeObservations;
        private int sumUsableOutDegree;
        private int sumApparentOutDegree;
        private int minUsableOutDegree = Integer.MAX_VALUE;
        private int sumNewOutEdges;
        @Nonnull
        private final Map<Tuple, List<String>> historyByNode = new LinkedHashMap<>();
        @Nonnull
        private final Set<Tuple> nodesThatReachedZero = new LinkedHashSet<>();
        @Nonnull
        private final List<String> zeroEvents = new ArrayList<>();

        RepairCandidateTally(@Nonnull final String label) {
            this.label = label;
        }

        void observe(final int round, @Nonnull final Tuple deleted,
                     @Nonnull final Map<Tuple, List<Tuple>> before,
                     @Nonnull final Map<Tuple, List<Tuple>> after) {
            deletes++;
            final Set<Tuple> pOut = existingNeighbors(before, deleted);
            final Set<Tuple> pIn = nodesPointingAt(before, deleted);
            final Set<Tuple> hop2 = hop2(before, deleted, pOut);
            sumPOut += pOut.size();
            sumPIn += pIn.size();
            sumHop2 += hop2.size();

            int pInInHop2 = 0;
            for (final Tuple node : pIn) {
                final boolean stillPointsAtDeleted = after.getOrDefault(node, ImmutableList.of()).contains(deleted);
                if (hop2.contains(node)) {
                    pInInHop2++;
                    if (stillPointsAtDeleted) {
                        keptDanglingInPInInHop2++;
                    }
                } else if (stillPointsAtDeleted) {
                    keptDanglingInPInNotInHop2++;
                }
            }
            sumPInInHop2 += pInInHop2;
            sumPInNotInHop2 += pIn.size() - pInInHop2;
            if (pIn.size() > pInInHop2) {
                deletesWithSomePInOutsideHop2++;
            }

            for (final Tuple candidate : hop2) {
                if (!newNeighbors(before, after, candidate).isEmpty()) {
                    sumHop2GivenNewOutEdge++;
                }
            }

            // Density and degree, measured over every node the delete left behind.
            for (final Map.Entry<Tuple, List<Tuple>> entry : after.entrySet()) {
                nodeObservations++;
                final int usable = countUsable(after, entry.getValue());
                sumUsableOutDegree += usable;
                sumApparentOutDegree += entry.getValue().size();
                minUsableOutDegree = Math.min(minUsableOutDegree, usable);
                sumNewOutEdges += newNeighbors(before, after, entry.getKey()).size();
            }

            for (final Map.Entry<Tuple, List<Tuple>> entry : before.entrySet()) {
                final Tuple node = entry.getKey();
                if (node.equals(deleted) || !after.containsKey(node)) {
                    continue;
                }
                final int usableBefore = countUsable(before, entry.getValue());
                final int usableAfter = countUsable(after, after.get(node));
                if (usableBefore == 0) {
                    nodesAlreadyAtZero++;
                }
                if (usableBefore == usableAfter) {
                    continue;
                }
                final Set<Tuple> newOut = newNeighbors(before, after, node);
                final Set<Tuple> newIn = newInEdges(before, after, node);
                final String line = String.format("r%d usable %d->%d inPOut=%b inPIn=%b inHop2=%b newOut=%s newIn=%s"
                                + " deleted=%s",
                        round, usableBefore, usableAfter, pOut.contains(node), pIn.contains(node),
                        hop2.contains(node), newOut, newIn, deleted);
                historyByNode.computeIfAbsent(node, ignored -> new ArrayList<>()).add(line);
                if (usableAfter >= usableBefore) {
                    continue;
                }
                usableOutDegreeDrops++;
                if (hop2.contains(node)) {
                    dropsWhileInHop2++;
                } else {
                    dropsWhileNotInHop2++;
                }
                if (usableAfter > 0) {
                    continue;
                }
                dropsToZero++;
                if (usableBefore == 1) {
                    zeroReachedFromOne++;
                } else {
                    zeroReachedFromAboveOne++;
                }
                if (pOut.contains(node)) {
                    zeroWhileInPOut++;
                }
                if (pIn.contains(node)) {
                    zeroWhileInPIn++;
                }
                if (hop2.contains(node)) {
                    zeroWhileInHop2++;
                } else {
                    zeroWhileNotInHop2++;
                }
                if (!newOut.isEmpty()) {
                    zeroWithNewOutEdge++;
                }
                if (!newIn.isEmpty()) {
                    zeroWithNewInEdge++;
                }
                nodesThatReachedZero.add(node);
                zeroEvents.add("node=" + node + " " + line + " |pOut|=" + pOut.size() + " |pIn|=" + pIn.size()
                        + " |hop2|=" + hop2.size() + " hop2=" + hop2 + " outEdgesBefore=" + entry.getValue()
                        + " outEdgesAfter=" + after.get(node));
            }
        }

        void log(final long seed) {
            logger.warn("seed={} phase=\"{}\": deletes={}, sum|pOut|={} (avg={}), sum|pIn|={} (avg={}),"
                            + " sum|pIn AND hop2|={} (avg={}), sum|pIn MINUS hop2|={} (avg={}),"
                            + " deletesWithSomePInOutsideHop2={}, sum|hop2|={} (avg={}),"
                            + " hop2NodesGivenANewOutEdge={} (avg={})",
                    seed, label, deletes, sumPOut, average(sumPOut), sumPIn, average(sumPIn),
                    sumPInInHop2, average(sumPInInHop2), sumPInNotInHop2, average(sumPInNotInHop2),
                    deletesWithSomePInOutsideHop2, sumHop2, average(sumHop2),
                    sumHop2GivenNewOutEdge, average(sumHop2GivenNewOutEdge));
            logger.warn("seed={} phase=\"{}\": danglingRefsKeptToTheDeletedNode: byPInMinusHop2={} (|pIn MINUS"
                            + " hop2|={}), byPInAndHop2={} (expected 0)",
                    seed, label, keptDanglingInPInNotInHop2, sumPInNotInHop2, keptDanglingInPInInHop2);
            logger.warn("seed={} phase=\"{}\": usableOutDegreeDrops={}, ofWhichInHop2={}, ofWhichNotInHop2={},"
                            + " dropsToZero={}, zeroFrom1={}, zeroFromAbove1={}, atZeroWasInPOut={},"
                            + " atZeroWasInPIn={}, atZeroWasInHop2={}, atZeroWasNotInHop2={},"
                            + " atZeroGotNewOutEdge={}, atZeroGotNewInEdge={}, nodeDeleteObservationsAlreadyAtZero={},"
                            + " distinctNodesThatReachedZero={}",
                    seed, label, usableOutDegreeDrops, dropsWhileInHop2, dropsWhileNotInHop2, dropsToZero,
                    zeroReachedFromOne, zeroReachedFromAboveOne, zeroWhileInPOut, zeroWhileInPIn, zeroWhileInHop2,
                    zeroWhileNotInHop2, zeroWithNewOutEdge, zeroWithNewInEdge, nodesAlreadyAtZero,
                    nodesThatReachedZero.size());
            logger.warn("seed={} phase=\"{}\": nodeObservations={}, meanUsableOutDegree={}, minUsableOutDegree={},"
                            + " meanOutDegreeInclDangling={}, newOutEdgesTotal={}, newOutEdgesPerDelete={}",
                    seed, label, nodeObservations,
                    nodeObservations == 0 ? "n/a" : String.format("%.3f", (double)sumUsableOutDegree / nodeObservations),
                    minUsableOutDegree == Integer.MAX_VALUE ? "n/a" : String.valueOf(minUsableOutDegree),
                    nodeObservations == 0 ? "n/a" : String.format("%.3f", (double)sumApparentOutDegree / nodeObservations),
                    sumNewOutEdges, average(sumNewOutEdges));
            for (final String event : zeroEvents) {
                logger.warn("seed={} phase=\"{}\": REACHED_ZERO {}", seed, label, event);
            }
            for (final Tuple node : nodesThatReachedZero) {
                logger.warn("seed={} phase=\"{}\": HISTORY of {} = {}", seed, label, node,
                        historyByNode.getOrDefault(node, ImmutableList.of()));
            }
        }

        @Nonnull
        private String average(final int sum) {
            return deletes == 0 ? "n/a" : String.format("%.3f", (double)sum / deletes);
        }

        /** Counts the neighbours of a node that still name a node the snapshot contains. */
        private static int countUsable(@Nonnull final Map<Tuple, List<Tuple>> adjacency,
                                      @Nonnull final List<Tuple> neighbors) {
            int usable = 0;
            for (final Tuple neighbor : neighbors) {
                if (adjacency.containsKey(neighbor)) {
                    usable++;
                }
            }
            return usable;
        }

        @Nonnull
        private static Set<Tuple> existingNeighbors(@Nonnull final Map<Tuple, List<Tuple>> adjacency,
                                                    @Nonnull final Tuple key) {
            return adjacency.getOrDefault(key, ImmutableList.of()).stream()
                    .filter(neighbor -> !neighbor.equals(key) && adjacency.containsKey(neighbor))
                    .collect(ImmutableSet.toImmutableSet());
        }

        @Nonnull
        private static Set<Tuple> nodesPointingAt(@Nonnull final Map<Tuple, List<Tuple>> adjacency,
                                                  @Nonnull final Tuple target) {
            return adjacency.entrySet().stream()
                    .filter(entry -> !entry.getKey().equals(target) && entry.getValue().contains(target))
                    .map(Map.Entry::getKey)
                    .collect(ImmutableSet.toImmutableSet());
        }

        /**
         * The repair's candidate set: the deleted node's existing out-neighbours plus <em>their</em> out-neighbours,
         * keeping only nodes the snapshot contains and dropping the deleted node itself. This is what
         * {@code findDeletionRepairCandidates} compiles before sampling; fetches of absent nodes return nothing, so
         * dangling references contribute no candidates.
         */
        @Nonnull
        private static Set<Tuple> hop2(@Nonnull final Map<Tuple, List<Tuple>> adjacency,
                                       @Nonnull final Tuple deleted,
                                       @Nonnull final Set<Tuple> pOut) {
            final Set<Tuple> candidates = new LinkedHashSet<>(pOut);
            for (final Tuple primary : pOut) {
                candidates.addAll(adjacency.getOrDefault(primary, ImmutableList.of()));
            }
            candidates.remove(deleted);
            candidates.retainAll(adjacency.keySet());
            return candidates;
        }

        /** The out-edges a node has after the delete that it did not have before: what the repair gave it. */
        @Nonnull
        private static Set<Tuple> newNeighbors(@Nonnull final Map<Tuple, List<Tuple>> before,
                                               @Nonnull final Map<Tuple, List<Tuple>> after,
                                               @Nonnull final Tuple node) {
            final List<Tuple> previous = before.getOrDefault(node, ImmutableList.of());
            return after.getOrDefault(node, ImmutableList.of()).stream()
                    .filter(neighbor -> !previous.contains(neighbor))
                    .collect(ImmutableSet.toImmutableSet());
        }

        /** The nodes that point at {@code node} after the delete but did not before. */
        @Nonnull
        private static Set<Tuple> newInEdges(@Nonnull final Map<Tuple, List<Tuple>> before,
                                             @Nonnull final Map<Tuple, List<Tuple>> after,
                                             @Nonnull final Tuple node) {
            final Set<Tuple> pointedBefore = nodesPointingAt(before, node);
            return nodesPointingAt(after, node).stream()
                    .filter(source -> !pointedBefore.contains(source))
                    .collect(ImmutableSet.toImmutableSet());
        }
    }

    @Nonnull
    private HNSW newCentroidStyleHnsw() {
        return new HNSW(getSubspace(), TestExecutors.defaultThreadPool(), centroidStyleConfig(),
                new TestHelpers.TestOnWriteListener(), new TestHelpers.TestOnReadListener());
    }

    /**
     * What a delete costs: the node writes, key/value bytes and wall-clock time of the delete transactions alone,
     * with the interleaved inserts excluded by sampling the counters around each delete. The repair rewrites every
     * node whose neighbour list it touches anyway, so the question this answers is how much of the repair's write
     * volume is attributable to handing a neighbour outgoing edges rather than incoming ones only.
     */
    @ParameterizedTest
    @RandomSeedSource({0x0fdbL, 0x5ca1eL, 123456L})
    void deleteWriteCostUnderChurn(final long seed)
            throws ExecutionException, InterruptedException, TimeoutException {
        final Random random = new Random(seed);
        final CountingOnWriteListener writeListener = new CountingOnWriteListener();
        final HNSW hnsw = new HNSW(getSubspace(), TestExecutors.defaultThreadPool(), centroidStyleConfig(),
                writeListener, new TestHelpers.TestOnReadListener());
        final List<PrimaryKeyAndVector> live = new ArrayList<>();
        int nextKey = 0;

        for (int i = 0; i < 13; i++) {
            final PrimaryKeyAndVector record = clusteredVector(random, nextKey++);
            runAsyncToSync(db, tr -> hnsw.insert(tr, record.primaryKey(), record.vector(), null));
            live.add(record);
        }

        final int numRounds = 200;
        long nodeWrites = 0;
        long keyValueWrites = 0;
        long bytesWritten = 0;
        long nanos = 0;
        for (int round = 0; round < numRounds; round++) {
            final PrimaryKeyAndVector victim = CommonTestHelpers.pickRandomVectors(random, live, 1).get(0);
            final long nodeWritesBefore = writeListener.getNodeWrites();
            final long keyValueWritesBefore = writeListener.getKeyValueWrites();
            final long bytesWrittenBefore = writeListener.getBytesWritten();
            final long startNanos = System.nanoTime();
            runAsyncToSync(db, tr -> hnsw.delete(tr, victim.primaryKey()));
            nanos += System.nanoTime() - startNanos;
            nodeWrites += writeListener.getNodeWrites() - nodeWritesBefore;
            keyValueWrites += writeListener.getKeyValueWrites() - keyValueWritesBefore;
            bytesWritten += writeListener.getBytesWritten() - bytesWrittenBefore;
            live.remove(victim);

            final PrimaryKeyAndVector fresh = clusteredVector(random, nextKey++);
            runAsyncToSync(db, tr -> hnsw.insert(tr, fresh.primaryKey(), fresh.vector(), null));
            live.add(fresh);
        }

        logger.warn("seed={}: {} deletes cost nodeWritesPerDelete={}, keyValueWritesPerDelete={},"
                        + " bytesWrittenPerDelete={}, millisPerDelete={}",
                seed, numRounds, String.format("%.3f", (double)nodeWrites / numRounds),
                String.format("%.3f", (double)keyValueWrites / numRounds),
                String.format("%.1f", (double)bytesWritten / numRounds),
                String.format("%.2f", nanos / 1e6d / numRounds));
        assertThat(nodeWrites).as("a delete must write something").isPositive();
    }

    /** Counts what a write actually costs: nodes written, key/value pairs written and bytes written. */
    private static final class CountingOnWriteListener implements OnWriteListener {
        private final AtomicLong nodeWrites = new AtomicLong();
        private final AtomicLong keyValueWrites = new AtomicLong();
        private final AtomicLong bytesWritten = new AtomicLong();

        long getNodeWrites() {
            return nodeWrites.get();
        }

        long getKeyValueWrites() {
            return keyValueWrites.get();
        }

        long getBytesWritten() {
            return bytesWritten.get();
        }

        @Override
        public void onNodeWritten(final int layer, @Nonnull final Node<? extends NodeReference> node) {
            nodeWrites.incrementAndGet();
        }

        @Override
        public void onKeyValueWritten(final int layer, @Nonnull final byte[] key, @Nonnull final byte[] value) {
            keyValueWrites.incrementAndGet();
            bytesWritten.addAndGet((long)key.length + value.length);
        }
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
}
