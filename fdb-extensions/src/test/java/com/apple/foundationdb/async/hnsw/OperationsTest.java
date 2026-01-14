/*
 * OperationsTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2025 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.async.AsyncIterator;
import com.apple.foundationdb.async.hnsw.TestHelpers.PrimaryKeyAndVector;
import com.apple.foundationdb.async.hnsw.TestHelpers.PrimaryKeyVectorAndDistance;
import com.apple.foundationdb.async.hnsw.TestHelpers.TestOnReadListener;
import com.apple.foundationdb.async.hnsw.TestHelpers.TestOnWriteListener;
import com.apple.foundationdb.linear.DoubleRealVector;
import com.apple.foundationdb.linear.HalfRealVector;
import com.apple.foundationdb.linear.Metric;
import com.apple.foundationdb.linear.RealVector;
import com.apple.foundationdb.rabitq.EncodedRealVector;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.test.TestDatabaseExtension;
import com.apple.foundationdb.test.TestExecutors;
import com.apple.foundationdb.test.TestSubspaceExtension;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.RandomSeedSource;
import com.apple.test.RandomizedTestUtils;
import com.apple.test.Tags;
import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.ObjectArrays;
import com.google.common.collect.Sets;
import com.google.common.collect.Streams;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static com.apple.foundationdb.async.hnsw.TestHelpers.orderedByDistances;
import static com.apple.foundationdb.async.hnsw.TestHelpers.randomVectors;
import static com.apple.foundationdb.linear.RealVectorTest.createRandomDoubleVector;
import static com.apple.foundationdb.linear.RealVectorTest.createRandomHalfVector;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.within;

/**
 * Tests testing insert/update/deletes of data into/in/from {@link HNSW}s.
 */
@Execution(ExecutionMode.CONCURRENT)
@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
@Tag(Tags.RequiresFDB)
@Tag(Tags.Slow)
class OperationsTest implements BaseTest {
    private static final Logger logger = LoggerFactory.getLogger(OperationsTest.class);

    @RegisterExtension
    static final TestDatabaseExtension dbExtension = new TestDatabaseExtension();
    @RegisterExtension
    TestSubspaceExtension subspaceExtension = new TestSubspaceExtension(dbExtension);
    @RegisterExtension
    TestSubspaceExtension rtSecondarySubspace = new TestSubspaceExtension(dbExtension);

    @TempDir
    Path tempDir;

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
    @RandomSeedSource({0x0fdbL, 0x5ca1eL, 123456L, 78910L, 1123581321345589L})
    void testCompactSerialization(final long seed) throws Exception {
        final Random random = new Random(seed);
        final int numDimensions = 768;
        final CompactStorageAdapter storageAdapter =
                new CompactStorageAdapter(HNSW.newConfigBuilder().build(numDimensions), CompactNode.factory(),
                        subspaceExtension.getSubspace(), OnWriteListener.NOOP, OnReadListener.NOOP);
        assertThat(storageAdapter.asCompactStorageAdapter()).isSameAs(storageAdapter);
        assertThatThrownBy(storageAdapter::asInliningStorageAdapter).isInstanceOf(VerifyException.class);

        final AbstractNode<NodeReference> originalNode =
                db.run(tr -> {
                    final NodeFactory<NodeReference> nodeFactory = storageAdapter.getNodeFactory();

                    final AbstractNode<NodeReference> randomCompactNode =
                            TestHelpers.createRandomCompactNode(random, nodeFactory, numDimensions, 16);

                    TestHelpers.writeNode(tr, storageAdapter, randomCompactNode, 0);
                    return randomCompactNode;
                });

        db.run(tr -> storageAdapter.fetchNode(tr, StorageTransform.identity(), 0,
                        originalNode.getPrimaryKey())
                .thenAccept(node ->
                        assertThat(node).satisfies(
                                n -> assertThat(n).isInstanceOf(CompactNode.class),
                                n -> assertThat(n.getKind()).isSameAs(NodeKind.COMPACT),
                                n -> assertThat((Object)n.getPrimaryKey()).isEqualTo(originalNode.getPrimaryKey()),
                                n -> assertThat(n.asCompactNode().getVector())
                                        .isEqualTo(originalNode.asCompactNode().getVector()),
                                n -> {
                                    final ArrayList<NodeReference> neighbors =
                                            Lists.newArrayList(node.getNeighbors());
                                    neighbors.sort(Comparator.comparing(NodeReference::getPrimaryKey));
                                    final ArrayList<NodeReference> originalNeighbors =
                                            Lists.newArrayList(originalNode.getNeighbors());
                                    originalNeighbors.sort(Comparator.comparing(NodeReference::getPrimaryKey));
                                    assertThat(neighbors).isEqualTo(originalNeighbors);
                                }
                )).join());

        assertThat(
                TestHelpers.dumpLayer(getDb(), getSubspace(), HNSW.newConfigBuilder()
                        .build(numDimensions), getTempDir(), "debug", 0))
                .isGreaterThan(0);
    }

    @ParameterizedTest
    @RandomSeedSource({0x0fdbL, 0x5ca1eL, 123456L, 78910L, 1123581321345589L})
    void testInliningSerialization(final long seed) throws Exception {
        final Random random = new Random(seed);
        final int numDimensions = 768;
        final InliningStorageAdapter storageAdapter =
                new InliningStorageAdapter(HNSW.newConfigBuilder().build(numDimensions),
                        InliningNode.factory(), subspaceExtension.getSubspace(),
                        OnWriteListener.NOOP, OnReadListener.NOOP);
        assertThat(storageAdapter.asInliningStorageAdapter()).isSameAs(storageAdapter);
        assertThatThrownBy(storageAdapter::asCompactStorageAdapter).isInstanceOf(VerifyException.class);

        final Node<NodeReferenceWithVector> originalNode =
                db.run(tr -> {
                    final NodeFactory<NodeReferenceWithVector> nodeFactory = storageAdapter.getNodeFactory();

                    final AbstractNode<NodeReferenceWithVector> randomInliningNode =
                            TestHelpers.createRandomInliningNode(random, nodeFactory, numDimensions, 16);

                    TestHelpers.writeNode(tr, storageAdapter, randomInliningNode, 1);
                    return randomInliningNode;
                });

        db.run(tr -> storageAdapter.fetchNode(tr, StorageTransform.identity(), 1,
                        originalNode.getPrimaryKey())
                .thenAccept(node ->
                        assertThat(node).satisfies(
                                n -> assertThat(n).isInstanceOf(InliningNode.class),
                                n -> assertThat(n.getKind()).isSameAs(NodeKind.INLINING),
                                n -> assertThat((Object)node.getPrimaryKey()).isEqualTo(originalNode.getPrimaryKey()),
                                n -> {
                                    final ArrayList<NodeReference> neighbors =
                                            Lists.newArrayList(node.getNeighbors());
                                    neighbors.sort(Comparator.comparing(NodeReference::getPrimaryKey)); // should not be necessary the way it is stored
                                    final ArrayList<NodeReference> originalNeighbors =
                                            Lists.newArrayList(originalNode.getNeighbors());
                                    originalNeighbors.sort(Comparator.comparing(NodeReference::getPrimaryKey));
                                    assertThat(neighbors).isEqualTo(originalNeighbors);
                                }
                        )).join());

        assertThat(
                TestHelpers.dumpLayer(getDb(), getSubspace(), HNSW.newConfigBuilder()
                        .setUseInlining(true)
                        .build(numDimensions), getTempDir(), "debug", 1))
                .isGreaterThan(0);
    }

    @Nonnull
    private static Stream<Arguments> differentConfigsAndMetrics() {
        return Streams.concat(differentConfigs(), differentMetrics());
    }

    @Nonnull
    private static Stream<Arguments> differentConfigs() {
        return RandomizedTestUtils.randomSeeds(0xdeadc0deL)
                .flatMap(seed -> Sets.cartesianProduct(ImmutableSet.of(false, true),
                                ImmutableSet.of(false, true),
                                ImmutableSet.of(false, true),
                                ImmutableSet.of(true, false)).stream()
                        .map(arguments -> Arguments.of(ObjectArrays.concat(seed,
                                new Object[] {HNSW.newConfigBuilder()
                                        .setMetric(Metric.EUCLIDEAN_METRIC)
                                        .setUseInlining(arguments.get(0))
                                        .setEfRepair(64)
                                        .setExtendCandidates(arguments.get(1))
                                        .setKeepPrunedConnections(arguments.get(2))
                                        .setUseRaBitQ(arguments.get(3))
                                        .setRaBitQNumExBits(5)
                                        .setSampleVectorStatsProbability(1.0d)
                                        .setMaintainStatsProbability(0.1d)
                                        .setStatsThreshold(100)
                                        .setM(16)
                                        .setMMax(32)
                                        .setMMax0(64)
                                        .build(128)}))));
    }

    @Nonnull
    private static Stream<Arguments> differentMetrics() {
        return RandomizedTestUtils.randomSeeds(0xdeadc0deL)
                .flatMap(seed -> Sets.cartesianProduct(ImmutableSet.of(Metric.COSINE_METRIC,
                                Metric.EUCLIDEAN_METRIC, Metric.EUCLIDEAN_SQUARE_METRIC)).stream()
                        .map(arguments -> Arguments.of(ObjectArrays.concat(seed,
                                new Object[] {HNSW.newConfigBuilder()
                                        .setMetric(arguments.get(0))
                                        .setUseInlining(false)
                                        .setEfRepair(64)
                                        .setExtendCandidates(false)
                                        .setKeepPrunedConnections(false)
                                        .setUseRaBitQ(false)
                                        .setRaBitQNumExBits(5)
                                        .setSampleVectorStatsProbability(1.0d)
                                        .setMaintainStatsProbability(0.1d)
                                        .setStatsThreshold(100)
                                        .setM(16)
                                        .setMMax(32)
                                        .setMMax0(64)
                                        .build(128)}))));
    }

    @ExtendWith(TestHelpers.DumpLayersIfFailure.class)
    @ParameterizedTest
    @MethodSource("differentConfigsAndMetrics")
    void testBasicInsert(final long seed, final Config config) {
        final Random random = new Random(seed);
        final int size = 1000;
        final TestOnWriteListener onWriteListener = new TestOnWriteListener();
        final TestOnReadListener onReadListener = new TestOnReadListener();

        final HNSW hnsw = new HNSW(subspaceExtension.getSubspace(), TestExecutors.defaultThreadPool(), config,
                onWriteListener, onReadListener);

        final int k = 50;
        final List<PrimaryKeyAndVector> insertedData = randomVectors(random, config.getNumDimensions(), size);

        for (int i = 0; i < size;) {
            i += TestHelpers.basicInsertBatch(getDb(), hnsw, 100, i,
                    (tr, nextId) -> insertedData.get(Math.toIntExact(nextId))).size();
        }

        final HalfRealVector queryVector = createRandomHalfVector(random, config.getNumDimensions());

        //
        // Attempt to mutate some records by updating them using the same primary keys but different random vectors.
        // This should not fail but should be silently ignored. If this succeeds, the following searches will all
        // return records that are not aligned with recordsOrderedByDistance.
        //
        for (int i = 0; i < 100; ) {
            i += TestHelpers.basicInsertBatch(getDb(), hnsw, 100, 0,
                    (tr, ignored) -> {
                        final var primaryKey = TestHelpers.createPrimaryKey(random.nextInt(1000));
                        final HalfRealVector dataVector = createRandomHalfVector(random, config.getNumDimensions());
                        return new PrimaryKeyAndVector(primaryKey, dataVector);
                    }).size();
        }

        onReadListener.reset();
        final long beginTs = System.nanoTime();
        final List<? extends ResultEntry> results =
                db.run(tr ->
                        hnsw.kNearestNeighborsSearch(tr, k, 100, true, queryVector).join());
        final long endTs = System.nanoTime();

        final ImmutableSet<Tuple> trueNN =
                orderedByDistances(config.getMetric(), insertedData, queryVector).stream()
                        .limit(k)
                        .map(PrimaryKeyVectorAndDistance::getPrimaryKey)
                        .collect(ImmutableSet.toImmutableSet());

        int recallCount = 0;
        for (ResultEntry resultEntry : results) {
            logger.info("nodeId ={} at distance={}", resultEntry.getPrimaryKey().getLong(0),
                    resultEntry.getDistance());
            if (trueNN.contains(resultEntry.getPrimaryKey())) {
                recallCount++;
            }
        }
        final double recall = (double)recallCount / (double)k;
        logger.info("search transaction took elapsedTime={}ms; read nodes={}, read bytes={}, recall={}",
                TimeUnit.NANOSECONDS.toMillis(endTs - beginTs),
                onReadListener.getNodeCountByLayer(), onReadListener.getBytesReadByLayer(),
                String.format(Locale.ROOT, "%.2f", recall * 100.0d));
        assertThat(recall).isGreaterThan(0.9);

        final Set<Long> insertedIds =
                LongStream.range(0, 1000)
                        .boxed()
                        .collect(Collectors.toSet());

        final Set<Long> readIds = Sets.newHashSet();
        TestHelpers.scanLayer(getDb(), getSubspace(), config, 0, 100,
                node ->
                        assertThat(readIds.add(node.getPrimaryKey().getLong(0))).isTrue());
        assertThat(readIds).isEqualTo(insertedIds);

        readIds.clear();
        TestHelpers.scanLayer(getDb(), getSubspace(), config, 1, 100,
                node -> assertThat(readIds.add(node.getPrimaryKey().getLong(0))).isTrue());
        assertThat(readIds.size()).isBetween(10, 100);
    }

    @ExtendWith(TestHelpers.DumpLayersIfFailure.class)
    @ParameterizedTest
    @MethodSource("differentMetrics")
    void testBasicInsertRingSearch(final long seed, final Config config) {
        final Random random = new Random(seed);
        final Metric metric = config.getMetric();
        final int size = 1000;
        final TestOnWriteListener onWriteListener = new TestOnWriteListener();
        final TestOnReadListener onReadListener = new TestOnReadListener();

        final HNSW hnsw = new HNSW(subspaceExtension.getSubspace(), TestExecutors.defaultThreadPool(), config,
                onWriteListener, onReadListener);

        final int k = 30;
        final List<PrimaryKeyAndVector> insertedData = randomVectors(random, config.getNumDimensions(), size);

        for (int i = 0; i < size;) {
            i += TestHelpers.basicInsertBatch(getDb(), hnsw, 100, i,
                    (tr, nextId) -> insertedData.get(Math.toIntExact(nextId))).size();
        }

        final double radius = 1d;
        final HalfRealVector queryVector = createRandomHalfVector(random, config.getNumDimensions());

        onReadListener.reset();
        final long beginTs = System.nanoTime();
        final List<? extends ResultEntry> results =
                db.run(tr ->
                        hnsw.kNearestNeighborsRingSearch(tr, k, 100, true, queryVector, radius).join());
        final long endTs = System.nanoTime();

        final ImmutableSet<Tuple> trueNN =
                orderedByDistances(TestHelpers.ringDistance(metric, radius), insertedData, queryVector).stream()
                        .limit(k)
                        .map(PrimaryKeyVectorAndDistance::getPrimaryKey)
                        .collect(ImmutableSet.toImmutableSet());

        int recallCount = 0;
        for (ResultEntry resultEntry : results) {
            if (trueNN.contains(resultEntry.getPrimaryKey())) {
                recallCount++;
            }
        }
        final double recall = (double)recallCount / (double)k;
        logger.info("ring search transaction took elapsedTime={}ms; read nodes={}, read bytes={}, recall={}",
                TimeUnit.NANOSECONDS.toMillis(endTs - beginTs),
                onReadListener.getNodeCountByLayer(), onReadListener.getBytesReadByLayer(),
                String.format(Locale.ROOT, "%.2f", recall * 100.0d));
        assertThat(recall).isGreaterThan(0.9);
    }

    @ParameterizedTest
    @MethodSource("differentConfigsAndMetrics")
    void testBasicInsertDelete(final long seed, final Config config) {
        final Random random = new Random(seed);
        final int size = 1000;
        final TestOnWriteListener onWriteListener = new TestOnWriteListener();
        final TestOnReadListener onReadListener = new TestOnReadListener();

        final HNSW hnsw = new HNSW(subspaceExtension.getSubspace(), TestExecutors.defaultThreadPool(), config,
                onWriteListener, onReadListener);

        final int k = 50;
        final List<PrimaryKeyAndVector> insertedData = randomVectors(random, config.getNumDimensions(), size);

        for (int i = 0; i < size;) {
            i += TestHelpers.basicInsertBatch(getDb(), hnsw, 100, i,
                    (tr, nextId) -> insertedData.get(Math.toIntExact(nextId))).size();
        }

        final int numVectorsPerDeleteBatch = 50;
        List<PrimaryKeyAndVector> remainingData = insertedData;
        do {
            final List<PrimaryKeyAndVector> toBeDeleted =
                    TestHelpers.pickRandomVectors(random, remainingData, numVectorsPerDeleteBatch);

            final long beginTs = System.nanoTime();
            db.run(tr -> {
                onWriteListener.reset();
                onReadListener.reset();

                for (final PrimaryKeyAndVector primaryKeyAndVector : toBeDeleted) {
                    hnsw.delete(tr, primaryKeyAndVector.getPrimaryKey()).join();
                }
                return null;
            });
            long endTs = System.nanoTime();

            assertThat(onWriteListener.getDeleteCountByLayer().get(0)).isEqualTo(toBeDeleted.size());

            logger.info("delete transaction of {} records after {} records took elapsedTime={}ms; read nodes={}, read bytes={}",
                    numVectorsPerDeleteBatch,
                    size - remainingData.size(),
                    TimeUnit.NANOSECONDS.toMillis(endTs - beginTs),
                    onReadListener.getNodeCountByLayer(), onReadListener.getBytesReadByLayer());

            final Set<PrimaryKeyAndVector> deletedSet = toBeDeleted.stream().collect(ImmutableSet.toImmutableSet());
            remainingData = remainingData.stream()
                    .filter(vector -> !deletedSet.contains(vector))
                    .collect(ImmutableList.toImmutableList());

            if (!remainingData.isEmpty()) {
                final HalfRealVector queryVector = createRandomHalfVector(random, config.getNumDimensions());
                final ImmutableSet<Tuple> trueNN =
                        orderedByDistances(config.getMetric(), remainingData, queryVector).stream()
                                .limit(k)
                                .map(PrimaryKeyVectorAndDistance::getPrimaryKey)
                                .collect(ImmutableSet.toImmutableSet());
                onReadListener.reset();

                final long beginTsQuery = System.nanoTime();
                final List<? extends ResultEntry> results =
                        db.run(tr ->
                                hnsw.kNearestNeighborsSearch(tr, k, 100, true, queryVector).join());
                final long endTsQuery = System.nanoTime();

                int recallCount = 0;
                for (ResultEntry resultEntry : results) {
                    if (trueNN.contains(resultEntry.getPrimaryKey())) {
                        recallCount++;
                    }
                }
                final double recall = (double)recallCount / (double)trueNN.size();

                logger.info("search transaction after delete of {} records took elapsedTime={}ms; read nodes={}, read bytes={}, recall={}",
                        size - remainingData.size(),
                        TimeUnit.NANOSECONDS.toMillis(endTsQuery - beginTsQuery),
                        onReadListener.getNodeCountByLayer(), onReadListener.getBytesReadByLayer(),
                        String.format(Locale.ROOT, "%.2f", recall * 100.0d));

                assertThat(recall).isGreaterThan(0.9);

                final long remainingNumNodes = TestHelpers.countNodesOnLayer(getDb(), getSubspace(), config, 0);
                assertThat(remainingNumNodes).isEqualTo(remainingData.size());
            }
        } while (!remainingData.isEmpty());

        final var accessInfo =
                db.run(transaction -> StorageAdapter.fetchAccessInfo(hnsw.getConfig(),
                        transaction, hnsw.getSubspace(), OnReadListener.NOOP).join());
        assertThat(accessInfo).isNull();
    }

    @ParameterizedTest()
    @RandomSeedSource({0x0fdbL, 0x5ca1eL, 123456L, 78910L, 1123581321345589L})
    void testBasicInsertWithRaBitQEncodings(final long seed) {
        final Random random = new Random(seed);
        final Metric metric = Metric.EUCLIDEAN_METRIC;

        final int numDimensions = 128;
        final HNSW hnsw = new HNSW(subspaceExtension.getSubspace(), TestExecutors.defaultThreadPool(),
                HNSW.newConfigBuilder()
                        .setMetric(metric)
                        .setUseRaBitQ(true)
                        .setRaBitQNumExBits(5)
                        .setSampleVectorStatsProbability(1.0d) // every vector is sampled
                        .setMaintainStatsProbability(1.0d) // for every vector we maintain the stats
                        .setStatsThreshold(950) // after 950 vectors we enable RaBitQ
                        .setM(32)
                        .setMMax(32)
                        .setMMax0(64)
                        .build(numDimensions),
                new TestOnWriteListener(), new TestOnReadListener());

        final int numRecords = 1000;
        final int k = 499;
        final List<PrimaryKeyAndVector> insertedData = randomVectors(random, numDimensions, numRecords);
        for (int i = 0; i < numRecords;) {
            i += TestHelpers.basicInsertBatch(getDb(), hnsw, 100, i,
                    (tr, nextId) -> insertedData.get(Math.toIntExact(nextId))).size();
        }

        final DoubleRealVector queryVector = createRandomDoubleVector(random, numDimensions);

        //
        // If we fetch the current state back from the db, some vectors are regular vectors and some vectors are
        // RaBitQ encoded. Since that information is not surfaced through the API, we need to scan layer 0, get
        // all vectors directly from disk (encoded/not-encoded, transformed/not-transformed) in order to check
        // that transformations/reconstructions are applied properly.
        //
        final Map<Tuple, RealVector> fromDBMap = Maps.newHashMap();
        TestHelpers.scanLayer(getDb(), getSubspace(), hnsw.getConfig(), 0, 100,
                node -> fromDBMap.put(node.getPrimaryKey(),
                        node.asCompactNode().getVector().getUnderlyingVector()));

        //
        // Still run a kNN search to make sure that recall is satisfactory.
        //
        final List<? extends ResultEntry> results =
                db.run(tr ->
                        hnsw.kNearestNeighborsSearch(tr, k, 500, true, queryVector).join());

        final ImmutableSet<Tuple> trueNN =
                orderedByDistances(metric, insertedData, queryVector)
                        .stream()
                        .map(PrimaryKeyAndVector::getPrimaryKey)
                        .collect(ImmutableSet.toImmutableSet());

        int recallCount = 0;
        int exactVectorCount = 0;
        int encodedVectorCount = 0;
        for (final ResultEntry resultEntry : results) {
            if (trueNN.contains(resultEntry.getPrimaryKey())) {
                recallCount ++;
            }

            final RealVector originalVector =
                    insertedData.get(Math.toIntExact(resultEntry.getPrimaryKey().getLong(0))).getVector();
            assertThat(originalVector).isNotNull();
            final RealVector fromDBVector = fromDBMap.get(resultEntry.getPrimaryKey());
            assertThat(fromDBVector).isNotNull();
            if (!(fromDBVector instanceof EncodedRealVector)) {
                assertThat(originalVector).isEqualTo(fromDBVector);
                exactVectorCount ++;
                final double distance = metric.distance(originalVector,
                        Objects.requireNonNull(resultEntry.getVector()));
                assertThat(distance).isCloseTo(0.0d, within(2E-12));
            } else {
                encodedVectorCount ++;
                final double distance = metric.distance(originalVector,
                        Objects.requireNonNull(resultEntry.getVector()).toDoubleRealVector());
                assertThat(distance).isCloseTo(0.0d, within(20.0d));
            }
        }
        final double recall = (double)recallCount / (double)k;
        assertThat(recall).isGreaterThan(0.9);
        // must have both kinds
        assertThat(exactVectorCount).isGreaterThan(0);
        assertThat(encodedVectorCount).isGreaterThan(0);
    }

    @ParameterizedTest
    @MethodSource("differentMetrics")
    void testOrderByDistance(final long seed, final Config config) {
        final Random random = new Random(seed);
        final int size = 1000;
        final TestOnWriteListener onWriteListener = new TestOnWriteListener();
        final TestOnReadListener onReadListener = new TestOnReadListener();

        final HNSW hnsw = new HNSW(subspaceExtension.getSubspace(), TestExecutors.defaultThreadPool(), config,
                onWriteListener, onReadListener);

        final List<PrimaryKeyAndVector> insertedData = randomVectors(random, config.getNumDimensions(), size);

        for (int i = 0; i < size;) {
            i += TestHelpers.basicInsertBatch(getDb(), hnsw, 100, i,
                    (tr, nextId) -> insertedData.get(Math.toIntExact(nextId))).size();
        }

        final int skip = 100;
        final HalfRealVector queryVector = createRandomHalfVector(random, config.getNumDimensions());

        final NavigableSet<PrimaryKeyVectorAndDistance> orderedByDistances =
                TestHelpers.orderedByDistances(config.getMetric(), insertedData, queryVector);
        final PrimaryKeyVectorAndDistance discriminator = Iterables.get(orderedByDistances, skip - 1);

        onReadListener.reset();

        final List<ResultEntry> results =
                db.run(tr -> {
                    final AsyncIterator<ResultEntry> it =
                            hnsw.orderByDistance(tr, 100, 1000, false,
                                    queryVector, discriminator.getDistance(), discriminator.getPrimaryKey());
                    final ImmutableList.Builder<ResultEntry> resultsBuilder = ImmutableList.builder();
                    while (it.hasNext()) {
                        resultsBuilder.add(it.next());
                    }
                    return resultsBuilder.build();
                });

        int numInversions = 0;
        for (int i = 1; i < results.size(); i++) {
            final ResultEntry previous = results.get(i - 1);
            final ResultEntry current = results.get(i);

            if (previous.getDistance() > current.getDistance()) {
                numInversions++;
            }
        }

        if (logger.isInfoEnabled()) {
            logger.info("total={}; inverted={}", results.size(), numInversions);
            assertThat(results).hasSizeBetween(size - skip - 5, size - skip);
            assertThat(numInversions).isLessThan(10);
        }

        final List<Tuple> groundTruth =
                orderedByDistances.stream()
                        .map(PrimaryKeyAndVector::getPrimaryKey)
                        .collect(ImmutableList.toImmutableList());

        final ImmutableSet<Tuple> groundTruthExpected =
                groundTruth.stream()
                        .skip(100)
                        .collect(ImmutableSet.toImmutableSet());

        final ImmutableSet<Tuple> resultIds =
                results.stream()
                        .map(ResultEntry::getPrimaryKey)
                        .collect(ImmutableSet.toImmutableSet());

        final Set<Tuple> commonIds = Sets.intersection(groundTruthExpected, resultIds);
        final int recallCount = commonIds.size();
        final double recall = (double)recallCount / groundTruthExpected.size();
        assertThat(recall).isGreaterThan(0.9d);

        assertThat(OrderQuality.score(ImmutableList.copyOf(resultIds),
                groundTruth, skip, 50))
                .satisfies(
                        quality -> assertThat(quality.getQuality()).isGreaterThan(0.9),
                        quality -> assertThat(quality.getContigScore()).isGreaterThan(0.9));
    }
}
