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
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.async.common.BaseTest;
import com.apple.foundationdb.async.common.CommonTestHelpers;
import com.apple.foundationdb.async.common.PrimaryKeyAndVector;
import com.apple.foundationdb.async.common.PrimaryKeyVectorAndDistance;
import com.apple.foundationdb.async.common.ResultEntry;
import com.apple.foundationdb.async.common.StorageTransform;
import com.apple.foundationdb.async.hnsw.TestHelpers.TestOnReadListener;
import com.apple.foundationdb.async.hnsw.TestHelpers.TestOnWriteListener;
import com.apple.foundationdb.linear.DoubleRealVector;
import com.apple.foundationdb.linear.FhtKacRotator;
import com.apple.foundationdb.linear.HalfRealVector;
import com.apple.foundationdb.linear.Metric;
import com.apple.foundationdb.linear.RealVector;
import com.apple.foundationdb.linear.RealVectorTest;
import com.apple.foundationdb.linear.Transformed;
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
import org.assertj.core.api.Assertions;
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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static com.apple.foundationdb.async.common.CommonTestHelpers.orderedByDistances;
import static com.apple.foundationdb.async.common.CommonTestHelpers.randomVectors;
import static com.apple.foundationdb.async.common.CommonTestHelpers.runAsyncToSync;
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

        runAsyncToSync(db, tr -> storageAdapter.fetchNode(tr, StorageTransform.identity(), 0,
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
                )));

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

        runAsyncToSync(db, tr -> storageAdapter.fetchNode(tr, StorageTransform.identity(), 1,
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
                        )));

        assertThat(
                TestHelpers.dumpLayer(getDb(), getSubspace(), HNSW.newConfigBuilder()
                        .setUseInlining(true)
                        .build(numDimensions), getTempDir(), "debug", 1))
                .isGreaterThan(0);
    }

    @Nonnull
    private static Stream<Arguments> seedAndIsNormalized() {
        return RandomizedTestUtils.randomSeeds(0xdeadc0deL, 0x1234567890L)
                .flatMap(seed -> ImmutableSet.of(false, true).stream()
                        .map(isNormalized -> Arguments.of(seed, isNormalized)));
    }

    @ParameterizedTest
    @MethodSource("seedAndIsNormalized")
    void testStorageTransform(final long seed, final boolean isNormalized) {
        final Random random = new Random(seed);
        final int numDimensions = random.nextInt(128);
        final FhtKacRotator rotator = new FhtKacRotator(seed, numDimensions, 10);
        final RealVector translation = RealVectorTest.createRandomDoubleVector(random, numDimensions);

        final StorageTransform storageTransform =
                new StorageTransform(rotator, translation, isNormalized);

        Assertions.assertThat(storageTransform.getNumDimensions()).isEqualTo(numDimensions);

        final RealVector x = RealVectorTest.createRandomDoubleVector(random, numDimensions);

        if (isNormalized) {
            final RealVector y = storageTransform.apply(x);
            final RealVector z = storageTransform.invertedApply(y);

            Assertions.assertThat(Metric.EUCLIDEAN_METRIC.distance(x.normalize(), z))
                    .isCloseTo(0, within(2E-10));
        } else {
            final RealVector y = storageTransform.apply(x);
            final RealVector z = storageTransform.invertedApply(y);

            Assertions.assertThat(Metric.EUCLIDEAN_METRIC.distance(x, z)).isCloseTo(0, within(2E-10));
        }
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
    void testBasicInsert(final long seed, final Config config) throws ExecutionException, InterruptedException, TimeoutException {
        final Random random = new Random(seed);
        final int size = 1000;
        final TestOnWriteListener onWriteListener = new TestOnWriteListener();
        final TestOnReadListener onReadListener = new TestOnReadListener();

        final HNSW hnsw = new HNSW(subspaceExtension.getSubspace(), TestExecutors.defaultThreadPool(), config,
                onWriteListener, onReadListener);

        final int k = 50;
        final List<PrimaryKeyAndVector> insertedData = randomVectors(random, config.numDimensions(), size);

        for (int i = 0; i < size;) {
            i += TestHelpers.basicInsertBatch(getDb(), hnsw, 100, i,
                    (tr, nextId) -> insertedData.get(Math.toIntExact(nextId))).size();
        }

        final HalfRealVector queryVector = createRandomHalfVector(random, config.numDimensions());

        //
        // Attempt to mutate some records by updating them using the same primary keys but different random vectors.
        // This should not fail but should be silently ignored. If this succeeds, the following searches will all
        // return records that are not aligned with recordsOrderedByDistance.
        //
        for (int i = 0; i < 100; ) {
            i += TestHelpers.basicInsertBatch(getDb(), hnsw, 100, 0,
                    (tr, ignored) -> {
                        final Tuple primaryKey = CommonTestHelpers.createPrimaryKey(random.nextInt(1000));
                        final HalfRealVector dataVector = createRandomHalfVector(random, config.numDimensions());
                        return new PrimaryKeyAndVector(primaryKey, dataVector);
                    }).size();
        }

        onReadListener.reset();
        final long beginTs = System.nanoTime();
        final List<? extends ResultEntry> results =
                runAsyncToSync(db, tr -> hnsw.kNearestNeighborsSearch(tr, k, 100, true, queryVector));
        final long endTs = System.nanoTime();

        final ImmutableSet<Tuple> trueNN =
                orderedByDistances(config.metric(), insertedData, queryVector).stream()
                        .limit(k)
                        .map(PrimaryKeyVectorAndDistance::primaryKey)
                        .collect(ImmutableSet.toImmutableSet());

        int recallCount = 0;
        for (ResultEntry resultEntry : results) {
            logger.info("nodeId ={} at distance={}", resultEntry.primaryKey().getLong(0),
                    resultEntry.distance());
            if (trueNN.contains(resultEntry.primaryKey())) {
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
    void testBasicInsertRingSearch(final long seed, final Config config) throws ExecutionException, InterruptedException, TimeoutException {
        final Random random = new Random(seed);
        final Metric metric = config.metric();
        final int size = 1000;
        final TestOnWriteListener onWriteListener = new TestOnWriteListener();
        final TestOnReadListener onReadListener = new TestOnReadListener();

        final HNSW hnsw = new HNSW(subspaceExtension.getSubspace(), TestExecutors.defaultThreadPool(), config,
                onWriteListener, onReadListener);

        final int k = 30;
        final List<PrimaryKeyAndVector> insertedData = randomVectors(random, config.numDimensions(), size);

        for (int i = 0; i < size;) {
            i += TestHelpers.basicInsertBatch(getDb(), hnsw, 100, i,
                    (tr, nextId) -> insertedData.get(Math.toIntExact(nextId))).size();
        }

        final double radius = 1d;
        final HalfRealVector queryVector = createRandomHalfVector(random, config.numDimensions());

        onReadListener.reset();
        final long beginTs = System.nanoTime();
        final List<? extends ResultEntry> results =
                runAsyncToSync(db, tr ->
                        hnsw.kNearestNeighborsRingSearch(tr, k, 100, true, queryVector, radius));
        final long endTs = System.nanoTime();

        final ImmutableSet<Tuple> trueNN =
                orderedByDistances(TestHelpers.ringDistance(metric, radius), insertedData, queryVector).stream()
                        .limit(k)
                        .map(PrimaryKeyVectorAndDistance::primaryKey)
                        .collect(ImmutableSet.toImmutableSet());

        int recallCount = 0;
        for (ResultEntry resultEntry : results) {
            if (trueNN.contains(resultEntry.primaryKey())) {
                recallCount++;
            }
        }
        final double recall = (double)recallCount / (double)k;
        logger.info("ring search transaction took elapsedTime={}ms; read nodes={}, read bytes={}, recall={}",
                TimeUnit.NANOSECONDS.toMillis(endTs - beginTs),
                onReadListener.getNodeCountByLayer(), onReadListener.getBytesReadByLayer(),
                String.format(Locale.ROOT, "%.2f", recall * 100.0d));
        assertThat(recall).isGreaterThan(0.8);
    }

    @ParameterizedTest
    @MethodSource("differentConfigsAndMetrics")
    void testBasicInsertDelete(final long seed, final Config config) throws ExecutionException, InterruptedException, TimeoutException {
        final Random random = new Random(seed);
        final int size = 1000;
        final TestOnWriteListener onWriteListener = new TestOnWriteListener();
        final TestOnReadListener onReadListener = new TestOnReadListener();

        final HNSW hnsw = new HNSW(subspaceExtension.getSubspace(), TestExecutors.defaultThreadPool(), config,
                onWriteListener, onReadListener);

        final int k = 50;
        final List<PrimaryKeyAndVector> insertedData = randomVectors(random, config.numDimensions(), size);

        for (int i = 0; i < size;) {
            i += TestHelpers.basicInsertBatch(getDb(), hnsw, 100, i,
                    (tr, nextId) -> insertedData.get(Math.toIntExact(nextId))).size();
        }

        final int numVectorsPerDeleteBatch = 50;
        List<PrimaryKeyAndVector> remainingData = insertedData;
        do {
            final List<PrimaryKeyAndVector> toBeDeleted =
                    CommonTestHelpers.pickRandomVectors(random, remainingData, numVectorsPerDeleteBatch);

            final long beginTs = System.nanoTime();
            runAsyncToSync(db, tr -> {
                onWriteListener.reset();
                onReadListener.reset();

                // Chain the deletes rather than issuing them concurrently: this test is about delete
                // semantics, not about hnsw's handling of concurrent mutations within one transaction.
                CompletableFuture<Void> future = AsyncUtil.DONE;
                for (final PrimaryKeyAndVector primaryKeyAndVector : toBeDeleted) {
                    future = future.thenCompose(ignored -> hnsw.delete(tr, primaryKeyAndVector.primaryKey()));
                }
                return future;
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
                final HalfRealVector queryVector = createRandomHalfVector(random, config.numDimensions());
                final ImmutableSet<Tuple> trueNN =
                        orderedByDistances(config.metric(), remainingData, queryVector).stream()
                                .limit(k)
                                .map(PrimaryKeyVectorAndDistance::primaryKey)
                                .collect(ImmutableSet.toImmutableSet());
                onReadListener.reset();

                final long beginTsQuery = System.nanoTime();
                final List<? extends ResultEntry> results =
                        runAsyncToSync(db, tr -> hnsw.kNearestNeighborsSearch(tr, k, 100, true, queryVector));
                final long endTsQuery = System.nanoTime();

                int recallCount = 0;
                for (ResultEntry resultEntry : results) {
                    if (trueNN.contains(resultEntry.primaryKey())) {
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
                runAsyncToSync(db, transaction -> StorageAdapter.fetchAccessInfo(hnsw.getConfig(),
                        transaction, hnsw.getSubspace(), OnReadListener.NOOP));
        assertThat(accessInfo).isNull();
    }

    @ParameterizedTest()
    @RandomSeedSource({0x0fdbL, 0x5ca1eL, 123456L, 78910L, 1123581321345589L})
    void testBasicInsertWithRaBitQEncodings(final long seed) throws ExecutionException, InterruptedException, TimeoutException {
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
                runAsyncToSync(db, tr -> hnsw.kNearestNeighborsSearch(tr, k, 500, true, queryVector));

        final ImmutableSet<Tuple> trueNN =
                orderedByDistances(metric, insertedData, queryVector)
                        .stream()
                        .map(PrimaryKeyVectorAndDistance::primaryKey)
                        .collect(ImmutableSet.toImmutableSet());

        int recallCount = 0;
        int exactVectorCount = 0;
        int encodedVectorCount = 0;
        for (final ResultEntry resultEntry : results) {
            if (trueNN.contains(resultEntry.primaryKey())) {
                recallCount ++;
            }

            final RealVector originalVector =
                    insertedData.get(Math.toIntExact(resultEntry.primaryKey().getLong(0))).vector();
            assertThat(originalVector).isNotNull();
            final RealVector fromDBVector = fromDBMap.get(resultEntry.primaryKey());
            assertThat(fromDBVector).isNotNull();
            if (!(fromDBVector instanceof EncodedRealVector)) {
                assertThat(originalVector).isEqualTo(fromDBVector);
                exactVectorCount ++;
                final double distance = metric.distance(originalVector,
                        Objects.requireNonNull(resultEntry.vector()));
                assertThat(distance).isCloseTo(0.0d, within(2E-12));
            } else {
                encodedVectorCount ++;
                final double distance = metric.distance(originalVector,
                        Objects.requireNonNull(resultEntry.vector()).toDoubleRealVector());
                assertThat(distance).isCloseTo(0.0d, within(20.0d));
            }
        }
        final double recall = (double)recallCount / (double)k;
        assertThat(recall).isGreaterThan(0.9);
        // must have both kinds
        assertThat(exactVectorCount).isGreaterThan(0);
        assertThat(encodedVectorCount).isGreaterThan(0);
    }

    @Nonnull
    private static Stream<Arguments> seedAndUseInlining() {
        return RandomizedTestUtils.randomSeeds(0x0fdbL, 0x5ca1eL)
                .flatMap(seed -> ImmutableSet.of(false, true).stream()
                        .map(useInlining -> Arguments.of(seed, useInlining)));
    }

    /**
     * Creates a structure that switches to RaBitQ after {@code 200} inserts, so that once more vectors than that are
     * inserted it holds both kinds of nodes: those written before the switch, stored as plain vectors in client
     * coordinates, and those written after it, stored encoded in the rotated and translated storage space.
     */
    @Nonnull
    private HNSW newStructureSwitchingToRaBitQ(final int numDimensions, final boolean useInlining) {
        return new HNSW(subspaceExtension.getSubspace(), TestExecutors.defaultThreadPool(),
                HNSW.newConfigBuilder()
                        .setMetric(Metric.EUCLIDEAN_METRIC)
                        .setUseInlining(useInlining)
                        .setUseRaBitQ(true)
                        .setRaBitQNumExBits(5)
                        .setSampleVectorStatsProbability(1.0d) // every vector is sampled
                        .setMaintainStatsProbability(1.0d) // for every vector we maintain the stats
                        .setStatsThreshold(200) // after 200 vectors we enable RaBitQ
                        .build(numDimensions),
                new TestOnWriteListener(), new TestOnReadListener());
    }

    private void insertAll(@Nonnull final HNSW hnsw, @Nonnull final List<PrimaryKeyAndVector> data)
            throws ExecutionException, InterruptedException, TimeoutException {
        for (int i = 0; i < data.size();) {
            i += TestHelpers.basicInsertBatch(getDb(), hnsw, 100, i,
                    (tr, nextId) -> data.get(Math.toIntExact(nextId))).size();
        }
    }

    @Nonnull
    private AccessInfo fetchAccessInfo(@Nonnull final HNSW hnsw)
            throws ExecutionException, InterruptedException, TimeoutException {
        return Objects.requireNonNull(runAsyncToSync(db, tr ->
                StorageAdapter.fetchAccessInfo(hnsw.getConfig(), tr, getSubspace(), OnReadListener.NOOP)));
    }

    /**
     * {@link StorageAdapter#scanLayer} decodes the nodes it reads with the storage transform it is given, exactly like
     * {@link StorageAdapter#fetchNode}. Only the transform brings a node written before the switch to RaBitQ, which is
     * stored in client coordinates, into the storage space, so such nodes are what this checks.
     */
    @ParameterizedTest
    @MethodSource("seedAndUseInlining")
    void scanLayerDecodesNodesLikeFetchNode(final long seed, final boolean useInlining) throws Exception {
        final Random random = new Random(seed);
        final int numDimensions = 64;
        final HNSW hnsw = newStructureSwitchingToRaBitQ(numDimensions, useInlining);
        insertAll(hnsw, randomVectors(random, numDimensions, 300));

        final AccessInfo accessInfo = fetchAccessInfo(hnsw);
        assertThat(accessInfo.canUseRaBitQ()).as("the structure must have switched to RaBitQ").isTrue();
        final Primitives primitives = hnsw.getLocator().primitives();
        final StorageTransform storageTransform = primitives.storageTransform(accessInfo);

        int numCheckedWrittenBeforeSwitch = 0;
        for (int layer = 0; layer <= accessInfo.getEntryNodeReference().getLayer(); layer ++) {
            final StorageAdapter<? extends NodeReference> storageAdapter = primitives.storageAdapterForLayer(layer);
            numCheckedWrittenBeforeSwitch += storageAdapter.isInliningStorageAdapter()
                                             ? assertInliningScanMatchesFetch(storageAdapter.asInliningStorageAdapter(),
                                                     storageTransform, layer)
                                             : assertCompactScanMatchesFetch(storageAdapter.asCompactStorageAdapter(),
                                                     storageTransform, layer);
        }
        assertThat(numCheckedWrittenBeforeSwitch)
                .as("vectors written before the switch must have been checked")
                .isPositive();
    }

    /**
     * Asserts that every node {@code scanLayer} reads on a compact layer has the vector {@code fetchNode} reads for it,
     * and returns how many of those nodes are stored as plain vectors, i.e. were written before the switch to RaBitQ.
     */
    private int assertCompactScanMatchesFetch(@Nonnull final CompactStorageAdapter storageAdapter,
                                              @Nonnull final StorageTransform storageTransform, final int layer)
            throws ExecutionException, InterruptedException, TimeoutException {
        final List<AbstractNode<NodeReference>> scannedNodes = db.run(tr ->
                ImmutableList.copyOf(storageAdapter.scanLayer(tr, storageTransform, layer, null, Integer.MAX_VALUE)));
        final List<AbstractNode<NodeReference>> storedNodes = db.run(tr ->
                ImmutableList.copyOf(storageAdapter.scanLayer(tr, StorageTransform.identity(), layer, null,
                        Integer.MAX_VALUE)));
        final List<AbstractNode<NodeReference>> fetchedNodes = runAsyncToSync(db, tr ->
                AsyncUtil.getAll(scannedNodes.stream()
                        .map(node -> storageAdapter.fetchNode(tr, storageTransform, layer, node.getPrimaryKey()))
                        .collect(ImmutableList.toImmutableList())));
        assertThat(storedNodes).hasSameSizeAs(scannedNodes);

        int numWrittenBeforeSwitch = 0;
        for (int i = 0; i < scannedNodes.size(); i ++) {
            final AbstractNode<NodeReference> scannedNode = scannedNodes.get(i);
            assertThat(scannedNode.asCompactNode().getVector())
                    .as("layer %d node %s", layer, scannedNode.getPrimaryKey())
                    .isEqualTo(fetchedNodes.get(i).asCompactNode().getVector());
            if (!(storedNodes.get(i).asCompactNode().getVector().getUnderlyingVector() instanceof EncodedRealVector)) {
                numWrittenBeforeSwitch ++;
            }
        }
        return numWrittenBeforeSwitch;
    }

    /**
     * Asserts that every neighbor reference {@code scanLayer} reads on an inlining layer carries the vector
     * {@code fetchNode} reads for it, and returns how many of those references are stored as plain vectors.
     */
    private int assertInliningScanMatchesFetch(@Nonnull final InliningStorageAdapter storageAdapter,
                                               @Nonnull final StorageTransform storageTransform, final int layer)
            throws ExecutionException, InterruptedException, TimeoutException {
        final List<AbstractNode<NodeReferenceWithVector>> scannedNodes = db.run(tr ->
                ImmutableList.copyOf(storageAdapter.scanLayer(tr, storageTransform, layer, null, Integer.MAX_VALUE)));
        final List<AbstractNode<NodeReferenceWithVector>> storedNodes = db.run(tr ->
                ImmutableList.copyOf(storageAdapter.scanLayer(tr, StorageTransform.identity(), layer, null,
                        Integer.MAX_VALUE)));
        final List<AbstractNode<NodeReferenceWithVector>> fetchedNodes = runAsyncToSync(db, tr ->
                AsyncUtil.getAll(scannedNodes.stream()
                        .map(node -> storageAdapter.fetchNode(tr, storageTransform, layer, node.getPrimaryKey()))
                        .collect(ImmutableList.toImmutableList())));
        assertThat(storedNodes).hasSameSizeAs(scannedNodes);

        int numWrittenBeforeSwitch = 0;
        for (int i = 0; i < scannedNodes.size(); i ++) {
            final Map<Tuple, RealVector> fetchedNeighborVectors = Maps.newHashMap();
            for (final NodeReferenceWithVector neighbor : fetchedNodes.get(i).getNeighbors()) {
                fetchedNeighborVectors.put(neighbor.getPrimaryKey(), neighbor.getVector().getUnderlyingVector());
            }
            final Map<Tuple, RealVector> scannedNeighborVectors = Maps.newHashMap();
            for (final NodeReferenceWithVector neighbor : scannedNodes.get(i).getNeighbors()) {
                scannedNeighborVectors.put(neighbor.getPrimaryKey(), neighbor.getVector().getUnderlyingVector());
            }
            assertThat(scannedNeighborVectors)
                    .as("layer %d node %s", layer, scannedNodes.get(i).getPrimaryKey())
                    .isEqualTo(fetchedNeighborVectors);
            for (final NodeReferenceWithVector neighbor : storedNodes.get(i).getNeighbors()) {
                if (!(neighbor.getVector().getUnderlyingVector() instanceof EncodedRealVector)) {
                    numWrittenBeforeSwitch ++;
                }
            }
        }
        return numWrittenBeforeSwitch;
    }

    /**
     * {@link HNSW#scanLayer} returns every vector in client coordinates: it reads each node into the storage space and
     * untransforms it from there. A node written before the switch to RaBitQ is stored in client coordinates, so it has
     * to come back as it was inserted rather than untransformed a second time.
     */
    @ParameterizedTest
    @RandomSeedSource({0x0fdbL, 0x5ca1eL, 123456L})
    void hnswScanLayerReturnsVectorsInClientCoordinates(final long seed) throws Exception {
        final Random random = new Random(seed);
        final Metric metric = Metric.EUCLIDEAN_METRIC;
        final int numDimensions = 64;
        final HNSW hnsw = newStructureSwitchingToRaBitQ(numDimensions, false);
        final List<PrimaryKeyAndVector> insertedData = randomVectors(random, numDimensions, 300);
        insertAll(hnsw, insertedData);
        final Map<Tuple, RealVector> insertedVectors = Maps.newHashMap();
        for (final PrimaryKeyAndVector primaryKeyAndVector : insertedData) {
            insertedVectors.put(primaryKeyAndVector.primaryKey(), primaryKeyAndVector.vector());
        }

        final Set<Tuple> writtenBeforeSwitch = Sets.newHashSet();
        TestHelpers.scanLayer(getDb(), getSubspace(), hnsw.getConfig(), 0, 100, node -> {
            if (!(node.asCompactNode().getVector().getUnderlyingVector() instanceof EncodedRealVector)) {
                writtenBeforeSwitch.add(node.getPrimaryKey());
            }
        });
        assertThat(writtenBeforeSwitch).as("the structure must hold both kinds of nodes")
                .isNotEmpty()
                .hasSizeLessThan(insertedData.size());

        final List<ResultEntry> resultEntries = new ArrayList<>();
        HNSW.scanLayer(hnsw.getConfig(), getSubspace(), getDb(), 0, 100, resultEntries::add);
        assertThat(resultEntries).hasSameSizeAs(insertedData);
        for (final ResultEntry resultEntry : resultEntries) {
            final RealVector insertedVector = Objects.requireNonNull(insertedVectors.get(resultEntry.primaryKey()));
            final RealVector scannedVector = Objects.requireNonNull(resultEntry.vector());
            if (writtenBeforeSwitch.contains(resultEntry.primaryKey())) {
                assertThat(metric.distance(insertedVector, scannedVector))
                        .as("node %s was written before the switch", resultEntry.primaryKey())
                        .isCloseTo(0.0d, within(2E-12));
            } else {
                assertThat(metric.distance(insertedVector, scannedVector.toDoubleRealVector()))
                        .as("node %s is encoded", resultEntry.primaryKey())
                        .isCloseTo(0.0d, within(20.0d));
            }
        }
    }

    /**
     * When the deleted node was the entry node and no candidate on its top layer can be traversed through, the delete
     * scans that layer for a node that can and offers it as the new entry node. The access info holds the entry node in
     * the storage space, so the offered vector has to be in the storage space as well, also for a node written before
     * the switch to RaBitQ, which is stored in client coordinates.
     * <p>
     * The graph is written directly. On layer 1 the entry node and its partner point only at each other, and a separate
     * pair of nodes points only at each other. Deleting the entry node leaves its partner, its only candidate, without
     * an outgoing edge, which is what makes the delete scan the layer, where the first node it reads with an outgoing
     * edge is the first node of the separate pair.
     */
    @ParameterizedTest
    @RandomSeedSource({0x0fdbL, 0x5ca1eL, 123456L})
    void deleteOfEntryNodeOffersScannedNodeInStorageSpace(final long seed) throws Exception {
        final Random random = new Random(seed);
        final int numDimensions = 16;
        final HNSW hnsw = new HNSW(subspaceExtension.getSubspace(), TestExecutors.defaultThreadPool(),
                HNSW.newConfigBuilder()
                        .setMetric(Metric.EUCLIDEAN_METRIC)
                        .setUseRaBitQ(true)
                        .build(numDimensions),
                new TestOnWriteListener(), new TestOnReadListener());
        final Primitives primitives = hnsw.getLocator().primitives();

        //
        // Four nodes whose top layer is 1, in the order the scan reads them: the entry node, its partner, and the
        // separate pair.
        //
        final List<Tuple> primaryKeys = LongStream.range(0, Long.MAX_VALUE)
                .mapToObj(i -> Tuple.from(i))
                .filter(primaryKey -> primitives.topLayer(primaryKey) == 1)
                .limit(4)
                .collect(ImmutableList.toImmutableList());
        final Tuple entryPrimaryKey = primaryKeys.get(0);
        final Tuple partnerPrimaryKey = primaryKeys.get(1);
        final Tuple firstOfPairPrimaryKey = primaryKeys.get(2);
        final Tuple secondOfPairPrimaryKey = primaryKeys.get(3);
        final Map<Tuple, RealVector> vectors = Maps.newHashMap();
        for (final Tuple primaryKey : primaryKeys) {
            vectors.put(primaryKey, createRandomDoubleVector(random, numDimensions));
        }

        final long rotatorSeed = random.nextLong();
        final RealVector negatedCentroid = createRandomDoubleVector(random, numDimensions);
        final StorageTransform storageTransform = primitives.storageTransform(rotatorSeed, negatedCentroid,
                primitives.isMetricNeedsNormalizedVectors());

        db.run(tr -> {
            final CompactStorageAdapter layer0StorageAdapter =
                    primitives.storageAdapterForLayer(0).asCompactStorageAdapter();
            for (final Tuple primaryKey : primaryKeys) {
                writePlainNode(tr, layer0StorageAdapter, 0, primaryKey, vectors.get(primaryKey),
                        primaryKeys.stream()
                                .filter(other -> !other.equals(primaryKey))
                                .collect(ImmutableList.toImmutableList()));
            }
            final CompactStorageAdapter layer1StorageAdapter =
                    primitives.storageAdapterForLayer(1).asCompactStorageAdapter();
            writePlainNode(tr, layer1StorageAdapter, 1, entryPrimaryKey, vectors.get(entryPrimaryKey),
                    ImmutableList.of(partnerPrimaryKey));
            writePlainNode(tr, layer1StorageAdapter, 1, partnerPrimaryKey, vectors.get(partnerPrimaryKey),
                    ImmutableList.of(entryPrimaryKey));
            writePlainNode(tr, layer1StorageAdapter, 1, firstOfPairPrimaryKey, vectors.get(firstOfPairPrimaryKey),
                    ImmutableList.of(secondOfPairPrimaryKey));
            writePlainNode(tr, layer1StorageAdapter, 1, secondOfPairPrimaryKey, vectors.get(secondOfPairPrimaryKey),
                    ImmutableList.of(firstOfPairPrimaryKey));
            StorageAdapter.writeAccessInfo(tr, getSubspace(),
                    new AccessInfo(new EntryNodeReference(entryPrimaryKey,
                            storageTransform.transform(vectors.get(entryPrimaryKey)), 1),
                            rotatorSeed, negatedCentroid),
                    OnWriteListener.NOOP);
            return null;
        });

        runAsyncToSync(db, tr -> hnsw.delete(tr, entryPrimaryKey));

        final EntryNodeReference newEntryNodeReference = fetchAccessInfo(hnsw).getEntryNodeReference();
        assertThat((Object)newEntryNodeReference.getPrimaryKey())
                .as("the scan must offer the first node it reads that has an outgoing edge")
                .isEqualTo(firstOfPairPrimaryKey);
        assertThat(newEntryNodeReference.getLayer()).isEqualTo(1);
        final Transformed<RealVector> expectedVector =
                storageTransform.transform(vectors.get(firstOfPairPrimaryKey));
        assertThat(expectedVector.getUnderlyingVector())
                .as("the storage transform must move the vector, or the next assertion proves nothing")
                .isNotEqualTo(vectors.get(firstOfPairPrimaryKey));
        assertThat(newEntryNodeReference.getVector())
                .as("the new entry node must be held in the storage space")
                .isEqualTo(expectedVector);
    }

    /**
     * Writes a compact node whose vector is stored as a plain vector in client coordinates, as for a node written
     * before the switch to RaBitQ.
     */
    private static void writePlainNode(@Nonnull final Transaction transaction,
                                       @Nonnull final CompactStorageAdapter storageAdapter, final int layer,
                                       @Nonnull final Tuple primaryKey, @Nonnull final RealVector vector,
                                       @Nonnull final List<Tuple> neighborPrimaryKeys) {
        TestHelpers.writeNode(transaction, storageAdapter,
                storageAdapter.getNodeFactory().create(primaryKey, StorageTransform.identity().transform(vector),
                        null,
                        neighborPrimaryKeys.stream()
                                .map(NodeReference::new)
                                .collect(ImmutableList.toImmutableList())),
                layer);
    }

    @ParameterizedTest
    @MethodSource("differentMetrics")
    void testOrderByDistance(final long seed, final Config config) throws ExecutionException, InterruptedException, TimeoutException {
        final Random random = new Random(seed);
        final int size = 1000;
        final TestOnWriteListener onWriteListener = new TestOnWriteListener();
        final TestOnReadListener onReadListener = new TestOnReadListener();

        final HNSW hnsw = new HNSW(subspaceExtension.getSubspace(), TestExecutors.defaultThreadPool(), config,
                onWriteListener, onReadListener);

        final List<PrimaryKeyAndVector> insertedData = randomVectors(random, config.numDimensions(), size);

        for (int i = 0; i < size;) {
            i += TestHelpers.basicInsertBatch(getDb(), hnsw, 100, i,
                    (tr, nextId) -> insertedData.get(Math.toIntExact(nextId))).size();
        }

        final int skip = 100;
        final HalfRealVector queryVector = createRandomHalfVector(random, config.numDimensions());

        final NavigableSet<PrimaryKeyVectorAndDistance> orderedByDistances =
                CommonTestHelpers.orderedByDistances(config.metric(), insertedData, queryVector);
        final PrimaryKeyVectorAndDistance discriminator = Iterables.get(orderedByDistances, skip - 1);

        onReadListener.reset();

        final List<ResultEntry> results =
                runAsyncToSync(db, tr ->
                        AsyncUtil.collectRemaining(
                                hnsw.orderByDistance(tr, 100, 1000, false,
                                        queryVector, discriminator.distance(), discriminator.primaryKey(), false),
                                TestExecutors.defaultThreadPool()));

        int numInversions = 0;
        for (int i = 1; i < results.size(); i++) {
            final ResultEntry previous = results.get(i - 1);
            final ResultEntry current = results.get(i);

            if (previous.distance() > current.distance()) {
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
                        .map(PrimaryKeyVectorAndDistance::primaryKey)
                        .collect(ImmutableList.toImmutableList());

        final ImmutableSet<Tuple> groundTruthExpected =
                groundTruth.stream()
                        .skip(100)
                        .collect(ImmutableSet.toImmutableSet());

        final ImmutableSet<Tuple> resultIds =
                results.stream()
                        .map(ResultEntry::primaryKey)
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
