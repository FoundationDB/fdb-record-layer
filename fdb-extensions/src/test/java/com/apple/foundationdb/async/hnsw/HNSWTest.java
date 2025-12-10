/*
 * HNSWTest.java
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
import com.apple.foundationdb.async.rtree.RTree;
import com.apple.foundationdb.linear.AffineOperator;
import com.apple.foundationdb.linear.DoubleRealVector;
import com.apple.foundationdb.linear.HalfRealVector;
import com.apple.foundationdb.linear.Metric;
import com.apple.foundationdb.linear.Quantizer;
import com.apple.foundationdb.linear.RealVector;
import com.apple.foundationdb.linear.StoredVecsIterator;
import com.apple.foundationdb.linear.Transformed;
import com.apple.foundationdb.rabitq.EncodedRealVector;
import com.apple.foundationdb.test.TestDatabaseExtension;
import com.apple.foundationdb.test.TestExecutors;
import com.apple.foundationdb.test.TestSubspaceExtension;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.RandomSeedSource;
import com.apple.test.RandomizedTestUtils;
import com.apple.test.SuperSlow;
import com.apple.test.Tags;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.ObjectArrays;
import com.google.common.collect.Sets;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.AfterTestExecutionCallback;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.params.ParameterInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.aggregator.ArgumentsAccessor;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static com.apple.foundationdb.linear.RealVectorTest.createRandomDoubleVector;
import static com.apple.foundationdb.linear.RealVectorTest.createRandomHalfVector;
import static org.assertj.core.api.Assertions.within;

/**
 * Tests testing insert/update/deletes of data into/in/from {@link RTree}s.
 */
@Execution(ExecutionMode.CONCURRENT)
@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
@Tag(Tags.RequiresFDB)
@Tag(Tags.Slow)
class HNSWTest {
    private static final Logger logger = LoggerFactory.getLogger(HNSWTest.class);

    @RegisterExtension
    static final TestDatabaseExtension dbExtension = new TestDatabaseExtension();
    @RegisterExtension
    TestSubspaceExtension rtSubspace = new TestSubspaceExtension(dbExtension);
    @RegisterExtension
    TestSubspaceExtension rtSecondarySubspace = new TestSubspaceExtension(dbExtension);

    private Database db;

    @BeforeEach
    public void setUpDb() {
        db = dbExtension.getDatabase();
    }

    @ParameterizedTest
    @RandomSeedSource({0x0fdbL, 0x5ca1eL, 123456L, 78910L, 1123581321345589L})
    void testCompactSerialization(final long seed) {
        final Random random = new Random(seed);
        final int numDimensions = 768;
        final CompactStorageAdapter storageAdapter =
                new CompactStorageAdapter(HNSW.newConfigBuilder().build(numDimensions), CompactNode.factory(),
                        rtSubspace.getSubspace(), OnWriteListener.NOOP, OnReadListener.NOOP);
        final AbstractNode<NodeReference> originalNode =
                db.run(tr -> {
                    final NodeFactory<NodeReference> nodeFactory = storageAdapter.getNodeFactory();

                    final AbstractNode<NodeReference> randomCompactNode =
                            createRandomCompactNode(random, nodeFactory, numDimensions, 16);

                    writeNode(tr, storageAdapter, randomCompactNode, 0);
                    return randomCompactNode;
                });

        db.run(tr -> storageAdapter.fetchNode(tr, AffineOperator.identity(), 0,
                        originalNode.getPrimaryKey())
                .thenAccept(node ->
                        Assertions.assertThat(node).satisfies(
                                n -> Assertions.assertThat(n).isInstanceOf(CompactNode.class),
                                n -> Assertions.assertThat(n.getKind()).isSameAs(NodeKind.COMPACT),
                                n -> Assertions.assertThat((Object)n.getPrimaryKey()).isEqualTo(originalNode.getPrimaryKey()),
                                n -> Assertions.assertThat(n.asCompactNode().getVector())
                                        .isEqualTo(originalNode.asCompactNode().getVector()),
                                n -> {
                                    final ArrayList<NodeReference> neighbors =
                                            Lists.newArrayList(node.getNeighbors());
                                    neighbors.sort(Comparator.comparing(NodeReference::getPrimaryKey));
                                    final ArrayList<NodeReference> originalNeighbors =
                                            Lists.newArrayList(originalNode.getNeighbors());
                                    originalNeighbors.sort(Comparator.comparing(NodeReference::getPrimaryKey));
                                    Assertions.assertThat(neighbors).isEqualTo(originalNeighbors);
                                }
                )).join());
    }

    @ParameterizedTest
    @RandomSeedSource({0x0fdbL, 0x5ca1eL, 123456L, 78910L, 1123581321345589L})
    void testInliningSerialization(final long seed) {
        final Random random = new Random(seed);
        final int numDimensions = 768;
        final InliningStorageAdapter storageAdapter =
                new InliningStorageAdapter(HNSW.newConfigBuilder().build(numDimensions),
                        InliningNode.factory(), rtSubspace.getSubspace(),
                        OnWriteListener.NOOP, OnReadListener.NOOP);
        final Node<NodeReferenceWithVector> originalNode =
                db.run(tr -> {
                    final NodeFactory<NodeReferenceWithVector> nodeFactory = storageAdapter.getNodeFactory();

                    final AbstractNode<NodeReferenceWithVector> randomInliningNode =
                            createRandomInliningNode(random, nodeFactory, numDimensions, 16);

                    writeNode(tr, storageAdapter, randomInliningNode, 0);
                    return randomInliningNode;
                });

        db.run(tr -> storageAdapter.fetchNode(tr, AffineOperator.identity(), 0,
                        originalNode.getPrimaryKey())
                .thenAccept(node ->
                        Assertions.assertThat(node).satisfies(
                                n -> Assertions.assertThat(n).isInstanceOf(InliningNode.class),
                                n -> Assertions.assertThat(n.getKind()).isSameAs(NodeKind.INLINING),
                                n -> Assertions.assertThat((Object)node.getPrimaryKey()).isEqualTo(originalNode.getPrimaryKey()),
                                n -> {
                                    final ArrayList<NodeReference> neighbors =
                                            Lists.newArrayList(node.getNeighbors());
                                    neighbors.sort(Comparator.comparing(NodeReference::getPrimaryKey)); // should not be necessary the way it is stored
                                    final ArrayList<NodeReference> originalNeighbors =
                                            Lists.newArrayList(originalNode.getNeighbors());
                                    originalNeighbors.sort(Comparator.comparing(NodeReference::getPrimaryKey));
                                    Assertions.assertThat(neighbors).isEqualTo(originalNeighbors);
                                }
                        )).join());
    }

    static Stream<Arguments> randomSeedsWithConfig() {
        return RandomizedTestUtils.randomSeeds(0xdeadc0deL)
                .flatMap(seed -> Sets.cartesianProduct(ImmutableSet.of(true, false),
                                ImmutableSet.of(true, false),
                                ImmutableSet.of(true, false),
                                ImmutableSet.of(true, false)).stream()
                        .map(arguments -> Arguments.of(ObjectArrays.concat(seed,
                                new Object[] {HNSW.newConfigBuilder()
                                        .setMetric(Metric.EUCLIDEAN_METRIC)
                                        .setUseInlining(arguments.get(0))
                                        .setExtendCandidates(arguments.get(1))
                                        .setKeepPrunedConnections(arguments.get(2))
                                        .setUseRaBitQ(arguments.get(3))
                                        .setRaBitQNumExBits(5)
                                        .setSampleVectorStatsProbability(1.0d)
                                        .setMaintainStatsProbability(0.1d)
                                        .setStatsThreshold(100)
                                        .setM(32)
                                        .setMMax(32)
                                        .setMMax0(64)
                                        .build(128)}))));
    }

    @ParameterizedTest
    @MethodSource("randomSeedsWithConfig")
    void testBasicInsert(final long seed, final Config config) {
        final Random random = new Random(seed);
        final Metric metric = config.getMetric();
        final TestOnReadListener onReadListener = new TestOnReadListener();

        final HNSW hnsw =
                new HNSW(rtSubspace.getSubspace(), TestExecutors.defaultThreadPool(), config,
                        OnWriteListener.NOOP, onReadListener);

        final int k = 50;
        final HalfRealVector queryVector = createRandomHalfVector(random, config.getNumDimensions());
        final TreeSet<PrimaryKeyVectorAndDistance> recordsOrderedByDistance =
                new TreeSet<>(Comparator.comparing(PrimaryKeyVectorAndDistance::getDistance));

        for (int i = 0; i < 1000; ) {
            i += basicInsertBatch(hnsw, 100, i, onReadListener,
                    (tr, nextId) -> {
                        final var primaryKey = createPrimaryKey(nextId);
                        final HalfRealVector dataVector = createRandomHalfVector(random, config.getNumDimensions());
                        final double distance = metric.distance(dataVector, queryVector);
                        final PrimaryKeyVectorAndDistance record =
                                new PrimaryKeyVectorAndDistance(primaryKey, dataVector, distance);
                        recordsOrderedByDistance.add(record);
                        if (recordsOrderedByDistance.size() > k) {
                            recordsOrderedByDistance.pollLast();
                        }
                        return record;
                    });
        }

        //
        // Attempt to mutate some records by updating them using the same primary keys but different random vectors.
        // This should not fail but should be silently ignored. If this succeeds, the following searches will all
        // return records that are not aligned with recordsOrderedByDistance.
        //
        for (int i = 0; i < 100; ) {
            i += basicInsertBatch(hnsw, 100, 0, onReadListener,
                    (tr, ignored) -> {
                        final var primaryKey = createPrimaryKey(random.nextInt(1000));
                        final HalfRealVector dataVector = createRandomHalfVector(random, config.getNumDimensions());
                        final double distance = metric.distance(dataVector, queryVector);
                        return new PrimaryKeyVectorAndDistance(primaryKey, dataVector, distance);
                    });
        }

        onReadListener.reset();
        final long beginTs = System.nanoTime();
        final List<? extends ResultEntry> results =
                db.run(tr ->
                        hnsw.kNearestNeighborsSearch(tr, k, 100, true, queryVector).join());
        final long endTs = System.nanoTime();

        final ImmutableSet<Tuple> trueNN =
                recordsOrderedByDistance.stream()
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
        Assertions.assertThat(recall).isGreaterThan(0.9);

        final Set<Long> insertedIds =
                LongStream.range(0, 1000)
                        .boxed()
                        .collect(Collectors.toSet());

        final Set<Long> readIds = Sets.newHashSet();
        scanLayer(config, 0, 100,
                node -> Assertions.assertThat(readIds.add(node.getPrimaryKey().getLong(0))).isTrue());
        Assertions.assertThat(readIds).isEqualTo(insertedIds);

        readIds.clear();
        scanLayer(config, 1, 100,
                node -> Assertions.assertThat(readIds.add(node.getPrimaryKey().getLong(0))).isTrue());
        Assertions.assertThat(readIds.size()).isBetween(10, 50);
    }

    @ExtendWith(HNSWTest.DumpLayersIfFailure.class)
    @ParameterizedTest
    @MethodSource("randomSeedsWithConfig")
    void testBasicInsertDelete(final long seed, final Config config) {
        final Random random = new Random(seed);
        final int size = 1000;
        final TestOnReadListener onReadListener = new TestOnReadListener();

        final HNSW hnsw = new HNSW(rtSubspace.getSubspace(), TestExecutors.defaultThreadPool(), config,
                OnWriteListener.NOOP, onReadListener);

        final int k = 50;
        final List<PrimaryKeyAndVector> insertedData = randomVectors(random, config.getNumDimensions(), 1000);

        for (int i = 0; i < size;) {
            i += basicInsertBatch(hnsw, 100, i, onReadListener,
                    (tr, nextId) -> insertedData.get(Math.toIntExact(nextId)));
        }

        final int numVectorsPerDeleteBatch = 100;
        List<PrimaryKeyAndVector> remainingData = insertedData;
        do {
            final List<PrimaryKeyAndVector> toBeDeleted =
                    pickRandomVectors(random, remainingData, numVectorsPerDeleteBatch);

            onReadListener.reset();

            long beginTs = System.nanoTime();
            db.run(tr -> {
                for (final PrimaryKeyAndVector primaryKeyAndVector : toBeDeleted) {
                    hnsw.delete(tr, primaryKeyAndVector.getPrimaryKey()).join();
                }
                return null;
            });
            long endTs = System.nanoTime();

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
                        orderedByDistances(Metric.EUCLIDEAN_METRIC, remainingData, queryVector).stream()
                                .limit(k)
                                .map(PrimaryKeyVectorAndDistance::getPrimaryKey)
                                .collect(ImmutableSet.toImmutableSet());

                onReadListener.reset();

                beginTs = System.nanoTime();
                final List<? extends ResultEntry> results =
                        db.run(tr ->
                                hnsw.kNearestNeighborsSearch(tr, k, 100, true, queryVector).join());
                endTs = System.nanoTime();

                int recallCount = 0;
                for (ResultEntry resultEntry : results) {
                    if (trueNN.contains(resultEntry.getPrimaryKey())) {
                        recallCount++;
                    }
                }
                final double recall = (double)recallCount / (double)trueNN.size();

//                if (recall == 0.7) {
//                    int layer = 0;
//                    while (true) {
//                        if (!dumpLayer(hnsw, "debug", layer++)) {
//                            break;
//                        }
//                    }
//                }

                logger.info("search transaction after delete of {} records took elapsedTime={}ms; read nodes={}, read bytes={}, recall={}",
                        size - remainingData.size(),
                        TimeUnit.NANOSECONDS.toMillis(endTs - beginTs),
                        onReadListener.getNodeCountByLayer(), onReadListener.getBytesReadByLayer(),
                        String.format(Locale.ROOT, "%.2f", recall * 100.0d));
                Assertions.assertThat(recall).isGreaterThan(0.9);

                final long remainingNumNodes = countNodesOnLayer(config, 0);
                Assertions.assertThat(remainingNumNodes).isEqualTo(remainingData.size());
            }
        } while (!remainingData.isEmpty());

        final var accessInfo =
                db.run(transaction -> StorageAdapter.fetchAccessInfo(hnsw.getConfig(),
                        transaction, hnsw.getSubspace(), OnReadListener.NOOP).join());
        Assertions.assertThat(accessInfo).isNull();
        Assertions.assertThat((Double)null).isNotNull();
    }

    @ParameterizedTest()
    @RandomSeedSource({0x0fdbL, 0x5ca1eL, 123456L, 78910L, 1123581321345589L})
    void testBasicInsertWithRaBitQEncodings(final long seed) {
        final Random random = new Random(seed);
        final Metric metric = Metric.EUCLIDEAN_METRIC;

        final int numDimensions = 128;
        final HNSW hnsw = new HNSW(rtSubspace.getSubspace(), TestExecutors.defaultThreadPool(),
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
                OnWriteListener.NOOP, OnReadListener.NOOP);

        final int k = 499;
        final DoubleRealVector queryVector = createRandomDoubleVector(random, numDimensions);
        final Map<Tuple, RealVector> dataMap = Maps.newHashMap();
        final TreeSet<PrimaryKeyVectorAndDistance> recordsOrderedByDistance =
                new TreeSet<>(Comparator.comparing(PrimaryKeyVectorAndDistance::getDistance));

        for (int i = 0; i < 1000;) {
            i += basicInsertBatch(hnsw, 100, i, new TestOnReadListener(),
                    (tr, nextId) -> {
                        final var primaryKey = createPrimaryKey(nextId);
                        final DoubleRealVector dataVector = createRandomDoubleVector(random, numDimensions);
                        final double distance = metric.distance(dataVector, queryVector);
                        dataMap.put(primaryKey, dataVector);

                        final PrimaryKeyVectorAndDistance record =
                                new PrimaryKeyVectorAndDistance(primaryKey, dataVector, distance);
                        recordsOrderedByDistance.add(record);
                        if (recordsOrderedByDistance.size() > k) {
                            recordsOrderedByDistance.pollLast();
                        }
                        return record;
                    });
        }

        //
        // If we fetch the current state back from the db, some vectors are regular vectors and some vectors are
        // RaBitQ encoded. Since that information is not surfaced through the API, we need to scan layer 0, get
        // all vectors directly from disk (encoded/not-encoded, transformed/not-transformed) in order to check
        // that transformations/reconstructions are applied properly.
        //
        final Map<Tuple, RealVector> fromDBMap = Maps.newHashMap();
        scanLayer(hnsw.getConfig(), 0, 100,
                node -> fromDBMap.put(node.getPrimaryKey(),
                        node.asCompactNode().getVector().getUnderlyingVector()));

        //
        // Still run a kNN search to make sure that recall is satisfactory.
        //
        final List<? extends ResultEntry> results =
                db.run(tr ->
                        hnsw.kNearestNeighborsSearch(tr, k, 500, true, queryVector).join());

        final ImmutableSet<Tuple> trueNN =
                recordsOrderedByDistance.stream()
                        .map(PrimaryKeyAndVector::getPrimaryKey)
                        .collect(ImmutableSet.toImmutableSet());

        int recallCount = 0;
        int exactVectorCount = 0;
        int encodedVectorCount = 0;
        for (final ResultEntry resultEntry : results) {
            if (trueNN.contains(resultEntry.getPrimaryKey())) {
                recallCount ++;
            }

            final RealVector originalVector = dataMap.get(resultEntry.getPrimaryKey());
            Assertions.assertThat(originalVector).isNotNull();
            final RealVector fromDBVector = fromDBMap.get(resultEntry.getPrimaryKey());
            Assertions.assertThat(fromDBVector).isNotNull();
            if (!(fromDBVector instanceof EncodedRealVector)) {
                Assertions.assertThat(originalVector).isEqualTo(fromDBVector);
                exactVectorCount ++;
                final double distance = metric.distance(originalVector,
                        Objects.requireNonNull(resultEntry.getVector()));
                Assertions.assertThat(distance).isCloseTo(0.0d, within(2E-12));
            } else {
                encodedVectorCount ++;
                final double distance = metric.distance(originalVector,
                        Objects.requireNonNull(resultEntry.getVector()).toDoubleRealVector());
                Assertions.assertThat(distance).isCloseTo(0.0d, within(20.0d));
            }
        }
        final double recall = (double)recallCount / (double)k;
        Assertions.assertThat(recall).isGreaterThan(0.9);
        // must have both kinds
        Assertions.assertThat(exactVectorCount).isGreaterThan(0);
        Assertions.assertThat(encodedVectorCount).isGreaterThan(0);
    }

    private int basicInsertBatch(final HNSW hnsw, final int batchSize,
                                 final long firstId, @Nonnull final TestOnReadListener onReadListener,
                                 @Nonnull final BiFunction<Transaction, Long, PrimaryKeyAndVector> insertFunction) {
        return db.run(tr -> {
            onReadListener.reset();
            final long beginTs = System.nanoTime();
            for (int i = 0; i < batchSize; i ++) {
                final var record = insertFunction.apply(tr, firstId + i);
                if (record == null) {
                    return i;
                }
                hnsw.insert(tr, record.getPrimaryKey(), record.getVector()).join();
            }
            final long endTs = System.nanoTime();
            logger.info("inserted batchSize={} records starting at nodeId={} took elapsedTime={}ms, readCounts={}, readBytes={}",
                    batchSize, firstId, TimeUnit.NANOSECONDS.toMillis(endTs - beginTs),
                    onReadListener.getNodeCountByLayer(), onReadListener.getBytesReadByLayer());
            return batchSize;
        });
    }

    @Test
    @SuperSlow
    void testSIFTInsertSmall() throws Exception {
        final Metric metric = Metric.EUCLIDEAN_METRIC;
        final int k = 100;
        final TestOnReadListener onReadListener = new TestOnReadListener();

        final HNSW hnsw = new HNSW(rtSubspace.getSubspace(), TestExecutors.defaultThreadPool(),
                HNSW.newConfigBuilder()
                        .setUseRaBitQ(true)
                        .setRaBitQNumExBits(5)
                        .setMetric(metric)
                        .setM(32)
                        .setMMax(32)
                        .setMMax0(64)
                        .build(128),
                OnWriteListener.NOOP, onReadListener);

        final Path siftSmallPath = Paths.get(".out/extracted/siftsmall/siftsmall_base.fvecs");

        final Map<Integer, RealVector> dataMap = Maps.newHashMap();

        try (final var fileChannel = FileChannel.open(siftSmallPath, StandardOpenOption.READ)) {
            final Iterator<DoubleRealVector> vectorIterator = new StoredVecsIterator.StoredFVecsIterator(fileChannel);

            int i = 0;
            final AtomicReference<RealVector> sumReference = new AtomicReference<>(null);
            while (vectorIterator.hasNext()) {
                i += basicInsertBatch(hnsw, 100, i, onReadListener,
                        (tr, nextId) -> {
                            if (!vectorIterator.hasNext()) {
                                return null;
                            }
                            final DoubleRealVector doubleVector = vectorIterator.next();
                            final Tuple currentPrimaryKey = createPrimaryKey(nextId);
                            final HalfRealVector currentVector = doubleVector.toHalfRealVector();

                            if (sumReference.get() == null) {
                                sumReference.set(currentVector);
                            } else {
                                sumReference.set(sumReference.get().add(currentVector));
                            }

                            dataMap.put(Math.toIntExact(currentPrimaryKey.getLong(0)), currentVector);
                            return new PrimaryKeyAndVector(currentPrimaryKey, currentVector);
                        });
            }
            Assertions.assertThat(i).isEqualTo(10000);
        }

        validateSIFTSmall(hnsw, dataMap, k);
    }

    private void validateSIFTSmall(@Nonnull final HNSW hnsw, @Nonnull final Map<Integer, RealVector> dataMap, final int k) throws IOException {
        final Metric metric = hnsw.getConfig().getMetric();
        final Path siftSmallGroundTruthPath = Paths.get(".out/extracted/siftsmall/siftsmall_groundtruth.ivecs");
        final Path siftSmallQueryPath = Paths.get(".out/extracted/siftsmall/siftsmall_query.fvecs");

        final TestOnReadListener onReadListener = (TestOnReadListener)hnsw.getOnReadListener();

        try (final var queryChannel = FileChannel.open(siftSmallQueryPath, StandardOpenOption.READ);
                final var groundTruthChannel = FileChannel.open(siftSmallGroundTruthPath, StandardOpenOption.READ)) {
            final Iterator<DoubleRealVector> queryIterator = new StoredVecsIterator.StoredFVecsIterator(queryChannel);
            final Iterator<List<Integer>> groundTruthIterator = new StoredVecsIterator.StoredIVecsIterator(groundTruthChannel);

            Verify.verify(queryIterator.hasNext() == groundTruthIterator.hasNext());

            while (queryIterator.hasNext()) {
                final HalfRealVector queryVector = queryIterator.next().toHalfRealVector();
                final Set<Integer> groundTruthIndices = ImmutableSet.copyOf(groundTruthIterator.next());
                onReadListener.reset();
                final long beginTs = System.nanoTime();
                final List<? extends ResultEntry> results =
                        db.run(tr -> hnsw.kNearestNeighborsSearch(tr, k, 100,
                                true, queryVector).join());
                final long endTs = System.nanoTime();
                logger.info("retrieved result in elapsedTimeMs={}, reading numNodes={}, readBytes={}",
                        TimeUnit.NANOSECONDS.toMillis(endTs - beginTs),
                        onReadListener.getNodeCountByLayer(), onReadListener.getBytesReadByLayer());

                int recallCount = 0;
                for (final ResultEntry resultEntry : results) {
                    final int primaryKeyIndex = (int)resultEntry.getPrimaryKey().getLong(0);

                    //
                    // Assert that the original vector and the reconstructed vector are the same-ish vector
                    // (minus reconstruction errors). The closeness value is dependent on the encoding quality settings,
                    // the dimensionality, and the metric in use. For now, we just set it to 20.0 as that should be
                    // fairly safe with respect to not giving us false-positives and also tripping for actual logic
                    // errors as the expected random distance is far larger.
                    //
                    final RealVector originalVector = dataMap.get(primaryKeyIndex);
                    Assertions.assertThat(originalVector).isNotNull();
                    final double distance = metric.distance(originalVector,
                            Objects.requireNonNull(resultEntry.getVector()).toDoubleRealVector());
                    Assertions.assertThat(distance).isCloseTo(0.0d, within(20.0d));

                    logger.trace("retrieved result nodeId = {} at distance = {} ",
                            primaryKeyIndex, resultEntry.getDistance());
                    if (groundTruthIndices.contains(primaryKeyIndex)) {
                        recallCount ++;
                    }
                }

                final double recall = (double)recallCount / k;
                Assertions.assertThat(recall).isGreaterThan(0.93);

                logger.info("query returned results recall={}", String.format(Locale.ROOT, "%.2f", recall * 100.0d));
            }
        }
    }

    @Nonnull
    private List<PrimaryKeyAndVector> randomVectors(@Nonnull final Random random, final int numDimensions,
                                                    final int numberOfVectors) {
        final ImmutableList.Builder<PrimaryKeyAndVector> resultBuilder = ImmutableList.builder();
        for (int i = 0; i < numberOfVectors; i ++) {
            final var primaryKey = createPrimaryKey(i);
            final HalfRealVector dataVector = createRandomHalfVector(random, numDimensions);
            resultBuilder.add(new PrimaryKeyAndVector(primaryKey, dataVector));
        }
        return resultBuilder.build();
    }

    @Nonnull
    private List<PrimaryKeyAndVector> pickRandomVectors(@Nonnull final Random random,
                                                        @Nonnull final Collection<PrimaryKeyAndVector> vectors,
                                                        final int numberOfVectors) {
        Verify.verify(numberOfVectors <= vectors.size());
        final List<PrimaryKeyAndVector> remainingVectors = Lists.newArrayList(vectors);
        final ImmutableList.Builder<PrimaryKeyAndVector> resultBuilder = ImmutableList.builder();
        for (int i = 0; i < numberOfVectors; i ++) {
            resultBuilder.add(remainingVectors.remove(random.nextInt(remainingVectors.size())));
        }
        return resultBuilder.build();
    }

    @Nonnull
    private NavigableSet<PrimaryKeyVectorAndDistance> orderedByDistances(@Nonnull final Metric metric,
                                                                         @Nonnull final List<PrimaryKeyAndVector> vectors,
                                                                         @Nonnull final HalfRealVector queryVector) {
        final TreeSet<PrimaryKeyVectorAndDistance> vectorsOrderedByDistance =
                new TreeSet<>(Comparator.comparing(PrimaryKeyVectorAndDistance::getDistance));
        for (final PrimaryKeyAndVector vector : vectors) {
            final double distance = metric.distance(vector.getVector(), queryVector);
            final PrimaryKeyVectorAndDistance record =
                    new PrimaryKeyVectorAndDistance(vector.getPrimaryKey(), vector.getVector(), distance);
            vectorsOrderedByDistance.add(record);
        }
        return vectorsOrderedByDistance;
    }

    private long countNodesOnLayer(@Nonnull final Config config, final int layer) {
        final AtomicLong counter = new AtomicLong();
        scanLayer(config, layer, 100, node -> counter.incrementAndGet());
        return counter.get();
    }

    private void scanLayer(@Nonnull final Config config,
                           final int layer,
                           final int batchSize,
                           @Nonnull final Consumer<AbstractNode<? extends NodeReference>> nodeConsumer) {
        HNSW.scanLayer(config, rtSubspace.getSubspace(), db, layer, batchSize, nodeConsumer);
    }

    private boolean dumpLayer(@Nonnull final Config config,
                              @Nonnull final String prefix, final int layer) throws IOException {
        final String verticesFileName = "/Users/nseemann/Downloads/vertices-" + prefix + "-" + layer + ".csv";
        final String edgesFileName = "/Users/nseemann/Downloads/edges-" + prefix + "-" + layer + ".csv";

        final AtomicLong numReadAtomic = new AtomicLong(0L);
        try (final BufferedWriter verticesWriter = new BufferedWriter(new FileWriter(verticesFileName));
                final BufferedWriter edgesWriter = new BufferedWriter(new FileWriter(edgesFileName))) {
            scanLayer(config, layer, 100, node -> {
                final CompactNode compactNode = node.asCompactNode();
                final Transformed<RealVector> vector = compactNode.getVector();
                try {
                    verticesWriter.write(compactNode.getPrimaryKey().getLong(0) + ",");
                    final RealVector realVector = vector.getUnderlyingVector();
                    for (int i = 0; i < realVector.getNumDimensions(); i++) {
                        if (i != 0) {
                            verticesWriter.write(",");
                        }
                        verticesWriter.write(String.valueOf(realVector.getComponent(i)));
                    }
                    verticesWriter.newLine();

                    for (final var neighbor : compactNode.getNeighbors()) {
                        edgesWriter.write(compactNode.getPrimaryKey().getLong(0) + "," +
                                neighbor.getPrimaryKey().getLong(0));
                        edgesWriter.newLine();
                    }
                    numReadAtomic.getAndIncrement();
                } catch (final IOException e) {
                    throw new RuntimeException("unable to write to file", e);
                }
            });
        }
        return numReadAtomic.get() != 0;
    }

    private <N extends NodeReference> void writeNode(@Nonnull final Transaction transaction,
                                                     @Nonnull final StorageAdapter<N> storageAdapter,
                                                     @Nonnull final AbstractNode<N> node,
                                                     final int layer) {
        final NeighborsChangeSet<N> insertChangeSet =
                new InsertNeighborsChangeSet<>(new BaseNeighborsChangeSet<>(ImmutableList.of()),
                        node.getNeighbors());
        storageAdapter.writeNode(transaction, Quantizer.noOpQuantizer(Metric.EUCLIDEAN_METRIC), layer, node,
                insertChangeSet);
    }

    @Nonnull
    private AbstractNode<NodeReference> createRandomCompactNode(@Nonnull final Random random,
                                                                @Nonnull final NodeFactory<NodeReference> nodeFactory,
                                                                final int numDimensions,
                                                                final int numberOfNeighbors) {
        final Tuple primaryKey = createRandomPrimaryKey(random);
        final ImmutableList.Builder<NodeReference> neighborsBuilder = ImmutableList.builder();
        for (int i = 0; i < numberOfNeighbors; i ++) {
            neighborsBuilder.add(createRandomNodeReference(random));
        }

        return nodeFactory.create(primaryKey,
                AffineOperator.identity().transform(createRandomHalfVector(random, numDimensions)),
                neighborsBuilder.build());
    }

    @Nonnull
    private AbstractNode<NodeReferenceWithVector> createRandomInliningNode(@Nonnull final Random random,
                                                                           @Nonnull final NodeFactory<NodeReferenceWithVector> nodeFactory,
                                                                           final int numDimensions,
                                                                           final int numberOfNeighbors) {
        final Tuple primaryKey = createRandomPrimaryKey(random);
        final ImmutableList.Builder<NodeReferenceWithVector> neighborsBuilder = ImmutableList.builder();
        for (int i = 0; i < numberOfNeighbors; i ++) {
            neighborsBuilder.add(createRandomNodeReferenceWithVector(random, numDimensions));
        }

        return nodeFactory.create(primaryKey,
                AffineOperator.identity().transform(createRandomHalfVector(random, numDimensions)),
                neighborsBuilder.build());
    }

    @Nonnull
    private NodeReference createRandomNodeReference(@Nonnull final Random random) {
        return new NodeReference(createRandomPrimaryKey(random));
    }

    @Nonnull
    private NodeReferenceWithVector createRandomNodeReferenceWithVector(@Nonnull final Random random,
                                                                        final int dimensionality) {
        return new NodeReferenceWithVector(createRandomPrimaryKey(random),
                AffineOperator.identity().transform(createRandomHalfVector(random, dimensionality)));
    }

    @Nonnull
    private static Tuple createRandomPrimaryKey(final @Nonnull Random random) {
        return createPrimaryKey(random.nextLong());
    }

    @Nonnull
    private static Tuple createPrimaryKey(final long nextId) {
        return Tuple.from(nextId);
    }

    public static class DumpLayersIfFailure implements AfterTestExecutionCallback {
        @Override
        public void afterTestExecution(@Nonnull final ExtensionContext context) {
            final Optional<Throwable> failure = context.getExecutionException();
            if (failure.isEmpty()) {
                return;
            }

            final ParameterInfo parameterInfo = ParameterInfo.get(context);

            if (parameterInfo != null) {
                final ArgumentsAccessor args = parameterInfo.getArguments();

                final HNSWTest hnswTest = (HNSWTest)context.getRequiredTestInstance();
                final Config config = (Config)args.get(1);
                logger.error("dumping contents of HNSW to disk");
                dumpLayers(hnswTest, config);
            } else {
                logger.error("test failed with no parameterized arguments (non-parameterized test or older JUnit).");
            }
        }

        private void dumpLayers(@Nonnull final HNSWTest hnswTest, @Nonnull final Config config) {
            int layer = 0;
            while (true) {
                try {
                    if (!hnswTest.dumpLayer(config, "debug", layer++)) {
                        break;
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    private static class TestOnReadListener implements OnReadListener {
        final Map<Integer, Long> nodeCountByLayer;
        final Map<Integer, Long> sumMByLayer;
        final Map<Integer, Long> bytesReadByLayer;

        public TestOnReadListener() {
            this.nodeCountByLayer = Maps.newConcurrentMap();
            this.sumMByLayer = Maps.newConcurrentMap();
            this.bytesReadByLayer = Maps.newConcurrentMap();
        }

        public Map<Integer, Long> getNodeCountByLayer() {
            return nodeCountByLayer;
        }

        public Map<Integer, Long> getBytesReadByLayer() {
            return bytesReadByLayer;
        }

        public Map<Integer, Long> getSumMByLayer() {
            return sumMByLayer;
        }

        public void reset() {
            nodeCountByLayer.clear();
            bytesReadByLayer.clear();
            sumMByLayer.clear();
        }

        @Override
        public void onNodeRead(final int layer, @Nonnull final Node<? extends NodeReference> node) {
            nodeCountByLayer.compute(layer, (l, oldValue) -> (oldValue == null ? 0 : oldValue) + 1L);
            sumMByLayer.compute(layer, (l, oldValue) -> (oldValue == null ? 0 : oldValue) + node.getNeighbors().size());
        }

        @Override
        public void onKeyValueRead(final int layer, @Nonnull final byte[] key, @Nullable final byte[] value) {
            bytesReadByLayer.compute(layer, (l, oldValue) -> (oldValue == null ? 0 : oldValue) +
                    key.length + (value == null ? 0 : value.length));
        }
    }

    private static class PrimaryKeyAndVector {
        @Nonnull
        private final Tuple primaryKey;
        @Nonnull
        private final RealVector vector;

        public PrimaryKeyAndVector(@Nonnull final Tuple primaryKey,
                                   @Nonnull final RealVector vector) {
            this.primaryKey = primaryKey;
            this.vector = vector;
        }

        @Nonnull
        public Tuple getPrimaryKey() {
            return primaryKey;
        }

        @Nonnull
        public RealVector getVector() {
            return vector;
        }

        @Override
        public boolean equals(final Object o) {
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            final PrimaryKeyAndVector that = (PrimaryKeyAndVector)o;
            return Objects.equals(getPrimaryKey(), that.getPrimaryKey()) && Objects.equals(getVector(), that.getVector());
        }

        @Override
        public int hashCode() {
            return Objects.hash(getPrimaryKey(), getVector());
        }
    }

    private static class PrimaryKeyVectorAndDistance extends PrimaryKeyAndVector {
        private final double distance;

        public PrimaryKeyVectorAndDistance(@Nonnull final Tuple primaryKey,
                                           @Nonnull final RealVector vector,
                                           final double distance) {
            super(primaryKey, vector);
            this.distance = distance;
        }

        public double getDistance() {
            return distance;
        }

        @Override
        public boolean equals(final Object o) {
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            if (!super.equals(o)) {
                return false;
            }
            final PrimaryKeyVectorAndDistance that = (PrimaryKeyVectorAndDistance)o;
            return Double.compare(getDistance(), that.getDistance()) == 0;
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), getDistance());
        }
    }
}
