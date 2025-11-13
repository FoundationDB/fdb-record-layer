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
import com.apple.foundationdb.Range;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.async.rtree.RTree;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.linear.AffineOperator;
import com.apple.foundationdb.linear.DoubleRealVector;
import com.apple.foundationdb.linear.HalfRealVector;
import com.apple.foundationdb.linear.Metric;
import com.apple.foundationdb.linear.Quantizer;
import com.apple.foundationdb.linear.RealVector;
import com.apple.foundationdb.linear.StoredVecsIterator;
import com.apple.foundationdb.rabitq.EncodedRealVector;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.test.TestDatabaseExtension;
import com.apple.foundationdb.test.TestExecutors;
import com.apple.foundationdb.test.TestSubspaceExtension;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.RandomSeedSource;
import com.apple.test.RandomizedTestUtils;
import com.apple.test.Tags;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.ObjectArrays;
import com.google.common.collect.Sets;
import org.assertj.core.api.Assertions;
import org.assertj.core.util.Lists;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BinaryOperator;
import java.util.function.Function;
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
    TestSubspaceExtension hnswSubspace = new TestSubspaceExtension(dbExtension);
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
                        hnswSubspace.getSubspace(), OnWriteListener.NOOP, OnReadListener.NOOP);
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
                        InliningNode.factory(), hnswSubspace.getSubspace(),
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

    static Stream<Arguments> randomSeedsWithOptions() {
        return RandomizedTestUtils.randomSeeds(0xdeadc0deL)
                .flatMap(seed -> Sets.cartesianProduct(ImmutableSet.of(true, false),
                                ImmutableSet.of(true, false),
                                ImmutableSet.of(true, false),
                                ImmutableSet.of(true, false)).stream()
                        .map(arguments -> Arguments.of(ObjectArrays.concat(seed, arguments.toArray()))));
    }

    @ParameterizedTest(name = "seed={0} useInlining={1} extendCandidates={2} keepPrunedConnections={3} useRaBitQ={4}")
    @MethodSource("randomSeedsWithOptions")
    void testBasicInsert(final long seed, final boolean useInlining, final boolean extendCandidates,
                         final boolean keepPrunedConnections, final boolean useRaBitQ) {
        final Random random = new Random(seed);
        final Metric metric = Metric.EUCLIDEAN_METRIC;
        final AtomicLong nextNodeIdAtomic = new AtomicLong(0L);

        final TestOnWriteListener onWriteListener = new TestOnWriteListener();
        final TestOnReadListener onReadListener = new TestOnReadListener();

        final int numDimensions = 128;
        final HNSW hnsw = new HNSW(hnswSubspace.getSubspace(), TestExecutors.defaultThreadPool(),
                HNSW.newConfigBuilder().setMetric(metric)
                        .setUseInlining(useInlining).setExtendCandidates(extendCandidates)
                        .setKeepPrunedConnections(keepPrunedConnections)
                        .setUseRaBitQ(useRaBitQ)
                        .setRaBitQNumExBits(5)
                        .setSampleVectorStatsProbability(1.0d)
                        .setMaintainStatsProbability(0.1d)
                        .setStatsThreshold(100)
                        .setM(32).setMMax(32).setMMax0(64).build(numDimensions),
                onWriteListener, onReadListener);

        final int k = 50;
        final HalfRealVector queryVector = createRandomHalfVector(random, numDimensions);
        final TreeSet<PrimaryKeyVectorAndDistance> recordsOrderedByDistance =
                new TreeSet<>(Comparator.comparing(PrimaryKeyVectorAndDistance::getDistance));

        for (long i = 0; i < 1000;) {
            final Stats batchStats = insertBatch(hnsw, i, 100,
                    id -> {
                        final var primaryKey = primaryKey(id);
                        final HalfRealVector dataVector = createRandomHalfVector(random, numDimensions);
                        final double distance = metric.distance(dataVector, queryVector);
                        final PrimaryKeyVectorAndDistance record =
                                new PrimaryKeyVectorAndDistance(primaryKey, dataVector, distance);
                        recordsOrderedByDistance.add(record);
                        if (recordsOrderedByDistance.size() > k) {
                            recordsOrderedByDistance.pollLast();
                        }
                        return record;
                    });
            batchStats.logInsertInfo(i);
            i += batchStats.getNumRecords();
        }

        onReadListener.reset();
        final long beginTs = System.nanoTime();
        final List<? extends ResultEntry> results =
                db.run(tr ->
                        hnsw.kNearestNeighborsSearch(tr, k, 100, true, queryVector).join());
        final long endTs = System.nanoTime();

        final ImmutableSet<Tuple> trueNN =
                recordsOrderedByDistance.stream()
                        .map(PrimaryKeyVectorAndDistance::getPrimaryKey)
                        .collect(ImmutableSet.toImmutableSet());

        int recallCount = 0;
        for (ResultEntry resultEntry : results) {
            logger.info("nodeId ={} at distance={}", resultEntry.getPrimaryKey().getLong(0),
                    resultEntry.getDistance());
            if (trueNN.contains(resultEntry.getPrimaryKey())) {
                recallCount ++;
            }
        }
        final double recall = (double)recallCount / (double)k;
        logger.info("search transaction took elapsedTime={}ms; read nodes={}, read bytes={}, recall={}",
                TimeUnit.NANOSECONDS.toMillis(endTs - beginTs),
                onReadListener.getNodesReadByLayer(), onReadListener.getBytesReadByLayer(),
                String.format(Locale.ROOT, "%.2f", recall * 100.0d));
        Assertions.assertThat(recall).isGreaterThan(0.9);

        final Set<Long> insertedIds =
                LongStream.range(0, 1000)
                        .boxed()
                        .collect(Collectors.toSet());

        final Set<Long> readIds = Sets.newHashSet();
        hnsw.scanLayer(db, 0, 100,
                node -> Assertions.assertThat(readIds.add(node.getPrimaryKey().getLong(0))).isTrue());
        Assertions.assertThat(readIds).isEqualTo(insertedIds);

        readIds.clear();
        hnsw.scanLayer(db, 1, 100,
                node -> Assertions.assertThat(readIds.add(node.getPrimaryKey().getLong(0))).isTrue());
        Assertions.assertThat(readIds.size()).isBetween(10, 50);
    }

    @ParameterizedTest()
    @RandomSeedSource({0x0fdbL, 0x5ca1eL, 123456L, 78910L, 1123581321345589L})
    void testBasicInsertWithRaBitQEncodings(final long seed) {
        final Random random = new Random(seed);
        final Metric metric = Metric.EUCLIDEAN_METRIC;

        final AtomicLong nextNodeIdAtomic = new AtomicLong(0L);
        final int numDimensions = 128;
        final HNSW hnsw = new HNSW(hnswSubspace.getSubspace(), TestExecutors.defaultThreadPool(),
                HNSW.newConfigBuilder().setMetric(metric)
                        .setUseRaBitQ(true)
                        .setRaBitQNumExBits(5)
                        .setSampleVectorStatsProbability(1.0d) // every vector is sampled
                        .setMaintainStatsProbability(1.0d) // for every vector we maintain the stats
                        .setStatsThreshold(950) // after 950 vectors we enable RaBitQ
                        .setM(32).setMMax(32).setMMax0(64).build(numDimensions),
                new TestOnWriteListener(), new TestOnReadListener());

        final int k = 499;
        final DoubleRealVector queryVector = createRandomDoubleVector(random, numDimensions);
        final Map<Tuple, RealVector> dataMap = Maps.newHashMap();
        final TreeSet<PrimaryKeyVectorAndDistance> recordsOrderedByDistance =
                new TreeSet<>(Comparator.comparing(PrimaryKeyVectorAndDistance::getDistance));

        for (long i = 0; i < 1000;) {
            final Stats batchStats = insertBatch(hnsw, i, 100,
                    id -> {
                        final var primaryKey = primaryKey(id);
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
            batchStats.logInsertInfo(i);
            i += batchStats.getNumRecords();
        }

        //
        // If we fetch the current state back from the db some vectors are regular vectors and some vectors are
        // RaBitQ encoded. Since that information is not surfaced through the API, we need to scan layer 0, get
        // all vectors directly from disk (encoded/not-encoded, transformed/not-transformed) in order to check
        // that transformations/reconstructions are applied properly.
        //
        final Map<Tuple, RealVector> fromDBMap = Maps.newHashMap();
        hnsw.scanLayer(db, 0, 100,
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

    private Stats insertBatch(@Nonnull final HNSW hnsw, final long startIndex, final int batchSize,
                              @Nonnull final Function<Long, PrimaryKeyAndVector> generatorFunction) {
        final TestOnReadListener onReadListener = (TestOnReadListener)hnsw.getOnReadListener();
        final TestOnWriteListener onWriteListener = (TestOnWriteListener)hnsw.getOnWriteListener();
        return db.run(tr -> {
            onReadListener.reset();
            onWriteListener.reset();
            final long beginTs = System.nanoTime();
            for (int i = 0; i < batchSize; i ++) {
                final var record = generatorFunction.apply(startIndex + i);
                if (record == null) {
                    return new Stats(i, System.nanoTime() - beginTs,
                            onReadListener.getNodesReadByLayer(), onReadListener.getBytesReadByLayer(),
                            onWriteListener.getNodesWrittenByLayer(), onWriteListener.getBytesReadByLayer(),
                            onReadListener.getSumMByLayer(), 0, 0, 0);
                }
                hnsw.insert(tr, record.getPrimaryKey(), record.getVector()).join();
            }
            return new Stats(batchSize, System.nanoTime() - beginTs,
                    onReadListener.getNodesReadByLayer(), onReadListener.getBytesReadByLayer(),
                    onWriteListener.getNodesWrittenByLayer(), onWriteListener.getBytesReadByLayer(),
                    onReadListener.getSumMByLayer(), 0, 0, 0);
        });
    }

    @Test
    //@SuperSlow
    void testSIFTInsertSmall() throws Exception {
        final Metric metric = Metric.EUCLIDEAN_METRIC;
        final int k = 100;
        final AtomicLong nextNodeIdAtomic = new AtomicLong(0L);

        final HNSW hnsw = new HNSW(hnswSubspace.getSubspace(), TestExecutors.defaultThreadPool(),
                HNSW.newConfigBuilder().setUseRaBitQ(true).setRaBitQNumExBits(5)
                        .setMetric(metric).setM(32).setMMax(32).setMMax0(64).build(128),
                new TestOnWriteListener(), new TestOnReadListener());

        final Path siftSmallPath = Paths.get(".out/extracted/siftsmall/siftsmall_base.fvecs");

        final Map<Integer, RealVector> dataMap = Maps.newHashMap();

        try (final var fileChannel = FileChannel.open(siftSmallPath, StandardOpenOption.READ)) {
            final Iterator<DoubleRealVector> vectorIterator = new StoredVecsIterator.StoredFVecsIterator(fileChannel);

            long i = 0;
            final AtomicReference<RealVector> sumReference = new AtomicReference<>(null);
            while (vectorIterator.hasNext()) {
                final Stats batchStats = insertBatch(hnsw, i, 100,
                        id -> {
                            if (!vectorIterator.hasNext()) {
                                return null;
                            }
                            final DoubleRealVector doubleVector = vectorIterator.next();
                            final Tuple currentPrimaryKey = primaryKey(id);
                            final HalfRealVector currentVector = doubleVector.toHalfRealVector();

                            if (sumReference.get() == null) {
                                sumReference.set(currentVector);
                            } else {
                                sumReference.set(sumReference.get().add(currentVector));
                            }

                            dataMap.put(Math.toIntExact(currentPrimaryKey.getLong(0)), currentVector);
                            return new PrimaryKeyAndVector(currentPrimaryKey, currentVector);
                        });
                batchStats.logInsertInfo(i);
                i += batchStats.getNumRecords();
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
                        onReadListener.getNodesReadByLayer(), onReadListener.getBytesReadByLayer());

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

    @Test
    void testSIFTInsert1MClear() {
        clearSubspace(getSameSubspace("testSIFTInsert1M"));
    }

    @Test
    @Timeout(value = 1000, unit = TimeUnit.MINUTES)
    void testSIFTInsert1M() throws Exception {
        final Metric metric = Metric.EUCLIDEAN_METRIC;
        final long skip = 420_000;
        final int statsQueueMaxSize = 10;
        final AtomicLong nextNodeIdAtomic = new AtomicLong(0L);

        final Subspace subspace = getSameSubspace("testSIFTInsert1M");

        final HNSW hnsw = new HNSW(subspace, TestExecutors.defaultThreadPool(),
                HNSW.newConfigBuilder().setUseRaBitQ(true).setRaBitQNumExBits(5)
                        .setMetric(metric).setM(32).setMMax(32).setMMax0(64).build(128),
                new TestOnWriteListener(), new TestOnReadListener());

        final Deque<Stats> insertStatsQueue = new ArrayDeque<>();
        Stats runningInsertStats = null;
        final Deque<Stats> queryStatsQueue = new ArrayDeque<>();
        Stats runningQueryStats = null;

        final Path siftBasePath = Paths.get(".out/downloads/sift_base.fvecs");
        try (final var fileChannel = FileChannel.open(siftBasePath, StandardOpenOption.READ)) {
            final Iterator<DoubleRealVector> vectorIterator = new StoredVecsIterator.StoredFVecsIterator(fileChannel);

            boolean doneSkipping = false;
            long i = 0;
            while (vectorIterator.hasNext()) {
                if (!doneSkipping) {
                    if (i < skip || db.run(transaction ->
                            hnsw.exists(transaction, Tuple.from(nextNodeIdAtomic.get()))).join()) {
                        if (skip >= i) {
                            if (i % 5000 == 0) {
                                logger.info("skipping numRecords = {}", i);
                            }
                        } else {
                            if (i % 10 == 0) {
                                logger.info("skipping records since record exists numRecords = {}", i);
                            }
                        }

                        vectorIterator.next();
                        i++;
                        nextNodeIdAtomic.set(i);
                        continue;
                    }

                    doneSkipping = true;
                    logger.info("done skipping numRecords = {}", i);
                }

                final Stats insertStats = insertBatch(hnsw, i, 10,
                        id -> {
                            if (!vectorIterator.hasNext()) {
                                return null;
                            }
                            final DoubleRealVector doubleVector = vectorIterator.next();
                            final Tuple currentPrimaryKey = primaryKey(id);
                            final HalfRealVector currentVector = doubleVector.toHalfRealVector();

                            return new PrimaryKeyAndVector(currentPrimaryKey, currentVector);
                        });
                insertStats.logInsertTrace(i);

                if (runningInsertStats == null) {
                    runningInsertStats = insertStats;
                    insertStatsQueue.addFirst(insertStats);
                } else {
                    while (insertStatsQueue.size() >= statsQueueMaxSize) {
                        final Stats removedStats = insertStatsQueue.removeLast();
                        runningInsertStats = runningInsertStats.remove(removedStats);
                    }
                    insertStatsQueue.addFirst(insertStats);
                    runningInsertStats = runningInsertStats.add(insertStats);
                }
                i += insertStats.getNumRecords();

                if (i % 1000 == 0) {
                    runningInsertStats.logInsertAveragesInfo(i - 1000);

                    final Stats queryStats = querySIFT(hnsw, i);
                    if (queryStats != null) {
                        if (runningQueryStats == null) {
                            runningQueryStats = queryStats;
                            queryStatsQueue.addFirst(queryStats);
                        } else {
                            while (queryStatsQueue.size() >= statsQueueMaxSize) {
                                final Stats removedStats = queryStatsQueue.removeLast();
                                runningQueryStats = runningQueryStats.remove(removedStats);
                            }
                            queryStatsQueue.addFirst(queryStats);
                            runningQueryStats = runningQueryStats.add(queryStats);
                        }

                        runningQueryStats.logQueryAveragesInfo(i);
                    }
                }
            }
        }
    }

    @Test
    void testSIFTQuery1M() throws Exception {
        final Subspace subspace = getSameSubspace("testSIFTInsert1M");
        final Metric metric = Metric.EUCLIDEAN_METRIC;

        final HNSW hnsw = new HNSW(subspace, TestExecutors.defaultThreadPool(),
                HNSW.newConfigBuilder().setUseRaBitQ(true).setRaBitQNumExBits(5)
                        .setMetric(metric).setM(32).setMMax(32).setMMax0(64).build(128),
                new TestOnWriteListener(), new TestOnReadListener());

        final Stats queryStats = Objects.requireNonNull(querySIFT(hnsw, 430000));
        queryStats.logQueryAveragesInfo(430000);
    }

    @Nullable
    private Stats querySIFT(@Nonnull final HNSW hnsw, final long numInserted) throws IOException {
        final Path siftGroundTruthPath = Paths.get(".out/downloads/sift_groundtruth.ivecs");
        final Path siftQueryPath = Paths.get(".out/downloads/sift_query.fvecs");

        final TestOnReadListener onReadListener = (TestOnReadListener)hnsw.getOnReadListener();

        Stats stats = null;
        try (final var queryChannel = FileChannel.open(siftQueryPath, StandardOpenOption.READ);
                final var groundTruthChannel = FileChannel.open(siftGroundTruthPath, StandardOpenOption.READ)) {
            final Iterator<DoubleRealVector> queryIterator = new StoredVecsIterator.StoredFVecsIterator(queryChannel);
            final Iterator<List<Integer>> groundTruthIterator = new StoredVecsIterator.StoredIVecsIterator(groundTruthChannel);

            Verify.verify(queryIterator.hasNext() == groundTruthIterator.hasNext());

            int i = 0;
            while (queryIterator.hasNext()) {
                final DoubleRealVector queryVector = queryIterator.next();
                final Set<Integer> groundTruthIndices = Sets.newHashSet(groundTruthIterator.next());
                // remove all indexes for items not yet inserted
                groundTruthIndices.removeIf(id -> id >= numInserted);
                if (groundTruthIndices.isEmpty()) {
                    continue;
                }

                onReadListener.reset();
                final long beginTs = System.nanoTime();
                final List<? extends ResultEntry> results =
                        db.run(tr -> hnsw.kNearestNeighborsSearch(tr, groundTruthIndices.size(),
                                efSearchFromK(groundTruthIndices.size()), true, queryVector).join());
                final long endTs = System.nanoTime();

                int recallCount = 0;
                for (final ResultEntry resultEntry : results) {
                    final int primaryKeyIndex = (int)resultEntry.getPrimaryKey().getLong(0);
                    if (groundTruthIndices.contains(primaryKeyIndex)) {
                        recallCount ++;
                    }
                }

                final Stats currentStats =
                        new Stats(0L, endTs - beginTs,
                                onReadListener.getNodesReadByLayer(), onReadListener.getBytesReadByLayer(),
                                ImmutableMap.of(), ImmutableMap.of(), ImmutableMap.of(), 1,
                                groundTruthIndices.size(), recallCount);

                if (stats == null) {
                    stats = currentStats;
                } else {
                    stats = stats.add(currentStats);
                }

                i ++;
                if (i >= 100) {
                    break;
                }
            }
        }
        return stats;
    }

    private static int efSearchFromK(final int k) {
        return 100;
        //return Math.min(Math.max(4 * k, 64), 400);
    }

    @Nonnull
    private Subspace getSameSubspace(@Nonnull final String name) {
        return dbExtension.getDatabase().runAsync(tr ->
                DirectoryLayer.getDefault().createOrOpen(tr, List.of("fdb-extensions-test"))
                        .thenApply(directorySubspace -> directorySubspace.subspace(Tuple.from(name)))
        ).join();
    }

    private void clearSubspace(@Nonnull final Subspace subspace) {
        dbExtension.getDatabase().run(tx -> {
            tx.clear(Range.startsWith(subspace.pack()));
            return null;
        });
    }

    private <N extends NodeReference> void writeNode(@Nonnull final Transaction transaction,
                                                     @Nonnull final StorageAdapter<N> storageAdapter,
                                                     @Nonnull final AbstractNode<N> node,
                                                     final int layer) {
        final NeighborsChangeSet<N> insertChangeSet =
                new InsertNeighborsChangeSet<>(new BaseNeighborsChangeSet<>(ImmutableList.of()),
                        node.getNeighbors());
        storageAdapter.writeNode(transaction, Quantizer.noOpQuantizer(Metric.EUCLIDEAN_METRIC), node, layer,
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
        return Tuple.from(random.nextLong());
    }

    @Nonnull
    private static Tuple primaryKey(final long id) {
        return Tuple.from(id);
    }

    private static class TestOnReadListener implements OnReadListener {
        @Nonnull
        private final Map<Integer, Long> nodesReadByLayer;
        @Nonnull
        private final Map<Integer, Long> bytesReadByLayer;
        @Nonnull
        private final Map<Integer, Long> sumMByLayer;

        public TestOnReadListener() {
            this.nodesReadByLayer = Maps.newConcurrentMap();
            this.bytesReadByLayer = Maps.newConcurrentMap();
            this.sumMByLayer = Maps.newConcurrentMap();
        }

        @Nonnull
        public Map<Integer, Long> getNodesReadByLayer() {
            return nodesReadByLayer;
        }

        @Nonnull
        public Map<Integer, Long> getBytesReadByLayer() {
            return bytesReadByLayer;
        }

        @Nonnull
        public Map<Integer, Long> getSumMByLayer() {
            return sumMByLayer;
        }

        public void reset() {
            nodesReadByLayer.clear();
            bytesReadByLayer.clear();
            sumMByLayer.clear();
        }

        @Override
        public void onNodeRead(final int layer, @Nonnull final Node<? extends NodeReference> node) {
            nodesReadByLayer.compute(layer, (l, oldValue) -> (oldValue == null ? 0 : oldValue) + 1L);
            sumMByLayer.compute(layer, (l, oldValue) -> (oldValue == null ? 0 : oldValue) + node.getNeighbors().size());
        }

        @Override
        public void onKeyValueRead(final int layer, @Nonnull final byte[] key, @Nullable final byte[] value) {
            bytesReadByLayer.compute(layer, (l, oldValue) -> (oldValue == null ? 0 : oldValue) +
                    key.length + (value == null ? 0 : value.length));
        }
    }

    private static class TestOnWriteListener implements OnWriteListener {
        @Nonnull
        private final Map<Integer, Long> nodesWrittenByLayer;
        @Nonnull
        private final Map<Integer, Long> bytesWrittenByLayer;

        public TestOnWriteListener() {
            this.nodesWrittenByLayer = Maps.newConcurrentMap();
            this.bytesWrittenByLayer = Maps.newConcurrentMap();
        }

        @Nonnull
        public Map<Integer, Long> getNodesWrittenByLayer() {
            return nodesWrittenByLayer;
        }

        @Nonnull
        public Map<Integer, Long> getBytesReadByLayer() {
            return bytesWrittenByLayer;
        }

        public void reset() {
            nodesWrittenByLayer.clear();
            bytesWrittenByLayer.clear();
        }

        @Override
        public void onNodeWritten(final int layer, @Nonnull final Node<? extends NodeReference> node) {
            nodesWrittenByLayer.compute(layer, (l, oldValue) -> (oldValue == null ? 0 : oldValue) + 1L);
        }

        @Override
        public void onKeyValueWritten(final int layer, @Nonnull final byte[] key, @Nonnull final byte[] value) {
            bytesWrittenByLayer.compute(layer, (l, oldValue) -> (oldValue == null ? 0 : oldValue) +
                    key.length + value.length);
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
    }

    private static class Stats {
        private final long numRecords;
        private final long elapsedTimeNs;
        @Nonnull
        private final Map<Integer, Long> nodesReadByLayerMap;
        @Nonnull
        private final Map<Integer, Long> bytesReadByLayerMap;
        @Nonnull
        private final Map<Integer, Long> nodesWrittenByLayerMap;
        @Nonnull
        private final Map<Integer, Long> bytesWrittenByLayerMap;
        @Nonnull
        @SuppressWarnings("checkstyle:MemberName")
        private final Map<Integer, Long> mByLayerMap;
        private final long numQueries;
        private final long numResults;
        private final long numRecall;

        public Stats(final long numRecords, final long elapsedTimeNs,
                     @Nonnull final Map<Integer, Long> nodesReadByLayerMap,
                     @Nonnull final Map<Integer, Long> bytesReadByLayerMap,
                     @Nonnull final Map<Integer, Long> nodesWrittenByLayerMap,
                     @Nonnull final Map<Integer, Long> bytesWrittenByLayerMap,
                     @Nonnull final Map<Integer, Long> mByLayerMap,
                     final long numQueries,
                     final long numResults,
                     final long numRecall) {
            this.numRecords = numRecords;
            this.elapsedTimeNs = elapsedTimeNs;
            this.nodesReadByLayerMap = ImmutableMap.copyOf(nodesReadByLayerMap);
            this.bytesReadByLayerMap = ImmutableMap.copyOf(bytesReadByLayerMap);
            this.nodesWrittenByLayerMap = ImmutableMap.copyOf(nodesWrittenByLayerMap);
            this.bytesWrittenByLayerMap = ImmutableMap.copyOf(bytesWrittenByLayerMap);
            this.mByLayerMap = ImmutableMap.copyOf(mByLayerMap);
            this.numQueries = numQueries;
            this.numResults = numResults;
            this.numRecall = numRecall;
        }

        public long getNumRecords() {
            return numRecords;
        }

        public long getElapsedTimeNs() {
            return elapsedTimeNs;
        }

        @Nonnull
        public Map<Integer, Long> getNodesReadByLayerMap() {
            return nodesReadByLayerMap;
        }

        @Nonnull
        public Map<Integer, Long> getBytesReadByLayerMap() {
            return bytesReadByLayerMap;
        }

        @Nonnull
        public Map<Integer, Long> getNodesWrittenByLayerMap() {
            return nodesWrittenByLayerMap;
        }

        @Nonnull
        public Map<Integer, Long> getBytesWrittenByLayerMap() {
            return bytesWrittenByLayerMap;
        }

        @Nonnull
        public Map<Integer, Long> getMByLayerMap() {
            return mByLayerMap;
        }

        public long getNumQueries() {
            return numQueries;
        }

        public long getNumResults() {
            return numResults;
        }

        public long getNumRecall() {
            return numRecall;
        }

        @Nonnull
        public Stats add(@Nonnull final Stats other) {
            return new Stats(getNumRecords() + other.getNumRecords(),
                    getElapsedTimeNs() + other.getElapsedTimeNs(),
                    aggregateMap(getNodesReadByLayerMap(), other.getNodesReadByLayerMap(), Long::sum),
                    aggregateMap(getBytesReadByLayerMap(), other.getBytesReadByLayerMap(), Long::sum),
                    aggregateMap(getNodesWrittenByLayerMap(), other.getNodesWrittenByLayerMap(), Long::sum),
                    aggregateMap(getBytesWrittenByLayerMap(), other.getBytesWrittenByLayerMap(), Long::sum),
                    aggregateMap(getMByLayerMap(), other.getMByLayerMap(), Long::sum),
                    getNumQueries() + other.getNumQueries(),
                    getNumResults() + other.getNumResults(),
                    getNumRecall() + other.getNumRecall());
        }

        @Nonnull
        public Stats remove(@Nonnull final Stats other) {
            return new Stats(getNumRecords() - other.getNumRecords(),
                    getElapsedTimeNs() - other.getElapsedTimeNs(),
                    aggregateMap(getNodesReadByLayerMap(), other.getNodesReadByLayerMap(), (l, r) -> l - r),
                    aggregateMap(getBytesReadByLayerMap(), other.getBytesReadByLayerMap(), (l, r) -> l - r),
                    aggregateMap(getNodesWrittenByLayerMap(), other.getNodesWrittenByLayerMap(), (l, r) -> l - r),
                    aggregateMap(getBytesWrittenByLayerMap(), other.getBytesWrittenByLayerMap(), (l, r) -> l - r),
                    aggregateMap(getMByLayerMap(), other.getMByLayerMap(), (l, r) -> l - r),
                    getNumQueries() - other.getNumQueries(),
                    getNumResults() - other.getNumResults(),
                    getNumRecall() - other.getNumRecall());
        }

        public void logInsertInfo(final long index) {
            if (logger.isInfoEnabled()) {
                logger.info(getInsertLogMessage(index));
            }
        }

        public void logInsertTrace(final long index) {
            if (logger.isTraceEnabled()) {
                logger.trace(getInsertLogMessage(index));
            }
        }

        @Nonnull
        private String getInsertLogMessage(final long index) {
            return String.format("inserted batchSize=%d records starting at nodeId=%d took elapsedTime=%dms, nodesRead=%s, bytesRead=%s",
                    getNumRecords(), index, TimeUnit.NANOSECONDS.toMillis(getElapsedTimeNs()),
                    getNodesReadByLayerMap(), getBytesReadByLayerMap());
        }

        public void logInsertAveragesInfo(final long index) {
            if (logger.isInfoEnabled()) {
                logger.info(getInsertAveragesLogMessage(index));
            }
        }

        @Nonnull
        private String getInsertAveragesLogMessage(final long index) {
            //return String.format("after inserting %d records starting at nodeId=%d; elapsedTime=%dms, nodesRead=%d, bytesRead=%d, nodesWritten=%d, bytesWritten=%d, m=%d, nodesRead=%s, bytesRead=%s, nodesWritten=%s, bytesWritten=%s, m=%s",
            return String.format("i %d,%d,%d,%d,%d,%d,%d,%d,%s,%s,%s,%s,%s",
                    getNumRecords(), index,
                    TimeUnit.NANOSECONDS.toMillis(getElapsedTimeNs() / getNumRecords()),
                    sumMap(getNodesReadByLayerMap()) / getNumRecords(),
                    sumMap(getBytesReadByLayerMap()) / getNumRecords(),
                    sumMap(getNodesWrittenByLayerMap()) / getNumRecords(),
                    sumMap(getBytesWrittenByLayerMap()) / getNumRecords(),
                    sumMap(getMByLayerMap()) / sumMap(getNodesReadByLayerMap()),
                    averageOfMap(getNodesReadByLayerMap(), getNumRecords()),
                    averageOfMap(getBytesReadByLayerMap(), getNumRecords()),
                    averageOfMap(getNodesWrittenByLayerMap(), getNumRecords()),
                    averageOfMap(getBytesWrittenByLayerMap(), getNumRecords()),
                    averageOfMap(getMByLayerMap(), getNodesReadByLayerMap()));
        }

        public void logQueryAveragesInfo(final long index) {
            if (logger.isInfoEnabled()) {
                logger.info(getQueryAveragesLogMessage(index));
            }
        }

        @Nonnull
        private String getQueryAveragesLogMessage(final long index) {
            //return String.format("querying, num=%d; averages after inserting %d records took elapsedTime=%dms, recall=%.2f, nodesRead=%d, bytesRead=%d, nodesRead=%s, bytesRead=%s",
            return String.format("%d,%d,%d,%.2f,%d,%d,%s,%s",
                    getNumQueries(),
                    index,
                    TimeUnit.NANOSECONDS.toMillis(getElapsedTimeNs() / getNumQueries()),
                    (double)getNumRecall() * 100.0d / getNumResults(),
                    sumMap(getNodesReadByLayerMap()) / getNumQueries(),
                    sumMap(getBytesReadByLayerMap()) / getNumQueries(),
                    averageOfMap(getNodesReadByLayerMap(), getNumQueries()),
                    averageOfMap(getBytesReadByLayerMap(), getNumQueries()));
        }

        @Nonnull
        private static Map<Integer, Long> aggregateMap(@Nonnull final Map<Integer, Long> map1,
                                                       @Nonnull final Map<Integer, Long> map2,
                                                       @Nonnull final BinaryOperator<Long> operator) {
            final ImmutableMap.Builder<Integer, Long> resultBuilder = ImmutableMap.builder();

            for (final Map.Entry<Integer, Long> entry1 : map1.entrySet()) {
                if (map2.containsKey(entry1.getKey())) {
                    resultBuilder.put(entry1.getKey(), operator.apply(entry1.getValue(), map2.get(entry1.getKey())));
                } else {
                    resultBuilder.put(entry1);
                }
            }

            for (final Map.Entry<Integer, Long> entry2 : map2.entrySet()) {
                if (!map1.containsKey(entry2.getKey())) {
                    resultBuilder.put(entry2);
                }
            }

            return resultBuilder.build();
        }

        @Nonnull
        private static Map<Integer, Long> averageOfMap(@Nonnull final Map<Integer, Long> map, final long numRecords) {
            final ImmutableMap.Builder<Integer, Long> resultBuilder = ImmutableMap.builder();
            for (final Map.Entry<Integer, Long> entry : map.entrySet()) {
                resultBuilder.put(entry.getKey(), entry.getValue() / numRecords);
            }
            return resultBuilder.build();
        }

        @Nonnull
        private static Map<Integer, Long> averageOfMap(@Nonnull final Map<Integer, Long> dividentMap,
                                                       @Nonnull final Map<Integer, Long> divisorMap) {
            final ImmutableMap.Builder<Integer, Long> resultBuilder = ImmutableMap.builder();
            for (final Map.Entry<Integer, Long> entry : dividentMap.entrySet()) {
                resultBuilder.put(entry.getKey(), entry.getValue() / divisorMap.get(entry.getKey()));
            }
            return resultBuilder.build();
        }

        private static long sumMap(@Nonnull final Map<Integer, Long> map) {
            return map.values().stream().mapToLong(v -> v).sum();
        }
    }
}
