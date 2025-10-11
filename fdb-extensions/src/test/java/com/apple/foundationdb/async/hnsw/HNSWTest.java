/*
 * HNSWTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2023 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.test.TestDatabaseExtension;
import com.apple.foundationdb.test.TestExecutors;
import com.apple.foundationdb.test.TestSubspaceExtension;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.Tags;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.ObjectArrays;
import com.google.common.collect.Sets;
import org.assertj.core.util.Lists;
import org.junit.jupiter.api.Assertions;
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
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;

/**
 * Tests testing insert/update/deletes of data into/in/from {@link RTree}s.
 */
@Execution(ExecutionMode.CONCURRENT)
@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
@Tag(Tags.RequiresFDB)
@Tag(Tags.Slow)
public class HNSWTest {
    private static final Logger logger = LoggerFactory.getLogger(HNSWTest.class);
    private static final int NUM_TEST_RUNS = 5;
    private static final int NUM_SAMPLES = 10_000;

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

    private static Stream<Long> randomSeeds() {
        return LongStream.generate(() -> new Random().nextLong())
                .limit(5)
                .boxed();
    }

    @ParameterizedTest(name = "seed={0}")
    @MethodSource("randomSeeds")
    public void testCompactSerialization(final long seed) {
        final Random random = new Random(seed);
        final CompactStorageAdapter storageAdapter =
                new CompactStorageAdapter(HNSW.DEFAULT_CONFIG, CompactNode.factory(), rtSubspace.getSubspace(),
                        OnWriteListener.NOOP, OnReadListener.NOOP);
        final Node<NodeReference> originalNode =
                db.run(tr -> {
                    final NodeFactory<NodeReference> nodeFactory = storageAdapter.getNodeFactory();

                    final Node<NodeReference> randomCompactNode =
                            createRandomCompactNode(random, nodeFactory, 768, 16);

                    writeNode(tr, storageAdapter, randomCompactNode, 0);
                    return randomCompactNode;
                });

        db.run(tr -> storageAdapter.fetchNode(tr, 0, originalNode.getPrimaryKey())
                .thenAccept(node -> {
                    Assertions.assertAll(
                            () -> Assertions.assertInstanceOf(CompactNode.class, node),
                            () -> Assertions.assertEquals(NodeKind.COMPACT, node.getKind()),
                            () -> Assertions.assertEquals(node.getPrimaryKey(), originalNode.getPrimaryKey()),
                            () -> Assertions.assertEquals(node.asCompactNode().getVector(),
                                    originalNode.asCompactNode().getVector()),
                            () -> {
                                final ArrayList<NodeReference> neighbors =
                                        Lists.newArrayList(node.getNeighbors());
                                neighbors.sort(Comparator.comparing(NodeReference::getPrimaryKey));
                                final ArrayList<NodeReference> originalNeighbors =
                                        Lists.newArrayList(originalNode.getNeighbors());
                                originalNeighbors.sort(Comparator.comparing(NodeReference::getPrimaryKey));
                                Assertions.assertEquals(neighbors, originalNeighbors);
                            }
                    );
                }).join());
    }

    @ParameterizedTest(name = "seed={0}")
    @MethodSource("randomSeeds")
    public void testInliningSerialization(final long seed) {
        final Random random = new Random(seed);
        final InliningStorageAdapter storageAdapter =
                new InliningStorageAdapter(HNSW.DEFAULT_CONFIG, InliningNode.factory(), rtSubspace.getSubspace(),
                        OnWriteListener.NOOP, OnReadListener.NOOP);
        final Node<NodeReferenceWithVector> originalNode =
                db.run(tr -> {
                    final NodeFactory<NodeReferenceWithVector> nodeFactory = storageAdapter.getNodeFactory();

                    final Node<NodeReferenceWithVector> randomInliningNode =
                            createRandomInliningNode(random, nodeFactory, 768, 16);

                    writeNode(tr, storageAdapter, randomInliningNode, 0);
                    return randomInliningNode;
                });

        db.run(tr -> storageAdapter.fetchNode(tr, 0, originalNode.getPrimaryKey())
                .thenAccept(node -> Assertions.assertAll(
                        () -> Assertions.assertInstanceOf(InliningNode.class, node),
                        () -> Assertions.assertEquals(NodeKind.INLINING, node.getKind()),
                        () -> Assertions.assertEquals(node.getPrimaryKey(), originalNode.getPrimaryKey()),
                        () -> {
                            final ArrayList<NodeReference> neighbors =
                                    Lists.newArrayList(node.getNeighbors());
                            neighbors.sort(Comparator.comparing(NodeReference::getPrimaryKey)); // should not be necessary the way it is stored
                            final ArrayList<NodeReference> originalNeighbors =
                                    Lists.newArrayList(originalNode.getNeighbors());
                            originalNeighbors.sort(Comparator.comparing(NodeReference::getPrimaryKey));
                            Assertions.assertEquals(neighbors, originalNeighbors);
                        }
                )).join());
    }

    static Stream<Arguments> randomSeedsWithOptions() {
        return Sets.cartesianProduct(ImmutableSet.of(true, false),
                        ImmutableSet.of(true, false),
                        ImmutableSet.of(true, false))
                .stream()
                .flatMap(arguments ->
                        LongStream.generate(() -> new Random().nextLong())
                                .limit(2)
                                .mapToObj(seed -> Arguments.of(ObjectArrays.concat(seed, arguments.toArray()))));
    }

    @ParameterizedTest(name = "seed={0} useInlining={1} extendCandidates={2} keepPrunedConnections={3}")
    @MethodSource("randomSeedsWithOptions")
    public void testBasicInsert(final long seed, final boolean useInlining, final boolean extendCandidates,
                                final boolean keepPrunedConnections) {
        final Random random = new Random(seed);
        final Metrics metric = Metrics.EUCLIDEAN_METRIC;
        final AtomicLong nextNodeIdAtomic = new AtomicLong(0L);

        final TestOnReadListener onReadListener = new TestOnReadListener();

        final int dimensions = 128;
        final HNSW hnsw = new HNSW(rtSubspace.getSubspace(), TestExecutors.defaultThreadPool(),
                HNSW.DEFAULT_CONFIG.toBuilder().setMetric(metric)
                        .setUseInlining(useInlining).setExtendCandidates(extendCandidates)
                        .setKeepPrunedConnections(keepPrunedConnections)
                        .setM(32).setMMax(32).setMMax0(64).build(),
                OnWriteListener.NOOP, onReadListener);

        final int k = 10;
        final HalfVector queryVector = VectorTest.createRandomHalfVector(random, dimensions);
        final TreeSet<NodeReferenceWithDistance> nodesOrderedByDistance =
                new TreeSet<>(Comparator.comparing(NodeReferenceWithDistance::getDistance));

        for (int i = 0; i < 1000;) {
            i += basicInsertBatch(hnsw, 100, nextNodeIdAtomic, onReadListener,
                    tr -> {
                        final var primaryKey = createNextPrimaryKey(nextNodeIdAtomic);
                        final HalfVector dataVector = VectorTest.createRandomHalfVector(random, dimensions);
                        final double distance = Vector.comparativeDistance(metric, dataVector, queryVector);
                        final NodeReferenceWithDistance nodeReferenceWithDistance =
                                new NodeReferenceWithDistance(primaryKey, dataVector, distance);
                        nodesOrderedByDistance.add(nodeReferenceWithDistance);
                        if (nodesOrderedByDistance.size() > k) {
                            nodesOrderedByDistance.pollLast();
                        }
                        return nodeReferenceWithDistance;
                    });
        }

        onReadListener.reset();
        final long beginTs = System.nanoTime();
        final List<? extends NodeReferenceAndNode<?>> results =
                db.run(tr -> hnsw.kNearestNeighborsSearch(tr, k, 100, queryVector).join());
        final long endTs = System.nanoTime();

        final ImmutableSet<Tuple> trueNN =
                ImmutableSet.copyOf(NodeReference.primaryKeys(nodesOrderedByDistance));

        int recallCount = 0;
        for (NodeReferenceAndNode<?> nodeReferenceAndNode : results) {
            final NodeReferenceWithDistance nodeReferenceWithDistance = nodeReferenceAndNode.getNodeReferenceWithDistance();
            logger.info("nodeId ={} at distance={}", nodeReferenceWithDistance.getPrimaryKey().getLong(0),
                    nodeReferenceWithDistance.getDistance());
            if (trueNN.contains(nodeReferenceAndNode.getNode().getPrimaryKey())) {
                recallCount ++;
            }
        }
        final double recall = (double)recallCount / (double)k;
        Assertions.assertTrue(recall > 0.93);

        logger.info("search transaction took elapsedTime={}ms; read nodes={}, read bytes={}, recall={}",
                TimeUnit.NANOSECONDS.toMillis(endTs - beginTs),
                onReadListener.getNodeCountByLayer(), onReadListener.getBytesReadByLayer(),
                String.format(Locale.ROOT, "%.2f", recall * 100.0d));

        final Set<Long> usedIds =
                LongStream.range(0, 1000)
                        .boxed()
                        .collect(Collectors.toSet());

        hnsw.scanLayer(db, 0, 100, node -> Assertions.assertTrue(usedIds.remove(node.getPrimaryKey().getLong(0))));
    }

    private int basicInsertBatch(final HNSW hnsw, final int batchSize,
                                 @Nonnull final AtomicLong nextNodeIdAtomic, @Nonnull final TestOnReadListener onReadListener,
                                 @Nonnull final Function<Transaction, NodeReferenceWithVector> insertFunction) {
        return db.run(tr -> {
            onReadListener.reset();
            final long nextNodeId = nextNodeIdAtomic.get();
            final long beginTs = System.nanoTime();
            for (int i = 0; i < batchSize; i ++) {
                final var newNodeReference = insertFunction.apply(tr);
                if (newNodeReference == null) {
                    return i;
                }
                hnsw.insert(tr, newNodeReference).join();
            }
            final long endTs = System.nanoTime();
            logger.info("inserted batchSize={} records starting at nodeId={} took elapsedTime={}ms, readCounts={}, MSums={}",
                    batchSize, nextNodeId, TimeUnit.NANOSECONDS.toMillis(endTs - beginTs),
                    onReadListener.getNodeCountByLayer(), onReadListener.getSumMByLayer());
            return batchSize;
        });
    }

    private int insertBatch(final HNSW hnsw, final int batchSize,
                            @Nonnull final AtomicLong nextNodeIdAtomic, @Nonnull final TestOnReadListener onReadListener,
                            @Nonnull final Function<Transaction, NodeReferenceWithVector> insertFunction) {
        return db.run(tr -> {
            onReadListener.reset();
            final long nextNodeId = nextNodeIdAtomic.get();
            final long beginTs = System.nanoTime();
            final ImmutableList.Builder<NodeReferenceWithVector> nodeReferenceWithVectorBuilder =
                    ImmutableList.builder();
            for (int i = 0; i < batchSize; i ++) {
                final var newNodeReference = insertFunction.apply(tr);
                if (newNodeReference != null) {
                    nodeReferenceWithVectorBuilder.add(newNodeReference);
                }
            }
            hnsw.insertBatch(tr, nodeReferenceWithVectorBuilder.build()).join();
            final long endTs = System.nanoTime();
            logger.info("inserted batch batchSize={} records starting at nodeId={} took elapsedTime={}ms, readCounts={}, MSums={}",
                    batchSize, nextNodeId, TimeUnit.NANOSECONDS.toMillis(endTs - beginTs),
                    onReadListener.getNodeCountByLayer(), onReadListener.getSumMByLayer());
            return batchSize;
        });
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.MINUTES)
    public void testSIFTInsertSmall() throws Exception {
        final Metrics metric = Metrics.EUCLIDEAN_METRIC;
        final int k = 100;
        final AtomicLong nextNodeIdAtomic = new AtomicLong(0L);

        final TestOnReadListener onReadListener = new TestOnReadListener();

        final HNSW hnsw = new HNSW(rtSubspace.getSubspace(), TestExecutors.defaultThreadPool(),
                HNSW.DEFAULT_CONFIG.toBuilder().setMetric(metric).setM(32).setMMax(32).setMMax0(64).build(),
                OnWriteListener.NOOP, onReadListener);

        final Path siftSmallPath = Paths.get(".out/extracted/siftsmall/siftsmall_base.fvecs");

        try (final var fileChannel = FileChannel.open(siftSmallPath, StandardOpenOption.READ)) {
            final Iterator<DoubleVector> vectorIterator = new StoredVecsIterator.StoredFVecsIterator(fileChannel);

            int i = 0;
            while (vectorIterator.hasNext()) {
                i += basicInsertBatch(hnsw, 100, nextNodeIdAtomic, onReadListener,
                        tr -> {
                            if (!vectorIterator.hasNext()) {
                                return null;
                            }
                            final DoubleVector doubleVector = vectorIterator.next();
                            final Tuple currentPrimaryKey = createNextPrimaryKey(nextNodeIdAtomic);
                            final HalfVector currentVector = doubleVector.toHalfVector();
                            return new NodeReferenceWithVector(currentPrimaryKey, currentVector);
                        });
            }
        }

        validateSIFTSmall(hnsw, k);
    }

    private void validateSIFTSmall(@Nonnull final HNSW hnsw, final int k) throws IOException {
        final Path siftSmallGroundTruthPath = Paths.get(".out/extracted/siftsmall/siftsmall_groundtruth.ivecs");
        final Path siftSmallQueryPath = Paths.get(".out/extracted/siftsmall/siftsmall_query.fvecs");

        final TestOnReadListener onReadListener = (TestOnReadListener)hnsw.getOnReadListener();

        try (final var queryChannel = FileChannel.open(siftSmallQueryPath, StandardOpenOption.READ);
                final var groundTruthChannel = FileChannel.open(siftSmallGroundTruthPath, StandardOpenOption.READ)) {
            final Iterator<DoubleVector> queryIterator = new StoredVecsIterator.StoredFVecsIterator(queryChannel);
            final Iterator<List<Integer>> groundTruthIterator = new StoredVecsIterator.StoredIVecsIterator(groundTruthChannel);

            Verify.verify(queryIterator.hasNext() == groundTruthIterator.hasNext());

            while (queryIterator.hasNext()) {
                final HalfVector queryVector = queryIterator.next().toHalfVector();
                final Set<Integer> groundTruthIndices = ImmutableSet.copyOf(groundTruthIterator.next());
                onReadListener.reset();
                final long beginTs = System.nanoTime();
                final List<? extends NodeReferenceAndNode<?>> results =
                        db.run(tr -> hnsw.kNearestNeighborsSearch(tr, k, 100, queryVector).join());
                final long endTs = System.nanoTime();
                logger.info("retrieved result in elapsedTimeMs={}, reading numNodes={}, readBytes={}",
                        TimeUnit.NANOSECONDS.toMillis(endTs - beginTs),
                        onReadListener.getNodeCountByLayer(), onReadListener.getBytesReadByLayer());

                int recallCount = 0;
                for (NodeReferenceAndNode<?> nodeReferenceAndNode : results) {
                    final NodeReferenceWithDistance nodeReferenceWithDistance =
                            nodeReferenceAndNode.getNodeReferenceWithDistance();
                    final int primaryKeyIndex = (int)nodeReferenceWithDistance.getPrimaryKey().getLong(0);
                    logger.trace("retrieved result nodeId = {} at distance = {} ",
                            primaryKeyIndex, nodeReferenceWithDistance.getDistance());
                    if (groundTruthIndices.contains(primaryKeyIndex)) {
                        recallCount ++;
                    }
                }

                final double recall = (double)recallCount / k;
                Assertions.assertTrue(recall > 0.93);

                logger.info("query returned results recall={}", String.format(Locale.ROOT, "%.2f", recall * 100.0d));
            }
        }
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.MINUTES)
    public void testSIFTInsertSmallUsingBatchAPI() throws Exception {
        final Metrics metric = Metrics.EUCLIDEAN_METRIC;
        final int k = 100;
        final AtomicLong nextNodeIdAtomic = new AtomicLong(0L);

        final TestOnReadListener onReadListener = new TestOnReadListener();

        final HNSW hnsw = new HNSW(rtSubspace.getSubspace(), TestExecutors.defaultThreadPool(),
                HNSW.DEFAULT_CONFIG.toBuilder().setMetric(metric).setM(32).setMMax(32).setMMax0(64).build(),
                OnWriteListener.NOOP, onReadListener);

        final Path siftSmallPath = Paths.get(".out/extracted/siftsmall/siftsmall_base.fvecs");

        try (final var fileChannel = FileChannel.open(siftSmallPath, StandardOpenOption.READ)) {
            final Iterator<DoubleVector> vectorIterator = new StoredVecsIterator.StoredFVecsIterator(fileChannel);

            int i = 0;
            while (vectorIterator.hasNext()) {
                i += insertBatch(hnsw, 100, nextNodeIdAtomic, onReadListener,
                        tr -> {
                            if (!vectorIterator.hasNext()) {
                                return null;
                            }
                            final DoubleVector doubleVector = vectorIterator.next();
                            final Tuple currentPrimaryKey = createNextPrimaryKey(nextNodeIdAtomic);
                            final HalfVector currentVector = doubleVector.toHalfVector();
                            return new NodeReferenceWithVector(currentPrimaryKey, currentVector);
                        });
            }
        }
        validateSIFTSmall(hnsw, k);
    }

    @Test
    public void testManyRandomVectors() {
        final Random random = new Random();
        for (long l = 0L; l < 3000000; l ++) {
            final HalfVector randomVector = VectorTest.createRandomHalfVector(random, 768);
            final Tuple vectorTuple = StorageAdapter.tupleFromVector(randomVector);
            final Vector roundTripVector = StorageAdapter.vectorFromTuple(HNSW.DEFAULT_CONFIG, vectorTuple);
            Vector.comparativeDistance(Metrics.EUCLIDEAN_METRIC, randomVector, roundTripVector);
            Assertions.assertEquals(randomVector, roundTripVector);
        }
    }

    private <N extends NodeReference> void writeNode(@Nonnull final Transaction transaction,
                                                     @Nonnull final StorageAdapter<N> storageAdapter,
                                                     @Nonnull final Node<N> node,
                                                     final int layer) {
        final NeighborsChangeSet<N> insertChangeSet =
                new InsertNeighborsChangeSet<>(new BaseNeighborsChangeSet<>(ImmutableList.of()),
                        node.getNeighbors());
        storageAdapter.writeNode(transaction, node, layer, insertChangeSet);
    }

    @Nonnull
    private Node<NodeReference> createRandomCompactNode(@Nonnull final Random random,
                                                        @Nonnull final NodeFactory<NodeReference> nodeFactory,
                                                        final int dimensionality,
                                                        final int numberOfNeighbors) {
        final Tuple primaryKey = createRandomPrimaryKey(random);
        final ImmutableList.Builder<NodeReference> neighborsBuilder = ImmutableList.builder();
        for (int i = 0; i < numberOfNeighbors; i ++) {
            neighborsBuilder.add(createRandomNodeReference(random));
        }

        return nodeFactory.create(primaryKey, VectorTest.createRandomHalfVector(random, dimensionality), neighborsBuilder.build());
    }

    @Nonnull
    private Node<NodeReferenceWithVector> createRandomInliningNode(@Nonnull final Random random,
                                                                   @Nonnull final NodeFactory<NodeReferenceWithVector> nodeFactory,
                                                                   final int dimensionality,
                                                                   final int numberOfNeighbors) {
        final Tuple primaryKey = createRandomPrimaryKey(random);
        final ImmutableList.Builder<NodeReferenceWithVector> neighborsBuilder = ImmutableList.builder();
        for (int i = 0; i < numberOfNeighbors; i ++) {
            neighborsBuilder.add(createRandomNodeReferenceWithVector(random, dimensionality));
        }

        return nodeFactory.create(primaryKey, VectorTest.createRandomHalfVector(random, dimensionality), neighborsBuilder.build());
    }

    @Nonnull
    private NodeReference createRandomNodeReference(@Nonnull final Random random) {
        return new NodeReference(createRandomPrimaryKey(random));
    }

    @Nonnull
    private NodeReferenceWithVector createRandomNodeReferenceWithVector(@Nonnull final Random random, final int dimensionality) {
        return new NodeReferenceWithVector(createRandomPrimaryKey(random), VectorTest.createRandomHalfVector(random, dimensionality));
    }

    @Nonnull
    private static Tuple createRandomPrimaryKey(final @Nonnull Random random) {
        return Tuple.from(random.nextLong());
    }

    @Nonnull
    private static Tuple createNextPrimaryKey(@Nonnull final AtomicLong nextIdAtomic) {
        return Tuple.from(nextIdAtomic.getAndIncrement());
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
        public void onKeyValueRead(final int layer, @Nonnull final byte[] key, @Nonnull final byte[] value) {
            bytesReadByLayer.compute(layer, (l, oldValue) -> (oldValue == null ? 0 : oldValue) +
                    key.length + value.length);
        }
    }
}
