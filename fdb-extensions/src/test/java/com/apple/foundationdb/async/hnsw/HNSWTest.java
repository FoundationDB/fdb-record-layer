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
import com.apple.foundationdb.linear.AffineOperator;
import com.apple.foundationdb.linear.DoubleRealVector;
import com.apple.foundationdb.linear.HalfRealVector;
import com.apple.foundationdb.linear.Metric;
import com.apple.foundationdb.linear.Quantizer;
import com.apple.foundationdb.linear.RealVector;
import com.apple.foundationdb.linear.StoredVecsIterator;
import com.apple.foundationdb.test.TestDatabaseExtension;
import com.apple.foundationdb.test.TestExecutors;
import com.apple.foundationdb.test.TestSubspaceExtension;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.RandomSeedSource;
import com.apple.test.RandomizedTestUtils;
import com.apple.test.Tags;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.ObjectArrays;
import com.google.common.collect.Sets;
import org.assertj.core.api.Assertions;
import org.assertj.core.util.Lists;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
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
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static com.apple.foundationdb.linear.RealVectorTest.createRandomHalfVector;

/**
 * Tests testing insert/update/deletes of data into/in/from {@link RTree}s.
 */
@Execution(ExecutionMode.CONCURRENT)
@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
@Tag(Tags.RequiresFDB)
@Tag(Tags.Slow)
public class HNSWTest {
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

    @Test
    void testConfig() {
        final HNSW.Config defaultConfig = HNSW.defaultConfig(768);

        Assertions.assertThat(HNSW.newConfigBuilder().build(768)).isEqualTo(defaultConfig);
        Assertions.assertThat(defaultConfig.toBuilder().build(768)).isEqualTo(defaultConfig);

        final long randomSeed = 1L;
        final Metric metric = Metric.COSINE_METRIC;
        final boolean useInlining = true;
        final int m = HNSW.DEFAULT_M + 1;
        final int mMax = HNSW.DEFAULT_M_MAX + 1;
        final int mMax0 = HNSW.DEFAULT_M_MAX_0 + 1;
        final int efConstruction = HNSW.DEFAULT_EF_CONSTRUCTION + 1;
        final boolean extendCandidates = true;
        final boolean keepPrunedConnections = true;
        final int statsThreshold = 1;
        final double sampleVectorStatsProbability = 0.000001d;
        final double maintainStatsProbability = 0.000001d;

        final boolean useRaBitQ = true;
        final int raBitQNumExBits = HNSW.DEFAULT_RABITQ_NUM_EX_BITS + 1;

        Assertions.assertThat(defaultConfig.getRandomSeed()).isNotEqualTo(randomSeed);
        Assertions.assertThat(defaultConfig.getMetric()).isNotSameAs(metric);
        Assertions.assertThat(defaultConfig.isUseInlining()).isNotEqualTo(useInlining);
        Assertions.assertThat(defaultConfig.getM()).isNotEqualTo(m);
        Assertions.assertThat(defaultConfig.getMMax()).isNotEqualTo(mMax);
        Assertions.assertThat(defaultConfig.getMMax0()).isNotEqualTo(mMax0);
        Assertions.assertThat(defaultConfig.getEfConstruction()).isNotEqualTo(efConstruction);
        Assertions.assertThat(defaultConfig.isExtendCandidates()).isNotEqualTo(extendCandidates);
        Assertions.assertThat(defaultConfig.isKeepPrunedConnections()).isNotEqualTo(keepPrunedConnections);

        Assertions.assertThat(defaultConfig.getSampleVectorStatsProbability()).isNotEqualTo(sampleVectorStatsProbability);
        Assertions.assertThat(defaultConfig.getMaintainStatsProbability()).isNotEqualTo(maintainStatsProbability);
        Assertions.assertThat(defaultConfig.getStatsThreshold()).isNotEqualTo(statsThreshold);

        Assertions.assertThat(defaultConfig.isUseRaBitQ()).isNotEqualTo(useRaBitQ);
        Assertions.assertThat(defaultConfig.getRaBitQNumExBits()).isNotEqualTo(raBitQNumExBits);

        final HNSW.Config newConfig =
                defaultConfig.toBuilder()
                        .setRandomSeed(randomSeed)
                        .setMetric(metric)
                        .setUseInlining(useInlining)
                        .setM(m)
                        .setMMax(mMax)
                        .setMMax0(mMax0)
                        .setEfConstruction(efConstruction)
                        .setExtendCandidates(extendCandidates)
                        .setKeepPrunedConnections(keepPrunedConnections)
                        .setSampleVectorStatsProbability(sampleVectorStatsProbability)
                        .setMaintainStatsProbability(maintainStatsProbability)
                        .setStatsThreshold(statsThreshold)
                        .setUseRaBitQ(useRaBitQ)
                        .setRaBitQNumExBits(raBitQNumExBits)
                        .build(768);

        Assertions.assertThat(newConfig.getRandomSeed()).isEqualTo(randomSeed);
        Assertions.assertThat(newConfig.getMetric()).isSameAs(metric);
        Assertions.assertThat(newConfig.isUseInlining()).isEqualTo(useInlining);
        Assertions.assertThat(newConfig.getM()).isEqualTo(m);
        Assertions.assertThat(newConfig.getMMax()).isEqualTo(mMax);
        Assertions.assertThat(newConfig.getMMax0()).isEqualTo(mMax0);
        Assertions.assertThat(newConfig.getEfConstruction()).isEqualTo(efConstruction);
        Assertions.assertThat(newConfig.isExtendCandidates()).isEqualTo(extendCandidates);
        Assertions.assertThat(newConfig.isKeepPrunedConnections()).isEqualTo(keepPrunedConnections);

        Assertions.assertThat(defaultConfig.getSampleVectorStatsProbability()).isEqualTo(sampleVectorStatsProbability);
        Assertions.assertThat(defaultConfig.getMaintainStatsProbability()).isEqualTo(maintainStatsProbability);
        Assertions.assertThat(defaultConfig.getStatsThreshold()).isEqualTo(statsThreshold);

        Assertions.assertThat(newConfig.isUseRaBitQ()).isEqualTo(useRaBitQ);
        Assertions.assertThat(newConfig.getRaBitQNumExBits()).isEqualTo(raBitQNumExBits);
    }

    @ParameterizedTest
    @RandomSeedSource({0x0fdbL, 0x5ca1eL, 123456L, 78910L, 1123581321345589L})
    void testCompactSerialization(final long seed) {
        final Random random = new Random(seed);
        final int numDimensions = 768;
        final CompactStorageAdapter storageAdapter =
                new CompactStorageAdapter(HNSW.DEFAULT_CONFIG_BUILDER.build(numDimensions), CompactNode.factory(),
                        rtSubspace.getSubspace(), OnWriteListener.NOOP, OnReadListener.NOOP);
        final Node<NodeReference> originalNode =
                db.run(tr -> {
                    final NodeFactory<NodeReference> nodeFactory = storageAdapter.getNodeFactory();

                    final Node<NodeReference> randomCompactNode =
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
                new InliningStorageAdapter(HNSW.DEFAULT_CONFIG_BUILDER.build(numDimensions), InliningNode.factory(), rtSubspace.getSubspace(),
                        OnWriteListener.NOOP, OnReadListener.NOOP);
        final Node<NodeReferenceWithVector> originalNode =
                db.run(tr -> {
                    final NodeFactory<NodeReferenceWithVector> nodeFactory = storageAdapter.getNodeFactory();

                    final Node<NodeReferenceWithVector> randomInliningNode =
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
        return RandomizedTestUtils.randomSeeds(0xdeadc0deL, 0xfdb5ca1eL, 0xf005ba1L)
                .flatMap(seed -> Sets.cartesianProduct(ImmutableSet.of(true, false),
                                ImmutableSet.of(true, false),
                                ImmutableSet.of(true, false)).stream()
                        .map(arguments -> Arguments.of(ObjectArrays.concat(seed, arguments.toArray()))));
    }

    @ParameterizedTest(name = "seed={0} useInlining={1} extendCandidates={2} keepPrunedConnections={3}")
    @MethodSource("randomSeedsWithOptions")
    void testBasicInsert(final long seed, final boolean useInlining, final boolean extendCandidates,
                         final boolean keepPrunedConnections) {
        final Random random = new Random(seed);
        final Metric metric = Metric.EUCLIDEAN_METRIC;
        final AtomicLong nextNodeIdAtomic = new AtomicLong(0L);

        final TestOnReadListener onReadListener = new TestOnReadListener();

        final int numDimensions = 128;
        final HNSW hnsw = new HNSW(rtSubspace.getSubspace(), TestExecutors.defaultThreadPool(),
                HNSW.DEFAULT_CONFIG_BUILDER.setMetric(metric)
                        .setUseInlining(useInlining).setExtendCandidates(extendCandidates)
                        .setKeepPrunedConnections(keepPrunedConnections)
                        .setM(32).setMMax(32).setMMax0(64).build(numDimensions),
                OnWriteListener.NOOP, onReadListener);

        final int k = 10;
        final HalfRealVector queryVector = createRandomHalfVector(random, numDimensions);
        final TreeSet<NodeReferenceWithDistance> nodesOrderedByDistance =
                new TreeSet<>(Comparator.comparing(NodeReferenceWithDistance::getDistance));

        for (int i = 0; i < 1000;) {
            i += basicInsertBatch(hnsw, 100, nextNodeIdAtomic, onReadListener,
                    tr -> {
                        final var primaryKey = createNextPrimaryKey(nextNodeIdAtomic);
                        final HalfRealVector dataVector = createRandomHalfVector(random, numDimensions);
                        final double distance = metric.distance(dataVector, queryVector);
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
        logger.info("search transaction took elapsedTime={}ms; read nodes={}, read bytes={}, recall={}",
                TimeUnit.NANOSECONDS.toMillis(endTs - beginTs),
                onReadListener.getNodeCountByLayer(), onReadListener.getBytesReadByLayer(),
                String.format(Locale.ROOT, "%.2f", recall * 100.0d));
        Assertions.assertThat(recall).isGreaterThan(0.79);

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
            logger.info("inserted batchSize={} records starting at nodeId={} took elapsedTime={}ms, readCounts={}, readBytes={}",
                    batchSize, nextNodeId, TimeUnit.NANOSECONDS.toMillis(endTs - beginTs),
                    onReadListener.getNodeCountByLayer(), onReadListener.getBytesReadByLayer());
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
            logger.info("inserted batch batchSize={} records starting at nodeId={} took elapsedTime={}ms, readCounts={}, readBytes={}",
                    batchSize, nextNodeId, TimeUnit.NANOSECONDS.toMillis(endTs - beginTs),
                    onReadListener.getNodeCountByLayer(), onReadListener.getBytesReadByLayer());
            return batchSize;
        });
    }

    @Test
    //@SuperSlow
    void testSIFTInsertSmall() throws Exception {
        final Metric metric = Metric.EUCLIDEAN_METRIC;
        final int k = 100;
        final AtomicLong nextNodeIdAtomic = new AtomicLong(0L);

        final TestOnReadListener onReadListener = new TestOnReadListener();

        final HNSW hnsw = new HNSW(rtSubspace.getSubspace(), TestExecutors.defaultThreadPool(),
                HNSW.DEFAULT_CONFIG_BUILDER.setUseRaBitQ(true).setRaBitQNumExBits(5)
                        .setMetric(metric).setM(32).setMMax(32).setMMax0(64).build(128),
                OnWriteListener.NOOP, onReadListener);

        final Path siftSmallPath = Paths.get(".out/extracted/siftsmall/siftsmall_base.fvecs");

        try (final var fileChannel = FileChannel.open(siftSmallPath, StandardOpenOption.READ)) {
            final Iterator<DoubleRealVector> vectorIterator = new StoredVecsIterator.StoredFVecsIterator(fileChannel);

            int i = 0;
            final AtomicReference<RealVector> sumReference = new AtomicReference<>(null);
            while (vectorIterator.hasNext()) {
                i += basicInsertBatch(hnsw, 100, nextNodeIdAtomic, onReadListener,
                        tr -> {
                            if (!vectorIterator.hasNext()) {
                                return null;
                            }
                            final DoubleRealVector doubleVector = vectorIterator.next();
                            final Tuple currentPrimaryKey = createNextPrimaryKey(nextNodeIdAtomic);
                            final HalfRealVector currentVector = doubleVector.toHalfRealVector();

                            if (sumReference.get() == null) {
                                sumReference.set(currentVector);
                            } else {
                                sumReference.set(sumReference.get().add(currentVector));
                            }

                            return new NodeReferenceWithVector(currentPrimaryKey, currentVector);
                        });
            }
            Assertions.assertThat(i).isEqualTo(10000);
        }

        validateSIFTSmall(hnsw, k);
    }

    private void validateSIFTSmall(@Nonnull final HNSW hnsw, final int k) throws IOException {
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
                //Assertions.assertThat(recall).isGreaterThan(0.8);

                logger.info("query returned results recall={}", String.format(Locale.ROOT, "%.2f", recall * 100.0d));
            }
        }
    }

    @Test
    //@SuperSlow
    void testSIFTInsertSmallUsingBatchAPI() throws Exception {
        final Metric metric = Metric.EUCLIDEAN_METRIC;
        final int k = 100;
        final AtomicLong nextNodeIdAtomic = new AtomicLong(0L);

        final TestOnReadListener onReadListener = new TestOnReadListener();

        final HNSW hnsw = new HNSW(rtSubspace.getSubspace(), TestExecutors.defaultThreadPool(),
                HNSW.DEFAULT_CONFIG_BUILDER.setMetric(metric).setM(32).setMMax(32).setMMax0(64).build(128),
                OnWriteListener.NOOP, onReadListener);

        final Path siftSmallPath = Paths.get(".out/extracted/siftsmall/siftsmall_base.fvecs");

        try (final var fileChannel = FileChannel.open(siftSmallPath, StandardOpenOption.READ)) {
            final Iterator<DoubleRealVector> vectorIterator = new StoredVecsIterator.StoredFVecsIterator(fileChannel);

            int i = 0;
            while (vectorIterator.hasNext()) {
                i += insertBatch(hnsw, 100, nextNodeIdAtomic, onReadListener,
                        tr -> {
                            if (!vectorIterator.hasNext()) {
                                return null;
                            }
                            final DoubleRealVector doubleVector = vectorIterator.next();
                            final Tuple currentPrimaryKey = createNextPrimaryKey(nextNodeIdAtomic);
                            final HalfRealVector currentVector = doubleVector.toHalfRealVector();
                            return new NodeReferenceWithVector(currentPrimaryKey, currentVector);
                        });
            }
            Assertions.assertThat(i).isEqualTo(10000);
        }
        validateSIFTSmall(hnsw, k);
    }

    private <N extends NodeReference> void writeNode(@Nonnull final Transaction transaction,
                                                     @Nonnull final StorageAdapter<N> storageAdapter,
                                                     @Nonnull final Node<N> node,
                                                     final int layer) {
        final NeighborsChangeSet<N> insertChangeSet =
                new InsertNeighborsChangeSet<>(new BaseNeighborsChangeSet<>(ImmutableList.of()),
                        node.getNeighbors());
        storageAdapter.writeNode(transaction, Quantizer.noOpQuantizer(Metric.EUCLIDEAN_METRIC), node, layer,
                insertChangeSet);
    }

    @Nonnull
    private Node<NodeReference> createRandomCompactNode(@Nonnull final Random random,
                                                        @Nonnull final NodeFactory<NodeReference> nodeFactory,
                                                        final int numDimensions,
                                                        final int numberOfNeighbors) {
        final Tuple primaryKey = createRandomPrimaryKey(random);
        final ImmutableList.Builder<NodeReference> neighborsBuilder = ImmutableList.builder();
        for (int i = 0; i < numberOfNeighbors; i ++) {
            neighborsBuilder.add(createRandomNodeReference(random));
        }

        return nodeFactory.create(primaryKey, createRandomHalfVector(random, numDimensions), neighborsBuilder.build());
    }

    @Nonnull
    private Node<NodeReferenceWithVector> createRandomInliningNode(@Nonnull final Random random,
                                                                   @Nonnull final NodeFactory<NodeReferenceWithVector> nodeFactory,
                                                                   final int numDimensions,
                                                                   final int numberOfNeighbors) {
        final Tuple primaryKey = createRandomPrimaryKey(random);
        final ImmutableList.Builder<NodeReferenceWithVector> neighborsBuilder = ImmutableList.builder();
        for (int i = 0; i < numberOfNeighbors; i ++) {
            neighborsBuilder.add(createRandomNodeReferenceWithVector(random, numDimensions));
        }

        return nodeFactory.create(primaryKey, createRandomHalfVector(random, numDimensions), neighborsBuilder.build());
    }

    @Nonnull
    private NodeReference createRandomNodeReference(@Nonnull final Random random) {
        return new NodeReference(createRandomPrimaryKey(random));
    }

    @Nonnull
    private NodeReferenceWithVector createRandomNodeReferenceWithVector(@Nonnull final Random random,
                                                                        final int dimensionality) {
        return new NodeReferenceWithVector(createRandomPrimaryKey(random),
                createRandomHalfVector(random, dimensionality));
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
