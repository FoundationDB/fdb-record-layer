/*
 * HNSWModificationTest.java
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
import com.apple.foundationdb.async.hnsw.Vector.HalfVector;
import com.apple.foundationdb.async.rtree.RTree;
import com.apple.foundationdb.test.TestDatabaseExtension;
import com.apple.foundationdb.test.TestExecutors;
import com.apple.foundationdb.test.TestSubspaceExtension;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.Tags;
import com.christianheina.langx.half4j.Half;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import org.assertj.core.util.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

/**
 * Tests testing insert/update/deletes of data into/in/from {@link RTree}s.
 */
@Execution(ExecutionMode.CONCURRENT)
@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
@Tag(Tags.RequiresFDB)
@Tag(Tags.Slow)
public class HNSWModificationTest {
    private static final Logger logger = LoggerFactory.getLogger(HNSWModificationTest.class);
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

    @Test
    public void testCompactSerialization() {
        final Random random = new Random(0);
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

    @Test
    public void testInliningSerialization() {
        final Random random = new Random(0);
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

    @Test
    public void testBasicInsert() {
        final Random random = new Random(0);
        final AtomicLong nextNodeIdAtomic = new AtomicLong(0L);

        final TestOnReadListener onReadListener = new TestOnReadListener();

        final int dimensions = 128;
        final HNSW hnsw = new HNSW(rtSubspace.getSubspace(), TestExecutors.defaultThreadPool(),
                HNSW.DEFAULT_CONFIG.toBuilder().setMetric(Metrics.EUCLIDEAN_METRIC.getMetric())
                        .setM(32).setMMax(32).setMMax0(64).build(),
                OnWriteListener.NOOP, onReadListener);

        for (int i = 0; i < 1000;) {
            i += basicInsertBatch(hnsw, 100, nextNodeIdAtomic, onReadListener,
                    tr -> new NodeReferenceWithVector(createNextPrimaryKey(nextNodeIdAtomic), createRandomVector(random, dimensions)));
        }

        onReadListener.reset();
        final long beginTs = System.nanoTime();
        final List<? extends NodeReferenceAndNode<?>> result =
                db.run(tr -> hnsw.kNearestNeighborsSearch(tr, 10, 100, createRandomVector(random, dimensions)).join());
        final long endTs = System.nanoTime();

        for (NodeReferenceAndNode<?> nodeReferenceAndNode : result) {
            final NodeReferenceWithDistance nodeReferenceWithDistance = nodeReferenceAndNode.getNodeReferenceWithDistance();
            logger.info("nodeId ={} at distance={}", nodeReferenceWithDistance.getPrimaryKey().getLong(0),
                    nodeReferenceWithDistance.getDistance());
        }
        System.out.println(onReadListener.getNodeCountByLayer());
        System.out.println(onReadListener.getBytesReadByLayer());

        logger.info("search transaction took elapsedTime={}ms", TimeUnit.NANOSECONDS.toMillis(endTs - beginTs));
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
                if (newNodeReference != null) {
                    hnsw.insert(tr, newNodeReference).join();
                }
            }
            final long endTs = System.nanoTime();
            logger.info("inserted batchSize={} records starting at nodeId={} took elapsedTime={}ms, readCounts={}, MSums={}", batchSize, nextNodeId,
                    TimeUnit.NANOSECONDS.toMillis(endTs - beginTs), onReadListener.getNodeCountByLayer(), onReadListener.getSumMByLayer());
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
            logger.info("inserted batch batchSize={} records starting at nodeId={} took elapsedTime={}ms, readCounts={}, MSums={}", batchSize, nextNodeId,
                    TimeUnit.NANOSECONDS.toMillis(endTs - beginTs), onReadListener.getNodeCountByLayer(), onReadListener.getSumMByLayer());
            return batchSize;
        });
    }

    @Test
    @Timeout(value = 150, unit = TimeUnit.MINUTES)
    public void testSIFTInsert10k() throws Exception {
        final Metric metric = Metrics.EUCLIDEAN_METRIC.getMetric();
        final int k = 10;
        final AtomicLong nextNodeIdAtomic = new AtomicLong(0L);

        final TestOnReadListener onReadListener = new TestOnReadListener();

        final HNSW hnsw = new HNSW(rtSubspace.getSubspace(), TestExecutors.defaultThreadPool(),
                HNSW.DEFAULT_CONFIG.toBuilder().setMetric(metric).setM(32).setMMax(32).setMMax0(64).build(),
                OnWriteListener.NOOP, onReadListener);

        final String tsvFile = "/Users/nseemann/Downloads/train-100k.tsv";
        final int dimensions = 128;

        final AtomicReference<HalfVector> queryVectorAtomic = new AtomicReference<>();
        final NavigableSet<NodeReferenceWithDistance> trueResults = new ConcurrentSkipListSet<>(
                Comparator.comparing(NodeReferenceWithDistance::getDistance));

        try (BufferedReader br = new BufferedReader(new FileReader(tsvFile))) {
            for (int i = 0; i < 10000;) {
                i += basicInsertBatch(hnsw, 100, nextNodeIdAtomic, onReadListener,
                        tr -> {
                            final String line;
                            try {
                                line = br.readLine();
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }

                            final String[] values = Objects.requireNonNull(line).split("\t");
                            Assertions.assertEquals(dimensions, values.length);
                            final Half[] halfs = new Half[dimensions];

                            for (int c = 0; c < values.length; c++) {
                                final String value = values[c];
                                halfs[c] = HNSWHelpers.halfValueOf(Double.parseDouble(value));
                            }
                            final Tuple currentPrimaryKey = createNextPrimaryKey(nextNodeIdAtomic);
                            final HalfVector currentVector = new HalfVector(halfs);
                            final HalfVector queryVector = queryVectorAtomic.get();
                            if (queryVector == null) {
                                queryVectorAtomic.set(currentVector);
                                return null;
                            } else {
                                final double currentDistance =
                                        Vector.comparativeDistance(metric, currentVector, queryVector);
                                if (trueResults.size() < k || trueResults.last().getDistance() > currentDistance) {
                                    trueResults.add(
                                            new NodeReferenceWithDistance(currentPrimaryKey, currentVector,
                                                    Vector.comparativeDistance(metric, currentVector, queryVector)));
                                }
                                if (trueResults.size() > k) {
                                    trueResults.remove(trueResults.last());
                                }
                                return new NodeReferenceWithVector(currentPrimaryKey, currentVector);
                            }
                        });
            }
        }

        onReadListener.reset();
        final long beginTs = System.nanoTime();
        final List<? extends NodeReferenceAndNode<?>> results =
                db.run(tr -> hnsw.kNearestNeighborsSearch(tr, k, 100, queryVectorAtomic.get()).join());
        final long endTs = System.nanoTime();

        for (NodeReferenceAndNode<?> nodeReferenceAndNode : results) {
            final NodeReferenceWithDistance nodeReferenceWithDistance = nodeReferenceAndNode.getNodeReferenceWithDistance();
            logger.info("retrieved result nodeId = {} at distance= {}", nodeReferenceWithDistance.getPrimaryKey().getLong(0),
                    nodeReferenceWithDistance.getDistance());
        }

        for (final NodeReferenceWithDistance nodeReferenceWithDistance : trueResults) {
            logger.info("true result nodeId ={} at distance={}", nodeReferenceWithDistance.getPrimaryKey().getLong(0),
                    nodeReferenceWithDistance.getDistance());
        }

        System.out.println(onReadListener.getNodeCountByLayer());
        System.out.println(onReadListener.getBytesReadByLayer());

        logger.info("search transaction took elapsedTime={}ms", TimeUnit.NANOSECONDS.toMillis(endTs - beginTs));
    }

    @Test
    @Timeout(value = 150, unit = TimeUnit.MINUTES)
    public void testSIFTInsert10kWithBatchInsert() throws Exception {
        final Metric metric = Metrics.EUCLIDEAN_METRIC.getMetric();
        final int k = 10;
        final AtomicLong nextNodeIdAtomic = new AtomicLong(0L);

        final TestOnReadListener onReadListener = new TestOnReadListener();

        final HNSW hnsw = new HNSW(rtSubspace.getSubspace(), TestExecutors.defaultThreadPool(),
                HNSW.DEFAULT_CONFIG.toBuilder().setMetric(metric).setM(32).setMMax(32).setMMax0(64).build(),
                OnWriteListener.NOOP, onReadListener);

        final String tsvFile = "/Users/nseemann/Downloads/train-100k.tsv";
        final int dimensions = 128;

        final AtomicReference<HalfVector> queryVectorAtomic = new AtomicReference<>();
        final NavigableSet<NodeReferenceWithDistance> trueResults = new ConcurrentSkipListSet<>(
                Comparator.comparing(NodeReferenceWithDistance::getDistance));

        try (BufferedReader br = new BufferedReader(new FileReader(tsvFile))) {
            for (int i = 0; i < 10000;) {
                i += insertBatch(hnsw, 100, nextNodeIdAtomic, onReadListener,
                        tr -> {
                            final String line;
                            try {
                                line = br.readLine();
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }

                            final String[] values = Objects.requireNonNull(line).split("\t");
                            Assertions.assertEquals(dimensions, values.length);
                            final Half[] halfs = new Half[dimensions];

                            for (int c = 0; c < values.length; c++) {
                                final String value = values[c];
                                halfs[c] = HNSWHelpers.halfValueOf(Double.parseDouble(value));
                            }
                            final Tuple currentPrimaryKey = createNextPrimaryKey(nextNodeIdAtomic);
                            final HalfVector currentVector = new HalfVector(halfs);
                            final HalfVector queryVector = queryVectorAtomic.get();
                            if (queryVector == null) {
                                queryVectorAtomic.set(currentVector);
                                return null;
                            } else {
                                final double currentDistance =
                                        Vector.comparativeDistance(metric, currentVector, queryVector);
                                if (trueResults.size() < k || trueResults.last().getDistance() > currentDistance) {
                                    trueResults.add(
                                            new NodeReferenceWithDistance(currentPrimaryKey, currentVector,
                                                    Vector.comparativeDistance(metric, currentVector, queryVector)));
                                }
                                if (trueResults.size() > k) {
                                    trueResults.remove(trueResults.last());
                                }
                                return new NodeReferenceWithVector(currentPrimaryKey, currentVector);
                            }
                        });
            }
        }

        onReadListener.reset();
        final long beginTs = System.nanoTime();
        final List<? extends NodeReferenceAndNode<?>> results =
                db.run(tr -> hnsw.kNearestNeighborsSearch(tr, k, 100, queryVectorAtomic.get()).join());
        final long endTs = System.nanoTime();

        for (NodeReferenceAndNode<?> nodeReferenceAndNode : results) {
            final NodeReferenceWithDistance nodeReferenceWithDistance = nodeReferenceAndNode.getNodeReferenceWithDistance();
            logger.info("retrieved result nodeId = {} at distance= {}", nodeReferenceWithDistance.getPrimaryKey().getLong(0),
                    nodeReferenceWithDistance.getDistance());
        }

        for (final NodeReferenceWithDistance nodeReferenceWithDistance : trueResults) {
            logger.info("true result nodeId ={} at distance={}", nodeReferenceWithDistance.getPrimaryKey().getLong(0),
                    nodeReferenceWithDistance.getDistance());
        }

        System.out.println(onReadListener.getNodeCountByLayer());
        System.out.println(onReadListener.getBytesReadByLayer());

        logger.info("search transaction took elapsedTime={}ms", TimeUnit.NANOSECONDS.toMillis(endTs - beginTs));
    }

    @Test
    public void testBasicInsertAndScanLayer() throws Exception {
        final Random random = new Random(0);
        final AtomicLong nextNodeId = new AtomicLong(0L);
        final HNSW hnsw = new HNSW(rtSubspace.getSubspace(), TestExecutors.defaultThreadPool(),
                HNSW.DEFAULT_CONFIG.toBuilder().setM(4).setMMax(4).setMMax0(4).build(),
                OnWriteListener.NOOP, OnReadListener.NOOP);

        db.run(tr -> {
            for (int i = 0; i < 100; i ++) {
                hnsw.insert(tr, createNextPrimaryKey(nextNodeId), createRandomVector(random, 2)).join();
            }
            return null;
        });

        int layer = 0;
        while (true) {
            if (!dumpLayer(hnsw, layer++)) {
                break;
            }
        }
    }

    @Test
    public void testManyRandomVectors() {
        final Random random = new Random();
        for (long l = 0L; l < 3000000; l ++) {
            final HalfVector randomVector = createRandomVector(random, 768);
            final Tuple vectorTuple = StorageAdapter.tupleFromVector(randomVector);
            final Vector<Half> roundTripVector = StorageAdapter.vectorFromTuple(vectorTuple);
            Vector.comparativeDistance(Metrics.EUCLIDEAN_METRIC.getMetric(), randomVector, roundTripVector);
            Assertions.assertEquals(randomVector, roundTripVector);
        }
    }

    @Test
    @Timeout(value = 150, unit = TimeUnit.MINUTES)
    public void testSIFTVectors() throws Exception {
        final AtomicLong nextNodeIdAtomic = new AtomicLong(0L);

        final TestOnReadListener onReadListener = new TestOnReadListener();

        final HNSW hnsw = new HNSW(rtSubspace.getSubspace(), TestExecutors.defaultThreadPool(),
                HNSW.DEFAULT_CONFIG.toBuilder().setMetric(Metrics.EUCLIDEAN_METRIC.getMetric())
                        .setM(32).setMMax(32).setMMax0(64).build(),
                OnWriteListener.NOOP, onReadListener);


        final String tsvFile = "/Users/nseemann/Downloads/train-100k.tsv";
        final int dimensions = 128;
        final var referenceVector = createRandomVector(new Random(0), dimensions);
        long count = 0L;
        double mean = 0.0d;
        double mean2 = 0.0d;

        try (BufferedReader br = new BufferedReader(new FileReader(tsvFile))) {
            for (int i = 0; i < 100_000; i ++) {
                final String line;
                try {
                    line = br.readLine();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }

                final String[] values = Objects.requireNonNull(line).split("\t");
                Assertions.assertEquals(dimensions, values.length);
                final Half[] halfs = new Half[dimensions];
                for (int c = 0; c < values.length; c++) {
                    final String value = values[c];
                    halfs[c] = HNSWHelpers.halfValueOf(Double.parseDouble(value));
                }
                final HalfVector newVector = new HalfVector(halfs);
                final double distance = Vector.comparativeDistance(Metrics.EUCLIDEAN_METRIC.getMetric(),
                        referenceVector, newVector);
                count++;
                final double delta = distance - mean;
                mean += delta / count;
                final double delta2 = distance - mean;
                mean2 += delta * delta2;
            }
        }
        final double sampleVariance = mean2 / (count - 1);
        final double standardDeviation = Math.sqrt(sampleVariance);
        logger.info("mean={}, sample_variance={}, stddeviation={}, cv={}", mean, sampleVariance, standardDeviation,
                standardDeviation / mean);
    }


    @ParameterizedTest
    @ValueSource(ints = {2, 3, 10, 100, 768})
    public void testManyVectorsStandardDeviation(final int dimensionality) {
        final Random random = new Random();
        final Metric metric = Metrics.EUCLIDEAN_METRIC.getMetric();
        long count = 0L;
        double mean = 0.0d;
        double mean2 = 0.0d;
        for (long i = 0L; i < 100000; i ++) {
            final HalfVector vector1 = createRandomVector(random, dimensionality);
            final HalfVector vector2 = createRandomVector(random, dimensionality);
            final double distance = Vector.comparativeDistance(metric, vector1, vector2);
            count = i + 1;
            final double delta = distance - mean;
            mean += delta / count;
            final double delta2 = distance - mean;
            mean2 += delta * delta2;
        }
        final double sampleVariance = mean2 / (count - 1);
        final double standardDeviation = Math.sqrt(sampleVariance);
        logger.info("mean={}, sample_variance={}, stddeviation={}, cv={}", mean, sampleVariance, standardDeviation,
                standardDeviation / mean);
    }

    private boolean dumpLayer(final HNSW hnsw, final int layer) throws IOException {
        final String verticesFileName = "/Users/nseemann/Downloads/vertices-" + layer + ".csv";
        final String edgesFileName = "/Users/nseemann/Downloads/edges-" + layer + ".csv";

        final AtomicLong numReadAtomic = new AtomicLong(0L);
        try (final BufferedWriter verticesWriter = new BufferedWriter(new FileWriter(verticesFileName));
                final BufferedWriter edgesWriter = new BufferedWriter(new FileWriter(edgesFileName))) {
            hnsw.scanLayer(db, layer, 100, node -> {
                final CompactNode compactNode = node.asCompactNode();
                final Vector<Half> vector = compactNode.getVector();
                try {
                    verticesWriter.write(compactNode.getPrimaryKey().getLong(0) + "," +
                            vector.getComponent(0) + "," +
                            vector.getComponent(1));
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

        return nodeFactory.create(primaryKey, createRandomVector(random, dimensionality), neighborsBuilder.build());
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

        return nodeFactory.create(primaryKey, createRandomVector(random, dimensionality), neighborsBuilder.build());
    }

    @Nonnull
    private NodeReference createRandomNodeReference(@Nonnull final Random random) {
        return new NodeReference(createRandomPrimaryKey(random));
    }

    @Nonnull
    private NodeReferenceWithVector createRandomNodeReferenceWithVector(@Nonnull final Random random, final int dimensionality) {
        return new NodeReferenceWithVector(createRandomPrimaryKey(random), createRandomVector(random, dimensionality));
    }

    @Nonnull
    private static Tuple createRandomPrimaryKey(final @Nonnull Random random) {
        return Tuple.from(random.nextLong());
    }

    @Nonnull
    private static Tuple createNextPrimaryKey(@Nonnull final AtomicLong nextIdAtomic) {
        return Tuple.from(nextIdAtomic.getAndIncrement());
    }

    @Nonnull
    private HalfVector createRandomVector(@Nonnull final Random random, final int dimensionality) {
        final Half[] components = new Half[dimensionality];
        for (int d = 0; d < dimensionality; d ++) {
            // don't ask
            components[d] = HNSWHelpers.halfValueOf(random.nextDouble());
        }
        return new HalfVector(components);
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
