/*
 * TestHelpers.java
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
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.async.common.BaseTest;
import com.apple.foundationdb.async.common.CommonTestHelpers;
import com.apple.foundationdb.async.common.PrimaryKeyAndVector;
import com.apple.foundationdb.async.common.ResultEntry;
import com.apple.foundationdb.linear.AffineOperator;
import com.apple.foundationdb.linear.DoubleRealVector;
import com.apple.foundationdb.linear.HalfRealVector;
import com.apple.foundationdb.linear.Metric;
import com.apple.foundationdb.linear.Quantizer;
import com.apple.foundationdb.linear.RealVector;
import com.apple.foundationdb.linear.StoredVecsIterator;
import com.apple.foundationdb.linear.Transformed;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.jupiter.api.extension.AfterTestExecutionCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterInfo;
import org.junit.jupiter.params.aggregator.ArgumentsAccessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.ToDoubleBiFunction;

import static com.apple.foundationdb.linear.RealVectorTest.createRandomHalfVector;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

/**
 * Test helpers for testing {@link HNSW}s.
 */
@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
class TestHelpers {
    private static final Logger logger = LoggerFactory.getLogger(TestHelpers.class);

    static void dumpQueryResults(@Nonnull final Path tempDir, @Nonnull final String prefix, final int layer,
                                 @Nonnull final List<? extends ResultEntry> results) throws Exception {
        final Path verticesFile = tempDir.resolve("vertices-" + prefix + "-" + layer + ".csv");
        try (final BufferedWriter verticesWriter = Files.newBufferedWriter(verticesFile)) {
            for (final ResultEntry result : results) {
                verticesWriter.write(Long.toString(result.getPrimaryKey().getLong(0)));
                verticesWriter.newLine();
            }
        }
    }

    @Nonnull
    static List<PrimaryKeyAndVector> basicInsertBatch(@Nonnull final Database db,
                                                      @Nonnull final HNSW hnsw,
                                                      final int batchSize,
                                                      final long firstId,
                                                      @Nonnull final BiFunction<Transaction, Long, PrimaryKeyAndVector> insertFunction)
            throws ExecutionException, InterruptedException, TimeoutException {
        return db.runAsync(tr -> {
            final TestOnWriteListener onWriteListener = (TestOnWriteListener)hnsw.getOnWriteListener();
            onWriteListener.reset();
            final TestOnReadListener onReadListener = (TestOnReadListener)hnsw.getOnReadListener();
            onReadListener.reset();

            final ImmutableList.Builder<PrimaryKeyAndVector> data = ImmutableList.builder();

            // In theory this could put all the futures in a List and run the inserts concurrently, but for a `basicInsertBatch`
            // it's probably better to not test the concurrent handling of hnsw, even if it makes the tests slower.
            CompletableFuture<Void> future = CompletableFuture.completedFuture(null);
            final long beginTs = System.nanoTime();
            for (int i = 0; i < batchSize; i ++) {
                final PrimaryKeyAndVector record = insertFunction.apply(tr, firstId + i);
                if (record == null) {
                    break;
                }
                data.add(record);
                future = future.thenCompose((vignore) -> hnsw.insert(tr, record.getPrimaryKey(), record.getVector(), null));
            }
            return future.thenApply(vignore -> data.build())
                    .whenComplete((result, error) -> {
                        if (error != null) {
                            logger.info("Failed to insert batchSize={}", error);
                        } else {
                            final long endTs = System.nanoTime();
                            logger.info("inserted batchSize={} records={} starting at nodeId={} took elapsedTime={}ms, readCounts={}, readBytes={}",
                                    batchSize, result.size(), firstId, TimeUnit.NANOSECONDS.toMillis(endTs - beginTs),
                                    onReadListener.getNodeCountByLayer(), onReadListener.getBytesReadByLayer());
                        }
                    });
        }).get(2, TimeUnit.MINUTES); // set a timeout for inserting a single batch including retries so setup won't run forever
    }

    static List<PrimaryKeyAndVector> insertSIFTSmall(@Nonnull final Database db,
                                                     @Nonnull final HNSW hnsw) throws Exception {
        final Path siftSmallPath = Paths.get(".out/extracted/siftsmall/siftsmall_base.fvecs");

        final ImmutableList.Builder<PrimaryKeyAndVector> insertedDataBuilder = ImmutableList.builder();

        try (final var fileChannel = FileChannel.open(siftSmallPath, StandardOpenOption.READ)) {
            final Iterator<DoubleRealVector> vectorIterator = new StoredVecsIterator.StoredFVecsIterator(fileChannel);

            final int batchSize = 50;
            int i = 0;
            while (vectorIterator.hasNext()) {
                final List<DoubleRealVector> batch =
                        Lists.newArrayList(Iterators.limit(vectorIterator, batchSize));
                final long currentBatchStart = i;
                final List<PrimaryKeyAndVector> insertedInBatch =
                        basicInsertBatch(db, hnsw, batchSize, i,
                                (tr, nextId) -> {
                                    final int indexInBatch = Math.toIntExact(nextId - currentBatchStart);
                                    if (indexInBatch >= batch.size()) {
                                        return null;
                                    }
                                    final Tuple currentPrimaryKey = CommonTestHelpers.createPrimaryKey(nextId);
                                    final DoubleRealVector doubleVector = batch.get(indexInBatch);
                                    return new PrimaryKeyAndVector(currentPrimaryKey, doubleVector);
                                });
                insertedDataBuilder.addAll(insertedInBatch);
                i += insertedInBatch.size();
            }
            assertThat(i).isEqualTo(10000);
        }
        return insertedDataBuilder.build();
    }

    static void validateSIFTSmall(@Nonnull final Database db,
                                  @Nonnull final HNSW hnsw,
                                  @Nonnull final List<PrimaryKeyAndVector> data,
                                  final int k) throws IOException {
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
                    // the dimensionality, and the metric in use. For now, we just set it to 30.0 as that should be
                    // fairly safe with respect to not giving us false-positives and also tripping for actual logic
                    // errors as the expected random distance is far larger.
                    //
                    final RealVector originalVector = data.get(primaryKeyIndex).getVector();
                    assertThat(originalVector).isNotNull();
                    final double distance = metric.distance(originalVector,
                            Objects.requireNonNull(resultEntry.getVector()).toDoubleRealVector());
                    assertThat(distance).isCloseTo(0.0d, within(30.0d));

                    logger.trace("retrieved result nodeId = {} at distance = {} ",
                            primaryKeyIndex, resultEntry.getDistance());
                    if (groundTruthIndices.contains(primaryKeyIndex)) {
                        recallCount ++;
                    }
                }

                final double recall = (double)recallCount / k;
                assertThat(recall).isGreaterThan(0.93);

                logger.info("query returned results recall={}", String.format(Locale.ROOT, "%.2f", recall * 100.0d));
            }
        }
    }

    static long countNodesOnLayer(@Nonnull Database db,
                                  @Nonnull Subspace subspace,
                                  @Nonnull final Config config, final int layer) {
        final AtomicLong counter = new AtomicLong();
        scanLayer(db, subspace, config, layer, 100,
                node -> counter.incrementAndGet());
        return counter.get();
    }

    static void scanLayer(@Nonnull final Database db,
                          @Nonnull final Subspace subspace,
                          @Nonnull final Config config,
                          final int layer,
                          final int batchSize,
                          @Nonnull final Consumer<AbstractNode<? extends NodeReference>> nodeConsumer) {
        HNSW.scanLayer(config, subspace, db, layer, batchSize, nodeConsumer);
    }

    static int getEntryLayer(@Nonnull final Database db,
                             @Nonnull final Subspace subspace,
                             @Nonnull final Config config) {
        @Nullable final AccessInfo accessInfo = db.run(readTransaction ->
                StorageAdapter.fetchAccessInfo(config, readTransaction, subspace, OnReadListener.NOOP).join());
        return accessInfo == null
               ? -1
               : accessInfo.getEntryNodeReference().getLayer();
    }

    static void dumpLayers(@Nonnull final Database db,
                           @Nonnull final Subspace subspace,
                           @Nonnull final Config config,
                           @Nonnull final Path tempDir) {
        final int entryLayer = getEntryLayer(db, subspace, config);

        if (entryLayer < 0) {
            return;
        }

        for (int layer = 0; layer < entryLayer; layer ++) {
            try {
                Verify.verify(dumpLayer(db, subspace, config, tempDir, "debug", layer) > 0);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    static long dumpLayer(@Nonnull final Database db,
                          @Nonnull final Subspace subspace,
                          @Nonnull final Config config,
                          @Nonnull final Path tempDir,
                          @Nonnull final String prefix, final int layer) throws IOException {
        final Path verticesFile = tempDir.resolve("vertices-" + prefix + "-" + layer + ".csv");
        final Path edgesFile = tempDir.resolve("edges-" + prefix + "-" + layer + ".csv");

        final StorageAdapter<? extends NodeReference> storageAdapter =
                HNSW.storageAdapterForLayer(config, subspace,
                        OnWriteListener.NOOP, OnReadListener.NOOP, layer);

        final AtomicLong numReadAtomic = new AtomicLong(0L);
        try (final BufferedWriter verticesWriter = Files.newBufferedWriter(verticesFile);
                final BufferedWriter edgesWriter = Files.newBufferedWriter(edgesFile)) {
            scanLayer(db, subspace, config, layer, 100, node -> {
                @Nullable final Transformed<RealVector> vector =
                        storageAdapter.isCompactStorageAdapter()
                        ? node.asCompactNode().getVector()
                        : null;
                try {
                    verticesWriter.write(Long.toString(node.getPrimaryKey().getLong(0)));
                    if (vector != null) {
                        verticesWriter.write(",");
                        final RealVector realVector = vector.getUnderlyingVector();
                        for (int i = 0; i < realVector.getNumDimensions(); i++) {
                            if (i != 0) {
                                verticesWriter.write(",");
                            }
                            verticesWriter.write(String.valueOf(realVector.getComponent(i)));
                        }
                    }
                    verticesWriter.newLine();

                    for (final var neighbor : node.getNeighbors()) {
                        edgesWriter.write(node.getPrimaryKey().getLong(0) + "," +
                                neighbor.getPrimaryKey().getLong(0));
                        edgesWriter.newLine();
                    }
                    numReadAtomic.getAndIncrement();
                } catch (final IOException e) {
                    throw new RuntimeException("unable to write to file", e);
                }
            });
        }
        return numReadAtomic.get();
    }

    static <N extends NodeReference> void writeNode(@Nonnull final Transaction transaction,
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
    static AbstractNode<NodeReference> createRandomCompactNode(@Nonnull final Random random,
                                                               @Nonnull final NodeFactory<NodeReference> nodeFactory,
                                                               final int numDimensions,
                                                               final int numberOfNeighbors) {
        final Tuple primaryKey = CommonTestHelpers.createRandomPrimaryKey(random);
        final ImmutableList.Builder<NodeReference> neighborsBuilder = ImmutableList.builder();
        for (int i = 0; i < numberOfNeighbors; i ++) {
            neighborsBuilder.add(createRandomNodeReference(random));
        }

        return nodeFactory.create(primaryKey,
                AffineOperator.identity().transform(createRandomHalfVector(random, numDimensions)),
                null,
                neighborsBuilder.build());
    }

    @Nonnull
    static AbstractNode<NodeReferenceWithVector> createRandomInliningNode(@Nonnull final Random random,
                                                                          @Nonnull final NodeFactory<NodeReferenceWithVector> nodeFactory,
                                                                          final int numDimensions,
                                                                          final int numberOfNeighbors) {
        final Tuple primaryKey = CommonTestHelpers.createRandomPrimaryKey(random);
        final ImmutableList.Builder<NodeReferenceWithVector> neighborsBuilder = ImmutableList.builder();
        for (int i = 0; i < numberOfNeighbors; i ++) {
            neighborsBuilder.add(createRandomNodeReferenceWithVector(random, numDimensions));
        }

        return nodeFactory.create(primaryKey,
                AffineOperator.identity().transform(createRandomHalfVector(random, numDimensions)),
                null,
                neighborsBuilder.build());
    }

    @Nonnull
    static NodeReference createRandomNodeReference(@Nonnull final Random random) {
        return new NodeReference(CommonTestHelpers.createRandomPrimaryKey(random));
    }

    @Nonnull
    static NodeReferenceWithVector createRandomNodeReferenceWithVector(@Nonnull final Random random,
                                                                       final int dimensionality) {
        return new NodeReferenceWithVector(CommonTestHelpers.createRandomPrimaryKey(random),
                AffineOperator.identity().transform(createRandomHalfVector(random, dimensionality)));
    }

    static ToDoubleBiFunction<RealVector, RealVector> ringDistance(@Nonnull final Metric metric,
                                                                   final double radius) {
        return (queryVector, dataVector) ->
                Math.abs(metric.distance(queryVector, dataVector) - radius);
    }

    static class DumpLayersIfFailure implements AfterTestExecutionCallback {
        @Override
        public void afterTestExecution(@Nonnull final ExtensionContext context) {
            final Optional<Throwable> failure = context.getExecutionException();
            if (failure.isEmpty()) {
                return;
            }

            final ParameterInfo parameterInfo = ParameterInfo.get(context);

            if (parameterInfo != null) {
                final ArgumentsAccessor args = parameterInfo.getArguments();

                final BaseTest baseTest = (BaseTest)context.getRequiredTestInstance();
                final Config config = (Config)args.get(1);
                logger.error("dumping contents of HNSW to {}", baseTest.getTempDir());
                dumpLayers(baseTest.getDb(), baseTest.getSubspace(), config, baseTest.getTempDir());
            } else {
                logger.error("test failed with no parameterized arguments (non-parameterized test or older JUnit).");
            }
        }
    }

    static class TestOnWriteListener implements OnWriteListener {
        final Map<Integer, Long> deleteCountByLayer;

        public TestOnWriteListener() {
            this.deleteCountByLayer = Maps.newConcurrentMap();
        }

        public Map<Integer, Long> getDeleteCountByLayer() {
            return deleteCountByLayer;
        }

        public void reset() {
            deleteCountByLayer.clear();
        }

        @Override
        public void onNodeDeleted(final int layer, @Nonnull final Tuple primaryKey) {
            deleteCountByLayer.compute(layer, (l, oldValue) -> (oldValue == null ? 0 : oldValue) + 1L);
        }
    }

    static class TestOnReadListener implements OnReadListener {
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
}
