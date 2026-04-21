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

package com.apple.foundationdb.async.guardiann;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.async.MoreAsyncUtil;
import com.apple.foundationdb.async.common.PrimaryKeyAndVector;
import com.apple.foundationdb.async.common.ResultEntry;
import com.apple.foundationdb.linear.DoubleRealVector;
import com.apple.foundationdb.linear.HalfRealVector;
import com.apple.foundationdb.linear.Metric;
import com.apple.foundationdb.linear.RealVector;
import com.apple.foundationdb.linear.StoredVecsIterator;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.apple.foundationdb.async.common.CommonTestHelpers.createPrimaryKey;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

/**
 * Test helpers for testing {@link Guardiann}s.
 */
@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
class TestHelpers {
    private static final Logger logger = LoggerFactory.getLogger(TestHelpers.class);

    @Nonnull
    static List<PrimaryKeyAndVector> basicInsertBatch(@Nonnull final Database db,
                                                      @Nonnull final Guardiann guardiann,
                                                      final int batchSize,
                                                      final long firstId,
                                                      @Nonnull final BiFunction<Transaction, Long, PrimaryKeyAndVector> insertFunction)
            throws ExecutionException, InterruptedException, TimeoutException {

        return db.runAsync(tr -> {
            final TestOnWriteListener onWriteListener = (TestOnWriteListener)guardiann.getOnWriteListener();
            onWriteListener.reset();
            final TestOnReadListener onReadListener = (TestOnReadListener)guardiann.getOnReadListener();
            onReadListener.reset();

            final ImmutableList.Builder<PrimaryKeyAndVector> data = ImmutableList.builder();

            final long beginTs = System.nanoTime();

            final CompletableFuture<Integer> loopFuture =
                    MoreAsyncUtil.forLoop(0, 0,
                            (i, u) ->
                                    i < batchSize && (int)u == i && onWriteListener.getSumTaskCounters() == 0,
                            i -> i + 1,
                            (i, u) -> {
                                final PrimaryKeyAndVector record = insertFunction.apply(tr, firstId + i);
                                if (record == null) {
                                    return CompletableFuture.completedFuture(i);
                                }
                                data.add(record);

                                return guardiann.insert(tr, record.getPrimaryKey(),
                                                record.getVector(), null)
                                        .thenApply(ignored -> i + 1);
                            }, guardiann.getExecutor());
            return loopFuture.thenApply(vignore -> data.build())
                    .whenComplete((result, error) -> {
                        if (error != null) {
                            logger.info("failed to insert batchSize={}", error);
                        } else {
                            final long endTs = System.nanoTime();
                            logger.info("inserted batchSize={} records={} starting at id={} took elapsedTime={}ms, readBytes={}",
                                    batchSize, result.size(), firstId, TimeUnit.NANOSECONDS.toMillis(endTs - beginTs),
                                    onReadListener.getBytesReadByLayer());
                        }
                    });
        }).get(2, TimeUnit.MINUTES); // set a timeout for inserting a single batch including retries so setup won't run forever
    }

    static List<PrimaryKeyAndVector> insertSIFTSmall(@Nonnull final Database db,
                                                     @Nonnull final Guardiann guardiann) throws Exception {
        return insertSIFT(db, guardiann, ".out/extracted/siftsmall/siftsmall_base.fvecs",
                10000, 20);
    }

    static List<PrimaryKeyAndVector> insertSIFT100k(@Nonnull final Database db,
                                                    @Nonnull final Guardiann guardiann,
                                                    final int numVectors,
                                                    final int batchSize) throws Exception {
        return insertSIFT(db, guardiann, "/Users/nseemann/downloads/embeddings-unified-model-1m-1.0.0.fvecs",
                numVectors, batchSize);
    }

    @Nonnull
    static List<PrimaryKeyAndVector> loadVectors(@Nonnull final String baseFile,
                                                 final int numVectors) throws Exception {
        final Path basePath = Paths.get(baseFile);

        final ImmutableList.Builder<PrimaryKeyAndVector> insertedDataBuilder = ImmutableList.builder();

        try (final var fileChannel = FileChannel.open(basePath, StandardOpenOption.READ)) {
            final Iterator<DoubleRealVector> vectorIterator = new StoredVecsIterator.StoredFVecsIterator(fileChannel);

            int i = 0;
            while (vectorIterator.hasNext() && i < numVectors) {
                final DoubleRealVector currentVector = vectorIterator.next();
                final Tuple currentPrimaryKey = createPrimaryKey(i++);
                insertedDataBuilder.add(new PrimaryKeyAndVector(currentPrimaryKey, currentVector));
            }
        }
        return insertedDataBuilder.build();
    }

    @Nonnull
    static List<PrimaryKeyAndVector> insertSIFT(@Nonnull final Database db,
                                                @Nonnull final Guardiann guardiann,
                                                @Nonnull final String baseFile,
                                                final int numVectors,
                                                final int desiredBatchSize) throws Exception {
        final Path siftPath = Paths.get(baseFile);

        final ImmutableList.Builder<PrimaryKeyAndVector> insertedDataBuilder = ImmutableList.builder();

        try (final var fileChannel = FileChannel.open(siftPath, StandardOpenOption.READ)) {
            final Iterator<DoubleRealVector> vectorIterator = new StoredVecsIterator.StoredFVecsIterator(fileChannel);

            int i = 0;
            while (vectorIterator.hasNext() && i < numVectors) {
                final int batchSize = Math.min(desiredBatchSize, numVectors - i);
                final List<DoubleRealVector> remainingBatch =
                        Lists.newArrayList(Iterators.limit(vectorIterator, batchSize));
                while (!remainingBatch.isEmpty()) {
                    final long currentBatchStart = i;
                    final List<PrimaryKeyAndVector> insertedInBatch =
                            basicInsertBatch(db, guardiann, remainingBatch.size(), i,
                                    (tr, nextId) -> {
                                        final int indexInBatch = Math.toIntExact(nextId - currentBatchStart);
                                        if (indexInBatch >= remainingBatch.size()) {
                                            return null;
                                        }
                                        final Tuple currentPrimaryKey = createPrimaryKey(nextId);
                                        final DoubleRealVector doubleVector = remainingBatch.get(indexInBatch);
                                        return new PrimaryKeyAndVector(currentPrimaryKey, doubleVector);
                                    });
                    //insertedDataBuilder.addAll(insertedInBatch);
                    final int numInsertedInBatch = insertedInBatch.size();
                    i += numInsertedInBatch;
                    remainingBatch.subList(0, numInsertedInBatch).clear();
                }
            }
            assertThat(i).isEqualTo(numVectors);
        }
        return insertedDataBuilder.build();
    }

    static void insertFirstRepeatedly(@Nonnull final Database db,
                                      @Nonnull final Guardiann guardiann,
                                      @Nonnull final String baseFile,
                                      final int numRepetitions,
                                      final int desiredBatchSize) throws Exception {
        final Path siftPath = Paths.get(baseFile);

        try (final var fileChannel = FileChannel.open(siftPath, StandardOpenOption.READ)) {
            final Iterator<DoubleRealVector> vectorIterator = new StoredVecsIterator.StoredFVecsIterator(fileChannel);

            if (!vectorIterator.hasNext()) {
                return;
            }

            int i = 0;
            final DoubleRealVector onlyVector = vectorIterator.next();

            while (i < numRepetitions) {
                final int batchSize = Math.min(desiredBatchSize, numRepetitions - i);
                final List<DoubleRealVector> remainingBatch =
                        IntStream.range(0, batchSize).mapToObj(ignored -> onlyVector)
                                .collect(Collectors.toList());
                while (!remainingBatch.isEmpty()) {
                    final long currentBatchStart = i;
                    final List<PrimaryKeyAndVector> insertedInBatch =
                            basicInsertBatch(db, guardiann, remainingBatch.size(), i,
                                    (tr, nextId) -> {
                                        final int indexInBatch = Math.toIntExact(nextId - currentBatchStart);
                                        if (indexInBatch >= remainingBatch.size()) {
                                            return null;
                                        }
                                        final Tuple currentPrimaryKey = createPrimaryKey(-1 - nextId);
                                        final DoubleRealVector doubleVector = remainingBatch.get(indexInBatch);
                                        return new PrimaryKeyAndVector(currentPrimaryKey, doubleVector);
                                    });
                    final int numInsertedInBatch = insertedInBatch.size();
                    i += numInsertedInBatch;
                    remainingBatch.subList(0, numInsertedInBatch).clear();
                }
            }
        }
    }

    static void validateSIFTSmall(@Nonnull final Database db,
                                  @Nonnull final Guardiann guardiann,
                                  @Nonnull final List<PrimaryKeyAndVector> data,
                                  final int k) throws IOException {
        validateSIFT(db, guardiann, data,
                ".out/extracted/siftsmall/siftsmall_queries.fvecs",
                ".out/extracted/siftsmall/siftsmall_groundtruth.ivecs", k);
    }

    static void validateSIFT(@Nonnull final Database db,
                             @Nonnull final Guardiann guardiann,
                             @Nonnull final List<PrimaryKeyAndVector> data,
                             @Nonnull final String queriesFile,
                             @Nonnull final String groundTruthFile,
                             final int k) throws IOException {
        validateSIFT(db, guardiann, data, queriesFile, groundTruthFile, k, -1);
    }

    static void validateSIFT(@Nonnull final Database db,
                             @Nonnull final Guardiann guardiann,
                             @Nonnull final List<PrimaryKeyAndVector> data,
                             @Nonnull final String queriesFile,
                             @Nonnull final String groundTruthFile,
                             final int k,
                             final int maxIndex) throws IOException {

        final Metric metric = guardiann.getConfig().getMetric();
        final Path siftQueryPath = Paths.get(queriesFile);
        final Path siftGroundTruthPath = Paths.get(groundTruthFile);

        final TestOnReadListener onReadListener = (TestOnReadListener)guardiann.getOnReadListener();

        try (final var queryChannel = FileChannel.open(siftQueryPath, StandardOpenOption.READ);
                final var groundTruthChannel = FileChannel.open(siftGroundTruthPath, StandardOpenOption.READ)) {
            final Iterator<DoubleRealVector> queryIterator = new StoredVecsIterator.StoredFVecsIterator(queryChannel);
            final Iterator<List<Integer>> groundTruthIterator = new StoredVecsIterator.StoredIVecsIterator(groundTruthChannel);

            Verify.verify(queryIterator.hasNext() == groundTruthIterator.hasNext());

            while (queryIterator.hasNext()) {
                final HalfRealVector queryVector = queryIterator.next().toHalfRealVector();
                final Set<Integer> groundTruthIndices =
                        groundTruthIterator.next()
                                .stream()
                                .filter(index -> maxIndex < 0 || index <= maxIndex)
                                .collect(ImmutableSet.toImmutableSet());
                if (groundTruthIndices.isEmpty()) {
                    logger.info("query ground truth does not have indices that have been inserted yet");
                    continue;
                }
                onReadListener.reset();
                final long beginTs = System.nanoTime();
                final List<? extends ResultEntry> results =
                        db.run(tr -> guardiann.kNearestNeighborsSearch(tr, k, 30000,
                                true, queryVector).join());
                final long endTs = System.nanoTime();
                logger.info("retrieved result in elapsedTimeMs={}, reading readBytes={}",
                        TimeUnit.NANOSECONDS.toMillis(endTs - beginTs), onReadListener.getBytesReadByLayer());

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

                final double recall = (double)recallCount / groundTruthIndices.size();
                //assertThat(recall).isGreaterThan(0.93);

                logger.info("query returned results recall={}, k={}",
                        String.format(Locale.ROOT, "%.2f", recall * 100.0d),
                        groundTruthIndices.size());
            }
        }
    }

    static void validateSIFT(@Nonnull final Database db,
                             @Nonnull final Guardiann guardiann,
                             @Nonnull final String queriesFile,
                             @Nonnull final String groundTruthFile,
                             final int k) throws IOException {
        validateSIFT(db, guardiann, queriesFile, groundTruthFile, k, -1);
    }

    static void validateSIFT(@Nonnull final Database db,
                             @Nonnull final Guardiann guardiann,
                             @Nonnull final String queriesFile,
                             @Nonnull final String groundTruthFile,
                             final int k,
                             final int maxIndex) throws IOException {
        final Path siftQueryPath = Paths.get(queriesFile);
        final Path siftGroundTruthPath = Paths.get(groundTruthFile);

        final TestOnReadListener onReadListener = (TestOnReadListener)guardiann.getOnReadListener();

        try (final var queryChannel = FileChannel.open(siftQueryPath, StandardOpenOption.READ);
                 final var groundTruthChannel = FileChannel.open(siftGroundTruthPath, StandardOpenOption.READ)) {
            final Iterator<DoubleRealVector> queryIterator = new StoredVecsIterator.StoredFVecsIterator(queryChannel);
            final Iterator<List<Integer>> groundTruthIterator = new StoredVecsIterator.StoredIVecsIterator(groundTruthChannel);

            Verify.verify(queryIterator.hasNext() == groundTruthIterator.hasNext());

            while (queryIterator.hasNext()) {
                final HalfRealVector queryVector = queryIterator.next().toHalfRealVector();
                final Set<Integer> groundTruthIndices =
                        groundTruthIterator.next()
                                .stream()
                                .filter(index -> maxIndex < 0 || index <= maxIndex)
                                .collect(ImmutableSet.toImmutableSet());
                if (groundTruthIndices.isEmpty()) {
                    logger.info("query ground truth does not have indices that have been inserted yet");
                    continue;
                }
                onReadListener.reset();
                final long beginTs = System.nanoTime();
                final List<? extends ResultEntry> results =
                        db.run(tr -> guardiann.kNearestNeighborsSearch(tr, k, 130000,
                                true, queryVector).join());
                final long endTs = System.nanoTime();
                logger.info("retrieved result in elapsedTimeMs={}, reading readBytes={}",
                        TimeUnit.NANOSECONDS.toMillis(endTs - beginTs), onReadListener.getBytesReadByLayer());

                int recallCount = 0;
                for (final ResultEntry resultEntry : results) {
                    final int primaryKeyIndex = (int)resultEntry.getPrimaryKey().getLong(0);

                    logger.trace("retrieved result nodeId = {} at distance = {} ",
                            primaryKeyIndex, resultEntry.getDistance());
                    if (groundTruthIndices.contains(primaryKeyIndex)) {
                        recallCount ++;
                    }
                }

                final double recall = (double)recallCount / groundTruthIndices.size();
                //assertThat(recall).isGreaterThan(0.93);

                logger.info("query returned results recall={}, k={}",
                        String.format(Locale.ROOT, "%.2f", recall * 100.0d), groundTruthIndices.size());
            }
        }
    }

    static class TestOnWriteListener implements OnWriteListener {
        @Nonnull
        private final Map<AbstractDeferredTask.Kind, Integer> taskExecutedByKindCounterMap;

        public TestOnWriteListener() {
            this.taskExecutedByKindCounterMap = Maps.newConcurrentMap();
        }

        @Override
        public void onTaskExecuted(@Nonnull final AbstractDeferredTask.Kind taskKind,
                                   @Nonnull final UUID taskId, @Nonnull final Set<UUID> targetClusterIds) {
            taskExecutedByKindCounterMap.compute(taskKind, (ignored, counter) ->
                    Objects.requireNonNullElse(counter, 0) + 1);
        }

        public int getTaskCounter(@Nonnull final AbstractDeferredTask.Kind taskKind) {
            return taskExecutedByKindCounterMap.getOrDefault(taskKind, 0);
        }

        public int getSumTaskCounters() {
            return taskExecutedByKindCounterMap.values().stream().mapToInt(i -> i).sum();
        }

        public void reset() {
            taskExecutedByKindCounterMap.clear();
        }
    }

    static class TestOnReadListener implements OnReadListener {
        final Map<Integer, Long> sumMByLayer;
        final Map<Integer, Long> bytesReadByLayer;

        public TestOnReadListener() {
            this.sumMByLayer = Maps.newConcurrentMap();
            this.bytesReadByLayer = Maps.newConcurrentMap();
        }

        public Map<Integer, Long> getBytesReadByLayer() {
            return bytesReadByLayer;
        }

        public Map<Integer, Long> getSumMByLayer() {
            return sumMByLayer;
        }

        public void reset() {
            bytesReadByLayer.clear();
            sumMByLayer.clear();
        }

        @Override
        public void onKeyValueRead(final int layer, @Nonnull final byte[] key, @Nullable final byte[] value) {
            bytesReadByLayer.compute(layer, (l, oldValue) -> (oldValue == null ? 0 : oldValue) +
                    key.length + (value == null ? 0 : value.length));
        }
    }
}
