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
import java.util.ArrayDeque;
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
            onWriteListener.pushFrame();
            final TestOnReadListener onReadListener = (TestOnReadListener)guardiann.getOnReadListener();
            onReadListener.pushFrame();

            final ImmutableList.Builder<PrimaryKeyAndVector> data = ImmutableList.builder();

            final long beginTs = System.nanoTime();

            final CompletableFuture<Integer> loopFuture =
                    MoreAsyncUtil.forLoop(0, 0,
                            (i, u) ->
                                    i < batchSize && (int)u == i && onWriteListener.getSumTaskExecutedCounters() == 0,
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
                            logger.trace("failed to insert batchSize={}", error);
                        } else {
                            final long endTs = System.nanoTime();
                            logger.trace("inserted batchSize={} records={} starting at id={} took elapsedTime={}ms, readBytes={}",
                                    batchSize, result.size(), firstId, TimeUnit.NANOSECONDS.toMillis(endTs - beginTs),
                                    onReadListener.getBytesReadByLayer());
                        }
                        onWriteListener.popFrame();
                        onReadListener.popFrame();
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
        return insertSIFT(db, guardiann, "/Users/nseemann/downloads/embeddings-unified-model-100k-1.0.0.fvecs",
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

        final TestOnReadListener onReadListener = (TestOnReadListener)guardiann.getOnReadListener();
        final TestOnWriteListener onWriteListener = (TestOnWriteListener)guardiann.getOnWriteListener();
        onReadListener.pushFrame();
        onWriteListener.pushFrame();

        try (final var fileChannel = FileChannel.open(siftPath, StandardOpenOption.READ)) {
            final Iterator<DoubleRealVector> vectorIterator = new StoredVecsIterator.StoredFVecsIterator(fileChannel);

            int i = 0;
            while (vectorIterator.hasNext() && i < numVectors) {
                onReadListener.pushFrame();
                onWriteListener.pushFrame();

                final int batchSize = Math.min(desiredBatchSize, numVectors - i);
                final long beginTs = System.nanoTime();
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
                final long endTs = System.nanoTime();
                final long totalBytesRead = onReadListener.getBytesReadByLayer().values().stream().mapToLong(x -> x).sum();
                final long totalBytesWritten = onWriteListener.getBytesWrittenByLayer().values().stream().mapToLong(x -> x).sum();

                logger.info("inserted batchSize={} for a total of numRecords={} took elapsedTime={}ms, bytesRead={}, bytesWritten={}, taskCountByKind={}",
                        batchSize, i, TimeUnit.NANOSECONDS.toMillis(endTs - beginTs),
                        totalBytesRead, totalBytesWritten, onWriteListener.getNumTasksEnqueuedByKind());

                onWriteListener.popFrame();
                onReadListener.popFrame();
            }
            assertThat(i).isEqualTo(numVectors);
        }
        logger.info("total number of tasks enqueued by kind={}", onWriteListener.getNumTasksEnqueuedByKind());
        logger.info("total number of tasks executed by kind={}", onWriteListener.getNumTasksExecutedByKind());

        onWriteListener.popFrame();
        onReadListener.popFrame();
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

            final int efSearch = (int)((double)k * 1.15);

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
                onReadListener.pushFrame();
                final long beginTs = System.nanoTime();
                final List<? extends ResultEntry> results =
                        db.run(tr -> guardiann.kNearestNeighborsSearch(tr, k, efSearch,
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
                onReadListener.popFrame();
            }
        }
    }

    static List<RealVector> readQueryVectors(@Nonnull final String queriesFile) throws IOException {
        final ImmutableList.Builder<RealVector> resultBuilder = ImmutableList.builder();
        final Path queryPath = Paths.get(queriesFile);

        try (final var queryChannel = FileChannel.open(queryPath, StandardOpenOption.READ)) {
            final Iterator<DoubleRealVector> queryIterator = new StoredVecsIterator.StoredFVecsIterator(queryChannel);

            while (queryIterator.hasNext()) {
                final DoubleRealVector queryVector = queryIterator.next();

                resultBuilder.add(queryVector);
            }
        }
        return resultBuilder.build();
    }

    static class TestOnWriteListener implements OnWriteListener {
        @Nonnull
        private final ArrayDeque<Frame> frames;

        public TestOnWriteListener() {
            this.frames = new ArrayDeque<>();
        }

        @Override
        public void onKeyValueWritten(final int layer, @Nonnull final byte[] key, @Nonnull final byte[] value) {
            for (final Frame frame : frames) {
                frame.bytesWrittenByLayer()
                        .compute(layer, (l, oldValue) -> (oldValue == null ? 0 : oldValue) +
                                key.length + (value == null ? 0 : value.length));
            }
        }

        @Override
        public void onTaskEnqueued(@Nonnull final AbstractDeferredTask.Kind taskKind,
                                   @Nonnull final UUID taskId, @Nonnull final Set<UUID> targetClusterIds) {
            for (final Frame frame : frames) {
                frame.numTasksEnqueuedByKind()
                        .compute(taskKind, (ignored, counter) ->
                                Objects.requireNonNullElse(counter, 0) + 1);
            }
        }

        @Override
        public void onTaskExecuted(@Nonnull final AbstractDeferredTask.Kind taskKind,
                                   @Nonnull final UUID taskId, @Nonnull final Set<UUID> targetClusterIds) {
            for (final Frame frame : frames) {
                frame.numTasksExecutedByKind().compute(taskKind, (ignored, counter) ->
                        Objects.requireNonNullElse(counter, 0) + 1);
            }
        }

        @Nonnull
        public Map<AbstractDeferredTask.Kind, Integer> getNumTasksEnqueuedByKind() {
            return Objects.requireNonNull(frames.peek()).numTasksEnqueuedByKind();
        }

        @Nonnull
        public Map<AbstractDeferredTask.Kind, Integer> getNumTasksExecutedByKind() {
            return Objects.requireNonNull(frames.peek()).numTasksExecutedByKind();
        }

        public int getSumTaskExecutedCounters() {
            return Objects.requireNonNull(frames.peek()).numTasksExecutedByKind().values().stream().mapToInt(i -> i).sum();
        }

        @Nonnull
        public Map<Integer, Long> getBytesWrittenByLayer() {
            return Objects.requireNonNull(frames.peek()).bytesWrittenByLayer();
        }

        public void pushFrame() {
            frames.push(new Frame(Maps.newConcurrentMap(), Maps.newConcurrentMap(), Maps.newConcurrentMap()));
        }

        public void popFrame() {
            frames.pop();
        }

        private record Frame(@Nonnull Map<Integer, Long> bytesWrittenByLayer,
                             @Nonnull Map<AbstractDeferredTask.Kind, Integer> numTasksEnqueuedByKind,
                             Map<AbstractDeferredTask.Kind, Integer> numTasksExecutedByKind) {
        }
    }

    static class TestOnReadListener implements OnReadListener {
        @Nonnull
        private final ArrayDeque<Frame> frames;

        public TestOnReadListener() {
            this.frames = new ArrayDeque<>();
        }

        @Nonnull
        public Map<Integer, Long> getBytesReadByLayer() {
            return Objects.requireNonNull(frames.peek()).bytesReadByLayer();
        }

        @Override
        public void onKeyValueRead(final int layer, @Nonnull final byte[] key, @Nullable final byte[] value) {
            for (final Frame frame : frames) {
                frame.bytesReadByLayer().compute(layer,
                        (l, oldValue) -> (oldValue == null ? 0 : oldValue) +
                                key.length + (value == null ? 0 : value.length));
            }
        }

        public void pushFrame() {
            frames.push(new Frame(Maps.newConcurrentMap()));
        }

        public void popFrame() {
            frames.pop();
        }

        private record Frame(@Nonnull Map<Integer, Long> bytesReadByLayer) {
        }
    }
}
