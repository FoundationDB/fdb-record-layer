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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiFunction;

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

            CompletableFuture<Void> future = CompletableFuture.completedFuture(null);
            final long beginTs = System.nanoTime();
            for (int i = 0; i < batchSize; i ++) {
                final PrimaryKeyAndVector record = insertFunction.apply(tr, firstId + i);
                if (record == null) {
                    break;
                }
                data.add(record);
                future = future.thenCompose(ignore ->
                                guardiann.insert(tr, record.getPrimaryKey(), record.getVector(), null))/*.thenAccept(ignored -> logger.info("record inserted"))*/;
            }
            return future.thenApply(vignore -> data.build())
                    .whenComplete((result, error) -> {
                        if (error != null) {
                            logger.info("failed to insert batchSize={}", error);
                        } else {
                            final long endTs = System.nanoTime();
                            logger.info("inserted batchSize={} records={} starting at nodeId={} took elapsedTime={}ms, readBytes={}",
                                    batchSize, result.size(), firstId, TimeUnit.NANOSECONDS.toMillis(endTs - beginTs),
                                    onReadListener.getBytesReadByLayer());
                        }
                    });
        }).get(2, TimeUnit.MINUTES); // set a timeout for inserting a single batch including retries so setup won't run forever
    }

    static List<PrimaryKeyAndVector> insertSIFTSmall(@Nonnull final Database db,
                                                     @Nonnull final Guardiann guardiann) throws Exception {
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
                        basicInsertBatch(db, guardiann, batchSize, i,
                                (tr, nextId) -> {
                                    final int indexInBatch = Math.toIntExact(nextId - currentBatchStart);
                                    if (indexInBatch >= batch.size()) {
                                        return null;
                                    }
                                    final Tuple currentPrimaryKey = createPrimaryKey(nextId);
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
                                  @Nonnull final Guardiann guardiann,
                                  @Nonnull final List<PrimaryKeyAndVector> data,
                                  final int k) throws IOException {
        final Metric metric = guardiann.getConfig().getMetric();
        final Path siftSmallGroundTruthPath = Paths.get(".out/extracted/siftsmall/siftsmall_groundtruth.ivecs");
        final Path siftSmallQueryPath = Paths.get(".out/extracted/siftsmall/siftsmall_query.fvecs");

        final TestOnReadListener onReadListener = (TestOnReadListener)guardiann.getOnReadListener();

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
                        db.run(tr -> guardiann.kNearestNeighborsSearch(tr, k, 10000,
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

                final double recall = (double)recallCount / k;
                assertThat(recall).isGreaterThan(0.93);

                logger.info("query returned results recall={}", String.format(Locale.ROOT, "%.2f", recall * 100.0d));
            }
        }
    }

    static class TestOnWriteListener implements OnWriteListener {
        public TestOnWriteListener() {
        }

        public void reset() {
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
