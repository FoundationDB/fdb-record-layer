/*
 * SiftTest.java
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
import com.apple.foundationdb.async.AsyncIterator;
import com.apple.foundationdb.async.hnsw.TestHelpers.PrimaryKeyAndVector;
import com.apple.foundationdb.async.hnsw.TestHelpers.PrimaryKeyVectorAndDistance;
import com.apple.foundationdb.async.hnsw.TestHelpers.TestOnReadListener;
import com.apple.foundationdb.async.hnsw.TestHelpers.TestOnWriteListener;
import com.apple.foundationdb.linear.DoubleRealVector;
import com.apple.foundationdb.linear.Metric;
import com.apple.foundationdb.linear.RealVector;
import com.apple.foundationdb.linear.StoredVecsIterator;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.test.TestClassSubspaceExtension;
import com.apple.foundationdb.test.TestDatabaseExtension;
import com.apple.foundationdb.test.TestExecutors;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.RandomSeedSource;
import com.apple.test.Tags;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.params.ParameterizedTest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.NavigableSet;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

/**
 * Tests testing insert/update/deletes of data into/in/from {@link HNSW}s.
 */
@Execution(ExecutionMode.SAME_THREAD)
@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
@Tag(Tags.RequiresFDB)
@Tag(Tags.Slow)
class SiftTest implements BaseTest {
    private static final Logger logger = LoggerFactory.getLogger(SiftTest.class);

    @RegisterExtension
    static final TestDatabaseExtension dbExtension = new TestDatabaseExtension();
    @RegisterExtension
    static final TestClassSubspaceExtension subspaceExtension = new TestClassSubspaceExtension(dbExtension);
    @RegisterExtension
    static final TestClassSubspaceExtension rtSecondarySubspace = new TestClassSubspaceExtension(dbExtension);

    @TempDir
    Path tempDir;

    private static Database db;
    private static HNSW hnsw;
    private static List<PrimaryKeyAndVector> insertedData;

    @Nonnull
    @Override
    public Database getDb() {
        return Objects.requireNonNull(db);
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

    @BeforeAll
    @Timeout(value = 10, unit = TimeUnit.MINUTES)
    public static void setUpDb() throws Exception {
        db = dbExtension.getDatabase();

        final TestOnWriteListener onWriteListener = new TestOnWriteListener();
        final TestOnReadListener onReadListener = new TestOnReadListener();

        final Metric metric = Metric.EUCLIDEAN_METRIC;
        final Config config =
                HNSW.newConfigBuilder()
                        .setUseRaBitQ(true)
                        .setRaBitQNumExBits(6)
                        .setMetric(metric)
                        .setM(32)
                        .setMMax(32)
                        .setMMax0(64)
                        .build(128);

        hnsw = new HNSW(subspaceExtension.getSubspace(),
                TestExecutors.defaultThreadPool(),
                config,
                onWriteListener,
                onReadListener);

        logger.info("Preparing db and inserting SIFT small dataset...");
        insertedData = TestHelpers.insertSIFTSmall(db, hnsw);
    }

    @Test
    void testInsertSIFTSmall() throws Exception {
        final int k = 100;
        TestHelpers.validateSIFTSmall(getDb(), hnsw, insertedData, k);
    }

    @ParameterizedTest
    @RandomSeedSource({0x0fdbL, 0x5ca1eL, 123456L, 78910L, 1123581321345589L})
    void testOrderByDistanceSIFTSmall(final long seed) throws Exception {
        final Random random = new Random(seed);

        // pick the query
        final int queryIndex = random.nextInt(100);

        final RealVector queryVector;
        final Path siftSmallQueryPath = Paths.get(".out/extracted/siftsmall/siftsmall_query.fvecs");
        try (final var queryChannel = FileChannel.open(siftSmallQueryPath, StandardOpenOption.READ)) {
            final Iterator<DoubleRealVector> queryIterator = new StoredVecsIterator.StoredFVecsIterator(queryChannel);

            for (int queryCounter = 0; queryCounter < queryIndex; queryCounter ++) {
                queryIterator.next();
            }
            queryVector = queryIterator.next();
        }

        final NavigableSet<PrimaryKeyVectorAndDistance> orderedByDistances =
                TestHelpers.orderedByDistances(Metric.EUCLIDEAN_METRIC, insertedData, queryVector);

        // pick a random marker and a non-zero length
        final int minIndex = random.nextInt(insertedData.size());

        final int length;
        if (random.nextBoolean()) {
            length = 1 + random.nextInt(insertedData.size() - minIndex);
        } else {
            length = insertedData.size() - minIndex;
        }

        //length = Math.min(insertedData.size() - minIndex, 100);
        int afterMaxIndex = minIndex + length;

        final PrimaryKeyVectorAndDistance minVectorAndDistance = Iterables.get(orderedByDistances, minIndex);
        final PrimaryKeyVectorAndDistance afterMaxVectorAndDistance =
                minIndex + length >= orderedByDistances.size()
                ? null
                : Iterables.get(orderedByDistances, afterMaxIndex);

        if (logger.isInfoEnabled()) {
            logger.info("min index={}; min distance={}; primaryKey={}",
                    minIndex, minVectorAndDistance.getDistance(), minVectorAndDistance.getPrimaryKey());
            if (afterMaxVectorAndDistance == null) {
                logger.info("after max index={}; after max distance=∞; after primaryKey=∞", afterMaxIndex);
            } else {
                logger.info("max index={}; max distance={}; primaryKey={}",
                        afterMaxIndex, afterMaxVectorAndDistance.getDistance(), afterMaxVectorAndDistance.getPrimaryKey());
            }
        }

        final TestOnReadListener onReadListener = ((TestOnReadListener)hnsw.getOnReadListener());

        final List<ResultEntry> results =
                db.run(tr -> {
                    onReadListener.reset();
                    final long startTs = System.nanoTime();
                    final AsyncIterator<ResultEntry> it =
                            hnsw.orderByDistance(tr, 100, 500, false, queryVector,
                                    minVectorAndDistance.getDistance(), minVectorAndDistance.getPrimaryKey());

                    final ImmutableList.Builder<ResultEntry> resultsBuilder = ImmutableList.builder();
                    for (int resultCounter = 0; resultCounter < length && it.hasNext(); resultCounter ++) {
                        resultsBuilder.add(it.next());
                    }
                    final long endTs = System.nanoTime();
                    final long durationsMs = TimeUnit.NANOSECONDS.toMillis(endTs - startTs);

                    if (logger.isInfoEnabled()) {
                        logger.info("streamed results; durationMs={}; nodeCountByLayer={}", durationsMs,
                                onReadListener.getNodeCountByLayer());
                    }

                    return resultsBuilder.build();
                });

        int numInversions = 0;
        for (int i = 1; i < results.size(); i++) {
            final ResultEntry previous = results.get(i - 1);
            final ResultEntry current = results.get(i);

            if (previous.getDistance() > current.getDistance()) {
                numInversions++;
            }
        }

        if (logger.isInfoEnabled()) {
            logger.info("total={}; inverted={}", results.size(), numInversions);
        }

        final List<Tuple> groundTruth =
                orderedByDistances.stream()
                        .map(PrimaryKeyAndVector::getPrimaryKey)
                        .collect(ImmutableList.toImmutableList());

        final Stream<Tuple> groundTruthSkipped =
                groundTruth.stream()
                        .skip(minIndex);

        final Stream<Tuple> groundTruthLimited;
        if (afterMaxVectorAndDistance == null) {
            groundTruthLimited = groundTruthSkipped;
        } else {
            groundTruthLimited = groundTruthSkipped.limit(length);
        }

        final ImmutableSet<Tuple> groundTruthExpected =
                groundTruthLimited.collect(ImmutableSet.toImmutableSet());

        final ImmutableSet<Tuple> resultIds =
                results.stream()
                        .map(ResultEntry::getPrimaryKey)
                        .collect(ImmutableSet.toImmutableSet());

        final Set<Tuple> commonIds = Sets.intersection(groundTruthExpected, resultIds);
        final int recallCount = commonIds.size();
        final double recall = (double)recallCount / groundTruth.size();

        if (logger.isInfoEnabled()) {
            logger.info("ground truth size={}; result records size={}; recallCount={}; recall={}",
                    groundTruth.size(), resultIds.size(), commonIds.size(),
                    String.format(Locale.ROOT, "%.2f", recall * 100.0d));
        }

        OrderQuality.Result r =
                OrderQuality.score(ImmutableList.copyOf(resultIds),
                        groundTruth, minIndex, 300);
        System.out.println(r);
    }
}
