/*
 * SiftTest.java
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
import com.apple.foundationdb.async.common.BaseTest;
import com.apple.foundationdb.async.common.PrimaryKeyAndVector;
import com.apple.foundationdb.async.common.ResultEntry;
import com.apple.foundationdb.async.hnsw.HNSW;
import com.apple.foundationdb.linear.Metric;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.test.TestClassSubspaceExtension;
import com.apple.foundationdb.test.TestDatabaseExtension;
import com.apple.foundationdb.test.TestExecutors;
import com.google.common.collect.Sets;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

public class SiftTest implements BaseTest {
    private static final Logger logger = LoggerFactory.getLogger(SiftTest.class);

    @RegisterExtension
    static final TestDatabaseExtension dbExtension = new TestDatabaseExtension();
    @RegisterExtension
    static final TestClassSubspaceExtension subspaceExtension = new TestClassSubspaceExtension(dbExtension);

    @TempDir
    Path tempDir;

    private static Database db;
    private static Guardiann guardiann;
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
    @Timeout(value = 30, unit = TimeUnit.MINUTES)
    public static void setUpDb() throws Exception {
        db = dbExtension.getDatabase();

        final TestHelpers.TestOnWriteListener onWriteListener = new TestHelpers.TestOnWriteListener();
        final TestHelpers.TestOnReadListener onReadListener = new TestHelpers.TestOnReadListener();

        final Metric metric = Metric.COSINE_METRIC;
        final Config config =
                Guardiann.newConfigBuilder()
                        .setUseRaBitQ(true)
                        .setRaBitQNumExBits(8)
                        .setMetric(metric)
                        .setPrimaryClusterMax(500)
                        .setPrimaryClusterMin(100)
                        .setPersistSequentialUuids(true)
                        .setClusterOverlap(0.0d)
                        .setReplicatedClusterTarget(1000)
                        .setReplicatedClusterMaxWrites(3000)
                        .build(512);

        guardiann = new Guardiann(subspaceExtension.getSubspace(),
                TestExecutors.defaultThreadPool(),
                config,
                onWriteListener,
                onReadListener);

        logger.info("Preparing db and inserting SIFT small dataset...");
        //insertedData = TestHelpers.insertSIFTSmall(db, guardiann);
        insertedData = TestHelpers.insertSIFT100k(db, guardiann, 100000, 50);
    }

    @SuppressWarnings("checkstyle:AbbreviationAsWordInName")
    @Test
    void testInsertSIFTSmall() throws Exception {
        final int k = 100;
        TestHelpers.validateSIFT(getDb(), guardiann, insertedData,
                "/Users/nseemann/Downloads/embeddings-unified-model-100k-queries-1.0.0.fvecs",
                "/Users/nseemann/Downloads/embeddings-unified-model-100k-groundtruth-1.0.0.ivecs", k);

        final HNSW centroidHnsw = guardiann.getLocator().primitives().getClusterCentroidsHnsw();

        final Set<ResultEntry> centroids = Sets.newConcurrentHashSet();
        scanCentroids(db, centroidHnsw.getSubspace(), centroidHnsw.getConfig(), 0, 100, centroids::add);

//        logger.info("checking clusters numCentroids={}", centroids.size());
//        final ListMultimap<UUID, Tuple> result = db.run(transaction -> {
//            final Search search = guardiann.getLocator().search();
//            return search.globalAssignmentCheck(transaction, ImmutableList.copyOf(centroids)).join();
//        });
//        System.out.println(result);

//        TestHelpers.validateSIFT(getDb(), guardiann, insertedData,
//                "/Users/nseemann/Downloads/sift-100k-queries.fvecs",
//                "/Users/nseemann/Downloads/sift-100k-groundtruth.ivecs", k);
    }

    static long countNodesCentroidHnsw(@Nonnull final Database db,
                                       @Nonnull final Subspace subspace,
                                       @Nonnull final com.apple.foundationdb.async.hnsw.Config config, final int layer) {
        final AtomicLong counter = new AtomicLong();
        scanCentroids(db, subspace, config, layer, 100,
                node -> counter.incrementAndGet());
        return counter.get();
    }

    static void scanCentroids(@Nonnull final Database db,
                              @Nonnull final Subspace subspace,
                              @Nonnull final com.apple.foundationdb.async.hnsw.Config config,
                              final int layer,
                              final int batchSize,
                              @Nonnull final Consumer<ResultEntry> consumer) {
        HNSW.scanLayer(config, subspace, db, layer, batchSize, consumer);
    }
}
