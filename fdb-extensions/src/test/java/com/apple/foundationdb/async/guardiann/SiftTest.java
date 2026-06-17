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
import com.apple.foundationdb.async.common.ResultEntry;
import com.apple.foundationdb.async.hnsw.HNSW;
import com.apple.foundationdb.linear.Metric;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.test.TestClassSubspaceExtension;
import com.apple.foundationdb.test.TestDatabaseExtension;
import com.apple.foundationdb.test.TestExecutors;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.nio.file.Path;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
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
    @Timeout(value = 300, unit = TimeUnit.MINUTES)
    public static void setUpDb() throws Exception {
        db = dbExtension.getDatabase();

        final TestHelpers.TestOnWriteListener onWriteListener = new TestHelpers.TestOnWriteListener();
        final TestHelpers.TestOnReadListener onReadListener = new TestHelpers.TestOnReadListener();

        final Metric metric = Metric.EUCLIDEAN_METRIC;
        final Config config =
                Guardiann.newConfigBuilder()
                        .setUseRaBitQ(true)
                        .setRaBitQNumExBits(6)
                        .setMetric(metric)
                        .setPrimaryClusterMax(512)
                        .setPrimaryClusterMin(100)
                        .setDeterministicRandomness(true)
                        .setReplicationPriorityMin(0.65d)
                        .setReplicatedClusterTarget(500)
                        .setReplicatedClusterMaxWrites(2000)
                        .build(128);

        guardiann = new Guardiann(subspaceExtension.getSubspace(),
                TestExecutors.defaultThreadPool(),
                config,
                onWriteListener,
                onReadListener);

        logger.info("Preparing db and inserting SIFT small dataset...");
        TestHelpers.insertSIFTSmall(db, guardiann);
    }

    @SuppressWarnings("checkstyle:AbbreviationAsWordInName")
    @Test
    void testInsertSIFTSmall() throws Exception {
        final int k = 100;
        TestHelpers.queryVectors(getDb(), guardiann,
                TestHelpers.SIFT_SMALL_QUERY_PATH,
                TestHelpers.SIFT_SMALL_GROUNDTRUTH_PATH, k);
        TestHelpers.assertGuardiannInvariants(getDb(), guardiann);
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
