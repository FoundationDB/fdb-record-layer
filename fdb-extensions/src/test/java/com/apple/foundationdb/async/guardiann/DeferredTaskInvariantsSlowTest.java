/*
 * DeferredTaskInvariantsSlowTest.java
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
import com.apple.foundationdb.linear.DoubleRealVector;
import com.apple.foundationdb.linear.Metric;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.test.TestClassSubspaceExtension;
import com.apple.foundationdb.test.TestDatabaseExtension;
import com.apple.foundationdb.test.TestExecutors;
import com.apple.test.SuperSlow;
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

/**
 * SIFT-1M sibling of {@link DeferredTaskInvariantsTest}. Same shape (load → drain → invariants
 * + recall), gated under {@link com.apple.test.SuperSlow} so the full 1M-vector load only runs in nightly
 * builds, not on every developer change. Lives in its own class because {@code @BeforeAll}
 * can't be conditionalized — the SIFT-small and SIFT-1M setups must not share fixtures.
 */
public class DeferredTaskInvariantsSlowTest implements BaseTest {
    private static final Logger logger = LoggerFactory.getLogger(DeferredTaskInvariantsSlowTest.class);

    @RegisterExtension
    static final TestDatabaseExtension dbExtension = new TestDatabaseExtension();
    @RegisterExtension
    static final TestClassSubspaceExtension subspaceExtension = new TestClassSubspaceExtension(dbExtension);

    @TempDir
    Path tempDir;

    private static Database db;
    private static Guardiann guardiann;
    private static List<DoubleRealVector> queries;
    private static List<Set<Integer>> groundTruth;

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
    @Timeout(value = 4, unit = TimeUnit.HOURS)
    public static void setUpDb() throws Exception {
        db = dbExtension.getDatabase();

        final TestHelpers.TestOnWriteListener onWriteListener = new TestHelpers.TestOnWriteListener();
        final TestHelpers.TestOnReadListener onReadListener = new TestHelpers.TestOnReadListener();

        final Config config = Guardiann.newConfigBuilder()
                .setUseRaBitQ(true)
                .setRaBitQNumExBits(6)
                .setMetric(Metric.EUCLIDEAN_METRIC)
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

        logger.info("loading SIFT-1M base vectors and inserting...");
        TestHelpers.insertSIFT1m(db, guardiann, 1_000_000, 100);

        logger.info("draining deferred-task queue to quiescence...");
        TestHelpers.runToQuiescence(db, guardiann);

        logger.info("pre-loading SIFT-1M query vectors and ground truth...");
        queries = TestHelpers.loadSiftQueryVectors(TestHelpers.SIFT_1M_QUERY_PATH);
        groundTruth = TestHelpers.loadSiftGroundTruth(TestHelpers.SIFT_1M_GROUNDTRUTH_PATH, -1);
    }

    @Test
    @SuperSlow
    void sift1mSatisfiesStructuralInvariants() {
        TestHelpers.assertGuardiannInvariants(getDb(), guardiann);
    }

    @Test
    @SuperSlow
    void sift1mRecallAtK100MeetsThreshold() {
        TestHelpers.assertRecallAtKAtLeast(getDb(), guardiann, queries, groundTruth, 100, 0.85d);
    }
}
