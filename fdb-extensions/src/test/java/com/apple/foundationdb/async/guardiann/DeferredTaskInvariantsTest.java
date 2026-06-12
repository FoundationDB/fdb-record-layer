/*
 * DeferredTaskInvariantsTest.java
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
 * Invariants suite for {@link Guardiann}'s deferred-task machinery. Loads SIFT-small once,
 * drains the deferred-task queue to quiescence, then re-runs the invariant battery in each
 * test method so failures point at one structural property at a time.
 * <p>
 * Per-task scenario tests (forcing a specific split/merge/reassign/collapse to fire) live in a
 * sibling class so this class can stay focused on "after a healthy insert burst, every
 * structural property is satisfied". A SIFT-1M variant marked {@link com.apple.test.SuperSlow}
 * can be added alongside once SIFT-small is green.
 */
public class DeferredTaskInvariantsTest implements BaseTest {
    private static final Logger logger = LoggerFactory.getLogger(DeferredTaskInvariantsTest.class);

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
    @Timeout(value = 30, unit = TimeUnit.MINUTES)
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

        logger.info("loading SIFT-small base vectors and inserting...");
        TestHelpers.insertSIFTSmall(db, guardiann);

        logger.info("draining deferred-task queue to quiescence...");
        TestHelpers.runToQuiescence(db, guardiann);

        logger.info("pre-loading SIFT-small query vectors and ground truth...");
        queries = TestHelpers.loadSiftQueryVectors(TestHelpers.SIFT_SMALL_QUERY_PATH);
        groundTruth = TestHelpers.loadSiftGroundTruth(TestHelpers.SIFT_SMALL_GROUNDTRUTH_PATH, -1);
    }

    /**
     * After insertion + drain, the cluster topology must satisfy every currently-defined
     * structural invariant: queue is quiescent, every primary VectorId is uniquely accounted
     * for, every replica references a live primary.
     */
    @Test
    void siftSmallSatisfiesStructuralInvariants() {
        TestHelpers.assertGuardiannInvariants(getDb(), guardiann);
    }

    /**
     * Search quality floor: mean set-based recall@100 over all SIFT-small queries must clear
     * 0.85. This is a conservative starting threshold — tighten once we have a few clean runs.
     */
    @Test
    void siftSmallRecallAtK100MeetsThreshold() {
        TestHelpers.assertRecallAtKAtLeast(getDb(), guardiann, queries, groundTruth, 1000, 0.85d);
    }
}
