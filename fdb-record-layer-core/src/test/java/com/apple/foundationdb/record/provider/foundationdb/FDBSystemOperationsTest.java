/*
 * FDBSystemOperationsTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2021 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.provider.foundationdb;

import com.apple.foundationdb.record.test.FDBDatabaseExtension;
import com.apple.foundationdb.record.test.FakeClusterFileUtil;
import com.apple.test.Tags;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import javax.annotation.Nonnull;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test class for {@link FDBSystemOperations}.
 */
@Tag(Tags.RequiresFDB)
@Execution(ExecutionMode.CONCURRENT)
public class FDBSystemOperationsTest {
    @RegisterExtension
    final FDBDatabaseExtension dbExtension = new FDBDatabaseExtension();
    private FDBDatabase fdb;
    private FDBStoreTimer timer;

    @BeforeEach
    public void setup() {
        fdb = dbExtension.getDatabase();
        timer = new FDBStoreTimer();
    }

    private <T> T run(@Nonnull Function<FDBDatabaseRunner, T> operation) {
        try (FDBDatabaseRunner runner = fdb.newRunner(timer, null)) {
            return operation.apply(runner);
        }
    }

    @Test
    void primaryDatacenter() {
        // Because we don't know what the data-center was set to run the test, the best this test
        // can do is validate that this doesn't throw an error.
        run(FDBSystemOperations::getPrimaryDatacenter);
    }

    @Test
    void clusterFilePath() {
        String clusterFilePath = run(FDBSystemOperations::getClusterFilePath);
        assertNotNull(clusterFilePath);

        // Note that fdb.getClusterFile() returns null if the client is configured to use the default
        // system cluster file, so this test can't compare the results of fdb.getClusterFile() to
        // clusterFilePath to check correctness. The best it can do is make sure the file exists.
        File file = new File(clusterFilePath);
        assertTrue(file.exists(), "cluster file should exist");
    }

    @Test
    void fakeClusterFilePath() throws IOException {
        String fakeClusterFilePath = FakeClusterFileUtil.createFakeClusterFile("readClusterFilePath");
        final FDBDatabase fakeDatabase = dbExtension.getDatabaseFactory().getDatabase(fakeClusterFilePath);
        final String readClusterFilePath;
        try (FDBDatabaseRunner runner = fakeDatabase.newRunner()) {
            readClusterFilePath = FDBSystemOperations.getClusterFilePath(runner);
        }
        assertEquals(fakeClusterFilePath, readClusterFilePath);
    }

    @Test
    void clusterConnectionString() {
        String connectionString = run(FDBSystemOperations::getConnectionString);
        assertNotNull(connectionString);
    }

    @Test
    void fakeClusterConnectionString() throws IOException {
        final String fakeClusterFilePath = FakeClusterFileUtil.createFakeClusterFile("readConnectionString");
        String fakeConnectionString;
        try (BufferedReader reader = new BufferedReader(new FileReader(fakeClusterFilePath))) {
            fakeConnectionString = reader.readLine();
        }

        final FDBDatabase fakeDatabase = dbExtension.getDatabaseFactory().getDatabase(fakeClusterFilePath);
        String readConnectionString;
        try (FDBDatabaseRunner runner = fakeDatabase.newRunner()) {
            readConnectionString = FDBSystemOperations.getConnectionString(runner);
        }
        assertEquals(fakeConnectionString, readConnectionString);
    }
}
