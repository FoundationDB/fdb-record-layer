/*
 * InProcessRelationalServerTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2024 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.server;

import com.apple.foundationdb.test.FDBTestEnvironment;
import io.grpc.ManagedChannel;
import io.grpc.inprocess.InProcessChannelBuilder;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;

/**
 * Test server instance that runs inprocess.
 */
public class InProcessRelationalServerTest {
    private static InProcessRelationalServer server;

    @BeforeAll
    public static void beforeAll() throws IOException {
        server = new InProcessRelationalServer(FDBTestEnvironment.randomClusterFile()).start();
    }

    @AfterAll
    public static void afterAll() throws IOException {
        if (server != null) {
            server.close();
        }
    }

    /**
     * Connect to the running in-process server and then run some simple JDBCService invocations. Verify basically
     * runs and then shut it all down. Runs basic test from sister test class, RelationalServerTest.
     */
    @Test
    public void simpleJDBCServiceClientOperation() throws IOException, InterruptedException {
        String serverName = this.server.getServerName();
        ManagedChannel managedChannel = InProcessChannelBuilder.forName(serverName).directExecutor().build();
        try {
            // Run the tests in the sister test class, RelationalServerTest.
            RelationalServerTest.simpleJDBCServiceClientOperation(managedChannel);
        } finally {
            managedChannel.shutdownNow();
        }
    }
}
