/*
 * RelationalServerTest.java
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

import com.apple.foundationdb.relational.grpc.jdbc.v1.DatabaseMetaDataRequest;
import com.apple.foundationdb.relational.grpc.jdbc.v1.JDBCServiceGrpc;
import com.apple.foundationdb.relational.grpc.jdbc.v1.StatementRequest;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.health.v1.HealthCheckRequest;
import io.grpc.health.v1.HealthCheckResponse;
import io.grpc.health.v1.HealthGrpc;
import org.apple.relational.grpc.GrpcConstants;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class RelationalServerTest {
    private static RelationalServer relationalServer;

    @BeforeAll
    public static void beforerAll() throws IOException {
        relationalServer =
                ServerTestUtil.createAndStartRelationalServer(GrpcConstants.DEFAULT_SERVER_PORT);
    }

    @AfterAll
    public static void afterAll() throws IOException {
        if (relationalServer != null) {
            relationalServer.close();
        }
    }

    /**
     * Stand up a server and then connect to it with a 'client', run
     * some simple JDBCService invocations, verify basically returns and then
     * shut it all down.
     */
    @Test
    public void simpleJDBCServiceClientOperation() throws IOException, InterruptedException {
        ManagedChannel managedChannel = ManagedChannelBuilder
                .forTarget("localhost:" + relationalServer.getPort())
                .usePlaintext().build();
        try {
            JDBCServiceGrpc.JDBCServiceBlockingStub stub =
                    JDBCServiceGrpc.newBlockingStub(managedChannel);
            StatementRequest statementRequest =
                    StatementRequest.newBuilder().setSql("hello").build();
            // Anemic test for now until we fill in some functionaliity.
            Assertions.assertNotNull(stub.executeStatement(statementRequest));
            DatabaseMetaDataRequest metaDataRequest =
                    DatabaseMetaDataRequest.newBuilder().build();
            Assertions.assertNotNull(stub.getMetaData(metaDataRequest));
        } finally {
            managedChannel.awaitTermination(10, TimeUnit.SECONDS);
        }
    }

    /**
     * Check the {@link HealthGrpc} Service is up and working.
     */
    @Test
    public void healthServiceClientOperation() throws IOException, InterruptedException {
        ManagedChannel managedChannel = ManagedChannelBuilder
                .forTarget("localhost:" + relationalServer.getPort())
                .usePlaintext().build();
        try {
            HealthGrpc.HealthBlockingStub stub = HealthGrpc.newBlockingStub(managedChannel);
            HealthCheckResponse healthCheckResponse =
                    stub.check(HealthCheckRequest.newBuilder().build());
            Assertions.assertEquals(HealthCheckResponse.ServingStatus.SERVING,
                    healthCheckResponse.getStatus());
            HealthCheckRequest healthCheckRequest =
                    HealthCheckRequest.newBuilder().setService(JDBCServiceGrpc.SERVICE_NAME).build();
            healthCheckResponse = stub.check(healthCheckRequest);
            Assertions.assertEquals(HealthCheckResponse.ServingStatus.SERVING,
                    healthCheckResponse.getStatus());
        } finally {
            managedChannel.awaitTermination(10, TimeUnit.SECONDS);
        }
    }
}
