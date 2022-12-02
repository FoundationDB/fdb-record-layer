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

import com.apple.foundationdb.relational.grpc.GrpcConstants;
import com.apple.foundationdb.relational.grpc.jdbc.v1.JDBCServiceGrpc;
import com.apple.foundationdb.relational.grpc.jdbc.v1.SQLException;
import com.apple.foundationdb.relational.grpc.jdbc.v1.StatementRequest;
import com.apple.foundationdb.relational.grpc.jdbc.v1.StatementResponse;

import com.google.protobuf.Any;
import com.google.protobuf.TextFormat;
import com.google.spanner.v1.ResultSet;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.health.v1.HealthCheckRequest;
import io.grpc.health.v1.HealthCheckResponse;
import io.grpc.health.v1.HealthGrpc;
import io.grpc.protobuf.StatusProto;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

public class RelationalServerTest {
    private static final Logger logger = Logger.getLogger(RelationalServerTest.class.getName());
    private static RelationalServer relationalServer;

    @BeforeAll
    public static void beforeAll() throws IOException {
        relationalServer = ServerTestUtil.createAndStartRelationalServer(GrpcConstants.DEFAULT_SERVER_PORT);
    }

    @AfterAll
    public static void afterAll() throws IOException {
        if (relationalServer != null) {
            relationalServer.close();
        }
    }

    private static void update(JDBCServiceGrpc.JDBCServiceBlockingStub stub, String database, String schema, String sql) {
        // TODO: Do I have to supply the database and the schema each time or can I rely on database metadata?
        // Maybe I do. Then its easy on the client-side changing which database to go against? Connection stays open
        // and we just change the target db on the server-side that we go against?
        StatementRequest statementRequest = StatementRequest.newBuilder().setSql(sql)
                .setDatabase(database).setSchema(schema).build();
        StatementResponse statementResponse = stub.update(statementRequest);
        // TODO: Make server-side return a row_count; doens't currently.
        Assertions.assertTrue(statementResponse.getRowCount() == 0);
    }

    @Nullable
    private static ResultSet execute(JDBCServiceGrpc.JDBCServiceBlockingStub stub, String database, String schema, String sql) {
        StatementRequest statementRequest = StatementRequest.newBuilder().setSql(sql)
                .setDatabase(database).setSchema(schema).build();
        StatementResponse statementResponse = stub.execute(statementRequest);
        return statementResponse.hasResultSet() ? statementResponse.getResultSet() : null;
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
        JDBCServiceGrpc.JDBCServiceBlockingStub stub =
                JDBCServiceGrpc.newBlockingStub(managedChannel);
        String sysdb = "/__SYS";
        String schema = "CATALOG";
        String testdb = "/test_db";
        try {
            update(stub, sysdb, schema, "Drop database \"" + testdb + "\"");
            update(stub, sysdb, schema,
                    "CREATE SCHEMA TEMPLATE test_template " +
                            "CREATE TABLE test_table (rest_no int64, name string, PRIMARY KEY(rest_no))");
            update(stub, sysdb, schema, "create database \"" + testdb + "\"");
            update(stub, sysdb, schema, "create schema \"" + testdb + "/test_schema\" with template test_template");
            ResultSet resultSet = execute(stub, sysdb, schema, "select * from databases;");
            Assertions.assertEquals(2, resultSet.getRowsCount());
            Assertions.assertEquals(1, resultSet.getRowsList().get(0).getValuesCount());
            Assertions.assertEquals(1, resultSet.getRowsList().get(1).getValuesCount());
            Assertions.assertTrue(resultSet.getRows(0).getValues(0).hasStringValue());
            Assertions.assertTrue(resultSet.getRows(1).getValues(0).hasStringValue());
            Assertions.assertEquals(sysdb, resultSet.getRows(0).getValues(0).getStringValue());
            Assertions.assertEquals(testdb, resultSet.getRows(1).getValues(0).getStringValue());
        } catch (Throwable t) {
            com.google.rpc.Status status = StatusProto.fromThrowable(t);
            if (status != null) {
                SQLException sqlException = null;
                for (Any any : status.getDetailsList()) {
                    if (!any.is(SQLException.class)) {
                        continue;
                    }
                    sqlException = any.unpack(SQLException.class);
                    break;
                }
                logger.severe(sqlException.getMessage());
                logger.severe(t + ", " + TextFormat.shortDebugString(sqlException));
            }
            throw t;
        } finally {
            update(stub, sysdb, schema, "Drop database \"/test_db\"");
            managedChannel.shutdownNow();
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
            managedChannel.shutdownNow();
        } finally {
            boolean timedout = managedChannel.awaitTermination(10, TimeUnit.SECONDS);
            logger.info(() -> "awaitTermination timedout=" + timedout);
        }
    }
}
