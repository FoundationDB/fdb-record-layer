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

import com.apple.foundationdb.record.RecordCoreInterruptedException;
import com.apple.foundationdb.relational.jdbc.grpc.GrpcConstants;
import com.apple.foundationdb.relational.jdbc.grpc.v1.JDBCServiceGrpc;
import com.apple.foundationdb.relational.jdbc.grpc.v1.ResultSet;
import com.apple.foundationdb.relational.jdbc.grpc.v1.StatementRequest;
import com.apple.foundationdb.relational.jdbc.grpc.v1.StatementResponse;
import com.apple.foundationdb.relational.recordlayer.RelationalKeyspaceProvider;
import com.apple.foundationdb.test.FDBTestEnvironment;
import com.apple.test.Tags;
import com.google.protobuf.TextFormat;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.health.v1.HealthCheckRequest;
import io.grpc.health.v1.HealthCheckResponse;
import io.grpc.health.v1.HealthGrpc;
import io.grpc.protobuf.StatusProto;
import io.prometheus.client.Collector;
import io.prometheus.client.CollectorRegistry;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.net.BindException;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

public class RelationalServerTest {
    private static final Logger logger = LogManager.getLogger(RelationalServerTest.class.getName());
    private static RelationalServer relationalServer;

    @BeforeAll
    public static void beforeAll() throws IOException {
        relationalServer = ServerTestUtil.createAndStartRelationalServer(GrpcConstants.DEFAULT_SERVER_PORT);
    }

    @BeforeEach
    public void beforeEach() {
        // Clear the default CollectorRegistry to prevent "Collector already registered" errors
        // when multiple tests run that create RelationalServer instances
        // BUT don't clear it for main() method tests since they rely on the default registry
        if (!Thread.currentThread().getStackTrace()[2].getMethodName().contains("testMain")) {
            CollectorRegistry.defaultRegistry.clear();
        }
    }

    @AfterEach
    public void afterEach() {
        // Clear again after each test to ensure clean state
        // BUT don't clear it for main() method tests since they rely on the default registry
        if (!Thread.currentThread().getStackTrace()[2].getMethodName().contains("testMain")) {
            CollectorRegistry.defaultRegistry.clear();
        }
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
        Assertions.assertEquals(0, statementResponse.getRowCount());
    }

    @Nullable
    private static ResultSet execute(JDBCServiceGrpc.JDBCServiceBlockingStub stub, String database, String schema, String sql) {
        StatementRequest statementRequest = StatementRequest.newBuilder().setSql(sql)
                .setDatabase(database).setSchema(schema).build();
        StatementResponse statementResponse = stub.execute(statementRequest);
        return statementResponse.hasResultSet() ? statementResponse.getResultSet() : null;
    }

    static void simpleJDBCServiceClientOperation(ManagedChannel managedChannel) {
        String sysDbPath = "/" + RelationalKeyspaceProvider.SYS;
        String testdb = "/FRL/server_test_db";
        JDBCServiceGrpc.JDBCServiceBlockingStub stub = JDBCServiceGrpc.newBlockingStub(managedChannel);
        try {
            update(stub, sysDbPath, RelationalKeyspaceProvider.CATALOG, "Drop database if exists \"" + testdb + "\"");
            update(stub, sysDbPath, RelationalKeyspaceProvider.CATALOG, "Drop schema template if exists test_template");
            update(stub, sysDbPath, RelationalKeyspaceProvider.CATALOG,
                    "CREATE SCHEMA TEMPLATE test_template " +
                            "CREATE TABLE test_table (rest_no bigint, name string, PRIMARY KEY(rest_no))");
            update(stub, sysDbPath, RelationalKeyspaceProvider.CATALOG, "create database \"" + testdb + "\"");
            update(stub, sysDbPath, RelationalKeyspaceProvider.CATALOG, "create schema \"" + testdb + "/test_schema\" with template test_template");
            ResultSet resultSet = execute(stub, sysDbPath, RelationalKeyspaceProvider.CATALOG, "select * from databases");
            Assertions.assertEquals(2, resultSet.getRowCount());
            Assertions.assertEquals(1, resultSet.getRow(0).getColumns().getColumnCount());
            Assertions.assertEquals(1, resultSet.getRow(1).getColumns().getColumnCount());
            Assertions.assertTrue(resultSet.getRow(0).getColumns().getColumn(0).hasString());
            Assertions.assertTrue(resultSet.getRow(1).getColumns().getColumn(0).hasString());
            Assertions.assertEquals(testdb, resultSet.getRow(0).getColumns().getColumn(0).getString());
            Assertions.assertEquals(sysDbPath, resultSet.getRow(1).getColumns().getColumn(0).getString());
        } catch (Throwable t) {
            com.google.rpc.Status status = StatusProto.fromThrowable(t);
            if (status != null) {
                // V3: printer().shortDebugString(status)
                // V4: printer().emittingSingleLine(true).printToString(status)
                logger.fatal(t + ", " + TextFormat.printer().printToString(status).replace("\n", " "));
            }
            throw t;
        } finally {
            update(stub, sysDbPath, RelationalKeyspaceProvider.CATALOG, "Drop database \"" + testdb + "\"");
        }
    }

    /**
     * Stand up a server and then connect to it with a 'client', run
     * some simple JDBCService invocations, verify basically returns and then
     * shut it all down.
     */
    @Test
    public void simpleJDBCServiceClientOperation() throws IOException, InterruptedException {
        ManagedChannel managedChannel =
                ManagedChannelBuilder.forTarget("localhost:" + relationalServer.getGrpcPort()).usePlaintext().build();
        try {
            simpleJDBCServiceClientOperation(managedChannel);
        } finally {
            managedChannel.shutdownNow();
        }
    }

    /**
     * Check the {@link HealthGrpc} Service is up and working.
     */
    @Test
    public void healthServiceClientOperation() throws IOException, InterruptedException {
        ManagedChannel managedChannel = ManagedChannelBuilder
                .forTarget("localhost:" + relationalServer.getGrpcPort())
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
            logger.info("awaitTermination timedout={}", timedout);
        }
    }

    /**
     * Simple check that the prometheus metrics gathering is working and that we can see the metrics with HTTP client.
     * Prometheus metrics are made for dashboarding and exotic querying, not for easy evalution in unit tests.
     */
    @Test
    public void testMetrics() throws IOException, InterruptedException {
        // Metrics names recorded for grpc -- all we currently record for prometheus -- can be gotten from
        // down the page on https://github.com/grpc-ecosystem/java-grpc-prometheus
        CollectorRegistry collectorRegistry = relationalServer.getCollectorRegistry();
        // In this test, we check that that 'total' for this metric goes up after we make some grpc calls.
        final String metricName = "grpc_server_handled";
        double before = countSampleValues(metricName, metricName + "_total", collectorRegistry);
        // Run some queries which will tickle grpc.
        simpleJDBCServiceClientOperation();
        double after = countSampleValues(metricName, metricName + "_total", collectorRegistry);
        Assertions.assertEquals(after - before, 7.0/* Expected Difference -- 4 calls*/);
        // Streaming is not implemented yet so these should be zero.
        var receivedAfter = findRecordedMetricOrThrow("grpc_server_msg_received", collectorRegistry);
        Assertions.assertEquals(0, receivedAfter.samples.size());
        // Streaming is not implemented yet so these should be zero.
        var sentAfter = findRecordedMetricOrThrow("grpc_server_msg_sent", collectorRegistry);
        Assertions.assertEquals(0, sentAfter.samples.size());

        // Assert I can read prometheus metrics via http client. We just grep it works. Parse is awkward. Can do better
        // when we have more metrics in the mix.
        HttpClient httpClient = HttpClient.newHttpClient();
        HttpRequest httpRequest = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + relationalServer.getHttpPort() + "/metrics")).GET().build();
        final HttpResponse<String> httpResponse = httpClient.send(httpRequest, HttpResponse.BodyHandlers.ofString());
        Assertions.assertEquals(httpResponse.statusCode(), 200);
        logger.info(() -> httpResponse.body());
        Assertions.assertTrue(httpResponse.body().contains("grpc_server_started_created{grpc_type=\"UNARY\"," +
                "grpc_service=\"grpc.relational.jdbc.v1.JDBCService\",grpc_method=\"update\",}"));
        Assertions.assertTrue(httpResponse.body().contains("grpc_server_started_created{grpc_type=\"UNARY\"," +
                "grpc_service=\"grpc.relational.jdbc.v1.JDBCService\",grpc_method=\"execute\",}"));
    }

    // Methods below are from test code of the apache-licensed https://github.com/grpc-ecosystem/java-grpc-prometheus
    // I can't reference the RegistryHelper because it doesn't get built into the java-grpc-prometheus jar (?) --
    // joys of Bazel builder.
    static Collector.MetricFamilySamples findRecordedMetricOrThrow(
            String name, CollectorRegistry collectorRegistry) {
        Optional<Collector.MetricFamilySamples> result = findRecordedMetric(name, collectorRegistry);
        if (!result.isPresent()) {
            throw new IllegalArgumentException("Could not find metric with name: " + name);
        }
        return result.get();
    }

    static Optional<Collector.MetricFamilySamples> findRecordedMetric(
            String name, CollectorRegistry collectorRegistry) {
        Enumeration<Collector.MetricFamilySamples> samples = collectorRegistry.metricFamilySamples();
        while (samples.hasMoreElements()) {
            Collector.MetricFamilySamples sample = samples.nextElement();
            if (sample.name.equals(name)) {
                return Optional.of(sample);
            }
        }
        return Optional.empty();
    }

    /**
     * Count the value for all values of <code>sampleName</code> for given <code>metricName</code>.
     */
    static double countSampleValues(
            String metricName, String sampleName, CollectorRegistry collectorRegistry) {
        Enumeration<Collector.MetricFamilySamples> samples = collectorRegistry.metricFamilySamples();
        double result = 0;
        while (samples.hasMoreElements()) {
            Collector.MetricFamilySamples sample = samples.nextElement();
            if (sample.name.equals(metricName)) {
                for (Collector.MetricFamilySamples.Sample s : sample.samples) {
                    if (s.name.equals(sampleName)) {
                        result += s.value;
                    }
                }
                return result;
            }
        }
        throw new IllegalArgumentException("Could not find sample family with name: " + metricName);
    }

    /**
     * Helper method to find an available port, retrying if BindException occurs.
     * Similar to the logic in ServerTestUtil.createAndStartRelationalServer but for testing purposes.
     */
    @SuppressWarnings("PMD.UnusedLocalVariable")
    static int findAvailablePort(int preferredPort, int maxRetries) throws IOException {
        for (int port = preferredPort; port <= (preferredPort + maxRetries); port += 2) {
            try (ServerSocket socket = new ServerSocket(port)) {
                // Socket creation successful, port is available
                socket.setReuseAddress(true);
                return port;
            } catch (BindException e) {
                // Port is in use, continue to next port
                if (port >= (preferredPort + maxRetries)) {
                    throw new IOException("Could not find available port after " + maxRetries + " attempts starting from " + preferredPort, e);
                }
            }
        }
        throw new IOException("Unexpected error finding available port");
    }

    @Test
    public void testMainWithHelp() throws Exception {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        PrintStream originalOut = System.out;
        try {
            System.setOut(new PrintStream(outputStream));
            RelationalServer.main(new String[]{"-h"});
            String output = outputStream.toString();
            Assertions.assertTrue(output.contains("usage: relational"));
            Assertions.assertTrue(output.contains("help"));
        } finally {
            System.setOut(originalOut);
        }
    }

    @Test
    @Tag(Tags.RequiresFDB)
    public void testMainRunServer() throws Exception {
        int grpcPort = findAvailablePort(3333, 100);
        int httpPort = grpcPort + 1;

        final ArrayList<String> args = new ArrayList<>();
        args.add("-g");
        args.add(String.valueOf(grpcPort));
        args.add("-p");
        args.add(String.valueOf(httpPort));
        String clusterFile = FDBTestEnvironment.randomClusterFile();
        if (clusterFile != null) {
            args.add("-c");
            args.add(clusterFile);
        }
        Thread serverThread = new Thread(() -> {
            try {
                RelationalServer.main(args.toArray(new String[0]));
            } catch (RecordCoreInterruptedException e) {
                // Expected after we've tested the port
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        serverThread.setDaemon(true);
        serverThread.start();

        // Give the server more time to start
        Thread.sleep(1000);

        // Verify the server is running on the specified port
        ManagedChannel managedChannel = ManagedChannelBuilder
                .forTarget("localhost:" + grpcPort)
                .usePlaintext()
                .build();
        try {
            HealthGrpc.HealthBlockingStub stub = HealthGrpc.newBlockingStub(managedChannel);
            HealthCheckResponse response = stub.check(HealthCheckRequest.newBuilder().build());
            Assertions.assertEquals(HealthCheckResponse.ServingStatus.SERVING, response.getStatus());
        } finally {
            managedChannel.shutdownNow();
            serverThread.interrupt();
        }
    }

}
