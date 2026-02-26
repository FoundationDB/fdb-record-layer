/*
 * ExternalServer.java
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

package com.apple.foundationdb.relational.yamltests.server;

import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.util.Assert;
import com.apple.foundationdb.relational.util.BuildVersion;
import com.google.common.base.Verify;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Assertions;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.jar.Attributes;
import java.util.jar.JarFile;
import java.util.jar.Manifest;

/**
 * Class to manage running an external server.
 */
public class ExternalServer {

    private static final Logger logger = LogManager.getLogger(ExternalServer.class);
    public static final String EXTERNAL_SERVER_PROPERTY_NAME = "yaml_testing_external_server";

    @Nonnull
    private final File serverJar;
    private int grpcPort;
    private int httpPort;
    private final SemanticVersion version;
    private Process serverProcess;
    @Nullable
    private final String clusterFile;

    /**
     * Create a new instance that will run a specific jar.
     *
     * @param serverJar the path to the jar to run
     */
    public ExternalServer(@Nonnull final File serverJar,
                          @Nullable final String clusterFile) throws IOException {
        this.clusterFile = clusterFile;

        this.serverJar = serverJar;
        Assertions.assertTrue(this.serverJar.exists(), "Jar could not be found " + serverJar.getAbsolutePath());
        this.version = getVersion(this.serverJar);
    }

    static {
        final String serverPath = Objects.requireNonNull(System.getProperty(EXTERNAL_SERVER_PROPERTY_NAME));
        // kill all existing processes
        ProcessHandle.allProcesses().filter(process ->
                process.info().arguments().map(arguments ->
                                Arrays.stream(arguments).anyMatch(argument ->
                                        argument.startsWith(serverPath)))
                        .orElse(false) &&
                        process.info().command().map(command ->
                                        command.endsWith("/java"))
                                .orElse(false))
                .forEach(process -> {
                    if (logger.isInfoEnabled()) {
                        logger.info("Killing existing server: pid=" + process.pid() + " " + process.info());
                    }
                    process.destroy();
                });
    }

    /**
     * Get the port to use when connecting.
     *
     * @return the grpc port that the server is listening to
     */
    public int getPort() {
        return grpcPort;
    }

    /**
     * The port to use to connect via HTTP. Used as an alternative to the gRPC API.
     *
     * @return the http port that the server is listening to
     */
    public int getHttpPort() {
        return httpPort;
    }

    /**
     * Get the version of the server.
     *
     * @return the version of the server being run.
     */
    public SemanticVersion getVersion() {
        return version;
    }

    public String getClusterFile() {
        return clusterFile;
    }

    public void start(Set<Integer> unavailablePorts) throws Exception {
        grpcPort = getAvailablePort(unavailablePorts);
        httpPort = getAvailablePort(unavailablePorts);
        ProcessBuilder processBuilder = new ProcessBuilder("java",
                // TODO add support for debugging by adding, but need to take care with ports
                // "-agentlib:jdwp=transport=dt_socket,server=y,address=8000,suspend=n",
                "-jar", serverJar.getAbsolutePath(),
                "--grpcPort", Integer.toString(grpcPort), "--httpPort", Integer.toString(httpPort));
        boolean saveServerLogs = Boolean.parseBoolean(System.getProperty("tests.saveServerLogs", "false"));
        @Nullable
        File outFile;
        @Nullable
        File errFile;
        if (saveServerLogs) {
            // Include current time in file names so that things sort nicely. We want the out and err logs
            // for the same server start to be adjacent, and it would be nice for the log files to sort
            // chronologically
            long currentTime = System.currentTimeMillis();
            outFile = File.createTempFile("fdb-relational-server-" + version + "-" + currentTime + "-" + grpcPort + "-out.", ".log");
            errFile = File.createTempFile("fdb-relational-server-" + version + "-" + currentTime + "-" + grpcPort + "-err.", ".log");
        } else {
            outFile = null;
            errFile = null;
        }
        ProcessBuilder.Redirect out = outFile == null ? ProcessBuilder.Redirect.DISCARD : ProcessBuilder.Redirect.to(outFile);
        ProcessBuilder.Redirect err = errFile == null ? ProcessBuilder.Redirect.DISCARD : ProcessBuilder.Redirect.to(errFile);
        processBuilder.redirectOutput(out);
        processBuilder.redirectError(err);
        if (clusterFile != null) {
            processBuilder.environment().put("FDB_CLUSTER_FILE", clusterFile);
        }

        if (!startServer(processBuilder)) {
            if (logger.isWarnEnabled()) {
                logger.warn(KeyValueLogMessage.of("Failed to start external server",
                        "jar", serverJar,
                        LogMessageKeys.VERSION, version,
                        "grpc_port", grpcPort,
                        "http_port", httpPort,
                        "out_file", outFile,
                        "err_file", errFile));
            }
            Assertions.fail("Failed to start the external server");
        }

        if (logger.isInfoEnabled()) {
            logger.info(KeyValueLogMessage.of("Started external server",
                    "jar", serverJar,
                    LogMessageKeys.VERSION, version,
                    "grpc_port", grpcPort,
                    "http_port", httpPort,
                    "out_file", outFile,
                    "err_file", errFile
            ));
        }
    }

    /**
     * Get a list of available servers in the download folder.
     *
     * @return a list of jar {@link File}s available to run
     */
    public static List<File> getAvailableServers() {
        final File externalDirectory = new File(Objects.requireNonNull(System.getProperty(EXTERNAL_SERVER_PROPERTY_NAME)));
        final File[] externalServers = Objects.requireNonNull(externalDirectory.listFiles(file -> file.getName().endsWith(".jar")));
        return List.of(externalServers);
    }

    private static SemanticVersion getVersion(File jar) throws IOException {
        try (JarFile jarFile = new JarFile(jar)) {
            final Manifest manifest = jarFile.getManifest();
            final Attributes mainAttributes = manifest.getMainAttributes();
            String version = mainAttributes.getValue("Specification-Version");
            if (version != null) {
                if (version.equals(BuildVersion.getInstance().getVersion())) {
                    // One of the external servers is locally built. In order for conditional execution
                    // in the test assertions to be executed correctly, it needs to be registered
                    // as such.
                    //
                    // Ideally, it would be nice if the two versions aligned, potentially by having
                    // SemanticVersion.current() return a version based on the BuildVersion.
                    // See: https://github.com/FoundationDB/fdb-record-layer/issues/3208 for more details
                    return SemanticVersion.current();
                } else {
                    return SemanticVersion.parse(version);
                }
            } else {
                return Assertions.fail("Server does not specify a version in the manifest: " + jar.getAbsolutePath());
            }
        }
    }

    private boolean startServer(ProcessBuilder processBuilder) throws IOException, SQLException, InterruptedException {
        serverProcess = processBuilder.start();
        return attemptConnectionWithRetry();
    }

    private boolean attemptConnectionWithRetry() throws SQLException, InterruptedException {
        final int maxAttempts = 20;
        boolean started = false;
        int attempts = 0;
        while (!started && attempts < maxAttempts) {
            long delay = 50L * attempts;
            Thread.sleep(delay);
            started = attemptConnection();
            attempts++;
            if (logger.isDebugEnabled()) {
                logger.debug(KeyValueLogMessage.of("Attempted to connect to external server",
                        LogMessageKeys.VERSION, version,
                        "grpc_port", getPort(),
                        "attempt", attempts,
                        "success", started));
            }
        }
        return started;
    }

    private boolean attemptConnection() throws SQLException {
        try (Connection connection = DriverManager.getConnection("jdbc:relational://localhost:" + getPort() + "/__SYS?schema=CATALOG")) {
            validateConnectionVersion(connection);
            return true;
        } catch (RuntimeException e) {
            if (e.getMessage().contains("UNAVAILABLE")) {
                // Error returned when the server hasn't started yet. Currently, this comes directly from gRPC, though
                // potentially we should be wrapping it with some kind of SQLException
                return false;
            }
            throw  e;
        }
    }

    public void validateConnectionVersion(@Nonnull Connection connection) throws SQLException {
        // Validate that the server has the expected version. Connect and make a request to the meta-data API,
        // and validate that the database product version matches the external server's version
        final DatabaseMetaData metaData = connection.getMetaData();
        final String expectedVersion = getVersion().equals(SemanticVersion.current()) ? BuildVersion.getInstance().getVersion() : getVersion().toString();
        Verify.verify(metaData.getDatabaseProductVersion().equals(expectedVersion),
                "external server expected version %s but had version %s", expectedVersion, metaData.getDatabaseProductVersion());
    }

    /**
     * Get a port that is currently available for the server.
     * @param unavailablePorts a set of ports that are known to be unavailable. This is useful for two reasons:
     * (1) when starting multiple servers, we include all previously allocated ports to avoid starting multiple
     * servers on the same ports, and (2) each server needs two ports, one for GRPC, and one for HTTP, so the
     * GRPC port can be noted as unavailable when asking for the http port. If nothing is unavailable, use an empty set.
     * The provided set must be mutable, as it will be updated during run as ports are allocated
     * @return a port that is not currently in use on the system.
     */
    private int getAvailablePort(@Nonnull final Set<Integer> unavailablePorts) {
        // running locally on my laptop, testing if a port is available takes 0 milliseconds, so no need to optimize
        for (int i = 1111; i < 9999; i++) {
            // Add the port immediately to the set of unavailable ports. We do this because there are
            // three possibilities: (1) it has already been allocated and so add returns false, (2) it
            // is determined to be unavailable by isAvailable, or (3) we allocate it to this server.
            // In any case, we don't want to consider this port again. Checking the set before calling
            // isAvailable() also ensures that we never need to call isAvailable() more than once for any
            // port
            if (unavailablePorts.add(i) && isAvailable(i)) {
                return i;
            }
        }
        return Assertions.fail("Could not find available port between 1111 and 9999");
    }

    public static boolean isAvailable(int port) {
        try (ServerSocket tcpSocket = new ServerSocket(port)) {
            tcpSocket.setReuseAddress(true);
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    public void stop() {
        if ((serverProcess != null) && serverProcess.isAlive()) {
            serverProcess.destroy();
        }
    }

    public static void startMultiple(@Nonnull Collection<ExternalServer> servers) throws Exception {
        startMultiple(servers, new HashSet<>());
    }

    public static void startMultiple(@Nonnull Collection<ExternalServer> servers, @Nonnull Set<Integer> unavailablePorts) throws Exception {
        final Map<Integer, ExternalServer> allocatedPorts = new HashMap<>();
        for (ExternalServer server : servers) {
            server.start(unavailablePorts);
            // Threading through unavailablePorts should be enough to make sure each port is unique.
            // Double check both ports, though
            checkPortIsUnique(server.getPort(), server, allocatedPorts);
            checkPortIsUnique(server.getHttpPort(), server, allocatedPorts);
        }
    }

    private static void checkPortIsUnique(int port, @Nonnull ExternalServer server, @Nonnull Map<Integer, ExternalServer> allocatedPorts) throws RelationalException {
        @Nullable ExternalServer preExistingServer = allocatedPorts.putIfAbsent(port, server);
        if (preExistingServer != null) {
            Assert.fail("allocated duplicate port (" + server.getPort() + ") to servers for versions " + server.getVersion() + " and " + preExistingServer.getVersion());
        }
    }
}
