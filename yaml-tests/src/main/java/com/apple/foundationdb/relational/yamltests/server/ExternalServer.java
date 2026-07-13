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
import com.apple.foundationdb.relational.util.BuildVersion;
import com.apple.foundationdb.relational.yamltests.connectionfactory.Clusters;
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
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.jar.Attributes;
import java.util.jar.JarFile;
import java.util.jar.Manifest;

/**
 * Class to manage running an external server.
 */
public class ExternalServer implements Clusters.BoundToCluster {

    private static final Logger logger = LogManager.getLogger(ExternalServer.class);
    public static final String EXTERNAL_SERVER_PROPERTY_NAME = "yaml_testing_external_server";

    /**
     * JVM-wide set of ports currently reserved by any {@link ExternalServer} instance. Closes the race
     * where two concurrent {@link #startMultiple(Collection)} calls (e.g. from parallel test classes'
     * {@code @BeforeAll}) each see the same port as available — this would otherwise let two test
     * classes both try to bind the same port (e.g. 1111). The atomic {@link Set#add(Object)} on a
     * {@link ConcurrentHashMap#newKeySet()} acts as a CAS: the winner returns {@code true} and owns
     * the port until {@link #stop()} releases it.
     */
    private static final Set<Integer> JVM_WIDE_CLAIMED_PORTS = ConcurrentHashMap.newKeySet();

    @Nonnull
    private final File serverJar;
    private int grpcPort;
    private int httpPort;
    private final SemanticVersion version;
    private Process serverProcess;
    @Nonnull
    private final String clusterFile;

    /**
     * Create a new instance that will run a specific jar.
     *
     * @param serverJar the path to the jar to run
     */
    public ExternalServer(@Nonnull final File serverJar,
                          @Nonnull final String clusterFile) throws IOException {
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

    @Nonnull
    @Override
    public String clusterFile() {
        return clusterFile;
    }

    public void start() throws Exception {
        grpcPort = getAvailablePort();
        boolean success = false;
        try {
            httpPort = getAvailablePort();
            ProcessBuilder processBuilder = new ProcessBuilder("java",
                    // TODO add support for debugging by adding, but need to take care with ports
                    // "-agentlib:jdwp=transport=dt_socket,server=y,address=8000,suspend=n",
                    "-jar", serverJar.getAbsolutePath(),
                    "--grpcPort", Integer.toString(grpcPort), "--httpPort", Integer.toString(httpPort));
            boolean saveServerLogs = Boolean.parseBoolean(System.getProperty("tests.saveServerLogs", "false"));
            // Always capture stderr to a tempfile so we can include its tail in the failure message
            // if the subprocess dies or never becomes available. Stdout is only retained when the
            // tests.saveServerLogs sysProp is set, since it can be voluminous in the happy path.
            // Include current time in file names so they sort nicely.
            long currentTime = System.currentTimeMillis();
            @Nullable
            File outFile;
            if (saveServerLogs) {
                outFile = File.createTempFile("fdb-relational-server-" + version + "-" + currentTime + "-" + grpcPort + "-out.", ".log");
            } else {
                outFile = null;
            }
            final File errFile = File.createTempFile("fdb-relational-server-" + version + "-" + currentTime + "-" + grpcPort + "-err.", ".log");
            // Don't keep the diagnostic file around in the success path (clutters tmp on dev boxes).
            errFile.deleteOnExit();
            ProcessBuilder.Redirect out = outFile == null ? ProcessBuilder.Redirect.DISCARD : ProcessBuilder.Redirect.to(outFile);
            processBuilder.redirectOutput(out);
            processBuilder.redirectError(ProcessBuilder.Redirect.to(errFile));
            if (clusterFile != null) {
                processBuilder.environment().put("FDB_CLUSTER_FILE", clusterFile);
            }

            if (!startServer(processBuilder)) {
                final String errTail = tailFile(errFile, 4096);
                if (logger.isWarnEnabled()) {
                    logger.warn(KeyValueLogMessage.of("Failed to start external server",
                            "jar", serverJar,
                            LogMessageKeys.VERSION, version,
                            "grpc_port", grpcPort,
                            "http_port", httpPort,
                            "out_file", outFile,
                            "err_file", errFile,
                            "subprocess_alive", serverProcess != null && serverProcess.isAlive(),
                            "exit_value", serverProcess != null && !serverProcess.isAlive() ? serverProcess.exitValue() : "n/a"));
                }
                Assertions.fail("Failed to start the external server (grpc_port=" + grpcPort
                        + ", http_port=" + httpPort
                        + ", alive=" + (serverProcess != null && serverProcess.isAlive())
                        + (serverProcess != null && !serverProcess.isAlive() ? ", exit=" + serverProcess.exitValue() : "")
                        + ")\n--- last bytes of subprocess stderr (" + errFile + ") ---\n"
                        + errTail
                        + "\n--- end ---");
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
            success = true;
        } finally {
            // If we claimed ports but the server failed to come up (Assertions.fail, IOException
            // from process.start, etc.), release the JVM-wide claims so the ports become available
            // to future allocators. stop() will not be called for a failed start, so this is the
            // only cleanup hook.
            if (!success) {
                JVM_WIDE_CLAIMED_PORTS.remove(grpcPort);
                JVM_WIDE_CLAIMED_PORTS.remove(httpPort);
            }
        }
    }

    /** Read the trailing {@code maxBytes} of {@code file} as a UTF-8 string, never throwing. */
    @Nonnull
    private static String tailFile(@Nonnull File file, int maxBytes) {
        try {
            final long size = file.length();
            try (java.io.RandomAccessFile raf = new java.io.RandomAccessFile(file, "r")) {
                final int read = (int) Math.min(maxBytes, size);
                raf.seek(size - read);
                final byte[] buf = new byte[read];
                raf.readFully(buf);
                return (size > read ? "…(truncated)…\n" : "") + new String(buf, java.nio.charset.StandardCharsets.UTF_8);
            }
        } catch (IOException e) {
            return "(failed to read " + file + ": " + e + ")";
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
        // Time-based budget rather than a fixed retry count: under JVM-wide contention from a
        // parallel test run (e.g. many test classes each spawning their own external servers,
        // YamlIntegrationTests starts ~5) the subprocess can take well over 10s to finish JVM
        // warmup, FDB connect, and bind its gRPC port. In isolation the same start completes in
        // ~3s, so the larger ceiling only kicks in on contended runs.
        final long deadlineMillis = System.currentTimeMillis() + 30_000L;
        int attempts = 0;
        boolean started = false;
        while (!started) {
            // If the subprocess has died, no amount of retrying will help — bail immediately so
            // the test fails quickly with an actionable signal instead of consuming the full budget.
            if (serverProcess != null && !serverProcess.isAlive()) {
                if (logger.isWarnEnabled()) {
                    logger.warn(KeyValueLogMessage.of("External server subprocess exited before becoming available",
                            LogMessageKeys.VERSION, version,
                            "grpc_port", getPort(),
                            "exit_value", serverProcess.exitValue(),
                            "attempts", attempts));
                }
                return false;
            }
            if (System.currentTimeMillis() >= deadlineMillis) {
                return false;
            }
            // Linear backoff capped at 1s so we don't pace much beyond steady state.
            final long delay = Math.min(50L * attempts, 1000L);
            if (delay > 0) {
                Thread.sleep(delay);
            }
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
     * Get a port that is currently available for the server and atomically claim it in
     * {@link #JVM_WIDE_CLAIMED_PORTS} so no other {@link ExternalServer} instance — in this batch
     * or any concurrent batch in another test class — will pick it. The claim is released by
     * {@link #stop()} or by {@link #start()} on a failed-startup path.
     * @return a port that is not currently in use on the system and not claimed by another ExternalServer.
     */
    private int getAvailablePort() {
        // running locally on my laptop, testing if a port is available takes 0 milliseconds, so no need to optimize
        for (int i = 1111; i < 9999; i++) {
            // Atomically claim across the whole JVM. If another ExternalServer already grabbed this
            // port, add() returns false and we keep scanning.
            if (!JVM_WIDE_CLAIMED_PORTS.add(i)) {
                continue;
            }
            if (isAvailable(i)) {
                return i;
            }
            // OS-level check failed (port used by something outside the JVM). Release the claim so
            // a later allocator can reconsider it once whatever is occupying it goes away.
            JVM_WIDE_CLAIMED_PORTS.remove(i);
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
        try {
            if ((serverProcess != null) && serverProcess.isAlive()) {
                serverProcess.destroy();
            }
        } finally {
            // Release JVM-wide port claims unconditionally so the ports become available to other
            // ExternalServer instances even if the subprocess already exited on its own. The default
            // int value (0) is never added to JVM_WIDE_CLAIMED_PORTS, so calling stop() before
            // start() is harmless.
            JVM_WIDE_CLAIMED_PORTS.remove(grpcPort);
            JVM_WIDE_CLAIMED_PORTS.remove(httpPort);
        }
    }

    public static void startMultiple(@Nonnull Collection<ExternalServer> servers) throws Exception {
        // Port uniqueness within and across batches is guaranteed by JVM_WIDE_CLAIMED_PORTS — each
        // server's start() atomically claims its grpc and http ports before binding.
        for (ExternalServer server : servers) {
            server.start();
        }
    }
}
