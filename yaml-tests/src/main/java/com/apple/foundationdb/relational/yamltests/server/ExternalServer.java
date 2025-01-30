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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Assertions;

import java.io.File;
import java.io.IOException;
import java.util.Objects;
import java.util.jar.Attributes;
import java.util.jar.JarFile;
import java.util.jar.Manifest;

/**
 * Class to manage running an external server.
 */
public class ExternalServer {

    private static final Logger logger = LogManager.getLogger(ExternalServer.class);
    public static final String EXTERNAL_SERVER_PROPERTY_NAME = "yaml_testing_external_server";

    private static final int SERVER_PORT = 1111;
    private final String jarName;
    private String version;
    private Process serverProcess;

    /**
     * Create a new extension that will run latest released version of the server, as downloaded by gradle.
     */
    public ExternalServer() {
        this(null);
    }

    /**
     * Create a new extension that will run a specific jar.
     * @param jarName the path to the jar to run
     */
    public ExternalServer(String jarName) {
        this.jarName = jarName;
    }

    /**
     * Get the port to use when connecting.
     * @return the grpc port that the server is listening to
     */
    public int getPort() {
        return SERVER_PORT;
    }

    /**
     * Get the version of the server.
     * @return the version of the server being run.
     */
    public String getVersion() {
        return version;
    }

    public void start() throws Exception {
        File jar;
        if (jarName == null) {
            final File externalDirectory = new File(Objects.requireNonNull(System.getProperty(EXTERNAL_SERVER_PROPERTY_NAME)));
            final File[] externalServers = Objects.requireNonNull(externalDirectory.listFiles(file -> file.getName().endsWith(".jar")));
            Assertions.assertEquals(1, externalServers.length);
            jar = externalServers[0];
        } else {
            jar = new File(jarName);
        }
        Assertions.assertTrue(jar.exists(), "Jar could not be found " + jar.getAbsolutePath());
        ProcessBuilder processBuilder = new ProcessBuilder("java",
                "-jar", "-agentlib:jdwp=transport=dt_socket,server=y,address=8000,suspend=n", jar.getAbsolutePath());
        processBuilder.redirectOutput(ProcessBuilder.Redirect.INHERIT);
        processBuilder.redirectError(ProcessBuilder.Redirect.INHERIT);

        if (!startServer(processBuilder)) {
            Assertions.fail("Failed to start the external server");
        }

        this.version = getVersion(jar);
        logger.info("Started {} Version: {}", jar, version);
    }

    private static String getVersion(File jar) throws IOException {
        try (JarFile jarFile = new JarFile(jar)) {
            final Manifest manifest = jarFile.getManifest();
            final Attributes mainAttributes = manifest.getMainAttributes();
            String version = mainAttributes.getValue("Specification-Version");
            if (version != null) {
                return version;
            } else {
                return Assertions.fail("Server does not specify a version in the manifest: " + jar.getAbsolutePath());
            }
        }
    }

    private boolean startServer(ProcessBuilder processBuilder) throws IOException, InterruptedException {
        try {
            serverProcess = processBuilder.start();
            // TODO: There should be a better way to figure out that the server is fully up and  running
            Thread.sleep(3000);
            if (!serverProcess.isAlive()) {
                throw new Exception("Failed to start server once - retrying");
            }
            return true;
        } catch (Exception ex) {
            // Try once more
            serverProcess = processBuilder.start();
            // TODO: There should be a better way to figure out that the server is fully up and  running
            Thread.sleep(3000);
        }

        return serverProcess.isAlive();
    }

    public void stop() {
        if ((serverProcess != null) && serverProcess.isAlive()) {
            serverProcess.destroy();
        }
    }

}
