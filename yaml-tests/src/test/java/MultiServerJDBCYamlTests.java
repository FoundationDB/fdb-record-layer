/*
 * MultiServerJDBCYamlTests.java
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

import com.apple.foundationdb.relational.api.RelationalConnection;
import com.apple.foundationdb.relational.yamltests.MultiServerConnectionFactory;
import com.apple.foundationdb.relational.yamltests.YamlRunner;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Nested;

import java.io.File;
import java.io.IOException;
import java.sql.DriverManager;
import java.util.List;
import java.util.Objects;

/**
 * A test runner to launch the YAML tests with multiple servers.
 * The tests will run against two servers: One JDBC embedded and the other launched from a jar, that represents another version
 * of the system. The tests that will run will leverage connections pointing at both servers and will direct requests at
 * either server according to the selection policy.
 */
public abstract class MultiServerJDBCYamlTests extends JDBCInProcessYamlIntegrationTests {

    private static final Logger LOG = LogManager.getLogger(JDBCExternalYamlIntegrationTests.class);
    public static final String EXTERNAL_SERVER_PROPERTY_NAME = "yaml_testing_external_server";
    public static final String SERVER_PORT = "1111";

    private static Process serverProcess;

    private final int initialConnection;

    protected MultiServerJDBCYamlTests(int initialConnection) {
        this.initialConnection = initialConnection;
    }

    /**
     * Concrete implementation of the test class that uses embedded server as the initial connection.
     */
    @Nested
    public static class MultiServerInitialConnectionEmbeddedTests extends MultiServerJDBCYamlTests {
        public MultiServerInitialConnectionEmbeddedTests() {
            super(0);
        }
    }

    /**
     * Concrete implementation of the test class that uses external server as the initial connection.
     */
    @Nested
    public static class MultiServerInitialConnectionExternalTests extends MultiServerJDBCYamlTests {
        public MultiServerInitialConnectionExternalTests() {
            super(1);
        }
    }

    @BeforeAll
    public static void startServer() throws IOException, InterruptedException {
        Assumptions.abort(); // Will be able to re-enable when we have a published external server to use here
        final File externalDirectory = new File(Objects.requireNonNull(System.getProperty(EXTERNAL_SERVER_PROPERTY_NAME)));
        final File[] externalServers = externalDirectory.listFiles(file -> file.getName().endsWith(".jar"));
        Assertions.assertEquals(1, externalServers.length);
        File jar = externalServers[0];
        LOG.info("Starting " + jar);
        ProcessBuilder processBuilder = new ProcessBuilder("java", "-jar", jar.getAbsolutePath());
        processBuilder.redirectOutput(ProcessBuilder.Redirect.INHERIT);
        processBuilder.redirectError(ProcessBuilder.Redirect.INHERIT);

        serverProcess = processBuilder.start();
        // TODO: There should be a better way to figure out that the server is fully up and  running
        Thread.sleep(3000);
        if ((serverProcess == null) || !serverProcess.isAlive()) {
            Assertions.fail("Failed to start the external server");
        }
    }

    @Override
    YamlRunner.YamlConnectionFactory createConnectionFactory() {
        return new MultiServerConnectionFactory(
                MultiServerConnectionFactory.ConnectionSelectionPolicy.ALTERNATE,
                initialConnection,
                super.createConnectionFactory(),
                List.of(createExternalServerConnection()));
    }

    YamlRunner.YamlConnectionFactory createExternalServerConnection() {
        return connectPath -> {
            String uriStr = connectPath.toString().replaceFirst("embed:", "relational://localhost:" + SERVER_PORT);
            return DriverManager.getConnection(uriStr).unwrap(RelationalConnection.class);
        };
    }

    @AfterAll
    public static void shutdownServer() {
        if ((serverProcess != null) && serverProcess.isAlive()) {
            serverProcess.destroy();
        }
    }

    @Override
    @Disabled("Test asserts about quantifiers")
    public void updateDeleteReturning() throws Exception {
        super.updateDeleteReturning();
    }

    @Override
    @Disabled("Test asserts about quantifiers")
    public void aggregateIndexTests() throws Exception {
        super.aggregateIndexTests();
    }

    @Override
    @Disabled("Test asserts about quantifiers")
    public void indexedFunctions() throws Exception {
        super.indexedFunctions();
    }

    @Override
    @Disabled("Test asserts about quantifiers")
    public void bitmap() throws Exception {
        super.bitmap();
    }

    @Override
    @Disabled("Test asserts about quantifiers")
    public void cte() throws Exception {
        super.cte();
    }

    @Override
    @Disabled("Test asserts about quantifiers")
    public void unionEmptyTables() throws Exception {
        super.unionEmptyTables();
    }

    @Override
    @Disabled("Test asserts about quantifiers")
    public void union() throws Exception {
        super.union();
    }
}
