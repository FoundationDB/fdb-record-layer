/*
 * MultiServerJDBCYamlTests.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2025 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.relational.yamltests.YamlExecutionContext;
import com.apple.foundationdb.relational.yamltests.YamlRunner;
import com.apple.foundationdb.relational.yamltests.server.RunExternalServerExtension;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.extension.RegisterExtension;

import javax.annotation.Nonnull;
import java.net.URI;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A test runner to launch the YAML tests with multiple servers.
 * The tests will run against two servers: One JDBC embedded and the other launched from a jar, that represents another version
 * of the system. The tests that will run will leverage connections pointing at both servers and will direct requests at
 * either server according to the selection policy.
 */
public abstract class MultiServerJDBCYamlTests extends JDBCInProcessYamlIntegrationTests {

    @RegisterExtension
    static RunExternalServerExtension externalServer = new RunExternalServerExtension();

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

    @Override
    YamlRunner.YamlConnectionFactory createConnectionFactory() {
        return new MultiServerConnectionFactory(
                MultiServerConnectionFactory.ConnectionSelectionPolicy.ALTERNATE,
                initialConnection,
                super.createConnectionFactory(),
                List.of(createExternalServerConnection()));
    }

    YamlRunner.YamlConnectionFactory createExternalServerConnection() {
        return new YamlRunner.YamlConnectionFactory() {
            @Override
            public RelationalConnection getNewConnection(@Nonnull URI connectPath) throws SQLException {
                String uriStr = connectPath.toString().replaceFirst("embed:", "relational://localhost:" + externalServer.getPort());
                return DriverManager.getConnection(uriStr).unwrap(RelationalConnection.class);
            }

            @Override
            public Set<String> getVersionsUnderTest() {
                return Set.of(externalServer.getVersion());
            }
        };
    }
}
