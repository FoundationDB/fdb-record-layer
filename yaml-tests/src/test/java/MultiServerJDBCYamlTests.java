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
import org.junit.jupiter.api.Disabled;
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

    /**
     * Concrete implementation of the test class that forces continuationa on every query.
     */
    @Nested
    public static class MultiServerForceContinuationsTests extends MultiServerJDBCYamlTests {
        public MultiServerForceContinuationsTests() {
            super(1);
        }

        @Override
        public Map<String, Object> getAdditionalOptions() {
            return Map.of(YamlExecutionContext.OPTION_FORCE_CONTINUATIONS, true);
        }

        @Override
        @Disabled("Infinite continuation loop (https://github.com/FoundationDB/fdb-record-layer/issues/3095)")
        public void aggregateEmptyTable() throws Exception {
            super.aggregateEmptyTable();
        }

        @Override
        @Disabled("Continuation verification (https://github.com/FoundationDB/fdb-record-layer/issues/3096)")
        public void aggregateIndexTests() throws Exception {
            super.aggregateIndexTests();
        }

        @Override
        @Disabled("Infinite continuation loop (https://github.com/FoundationDB/fdb-record-layer/issues/3095)")
        public void aggregateIndexTestsCount() throws Exception {
            super.aggregateIndexTestsCount();
        }

        @Override
        @Disabled("Infinite continuation loop (https://github.com/FoundationDB/fdb-record-layer/issues/3095)")
        public void aggregateIndexTestsCountEmpty() throws Exception {
            super.aggregateIndexTestsCountEmpty();
        }

        @Override
        @Disabled("Continuation error (https://github.com/FoundationDB/fdb-record-layer/issues/3097)")
        public void bitmap() throws Exception {
            super.bitmap();
        }

        @Override
        @Disabled("Infinite continuation (https://github.com/FoundationDB/fdb-record-layer/issues/3095)")
        void booleanTypes() throws Exception {
            super.booleanTypes();
        }

        @Override
        @Disabled("Continuation verification (https://github.com/FoundationDB/fdb-record-layer/issues/3096)")
        void catalog() throws Exception {
            super.catalog();
        }

        @Override
        @Disabled("Continuation verification (https://github.com/FoundationDB/fdb-record-layer/issues/3096)")
        void createDrop() throws Exception {
            super.createDrop();
        }

        @Override
        @Disabled("Continuation verification (https://github.com/FoundationDB/fdb-record-layer/issues/3096)")
        public void fieldIndexTestsProto() throws Exception {
            super.fieldIndexTestsProto();
        }

        @Override
        @Disabled("Continuation mismatch (https://github.com/FoundationDB/fdb-record-layer/issues/3098)")
        void functions() throws Exception {
            super.functions();
        }

        @Override
        @Disabled("Infinite continuation loop (https://github.com/FoundationDB/fdb-record-layer/issues/3095)")
        public void groupByTests() throws Exception {
            super.groupByTests();
        }

        @Override
        @Disabled("Infinite continuation loop (https://github.com/FoundationDB/fdb-record-layer/issues/3095)")
        public void joinTests() throws Exception {
            super.joinTests();
        }

        @Override
        @Disabled("Like continuation failure (https://github.com/FoundationDB/fdb-record-layer/issues/3099)")
        void like() throws Exception {
            super.like();
        }

        @Override
        @Disabled("continuation verification (https://github.com/FoundationDB/fdb-record-layer/issues/3096)")
        public void nullOperator() throws Exception {
            super.nullOperator();
        }

        @Override
        @Disabled("Infinite loop (https://github.com/FoundationDB/fdb-record-layer/issues/3095)")
        public void primaryKey() throws Exception {
            super.primaryKey();
        }

        @Override
        @Disabled("maxRows ignored (https://github.com/FoundationDB/fdb-record-layer/issues/3100)")
        public void recursiveCte() throws Exception {
            super.recursiveCte();
        }

        @Override
        @Disabled("Infinite continuation loop (https://github.com/FoundationDB/fdb-record-layer/issues/3095)")
        public void selectAStar() throws Exception {
            super.selectAStar();
        }

        @Override
        @Disabled("continuation verification (https://github.com/FoundationDB/fdb-record-layer/issues/3096)")
        public void standardTests() throws Exception {
            super.standardTests();
        }

        @Override
        @Disabled("continuation verification (https://github.com/FoundationDB/fdb-record-layer/issues/3096)")
        public void standardTestsWithProto() throws Exception {
            super.standardTestsWithProto();
        }

        @Override
        @Disabled("Continuation verification (https://github.com/FoundationDB/fdb-record-layer/issues/3096)")
        public void standardTestsWithMetaData() throws Exception {
            super.standardTestsWithMetaData();
        }

        @Override
        @Disabled("Infinite continuation loop (https://github.com/FoundationDB/fdb-record-layer/issues/3095)")
        public void subqueryTests() throws Exception {
            super.subqueryTests();
        }

        @Override
        @Disabled("Infinite continuation loop (https://github.com/FoundationDB/fdb-record-layer/issues/3095)")
        public void unionEmptyTables() throws Exception {
            super.unionEmptyTables();
        }

        @Override
        @Disabled("Infinite continuation loop (https://github.com/FoundationDB/fdb-record-layer/issues/3095)")
        public void union() throws Exception {
            super.union();
        }

        @Override
        @Disabled("maxRows ignored on update (https://github.com/FoundationDB/fdb-record-layer/issues/3100)")
        public void updateDeleteReturning() throws Exception {
            super.updateDeleteReturning();
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
