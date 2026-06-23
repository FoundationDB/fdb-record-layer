/*
 * CurrentVersionTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2026 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.relational.yamltests.SimpleYamlConnection;
import com.apple.foundationdb.relational.yamltests.YamlConnection;
import com.apple.foundationdb.relational.yamltests.YamlConnectionFactory;
import com.apple.foundationdb.relational.yamltests.YamlExecutionContext;
import com.apple.foundationdb.relational.yamltests.YamlRunner;
import com.apple.foundationdb.relational.yamltests.configs.EmbeddedConfig;
import com.apple.foundationdb.relational.yamltests.server.SemanticVersion;
import com.apple.foundationdb.test.FDBTestEnvironment;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nonnull;
import java.net.URI;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests that tests with setup blocks based on the current version are executed correctly.
 */
class CurrentVersionTest {
    private static final SemanticVersion VERSION = SemanticVersion.parse("3.0.18.0");
    private static final String CLUSTER_FILE = FDBTestEnvironment.randomClusterFile();
    private static final EmbeddedConfig config = new EmbeddedConfig(List.of(CLUSTER_FILE));

    @BeforeAll
    static void beforeAll() throws Exception {
        config.beforeAll();
    }

    @AfterAll
    static void afterAll() throws Exception {
        config.afterAll();
    }

    private void doRun(String testName) throws Exception {
        new YamlRunner("current-version/" + testName + ".yamsql", createConnectionFactory(), YamlExecutionContext.ContextOptions.EMPTY_OPTIONS).run();
    }

    YamlConnectionFactory createConnectionFactory() {
        return new YamlConnectionFactory() {
            @Override
            public YamlConnection getNewConnection(@Nonnull URI connectPath, int clusterIndex) throws SQLException {
                return new SimpleYamlConnection(DriverManager.getConnection(connectPath.toString()), VERSION, CLUSTER_FILE);
            }

            @Override
            public Set<SemanticVersion> getVersionsUnderTest() {
                return Set.of(VERSION);
            }

        };
    }

    static Stream<String> shouldFail() {
        return Stream.of(
                "malformed-version-schema-template-variants",
                "malformed-version-setup-variants",
                "missing-version-tag-schema-template-variants",
                "missing-version-tag-setup-variants",
                "non-exhaustive-schema-template-variants",
                "non-exhaustive-setup-variants",
                "overlapping-schema-template-variants",
                "overlapping-setup-variants"
        );
    }

    @ParameterizedTest
    @MethodSource
    void shouldFail(String testName) {
        assertThrows(YamlExecutionContext.YamlExecutionError.class, () ->
                doRun(testName));
    }


    static Stream<String> shouldPass() {
        return Stream.of(
                "schema-template-variants",
                "setup-block-variants",
                "multiple-schema-template-variants",
                "multiple-setup-variants"
        );
    }

    @ParameterizedTest
    @MethodSource
    void shouldPass(String testName) throws Exception {
        doRun(testName);
    }
}
