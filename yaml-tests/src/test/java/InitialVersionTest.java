/*
 * InitialVersionTest.java
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
import java.util.Set;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests that tests based on the initial version flags skip what they should and nothing else.
 */
public class InitialVersionTest {
    private static final SemanticVersion VERSION = SemanticVersion.parse("3.0.18.0");
    private static final EmbeddedConfig config = new EmbeddedConfig(FDBTestEnvironment.randomClusterFile());

    @BeforeAll
    static void beforeAll() throws Exception {
        config.beforeAll();
    }

    @AfterAll
    static void afterAll() throws Exception {
        config.afterAll();
    }

    private void doRunFixedVersion(String testName) throws Exception {
        doRun(testName, createConnectionFactory());
    }

    private void doRunCurrentVersion(String testName) throws Exception {
        doRun(testName, config.createConnectionFactory());
    }

    private void doRun(String testName, YamlConnectionFactory connectionFactory) throws Exception {
        new YamlRunner("initial-version/" + testName + ".yamsql", connectionFactory, YamlExecutionContext.ContextOptions.EMPTY_OPTIONS).run();
    }

    YamlConnectionFactory createConnectionFactory() {
        return new YamlConnectionFactory() {
            @Override
            public YamlConnection getNewConnection(@Nonnull URI connectPath) throws SQLException {
                return new SimpleYamlConnection(DriverManager.getConnection(connectPath.toString()), VERSION);
            }

            @Override
            public Set<SemanticVersion> getVersionsUnderTest() {
                return Set.of(VERSION);
            }

        };
    }

    static Stream<String> shouldFail() {
        return Stream.of(
                "do-not-allow-max-rows-in-at-least",
                "do-not-allow-max-rows-in-less-than",
                "explain-after-version",
                "mid-query",
                "non-exhaustive-versions",
                "non-exhaustive-current-version",
                "wrong-result-at-least",
                "wrong-result-less-than",
                "wrong-count-at-least",
                "wrong-count-less-than",
                "wrong-unordered-at-least",
                "wrong-unordered-less-than",
                "wrong-error-at-least",
                "wrong-error-less-than"
        );
    }

    @ParameterizedTest
    @MethodSource
    void shouldFail(String testName) {
        assertThrows(YamlExecutionContext.YamlExecutionError.class, () ->
                doRunFixedVersion(testName));
    }


    static Stream<String> shouldPass() {
        return Stream.of(
                "less-than-version-tests",
                "at-least-version-tests"
        );
    }

    @ParameterizedTest
    @MethodSource
    void shouldPass(String testName) throws Exception {
        doRunFixedVersion(testName);
    }

    static Stream<String> shouldFailOnCurrent() {
        return Stream.of(
                "mid-query",
                "non-exhaustive-versions",
                "non-exhaustive-current-version",
                "wrong-result-at-least",
                "wrong-count-at-least",
                "wrong-unordered-at-least",
                "wrong-error-at-least"
        );
    }

    @ParameterizedTest
    @MethodSource
    void shouldFailOnCurrent(String testName) {
        assertThrows(YamlExecutionContext.YamlExecutionError.class, () ->
                doRunCurrentVersion(testName));
    }

    static Stream<String> shouldPassOnCurrent() {
        return Stream.of(
                "at-least-current-version",
                "at-least-version-tests",
                "wrong-result-less-than",
                "wrong-count-less-than",
                "wrong-unordered-less-than",
                "wrong-error-less-than"
        );
    }

    @ParameterizedTest
    @MethodSource
    void shouldPassOnCurrent(String testName) throws Exception {
        doRunCurrentVersion(testName);
    }
}
