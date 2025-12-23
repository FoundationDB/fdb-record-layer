/*
 * SupportedVersionTest.java
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
import com.apple.foundationdb.relational.yamltests.configs.YamlTestConfig;
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
 * Tests with {@link com.apple.foundationdb.relational.yamltests.block.IncludeBlock}.
 */
public class IncludeBlockTest {

    private static final SemanticVersion VERSION = SemanticVersion.parse("4.4.8.0");
    private static final YamlTestConfig config = new EmbeddedConfig(FDBTestEnvironment.randomClusterFile());
    private static final boolean CORRECT_METRICS = true;
    private static final String CLUSTER_FILE = FDBTestEnvironment.randomClusterFile();

    @BeforeAll
    static void beforeAll() throws Exception {
        config.beforeAll();
    }

    @AfterAll
    static void afterAll() throws Exception {
        config.afterAll();
    }

    private void doRun(String fileName) throws Exception {
        final YamlExecutionContext.ContextOptions options;
        if (CORRECT_METRICS) {
            options = YamlExecutionContext.ContextOptions.of(YamlExecutionContext.OPTION_CORRECT_METRICS, true);
        } else {
            options = YamlExecutionContext.ContextOptions.EMPTY_OPTIONS;
        }
        new YamlRunner(fileName, createConnectionFactory(), options).run();
    }

    YamlConnectionFactory createConnectionFactory() {
        return new YamlConnectionFactory() {
            @Override
            public YamlConnection getNewConnection(@Nonnull URI connectPath) throws SQLException {
                return new SimpleYamlConnection(DriverManager.getConnection(connectPath.toString()), VERSION, CLUSTER_FILE);
            }

            @Override
            public Set<SemanticVersion> getVersionsUnderTest() {
                return Set.of(VERSION);
            }
        };
    }

    static Stream<String> includesShouldFail() {
        return Stream.of(
                "include-1",
                "include-2",
                "include-3",
                "include-4"
        );
    }

    @ParameterizedTest
    @MethodSource("includesShouldFail")
    void includesShouldFail(String filename) {
        assertThrows(YamlExecutionContext.YamlExecutionError.class, () ->
                doRun("include-block/includes/" + filename + ".yamsql"));
    }

    static Stream<String> shouldFail() {
        return Stream.of(
                "options-in-included"
        );
    }

    @ParameterizedTest
    @MethodSource("shouldFail")
    void shouldFail(String filename) {
        assertThrows(YamlExecutionContext.YamlExecutionError.class, () ->
                doRun("include-block/shouldFail/" + filename + ".yamsql"));
    }

    static Stream<String> shouldPass() {
        return Stream.of(
                "single-connection",
                "multiple-includes",
                "multiple-same-includes",
                "nested-includes"
        );
    }

    @ParameterizedTest
    @MethodSource("shouldPass")
    void shouldPass(String filename) throws Exception {
        doRun("include-block/shouldPass/" + filename + ".yamsql");
    }
}
