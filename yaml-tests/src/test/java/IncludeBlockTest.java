/*
 * IncludeBlockTest.java
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
    private static final boolean CORRECT_METRICS = false;
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

    // yamsql files that works if they are rightly included in their parent yamsql file (that is, the parent file has
    // the include block that references these file). However, when run independently, they will not run successfully
    // due to lack of proper setup block.
    static Stream<String> includesShouldFail() {
        return Stream.of(
                "include-connects-to-global-connection",
                "include-has-options",
                "include-no-explain",
                "include-recursion",
                "include-with-explain",
                "include-with-a-test-block",
                "include-with-include"
        );
    }

    // yamsql files that works if they are rightly included in their parent yamsql file (that is, the parent file has
    // the include block that references these file). These file can also work independently and hence, running them
    // should be successful as they have all the ingredients for setup.
    static Stream<String> includesShouldPass() {
        return Stream.of(
                "include-scoped"
        );
    }

    // top-level yamsql files, with one or more includes, that do not run successfully.
    @ParameterizedTest
    @MethodSource("includesShouldFail")
    void includesShouldFail(String filename) {
        assertThrows(YamlExecutionContext.YamlExecutionError.class, () ->
                doRun("include-block/includes/" + filename + ".yamsql"));
    }

    // top-level yamsql files, with one or more includes, that is expected to run successfully.
    @ParameterizedTest
    @MethodSource("includesShouldPass")
    void includesShouldPass(String filename) throws Exception {
        doRun("include-block/includes/" + filename + ".yamsql");
    }

    static Stream<String> shouldFail() {
        return Stream.of(
                "cycle-in-include",
                "include-non-existent",
                "include-scope-not-visible-outside",
                "options-in-included",
                "simple-include-with-wrong-metric"
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
                "include-falls-back-to-global-available-uri",
                "include-prioritize-local-uri",
                "multiple-includes",
                "multiple-same-includes",
                "nested-includes",
                "simple-include",
                "simple-include-different-env"
        );
    }

    @ParameterizedTest
    @MethodSource("shouldPass")
    void shouldPass(String filename) throws Exception {
        doRun("include-block/shouldPass/" + filename + ".yamsql");
    }
}
