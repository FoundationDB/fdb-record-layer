/*
 * CheckResultMetadataTest.java
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
import java.util.Set;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests that {@code resultMetadata} checks fail correctly when the expected metadata does not match
 * the actual result-set metadata, and pass when it does.
 * <p>
 *     Negative (shouldFail) tests are the most important: if a mismatch silently passes we would
 *     have false confidence in the coverage.
 * </p>
 */
public class CheckResultMetadataTest {

    private static final SemanticVersion VERSION = SemanticVersion.parse("4.4.8.0");
    private static final String CLUSTER_FILE = FDBTestEnvironment.randomClusterFile();
    private static final EmbeddedConfig config = new EmbeddedConfig(CLUSTER_FILE);

    @BeforeAll
    static void beforeAll() throws Exception {
        config.beforeAll();
    }

    @AfterAll
    static void afterAll() throws Exception {
        config.afterAll();
    }

    private void doRun(String fileName) throws Exception {
        new YamlRunner(fileName, createConnectionFactory(), YamlExecutionContext.ContextOptions.EMPTY_OPTIONS).run();
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

    // ── negative tests ────────────────────────────────────────────────────────

    /**
     * Files that must raise {@link YamlExecutionContext.YamlExecutionError} because the
     * {@code resultMetadata} block does not match what the query actually returns.
     */
    static Stream<String> shouldFail() {
        return Stream.of(
                "wrong-column-name",                    // expected "WRONG", actual "ID"
                "wrong-column-type",                    // expected INTEGER, actual BIGINT
                "missing-column",                       // expected 1 column, actual 2
                "extra-column",                         // expected 2 columns, actual 1
                "wrong-column-order",                   // columns listed in reversed order
                "wrong-nested-struct-name",             // struct column name wrong
                "wrong-nested-struct-type",             // struct column typed as BIGINT
                "wrong-metadata-on-continuation-page"   // inline check on continuation page fails
        );
    }

    @ParameterizedTest
    @MethodSource("shouldFail")
    void shouldFail(String filename) {
        assertThrows(YamlExecutionContext.YamlExecutionError.class, () ->
                doRun("check-result-metadata/shouldFail/" + filename + ".yamsql"));
    }

    // ── positive tests ────────────────────────────────────────────────────────

    /**
     * Files that must complete without error because the {@code resultMetadata} block
     * correctly describes the query's result-set columns.
     */
    static Stream<String> shouldPass() {
        return Stream.of(
                "single-column",                   // one BIGINT column
                "multiple-columns",                // two columns, also exercises case-insensitive name match
                "nested-struct-column",            // struct column reported as STRUCT type
                "metadata-on-continuation-page"   // inline check on continuation page
        );
    }

    @ParameterizedTest
    @MethodSource("shouldPass")
    void shouldPass(String filename) throws Exception {
        doRun("check-result-metadata/shouldPass/" + filename + ".yamsql");
    }
}
