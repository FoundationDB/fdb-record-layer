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

import com.apple.foundationdb.relational.api.RelationalConnection;
import com.apple.foundationdb.relational.yamltests.YamlConnection;
import com.apple.foundationdb.relational.yamltests.YamlConnectionFactory;
import com.apple.foundationdb.relational.yamltests.YamlExecutionContext;
import com.apple.foundationdb.relational.yamltests.YamlRunner;
import com.apple.foundationdb.relational.yamltests.configs.EmbeddedConfig;
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
 * Tests that {@code supported_version} skip what they should and nothing else.
 */
public class SupportedVersionTest {

    private static EmbeddedConfig config = new EmbeddedConfig();

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
                return new YamlConnection(DriverManager.getConnection(connectPath.toString()));
            }

            @Override
            public Set<String> getVersionsUnderTest() {
                return Set.of("3.0.18.0");
            }
        };
    }

    static Stream<String> shouldFail() {
        return Stream.of(
                "supported-at-file",
                "supported-at-block",
                "unsupported-at-block-only",
                "supported-at-query",
                "unspecified",
                "lower-at-block",
                "lower-at-query",
                "late-query-supported-version",
                "late-file-options"
        );
    }

    @ParameterizedTest
    @MethodSource("shouldFail")
    void shouldFail(String filename) {
        assertThrows(YamlExecutionContext.YamlExecutionError.class, () ->
                doRun("supported-version/" + filename + ".yamsql"));
    }


    static Stream<String> shouldPass() {
        return Stream.of(
                "unsupported-at-file", // technically for this one the whole test is ignored
                "unsupported-at-block",
                "unsupported-at-query",
                "current-version-at-file",
                "current-version-at-block",
                "current-version-at-query",
                "higher-at-block",
                "higher-at-query",
                "fully-supported",
                "query-with-multiple-configs"
        );
    }

    @ParameterizedTest
    @MethodSource("shouldPass")
    void shouldPass(String filename) throws Exception {
        doRun("supported-version/" + filename + ".yamsql");
    }
}
