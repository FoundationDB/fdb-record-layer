/*
 * CustomTagTest
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

import com.apple.foundationdb.relational.yamltests.YamlConnectionFactory;
import com.apple.foundationdb.relational.yamltests.YamlExecutionContext;
import com.apple.foundationdb.relational.yamltests.YamlRunner;
import com.apple.foundationdb.relational.yamltests.configs.EmbeddedConfig;
import com.apple.foundationdb.test.FDBTestEnvironment;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nonnull;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests for custom YAML tags such as {@link com.apple.foundationdb.relational.yamltests.tags.IgnoreTag}.
 */
class CustomTagTest {
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

    private void doRun(String testName, YamlConnectionFactory connectionFactory) throws Exception {
        new YamlRunner(testName, connectionFactory, YamlExecutionContext.ContextOptions.EMPTY_OPTIONS).run();
    }

    @Nonnull
    static Stream<String> shouldPass() {
        return Stream.of(
                "ignore-tag",
                "pos-tag"
        );
    }

    @ParameterizedTest
    @MethodSource("shouldPass")
    void shouldPass(String filename) throws Exception {
        doRun("custom-tags/shouldPass/" + filename + ".yamsql", config.createConnectionFactory());
    }
}
