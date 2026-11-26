/*
 * NamedParameterInjectionTest.java
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

import com.apple.foundationdb.relational.yamltests.YamlExecutionContext;
import com.apple.foundationdb.relational.yamltests.YamlRunner;
import com.apple.foundationdb.relational.yamltests.configs.EmbeddedConfig;
import com.apple.foundationdb.relational.yamltests.configs.YamlTestConfig;
import com.apple.foundationdb.test.FDBTestEnvironment;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests that malformed named parameter injections ({@code !! ?name ... !!}) are rejected. Well-formed named parameter
 * injections are tested in {@code prepared.yamsql}.
 */
class NamedParameterInjectionTest {

    private static final YamlTestConfig config = new EmbeddedConfig(FDBTestEnvironment.allClusterFiles());

    @BeforeAll
    static void beforeAll() throws Exception {
        config.beforeAll();
    }

    @AfterAll
    static void afterAll() throws Exception {
        config.afterAll();
    }

    private void doRun(String fileName) throws Exception {
        new YamlRunner(fileName, config.createConnectionFactory(), YamlExecutionContext.ContextOptions.EMPTY_OPTIONS).run();
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "value-given-twice", // two snippets give a value to the same named parameter
            "value-not-given"    // no snippet gives a value to a named parameter
    })
    void malformedNamedParameterFails(String filename) {
        assertThrows(YamlExecutionContext.YamlExecutionError.class, () ->
                doRun("named-parameters/shouldFail/" + filename + ".yamsql"));
    }

    @Test
    void namedParameterGivenOnceAndReusedPasses() throws Exception {
        doRun("named-parameters/shouldPass/value-given-once.yamsql");
    }
}
