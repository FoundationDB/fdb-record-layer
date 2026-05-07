/*
 * CheckExplainTest.java
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
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests that {@code explain} and {@code explainContains} checks fail correctly on a plan mismatch
 * and pass when the plan matches. Exercises the full {@link YamlRunner} stack (requires FDB).
 * <p>
 *     Pure-Java unit tests for YAMSQL correction logic live in
 *     {@link com.apple.foundationdb.relational.yamltests.YamlCorrectionUnitTest}.
 * </p>
 */
class CheckExplainTest {

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

    // ── negative tests ────────────────────────────────────────────────────────

    /**
     * Files that must raise {@link YamlExecutionContext.YamlExecutionError} because the
     * {@code explain} / {@code explainContains} block does not match the actual plan.
     */
    static Stream<String> shouldFail() {
        return Stream.of(
                "wrong-plan",     // exact explain string doesn't match the actual plan
                "wrong-contains"  // explainContains fragment is not present in the actual plan
        );
    }

    @ParameterizedTest
    @MethodSource("shouldFail")
    void shouldFail(String filename) {
        assertThrows(YamlExecutionContext.YamlExecutionError.class, () ->
                doRun("check-explain/shouldFail/" + filename + ".yamsql"));
    }

    // ── positive tests ────────────────────────────────────────────────────────

    /**
     * Files that must complete without error because the {@code explainContains} fragment
     * is present in the actual plan.
     */
    static Stream<String> shouldPass() {
        return Stream.of(
                "contains",  // "SCAN" is present in every full-table-scan plan
                "exact"      // exact explain string matches the full-table-scan plan
        );
    }

    @ParameterizedTest
    @MethodSource("shouldPass")
    void shouldPass(String filename) throws Exception {
        doRun("check-explain/shouldPass/" + filename + ".yamsql");
    }

    // ── add-explain test ──────────────────────────────────────────────────────

    /**
     * Runs a YAMSQL file that has no {@code explain:} block with {@code OPTION_ADD_EXPLAIN}, then
     * verifies that the runner wrote an {@code explain:} line back into the file.  Any
     * {@code explain:} line left by a previous run is stripped at the start so the test is
     * self-resetting.  Skipped in CI because correction mode is not permitted there.
     */
    @Test
    void addExplainInsertsExplainBlockIntoFile() throws Exception {
        Assumptions.assumeFalse(YamlExecutionContext.isInCI(), "Skipped in CI: cannot modify YAMSQL files");

        final String resourcePath = "check-explain/addExplain/add-explain.yamsql";
        final Path filePath = Path.of(System.getProperty("user.dir"), "src", "test", "resources", resourcePath);

        // Strip any explain: line left by a previous run so we always start without one.
        final List<String> original = Files.readAllLines(filePath, StandardCharsets.UTF_8);
        final List<String> stripped = original.stream()
                .filter(line -> !line.stripLeading().startsWith("- explain:"))
                .collect(Collectors.toList());
        Files.write(filePath, stripped, StandardCharsets.UTF_8);

        // Run with OPTION_ADD_EXPLAIN: the synthetic explain config is created and the
        // actual plan is written back into the file by replaceFilesIfRequired().
        new YamlRunner(resourcePath, config.createConnectionFactory(),
                YamlExecutionContext.ContextOptions.of(YamlExecutionContext.OPTION_ADD_EXPLAIN, true)).run();

        // Verify exactly one explain line was written into the file.
        final List<String> updated = Files.readAllLines(filePath, StandardCharsets.UTF_8);
        assertEquals(1, updated.stream().filter(line -> line.stripLeading().startsWith("- explain:")).count());
    }
}
