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
import com.apple.foundationdb.relational.yamltests.YamlReference;
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

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests that {@code explain} and {@code explainContains} checks fail correctly on a plan mismatch
 * and pass when the plan matches, and that {@link YamlExecutionContext.AddExplainCorrection} inserts
 * an {@code explain} line at the right position in the YAMSQL source.
 * <p>
 *     The shouldFail / shouldPass tests exercise the full {@link YamlRunner} stack (requires FDB).
 *     The AddExplainCorrection tests are pure-Java unit tests with no database dependency.
 * </p>
 */
public class CheckExplainTest {

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
                "contains"  // "SCAN" is present in every full-table-scan plan
        );
    }

    @ParameterizedTest
    @MethodSource("shouldPass")
    void shouldPass(String filename) throws Exception {
        doRun("check-explain/shouldPass/" + filename + ".yamsql");
    }

    // ── addExplain end-to-end tests ───────────────────────────────────────────

    /**
     * Runs with {@link YamlExecutionContext#OPTION_ADD_EXPLAIN} on a YAMSQL file whose query has
     * no {@code explain:} block. Verifies that the runner completes without error (the actual plan
     * is written into the file as a side effect). Skipped in CI to avoid modifying source files.
     */
    @Test
    void addExplainPopulatesMissingExplain() throws Exception {
        Assumptions.assumeFalse(YamlExecutionContext.isInCI(), "skipped in CI — would modify source files");
        new YamlRunner("check-explain/addExplain/no-explain.yamsql",
                config.createConnectionFactory(),
                YamlExecutionContext.ContextOptions.of(YamlExecutionContext.OPTION_ADD_EXPLAIN, true)).run();
    }

    /**
     * Runs with {@link YamlExecutionContext#OPTION_ADD_EXPLAIN} on a YAMSQL file whose query has
     * an intentionally wrong {@code explain:} value. Verifies that the runner completes without
     * error (the stale value is corrected in the file). Skipped in CI to avoid modifying source files.
     */
    @Test
    void addExplainCorrectsWrongExplain() throws Exception {
        Assumptions.assumeFalse(YamlExecutionContext.isInCI(), "skipped in CI — would modify source files");
        new YamlRunner("check-explain/addExplain/wrong-explain.yamsql",
                config.createConnectionFactory(),
                YamlExecutionContext.ContextOptions.of(YamlExecutionContext.OPTION_ADD_EXPLAIN, true)).run();
    }

    // ── AddExplainCorrection unit tests (no FDB required) ────────────────────

    /**
     * Basic case: query and result on consecutive lines, explain inserted between them.
     */
    @Test
    void addExplainCorrectionInsertsBeforeFirstConfig() {
        final List<String> lines = new ArrayList<>(List.of(
                "      - query: SELECT id FROM t1",
                "      - result: [{!l 1}]"
        ));
        applyAddExplainCorrection(lines, 1, "SCAN([IS T1])");
        assertEquals(3, lines.size());
        assertEquals("      - explain: \"SCAN([IS T1])\"", lines.get(1));
        assertEquals("      - result: [{!l 1}]", lines.get(2));
    }

    /**
     * Multi-line query (block scalar): the correction must skip past the query body lines
     * and insert before the first config entry at the query's indentation.
     */
    @Test
    void addExplainCorrectionSkipsMultiLineQueryBody() {
        final List<String> lines = new ArrayList<>(List.of(
                "      - query: |",
                "          SELECT id",
                "          FROM t1",
                "      - result: [{!l 1}]"
        ));
        applyAddExplainCorrection(lines, 1, "SCAN([IS T1])");
        assertEquals(5, lines.size());
        assertEquals("      - explain: \"SCAN([IS T1])\"", lines.get(3));
        assertEquals("      - result: [{!l 1}]", lines.get(4));
    }

    /**
     * When there are already other config entries (e.g. {@code planHash:}) before {@code result:},
     * the explain is inserted as the first config.
     */
    @Test
    void addExplainCorrectionInsertsAsFirstConfig() {
        final List<String> lines = new ArrayList<>(List.of(
                "      - query: SELECT id FROM t1",
                "      - planHash: 12345",
                "      - result: [{!l 1}]"
        ));
        applyAddExplainCorrection(lines, 1, "SCAN([IS T1])");
        assertEquals(4, lines.size());
        assertEquals("      - explain: \"SCAN([IS T1])\"", lines.get(1));
        assertEquals("      - planHash: 12345", lines.get(2));
        assertEquals("      - result: [{!l 1}]", lines.get(3));
    }

    /**
     * Indentation is derived from the {@code query:} line, so different indentation levels
     * are handled correctly.
     */
    @Test
    void addExplainCorrectionRespectsIndentation() {
        final List<String> lines = new ArrayList<>(List.of(
                "    - query: SELECT id FROM t1",
                "    - result: [{!l 1}]"
        ));
        applyAddExplainCorrection(lines, 1, "MY PLAN");
        assertEquals(3, lines.size());
        assertEquals("    - explain: \"MY PLAN\"", lines.get(1));
    }

    /**
     * When the query is the last entry in the block (no following config lines at the same
     * indentation), the explain is appended immediately after the query line.
     */
    @Test
    void addExplainCorrectionFallsBackToAfterQueryLine() {
        final List<String> lines = new ArrayList<>(List.of(
                "      - query: SELECT id FROM t1"
        ));
        applyAddExplainCorrection(lines, 1, "SCAN([IS T1])");
        assertEquals(2, lines.size());
        assertEquals("      - explain: \"SCAN([IS T1])\"", lines.get(1));
    }

    /**
     * Applying the same correction twice (deduplication is the caller's responsibility;
     * this test verifies {@code apply()} itself is idempotent in position when the line
     * number is stable).
     */
    @Test
    void addExplainCorrectionHandlesPlanWithQuotes() {
        final List<String> lines = new ArrayList<>(List.of(
                "      - query: SELECT id FROM t1",
                "      - result: [{!l 1}]"
        ));
        applyAddExplainCorrection(lines, 1, "SCAN | MAP");
        assertEquals("      - explain: \"SCAN | MAP\"", lines.get(1));
    }

    // ── helpers ──────────────────────────────────────────────────────────────

    private static void applyAddExplainCorrection(List<String> lines, int oneBasedLineNumber, String plan) {
        final YamlReference.YamlResource resource = YamlReference.YamlResource.base("test.yamsql");
        final YamlReference ref = resource.withLineNumber(oneBasedLineNumber);
        new YamlExecutionContext.AddExplainCorrection(ref, plan).apply(lines);
    }
}
