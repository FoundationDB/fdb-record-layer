/*
 * YamlCorrectionUnitTest.java
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

package com.apple.foundationdb.relational.yamltests;

import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.yamltests.command.QueryConfig;
import com.apple.foundationdb.relational.yamltests.command.queryconfigs.CheckExplainConfig;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.opentest4j.AssertionFailedError;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Pure-Java unit tests for YAMSQL file-correction logic. No FDB or database connection required.
 * <p>
 *     Covers {@link YamlExecutionContext.AddExplainCorrection} (file-editing) and the null-value
 *     (synthetic) path in {@link CheckExplainConfig} that is triggered when
 *     {@code OPTION_ADD_EXPLAIN} is active and a query has no {@code explain:} block yet.
 * </p>
 */
class YamlCorrectionUnitTest {

    // ── AddExplainCorrection tests ────────────────────────────────────────────

    /**
     * Basic case: query and result on consecutive lines, explain inserted between them.
     */
    @Test
    void addExplainCorrectionInsertsBeforeFirstConfig() {
        final List<String> originalList = List.of(
                "      - query: SELECT id FROM t1",
                "      - result: [{!l 1}]"
        );
        final List<String> lines = new ArrayList<>(originalList);
        applyAddExplainCorrection(lines, 1, "SCAN([IS T1])");
        assertEquals(lines, List.of(originalList.get(0), "      - explain: \"SCAN([IS T1])\"", originalList.get(1)));
    }

    /**
     * Multi-line query (block scalar): the correction must skip past the query body lines
     * and insert before the first config entry at the query's indentation.
     */
    @Test
    void addExplainCorrectionSkipsMultiLineQueryBody() {
        final List<String> originalList = List.of(
                "      - query: |",
                "          SELECT id",
                "          FROM t1",
                "      - result: [{!l 1}]"
        );
        final List<String> lines = new ArrayList<>(originalList);
        applyAddExplainCorrection(lines, 1, "SCAN([IS T1])");
        assertEquals(lines, List.of(originalList.get(0), originalList.get(1), originalList.get(2), "      - explain: \"SCAN([IS T1])\"", originalList.get(3)));
    }

    /**
     * When there are already other config entries (e.g. {@code planHash:}) before {@code result:},
     * the explain is inserted as the first config.
     */
    @Test
    void addExplainCorrectionInsertsAsFirstConfig() {
        final List<String> originalList = List.of(
                "      - query: SELECT id FROM t1",
                "      - planHash: 12345",
                "      - result: [{!l 1}]"
        );
        final List<String> lines = new ArrayList<>(originalList);
        applyAddExplainCorrection(lines, 1, "SCAN([IS T1])");
        assertEquals(lines, List.of(originalList.get(0), "      - explain: \"SCAN([IS T1])\"", originalList.get(1), originalList.get(2)));
    }

    /**
     * Indentation is derived from the {@code query:} line, so different indentation levels
     * are handled correctly.
     */
    @Test
    void addExplainCorrectionRespectsIndentation() {
        final List<String> originalList = List.of(
                "    - query: SELECT id FROM t1",
                "    - result: [{!l 1}]"
        );
        final List<String> lines = new ArrayList<>(originalList);
        applyAddExplainCorrection(lines, 1, "MY PLAN");
        assertEquals(lines, List.of(originalList.get(0), "    - explain: \"MY PLAN\"", originalList.get(1)));
    }

    /**
     * When the query is the last entry in the block (no following config lines at the same
     * indentation), the explain is appended immediately after the query line.
     */
    @Test
    void addExplainCorrectionFallsBackToAfterQueryLine() {
        final List<String> originalList = List.of(
                "      - query: SELECT id FROM t1"
        );
        final List<String> lines = new ArrayList<>(originalList);
        applyAddExplainCorrection(lines, 1, "SCAN([IS T1])");
        assertEquals(lines, List.of(originalList.get(0), "      - explain: \"SCAN([IS T1])\""));
    }

    /**
     * Plan strings containing special characters (e.g. {@code |}) must be quoted correctly in the
     * inserted {@code explain:} line.
     */
    @Test
    void addExplainCorrectionHandlesPlanWithQuotes() {
        final List<String> originalList = List.of(
                "      - query: SELECT id FROM t1",
                "      - result: [{!l 1}]"
        );
        final List<String> lines = new ArrayList<>(originalList);
        applyAddExplainCorrection(lines, 1, "SCAN | MAP");
        assertEquals(lines, List.of(originalList.get(0), "      - explain: \"SCAN | MAP\"", originalList.get(1)));
    }

    /**
     * When the query has both a {@code result:} and a {@code resultMetadata:} block, the explain
     * is inserted before {@code result:} and the metadata block is preserved at the end.
     */
    @Test
    void addExplainCorrectionPreservesResultMetadata() {
        final List<String> originalList = List.of(
                "      - query: SELECT id FROM t1",
                "      - result: [{!l 1}]",
                "      - resultMetadata: [{name: ID, type: BIGINT}]"
        );
        final List<String> lines = new ArrayList<>(originalList);
        applyAddExplainCorrection(lines, 1, "SCAN([IS T1])");
        assertEquals(lines, List.of(originalList.get(0), "      - explain: \"SCAN([IS T1])\"", originalList.get(1), originalList.get(2)));
    }

    // ── CheckExplainConfig null-value (synthetic) path tests ─────────────────

    // Exposes checkResultInternal (protected) for direct testing from this package.
    static class TestableCheckExplainConfig extends CheckExplainConfig {
        TestableCheckExplainConfig(YamlReference reference, YamlExecutionContext executionContext) {
            super(QueryConfig.QUERY_CONFIG_EXPLAIN, null, reference, executionContext, true, "block");
        }

        void invoke(Object actual) throws SQLException {
            checkResultInternal("SELECT 1", actual, "SELECT 1", List.of());
        }
    }

    /**
     * When value==null and {@code addExplain()} returns false — because {@code shouldAddExplains()}
     * is false in an {@code EMPTY_OPTIONS} context — {@code checkResultInternal} must report a
     * test failure rather than silently returning.
     */
    @Test
    void nullValueReportsFailureWhenAddExplainReturnsFalse() throws Exception {
        final YamlConnectionFactory factory = Mockito.mock(YamlConnectionFactory.class);
        final var resource = YamlReference.YamlResource.base("check-explain/shouldPass/contains.yamsql");
        final var ctx = new YamlExecutionContext(resource, factory, YamlExecutionContext.ContextOptions.EMPTY_OPTIONS);
        ctx.registerResource(resource);

        final var config = new TestableCheckExplainConfig(resource.withLineNumber(1), ctx);
        final var rs = Mockito.mock(RelationalResultSet.class);
        Mockito.when(rs.getString(1)).thenReturn("SOME PLAN");

        assertThrows(AssertionFailedError.class, () -> config.invoke(rs));
    }

    // ── helpers ───────────────────────────────────────────────────────────────

    private static void applyAddExplainCorrection(List<String> lines, int oneBasedLineNumber, String plan) {
        final YamlReference.YamlResource resource = YamlReference.YamlResource.base("test.yamsql");
        final YamlReference ref = resource.withLineNumber(oneBasedLineNumber);
        new YamlExecutionContext.AddExplainCorrection(ref, plan).apply(lines);
    }
}
