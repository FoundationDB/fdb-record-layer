/*
 * CheckExplainConfigTest.java
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
import com.apple.foundationdb.relational.api.RelationalStruct;
import com.apple.foundationdb.relational.yamltests.command.QueryConfig;
import com.apple.foundationdb.relational.yamltests.command.queryconfigs.CheckExplainConfig;
import com.apple.foundationdb.relational.yamltests.generated.stats.PlannerMetricsProto;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;
import org.opentest4j.AssertionFailedError;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Pure-Java unit tests for {@link CheckExplainConfig}. No FDB or database connection required.
 * <p>
 * Covers:
 * <ul>
 *     <li>{@code addExplain} path (synthetic, {@code getVal()==null})</li>
 *     <li>{@code checkExplainContains} path ({@code !isExact})</li>
 *     <li>{@code checkExplainAndMetrics} path (exact match, 7 sub-cases)</li>
 * </ul>
 * {@link YamlExecutionContext} is mocked so each test controls exactly which options are
 * active and which maintainers are in use.
 */
class CheckExplainConfigTest {

    private static final YamlReference.YamlResource RESOURCE =
            YamlReference.YamlResource.base("check-explain/shouldPass/contains.yamsql");
    private static final YamlReference REFERENCE = RESOURCE.withLineNumber(1);
    private static final String PLAN_A = "SCAN([IS T1])";
    private static final String PLAN_B = "COVERING([IS T1] -> [ID: KEY[0]])";
    private static final String PLAN_DOT = "digraph G {}";

    @Test
    void addExplainAndMetricsQueuesCorrectionWhenFileLoaded() throws Exception {
        final var filesMaintainer = loadedFilesMaintainer();
        final var metricsMaintainer = getMetricsMaintainer(null);
        final var yamlExecutionContext = mockExecutionContext(false, false, true, filesMaintainer, metricsMaintainer);

        syntheticConfig(yamlExecutionContext).invoke(mockResultSet(PLAN_A, PLAN_DOT, metricsStruct(10, 5)));

        final var corrections = filesMaintainer.getPendingCorrections(RESOURCE);
        Assertions.assertEquals(1, corrections.size());
        Assertions.assertInstanceOf(YamlFilesMaintainer.AddExplainCorrection.class, corrections.get(0));
        // addExplainAndMetrics now also records the actual metrics for the new query
        assertMetricsWritten(metricsMaintainer, PLAN_A, 10);
    }

    @Test
    void addExplainAndMetricsThrowsWhenFileNotLoaded() {
        final var filesMaintainer = new YamlFilesMaintainer(); // file never loaded
        final var executionContext = mockExecutionContext(false, false, true, filesMaintainer, getMetricsMaintainer(null));

        // the IllegalStateException from verifyFileLoaded is wrapped by wrapContext
        assertThrows(YamlExecutionContext.YamlExecutionError.class,
                () -> syntheticConfig(executionContext).invoke(mockResultSet(PLAN_A, PLAN_DOT, null)));
    }

    @Test
    void checkExplainContainsPassesWhenFragmentFound() {
        final var executionContext = mockExecutionContext(false, false, false, new YamlFilesMaintainer(), getMetricsMaintainer(null));

        // "SCAN" is contained in PLAN_A — no exception
        Assertions.assertDoesNotThrow(
                () -> containsConfig(executionContext, "SCAN").invoke(mockResultSet(PLAN_A, PLAN_DOT, null)));
    }

    @Test
    void checkExplainContainsThrowsWhenFragmentMissing() {
        final var executionContext = mockExecutionContext(false, false, false, new YamlFilesMaintainer(), getMetricsMaintainer(null));

        assertThrows(AssertionFailedError.class,
                () -> containsConfig(executionContext, "INDEX_SCAN").invoke(mockResultSet(PLAN_A, PLAN_DOT, null)));
    }

    static Stream<Arguments> noMismatchAllFlagsArgs() {
        return Stream.of(Boolean.TRUE, Boolean.FALSE)
                .flatMap(checkExplain -> Stream.of(Boolean.TRUE, Boolean.FALSE)
                        .map(checkMetrics -> Arguments.of(checkExplain, checkMetrics)));
    }

    // P, M, _, _, EM, AM (4 cases)
    // Outcome: _, Preserved - No Dirty
    @ParameterizedTest
    @MethodSource("noMismatchAllFlagsArgs")
    void noMismatchAllFlags(boolean correctExplains, boolean correctMetrics) throws Exception {
        final var expectedMetrics = metricsInfo(PLAN_A, 10, 5);
        final var metricsMaintainer = getMetricsMaintainer(expectedMetrics);
        final var filesMaintainer = new YamlFilesMaintainer();
        final var executionContext = mockExecutionContext(correctExplains, correctMetrics, false, filesMaintainer, metricsMaintainer);
        exactConfig(executionContext, PLAN_A).invoke(mockResultSet(PLAN_A, PLAN_DOT, metricsStruct(10, 5)));
        assertNoFileCorrections(filesMaintainer);
        assertMetricsPreserved(metricsMaintainer, expectedMetrics);
    }

    static Stream<Arguments> noMismatchNoActualMetricsArgs() {
        return Stream.of(Boolean.TRUE, Boolean.FALSE)
                .flatMap(checkExplain -> Stream.of(Boolean.TRUE, Boolean.FALSE)
                        .flatMap(checkMetrics -> Stream.of(Boolean.TRUE, Boolean.FALSE)
                                .map(hasExpectedMetrics -> Arguments.of(checkExplain, checkMetrics, hasExpectedMetrics))));
    }

    // P, _, _, _, _, !AM (16 cases)
    // Outcome: _, Nothing - No Dirty
    @ParameterizedTest
    @MethodSource("noMismatchNoActualMetricsArgs")
    void noMismatchNoActualMetrics(boolean correctExplains, boolean correctMetrics,
                                   boolean hasExpectedMetrics) throws Exception {
        final var expectedInfo = hasExpectedMetrics ? metricsInfo(PLAN_A, 10, 5) : null;
        final var metricsMaintainer = getMetricsMaintainer(expectedInfo);
        final var filesMaintainer = new YamlFilesMaintainer();
        final var executionContext = mockExecutionContext(correctExplains, correctMetrics, false, filesMaintainer, metricsMaintainer);
        exactConfig(executionContext, PLAN_A).invoke(mockResultSet(PLAN_A, PLAN_DOT, null));
        assertNoFileCorrections(filesMaintainer);
        assertMetricsNotWritten(metricsMaintainer);
    }

    static Stream<Arguments> noMismatchCorrectNoneNoExpectedMetricsArgs() {
        return Stream.of(Boolean.TRUE, Boolean.FALSE).map(Arguments::of);
    }

    // P, _, !CE, !CM, !EM, AM (2 cases) — M irrelevant when !EM; both counter values produce the same failure
    // Outcome: _, FAIL_NO_METRICS
    @ParameterizedTest
    @MethodSource("noMismatchCorrectNoneNoExpectedMetricsArgs")
    void noMismatchCorrectNoneNoExpectedMetrics(boolean metricsMatch) throws SQLException {
        final var metricsMaintainer = getMetricsMaintainer(null);
        final var filesMaintainer = new YamlFilesMaintainer();
        final var executionContext = mockExecutionContext(false, false, false, filesMaintainer, metricsMaintainer);
        final var struct = metricsMatch ? metricsStruct(10, 5) : metricsStruct(20, 5);
        final var err = assertThrows(AssertionFailedError.class,
                () -> exactConfig(executionContext, PLAN_A).invoke(mockResultSet(PLAN_A, PLAN_DOT, struct)));
        assertTrue(err.getMessage().contains("No planner metrics"));
    }

    static Stream<Arguments> planMatchMetricsDifferCorrectMetricsArgs() {
        return Stream.of(Boolean.TRUE, Boolean.FALSE).map(Arguments::of);
    }

    // P, !M, _, CM, EM, AM (2 cases)
    // Outcome: _, Written-Dirty
    @ParameterizedTest
    @MethodSource("planMatchMetricsDifferCorrectMetricsArgs")
    void planMatchMetricsDifferCorrectMetrics(boolean correctExplains) throws Exception {
        final var metricsMaintainer = getMetricsMaintainer(metricsInfo(PLAN_A, 10, 5));
        final var filesMaintainer = new YamlFilesMaintainer();
        final var executionContext = mockExecutionContext(correctExplains, true, false, filesMaintainer, metricsMaintainer);
        exactConfig(executionContext, PLAN_A).invoke(mockResultSet(PLAN_A, PLAN_DOT, metricsStruct(20, 5)));
        assertNoFileCorrections(filesMaintainer);
        assertMetricsWritten(metricsMaintainer, PLAN_A, 20);
    }

    // P, !M, _, !CM, EM, AM (2 cases) — CE free; no correction permitted
    // Outcome: _, FAIL_METRICS_CHANGED
    static Stream<Arguments> planMatchMetricsDifferNoCorrectionArgs() {
        return Stream.of(Boolean.TRUE, Boolean.FALSE).map(ce -> Arguments.of(ce));
    }

    @ParameterizedTest
    @MethodSource("planMatchMetricsDifferNoCorrectionArgs")
    void planMatchMetricsDifferNoCorrection(boolean correctExplains) {
        final var metricsMaintainer = getMetricsMaintainer(metricsInfo(PLAN_A, 10, 5));
        final var filesMaintainer = new YamlFilesMaintainer();
        final var executionContext = mockExecutionContext(correctExplains, false, false, filesMaintainer, metricsMaintainer);
        final var err = assertThrows(AssertionFailedError.class,
                () -> exactConfig(executionContext, PLAN_A).invoke(mockResultSet(PLAN_A, PLAN_DOT, metricsStruct(20, 5))));
        assertTrue(err.getMessage().contains("Planner metrics have changed"));
    }

    static Stream<Arguments> planMatchBypassGuardArgs() {
        return Stream.of(
                Arguments.of(true,  true),  // CE, CM
                Arguments.of(false, true)   // !CE, CM — only CM bypasses the guard
        );
    }

    // P, _, CM, !EM, AM (2 cases) — only CM bypasses FAIL_NO_METRICS; CE alone no longer does
    // Outcome: _, Written-Dirty
    @ParameterizedTest
    @MethodSource("planMatchBypassGuardArgs")
    void planMatchBypassGuardWritesActual(boolean correctExplains, boolean correctMetrics) throws Exception {
        final var metricsMaintainer = getMetricsMaintainer(null);
        final var filesMaintainer = new YamlFilesMaintainer();
        final var executionContext = mockExecutionContext(correctExplains, correctMetrics, false, filesMaintainer, metricsMaintainer);
        exactConfig(executionContext, PLAN_A).invoke(mockResultSet(PLAN_A, PLAN_DOT, metricsStruct(20, 5)));
        assertNoFileCorrections(filesMaintainer);
        assertMetricsWritten(metricsMaintainer, PLAN_A, 20);
    }

    // P, _, CE, !CM, !EM, AM (1 case) — CE alone cannot bypass FAIL_NO_METRICS guard
    // Outcome: _, FAIL_NO_METRICS
    @Test
    void planMatchCeOnlyNoExpectedMetricsFails() {
        final var metricsMaintainer = getMetricsMaintainer(null);
        final var filesMaintainer = new YamlFilesMaintainer();
        final var executionContext = mockExecutionContext(true, false, false, filesMaintainer, metricsMaintainer);
        final var err = assertThrows(AssertionFailedError.class,
                () -> exactConfig(executionContext, PLAN_A).invoke(mockResultSet(PLAN_A, PLAN_DOT, metricsStruct(20, 5))));
        assertTrue(err.getMessage().contains("No planner metrics"));
    }

    static Stream<Arguments> planMismatchWritesExplainUpdatedMetricsArgs() {
        return Stream.of(Boolean.TRUE, Boolean.FALSE)
                .flatMap(correctMetrics -> Stream.of(Boolean.TRUE, Boolean.FALSE)
                        .map(hasActualMetrics -> Arguments.of(correctMetrics, hasActualMetrics)));
    }

    // !P, M, CE, _, EM, _ (4 cases) — CM free; AM may be present with matching counters or absent
    // Outcome: F, Written(explain updated)-Dirty
    @ParameterizedTest
    @MethodSource("planMismatchWritesExplainUpdatedMetricsArgs")
    void planMismatchWritesExplainUpdatedMetrics(boolean correctMetrics, boolean hasActualMetrics) throws Exception {
        final var metricsMaintainer = getMetricsMaintainer(metricsInfo(PLAN_A, 10, 5));
        final var filesMaintainer = loadedFilesMaintainer();
        final var executionContext = mockExecutionContext(true, correctMetrics, false, filesMaintainer, metricsMaintainer);
        final var struct = hasActualMetrics ? metricsStruct(10, 5) : null; // matching counters when present
        exactConfig(executionContext, PLAN_A).invoke(mockResultSet(PLAN_B, PLAN_DOT, struct));
        assertExplainCorrectionQueued(filesMaintainer, PLAN_B);
        assertMetricsWritten(metricsMaintainer, PLAN_B, 10); // new plan, counters from expected
    }

    // !P, _, CE, CM, !EM, AM (1 case) — only CM bypasses FAIL_NO_METRICS; CE alone no longer does
    // Outcome: F, Written-Dirty
    @Test
    void planMismatchWritesActualMetrics() throws Exception {
        final var metricsMaintainer = getMetricsMaintainer(null);
        final var filesMaintainer = loadedFilesMaintainer();
        final var executionContext = mockExecutionContext(true, true, false, filesMaintainer, metricsMaintainer);
        exactConfig(executionContext, PLAN_A).invoke(mockResultSet(PLAN_B, PLAN_DOT, metricsStruct(10, 5)));
        assertExplainCorrectionQueued(filesMaintainer, PLAN_B);
        assertMetricsWritten(metricsMaintainer, PLAN_B, 10);
    }

    // !P, _, CE, !CM, !EM, AM (1 case) — CE alone cannot bypass FAIL_NO_METRICS guard
    // Outcome: F, FAIL_NO_METRICS
    @Test
    void planMismatchCeOnlyNoExpectedMetricsFails() throws Exception {
        final var metricsMaintainer = getMetricsMaintainer(null);
        final var filesMaintainer = loadedFilesMaintainer();
        final var executionContext = mockExecutionContext(true, false, false, filesMaintainer, metricsMaintainer);
        final var err = assertThrows(AssertionFailedError.class,
                () -> exactConfig(executionContext, PLAN_A).invoke(mockResultSet(PLAN_B, PLAN_DOT, metricsStruct(10, 5))));
        assertTrue(err.getMessage().contains("No planner metrics"));
        assertExplainCorrectionQueued(filesMaintainer, PLAN_B);
    }

    static Stream<Arguments> planMismatchNoMetricsArgs() {
        return Stream.of(Boolean.TRUE, Boolean.FALSE).map(Arguments::of);
    }

    // !P, _, CE, _, !EM, !AM (2 cases) — CM free; no metrics to write
    // Outcome: F, Nothing-No Dirty
    @ParameterizedTest
    @MethodSource("planMismatchNoMetricsArgs")
    void planMismatchNoMetrics(boolean correctMetrics) throws Exception {
        final var metricsMaintainer = getMetricsMaintainer(null);
        final var filesMaintainer = loadedFilesMaintainer();
        final var executionContext = mockExecutionContext(true, correctMetrics, false, filesMaintainer, metricsMaintainer);
        exactConfig(executionContext, PLAN_A).invoke(mockResultSet(PLAN_B, PLAN_DOT, null));
        assertExplainCorrectionQueued(filesMaintainer, PLAN_B);
        assertMetricsNotWritten(metricsMaintainer);
    }

    static Stream<Arguments> bothMismatchWritesExplainUpdatedMetricsArgs() {
        return Stream.of(Boolean.TRUE, Boolean.FALSE).map(Arguments::of);
    }

    // !P, !M, CE, _, EM, !AM (2 cases) — CM free; no actual counters, explain changed, EM present
    // Outcome: F, Written(explain updated)-Dirty
    @ParameterizedTest
    @MethodSource("bothMismatchWritesExplainUpdatedMetricsArgs")
    void bothMismatchWritesExplainUpdatedMetrics(boolean correctMetrics) throws Exception {
        final var metricsMaintainer = getMetricsMaintainer(metricsInfo(PLAN_A, 10, 5));
        final var filesMaintainer = loadedFilesMaintainer();
        final var executionContext = mockExecutionContext(true, correctMetrics, false, filesMaintainer, metricsMaintainer);
        exactConfig(executionContext, PLAN_A).invoke(mockResultSet(PLAN_B, PLAN_DOT, null));
        assertExplainCorrectionQueued(filesMaintainer, PLAN_B);
        assertMetricsWritten(metricsMaintainer, PLAN_B, 10); // counters from expected
    }

    static Stream<Arguments> bothMismatchWritesActualMetricsArgs() {
        return Stream.of(
                Arguments.of(true, true),   // CM=T, EM
                Arguments.of(true, false)   // CM=T, !EM
        );
    }

    // !P, !M, CE, CM|!EM, AM (2 cases) — CM=T corrects; removed CE-only !EM case (now FAIL_NO_METRICS)
    // Outcome: F, Written-Dirty
    @ParameterizedTest
    @MethodSource("bothMismatchWritesActualMetricsArgs")
    void bothMismatchWritesActualMetrics(boolean correctMetrics, boolean hasExpectedMetrics) throws Exception {
        final var metricsMaintainer = getMetricsMaintainer(hasExpectedMetrics ? metricsInfo(PLAN_A, 10, 5) : null);
        final var filesMaintainer = loadedFilesMaintainer();
        final var executionContext = mockExecutionContext(true, correctMetrics, false, filesMaintainer, metricsMaintainer);
        exactConfig(executionContext, PLAN_A).invoke(mockResultSet(PLAN_B, PLAN_DOT, metricsStruct(20, 5)));
        assertExplainCorrectionQueued(filesMaintainer, PLAN_B);
        assertMetricsWritten(metricsMaintainer, PLAN_B, 20);
    }

    // !P, !M, CE, !CM, !EM, AM (1 case) — CE alone cannot bypass FAIL_NO_METRICS guard
    // Outcome: F, FAIL_NO_METRICS
    @Test
    void bothMismatchCeOnlyNoExpectedMetricsFails() throws Exception {
        final var metricsMaintainer = getMetricsMaintainer(null);
        final var filesMaintainer = loadedFilesMaintainer();
        final var executionContext = mockExecutionContext(true, false, false, filesMaintainer, metricsMaintainer);
        final var err = assertThrows(AssertionFailedError.class,
                () -> exactConfig(executionContext, PLAN_A).invoke(mockResultSet(PLAN_B, PLAN_DOT, metricsStruct(20, 5))));
        assertTrue(err.getMessage().contains("No planner metrics"));
        assertExplainCorrectionQueued(filesMaintainer, PLAN_B);
    }

    // !P, !M, CE, !CM, EM, AM (1 case) — CE corrects explain but CM absent; metrics check fails
    // Outcome: F, FAIL_METRICS_CHANGED
    @Test
    void bothMismatchFailMetricsChanged() throws Exception {
        final var metricsMaintainer = getMetricsMaintainer(metricsInfo(PLAN_A, 10, 5));
        final var filesMaintainer = loadedFilesMaintainer();
        final var executionContext = mockExecutionContext(true, false, false, filesMaintainer, metricsMaintainer);
        final var err = assertThrows(AssertionFailedError.class,
                () -> exactConfig(executionContext, PLAN_A).invoke(mockResultSet(PLAN_B, PLAN_DOT, metricsStruct(20, 5))));
        assertTrue(err.getMessage().contains("Planner metrics have changed"));
        assertExplainCorrectionQueued(filesMaintainer, PLAN_B);
    }

    static Stream<Arguments> bothMismatchNoMetricsArgs() {
        return Stream.of(Boolean.TRUE, Boolean.FALSE).map(cm -> Arguments.of(cm));
    }

    // !P, !M, CE, _, !EM, !AM (2 cases) — CM free; no metrics at all
    // Outcome: F, Nothing-No Dirty
    @ParameterizedTest
    @MethodSource("bothMismatchNoMetricsArgs")
    void bothMismatchNoMetrics(boolean correctMetrics) throws Exception {
        final var metricsMaintainer = getMetricsMaintainer(null);
        final var filesMaintainer = loadedFilesMaintainer();
        final var executionContext = mockExecutionContext(true, correctMetrics, false, filesMaintainer, metricsMaintainer);
        exactConfig(executionContext, PLAN_A).invoke(mockResultSet(PLAN_B, PLAN_DOT, null));
        assertExplainCorrectionQueued(filesMaintainer, PLAN_B);
        assertMetricsNotWritten(metricsMaintainer);
    }

    static Stream<Arguments> planMismatchDoNotCorrectExplainArgs() {
        return Stream.of(Boolean.TRUE, Boolean.FALSE)
                .flatMap(correctMetrics -> Stream.of(Boolean.TRUE, Boolean.FALSE)
                        .flatMap(hasExpectedMetrics -> Stream.of(Boolean.TRUE, Boolean.FALSE)
                                .flatMap(hasActualMetrics -> Stream.of(Boolean.TRUE, Boolean.FALSE)
                                        .map(metricsMatch -> Arguments.of(correctMetrics, hasExpectedMetrics, hasActualMetrics, metricsMatch)))));
    }

    // !P, _, !CE, _, _, _ (16 cases)
    // Outcome: _, FAIL_PLAN
    @ParameterizedTest
    @MethodSource("planMismatchDoNotCorrectExplainArgs")
    void planMismatchDoNotCorrectExplain(boolean correctMetrics, boolean hasExpectedMetrics,
                                         boolean hasActualMetrics, boolean metricsMatch) throws Exception {
        final var metricsMaintainer = getMetricsMaintainer(hasExpectedMetrics ? metricsInfo(PLAN_A, 10, 5) : null);
        final var executionContext = mockExecutionContext(false, correctMetrics, false, new YamlFilesMaintainer(), metricsMaintainer);
        final var struct = hasActualMetrics ? (metricsMatch ? metricsStruct(10, 5) : metricsStruct(20, 5)) : null;
        final var err = assertThrows(AssertionFailedError.class,
                () -> exactConfig(executionContext, PLAN_A).invoke(mockResultSet(PLAN_B, PLAN_DOT, struct)));
        assertTrue(err.getMessage().contains("plan mismatch"));
    }

    private static void assertNoFileCorrections(@Nonnull YamlFilesMaintainer filesMaintainer) {
        assertTrue(filesMaintainer.getPendingCorrections(RESOURCE).isEmpty());
    }

    private static void assertMetricsNotWritten(@Nonnull YamlMetricsMaintainer metricsMaintainer) {
        assertFalse(metricsMaintainer.isMetricsDirty());
        Assertions.assertNull(metricsMaintainer.getActualMetrics(buildIdentifier()));
    }

    private static void assertMetricsPreserved(@Nonnull YamlMetricsMaintainer metricsMaintainer,
                                                @Nonnull PlannerMetricsProto.Info expected) {
        assertFalse(metricsMaintainer.isMetricsDirty());
        Assertions.assertEquals(expected, metricsMaintainer.getActualMetrics(buildIdentifier()));
    }

    private static void assertMetricsWritten(@Nonnull YamlMetricsMaintainer metricsMaintainer,
                                              @Nonnull String expectedPlan, long expectedTaskCount) {
        assertTrue(metricsMaintainer.isMetricsDirty());
        final var stored = metricsMaintainer.getActualMetrics(buildIdentifier());
        Assertions.assertNotNull(stored);
        Assertions.assertEquals(expectedPlan, stored.getExplain());
        Assertions.assertEquals(expectedTaskCount, stored.getCountersAndTimers().getTaskCount());
    }

    /** Builds a mocked context wiring in the provided maintainers and option flags. */
    private static YamlExecutionContext mockExecutionContext(boolean correctExplains, boolean correctMetrics,
                                                             boolean addExplains,
                                                             @Nonnull YamlFilesMaintainer filesMaintainer,
                                                             @Nonnull YamlMetricsMaintainer metricsMaintainer) {
        final var executionContext = Mockito.mock(YamlExecutionContext.class);
        Mockito.when(executionContext.shouldCorrectExplains()).thenReturn(correctExplains);
        Mockito.when(executionContext.shouldCorrectMetrics()).thenReturn(correctMetrics);
        Mockito.when(executionContext.shouldAddExplains()).thenReturn(addExplains);
        Mockito.when(executionContext.shouldShowPlanOnDiff()).thenReturn(false);
        Mockito.when(executionContext.getFilesMaintainer()).thenReturn(filesMaintainer);
        Mockito.when(executionContext.getMetricsMaintainer()).thenReturn(metricsMaintainer);
        return executionContext;
    }

    private static YamlFilesMaintainer loadedFilesMaintainer() throws Exception {
        final var filesMaintainer = new YamlFilesMaintainer();
        filesMaintainer.loadFile(RESOURCE);
        return filesMaintainer;
    }

    private static YamlMetricsMaintainer getMetricsMaintainer(@Nullable PlannerMetricsProto.Info metricsInfo) {
        final ImmutableMap<PlannerMetricsProto.Identifier, PlannerMetricsProto.Info> expectedMetricsMap =
                metricsInfo == null ? ImmutableMap.of() :
                ImmutableMap.of(buildIdentifier(), metricsInfo);
        return new YamlMetricsMaintainer(RESOURCE, expectedMetricsMap);
    }

    private static TestableConfig syntheticConfig(@Nonnull YamlExecutionContext executionContext) {
        return new TestableConfig(REFERENCE, executionContext, null, true);
    }

    private static TestableConfig exactConfig(@Nonnull YamlExecutionContext executionContext, @Nonnull String expected) {
        return new TestableConfig(REFERENCE, executionContext, expected, true);
    }

    private static TestableConfig containsConfig(@Nonnull YamlExecutionContext executionContext, @Nonnull String fragment) {
        return new TestableConfig(REFERENCE, executionContext, fragment, false);
    }

    private static RelationalResultSet mockResultSet(@Nonnull String plan, @Nonnull String planDot,
                                                     @Nullable RelationalStruct metricsStruct) throws SQLException {
        final var rs = Mockito.mock(RelationalResultSet.class);
        Mockito.when(rs.getString(1)).thenReturn(plan);
        Mockito.when(rs.getString(3)).thenReturn(planDot);
        Mockito.when(rs.getStruct(6)).thenReturn(metricsStruct);
        return rs;
    }

    private static RelationalStruct metricsStruct(long taskCount, long transformCount) throws SQLException {
        final var s = Mockito.mock(RelationalStruct.class);
        Mockito.when(s.getLong(1)).thenReturn(taskCount);     // task_count (tracked)
        Mockito.when(s.getLong(2)).thenReturn(1_000_000L);    // task_total_time_ns (must be > 0)
        Mockito.when(s.getLong(3)).thenReturn(transformCount); // transform_count (tracked)
        Mockito.when(s.getLong(4)).thenReturn(1_000L);        // transform_time_ns
        Mockito.when(s.getLong(5)).thenReturn(0L);            // transform_yield_count (tracked)
        Mockito.when(s.getLong(6)).thenReturn(1_000L);        // insert_time_ns
        Mockito.when(s.getLong(7)).thenReturn(0L);            // insert_new_count (tracked)
        Mockito.when(s.getLong(8)).thenReturn(0L);            // insert_reused_count (tracked)
        return s;
    }

    private static PlannerMetricsProto.Info metricsInfo(@Nonnull String plan, long taskCount, long transformCount) {
        return PlannerMetricsProto.Info.newBuilder()
                .setExplain(plan)
                .setDot(PLAN_DOT)
                .setCountersAndTimers(PlannerMetricsProto.CountersAndTimers.newBuilder()
                        .setTaskCount(taskCount)
                        .setTaskTotalTimeNs(1_000_000L)
                        .setTransformCount(transformCount)
                        .setTransformTimeNs(1_000L)
                        .setTransformYieldCount(0L)
                        .setInsertTimeNs(1_000L)
                        .setInsertNewCount(0L)
                        .setInsertReusedCount(0L))
                .build();
    }

    private static PlannerMetricsProto.Identifier buildIdentifier() {
        return PlannerMetricsProto.Identifier.newBuilder()
                .setBlockName("testBlock")
                .setQuery("SELECT 1")
                .build();
    }

    private static void assertExplainCorrectionQueued(@Nonnull YamlFilesMaintainer filesMaintainer,
                                                       @Nonnull String expectedPlan) {
        final var corrections = filesMaintainer.getPendingCorrections(RESOURCE);
        Assertions.assertEquals(1, corrections.size());
        final var correction = corrections.get(0);
        Assertions.assertInstanceOf(YamlFilesMaintainer.ExplainCorrection.class, correction);
        Assertions.assertEquals(REFERENCE.getLineNumber(), correction.getLineNumber());
        final var lines = new ArrayList<>(List.of("      - explain: \"OLD\""));
        correction.apply(lines);
        assertTrue(lines.get(0).contains(expectedPlan));
    }

    static class TestableConfig extends CheckExplainConfig {
        TestableConfig(@Nonnull YamlReference reference, @Nonnull YamlExecutionContext executionContext,
                       @Nullable Object value, boolean isExact) {
            super(QueryConfig.QUERY_CONFIG_EXPLAIN, value, reference, executionContext, isExact, "testBlock");
        }

        void invoke(@Nonnull Object actual) throws SQLException {
            checkResultInternal("SELECT 1", actual, "SELECT 1", List.of());
        }
    }
}
