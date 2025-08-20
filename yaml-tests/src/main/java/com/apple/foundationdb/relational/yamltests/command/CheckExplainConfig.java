/*
 * CheckExplainConfig.java
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

package com.apple.foundationdb.relational.yamltests.command;

import com.apple.foundationdb.record.query.plan.cascades.debug.BrowserHelper;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.RelationalStruct;
import com.apple.foundationdb.relational.yamltests.MaintainYamlTestConfig;
import com.apple.foundationdb.relational.yamltests.YamlExecutionContext;
import com.apple.foundationdb.relational.yamltests.generated.stats.PlannerMetricsProto;
import com.github.difflib.text.DiffRow;
import com.github.difflib.text.DiffRowGenerator;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Descriptors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

import static com.apple.foundationdb.relational.yamltests.command.QueryCommand.reportTestFailure;

/**
 + * QueryConfig associated with {@link QueryConfig#QUERY_CONFIG_EXPLAIN} and
 + * {@link QueryConfig#QUERY_CONFIG_EXPLAIN_CONTAINS}, that validates that the results of running {@code EXPLAIN} with
 + * the query under test matches the explain results. In addition, this gathers the planner metrics from the results of
 + * the explains and compares them to the ones committed. It doesn't compare raw timing, because that would, naturally
 + * change between runs, but those are stored to provide context. In the event that
 + * {@link MaintainYamlTestConfig} is used to correct the explain and/or
 + * metrics, this will record the actual values via the {@link YamlExecutionContext}, which will save them when the
 + * test completes.
 + */
class CheckExplainConfig extends QueryConfig {

    private static final Logger logger = LogManager.getLogger(CheckExplainConfig.class);
    private final int lineNumber;
    private final YamlExecutionContext executionContext;
    private final boolean isExact;
    private final String blockName;

    public CheckExplainConfig(final String configName, final Object value, final int lineNumber, final YamlExecutionContext executionContext, final boolean isExact, final String blockName) {
        super(configName, value, lineNumber, executionContext);
        this.lineNumber = lineNumber;
        this.executionContext = executionContext;
        this.isExact = isExact;
        this.blockName = blockName;
    }

    @Override
    String decorateQuery(@Nonnull String query) {
        return "EXPLAIN " + query;
    }

    @SuppressWarnings({"PMD.CloseResource", "PMD.EmptyWhileStmt"}) // lifetime of autocloseable resource persists beyond method
    @Override
    void checkResultInternal(@Nonnull String currentQuery, @Nonnull Object actual,
                             @Nonnull String queryDescription, @Nonnull List<String> setups) throws SQLException {
        logger.debug("⛳️ Matching plan for query '{}'", queryDescription);
        final var resultSet = (RelationalResultSet) actual;
        resultSet.next();
        final var actualPlan = resultSet.getString(1);
        final var actualDot = resultSet.getString(3);
        final var metricsMap = executionContext.getMetricsMap();
        final var identifier = PlannerMetricsProto.Identifier.newBuilder()
                .setBlockName(blockName)
                .setQuery(currentQuery)
                .addAllSetups(setups)
                .build();
        final var expectedPlannerMetricsInfo = metricsMap.get(identifier);

        final String expectedDot = expectedPlannerMetricsInfo == null ? null : expectedPlannerMetricsInfo.getDot();

        checkExplain(queryDescription, actualPlan, actualDot, expectedDot);

        final var actualPlannerMetrics = resultSet.getStruct(6);
        if (isExact && actualPlannerMetrics != null) {
            Objects.requireNonNull(actualDot);
            checkMetrics(currentQuery, setups, actualPlannerMetrics, expectedPlannerMetricsInfo, actualPlan, actualDot);
        }
    }

    private void checkExplain(final @Nonnull String queryDescription,
                              final @Nonnull String actualPlan,
                              final @Nullable String actualDot,
                              final @Nullable String expectedDot) {
        var success = isExact ? getVal().equals(actualPlan) : actualPlan.contains((String) getVal());
        if (success) {
            logger.debug("✅️ plan match!");
        } else {
            if (executionContext.shouldShowPlanOnDiff() &&
                    actualDot != null && expectedDot != null) {
                BrowserHelper.browse("/showPlanDiff.html",
                        ImmutableMap.of("$SQL", queryDescription,
                                "$DOT_EXPECTED", expectedDot,
                                "$DOT_ACTUAL", actualDot));
            }

            final var expectedPlan = getValueString();
            final var diffGenerator = DiffRowGenerator.create()
                    .showInlineDiffs(true)
                    .inlineDiffByWord(true)
                    .newTag(f -> f ? CommandUtil.Color.RED.toString() : CommandUtil.Color.RESET.toString())
                    .oldTag(f -> f ? CommandUtil.Color.GREEN.toString() : CommandUtil.Color.RESET.toString())
                    .build();
            final List<DiffRow> diffRows = diffGenerator.generateDiffRows(
                    Collections.singletonList(expectedPlan),
                    Collections.singletonList(actualPlan));
            final var planDiffs = new StringBuilder();
            for (final var diffRow : diffRows) {
                planDiffs.append(diffRow.getOldLine()).append('\n').append(diffRow.getNewLine()).append('\n');
            }
            if (isExact && executionContext.shouldCorrectExplains()) {
                if (!executionContext.correctExplain(getLineNumber() - 1, actualPlan)) {
                    reportTestFailure("‼️ Cannot correct explain plan at line " + getLineNumber());
                } else {
                    logger.debug("⭐️ Successfully replaced plan at line {}", getLineNumber());
                }
            } else {
                final var diffMessage = String.format(Locale.ROOT, "‼️ plan mismatch at line %d:%n" +
                        "⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤%n%s" +
                        "⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤%n" +
                        "↪ expected plan %s:%n%s%n" +
                        "⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤%n" +
                        "↩ actual plan:%n%s",
                        getLineNumber(), planDiffs, (!isExact ? "fragment" : ""), getValueString(), actualPlan);
                reportTestFailure(diffMessage);
            }
        }
    }

    private void checkMetrics(final @Nonnull String currentQuery,
                              final @Nonnull List<String> setups,
                              final @Nonnull RelationalStruct actualPlannerMetrics,
                              final @Nullable PlannerMetricsProto.Info expectedPlannerMetricsInfo,
                              final @Nonnull String actualPlan,
                              final @Nonnull String actualDot) throws SQLException {
        final var taskCount = actualPlannerMetrics.getLong(1);
        Verify.verify(taskCount > 0);
        final var taskTotalTimeInNs = actualPlannerMetrics.getLong(2);
        Verify.verify(taskTotalTimeInNs > 0);

        if (expectedPlannerMetricsInfo == null && !executionContext.shouldCorrectMetrics()) {
            reportTestFailure("‼️ No planner metrics for line " + getLineNumber());
        }
        final var actualInfo = PlannerMetricsProto.Info.newBuilder()
                .setExplain(actualPlan)
                .setDot(actualDot)
                .setCountersAndTimers(PlannerMetricsProto.CountersAndTimers.newBuilder()
                                .setTaskCount(taskCount)
                                .setTaskTotalTimeNs(taskTotalTimeInNs)
                                .setTransformCount(actualPlannerMetrics.getLong(3))
                                .setTransformTimeNs(actualPlannerMetrics.getLong(4))
                                .setTransformYieldCount(actualPlannerMetrics.getLong(5))
                                .setInsertTimeNs(actualPlannerMetrics.getLong(6))
                                .setInsertNewCount(actualPlannerMetrics.getLong(7))
                                .setInsertReusedCount(actualPlannerMetrics.getLong(8)))
                .build();
        if (expectedPlannerMetricsInfo == null) {
            executionContext.putMetrics(blockName, currentQuery, lineNumber, actualInfo, setups);
            executionContext.markDirty();
            logger.debug("⭐️ Successfully inserted new planner metrics at line {}", getLineNumber());
        } else {
            final var expectedCountersAndTimers = expectedPlannerMetricsInfo.getCountersAndTimers();
            final var actualCountersAndTimers = actualInfo.getCountersAndTimers();
            final var metricsDescriptor = expectedCountersAndTimers.getDescriptorForType();

            executionContext.putMetrics(blockName, currentQuery, lineNumber, actualInfo, setups);
            if (areMetricsDifferent(expectedCountersAndTimers, actualCountersAndTimers, metricsDescriptor)) {
                if (executionContext.shouldCorrectMetrics()) {
                    executionContext.markDirty();
                    logger.debug("⭐️ Successfully updated planner metrics at line {}", getLineNumber());
                } else {
                    reportTestFailure("‼️ Planner metrics have changed for line " + getLineNumber());
                }
            }
        }
    }

    private boolean areMetricsDifferent(final PlannerMetricsProto.CountersAndTimers expectedCountersAndTimers,
                                        final PlannerMetricsProto.CountersAndTimers actualCountersAndTimers,
                                        final Descriptors.Descriptor metricsDescriptor) {
        return isMetricDifferent(expectedCountersAndTimers,
                actualCountersAndTimers,
                metricsDescriptor.findFieldByName("task_count"),
                lineNumber) |
                isMetricDifferent(expectedCountersAndTimers,
                        actualCountersAndTimers,
                        metricsDescriptor.findFieldByName("transform_count"),
                        lineNumber) |
                isMetricDifferent(expectedCountersAndTimers,
                        actualCountersAndTimers,
                        metricsDescriptor.findFieldByName("transform_yield_count"),
                        lineNumber) |
                isMetricDifferent(expectedCountersAndTimers,
                        actualCountersAndTimers,
                        metricsDescriptor.findFieldByName("insert_new_count"),
                        lineNumber) |
                isMetricDifferent(expectedCountersAndTimers,
                        actualCountersAndTimers,
                        metricsDescriptor.findFieldByName("insert_reused_count"),
                        lineNumber);
    }

    private static boolean isMetricDifferent(@Nonnull final PlannerMetricsProto.CountersAndTimers expected,
                                             @Nonnull final PlannerMetricsProto.CountersAndTimers actual,
                                             @Nonnull final Descriptors.FieldDescriptor fieldDescriptor,
                                             int lineNumber) {
        final long expectedMetric = (long)expected.getField(fieldDescriptor);
        final long actualMetric = (long)actual.getField(fieldDescriptor);
        if (expectedMetric != actualMetric) {
            logger.warn("‼️ metric {} differs; lineNumber = {}; expected = {}; actual = {}",
                    fieldDescriptor.getName(), lineNumber, expectedMetric, actualMetric);
            return true;
        }
        return false;
    }
}
