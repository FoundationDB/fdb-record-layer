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

package com.apple.foundationdb.relational.yamltests.command.queryconfigs;

import com.apple.foundationdb.record.query.plan.cascades.debug.BrowserHelper;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.yamltests.MaintainYamlTestConfig;
import com.apple.foundationdb.relational.yamltests.YamlMetricsMaintainer;
import com.apple.foundationdb.relational.yamltests.YamlReference;
import com.apple.foundationdb.relational.yamltests.YamlExecutionContext;
import com.apple.foundationdb.relational.yamltests.command.CommandUtil;
import com.apple.foundationdb.relational.yamltests.command.QueryCommand;
import com.apple.foundationdb.relational.yamltests.command.QueryConfig;
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
public class CheckExplainConfig extends QueryConfig {

    private static final Logger logger = LogManager.getLogger(CheckExplainConfig.class);
    private final YamlExecutionContext executionContext;
    private final boolean isExact;
    private final String blockName;

    public CheckExplainConfig(final String configName, final Object value, @Nonnull final YamlReference reference, final YamlExecutionContext executionContext, final boolean isExact, final String blockName) {
        super(configName, value, reference);
        this.executionContext = executionContext;
        this.isExact = isExact;
        this.blockName = blockName;
    }

    @Override
    protected String decorateQuery(@Nonnull String query) {
        return "EXPLAIN " + query;
    }

    @SuppressWarnings({"PMD.CloseResource", "PMD.EmptyWhileStmt"}) // lifetime of autocloseable resource persists beyond method
    @Override
    protected void checkResultInternal(@Nonnull String currentQuery, @Nonnull Object actual,
                                       @Nonnull String queryDescription, @Nonnull List<String> setups) throws SQLException {
        logger.debug("⛳️ Matching plan for query '{}'", queryDescription);
        final var resultSet = (RelationalResultSet) actual;
        resultSet.next();
        final var actualPlannerMetricsInfo = createPlannerMetricsInfo(resultSet);
        final var identifier = PlannerMetricsProto.Identifier.newBuilder()
                .setBlockName(blockName)
                .setQuery(currentQuery)
                .addAllSetups(setups)
                .build();
        final var expectedPlannerMetricsInfo = executionContext.getMetricsMaintainer().getMetrics(identifier);

        if (getVal() == null) {
            addExplainAndMetrics(identifier, actualPlannerMetricsInfo);
        } else if (!isExact) {
            checkExplainContains(queryDescription, actualPlannerMetricsInfo, expectedPlannerMetricsInfo);
        } else {
            checkExplainAndMetrics(queryDescription, identifier, actualPlannerMetricsInfo, expectedPlannerMetricsInfo);
        }
    }

    private static PlannerMetricsProto.Info createPlannerMetricsInfo(@Nonnull final RelationalResultSet resultSet) throws SQLException {
        final var builder = PlannerMetricsProto.Info.newBuilder();
        final var plan = resultSet.getString(1);
        if (plan == null) {
            QueryCommand.reportTestFailure("‼️ EXPLAIN result is missing the plan string");
        } else {
            builder.setExplain(plan);
        }
        final var dot = resultSet.getString(3);
        if (dot == null) {
            QueryCommand.reportTestFailure("‼️ EXPLAIN result is missing the DOT representation");
        } else {
            builder.setDot(dot);
        }
        final var metricsStruct = resultSet.getStruct(6);
        if (metricsStruct != null) {
            final var taskCount = metricsStruct.getLong(1);
            Verify.verify(taskCount > 0);
            final var taskTotalTimeInNs = metricsStruct.getLong(2);
            Verify.verify(taskTotalTimeInNs > 0);
            builder.setCountersAndTimers(PlannerMetricsProto.CountersAndTimers.newBuilder()
                    .setTaskCount(taskCount)
                    .setTaskTotalTimeNs(taskTotalTimeInNs)
                    .setTransformCount(metricsStruct.getLong(3))
                    .setTransformTimeNs(metricsStruct.getLong(4))
                    .setTransformYieldCount(metricsStruct.getLong(5))
                    .setInsertTimeNs(metricsStruct.getLong(6))
                    .setInsertNewCount(metricsStruct.getLong(7))
                    .setInsertReusedCount(metricsStruct.getLong(8)));
        }
        return builder.build();
    }

    private void addExplainAndMetrics(
            @Nonnull final PlannerMetricsProto.Identifier identifier,
            @Nonnull final PlannerMetricsProto.Info actualPlannerMetricsInfo) {
        try {
            executionContext.getFilesMaintainer().addExplain(getReference(), actualPlannerMetricsInfo.getExplain());
        } catch (Throwable throwable) {
            throw YamlExecutionContext.wrapContext(throwable, () -> "‼️ Cannot add explain", QUERY_CONFIG_EXPLAIN, getReference());
        }
        logger.debug(() -> "⭐️ Successfully added plan at " + getReference());
        recordMetrics(identifier, actualPlannerMetricsInfo, true);
    }

    private void checkExplainContains(@Nonnull final String queryDescription,
                                       @Nonnull final PlannerMetricsProto.Info actualPlannerMetricsInfo,
                                       @Nullable final PlannerMetricsProto.Info expectedPlannerMetricsInfo) {
        final var actualPlan = actualPlannerMetricsInfo.getExplain();
        if (actualPlan.contains(Objects.requireNonNull((String) getVal()))) {
            logger.debug("✅️ plan fragment match!");
        } else {
            showPlanDiffIfNeeded(queryDescription, actualPlannerMetricsInfo, expectedPlannerMetricsInfo);
            calcPlanDiffAndReportFailure(actualPlan);
        }
    }

    private void showPlanDiffIfNeeded(@Nonnull final String queryDescription,
                                      @Nonnull final PlannerMetricsProto.Info actualPlannerMetricsInfo,
                                      @Nullable final PlannerMetricsProto.Info expectedPlannerMetricsInfo) {
        if (!executionContext.shouldShowPlanOnDiff()) {
            return;
        }
        final var actualDot = actualPlannerMetricsInfo.getDot();
        final var expectedDot = expectedPlannerMetricsInfo == null ? null : expectedPlannerMetricsInfo.getDot();
        if (!actualDot.isEmpty() && expectedDot != null && !expectedDot.isEmpty()) {
            BrowserHelper.browse("/showPlanDiff.html",
                    ImmutableMap.of("$SQL", queryDescription,
                            "$DOT_EXPECTED", expectedDot,
                            "$DOT_ACTUAL", actualDot));
        }
    }

    private void calcPlanDiffAndReportFailure(@Nonnull final String actualPlan) {
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
        final var diffMessage = String.format(Locale.ROOT, "‼️ plan mismatch at %s:%n" +
                "⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤%n%s" +
                "⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤%n" +
                "↪ expected plan %s:%n%s%n" +
                "⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤%n" +
                "↩ actual plan:%n%s",
                getReference(), planDiffs, (!isExact ? "fragment" : ""), getValueString(), actualPlan);
        QueryCommand.reportTestFailure(diffMessage);
    }

    private void checkExplainAndMetrics(@Nonnull final String queryDescription,
                                        @Nonnull final PlannerMetricsProto.Identifier identifier,
                                        @Nonnull final PlannerMetricsProto.Info actualPlannerMetricsInfo,
                                        @Nullable final PlannerMetricsProto.Info expectedPlannerMetricsInfo) {
        final var actualPlan = actualPlannerMetricsInfo.getExplain();
        var explainIsChanged = false;
        if (Objects.requireNonNull(getVal()).equals(actualPlan)) {
            logger.debug("✅️ plan match!");
        } else {
            showPlanDiffIfNeeded(queryDescription, actualPlannerMetricsInfo, expectedPlannerMetricsInfo);
            if (executionContext.shouldCorrectExplains() || executionContext.shouldAddExplains()) {
                try {
                    executionContext.getFilesMaintainer().correctExplain(getReference(), actualPlan);
                } catch (Throwable throwable) {
                    throw YamlExecutionContext.wrapContext(throwable, () -> "‼️ Cannot correct metrics", QUERY_CONFIG_EXPLAIN, getReference());
                }
                explainIsChanged = true;
                logger.debug(() -> "⭐️ Successfully replaced plan at " + getReference());
            } else {
                calcPlanDiffAndReportFailure(actualPlan);
            }
        }

        // No actual metrics to compare with
        if (!actualPlannerMetricsInfo.hasCountersAndTimers()) {
            // In this case, if there are existing metrics and the plan is changed -> make sure to only update the plan
            if (explainIsChanged && expectedPlannerMetricsInfo != null) {
                correctExplainAndRecordMetrics(identifier, expectedPlannerMetricsInfo, actualPlannerMetricsInfo.getExplain());
            }
        // actual metrics are there, but existing metrics are not found -> case of new query / changed query
        } else if (expectedPlannerMetricsInfo == null) {
            // If we CANNOT correct metrics, error out since metrics are ALWAYS expected
            if (!executionContext.shouldCorrectMetrics()) {
                QueryCommand.reportTestFailure("‼️ No planner metrics at " + getReference());
            }
            // else, add the actual
            recordMetrics(identifier, actualPlannerMetricsInfo, true);
            logger.debug(() -> "⭐️ Successfully inserted new planner metrics at " + getReference());
        // both actual and existing are there, but there are differences
        } else if (areMetricsDifferent(expectedPlannerMetricsInfo, actualPlannerMetricsInfo)) {
            // If we CANNOT correct metrics, error out
            if (!executionContext.shouldCorrectMetrics()) {
                QueryCommand.reportTestFailure("‼️ Planner metrics have changed for " + getReference());
            }
            // else, add the actual
            recordMetrics(identifier, actualPlannerMetricsInfo, true);
            logger.debug(() -> "⭐️ Successfully updated planner metrics at " + getReference());
        // both actual and existing are there, and they match. However, the plan has some changes
        } else if (explainIsChanged) {
            correctExplainAndRecordMetrics(identifier, expectedPlannerMetricsInfo, actualPlannerMetricsInfo.getExplain());
        // Else, just preserve the existing metrics that we have gotten for this query
        } else {
            recordMetrics(identifier, expectedPlannerMetricsInfo, false);
        }
    }

    private void correctExplainAndRecordMetrics(@Nonnull final PlannerMetricsProto.Identifier identifier,
                               @Nonnull final PlannerMetricsProto.Info info, @Nonnull String plan) {
        final var expectedMetricsWithActualPlan = info.toBuilder().setExplain(plan).build();
        recordMetrics(identifier, expectedMetricsWithActualPlan, true);
    }

    private void recordMetrics(@Nonnull final PlannerMetricsProto.Identifier identifier,
                               @Nonnull final PlannerMetricsProto.Info info,
                               final boolean dirty) {
        try {
            executionContext.getMetricsMaintainer().putMetrics(identifier, getReference(), info);
            if (dirty) {
                executionContext.getMetricsMaintainer().markDirty();
            }
        } catch (Throwable throwable) {
            throw YamlExecutionContext.wrapContext(throwable, () -> "‼️ Cannot put metrics", QUERY_CONFIG_EXPLAIN, getReference());
        }
    }

    private boolean areMetricsDifferent(@Nonnull final PlannerMetricsProto.Info expectedPlannerMetricsInfo,
                                        @Nonnull final PlannerMetricsProto.Info actualPlannerMetricsInfo) {
        final var expectedCountersAndTimers = expectedPlannerMetricsInfo.getCountersAndTimers();
        final var actualCountersAndTimers = actualPlannerMetricsInfo.getCountersAndTimers();
        final var metricsDescriptor = expectedCountersAndTimers.getDescriptorForType();
        boolean different = false;
        for (String fieldName : YamlMetricsMaintainer.TRACKED_METRIC_FIELDS) {
            // Check each metric. Do NOT short-circuit because we want to log any metrics
            // that have changed (a side effect of isMetricDifferent)
            different |= isMetricDifferent(expectedCountersAndTimers, actualCountersAndTimers,
                    metricsDescriptor.findFieldByName(fieldName), getReference());
        }
        return different;
    }

    private static boolean isMetricDifferent(@Nonnull final PlannerMetricsProto.CountersAndTimers expected,
                                             @Nonnull final PlannerMetricsProto.CountersAndTimers actual,
                                             @Nonnull final Descriptors.FieldDescriptor fieldDescriptor,
                                             @Nonnull final YamlReference reference) {
        final long expectedMetric = (long)expected.getField(fieldDescriptor);
        final long actualMetric = (long)actual.getField(fieldDescriptor);
        if (expectedMetric != actualMetric) {
            if (logger.isWarnEnabled()) {
                logger.warn("‼️ metric {} differs; ref = {}; expected = {}; actual = {}",
                        fieldDescriptor.getName(), reference, expectedMetric, actualMetric);
            }
            return true;
        }
        return false;
    }
}
