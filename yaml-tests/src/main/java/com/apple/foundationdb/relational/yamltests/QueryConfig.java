/*
 * QueryConfig.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2024 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.relational.recordlayer.ErrorCapturingResultSet;
import com.apple.foundationdb.relational.util.Assert;

import com.github.difflib.text.DiffRow;
import com.github.difflib.text.DiffRowGenerator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.apple.foundationdb.relational.yamltests.QueryCommand.reportTestFailure;

/**
 * {@link QueryConfig} defines how the output of the query execution or the error (in case of failure in query
 * execution) is to be tested. To do so, it utilizes 2 methods
 * <ul>
 *     <li>{@code checkResult}: checks for the result from the query execution.</li>
 *     <li>{@code checkError}: checks for the error.</li>
 * </ul>
 */
@SuppressWarnings({"PMD.GuardLogStatement"})
public abstract class QueryConfig {
    private static final Logger logger = LogManager.getLogger(QueryConfig.class);

    static final String QUERY_CONFIG_RESULT = "result";
    static final String QUERY_CONFIG_UNORDERED_RESULT = "unorderedResult";
    static final String QUERY_CONFIG_EXPLAIN = "explain";
    static final String QUERY_CONFIG_EXPLAIN_CONTAINS = "explainContains";
    static final String QUERY_CONFIG_COUNT = "count";
    static final String QUERY_CONFIG_ERROR = "error";
    static final String QUERY_CONFIG_PLAN_HASH = "planHash";

    private static final Map<String, QueryConfig> configsMap = new HashMap<>();

    abstract void checkResult(@Nonnull Object actual, @Nonnull String query,
                              @Nonnull QueryCommand.QueryConfigWithValue configWithValue) throws SQLException;

    abstract void checkError(@Nonnull SQLException e, @Nonnull String query, @Nonnull QueryCommand.QueryConfigWithValue configWithValue) throws SQLException;

    static {
        // checks the result set of the query, maintaining the order.
        configsMap.put(QUERY_CONFIG_RESULT, getCheckResultConfig(true));
        // checks the result set of the query, not actually looking at the order.
        configsMap.put(QUERY_CONFIG_UNORDERED_RESULT, getCheckResultConfig(false));
        // checks the 'explain' of the query, the result should be complete match.
        configsMap.put(QUERY_CONFIG_EXPLAIN, getCheckExplainConfig(true));
        // checks the 'explain' of the query, the result can be a partial match.
        configsMap.put(QUERY_CONFIG_EXPLAIN_CONTAINS, getCheckExplainConfig(false));
        // checks the number of rows affected by the query.
        configsMap.put(QUERY_CONFIG_COUNT, getCheckCountConfig());
        // checks the error code with which the query should fail.
        configsMap.put(QUERY_CONFIG_ERROR, getCheckErrorConfig());
        // checks for the plan hash of the query.
        configsMap.put(QUERY_CONFIG_PLAN_HASH, getCheckPlanHashConfig());
        // no check. just consume the result.
        configsMap.put(null, getNoCheckConfig());
    }

    private static void defaultResponseOnError(SQLException e, int lineNumber) throws SQLException {
        final var diffMessage = String.format("‼️ statement failed with the following error at line %s:%n" +
                "⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤%n" +
                "%s%n" +
                "⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤%n",
                lineNumber, e.getMessage());
        logger.error(diffMessage);
        throw e;
    }

    private static QueryConfig getCheckResultConfig(boolean isExpectedOrdered) {
        return new QueryConfig() {
            @Override
            void checkResult(@Nonnull Object actual, @Nonnull String query,
                             @Nonnull QueryCommand.QueryConfigWithValue configWithValue) throws SQLException {
                logger.debug("⛳️ Matching results of query '{}'", query);
                final var resultSet = (RelationalResultSet) actual;
                final var matchResult = Matchers.matchResultSet(configWithValue.getVal(), resultSet, isExpectedOrdered);
                if (!matchResult.equals(Matchers.ResultSetMatchResult.success())) {
                    reportTestFailure(String.format("‼️ result mismatch at line %d:%n" +
                            "⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤%n" +
                            Matchers.notNull(matchResult.getExplanation(), "failure error message") + "%n" +
                            "⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤%n" +
                            "↪ expected result:%n" +
                            configWithValue.valueString() + "%n" +
                            "⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤%n" +
                            "↩ actual result:%n" +
                            Matchers.notNull(matchResult.getResultSetPrinter(), "failure error actual result set"),
                            configWithValue.getLineNumber()));
                } else {
                    logger.debug("✅ results match!");
                }
            }

            @Override
            void checkError(@Nonnull SQLException e, @Nonnull String query, @Nonnull QueryCommand.QueryConfigWithValue configWithValue) throws SQLException {
                defaultResponseOnError(e, configWithValue.getLineNumber());
            }
        };
    }

    private static QueryConfig getCheckExplainConfig(boolean isExact) {
        return new QueryConfig() {
            @Override
            void checkResult(@Nonnull Object actual, @Nonnull String query,
                             @Nonnull QueryCommand.QueryConfigWithValue configWithValue) throws SQLException {
                logger.debug("⛳️ Matching plan for query '{}'", query);
                final var resultSet = (RelationalResultSet) actual;
                resultSet.next();
                final var actualPlan = resultSet.getString(1);
                var success = isExact ? configWithValue.getVal().equals(actualPlan) : actualPlan.contains((String) configWithValue.getVal());
                if (success) {
                    logger.debug("✅️ plan match!");
                } else {
                    final var expectedPlan = configWithValue.valueString();
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
                    final var execCtx = configWithValue.getExecutionContext();
                    if (isExact && execCtx.shouldCorrectExplains()) {
                        if (!execCtx.correctExplain(configWithValue.getLineNumber() - 1, actualPlan)) {
                            reportTestFailure("‼️ Cannot correct explain plan at line " + configWithValue.getLineNumber());
                        } else {
                            logger.debug("⭐️ Successfully replaced plan at line " + configWithValue.getLineNumber());
                        }
                    } else {
                        final var diffMessage = String.format("‼️ plan mismatch at line %d:%n" +
                                "⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤%n" +
                                planDiffs +
                                "⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤%n" +
                                "↪ expected plan %s:%n" +
                                configWithValue.valueString() + "%n" +
                                "⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤%n" +
                                "↩ actual plan:%n" +
                                actualPlan,
                                configWithValue.getLineNumber(), (!isExact ? "fragment" : ""));
                        reportTestFailure(diffMessage);
                    }
                }
            }

            @Override
            void checkError(@Nonnull SQLException e, @Nonnull String query, @Nonnull QueryCommand.QueryConfigWithValue configWithValue) throws SQLException {
                defaultResponseOnError(e, configWithValue.getLineNumber());
            }
        };
    }

    private static QueryConfig getCheckErrorConfig() {
        return new QueryConfig() {
            @Override
            void checkResult(@Nonnull Object actual, @Nonnull String query,
                             @Nonnull QueryCommand.QueryConfigWithValue configWithValue) throws SQLException {
                Matchers.ResultSetPrettyPrinter resultSetPrettyPrinter = new Matchers.ResultSetPrettyPrinter();
                if (actual instanceof ErrorCapturingResultSet) {
                    Matchers.printRemaining((ErrorCapturingResultSet) actual, resultSetPrettyPrinter);
                    reportTestFailure(String.format(
                            "‼️ expecting statement to throw an error, however it returned a result set at line %d%n" +
                                    "⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤%n" +
                                    "%s%n" +
                                    "⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤%n",
                            configWithValue.getLineNumber(), resultSetPrettyPrinter));
                } else if (actual instanceof Integer) {
                    reportTestFailure(String.format(
                            "‼️ expecting statement to throw an error, however it returned a count at line %d%n" +
                                    "⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤%n" +
                                    "%s%n" +
                                    "⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤%n",
                            configWithValue.getLineNumber(), actual));
                } else {
                    reportTestFailure(String.format("‼️ unexpected query result of type '%s' (expecting '%s') at line %d%n",
                            actual.getClass().getSimpleName(),
                            ErrorCapturingResultSet.class.getSimpleName(),
                            configWithValue.getLineNumber()));
                }
            }

            @Override
            void checkError(@Nonnull SQLException e, @Nonnull String query, @Nonnull QueryCommand.QueryConfigWithValue configWithValue) {
                logger.debug("⛳️ checking error code resulted from executing '{}'", query);
                if (!e.getSQLState().equals(configWithValue.getVal())) {
                    reportTestFailure(String.format("‼️ expecting '%s' error code, got '%s' instead at line %d!",
                            configWithValue.getVal(), e.getSQLState(), configWithValue.getLineNumber()), e);
                } else {
                    logger.debug("✅ error codes '{}' match!", configWithValue.getVal());
                }
            }
        };
    }

    private static QueryConfig getCheckCountConfig() {
        return new QueryConfig() {
            @Override
            void checkResult(@Nonnull Object actual, @Nonnull String query,
                             @Nonnull QueryCommand.QueryConfigWithValue configWithValue) {
                logger.debug("⛳️ Matching count of update query '{}'", query);
                if (!Matchers.matches(configWithValue.getVal(), actual)) {
                    reportTestFailure(String.format("‼️ Expected count value %d, but got %d at line %d",
                            (Integer) configWithValue.getVal(), (Integer) actual, configWithValue.getLineNumber()));
                } else {
                    logger.debug("✅ results match!");
                }
            }

            @Override
            void checkError(@Nonnull SQLException e, @Nonnull String query, @Nonnull QueryCommand.QueryConfigWithValue configWithValue) throws SQLException {
                defaultResponseOnError(e, configWithValue.getLineNumber());
            }
        };
    }

    private static QueryConfig getCheckPlanHashConfig() {
        return new QueryConfig() {
            @Override
            void checkResult(@Nonnull Object actual, @Nonnull String query,
                             @Nonnull QueryCommand.QueryConfigWithValue configWithValue) throws SQLException {
                logger.debug("⛳️ Matching plan hash of query '{}'", query);
                final var resultSet = (RelationalResultSet) actual;
                if (!Matchers.matches(configWithValue.getVal(), resultSet.getPlanHash())) {
                    reportTestFailure("‼️ Incorrect plan hash at line " + configWithValue.getLineNumber());
                }
                logger.debug("✅️ plan hash matches!");
            }

            @Override
            void checkError(@Nonnull SQLException e, @Nonnull String query, @Nonnull QueryCommand.QueryConfigWithValue configWithValue) throws SQLException {
                defaultResponseOnError(e, configWithValue.getLineNumber());
            }
        };
    }

    private static QueryConfig getNoCheckConfig() {
        return new QueryConfig() {
            @Override
            void checkResult(@Nonnull Object actual, @Nonnull String query,
                             @Nonnull QueryCommand.QueryConfigWithValue configWithValue) throws SQLException {
                if (actual instanceof RelationalResultSet) {
                    final var resultSet = (RelationalResultSet) actual;
                    // slurp
                    boolean valid = true;
                    while (valid) { // suppress check style
                        valid = ((RelationalResultSet) actual).next();
                    }
                    resultSet.close();
                }
            }

            @Override
            void checkError(@Nonnull SQLException e, @Nonnull String query, @Nonnull QueryCommand.QueryConfigWithValue configWithValue) throws SQLException {
                defaultResponseOnError(e, configWithValue.getLineNumber());
            }
        };
    }

    @Nonnull
    public static QueryConfig resolve(@Nullable final Object key) {
        final String keyString = key instanceof String ? (String) key : null;
        Assert.thatUnchecked(configsMap.containsKey(keyString), String.format("‼️ '%s' is not a valid configuration, available configuration(s): '%s'",
                key, String.join(",", configsMap.keySet())));
        return configsMap.get(keyString);
    }
}
