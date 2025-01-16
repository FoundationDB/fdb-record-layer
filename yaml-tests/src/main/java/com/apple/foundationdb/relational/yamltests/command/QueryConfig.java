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

package com.apple.foundationdb.relational.yamltests.command;

import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.recordlayer.ErrorCapturingResultSet;
import com.apple.foundationdb.relational.util.Assert;
import com.apple.foundationdb.relational.yamltests.CustomYamlConstructor;
import com.apple.foundationdb.relational.yamltests.Matchers;
import com.apple.foundationdb.relational.yamltests.YamlExecutionContext;
import com.apple.foundationdb.tuple.ByteArrayUtil2;
import com.github.difflib.text.DiffRow;
import com.github.difflib.text.DiffRowGenerator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opentest4j.AssertionFailedError;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;

import static com.apple.foundationdb.relational.yamltests.command.QueryCommand.reportTestFailure;

/**
 * A {@link QueryConfig} defines how the query is to be run and how the output of the query execution or the error
 * (in case of failure in query execution) is to be tested. To do so, it utilizes 2 methods
 * <ul>
 *     <li>{@code decorateQuery}: takes in the canonical query string and changes it as is needed to be run</li>
 *     <li>{@code checkResult}: checks for the result from the query execution.</li>
 *     <li>{@code checkError}: checks for the error.</li>
 * </ul>
 */
@SuppressWarnings({"PMD.GuardLogStatement", "PMD.AvoidCatchingThrowable"})
public abstract class QueryConfig {
    private static final Logger logger = LogManager.getLogger(QueryConfig.class);

    public static final String QUERY_CONFIG_RESULT = "result";
    public static final String QUERY_CONFIG_UNORDERED_RESULT = "unorderedResult";
    public static final String QUERY_CONFIG_EXPLAIN = "explain";
    public static final String QUERY_CONFIG_EXPLAIN_CONTAINS = "explainContains";
    public static final String QUERY_CONFIG_COUNT = "count";
    public static final String QUERY_CONFIG_ERROR = "error";
    public static final String QUERY_CONFIG_PLAN_HASH = "planHash";
    public static final String QUERY_CONFIG_NO_CHECKS = "noChecks";
    public static final String QUERY_CONFIG_MAX_ROWS = "maxRows";

    @Nullable private final Object value;
    private final int lineNumber;
    @Nonnull
    private final YamlExecutionContext executionContext;
    @Nullable private final String configName;

    private QueryConfig(@Nullable String configName, @Nullable Object value, int lineNumber, @Nonnull YamlExecutionContext executionContext) {
        this.configName = configName;
        this.value = value;
        this.lineNumber = lineNumber;
        this.executionContext = executionContext;
    }

    int getLineNumber() {
        return lineNumber;
    }

    @Nullable
    Object getVal() {
        return value;
    }

    @Nullable
    String getConfigName() {
        return configName;
    }

    String getValueString() {
        if (value instanceof byte[]) {
            return ByteArrayUtil2.loggable((byte[]) value);
        } else if (value != null) {
            return value.toString();
        } else {
            return "";
        }
    }

    String decorateQuery(@Nonnull String query) {
        return query;
    }

    final void checkResult(@Nonnull Object actual, @Nonnull String queryDescription) {
        try {
            checkResultInternal(actual, queryDescription);
        } catch (AssertionFailedError e) {
            throw executionContext.wrapContext(e,
                    () -> "‼️Check result failed in config at line " + getLineNumber(),
                    String.format("config [%s: %s] ", getConfigName(), getVal()), getLineNumber());
        } catch (Throwable e) {
            throw executionContext.wrapContext(e,
                    () -> "‼️Failed to test config at line " + getLineNumber(),
                    String.format("config [%s: %s] ", getConfigName(), getVal()), getLineNumber());
        }
    }

    final void checkError(@Nonnull SQLException actual, @Nonnull String queryDescription) {
        try {
            checkErrorInternal(actual, queryDescription);
        } catch (AssertionFailedError e) {
            throw executionContext.wrapContext(e,
                    () -> "‼️Check result failed in config at line " + getLineNumber(),
                    String.format("config [%s: %s] ", getConfigName(), getVal()), getLineNumber());
        } catch (Throwable e) {
            throw executionContext.wrapContext(e,
                    () -> "‼️Failed to test config at line " + getLineNumber(),
                    String.format("config [%s: %s] ", getConfigName(), getVal()), getLineNumber());
        }
    }

    abstract void checkResultInternal(@Nonnull Object actual, @Nonnull String queryDescription) throws SQLException;

    void checkErrorInternal(@Nonnull SQLException e, @Nonnull String queryDescription) throws SQLException {
        final var diffMessage = String.format("‼️ statement failed with the following error at line %s:%n" +
                "⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤%n" +
                "%s%n" +
                "⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤%n",
                getLineNumber(), e.getMessage());
        logger.error(diffMessage);
        throw e;

    }

    private static QueryConfig getCheckResultConfig(boolean isExpectedOrdered, @Nullable String configName,
                                                    @Nullable Object value, int lineNumber, @Nonnull YamlExecutionContext executionContext) {
        return new QueryConfig(configName, value, lineNumber, executionContext) {

            @Override
            void checkResultInternal(@Nonnull Object actual, @Nonnull String queryDescription) throws SQLException {
                logger.debug("⛳️ Matching results of query '{}'", queryDescription);
                try (RelationalResultSet resultSet = (RelationalResultSet)actual) {
                    final var matchResult = Matchers.matchResultSet(getVal(), resultSet, isExpectedOrdered);
                    if (!matchResult.getLeft().equals(Matchers.ResultSetMatchResult.success())) {
                        var toReport = "‼️ result mismatch at line " + getLineNumber() + ":\n" +
                                "⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤\n" +
                                Matchers.notNull(matchResult.getLeft().getExplanation(), "failure error message") + "\n" +
                                "⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤\n";
                        final var valueString = getValueString();
                        if (!valueString.isEmpty()) {
                            toReport += "↪ expected result:\n" +
                                    getValueString() + "\n";
                        }
                        if (matchResult.getRight() != null) {
                            toReport += "⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤\n" +
                                    "↩ actual result left to be matched:\n" +
                                    Matchers.notNull(matchResult.getRight(), "failure error actual result set");
                        }
                        reportTestFailure(toReport);
                    } else {
                        logger.debug("✅ results match!");
                    }
                }
            }
        };
    }

    private static QueryConfig getCheckExplainConfig(boolean isExact, @Nonnull String configName, @Nullable Object value,
                                                     int lineNumber, @Nonnull YamlExecutionContext executionContext) {
        return new QueryConfig(configName, value, lineNumber, executionContext) {

            @Override
            String decorateQuery(@Nonnull String query) {
                return "EXPLAIN " + query;
            }

            @SuppressWarnings({"PMD.CloseResource", "PMD.EmptyWhileStmt"}) // lifetime of autocloseable resource persists beyond method
            @Override
            void checkResultInternal(@Nonnull Object actual, @Nonnull String queryDescription) throws SQLException {
                logger.debug("⛳️ Matching plan for query '{}'", queryDescription);
                final var resultSet = (RelationalResultSet) actual;
                resultSet.next();
                final var actualPlan = resultSet.getString(1);
                var success = isExact ? getVal().equals(actualPlan) : actualPlan.contains((String) getVal());
                if (success) {
                    logger.debug("✅️ plan match!");
                } else {
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
                        final var diffMessage = String.format("‼️ plan mismatch at line %d:%n" +
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
        };
    }

    private static QueryConfig getCheckErrorConfig(@Nullable Object value, int lineNumber, @Nonnull YamlExecutionContext executionContext) {
        return new QueryConfig(QUERY_CONFIG_ERROR, value, lineNumber, executionContext) {

            @Override
            void checkResultInternal(@Nonnull Object actual, @Nonnull String queryDescription) throws SQLException {
                Matchers.ResultSetPrettyPrinter resultSetPrettyPrinter = new Matchers.ResultSetPrettyPrinter();
                if (actual instanceof ErrorCapturingResultSet) {
                    Matchers.printRemaining((ErrorCapturingResultSet) actual, resultSetPrettyPrinter);
                    reportTestFailure(String.format(
                            "‼️ expecting statement to throw an error, however it returned a result set at line %d%n" +
                                    "⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤%n" +
                                    "%s%n" +
                                    "⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤%n",
                            getLineNumber(), resultSetPrettyPrinter));
                } else if (actual instanceof Integer) {
                    reportTestFailure(String.format(
                            "‼️ expecting statement to throw an error, however it returned a count at line %d%n" +
                                    "⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤%n" +
                                    "%s%n" +
                                    "⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤%n",
                            getLineNumber(), actual));
                } else {
                    reportTestFailure(String.format("‼️ unexpected query result of type '%s' (expecting '%s') at line %d%n",
                            actual.getClass().getSimpleName(),
                            ErrorCapturingResultSet.class.getSimpleName(),
                            getLineNumber()));
                }
            }

            @Override
            void checkErrorInternal(@Nonnull SQLException e, @Nonnull String queryDescription) {
                logger.debug("⛳️ Checking error code resulted from executing '{}'", queryDescription);
                if (!e.getSQLState().equals(getVal())) {
                    reportTestFailure(String.format("‼️ expecting '%s' error code, got '%s' instead at line %d!",
                            getVal(), e.getSQLState(), getLineNumber()), e);
                } else {
                    logger.debug("✅ error codes '{}' match!", getVal());
                }
            }
        };
    }

    private static QueryConfig getCheckCountConfig(@Nullable Object value, int lineNumber, @Nonnull YamlExecutionContext executionContext) {
        return new QueryConfig(QUERY_CONFIG_COUNT, value, lineNumber, executionContext) {

            @Override
            void checkResultInternal(@Nonnull Object actual, @Nonnull String queryDescription) {
                logger.debug("⛳️ Matching count of update query '{}'", queryDescription);
                if (!Matchers.matches(getVal(), actual)) {
                    reportTestFailure(String.format("‼️ Expected count value %d, but got %d at line %d",
                            (Integer) getVal(), (Integer) actual, getLineNumber()));
                } else {
                    logger.debug("✅ Results match!");
                }
            }
        };
    }

    private static QueryConfig getCheckPlanHashConfig(@Nullable Object value, int lineNumber, @Nonnull YamlExecutionContext executionContext) {
        return new QueryConfig(QUERY_CONFIG_PLAN_HASH, value, lineNumber, executionContext) {

            @Override
            String decorateQuery(@Nonnull String query) {
                return "EXPLAIN " + query;
            }

            @SuppressWarnings("PMD.CloseResource") // lifetime of autocloseable persists beyond method
            @Override
            void checkResultInternal(@Nonnull Object actual, @Nonnull String queryDescription) throws SQLException {
                logger.debug("⛳️ Matching plan hash of query '{}'", queryDescription);
                final var resultSet = (RelationalResultSet) actual;
                resultSet.next();
                final var actualPlanHash = resultSet.getInt(2);
                if (!Matchers.matches(getVal(), actualPlanHash)) {
                    reportTestFailure("‼️ Incorrect plan hash: expecting " + getVal() + " got " + actualPlanHash + " at line " + getLineNumber());
                }
                logger.debug("✅️ Plan hash matches!");
            }
        };
    }

    public static QueryConfig getNoCheckConfig(int lineNumber, @Nonnull YamlExecutionContext executionContext) {
        return new QueryConfig(QUERY_CONFIG_NO_CHECKS, null, lineNumber, executionContext) {
            @SuppressWarnings("PMD.CloseResource") // lifetime of autocloseable persists beyond method
            @Override
            void checkResultInternal(@Nonnull Object actual, @Nonnull String queryDescription) throws SQLException {
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
        };
    }

    private static QueryConfig getMaxRowConfig(@Nonnull Object value, int lineNumber, @Nonnull YamlExecutionContext executionContext) {
        return new QueryConfig(QUERY_CONFIG_MAX_ROWS, value, lineNumber, executionContext) {
            @Override
            void checkResultInternal(@Nonnull Object actual, @Nonnull String queryDescription) throws SQLException {
                Assert.failUnchecked("No results to check on a maxRow config");
            }
        };
    }

    public static QueryConfig parse(@Nonnull Object object, @Nonnull YamlExecutionContext executionContext) {
        final var configEntry = Matchers.notNull(Matchers.firstEntry(object, "query configuration"), "query configuration");
        final var linedObject = CustomYamlConstructor.LinedObject.cast(configEntry.getKey(), () -> "Invalid config key-value pair: " + configEntry);
        final var lineNumber = linedObject.getLineNumber();
        try {
            final var key = Matchers.notNull(Matchers.string(linedObject.getObject(), "query configuration"), "query configuration");
            final var value = Matchers.notNull(configEntry, "query configuration").getValue();
            if (QUERY_CONFIG_COUNT.equals(key)) {
                return getCheckCountConfig(value, lineNumber, executionContext);
            } else if (QUERY_CONFIG_ERROR.equals(key)) {
                return getCheckErrorConfig(value, lineNumber, executionContext);
            } else if (QUERY_CONFIG_EXPLAIN.equals(key)) {
                return getCheckExplainConfig(true, key, value, lineNumber, executionContext);
            } else if (QUERY_CONFIG_EXPLAIN_CONTAINS.equals(key)) {
                return getCheckExplainConfig(false, key, value, lineNumber, executionContext);
            } else if (QUERY_CONFIG_PLAN_HASH.equals(key)) {
                return getCheckPlanHashConfig(value, lineNumber, executionContext);
            } else if (key.contains(QUERY_CONFIG_RESULT)) {
                return getCheckResultConfig(true, key, value, lineNumber, executionContext);
            } else if (QUERY_CONFIG_UNORDERED_RESULT.equals(key)) {
                return getCheckResultConfig(false, key, value, lineNumber, executionContext);
            } else if (QUERY_CONFIG_MAX_ROWS.equals(key)) {
                return getMaxRowConfig(value, lineNumber, executionContext);
            } else {
                Assert.failUnchecked("‼️ '%s' is not a valid configuration");
            }
            // should not happen
            return null;
        } catch (Exception e) {
            throw executionContext.wrapContext(e, () -> "‼️ Error parsing the query config at line " + lineNumber, "config", lineNumber);
        }
    }
}
