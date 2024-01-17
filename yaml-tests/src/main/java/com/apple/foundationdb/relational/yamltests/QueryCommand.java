/*
 * QueryCommand.java
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

import com.apple.foundationdb.record.query.plan.cascades.debug.Debugger;
import com.apple.foundationdb.record.query.plan.debug.DebuggerWithSymbolTables;
import com.apple.foundationdb.tuple.ByteArrayUtil2;
import com.apple.foundationdb.relational.api.Continuation;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.cli.CliCommandFactory;
import com.apple.foundationdb.relational.cli.formatters.ResultSetFormat;
import com.apple.foundationdb.relational.recordlayer.ContinuationImpl;
import com.apple.foundationdb.relational.recordlayer.ErrorCapturingResultSet;
import com.apple.foundationdb.relational.util.Assert;
import com.apple.foundationdb.relational.util.SpotBugsSuppressWarnings;

import com.github.difflib.text.DiffRow;
import com.github.difflib.text.DiffRowGenerator;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Assertions;
import org.opentest4j.AssertionFailedError;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

@SuppressWarnings({"PMD.GuardLogStatement"})
// It already is, but PMD is confused and reporting error in unrelated locations.
public class QueryCommand extends Command {
    private static final Logger logger = LogManager.getLogger(QueryCommand.class);

    public static class ExplainMismatchError extends AssertionFailedError {

        private static final long serialVersionUID = 1L;

        @Nonnull
        private final String expectedPlan;
        @Nonnull
        private final String actualPlan;

        public ExplainMismatchError(@Nonnull final String message, @Nonnull final String expectedPlan, @Nonnull final String actualPlan) {
            super(message);
            this.expectedPlan = expectedPlan;
            this.actualPlan = actualPlan;
        }

        @Nonnull
        public String getExpectedPlan() {
            return expectedPlan;
        }

        @Nonnull
        public String getActualPlan() {
            return actualPlan;
        }
    }

    public enum QueryConfig {
        RESULT("result"),
        UNORDERED_RESULT("unorderedResult"),
        EXPLAIN("explain"),
        EXPLAIN_CONTAINS("explainContains"),
        COUNT("count"),
        ERROR("error"),
        PLAN_HASH("planHash");

        @Nonnull
        private final String label;

        QueryConfig(@Nonnull final String label) {
            this.label = label;
        }

        public String getLabel() {
            return this.label;
        }

        @SpotBugsSuppressWarnings(value = "NP_NONNULL_RETURN_VIOLATION", justification = "should never happen, fail throws")
        @Nonnull
        public static QueryConfig resolve(@Nonnull final String label) throws Exception {
            final var maybeConfig = Arrays.stream(values()).filter(val -> val.label.equals(label)).findFirst();
            if (maybeConfig.isPresent()) {
                return maybeConfig.get();
            }
            Assert.fail(String.format("‼️ '%s' is not a valid configuration, available configuration(s): '%s'",
                    label, Arrays.stream(values()).map(v -> v.label).collect(Collectors.joining(","))));
            return null;
        }
    }

    private static class QueryConfigWithValue {
        private final QueryConfig config;
        private final Object val;
        private final int lineNumber;

        QueryConfigWithValue(@Nonnull QueryConfig config, @Nonnull Object val, int lineNumber) {
            this.config = config;
            this.val = val;
            this.lineNumber = lineNumber;
        }

        public String valueString() {
            if (val instanceof byte[]) {
                return ByteArrayUtil2.loggable((byte[]) val);
            } else {
                return val.toString();
            }
        }
    }

    private void logAndThrowUnexpectedException(SQLException e, int lineNumber) {
        final var diffMessage = String.format("‼️ statement failed with the following error at line %s:%n" +
                        "⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤%n" +
                        "%s%n" +
                        "⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤%n",
                lineNumber, e.getMessage());
        logger.error(diffMessage);
        throw new RelationalException(e).toUncheckedWrappedException();
    }

    private void executeWithNoConfig(@Nonnull String queryString, int lineNumber, @Nonnull final CliCommandFactory factory)
            throws Exception {
        final var queryResults = executeQuery(queryString, lineNumber, factory, null);
        if (queryResults instanceof RelationalResultSet) {
            final var resultSet = (RelationalResultSet) queryResults;
            // slurp
            boolean valid = true;
            while (valid) { // suppress check style
                valid = ((RelationalResultSet) queryResults).next();
            }
            resultSet.close();
        }
    }

    private QueryConfigWithValue getQueryConfigWithValue(Object config) throws Exception {
        final var queryConfigAndValue = Matchers.firstEntry(config, "query configuration");
        final var queryConfigLinedObject = ((CustomYamlConstructor.LinedObject) Matchers.notNull(queryConfigAndValue, "query configuration").getKey());
        final var queryConfig = QueryConfig.resolve(
                Matchers.notNull(Matchers.string(queryConfigLinedObject.getObject(), "query configuration"), "query configuration"));
        final var configVal = Matchers.notNull(queryConfigAndValue, "query configuration").getValue();
        return new QueryConfigWithValue(queryConfig, configVal, queryConfigLinedObject.getStartMark().getLine() + 1);
    }

    private String appendWithContinuationIfPresent(String queryString, Continuation continuation) {
        String currentQuery = queryString;
        if (!currentQuery.isEmpty() && currentQuery.charAt(currentQuery.length() - 1) == ';') {
            currentQuery = currentQuery.substring(0, currentQuery.length() - 1);
        }
        if (continuation instanceof ContinuationImpl) {
            currentQuery += String.format(" with continuation b64'%s'", ResultSetFormat.formatContinuation(continuation));
        }
        return currentQuery;
    }

    private void checkCount(Object actual, QueryConfigWithValue queryConfigWithValue, String query) throws Exception {
        logger.debug("⛳️ Matching count of update query '{}'", query);
        if (!Matchers.matches(queryConfigWithValue.val, actual)) {
            reportTestFailure(String.format("‼️ Expected count value %d, but got %d at line %d",
                    (Integer) queryConfigWithValue.val, (Integer) actual, queryConfigWithValue.lineNumber));
        } else {
            logger.debug("✅ results match!");
        }
    }

    private Continuation checkForResult(Object queryResults, QueryConfigWithValue queryConfigWithValue, String query) throws Exception {
        logger.debug("⛳️ Matching results of query '{}'", query);
        final var resultSet = (RelationalResultSet) queryResults;
        final var matchResult = Matchers.matchResultSet(queryConfigWithValue.val, resultSet, queryConfigWithValue.config != QueryConfig.UNORDERED_RESULT);
        if (!matchResult.equals(Matchers.ResultSetMatchResult.success())) {
            reportTestFailure(String.format("‼️ result mismatch at line %d:%n" +
                            "⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤%n" +
                            Matchers.notNull(matchResult.getExplanation(), "failure error message") + "%n" +
                            "⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤%n" +
                            "↪ expected result:%n" +
                            queryConfigWithValue.valueString() + "%n" +
                            "⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤%n" +
                            "↩ actual result:%n" +
                            Matchers.notNull(matchResult.getResultSetPrinter(), "failure error actual result set"),
                            queryConfigWithValue.lineNumber));
        } else {
            logger.debug("✅ results match!");
        }
        return resultSet.getContinuation();
    }

    private void checkExplain(@Nonnull Object queryResults,
                              @Nonnull final QueryConfigWithValue queryConfigWithValue,
                              @Nonnull final String query) throws Exception {
        logger.debug("⛳️ Matching plan for query '{}'", query);
        final var resultSet = (RelationalResultSet) queryResults;
        resultSet.next();
        String actualPlan = resultSet.getString(1);
        boolean success = false;
        if (queryConfigWithValue.config == QueryConfig.EXPLAIN) {
            success = queryConfigWithValue.val.equals(actualPlan);
        } else if (queryConfigWithValue.config == QueryConfig.EXPLAIN_CONTAINS) {
            success = actualPlan.contains((String) queryConfigWithValue.val);
        }
        if (success) {
            logger.debug("✅️ plan match!");
        } else {
            final var expectedPlan = queryConfigWithValue.val.toString();
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
            final var diffMessage = String.format("‼️ plan mismatch at line %d:%n" +
                    "⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤%n" +
                    planDiffs +
                    "⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤%n" +
                    "↪ expected plan %s:%n" +
                    queryConfigWithValue.val + "%n" +
                    "⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤%n" +
                    "↩ actual plan:%n" +
                    actualPlan,
                    queryConfigWithValue.lineNumber,
                    (queryConfigWithValue.config == QueryConfig.EXPLAIN_CONTAINS ? " fragment" : ""));
            reportTestFailure(diffMessage, new ExplainMismatchError(diffMessage, expectedPlan, actualPlan));
        }

        // Should be all done, and should not have any more rows
        Assertions.assertFalse(resultSet.next(), "‼️ No more result expected at line " + queryConfigWithValue.lineNumber);
        Assertions.assertEquals(RelationalResultSet.NoNextRowReason.NO_MORE_ROWS, resultSet.noNextRowReason(), "‼️ No rows reason expected to be no more rows at line " + queryConfigWithValue.lineNumber);
    }

    private void checkForPlanHash(@Nonnull final Object queryResults, @Nonnull QueryConfigWithValue queryConfigWithValue,
                                  @Nonnull String query) throws Exception {
        logger.debug("⛳️ Matching plan hash of query '{}'", query);
        final var resultSet = (RelationalResultSet) queryResults;

        if (!Matchers.matches(queryConfigWithValue.val, resultSet.getPlanHash())) {
            reportTestFailure("‼️ Incorrect plan hash at line " + queryConfigWithValue.lineNumber);
        }
        logger.debug("✅️ plan hash matches!");
    }

    private void checkForError(Object queryResults, SQLException sqlException, String expectedErrorCode,
                                       int lineNumber, String query) throws Exception {
        if (queryResults != null) {
            Matchers.ResultSetPrettyPrinter resultSetPrettyPrinter = new Matchers.ResultSetPrettyPrinter();
            if (queryResults instanceof ErrorCapturingResultSet) {
                Matchers.printRemaining((ErrorCapturingResultSet) queryResults, resultSetPrettyPrinter);
                reportTestFailure(String.format(
                        "‼️ expecting statement to throw an error, however it returned a result set at line %d%n" +
                                "⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤%n" +
                                "%s%n" +
                                "⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤%n",
                        lineNumber, resultSetPrettyPrinter));
            } else if (queryResults instanceof Integer) {
                reportTestFailure(String.format(
                        "‼️ expecting statement to throw an error, however it returned a count at line %d%n" +
                                "⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤%n" +
                                "%s%n" +
                                "⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤%n",
                        lineNumber, queryResults));
            } else {
                reportTestFailure(String.format("‼️ unexpected query result of type '%s' (expecting '%s') at line %d%n",
                        queryResults.getClass().getSimpleName(),
                        ErrorCapturingResultSet.class.getSimpleName(),
                        lineNumber));
            }
        }
        logger.debug("⛳️ checking error code resulted from executing '{}'", query);
        if (sqlException == null) {
            if (expectedErrorCode != null) {
                reportTestFailure("‼️ unexpected NULL SQLException at line " + lineNumber + "!");
            }
        } else if (!sqlException.getSQLState().equals(expectedErrorCode)) {
            reportTestFailure(String.format("‼️ expecting '%s' error code, got '%s' instead at line %d!",
                    expectedErrorCode, sqlException.getSQLState(), lineNumber));
        } else {
            logger.debug("✅ error codes '{}' match!", expectedErrorCode);
        }
    }

    private void reportTestFailure(@Nonnull String message) {
        reportTestFailure(message, null);
    }

    private void reportTestFailure(@Nonnull String message, @Nullable AssertionFailedError error) {
        logger.error(message);
        if (error == null) {
            Assertions.fail(message);
        } else {
            throw error;
        }
    }

    private Continuation executeWithAConfig(@Nonnull String queryString, @Nonnull final CliCommandFactory factory,
                                            @Nonnull QueryConfigWithValue queryConfigWithValue) throws Exception {

        final var config = queryConfigWithValue.config;
        Continuation continuation = ContinuationImpl.END;
        switch (config) {
            case COUNT:
                checkCount(executeQuery(queryString, queryConfigWithValue.lineNumber, factory, config),
                        queryConfigWithValue, queryString);
                break;
            case RESULT:
            case UNORDERED_RESULT:
                continuation = checkForResult(executeQuery(queryString, queryConfigWithValue.lineNumber, factory, config),
                        queryConfigWithValue, queryString);
                break;
            case EXPLAIN:
            case EXPLAIN_CONTAINS:
                checkExplain(executeQuery(queryString, queryConfigWithValue.lineNumber, factory, config),
                        queryConfigWithValue, queryString);
                break;
            case ERROR:
                final var exceptionOrQueryResult = executeQuery(queryString, queryConfigWithValue.lineNumber, factory, config, false);
                checkForError(exceptionOrQueryResult.getRight(), exceptionOrQueryResult.getLeft(),
                        Matchers.string(queryConfigWithValue.val, "expected error code"), queryConfigWithValue.lineNumber, queryString);
                break;
            case PLAN_HASH:
                checkForPlanHash(executeQuery(queryString, queryConfigWithValue.lineNumber, factory, config),
                        queryConfigWithValue, queryString);
                break;
            default:
                Assert.fail(String.format("‼️ No handler for query configuration '%s'", queryConfigWithValue.config.label));
                break;
        }
        return continuation;
    }

    private Object executeQuery(@Nonnull String queryString, int lineNumber, @Nonnull CliCommandFactory factory,
                                                 @Nullable QueryConfig config) {
        return executeQuery(queryString, lineNumber, factory, config, true).getRight();
    }

    private Pair<SQLException, Object> executeQuery(@Nonnull String queryString, int lineNumber, @Nonnull CliCommandFactory factory,
                                                    @Nullable QueryConfig config, boolean throwException) {
        logger.debug("⏳ Executing query '{}'", queryString);
        Object queryResults = null;
        SQLException sqlException = null;

        final var savedDebugger = Debugger.getDebugger();
        if ((config == QueryConfig.EXPLAIN || config == QueryConfig.EXPLAIN_CONTAINS) && Debugger.getDebugger() == null) {
            Debugger.setDebugger(new DebuggerWithSymbolTables());
            Debugger.setup();
        }
        try {
            queryResults = factory.getQueryCommand(queryString).call();
        } catch (SQLException se) {
            sqlException = se;
        } finally {
            Debugger.setDebugger(savedDebugger);
        }
        logger.debug("👍 Finished executing query '{}'", queryString);
        if (sqlException != null) {
            if (throwException) {
                logAndThrowUnexpectedException(sqlException, lineNumber);
            } else {
                return Pair.of(sqlException, null);
            }
        }
        return Pair.of(null, queryResults);
    }

    @Override
    public void invoke(@Nonnull final List<?> region, @Nonnull final CliCommandFactory factory) throws Exception {
        if (Debugger.getDebugger() != null) {
            Debugger.getDebugger().onSetup(); // clean all symbols before the next query.
        }
        final var queryCommand = Matchers.firstEntry(Matchers.first(region), "query string");
        final var queryString = Matchers.string(Matchers.notNull(queryCommand.getValue(), "query string"), "query string");
        final var queryConfigsWithValue = region.stream().skip(1).collect(Collectors.toList());
        boolean queryHasRun = false;
        boolean queryIsRunning = false;
        Continuation continuation = null;
        var queryConfigWithValuesIterator = queryConfigsWithValue.listIterator();
        while (queryConfigWithValuesIterator.hasNext()) {
            var queryConfigWithValue = getQueryConfigWithValue(queryConfigWithValuesIterator.next());
            if (queryConfigWithValue.config == QueryConfig.PLAN_HASH) {
                Assert.that(!queryIsRunning, "Plan hash test should not be intermingled with query result tests");
                executeWithAConfig(Objects.requireNonNull(queryString), factory, queryConfigWithValue);
            } else if (queryConfigWithValue.config == QueryConfig.EXPLAIN || queryConfigWithValue.config == QueryConfig.EXPLAIN_CONTAINS) {
                Assert.that(!queryIsRunning, "Explain test should not be intermingled with query result tests");
                executeWithAConfig("explain " + queryString, factory, queryConfigWithValue);
            } else {
                if (queryConfigWithValue.config == QueryConfig.ERROR) {
                    Assert.that(!queryConfigWithValuesIterator.hasNext(), "ERROR config should be the last config specified.");
                }

                final var currentQueryString = appendWithContinuationIfPresent(queryString, continuation);
                if (continuation != null && continuation.atEnd() && queryConfigWithValue.config == QueryConfig.RESULT) {
                    Assert.fail(String.format("‼️ Expecting to match a continuation, however no more rows are available to fetch at line %d%n" +
                            "⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤%n" +
                            "%s%n" +
                            "⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤%n",
                            queryConfigWithValue.lineNumber, queryConfigWithValue));
                }
                continuation = executeWithAConfig(currentQueryString, factory, queryConfigWithValue);

                if (continuation == null || continuation.atEnd() || !queryConfigWithValuesIterator.hasNext()) {
                    queryHasRun = true;
                    queryIsRunning = false;
                } else {
                    queryIsRunning = true;
                }
            }
        }

        if (!queryHasRun) {
            executeWithNoConfig(queryString, ((CustomYamlConstructor.LinedObject) queryCommand.getKey()).getStartMark().getLine() + 1, factory);
        }
    }
}
