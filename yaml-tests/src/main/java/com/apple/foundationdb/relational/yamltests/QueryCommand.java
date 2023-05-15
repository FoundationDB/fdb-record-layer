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
import com.apple.foundationdb.relational.api.Continuation;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.cli.CliCommandFactory;
import com.apple.foundationdb.relational.cli.formatters.ResultSetFormat;
import com.apple.foundationdb.relational.recordlayer.ContinuationImpl;
import com.apple.foundationdb.relational.recordlayer.util.Assert;
import com.apple.foundationdb.relational.util.SpotBugsSuppressWarnings;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Assertions;

import javax.annotation.Nonnull;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

@SuppressWarnings({"PMD.GuardLogStatement"})
// It already is, but PMD is confused and reporting error in unrelated locations.
class QueryCommand extends Command {
    private static final Logger logger = LogManager.getLogger(QueryCommand.class);

    private enum QueryConfig {
        RESULT("result"),
        UNORDERED_RESULT("unorderedResult"),
        EXPLAIN("explain"),
        ERROR("error");

        @Nonnull
        private final String label;

        QueryConfig(@Nonnull final String label) {
            this.label = label;
        }

        @SpotBugsSuppressWarnings(value = "NP_NONNULL_RETURN_VIOLATION", justification = "should never happen, fail throws")
        @Nonnull
        public static QueryConfig resolve(@Nonnull final String label) throws Exception {
            for (final var c : values()) {
                if (c.label.equals(label)) {
                    return c;
                }
            }
            Assert.fail(String.format("‼️ '%s' is not a valid configuration, available configuration(s): '%s'",
                    label,
                    Arrays.stream(values()).map(v -> v.label).collect(Collectors.joining(","))));
            return null;
        }
    }

    private static class QueryConfigWithValue {
        private final QueryConfig config;
        private final Object val;

        QueryConfigWithValue(@Nonnull QueryConfig config, @Nonnull Object val) {
            this.config = config;
            this.val = val;
        }
    }

    private void logAndThrowUnexpectedException(SQLException e) throws Exception {
        logger.error("‼️ statement failed with the following error:\n" +
                "⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤\n" +
                "{}\n" +
                "⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤\n",
                e.getMessage());
        throw e;
    }

    private void executeNoCheckStatement(@Nonnull String queryString, @Nonnull final CliCommandFactory factory)
            throws Exception {
        logger.debug("executing query '{}'", queryString);
        Object queryResults = null;
        SQLException sqlException = null;
        try {
            queryResults = factory.getQueryCommand(queryString).call();
        } catch (SQLException se) {
            sqlException = se;
        }
        logger.debug("finished executing query '{}'", queryString);
        if (sqlException != null) {
            logAndThrowUnexpectedException(sqlException);
        }

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
        final var queryConfig = QueryConfig.resolve(
                Matchers.notNull(Matchers.string(Matchers.notNull(queryConfigAndValue, "query configuration").getKey(), "query configuration"), "query configuration"));
        final var configVal = Matchers.notNull(queryConfigAndValue, "query configuration").getValue();
        return new QueryConfigWithValue(queryConfig, configVal);
    }

    private String appendWithContinuationIfPresent(String queryString, Continuation continuation) {
        String currentQuery = queryString;
        if (currentQuery.length() > 0 && currentQuery.charAt(currentQuery.length() - 1) == ';') {
            currentQuery = currentQuery.substring(0, currentQuery.length() - 1);
        }
        if (continuation instanceof ContinuationImpl) {
            currentQuery += String.format(" with continuation '%s'", ResultSetFormat.formatContinuation(continuation));
        }
        return currentQuery;
    }

    private Continuation checkForResult(Object queryResults, SQLException sqlException,
                                        QueryConfigWithValue queryConfigWithValue, String query) throws Exception {
        if (sqlException != null) {
            logAndThrowUnexpectedException(sqlException);
        }
        logger.debug("matching results of query '{}'", query);
        final var resultSet = (RelationalResultSet) queryResults;
        final var matchResult = Matchers.matchResultSet(queryConfigWithValue.val, resultSet, queryConfigWithValue.config != QueryConfig.UNORDERED_RESULT);
        if (!matchResult.equals(Matchers.ResultSetMatchResult.success())) {
            logger.error(String.format("‼️ result mismatch:%n" +
                    Matchers.notNull(matchResult.getExplanation(), "failure error message") + "%n" +
                    "⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤%n" +
                    "↪ expected result:%n" +
                    (queryConfigWithValue.val == null ? "<NULL>" : queryConfigWithValue.val.toString()) + "%n" +
                    "⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤%n" +
                    "↩ actual result:%n" +
                    "⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤%n" +
                    Matchers.notNull(matchResult.getResultSetPrinter(), "failure error actual result set")));
            Assertions.fail("incorrect results!");
        } else {
            logger.debug("✔️ results match!");
        }
        return resultSet.getContinuation();
    }

    private Continuation executeWithAConfig(@Nonnull String query, @Nonnull final CliCommandFactory factory,
                                            @Nonnull QueryConfigWithValue queryConfigWithValue) throws Exception {
        logger.debug("executing query '{}'", query);
        Object queryResults = null;
        SQLException sqlException = null;

        final var config = queryConfigWithValue.config;
        final var savedDebugger = Debugger.getDebugger();

        if (config == QueryConfig.EXPLAIN && Debugger.getDebugger() == null) {
            Debugger.setDebugger(new DebuggerWithSymbolTables());
            Debugger.setup();
        }

        try {
            queryResults = factory.getQueryCommand(query).call();
        } catch (SQLException se) {
            sqlException = se;
        } finally {
            Debugger.setDebugger(savedDebugger);
        }
        logger.debug("finished executing query '{}'", query);
        var continuation = ContinuationImpl.END;
        switch (config) {
            case RESULT:
            case UNORDERED_RESULT:
                continuation = checkForResult(queryResults, sqlException, queryConfigWithValue, query);
                break;
            case EXPLAIN:
                checkForResult(queryResults, sqlException, queryConfigWithValue, query);
                break;
            case ERROR:
                continuation = checkForError(queryResults, sqlException, Matchers.string(queryConfigWithValue.val, "expected error code"), query);
                break;
            default:
                Assert.fail(String.format("‼️ no handler for query configuration '%s'", queryConfigWithValue.config.label));
                break;
        }
        return continuation;
    }

    @Override
    public void invoke(@Nonnull final List<?> region, @Nonnull final CliCommandFactory factory) throws Exception {
        if (Debugger.getDebugger() != null) {
            Debugger.getDebugger().onSetup(); // clean all symbols before the next query.
        }
        final var queryString = Matchers.string(Matchers.notNull(Matchers.firstEntry(Matchers.first(region), "query string").getValue(), "query string"), "query string");
        final var regionWithoutQuery = region.stream().skip(1).collect(Collectors.toList());
        boolean queryHasRun = false;
        Continuation continuation = null;
        var configIterator = regionWithoutQuery.listIterator();
        for (int i = 0; configIterator.hasNext(); i++) {
            Object o = configIterator.next();
            var queryConfigWithValue = getQueryConfigWithValue(o);
            if (queryConfigWithValue.config == QueryConfig.EXPLAIN) {
                Assert.that(i == 0, "EXPLAIN config should be the first config specified.");
                executeWithAConfig("explain " + queryString, factory, queryConfigWithValue);
            } else {
                if (queryConfigWithValue.config == QueryConfig.ERROR) {
                    Assert.that(!configIterator.hasNext(), "ERROR config should be the last config specified.");
                }

                final var currentQueryString = appendWithContinuationIfPresent(queryString, continuation);
                if (continuation != null && continuation.atEnd() && queryConfigWithValue.config == QueryConfig.RESULT) {
                    Assert.fail(String.format("‼️ Expecting to match a continuation, however no more rows are available to fetch%n" +
                            "⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤%n" +
                            "%s%n" +
                            "⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤%n",
                            queryConfigWithValue));
                }
                continuation = executeWithAConfig(currentQueryString, factory, queryConfigWithValue);

                if (continuation == null || continuation.atEnd()) {
                    queryHasRun = true;
                }
            }
        }

        if (!queryHasRun) {
            executeNoCheckStatement(queryString, factory);
        }
    }
}
