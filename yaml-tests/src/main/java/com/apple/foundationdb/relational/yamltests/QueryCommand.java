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
import com.apple.foundationdb.relational.api.RelationalConnection;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.RelationalStatement;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.api.metrics.RelationalMetric;
import com.apple.foundationdb.relational.cli.formatters.ResultSetFormat;
import com.apple.foundationdb.relational.recordlayer.ContinuationImpl;
import com.apple.foundationdb.relational.recordlayer.EmbeddedRelationalConnection;
import com.apple.foundationdb.relational.util.Assert;

import com.apple.foundationdb.relational.util.Environment;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;

@SuppressWarnings({"PMD.GuardLogStatement"})
// It already is, but PMD is confused and reporting error in unrelated locations.
public class QueryCommand extends Command {
    private static final Logger logger = LogManager.getLogger(QueryCommand.class);

    boolean checkCache;

    public static final class QueryConfigWithValue {
        @Nonnull
        private final QueryConfig config;
        @Nullable
        private final Object val;
        private final int lineNumber;
        @Nullable
        private final String configName;
        @Nonnull
        private final YamlRunner.YamlExecutionContext executionContext;

        private QueryConfigWithValue(@Nonnull QueryConfig config, @Nullable Object val, int lineNumber,
                                     @Nullable String configName, @Nonnull YamlRunner.YamlExecutionContext executionContext) {
            this.config = config;
            this.val = val;
            this.lineNumber = lineNumber;
            this.configName = configName;
            this.executionContext = executionContext;
        }

        public int getLineNumber() {
            return lineNumber;
        }

        @Nullable
        public Object getVal() {
            return val;
        }

        @Nonnull
        public QueryConfig getConfig() {
            return config;
        }

        @Nonnull
        public YamlRunner.YamlExecutionContext getExecutionContext() {
            return executionContext;
        }

        public String valueString() {
            if (val instanceof byte[]) {
                return ByteArrayUtil2.loggable((byte[]) val);
            } else if (val != null) {
                return val.toString();
            } else {
                return "";
            }
        }
    }

    QueryCommand(boolean checkCache) {
        this.checkCache = checkCache;
    }

    private QueryConfigWithValue getQueryConfigWithValue(@Nonnull Object config, @Nonnull YamlRunner.YamlExecutionContext executionContext) {
        final var queryConfigAndValue = Matchers.firstEntry(config, "query configuration and value");
        final var queryConfigLinedObject = (CustomYamlConstructor.LinedObject) Matchers.notNull(queryConfigAndValue, "query configuration").getKey();
        final var queryConfigString = Matchers.notNull(Matchers.string(queryConfigLinedObject.getObject(), "query configuration"), "query configuration");
        final var configVal = Matchers.notNull(queryConfigAndValue, "query configuration").getValue();
        return new QueryConfigWithValue(QueryConfig.resolve(queryConfigString), configVal,
                queryConfigLinedObject.getStartMark().getLine() + 1, queryConfigString, executionContext);
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

    @Override
    public void invoke(@Nonnull final List<?> region, @Nonnull final RelationalConnection connection,
                       @Nonnull YamlRunner.YamlExecutionContext executionContext) throws SQLException, RelationalException {
        enableCascadesDebugger();
        final var queryCommand = Matchers.firstEntry(Matchers.first(region), "query string");
        final var queryString = Matchers.notNull(Matchers.string(Matchers.notNull(queryCommand.getValue(), "query string"), "query string"), "query string");
        final var queryConfigsWithValue = region.stream().skip(1).collect(Collectors.toList());
        boolean queryHasRun = false;
        boolean queryIsRunning = false;
        Continuation continuation = null;
        var queryConfigWithValuesIterator = queryConfigsWithValue.listIterator();
        while (queryConfigWithValuesIterator.hasNext()) {
            var queryConfigWithValue = getQueryConfigWithValue(queryConfigWithValuesIterator.next(), executionContext);
            if (QueryConfig.QUERY_CONFIG_PLAN_HASH.equals(queryConfigWithValue.configName)) {
                Assert.that(!queryIsRunning, "Plan hash test should not be intermingled with query result tests");
                executeConfig(queryString, queryConfigWithValue, connection);
            } else if (QueryConfig.QUERY_CONFIG_EXPLAIN.equals(queryConfigWithValue.configName) || QueryConfig.QUERY_CONFIG_EXPLAIN_CONTAINS.equals(queryConfigWithValue.configName)) {
                Assert.thatUnchecked(!queryIsRunning, "Explain test should not be intermingled with query result tests");
                final var savedDebugger = Debugger.getDebugger();
                try {
                    // ignore debugger configuration, always set the debugger for explain, so we can always get consistent
                    // results
                    Debugger.setDebugger(new DebuggerWithSymbolTables());
                    Debugger.setup();
                    executeConfig("explain " + queryString, queryConfigWithValue, connection);
                } finally {
                    Debugger.setDebugger(savedDebugger);
                }
            } else {
                if (QueryConfig.QUERY_CONFIG_ERROR.equals(queryConfigWithValue.configName)) {
                    Assert.that(!queryConfigWithValuesIterator.hasNext(), "ERROR config should be the last config specified.");
                }

                final var currentQueryString = appendWithContinuationIfPresent(queryString, continuation);
                if (continuation != null && continuation.atEnd() && QueryConfig.QUERY_CONFIG_RESULT.equals(queryConfigWithValue.configName)) {
                    Assert.fail(String.format("‼️ Expecting to match a continuation, however no more rows are available to fetch at line %d%n" +
                                    "⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤%n" +
                                    "%s%n" +
                                    "⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤%n",
                            queryConfigWithValue.lineNumber, queryConfigWithValue));
                }
                continuation = executeConfig(currentQueryString, queryConfigWithValue, connection);

                if (continuation == null || continuation.atEnd() || !queryConfigWithValuesIterator.hasNext()) {
                    queryHasRun = true;
                    queryIsRunning = false;
                } else {
                    queryIsRunning = true;
                }
            }
        }

        if (!queryHasRun) {
            final var lineNumber = ((CustomYamlConstructor.LinedObject) queryCommand.getKey()).getStartMark().getLine() + 1;
            final var queryConfigWithValue = new QueryConfigWithValue(QueryConfig.resolve(null), null, lineNumber, null, executionContext);
            executeConfig(queryString, queryConfigWithValue, connection);
        }
    }

    private Continuation executeConfig(@Nonnull String queryString, @Nonnull QueryConfigWithValue configWithValue, @Nonnull RelationalConnection connection) throws SQLException, RelationalException {
        logger.debug("⏳ Executing query '{}'", queryString);
        Continuation continuation = ContinuationImpl.END;
        try {
            try (var s = connection.createStatement()) {
                final var queryResult = executeQueryAndCheckCacheIfNeeded(s, connection, queryString, configWithValue.getLineNumber());
                configWithValue.getConfig().checkResult(queryResult, queryString, configWithValue);
                if (queryResult instanceof RelationalResultSet) {
                    continuation = ((RelationalResultSet) queryResult).getContinuation();
                }
            }
        } catch (SQLException sqle) {
            configWithValue.getConfig().checkError(sqle, queryString, configWithValue);
        }
        logger.debug("👍 Finished executing query '{}'", queryString);
        return continuation;
    }

    private Object executeQueryAndCheckCacheIfNeeded(@Nonnull RelationalStatement s, @Nonnull RelationalConnection connection,
                                                     @Nonnull String queryString, int lineNumber) throws SQLException, RelationalException {
        if (!checkCache) {
            return executeQuery(s, queryString);
        }
        Assumptions.assumeTrue(connection instanceof EmbeddedRelationalConnection, "Not possible to check for cache hit!");
        final var embeddedRelationalConnection = (EmbeddedRelationalConnection) connection;
        final var preValue = embeddedRelationalConnection.getMetricCollector() != null &&
                embeddedRelationalConnection.getMetricCollector().hasCounter(RelationalMetric.RelationalCount.PLAN_CACHE_TERTIARY_HIT) ?
                embeddedRelationalConnection.getMetricCollector().getCountsForCounter(RelationalMetric.RelationalCount.PLAN_CACHE_TERTIARY_HIT) : 0;
        final var toReturn = executeQuery(s, queryString);
        final var postValue = embeddedRelationalConnection.getMetricCollector().hasCounter(RelationalMetric.RelationalCount.PLAN_CACHE_TERTIARY_HIT) ?
                embeddedRelationalConnection.getMetricCollector().getCountsForCounter(RelationalMetric.RelationalCount.PLAN_CACHE_TERTIARY_HIT) : 0;
        if (preValue + 1 != postValue) {
            reportTestFailure("‼️ Expected to retrieve the plan from the cache at line " + lineNumber);
        } else {
            logger.debug("🎁 Retrieved the plan from the cache!");
        }
        return toReturn;
    }

    private static Object executeQuery(@Nonnull RelationalStatement s, @Nonnull String queryString) throws SQLException {
        return s.execute(queryString) ? s.getResultSet() : s.getUpdateCount();
    }

    static void reportTestFailure(@Nonnull String message) {
        reportTestFailure(message, null);
    }

    static void reportTestFailure(@Nonnull String message, @Nullable Throwable throwable) {
        logger.error(message);
        if (throwable == null) {
            Assertions.fail(message);
        } else {
            Assertions.fail(message, throwable);
        }
    }


    /**
     * Enables internal Cascades debugger which, among other things, sets plan identifiers in a stable fashion making
     * it easier to view plans and reproduce planning steps.
     *
     * @implNote
     * This is copied from fdb-relational-core:test subproject to avoid having a dependency just for getting access to it.
     */
    private static void enableCascadesDebugger() {
        if (Debugger.getDebugger() == null && Environment.isDebug()) {
            Debugger.setDebugger(new DebuggerWithSymbolTables());
        }
        Debugger.setup();
    }
}
