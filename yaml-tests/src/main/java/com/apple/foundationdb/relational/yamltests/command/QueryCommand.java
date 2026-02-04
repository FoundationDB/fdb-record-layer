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

package com.apple.foundationdb.relational.yamltests.command;

import com.apple.foundationdb.record.query.plan.cascades.debug.Debugger;
import com.apple.foundationdb.record.query.plan.cascades.debug.DebuggerWithSymbolTables;
import com.apple.foundationdb.relational.api.Continuation;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.recordlayer.util.ExceptionUtil;
import com.apple.foundationdb.relational.util.Assert;
import com.apple.foundationdb.relational.util.Environment;
import com.apple.foundationdb.relational.yamltests.CustomYamlConstructor;
import com.apple.foundationdb.relational.yamltests.Matchers;
import com.apple.foundationdb.relational.yamltests.YamlReference;
import com.apple.foundationdb.relational.yamltests.YamlConnection;
import com.apple.foundationdb.relational.yamltests.YamlExecutionContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Assertions;
import org.opentest4j.TestAbortedException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.sql.SQLException;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * A {@link QueryCommand} is one of the possible {@link Command} supported in the YAML testing framework that is used
 * to execute a SQL query against the environment and test its result (or error) in more than one way using configs.
 * At its core, a {@link QueryCommand} consists of
 * <ul>
 *     <li>A {@link QueryInterpreter} that parses the query string and evaluates its parameter injection snippets. If
 *     there are parameter injections that are unbound, this class bind them using a given {@link Random}.
 *     Ultimately, it adapts the query string by injecting parameters and instantiates a {@link QueryExecutor} that is
 *     responsible for query execution in one of the possible ways.
 *
 *     <li>List of {@link QueryConfig}s where each config determines what needs to be tested in the query execution and
 *     how. A {@link QueryExecutor} is executed in order with all the available configs.</li>
 * </ul>
 */
@SuppressWarnings({"PMD.GuardLogStatement", "PMD.AvoidCatchingThrowable"})
// It already is, but PMD is confused and reporting error in unrelated locations.
public final class QueryCommand extends Command {
    private static final Logger logger = LogManager.getLogger(QueryCommand.class);

    @Nonnull
    private final List<QueryConfig> queryConfigs;
    @Nonnull
    private final QueryInterpreter queryInterpreter;
    @Nonnull
    private final AtomicReference<Throwable> maybeExecutionThrowable = new AtomicReference<>();
    private final boolean needsSerialEnvironment;

    @Nullable
    public Throwable getMaybeExecutionThrowable() {
        return maybeExecutionThrowable.get();
    }

    @Nonnull
    public static Command parse(@Nonnull final YamlReference.YamlResource resource, @Nonnull final Object object, @Nonnull final String blockName, @Nonnull final YamlExecutionContext executionContext) {
        final var queryCommand = Matchers.firstEntry(Matchers.first(Matchers.arrayList(object, "query command")), "query command");
        final var linedObject = CustomYamlConstructor.LinedObject.cast(queryCommand.getKey(), () -> "Invalid command key-value pair: " + queryCommand);
        final var reference = resource.withLineNumber(Matchers.notNull(linedObject, "query").getLineNumber());
        try {
            if (Debugger.getDebugger() != null) {
                Debugger.getDebugger().onSetup(); // clean all symbols before the next query.
            }
            final var queryString = Matchers.notNull(Matchers.string(queryCommand.getValue(), "query string"), "query string");
            final var queryInterpreter = QueryInterpreter.withQueryString(reference, queryString, executionContext);
            final List<?> queryConfigsWithValueList = Matchers.arrayList(object).stream().skip(1).collect(Collectors.toList());
            final var configs = queryConfigsWithValueList.isEmpty() ?
                    List.of(QueryConfig.getNoCheckConfig(reference)) :
                    QueryConfig.parseConfigs(blockName, reference, queryConfigsWithValueList, executionContext);

            final List<QueryConfig> skipConfigs = configs.stream().filter(config -> config instanceof QueryConfig.SkipConfig)
                    .collect(Collectors.toList());
            Assert.thatUnchecked(skipConfigs.size() < 2, "Query should not have more than one skip");
            if (!skipConfigs.isEmpty()) {
                return new SkippedCommand(reference, executionContext,
                        ((QueryConfig.SkipConfig)skipConfigs.get(0)).getMessage(),
                        queryInterpreter.getQuery());
            }
            final boolean hasDebuggerConfig =
                    configs.stream()
                            .anyMatch(config -> Objects.equals(config.getConfigName(), QueryConfig.QUERY_CONFIG_DEBUGGER));
            return new QueryCommand(reference, queryInterpreter, configs, executionContext, hasDebuggerConfig);
        } catch (Exception e) {
            throw executionContext.wrapContext(e,
                    () -> "‼️ Error parsing query command at " + reference, "query", reference);
        }
    }

    public static QueryCommand withQueryString(@Nonnull final YamlReference reference, @Nonnull String singleExecutableCommand,
                                               @Nonnull final YamlExecutionContext executionContext) {
        return new QueryCommand(reference, QueryInterpreter.withQueryString(reference, singleExecutableCommand, executionContext),
                List.of(QueryConfig.getNoCheckConfig(reference)), executionContext, false);
    }

    private QueryCommand(@Nonnull final YamlReference reference, @Nonnull QueryInterpreter interpreter, @Nonnull List<QueryConfig> configs,
                         @Nonnull final YamlExecutionContext executionContext, final boolean needsSerialEnvironment) {
        super(reference, executionContext);
        if (Debugger.getDebugger() != null) {
            Debugger.getDebugger().onSetup(); // clean all symbols before the next query.
        }
        this.queryInterpreter = interpreter;
        this.queryConfigs = configs;
        this.needsSerialEnvironment = needsSerialEnvironment;
        Assert.thatUnchecked(queryConfigs.stream().noneMatch(config -> config instanceof QueryConfig.SkipConfig),
                "SkipConfig should not have gotten into QueryCommand " + reference);
    }

    public void execute(@Nonnull final YamlConnection connection, boolean checkCache, @Nonnull QueryExecutor executor) {
        try {
            // checkCache implies that we are repeating the query to check that it hits the cache
            // If the underlying connection does not support access to the metric collector, we won't be able to
            // actually check whether the cache was used, so we can skip this execution entirely.
            if (checkCache && !connection.supportsMetricCollector()) {
                logger.debug("⚠️ Not possible to check for cache hit with non-EmbeddedRelationalConnection!");
            } else {
                executeInternal(connection, checkCache, executor);
            }
        } catch (TestAbortedException tAE) {
            throw tAE;
        } catch (Throwable e) {
            if (maybeExecutionThrowable.get() == null) {
                maybeExecutionThrowable.set(executionContext.wrapContext(e,
                        () -> "‼️ Error executing query command at " + getReference() + " against connection for versions " + connection.getVersions(),
                        String.format(Locale.ROOT, "query [%s] ", executor), getReference()));
            }
        }
    }

    @Override
    void executeInternal(@Nonnull final YamlConnection connection) throws SQLException, RelationalException {
        executeInternal(connection, false, instantiateExecutor(null, false));
    }

    private void executeInternal(@Nonnull final YamlConnection connection, boolean checkCache, @Nonnull QueryExecutor executor)
            throws SQLException, RelationalException {
        enableCascadesDebugger();
        boolean shouldExecute = true;
        boolean queryIsRunning = false;
        Continuation continuation = null;
        Integer maxRows = null;
        boolean exhausted = false;
        boolean errored = false;

        final DebuggerImplementation debuggerImplementation =
                queryConfigs.stream()
                        .filter(config -> Objects.equals(config.getConfigName(), QueryConfig.QUERY_CONFIG_DEBUGGER))
                        .map(config -> (DebuggerImplementation)Objects.requireNonNull(config.getVal()))
                        .findFirst()
                        .orElse(DebuggerImplementation.SANE);

        var queryConfigsIterator = queryConfigs.listIterator();
        while (queryConfigsIterator.hasNext()) {
            var queryConfig = queryConfigsIterator.next();
            if (queryConfig instanceof QueryConfig.InitialVersionCheckConfig) {
                Assert.thatUnchecked(!queryIsRunning, "Initial version checks should not be executed while a query is running");
                shouldExecute = ((QueryConfig.InitialVersionCheckConfig)queryConfig).shouldExecute(connection);
                continue;
            }
            if (!shouldExecute) {
                continue;
            }

            Assert.that(!errored, "ERROR config should be the last config specified.");
            if (QueryConfig.QUERY_CONFIG_MAX_ROWS.equals(queryConfig.getConfigName())) {
                maxRows = Integer.parseInt(queryConfig.getValueString());
            } else if (QueryConfig.QUERY_CONFIG_PLAN_HASH.equals(queryConfig.getConfigName())) {
                Assert.that(!queryIsRunning, "Plan hash test should not be intermingled with query result tests");
                // Ignore debugger configuration, always set the debugger for plan hashes. Executing the plan hash
                // can result in the explain plan being put into the plan cache, so running without the debugger
                // can pollute cache and thus interfere with the explain's results
                Integer finalMaxRows = maxRows;
                runWithDebugger(executionContext, debuggerImplementation,
                        () -> executor.execute(connection, null, queryConfig, checkCache, finalMaxRows));
            } else if (QueryConfig.QUERY_CONFIG_EXPLAIN.equals(queryConfig.getConfigName()) || QueryConfig.QUERY_CONFIG_EXPLAIN_CONTAINS.equals(queryConfig.getConfigName())) {
                Assert.that(!queryIsRunning, "Explain test should not be intermingled with query result tests");
                // ignore debugger configuration, always set the debugger for explain, so we can always get consistent
                // results
                Integer finalMaxRows1 = maxRows;
                runWithDebugger(executionContext, debuggerImplementation,
                        () -> executor.execute(connection, null, queryConfig, checkCache, finalMaxRows1));
            } else if (QueryConfig.QUERY_CONFIG_NO_OP.equals(queryConfig.getConfigName())) {
                // Do nothing for noop execution.
                continue;
            } else if (QueryConfig.QUERY_CONFIG_SETUP.equals(queryConfig.getConfigName())) {
                Assert.that(!queryIsRunning, "Transaction setup should not be intermingled with query results");
                final String setupStatement = Matchers.notNull(Matchers.string(Matchers.notNull(queryConfig.getVal(),
                                "Setup Config Val"), "Transaction setup"), "Transaction setup");
                // we restrict transaction setups to CREATE TEMPORARY FUNCTION, because other mutations could be hard
                // to reason about when running in a parallel world. It's possible the right answer is that we shouldn't
                // commit after the query, or that we shouldn't allow things that modify state in the database. This will
                // become clearer as we have more related tests, and for now, just try to stop people from being confused.
                final String allowedStatement = "CREATE TEMPORARY FUNCTION";
                Assert.that(setupStatement.regionMatches(true, 0, allowedStatement, 0, allowedStatement.length()),
                        "Only \"CREATE TEMPORARY FUNCTION\" is allowed for transaction setups");
                executor.addSetup(setupStatement);
            } else if (!QueryConfig.QUERY_CONFIG_SUPPORTED_VERSION.equals(queryConfig.getConfigName()) &&
                    !QueryConfig.QUERY_CONFIG_DEBUGGER.equals(queryConfig.getConfigName())) {
                if (QueryConfig.QUERY_CONFIG_ERROR.equals(queryConfig.getConfigName())) {
                    errored = true;
                }

                if (exhausted && (QueryConfig.QUERY_CONFIG_RESULT.equals(queryConfig.getConfigName()) || QueryConfig.QUERY_CONFIG_UNORDERED_RESULT.equals(queryConfig.getConfigName()))) {
                    Assert.fail(String.format(Locale.ROOT, "‼️ Expecting more results, however no more rows are available to fetch at %s%n" +
                            "⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤%n" +
                            "%s%n" +
                            "⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤%n",
                            queryConfig.getReference(), queryConfig.getValueString()));
                }
                continuation = executor.execute(connection, continuation, queryConfig, checkCache, maxRows);
                if (continuation == null || continuation.atEnd()) {
                    queryIsRunning = false;
                    exhausted = true;
                } else if (!queryConfigsIterator.hasNext()) {
                    queryIsRunning = false;
                    Assert.fail(String.format(Locale.ROOT, "Query returned more results, but no more were expected after %s",
                            queryConfig.getReference()));
                } else {
                    queryIsRunning = true;
                }
            }
        }
    }

    @Nonnull
    public QueryExecutor instantiateExecutor(@Nullable Random random, boolean runAsPreparedStatement) {
        return queryInterpreter.getExecutor(random, runAsPreparedStatement);
    }

    public static void reportTestFailure(@Nonnull String message) {
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

    public boolean isNeedsSerialEnvironment() {
        return needsSerialEnvironment;
    }

    private static void runWithDebugger(@Nonnull YamlExecutionContext executionContext,
                                        @Nonnull DebuggerImplementation debuggerImplementation,
                                        @Nonnull ThrowingRunnable r) throws SQLException {
        final var savedDebugger = Debugger.getDebugger();
        try {
            Debugger.setDebugger(debuggerImplementation.newDebugger(executionContext));
            Debugger.setup();
            r.run();
        } catch (Exception t) {
            throw ExceptionUtil.toRelationalException(t).toSqlException();
        } finally {
            Debugger.setDebugger(savedDebugger);
        }
    }

    @FunctionalInterface
    public interface ThrowingRunnable {
        void run() throws Exception;
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
            Debugger.setDebugger(DebuggerWithSymbolTables.withoutSanityChecks());
        }
        Debugger.setup();
    }
}
