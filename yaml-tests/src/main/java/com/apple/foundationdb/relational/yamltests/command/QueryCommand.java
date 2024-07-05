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
import com.apple.foundationdb.record.query.plan.debug.DebuggerWithSymbolTables;
import com.apple.foundationdb.relational.api.Continuation;
import com.apple.foundationdb.relational.api.RelationalConnection;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.recordlayer.EmbeddedRelationalConnection;
import com.apple.foundationdb.relational.util.Assert;
import com.apple.foundationdb.relational.util.Environment;
import com.apple.foundationdb.relational.yamltests.CustomYamlConstructor;
import com.apple.foundationdb.relational.yamltests.Matchers;
import com.apple.foundationdb.relational.yamltests.YamlExecutionContext;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Assertions;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.sql.SQLException;
import java.util.List;
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
@SuppressWarnings({"PMD.GuardLogStatement"})
// It already is, but PMD is confused and reporting error in unrelated locations.
public final class QueryCommand extends Command {
    private static final Logger logger = LogManager.getLogger(QueryCommand.class);

    @Nonnull
    private final List<QueryConfig> queryConfigs;
    @Nonnull
    private final QueryInterpreter queryInterpreter;
    @Nonnull
    private final AtomicReference<Throwable> maybeExecutionError = new AtomicReference<>();

    @Nullable
    public Throwable getMaybeExecutionError() {
        return maybeExecutionError.get();
    }

    @Nonnull
    public static QueryCommand parse(@Nonnull Object object, @Nonnull final YamlExecutionContext executionContext) {
        final var queryCommand = Matchers.firstEntry(Matchers.first(Matchers.arrayList(object, "query command")), "query command");
        final var linedObject = CustomYamlConstructor.LinedObject.cast(queryCommand.getKey(), () -> "Invalid command key-value pair: " + queryCommand);
        final var lineNumber = Matchers.notNull(linedObject, "query").getLineNumber();
        try {
            if (Debugger.getDebugger() != null) {
                Debugger.getDebugger().onSetup(); // clean all symbols before the next query.
            }
            final var queryString = Matchers.notNull(Matchers.string(queryCommand.getValue(), "query string"), "query string");
            final var queryInterpreter = QueryInterpreter.withQueryString(lineNumber, queryString, executionContext);
            final var queryConfigsWithValueList = Matchers.arrayList(object).stream().skip(1).collect(Collectors.toList());
            final var configs = queryConfigsWithValueList.isEmpty() ?
                    List.of(QueryConfig.getNoCheckConfig(lineNumber, executionContext)) :
                    queryConfigsWithValueList.stream().map(l -> QueryConfig.parse(l, executionContext)).collect(Collectors.toList());
            return new QueryCommand(lineNumber, queryInterpreter, configs, executionContext);
        } catch (Exception e) {
            throw executionContext.wrapContext(e,
                    () -> "‼️ Error parsing query command at line " + lineNumber, "query", lineNumber);
        }
    }

    public static QueryCommand withQueryString(int lineNumber, @Nonnull String singleExecutableCommand,
                                               @Nonnull final YamlExecutionContext executionContext) {
        return new QueryCommand(lineNumber, QueryInterpreter.withQueryString(lineNumber, singleExecutableCommand, executionContext),
                List.of(QueryConfig.getNoCheckConfig(lineNumber, executionContext)), executionContext);
    }

    private QueryCommand(int lineNumber, @Nonnull QueryInterpreter interpreter, @Nonnull List<QueryConfig> configs,
                         @Nonnull final YamlExecutionContext executionContext) {
        super(lineNumber, executionContext);
        if (Debugger.getDebugger() != null) {
            Debugger.getDebugger().onSetup(); // clean all symbols before the next query.
        }
        this.queryInterpreter = interpreter;
        this.queryConfigs = configs;
    }

    public void execute(@Nonnull final RelationalConnection connection, boolean checkCache, @Nonnull QueryExecutor executor) {
        try {
            if (!(connection instanceof EmbeddedRelationalConnection) && checkCache) {
                logger.debug("⚠️ Not possible to check for cache hit with non-EmbeddedRelationalConnection!");
            } else {
                executeInternal(connection, checkCache, executor);
            }
        } catch (Exception e) {
            if (maybeExecutionError.get() == null) {
                maybeExecutionError.set(executionContext.wrapContext(e,
                        () -> "‼️ Error executing query command at line " + getLineNumber(),
                        String.format("query [%s] ", executor), getLineNumber()));
            }
        }
    }

    @Override
    void executeInternal(@Nonnull final RelationalConnection connection) throws SQLException, RelationalException {
        executeInternal(connection, false, instantiateExecutor(null, false));
    }

    private void executeInternal(@Nonnull final RelationalConnection connection, boolean checkCache, @Nonnull QueryExecutor executor)
            throws SQLException, RelationalException {
        enableCascadesDebugger();
        boolean queryHasRun = false;
        boolean queryIsRunning = false;
        Continuation continuation = null;
        var queryConfigsIterator = queryConfigs.listIterator();
        while (queryConfigsIterator.hasNext()) {
            var queryConfig = queryConfigsIterator.next();
            if (QueryConfig.QUERY_CONFIG_PLAN_HASH.equals(queryConfig.getConfigName())) {
                Assert.that(!queryIsRunning, "Plan hash test should not be intermingled with query result tests");
                executor.execute(connection, null, queryConfig, checkCache);
            } else if (QueryConfig.QUERY_CONFIG_EXPLAIN.equals(queryConfig.getConfigName()) || QueryConfig.QUERY_CONFIG_EXPLAIN_CONTAINS.equals(queryConfig.getConfigName())) {
                Assert.thatUnchecked(!queryIsRunning, "Explain test should not be intermingled with query result tests");
                final var savedDebugger = Debugger.getDebugger();
                try {
                    // ignore debugger configuration, always set the debugger for explain, so we can always get consistent
                    // results
                    Debugger.setDebugger(DebuggerWithSymbolTables.withoutSanityChecks());
                    Debugger.setup();
                    executor.execute(connection, null, queryConfig, checkCache);
                } finally {
                    Debugger.setDebugger(savedDebugger);
                }
            } else {
                if (QueryConfig.QUERY_CONFIG_ERROR.equals(queryConfig.getConfigName())) {
                    Assert.that(!queryConfigsIterator.hasNext(), "ERROR config should be the last config specified.");
                }

                if (continuation != null && continuation.atEnd() && QueryConfig.QUERY_CONFIG_RESULT.equals(queryConfig.getConfigName())) {
                    Assert.fail(String.format("‼️ Expecting to match a continuation, however no more rows are available to fetch at line %d%n" +
                            "⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤%n" +
                            "%s%n" +
                            "⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤%n",
                            queryConfig.getLineNumber(), queryConfig.getValueString()));
                }
                continuation = executor.execute(connection, continuation, queryConfig, checkCache);
                if (continuation == null || continuation.atEnd() || !queryConfigsIterator.hasNext()) {
                    queryHasRun = true;
                    queryIsRunning = false;
                } else {
                    queryIsRunning = true;
                }
            }
        }
        if (!queryHasRun) {
            executor.execute(connection, null, QueryConfig.getNoCheckConfig(getLineNumber(), executionContext), checkCache);
        }
    }

    @Nonnull
    public QueryExecutor instantiateExecutor(@Nullable Random random, boolean runAsPreparedStatement) {
        return queryInterpreter.getExecutor(random, runAsPreparedStatement);
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
            Debugger.setDebugger(DebuggerWithSymbolTables.withoutSanityChecks());
        }
        Debugger.setup();
    }
}
