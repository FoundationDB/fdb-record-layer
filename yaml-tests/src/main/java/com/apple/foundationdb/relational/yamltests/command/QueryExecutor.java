/*
 * QueryExecutor.java
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

import com.apple.foundationdb.relational.api.Continuation;
import com.apple.foundationdb.relational.api.RelationalConnection;
import com.apple.foundationdb.relational.api.RelationalPreparedStatement;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.api.metrics.RelationalMetric;
import com.apple.foundationdb.relational.recordlayer.ContinuationImpl;
import com.apple.foundationdb.relational.recordlayer.EmbeddedRelationalConnection;
import com.apple.foundationdb.relational.yamltests.YamlRunner;
import com.apple.foundationdb.relational.yamltests.command.parameterinjection.Parameter;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Assumptions;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static com.apple.foundationdb.relational.yamltests.command.QueryCommand.reportTestFailure;

/**
 * {@link QueryExecutor} executes a query and verifies its results (or error) against a {@link QueryConfig}.
 */
public class QueryExecutor {
    private static final Logger logger = LogManager.getLogger(QueryExecutor.class);

    @Nonnull
    private final String query;
    private final int lineNumber;
    /**
     * List of bound parameters to be set using the {@link RelationalPreparedStatement}. The existence of this field
     * means that the query should be executed as a prepared statement.
     */
    @Nullable
    private final List<Parameter> parameters;

    QueryExecutor(@Nonnull String query, int lineNumber) {
        this(query, lineNumber, null);
    }

    QueryExecutor(@Nonnull String query, int lineNumber, @Nullable List<Parameter> parameters) {
        this.lineNumber = lineNumber;
        this.query = query;
        this.parameters = parameters;
    }

    public Continuation execute(@Nonnull RelationalConnection connection, @Nonnull YamlRunner.YamlExecutionContext executionContext,
                                @Nonnull QueryConfig config, @Nullable Continuation continuation, boolean checkCache)
            throws RelationalException, SQLException {
        Continuation continuationAfter = ContinuationImpl.END;
        final var currentQuery = config.decorateQuery(query, continuation);
        try {
            if (parameters == null) {
                logger.debug("⏳ Executing query '{}'", printQuery());
                try (var s = connection.createStatement()) {
                    final var queryResult = executeQueryAndCheckCacheIfNeeded(s, connection, currentQuery, checkCache);
                    config.checkResult(queryResult, printQuery(), executionContext);
                    if (queryResult instanceof RelationalResultSet) {
                        continuationAfter = ((RelationalResultSet) queryResult).getContinuation();
                    }
                }
            } else {
                logger.debug("⏳ Executing query '{}'", printQuery());
                try (var s = connection.prepareStatement(currentQuery)) {
                    setParametersInPreparedStatement(s, connection);
                    final var queryResult = executeQueryAndCheckCacheIfNeeded(s, connection, null, checkCache);
                    config.checkResult(queryResult, printQuery(), executionContext);
                    if (queryResult instanceof RelationalResultSet) {
                        continuationAfter = ((RelationalResultSet) queryResult).getContinuation();
                    }
                }
            }
        } catch (SQLException sqle) {
            config.checkError(sqle, query);
        }
        logger.debug("👍 Finished executing query '{}'", printQuery());
        return continuationAfter;
    }

    private Object executeQueryAndCheckCacheIfNeeded(@Nonnull Statement s, @Nonnull RelationalConnection connection,
                                                     @Nullable String queryString, boolean checkCache)
            throws SQLException, RelationalException {
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

    private static Object executeQuery(@Nonnull Statement s, @Nullable String q) throws SQLException {
        final var execResult = q == null ? ((PreparedStatement) s).execute() : s.execute(q);
        return execResult ? s.getResultSet() : s.getUpdateCount();
    }

    private void setParametersInPreparedStatement(@Nonnull RelationalPreparedStatement s, @Nonnull Connection connection) throws SQLException {
        int counter = 1;
        for (var parameter : Objects.requireNonNull(parameters)) {
            s.setObject(counter++, parameter.getSqlObject(connection));
        }
    }

    @Nonnull
    private String printQuery() {
        if (parameters == null) {
            return query;
        } else {
            int counter = 0;
            var paramList = new ArrayList<String>();
            for (var parameter : Objects.requireNonNull(parameters)) {
                paramList.add(String.format("%d -> %s", counter++, parameter.getString()));
            }
            return query + " with parameters " + String.join(", ", paramList);
        }
    }
}
