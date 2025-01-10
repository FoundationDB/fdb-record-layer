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
import com.apple.foundationdb.relational.yamltests.command.parameterinjection.Parameter;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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
@SuppressWarnings({"PMD.GuardLogStatement"})
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

    public Continuation execute(@Nonnull RelationalConnection connection, @Nullable Continuation continuation,
                                @Nonnull QueryConfig config, boolean checkCache, int maxRows) throws RelationalException, SQLException {
        Continuation continuationAfter = ContinuationImpl.END;
        final var currentQuery = config.decorateQuery(query, continuation);
        try {
            if (parameters == null) {
                logger.debug("⏳ Executing query '{}'", this.toString());
                try (var s = connection.createStatement()) {
                    s.setMaxRows(maxRows);
                    final var queryResult = executeQueryAndCheckCacheIfNeeded(s, connection, currentQuery, checkCache);
                    config.checkResult(queryResult, this.toString());
                    if (queryResult instanceof RelationalResultSet) {
                        continuationAfter = ((RelationalResultSet) queryResult).getContinuation();
                    }
                }
            } else {
                logger.debug("⏳ Executing query '{}'", this.toString());
                try (var s = connection.prepareStatement(currentQuery)) {
                    s.setMaxRows(maxRows);
                    setParametersInPreparedStatement(s, connection);
                    final var queryResult = executeQueryAndCheckCacheIfNeeded(s, connection, null, checkCache);
                    config.checkResult(queryResult, this.toString());
                    if (queryResult instanceof RelationalResultSet) {
                        continuationAfter = ((RelationalResultSet) queryResult).getContinuation();
                    }
                }
            }
            logger.debug("👍 Finished executing query '{}'", this.toString());
        } catch (SQLException sqle) {
            config.checkError(sqle, query);
        }
        return continuationAfter;
    }

    @SuppressWarnings({
            "PMD.CloseResource", // lifetime of autocloseable resource persists beyond current method
            "PMD.CompareObjectsWithEquals" // pointer equality used on purpose
    })
    private Object executeQueryAndCheckCacheIfNeeded(@Nonnull Statement s, @Nonnull RelationalConnection connection,
                                                     @Nullable String queryString, boolean checkCache)
            throws SQLException, RelationalException {
        if (!checkCache) {
            return executeQuery(s, queryString);
        }
        final var embeddedRelationalConnection = (EmbeddedRelationalConnection) connection;
        final var preMetricCollector = embeddedRelationalConnection.getMetricCollector();
        final var preValue = preMetricCollector != null &&
                preMetricCollector.hasCounter(RelationalMetric.RelationalCount.PLAN_CACHE_TERTIARY_HIT) ?
                preMetricCollector.getCountsForCounter(RelationalMetric.RelationalCount.PLAN_CACHE_TERTIARY_HIT) : 0;
        final var toReturn = executeQuery(s, queryString);
        final var postMetricCollector = embeddedRelationalConnection.getMetricCollector();
        final var postValue = postMetricCollector.hasCounter(RelationalMetric.RelationalCount.PLAN_CACHE_TERTIARY_HIT) ?
                postMetricCollector.getCountsForCounter(RelationalMetric.RelationalCount.PLAN_CACHE_TERTIARY_HIT) : 0;
        final var planFound = preMetricCollector != postMetricCollector ? postValue == 1 : postValue == preValue + 1;
        if (!planFound) {
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
    @Override
    public String toString() {
        if (parameters == null) {
            return query;
        } else {
            int counter = 0;
            var paramList = new ArrayList<String>();
            for (var parameter : Objects.requireNonNull(parameters)) {
                paramList.add(String.format(counter++ + " -> " + parameter));
            }
            return query + " with parameters (" + String.join(", ", paramList) + ")";
        }
    }
}
