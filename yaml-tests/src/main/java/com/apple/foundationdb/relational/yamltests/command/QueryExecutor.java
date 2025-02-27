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
import com.apple.foundationdb.relational.api.RelationalPreparedStatement;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.RelationalResultSetMetaData;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.api.metrics.RelationalMetric;
import com.apple.foundationdb.relational.yamltests.AggregateResultSet;
import com.apple.foundationdb.relational.yamltests.YamlConnection;
import com.apple.foundationdb.relational.yamltests.command.parameterinjection.Parameter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Assertions;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

import static com.apple.foundationdb.relational.yamltests.command.QueryCommand.reportTestFailure;

/**
 * {@link QueryExecutor} executes a query and verifies its results (or error) against a {@link QueryConfig}.
 */
@SuppressWarnings({"PMD.GuardLogStatement"})
public class QueryExecutor {
    private static final Logger logger = LogManager.getLogger(QueryExecutor.class);
    private static final int FORCED_MAX_ROWS = 1; // The maxRows number to use when we are forcing it on the test
    private static final int MAX_CONTINUATIONS_ALLOWED = 100;

    @Nonnull
    private final String query;
    private final int lineNumber;
    // Whether to force continuations if the query does not use continuations explicitly.
    private final boolean forceContinuations;
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
        this(query, lineNumber, parameters, false);
    }

    /**
     * Constructor.
     * @param query the query to execute
     * @param lineNumber the line number from the test script
     * @param parameters the parameters to bind
     * @param forceContinuations Whether to force continuations in case the query does not explicitly specify maxRows.
     */
    QueryExecutor(@Nonnull String query, int lineNumber, @Nullable List<Parameter> parameters, boolean forceContinuations) {
        this.lineNumber = lineNumber;
        this.query = query;
        this.parameters = parameters;
        this.forceContinuations = forceContinuations;
    }

    /**
     * Execute the query.
     * @param connection the connection to use
     * @param continuation the continuation to use. Null is none.
     * @param config the query config relevant for the execution
     * @param checkCache whether to validate the cache parameters
     * @param maxRows the maxRows value to use. Null for default
     * @return the continuation returned by the execution
     * @throws RelationalException in case of error
     */
    @Nullable
    public Continuation execute(@Nonnull YamlConnection connection, @Nullable Continuation continuation,
                                @Nonnull QueryConfig config, boolean checkCache, @Nullable Integer maxRows) throws RelationalException {
        final var currentQuery = config.decorateQuery(query);
        if (continuation == null) {
            // no continuation - start the query execution from the beginning
            return executeQuery(connection, config, currentQuery, checkCache, maxRows);
        } else if (continuation.atBeginning()) {
            // Continuation cannot be at beginning if it was returned from a query
            reportTestFailure("Received continuation shouldn't be at beginning");
            return null;
        } else {
            // Have a continuation - continue
            return executeContinuation(connection, continuation, config, maxRows);
        }
    }

    @SuppressWarnings({
            "PMD.CloseResource", // lifetime of autocloseable resource persists beyond current method
            "PMD.CompareObjectsWithEquals" // pointer equality used on purpose
    })
    private Object executeStatementAndCheckCacheIfNeeded(@Nonnull Statement s, @Nonnull YamlConnection connection,
                                                         @Nullable String queryString, boolean checkCache, @Nullable Integer maxRows) throws SQLException, RelationalException {
        if (!checkCache) {
            return executeStatementAndCheckForceContinuations(s, queryString, connection, maxRows);
        }
        final var preMetricCollector = connection.getMetricCollector();
        final var preValue = preMetricCollector != null &&
                preMetricCollector.hasCounter(RelationalMetric.RelationalCount.PLAN_CACHE_TERTIARY_HIT) ?
                preMetricCollector.getCountsForCounter(RelationalMetric.RelationalCount.PLAN_CACHE_TERTIARY_HIT) : 0;
        final var toReturn = executeStatementAndCheckForceContinuations(s, queryString, connection, maxRows);
        final var postMetricCollector = connection.getMetricCollector();
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

    @Nullable
    private Continuation executeQuery(@Nonnull YamlConnection connection, @Nonnull QueryConfig config,
                                      @Nonnull String currentQuery, boolean checkCache, @Nullable Integer maxRows) throws RelationalException {
        Continuation continuationAfter = null;
        try {
            if (parameters == null) {
                logger.debug("⏳ Executing query '{}'", this.toString());
                try (var s = connection.createStatement()) {
                    final var queryResult = executeStatementAndCheckCacheIfNeeded(s, connection, currentQuery, checkCache, maxRows);
                    config.checkResult(currentQuery, queryResult, this.toString(), connection);
                    if (queryResult instanceof RelationalResultSet) {
                        continuationAfter = ((RelationalResultSet) queryResult).getContinuation();
                    }
                }
            } else {
                logger.debug("⏳ Executing query '{}'", this.toString());
                try (var s = connection.prepareStatement(currentQuery)) {
                    setParametersInPreparedStatement(s);
                    final var queryResult = executeStatementAndCheckCacheIfNeeded(s, connection, null, checkCache, maxRows);
                    config.checkResult(currentQuery, queryResult, this.toString(), connection);
                    if (queryResult instanceof RelationalResultSet) {
                        continuationAfter = ((RelationalResultSet) queryResult).getContinuation();
                    }
                }
            }
            logger.debug("👍 Finished executing query '{}'", this.toString());
        } catch (SQLException sqle) {
            config.checkError(sqle, query, connection);
        }
        return continuationAfter;
    }

    @Nullable
    private Continuation executeContinuation(@Nonnull YamlConnection connection, @Nonnull Continuation continuation,
                                             @Nonnull QueryConfig config, @Nullable Integer maxRows) {
        Continuation continuationAfter = null;
        try {
            logger.debug("⏳ Executing continuation for query '{}'", this.toString());
            final var executeContinuationQuery = "EXECUTE CONTINUATION ?;";
            try (var s = prepareContinuationStatement(connection, continuation, maxRows)) {
                // We bypass checking for cache since the "EXECUTE CONTINUATION ..." statement does not need to be checked
                // for caching.
                final var queryResult = executeStatement(s, null);
                config.checkResult(executeContinuationQuery, queryResult, this.toString(), connection);
                if (queryResult instanceof RelationalResultSet) {
                    continuationAfter = ((RelationalResultSet) queryResult).getContinuation();
                }
            }
            logger.debug("👍 Finished Executing continuation for query '{}'", this.toString());
        } catch (SQLException sqle) {
            config.checkError(sqle, query, connection);
        }
        return continuationAfter;
    }

    private RelationalPreparedStatement prepareContinuationStatement(@Nonnull YamlConnection connection,
                                                                     @Nonnull Continuation continuation,
                                                                     @Nullable Integer maxRows) throws SQLException {
        var s = connection.prepareStatement("EXECUTE CONTINUATION ?;");
        if (maxRows != null) {
            s.setMaxRows(maxRows);
        }
        if (parameters != null) {
            setParametersInPreparedStatement(s);
        }
        // set continuation
        s.setBytes(1, continuation.serialize());
        return s;
    }

    private Object executeStatementAndCheckForceContinuations(@Nonnull Statement s, @Nullable String queryString,
                                                              final YamlConnection connection, @Nullable Integer maxRows) throws SQLException {
        // Check if we need to force continuations
        if ((maxRows == null) && forceContinuations && isForcedContinuationsEligible(queryString)) {
            return executeStatementWithForcedContinuations(s, queryString, connection);
        } else {
            if (maxRows != null) {
                s.setMaxRows(maxRows);
            }
            // No change to behavior
            return executeStatement(s, queryString);
        }
    }

    private boolean isForcedContinuationsEligible(final String queryString) {
        return ((queryString == null) || queryString.trim().toLowerCase(Locale.ROOT).startsWith("select"));
    }

    private Object executeStatementWithForcedContinuations(final @Nonnull Statement s,
                                                           final @Nullable String queryString,
                                                           final @Nonnull YamlConnection connection) throws SQLException {
        s.setMaxRows(FORCED_MAX_ROWS);
        Object result = executeStatement(s, queryString);
        if (result instanceof RelationalResultSet) {
            @SuppressWarnings("PMD.CloseResource")
            RelationalResultSet resultSet = (RelationalResultSet)result;
            List<RelationalResultSet> results = new ArrayList<>();
            final RelationalResultSetMetaData metadata = resultSet.getMetaData(); // The first metadata will be used for all

            boolean hasResult = resultSet.next(); // Initialize result set value retrieval. Has only one row.
            // Edge case: when there are no results at all, return the empty result set that is appropriate in this case
            if (!hasResult) {
                return resultSet;
            }
            results.add(resultSet);
            // Have continuations - keep running the query
            Continuation continuation = resultSet.getContinuation();
            int count = 0;
            while (!continuation.atEnd()) {
                if (continuation.atBeginning()) {
                    reportTestFailure("Received continuation shouldn't be at beginning");
                }
                try (var s2 = prepareContinuationStatement(connection, continuation, FORCED_MAX_ROWS)) {
                    resultSet = (RelationalResultSet)executeStatement(s2, null);
                    final boolean hasNext = resultSet.next(); // Initialize result set value retrieval. Has only one row.
                    continuation = resultSet.getContinuation();
                    if (!continuation.atEnd()) {
                        results.add(resultSet);
                    } else {
                        // We assume that the last result is empty because of the maxRows:1
                        Assertions.assertFalse(hasNext, "Result has more rows than maxRows allowed");
                    }
                }
                count += 1; // PMD failure for ++
                if (count > MAX_CONTINUATIONS_ALLOWED) {
                    reportTestFailure("Too many continuations for query. Test aborted.");
                }
            }
            // Use first metadata for the aggregated result set as they are all the same
            // Use last continuation
            return new AggregateResultSet(metadata, continuation, results.iterator());
        } else {
            // non-result set - just return
            return result;
        }
    }

    private static Object executeStatement(@Nonnull Statement s, @Nullable String q) throws SQLException {
        final var execResult = q == null ? ((PreparedStatement) s).execute() : s.execute(q);
        return execResult ? s.getResultSet() : s.getUpdateCount();
    }

    private void setParametersInPreparedStatement(@Nonnull RelationalPreparedStatement statement) throws SQLException {
        int counter = 1;
        for (var parameter : Objects.requireNonNull(parameters)) {
            statement.setObject(counter++, parameter.getSqlObject(statement.getConnection()));
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
