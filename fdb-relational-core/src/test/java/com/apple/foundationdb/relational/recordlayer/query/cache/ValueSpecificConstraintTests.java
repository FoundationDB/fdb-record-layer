/*
 * ValueSpecificConstraintTests.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2025 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.recordlayer.query.cache;

import com.apple.foundationdb.linear.HalfRealVector;
import com.apple.foundationdb.record.query.plan.cascades.predicates.simplification.ConstantFoldingValuePredicateRule;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.RelationalConnection;
import com.apple.foundationdb.relational.recordlayer.EmbeddedRelationalExtension;
import com.apple.foundationdb.relational.recordlayer.LogAppenderRule;
import com.apple.foundationdb.relational.recordlayer.Utils;
import com.apple.foundationdb.relational.recordlayer.query.PlanGenerator;
import com.apple.foundationdb.relational.utils.Ddl;
import com.apple.foundationdb.relational.utils.SchemaTemplateRule;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.logging.log4j.Level;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import javax.annotation.Nonnull;
import java.net.URI;
import java.util.Map;
import java.util.TreeMap;

public class ValueSpecificConstraintTests {

    @RegisterExtension
    @Order(0)
    public final EmbeddedRelationalExtension relationalExtension = new EmbeddedRelationalExtension();

    @RegisterExtension
    @Order(1)
    public final LogAppenderRule logAppender = new LogAppenderRule("TemporaryFunctionTests", PlanGenerator.class, Level.INFO);

    public ValueSpecificConstraintTests() {
        Utils.enableCascadesDebugger();
    }

    private void queryShouldMissCache(@Nonnull final RelationalConnection connection, @Nonnull final String query) throws Exception {
        final var queryWithCacheLoggingOption = query + " options (log query)";
        connection.createStatement().executeQuery(queryWithCacheLoggingOption);
        Assertions.assertTrue(logAppender.lastMessageIsCacheMiss());
    }

    private void queryShouldHitCache(@Nonnull final RelationalConnection connection, @Nonnull final String query) throws Exception {
        final var queryWithCacheLoggingOption = query + " options (log query)";
        connection.createStatement().executeQuery(queryWithCacheLoggingOption);
        Assertions.assertTrue(logAppender.lastMessageIsCacheHit());
    }

    private void preparedQueryShouldMissCache(@Nonnull final RelationalConnection connection, @Nonnull final String query,
                                              @Nonnull final Map<Integer, Object> preparedParams) throws Exception {
        final var queryWithCacheLoggingOption = query + " options (log query)";
        final var statement = connection.prepareStatement(queryWithCacheLoggingOption);
        for (final var entry : preparedParams.entrySet()) {
            statement.setObject(entry.getKey(), entry.getValue());
        }
        statement.execute();
        Assertions.assertTrue(logAppender.lastMessageIsCacheMiss());
    }

    private void preparedQueryShouldHitCache(@Nonnull final RelationalConnection connection, @Nonnull final String query,
                                             @Nonnull final Map<Integer, Object> preparedParams) throws Exception {
        final var queryWithCacheLoggingOption = query + " options (log query)";
        final var statement = connection.prepareStatement(queryWithCacheLoggingOption);
        for (final var entry : preparedParams.entrySet()) {
            statement.setObject(entry.getKey(), entry.getValue());
        }
        statement.execute();
        Assertions.assertTrue(logAppender.lastMessageIsCacheHit());
    }

    @Test
    void constrainingLiteralTrueBooleanWorks() throws Exception {
        final String schemaTemplate = "create table t(pk bigint, a bigint, b boolean, primary key(pk))";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            final var connection = ddl.setSchemaAndGetConnection();
            queryShouldMissCache(ddl.getConnection(), "select a + 42 from t where b = true");
            queryShouldHitCache(ddl.getConnection(), "select a + 42 from t where b = true");

            // 'true' is included in the constraints, therefore, we must not pick the query from the cache when using 'false'
            queryShouldMissCache(ddl.getConnection(), "select a + 42 from t where b = false");
            queryShouldHitCache(ddl.getConnection(), "select a + 42 from t where b = false");

            // 'true' and 'false' are included in the constraints, therefore, we must not pick the query from the cache when using 'null'
            queryShouldMissCache(ddl.getConnection(), "select a + 42 from t where b = null");
            queryShouldHitCache(ddl.getConnection(), "select a + 42 from t where b = null");
        }
    }

    @Test
    void constrainingPreparedTrueBooleanWorks() throws Exception {
        final String schemaTemplate = "create table t(pk bigint, a bigint, b boolean, primary key(pk))";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            final var connection = ddl.setSchemaAndGetConnection();
            preparedQueryShouldMissCache(connection, "select a + 42 from t where b = ?", ImmutableMap.of(1, true));
            preparedQueryShouldHitCache(connection, "select a + 42 from t where b = ?", ImmutableMap.of(1, true));

            // 'true' is included in the constraints, therefore, we must not pick the query from the cache when using 'false'
            preparedQueryShouldMissCache(connection, "select a + 42 from t where b = ?", ImmutableMap.of(1, false));
            preparedQueryShouldHitCache(connection, "select a + 42 from t where b = ?", ImmutableMap.of(1, false));

            // 'true' and 'false' are included in the constraints, therefore, we must not pick the query from the cache when using 'null'
            final Map<Integer, Object> map = new TreeMap<>();
            map.put(1, null);
            preparedQueryShouldMissCache(connection, "select a + 42 from t where b = ?", map);
            preparedQueryShouldHitCache(connection, "select a + 42 from t where b = ?", map);
        }
    }

    @Test
    void constrainingLiteralFalseBooleanWorks() throws Exception {
        final String schemaTemplate = "create table t(pk bigint, a bigint, b boolean, primary key(pk))";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            final var connection = ddl.setSchemaAndGetConnection();
            queryShouldMissCache(connection, "select a + 42 from t where b = false");
            queryShouldHitCache(connection, "select a + 42 from t where b = false");

            // 'false' is included in the constraints, therefore, we must not pick the query from the cache when using 'true'
            queryShouldMissCache(connection, "select a + 42 from t where b = true");
            queryShouldHitCache(connection, "select a + 42 from t where b = true");


            // 'false' and 'true' are included in the constraints, therefore, we must not pick the query from the cache when using 'null'
            queryShouldMissCache(connection, "select a + 42 from t where b = null");
            queryShouldHitCache(connection, "select a + 42 from t where b = null");
        }
    }

    @Test
    void constrainingPreparedFalseBooleanWorks() throws Exception {
        final String schemaTemplate = "create table t(pk bigint, a bigint, b boolean, primary key(pk))";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            final var connection = ddl.setSchemaAndGetConnection();
            preparedQueryShouldMissCache(connection, "select a + 42 from t where b = ?", ImmutableMap.of(1, false));
            preparedQueryShouldHitCache(connection, "select a + 42 from t where b = ?", ImmutableMap.of(1, false));

            // 'false' is included in the constraints, therefore, we must not pick the query from the cache when using 'true'
            preparedQueryShouldMissCache(connection, "select a + 42 from t where b = ?", ImmutableMap.of(1, true));
            preparedQueryShouldHitCache(connection, "select a + 42 from t where b = ?", ImmutableMap.of(1, true));

            // 'false' and 'true' are included in the constraints, therefore, we must not pick the query from the cache when using 'null'
            final Map<Integer, Object> map = new TreeMap<>();
            map.put(1, null);
            preparedQueryShouldMissCache(connection, "select a + 42 from t where b = ?", map);
            preparedQueryShouldHitCache(connection, "select a + 42 from t where b = ?", map);
        }
    }

    @Test
    void constrainingLiteralNullBooleanWorks() throws Exception {
        final String schemaTemplate = "create table t(pk bigint, a bigint, b boolean, primary key(pk))";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            final var connection = ddl.setSchemaAndGetConnection();
            queryShouldMissCache(connection, "select a + 42 from t where b = null");
            queryShouldHitCache(connection, "select a + 42 from t where b = null");

            // 'null' is included in the constraints, therefore, we must not pick the query from the cache when using 'false'
            queryShouldMissCache(connection, "select a + 42 from t where b = false");
            queryShouldHitCache(connection, "select a + 42 from t where b = false");

            // 'null' and 'false' are included in the constraints, therefore, we must not pick the query from the cache when using 'true'
            queryShouldMissCache(connection, "select a + 42 from t where b = true");
            queryShouldHitCache(connection, "select a + 42 from t where b = true");
        }
    }

    @Test
    void constrainingPreparedNullBooleanWorks() throws Exception {
        final String schemaTemplate = "create table t(pk bigint, a bigint, b boolean, primary key(pk))";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            final var connection = ddl.setSchemaAndGetConnection();
            final Map<Integer, Object> map = new TreeMap<>();
            map.put(1, null);
            preparedQueryShouldMissCache(connection, "select a + 42 from t where b = ?", map);
            preparedQueryShouldHitCache(connection, "select a + 42 from t where b = ?", map);

            // 'null' is included in the constraints, therefore, we must not pick the query from the cache when using 'false'
            preparedQueryShouldMissCache(connection, "select a + 42 from t where b = ?", ImmutableMap.of(1, false));
            preparedQueryShouldHitCache(connection, "select a + 42 from t where b = ?", ImmutableMap.of(1, false));

            // 'null' and 'false' are included in the constraints, therefore, we must not pick the query from the cache when using 'true'
            preparedQueryShouldMissCache(connection, "select a + 42 from t where b = ?", ImmutableMap.of(1, true));
            preparedQueryShouldHitCache(connection, "select a + 42 from t where b = ?", ImmutableMap.of(1, true));
        }
    }

    @Test
    void constrainingNonNullLiteralWorks() throws Exception {
        final String schemaTemplate = "create table t(pk bigint, a bigint, b boolean, primary key(pk))";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            final var connection = ddl.setSchemaAndGetConnection();
            queryShouldMissCache(connection, "select a + 43 from t where a = 42");
            queryShouldHitCache(connection, "select a + 43 from t where a = 45"); // different literals are ok here (no index filters)

            // not-null is included in the constraints, therefore, we must not pick the query from the cache when using 'null'
            queryShouldMissCache(connection, "select a + 43 from t where a = null");
            queryShouldHitCache(connection, "select a + 43 from t where a = null");
        }
    }

    @Test
    void constrainingPreparedNotNullWorks() throws Exception {
        final String schemaTemplate = "create table t(pk bigint, a bigint, b boolean, primary key(pk))";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            final var connection = ddl.setSchemaAndGetConnection();
            preparedQueryShouldMissCache(connection, "select a + 43 from t where a = ?", ImmutableMap.of(1, 42));
            preparedQueryShouldHitCache(connection, "select a + 43 from t where a = ?", ImmutableMap.of(1, 45)); // different literals are ok here (no index filters)

            // 'not-null' is included in the constraints, therefore, we must not pick the query from the cache when using 'null'
            final Map<Integer, Object> map = new TreeMap<>();
            map.put(1, null);
            preparedQueryShouldMissCache(connection, "select a + 43 from t where a = ?", map);
            preparedQueryShouldHitCache(connection, "select a + 43 from t where a = ?", map);
        }
    }

    @Test
    void constrainingNullLiteralWorks() throws Exception {
        final String schemaTemplate = "create table t(pk bigint, a bigint, b boolean, primary key(pk))";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            final var connection = ddl.setSchemaAndGetConnection();

            queryShouldMissCache(connection, "select a + 43 from t where a = null");
            queryShouldHitCache(connection, "select a + 43 from t where a = null");

            // 'null' is included in the constraints, therefore, we must not pick the query from the cache when using 'not-null'
            queryShouldMissCache(connection, "select a + 43 from t where a = 42");
            queryShouldHitCache(connection, "select a + 43 from t where a = 45"); // different literals are ok here (no index filters)
        }
    }

    @Test
    void constrainingPreparedNullWorks() throws Exception {
        final String schemaTemplate = "create table t(pk bigint, a bigint, b boolean, primary key(pk))";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            final var connection = ddl.setSchemaAndGetConnection();
            final Map<Integer, Object> map = new TreeMap<>();
            map.put(1, null);
            preparedQueryShouldMissCache(connection, "select a + 43 from t where a = ?", map);
            preparedQueryShouldHitCache(connection, "select a + 43 from t where a = ?", map);

            // 'null' is included in the constraints, therefore, we must not pick the query from the cache when using 'not-null'
            preparedQueryShouldMissCache(connection, "select a + 43 from t where a = ?", ImmutableMap.of(1, 42));
            preparedQueryShouldHitCache(connection, "select a + 43 from t where a = ?", ImmutableMap.of(1, 45)); // different literals are ok here (no index filters)
        }
    }

    @Test
    void sameQueryDifferentRewriteRulesEnablement() throws Exception {
        final String schemaTemplate = "create table t(pk bigint, a bigint, b boolean, primary key(pk))";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            final var connection = ddl.setSchemaAndGetConnection();
            final Map<Integer, Object> map = new TreeMap<>();
            map.put(1, null);

            // 1. disable planner rules.
            connection.setOption(Options.Name.DISABLE_PLANNER_REWRITING, true);
            preparedQueryShouldMissCache(connection, "select a + 43 from t where a = ?", map);
            // should get a cache hit, since the connection options did not change.
            preparedQueryShouldHitCache(connection, "select a + 43 from t where a = ?", map);

            // enabling the planner rewrite rules must result in a cache miss when attempting to
            // execute the same query.
            connection.setOption(Options.Name.DISABLE_PLANNER_REWRITING, false);
            preparedQueryShouldMissCache(connection, "select a + 43 from t where a = ?", map);

            // same, when we're selective about which rules are activated.
            connection.setOption(Options.Name.DISABLED_PLANNER_RULES, ImmutableSet.of(ConstantFoldingValuePredicateRule.class.getSimpleName()));
            preparedQueryShouldMissCache(connection, "select a + 43 from t where a = ?", map);

            // should get a plan cache hit when attempting to plan the query with the same options.
            preparedQueryShouldHitCache(connection, "select a + 43 from t where a = ?", map);
        }
    }

    @Test
    void hnswQueriesWithDifferentRuntimeOptionsAreCachedCorrectly() throws Exception {
        final String schemaTemplate =
                "CREATE TABLE photos(zone string, recordId string, name string, embedding vector(3, half), primary key (zone, recordId)) " +
                "CREATE VIEW V1 AS SELECT embedding, zone, name from photos " +
                "CREATE VECTOR INDEX MV1 USING HNSW ON V1(embedding) PARTITION BY(zone, name)" +
                " OPTIONS (METRIC = EUCLIDEAN_METRIC)";

        final var queryVector = new HalfRealVector(new double[] {1.2f, -0.4f, 3.14f});

        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension)
                .schemaTemplate(schemaTemplate).schemaTemplateOptions((new SchemaTemplateRule.SchemaTemplateOptions(true, true))).build()) {
            final var connection = ddl.setSchemaAndGetConnection();

            // First query with EF_SEARCH = 100 and k = 10
            preparedQueryShouldMissCache(connection,
                    "SELECT * FROM photos WHERE zone = '1' and name = 'Alice' " +
                    "qualify row_number() OVER (PARTITION BY zone, name ORDER BY euclidean_distance(embedding, ?) DESC OPTIONS EF_SEARCH = 100) < ?",
                    ImmutableMap.of(1, queryVector, 2, 10));

            // Same query should hit cache
            preparedQueryShouldHitCache(connection,
                    "SELECT * FROM photos WHERE zone = '1' and name = 'Alice' " +
                    "qualify row_number() OVER (PARTITION BY zone, name ORDER BY euclidean_distance(embedding, ?) DESC OPTIONS EF_SEARCH = 100) < ?",
                    ImmutableMap.of(1, queryVector, 2, 10));

            // Different EF_SEARCH value should miss cache
            preparedQueryShouldMissCache(connection,
                    "SELECT * FROM photos WHERE zone = '1' and name = 'Alice' " +
                    "qualify row_number() OVER (PARTITION BY zone, name ORDER BY euclidean_distance(embedding, ?) DESC OPTIONS EF_SEARCH = 200) < ?",
                    ImmutableMap.of(1, queryVector, 2, 10));

            // Same EF_SEARCH = 200 should hit cache
            preparedQueryShouldHitCache(connection,
                    "SELECT * FROM photos WHERE zone = '1' and name = 'Alice' " +
                    "qualify row_number() OVER (PARTITION BY zone, name ORDER BY euclidean_distance(embedding, ?) DESC OPTIONS EF_SEARCH = 200) < ?",
                    ImmutableMap.of(1, queryVector, 2, 10));

            // Different k value (15 instead of 10) should miss cache
            preparedQueryShouldHitCache(connection,
                    "SELECT * FROM photos WHERE zone = '1' and name = 'Alice' " +
                    "qualify row_number() OVER (PARTITION BY zone, name ORDER BY euclidean_distance(embedding, ?) DESC OPTIONS EF_SEARCH = 200) < ?",
                    ImmutableMap.of(1, queryVector, 2, 15));
        }
    }
}
