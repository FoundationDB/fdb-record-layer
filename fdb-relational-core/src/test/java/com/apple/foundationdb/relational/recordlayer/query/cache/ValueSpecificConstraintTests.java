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

import com.apple.foundationdb.relational.api.RelationalConnection;
import com.apple.foundationdb.relational.recordlayer.EmbeddedRelationalExtension;
import com.apple.foundationdb.relational.recordlayer.LogAppenderRule;
import com.apple.foundationdb.relational.recordlayer.Utils;
import com.apple.foundationdb.relational.recordlayer.query.PlanGenerator;
import com.apple.foundationdb.relational.utils.Ddl;
import com.google.common.collect.ImmutableMap;
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
}
