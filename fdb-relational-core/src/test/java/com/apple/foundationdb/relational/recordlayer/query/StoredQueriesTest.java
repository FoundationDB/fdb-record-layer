/*
 * StoredQueriesTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2026 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.recordlayer.query;

import com.apple.foundationdb.relational.api.RelationalConnection;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.metrics.RelationalMetric;
import com.apple.foundationdb.relational.recordlayer.EmbeddedRelationalConnection;
import com.apple.foundationdb.relational.recordlayer.EmbeddedRelationalExtension;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerSchemaTemplate;
import com.apple.foundationdb.relational.recordlayer.query.cache.QueryCacheKey;
import com.apple.foundationdb.relational.recordlayer.query.cache.RelationalPlanCache;
import com.apple.foundationdb.relational.utils.ConnectionUtils;
import com.apple.foundationdb.relational.utils.Ddl;
import com.apple.foundationdb.relational.utils.RelationalAssertions;
import com.codahale.metrics.MetricFilter;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.net.URI;
import java.sql.SQLException;

class StoredQueriesTest {

    private static final String SCHEMA_TEMPLATE =
            "CREATE TABLE t1(id bigint, col1 bigint, col2 bigint, PRIMARY KEY(id))" +
                    " CREATE INDEX i1 AS SELECT col1 FROM t1" +
                    " CREATE STORED QUERY by_col1 AS select * from t1 where col1 = 10" +
                    " CREATE STORED QUERY by_id AS select * from t1 where id = 1";

    /** One stored query whose body calls a single temp function. */
    private static final String SCHEMA_TEMPLATE_TF_SINGLE =
            "CREATE TABLE t1(id bigint, col1 bigint, col2 bigint, PRIMARY KEY(id))" +
                    " CREATE INDEX i1 AS SELECT col1 FROM t1" +
                    " CREATE STORED QUERY by_x" +
                    "   DECLARE" +
                    "       FUNCTION sq1(in x bigint) AS (SELECT * FROM t1 WHERE col1 < 40 + x)" +
                    " AS SELECT * FROM sq1(10)";


    @RegisterExtension
    @Order(0)
    public final EmbeddedRelationalExtension relationalExtension = new EmbeddedRelationalExtension();

    @BeforeEach
    void clearMetrics() {
        relationalExtension.getMetricRegistry().removeMatching(MetricFilter.ALL);
    }

    private long eventCounterCount(RelationalMetric.RelationalCount count) {
        return relationalExtension.getMetricRegistry().counter(count.title()).getCount();
    }

    private long countCachedPlans(RelationalConnection connection, String templateName) throws SQLException {
        final var embeddedConnection = connection.unwrap(EmbeddedRelationalConnection.class);
        final RelationalPlanCache cache = embeddedConnection.getRecordLayerDatabase().getPlanCache();
        if (cache == null) {
            return 0;
        }
        long total = 0;
        for (QueryCacheKey secondaryKey : cache.getStats().getAllSecondaryKeys(templateName)) {
            total += cache.getStats().getAllTertiaryMappings(templateName, secondaryKey).size();
        }
        return total;
    }

    @Test
    void storedQueriesInTemplate() throws Exception {
        try (var ddl = Ddl.builder()
                .database(URI.create("/TEST/STOREDQUERIES_DB"))
                .relationalExtension(relationalExtension)
                .schemaTemplate(SCHEMA_TEMPLATE)
                .build()) {
            final var connection = ddl.setSchemaAndGetConnection();
            final var embeddedConnection = connection.unwrap(EmbeddedRelationalConnection.class);
            embeddedConnection.setAutoCommit(false);
            embeddedConnection.createNewTransaction();
            final var schemaTemplate = embeddedConnection.getSchemaTemplate().unwrap(RecordLayerSchemaTemplate.class);
            embeddedConnection.rollback();
            embeddedConnection.setAutoCommit(true);
            final var storedQueries = schemaTemplate.getStoredQueries();
            Assertions.assertEquals(2, storedQueries.size());
            Assertions.assertEquals("select * from t1 where col1 = 10", storedQueries.get("BY_COL1").getQuery());
            Assertions.assertEquals("select * from t1 where id = 1", storedQueries.get("BY_ID").getQuery());
            Assertions.assertEquals(0, countCachedPlans(connection, ddl.getSchemaTemplateName())); // we do not generate plans at ddl execution for now
        }
    }

    @Test
    void startupPlanGeneration() throws Exception {
        try (var ddl = Ddl.builder()
                .database(URI.create("/TEST/RESTART_DB"))
                .relationalExtension(relationalExtension)
                .schemaTemplate(SCHEMA_TEMPLATE)
                .build()) {
            final String templateName = ddl.getSchemaTemplateName();

            // create a new engine
            final var engineDriver = relationalExtension.getDriver(
                    com.apple.foundationdb.record.provider.foundationdb.FormatVersion.getDefaultFormatVersion());

            // Connect via the fresh driver and verify the fresh engine's cache has the plans
            Assertions.assertEquals(Long.valueOf(2), new ConnectionUtils(engineDriver).getFromCatalog(
                    conn -> countCachedPlans(conn, templateName)));
        }
    }

    @Test
    void storedQueriesUsage() throws Exception {
        final String dbUri = "/TEST/STOREDQUERIES_DB2";
        try (var ddl = Ddl.builder()
                .database(URI.create(dbUri))
                .relationalExtension(relationalExtension)
                .schemaTemplate(SCHEMA_TEMPLATE)
                .build()) {
            final var connection = ddl.setSchemaAndGetConnection();
            final String templateName = ddl.getSchemaTemplateName();
            final String schemaName = connection.getSchema();

            Assertions.assertEquals(0, countCachedPlans(connection, templateName));

            try (var stmt = connection.createStatement()) {
                stmt.execute("INSERT INTO T1 VALUES (1, 10, 1)");
                stmt.execute("INSERT INTO T1 VALUES (2, 20, 2)");
                stmt.execute("INSERT INTO T1 VALUES (3, 30, 3)");
            }
            Assertions.assertEquals(0, countCachedPlans(connection, templateName)); // we do not generate plans at ddl execution for now

            // create a new engine
            final var engineDriver = relationalExtension.getDriver(
                    com.apple.foundationdb.record.provider.foundationdb.FormatVersion.getDefaultFormatVersion());
            final var connectionUtils = new ConnectionUtils(engineDriver);

            // OfflineStoredQueriesProcessor ran during fresh-engine construction and
            // warmed both stored queries: 2 L3 cache misses.
            Assertions.assertEquals(2, eventCounterCount(RelationalMetric.RelationalCount.PLAN_CACHE_TERTIARY_MISS));
            Assertions.assertEquals(0, eventCounterCount(RelationalMetric.RelationalCount.PLAN_CACHE_TERTIARY_HIT));
            // Connect via the fresh driver and verify the fresh engine's cache has 2 plans
            Assertions.assertEquals(Long.valueOf(2), connectionUtils.getFromCatalog(c -> countCachedPlans(c, templateName)));

            // select statement should hit the cache, no new entries
            connectionUtils.runAgainstConnection(dbUri, schemaName, c -> {
                try (var stmt = c.createStatement(); RelationalResultSet rs = stmt.executeQuery("select * from t1 where col1 = 10")) {
                    Assertions.assertTrue(rs.next());
                    Assertions.assertEquals(1, rs.getLong("ID"));
                    Assertions.assertFalse(rs.next());
                }
            });
            // query hit the cache: hit counter +1, miss counter unchanged.
            Assertions.assertEquals(1, eventCounterCount(RelationalMetric.RelationalCount.PLAN_CACHE_TERTIARY_HIT));
            Assertions.assertEquals(2, eventCounterCount(RelationalMetric.RelationalCount.PLAN_CACHE_TERTIARY_MISS));
            // 2 plans in the cache
            Assertions.assertEquals(Long.valueOf(2), connectionUtils.getFromCatalog(c -> countCachedPlans(c, templateName)));

            // select statement should hit another cache, no new entries
            connectionUtils.runAgainstConnection(dbUri, schemaName, c -> {
                try (var stmt = c.createStatement(); RelationalResultSet rs = stmt.executeQuery("select * from t1 where id = 1")) {
                    Assertions.assertTrue(rs.next());
                    Assertions.assertEquals(1, rs.getLong("ID"));
                    Assertions.assertFalse(rs.next());
                }
            });
            // query hit the cache too: hit counter +1, miss counter still unchanged.
            Assertions.assertEquals(2, eventCounterCount(RelationalMetric.RelationalCount.PLAN_CACHE_TERTIARY_HIT));
            Assertions.assertEquals(2, eventCounterCount(RelationalMetric.RelationalCount.PLAN_CACHE_TERTIARY_MISS));
            // 2 plans in the cache
            Assertions.assertEquals(Long.valueOf(2), connectionUtils.getFromCatalog(c -> countCachedPlans(c, templateName)));

            // non-stored query, new record in the cache
            connectionUtils.runAgainstConnection(dbUri, schemaName, c -> {
                try (var stmt = c.createStatement(); RelationalResultSet rs = stmt.executeQuery("select * from t1 where col2 = 1")) {
                    Assertions.assertTrue(rs.next());
                    Assertions.assertEquals(1, rs.getLong("ID"));
                    Assertions.assertFalse(rs.next());
                }
            });
            // new (3) plan in the cache
            Assertions.assertEquals(Long.valueOf(3), connectionUtils.getFromCatalog(c -> countCachedPlans(c, templateName)));
            // SELECT col2 is NOT pre-warmed: miss counter +1, hit counter unchanged.
            Assertions.assertEquals(2, eventCounterCount(RelationalMetric.RelationalCount.PLAN_CACHE_TERTIARY_HIT));
            Assertions.assertEquals(3, eventCounterCount(RelationalMetric.RelationalCount.PLAN_CACHE_TERTIARY_MISS));
        }
    }

    @Test
    void storedQueriesUsageParams() throws Exception {
        final String dbUri = "/TEST/STOREDQUERIES_DB3";
        try (var ddl = Ddl.builder()
                .database(URI.create(dbUri))
                .relationalExtension(relationalExtension)
                .schemaTemplate(SCHEMA_TEMPLATE)
                .build()) {
            final var connection = ddl.setSchemaAndGetConnection();
            final String templateName = ddl.getSchemaTemplateName();
            final String schemaName = connection.getSchema();

            Assertions.assertEquals(0, countCachedPlans(connection, templateName));

            try (var stmt = connection.createStatement()) {
                stmt.execute("INSERT INTO T1 VALUES (1, 10, 1)");
                stmt.execute("INSERT INTO T1 VALUES (2, 20, 2)");
                stmt.execute("INSERT INTO T1 VALUES (3, 30, 3)");
            }
            Assertions.assertEquals(0, countCachedPlans(connection, templateName)); // we do not generate plans at ddl execution for now

            // create a new engine
            final var engineDriver = relationalExtension.getDriver(
                    com.apple.foundationdb.record.provider.foundationdb.FormatVersion.getDefaultFormatVersion());
            final var connectionUtils = new ConnectionUtils(engineDriver);

            // Connect via the fresh driver and verify the fresh engine's cache has the plans
            Assertions.assertEquals(Long.valueOf(2), connectionUtils.getFromCatalog(c -> countCachedPlans(c, templateName)));

            // select with different literal than stored — canonical SQL matches, cache hit
            connectionUtils.runAgainstConnection(dbUri, schemaName, c -> {
                try (var stmt = c.createStatement(); RelationalResultSet rs = stmt.executeQuery("select * from t1 where col1 = 20")) {
                    Assertions.assertTrue(rs.next());
                    Assertions.assertEquals(2, rs.getLong("ID"));
                    Assertions.assertFalse(rs.next());
                }
            });
            Assertions.assertEquals(Long.valueOf(2), connectionUtils.getFromCatalog(c -> countCachedPlans(c, templateName)));

            // select with different literal than stored — canonical SQL matches, cache hit
            connectionUtils.runAgainstConnection(dbUri, schemaName, c -> {
                try (var stmt = c.createStatement(); RelationalResultSet rs = stmt.executeQuery("select * from t1 where id = 2")) {
                    Assertions.assertTrue(rs.next());
                    Assertions.assertEquals(2, rs.getLong("ID"));
                    Assertions.assertFalse(rs.next());
                }
            });
            Assertions.assertEquals(Long.valueOf(2), connectionUtils.getFromCatalog(c -> countCachedPlans(c, templateName)));
        }
    }

    @Test
    void storedQueriesUsageJdbcPrepare() throws Exception {
        final String dbUri = "/TEST/STOREDQUERIES_DB4";
        try (var ddl = Ddl.builder()
                .database(URI.create(dbUri))
                .relationalExtension(relationalExtension)
                .schemaTemplate(SCHEMA_TEMPLATE)
                .build()) {
            final var connection = ddl.setSchemaAndGetConnection();
            final String templateName = ddl.getSchemaTemplateName();
            final String schemaName = connection.getSchema();

            Assertions.assertEquals(0, countCachedPlans(connection, templateName));

            try (var stmt = connection.createStatement()) {
                stmt.execute("INSERT INTO T1 VALUES (1, 10, 1)");
                stmt.execute("INSERT INTO T1 VALUES (2, 20, 2)");
                stmt.execute("INSERT INTO T1 VALUES (3, 30, 3)");
            }
            Assertions.assertEquals(0, countCachedPlans(connection, templateName)); // we do not generate plans at ddl execution for now

            // create a new engine
            final var engineDriver = relationalExtension.getDriver(
                    com.apple.foundationdb.record.provider.foundationdb.FormatVersion.getDefaultFormatVersion());
            final var connectionUtils = new ConnectionUtils(engineDriver);

            // Connect via the fresh driver and verify the fresh engine's cache has the plans
            Assertions.assertEquals(Long.valueOf(2), connectionUtils.getFromCatalog(c -> countCachedPlans(c, templateName)));

            // JDBC PreparedStatement on col1 with bound parameter — canonical SQL matches stored BY_COL1, cache hit
            connectionUtils.runAgainstConnection(dbUri, schemaName, c -> {
                try (var ps = c.prepareStatement("select * from t1 where col1 = ?")) {
                    ps.setInt(1, 20);
                    try (RelationalResultSet rs = ps.executeQuery()) {
                        Assertions.assertTrue(rs.next());
                        Assertions.assertEquals(2, rs.getLong("ID"));
                        Assertions.assertFalse(rs.next());
                    }
                }
            });
            Assertions.assertEquals(Long.valueOf(2), connectionUtils.getFromCatalog(c -> countCachedPlans(c, templateName)));

            // JDBC PreparedStatement on id with bound parameter — canonical SQL matches stored BY_ID, cache hit
            connectionUtils.runAgainstConnection(dbUri, schemaName, c -> {
                try (var ps = c.prepareStatement("select * from t1 where id = ?")) {
                    ps.setInt(1, 2);
                    try (RelationalResultSet rs = ps.executeQuery()) {
                        Assertions.assertTrue(rs.next());
                        Assertions.assertEquals(2, rs.getLong("ID"));
                        Assertions.assertFalse(rs.next());
                    }
                }
            });
            Assertions.assertEquals(Long.valueOf(2), connectionUtils.getFromCatalog(c -> countCachedPlans(c, templateName)));
        }
    }

    @Test
    void storedQueriesUsageTypedParam() throws Exception {
        // A stored query whose body uses an inline typed positional parameter (?{bigint}) instead of a concrete
        // literal, so it is planned value-free at warmup and reused by any runtime value of the declared type.
        final String schemaTemplate =
                "CREATE TABLE t1(id bigint, col1 bigint, col2 bigint, PRIMARY KEY(id))" +
                        " CREATE INDEX i1 AS SELECT col1 FROM t1" +
                        " CREATE STORED QUERY by_col1_typed AS select * from t1 where col1 > ?{bigint}";
        final String dbUri = "/TEST/STOREDQUERIES_TYPED_DB";
        try (var ddl = Ddl.builder()
                .database(URI.create(dbUri))
                .relationalExtension(relationalExtension)
                .schemaTemplate(schemaTemplate)
                .build()) {
            final var connection = ddl.setSchemaAndGetConnection();
            final String templateName = ddl.getSchemaTemplateName();
            final String schemaName = connection.getSchema();

            try (var stmt = connection.createStatement()) {
                stmt.execute("INSERT INTO T1 VALUES (1, 10, 1)");
                stmt.execute("INSERT INTO T1 VALUES (2, 20, 2)");
                stmt.execute("INSERT INTO T1 VALUES (3, 30, 3)");
            }

            // Fresh engine: OfflineStoredQueriesProcessor warms the single value-free stored query.
            final var engineDriver = relationalExtension.getDriver(
                    com.apple.foundationdb.record.provider.foundationdb.FormatVersion.getDefaultFormatVersion());
            final var connectionUtils = new ConnectionUtils(engineDriver);
            Assertions.assertEquals(Long.valueOf(1), connectionUtils.getFromCatalog(c -> countCachedPlans(c, templateName)));

            // Runtime 'col1 > ?' bound to a bigint (setLong) — canonical SQL and OfType(LONG) match the warmed
            // value-free plan, so it is reused (no new plan is generated: countCachedPlans stays 1). A type mismatch
            // or a value-derived plan would instead replan and bump the count to 2.
            connectionUtils.runAgainstConnection(dbUri, schemaName, c -> {
                try (var ps = c.prepareStatement("select * from t1 where col1 > ?")) {
                    ps.setLong(1, 15L);
                    try (RelationalResultSet rs = ps.executeQuery()) {
                        Assertions.assertTrue(rs.next());
                        Assertions.assertEquals(2, rs.getLong("ID"));
                        Assertions.assertTrue(rs.next());
                        Assertions.assertEquals(3, rs.getLong("ID"));
                        Assertions.assertFalse(rs.next());
                    }
                }
            });
            Assertions.assertEquals(Long.valueOf(1), connectionUtils.getFromCatalog(c -> countCachedPlans(c, templateName)));
        }
    }

    @Test
    void storedQueriesUsageTwoTypedParams() throws Exception {
        // A stored query with two inline typed parameters. Because ?{type} is a single lexer token, it occupies
        // exactly one token slot (like a bare ?), so the second parameter's constant id (derived from the token
        // index) matches the second ? of a runtime col1 > ? and col2 < ?. A multi-token annotation would
        // shift it and bind the runtime values to the wrong slots.
        final String schemaTemplate =
                "CREATE TABLE t1(id bigint, col1 bigint, col2 bigint, PRIMARY KEY(id))" +
                        " CREATE INDEX i1 AS SELECT col1 FROM t1" +
                        " CREATE STORED QUERY by_two_typed AS select * from t1 where col1 > ?{bigint} and col2 < ?{bigint}";
        final String dbUri = "/TEST/STOREDQUERIES_TWO_TYPED_DB";
        try (var ddl = Ddl.builder()
                .database(URI.create(dbUri))
                .relationalExtension(relationalExtension)
                .schemaTemplate(schemaTemplate)
                .build()) {
            final var connection = ddl.setSchemaAndGetConnection();
            final String templateName = ddl.getSchemaTemplateName();
            final String schemaName = connection.getSchema();

            try (var stmt = connection.createStatement()) {
                stmt.execute("INSERT INTO T1 VALUES (1, 10, 1)");
                stmt.execute("INSERT INTO T1 VALUES (2, 20, 2)");
                stmt.execute("INSERT INTO T1 VALUES (3, 30, 3)");
            }

            // Fresh engine: warm the single value-free stored query with two typed parameters.
            final var engineDriver = relationalExtension.getDriver(
                    com.apple.foundationdb.record.provider.foundationdb.FormatVersion.getDefaultFormatVersion());
            final var connectionUtils = new ConnectionUtils(engineDriver);
            Assertions.assertEquals(Long.valueOf(1), connectionUtils.getFromCatalog(c -> countCachedPlans(c, templateName)));

            // Runtime 'col1 > ? and col2 < ?' with two bound bigints. Both values must bind to the correct constant
            // ids: col1 > 15 -> {id 2, 3}; col2 < 3 -> {id 1, 2}; intersection -> id 2. A shifted second constant id
            // would bind the values to the wrong slots (wrong rows) or fail to reuse the plan.
            connectionUtils.runAgainstConnection(dbUri, schemaName, c -> {
                try (var ps = c.prepareStatement("select * from t1 where col1 > ? and col2 < ?")) {
                    ps.setLong(1, 15L);
                    ps.setLong(2, 3L);
                    try (RelationalResultSet rs = ps.executeQuery()) {
                        Assertions.assertTrue(rs.next());
                        Assertions.assertEquals(2, rs.getLong("ID"));
                        Assertions.assertFalse(rs.next());
                    }
                }
            });
            Assertions.assertEquals(Long.valueOf(1), connectionUtils.getFromCatalog(c -> countCachedPlans(c, templateName)));
        }
    }

    @Test
    void storedQueriesFilteredIndexCharacterization() throws Exception {
        // A value-free stored query against a schema whose only col1 index is range-filtered. Selecting that
        // index needs the parameter's value (to prove it falls in the index range), which is absent at warmup — so this
        // characterizes what warmup does in that case (skip + runtime replan, or something else).
        final String schemaTemplate =
                "CREATE TABLE t1(id bigint, col1 bigint, col2 bigint, PRIMARY KEY(id))" +
                        " CREATE INDEX i2 AS SELECT col1 FROM t1 WHERE col1 > 42" +
                        " CREATE STORED QUERY by_col1_filtered AS select * from t1 where col1 > ?{bigint}";
        final String dbUri = "/TEST/STOREDQUERIES_FILTERED_DB";
        try (var ddl = Ddl.builder()
                .database(URI.create(dbUri))
                .relationalExtension(relationalExtension)
                .schemaTemplate(schemaTemplate)
                .build()) {
            final var connection = ddl.setSchemaAndGetConnection();
            final String templateName = ddl.getSchemaTemplateName();
            final String schemaName = connection.getSchema();

            try (var stmt = connection.createStatement()) {
                stmt.execute("INSERT INTO T1 VALUES (1, 10, 1)");
                stmt.execute("INSERT INTO T1 VALUES (2, 20, 2)");
                stmt.execute("INSERT INTO T1 VALUES (3, 30, 3)");
            }

            final var engineDriver = relationalExtension.getDriver(
                    com.apple.foundationdb.record.provider.foundationdb.FormatVersion.getDefaultFormatVersion());
            final var connectionUtils = new ConnectionUtils(engineDriver);

            // HYPOTHESIS: selecting the range-filtered index i2 needs the value, which is absent at warmup, so
            // OfflineStoredQueriesProcessor's per-query catch skips this stored query -> nothing is pre-warmed.
            Assertions.assertEquals(Long.valueOf(0), connectionUtils.getFromCatalog(c -> countCachedPlans(c, templateName)));

            // Correctness regardless: at runtime the value is present, so the query replans and returns the right rows
            // (col1 > 15 -> id 2, 3; i2 covers only col1 > 42, so a scan is used for value 15).
            connectionUtils.runAgainstConnection(dbUri, schemaName, c -> {
                try (var ps = c.prepareStatement("select * from t1 where col1 > ?")) {
                    ps.setLong(1, 15L);
                    try (RelationalResultSet rs = ps.executeQuery()) {
                        Assertions.assertTrue(rs.next());
                        Assertions.assertEquals(2, rs.getLong("ID"));
                        Assertions.assertTrue(rs.next());
                        Assertions.assertEquals(3, rs.getLong("ID"));
                        Assertions.assertFalse(rs.next());
                    }
                }
            });

            Assertions.assertEquals(Long.valueOf(1), connectionUtils.getFromCatalog(c -> countCachedPlans(c, templateName)));
        }
    }

    @Test
    void storedQueriesTypedParamIncompatibleType() throws Exception {
        // The declared parameter type (string) is incompatible with the column it is compared to (col1 is
        // bigint), so col1 > ?{string} is ill-typed. The body is stored verbatim at DDL time and only
        // type-checked when planned at warmup, so this characterizes that the ill-typed stored query fails to warm
        // (and is skipped) rather than producing a broken plan.
        final String schemaTemplate =
                "CREATE TABLE t1(id bigint, col1 bigint, col2 bigint, PRIMARY KEY(id))" +
                        " CREATE INDEX i1 AS SELECT col1 FROM t1" +
                        " CREATE STORED QUERY by_col1_badtype AS select * from t1 where col1 > ?{string}";
        final String dbUri = "/TEST/STOREDQUERIES_BADTYPE_DB";
        try (var ddl = Ddl.builder()
                .database(URI.create(dbUri))
                .relationalExtension(relationalExtension)
                .schemaTemplate(schemaTemplate)
                .build()) {
            final var connection = ddl.setSchemaAndGetConnection();
            final String templateName = ddl.getSchemaTemplateName();

            try (var stmt = connection.createStatement()) {
                stmt.execute("INSERT INTO T1 VALUES (1, 10, 1)");
                stmt.execute("INSERT INTO T1 VALUES (2, 20, 2)");
            }

            // The body 'col1 > ?{string}' is ill-typed (bigint column compared to a string parameter). It is stored
            // verbatim at DDL time and only type-checked when planned at warmup, so warmup fails to plan it and skips
            // it (OfflineStoredQueriesProcessor's per-query catch): nothing is pre-warmed.
            final var engineDriver = relationalExtension.getDriver(
                    com.apple.foundationdb.record.provider.foundationdb.FormatVersion.getDefaultFormatVersion());
            final var connectionUtils = new ConnectionUtils(engineDriver);
            Assertions.assertEquals(Long.valueOf(0), connectionUtils.getFromCatalog(c -> countCachedPlans(c, templateName)));
        }
    }

    @Test
    void storedQueriesUsageTypedParamArithmetic() throws Exception {
        // The typed parameter appears inside a constant-foldable sub-expression (?{bigint} + 3). If the planner
        // tries to constant-fold that sub-expression at warmup it would dereference the value-free (unbound) constant.
        // This characterizes what happens (warm value-free, or skip because folding fails).
        final String schemaTemplate =
                "CREATE TABLE t1(id bigint, col1 bigint, col2 bigint, PRIMARY KEY(id))" +
                        " CREATE INDEX i1 AS SELECT col1 FROM t1" +
                        " CREATE STORED QUERY by_col1_arith AS select * from t1 where col1 > ?{bigint} + 3";
        final String dbUri = "/TEST/STOREDQUERIES_ARITH_DB";
        try (var ddl = Ddl.builder()
                .database(URI.create(dbUri))
                .relationalExtension(relationalExtension)
                .schemaTemplate(schemaTemplate)
                .build()) {
            final var connection = ddl.setSchemaAndGetConnection();
            final String templateName = ddl.getSchemaTemplateName();
            final String schemaName = connection.getSchema();

            try (var stmt = connection.createStatement()) {
                stmt.execute("INSERT INTO T1 VALUES (1, 10, 1)");
                stmt.execute("INSERT INTO T1 VALUES (2, 20, 2)");
                stmt.execute("INSERT INTO T1 VALUES (3, 30, 3)");
            }

            final var engineDriver = relationalExtension.getDriver(
                    com.apple.foundationdb.record.provider.foundationdb.FormatVersion.getDefaultFormatVersion());
            final var connectionUtils = new ConnectionUtils(engineDriver);

            // The parameter appears in the constant-foldable sub-expression 'COV + 3', but the whole predicate is not
            // constant (col1 is a field), so the planner keeps 'COV + 3' symbolic rather than dereferencing the
            // value-free constant. The stored query therefore warms value-free (one tertiary mapping).
            Assertions.assertEquals(Long.valueOf(1), connectionUtils.getFromCatalog(c -> countCachedPlans(c, templateName)));

            // Runtime 'col1 > ? + 3' with 12 -> col1 > 15 -> id 2, 3. Reuses the warmed plan (hit): count stays 1.
            connectionUtils.runAgainstConnection(dbUri, schemaName, c -> {
                try (var ps = c.prepareStatement("select * from t1 where col1 > ? + 3")) {
                    ps.setLong(1, 12L);
                    try (RelationalResultSet rs = ps.executeQuery()) {
                        Assertions.assertTrue(rs.next());
                        Assertions.assertEquals(2, rs.getLong("ID"));
                        Assertions.assertTrue(rs.next());
                        Assertions.assertEquals(3, rs.getLong("ID"));
                        Assertions.assertFalse(rs.next());
                    }
                }
            });
            Assertions.assertEquals(Long.valueOf(1), connectionUtils.getFromCatalog(c -> countCachedPlans(c, templateName)));
        }
    }

    @Test
    void storedQueriesUsageTypedParamString() throws Exception {
        // A value-free stored query with a non-numeric (string) typed parameter on an indexed string column.
        final String schemaTemplate =
                "CREATE TABLE ts(id bigint, name string, PRIMARY KEY(id))" +
                        " CREATE INDEX i_name AS SELECT name FROM ts" +
                        " CREATE STORED QUERY by_name AS select * from ts where name = ?{string}";
        final String dbUri = "/TEST/STOREDQUERIES_STRING_DB";
        try (var ddl = Ddl.builder()
                .database(URI.create(dbUri))
                .relationalExtension(relationalExtension)
                .schemaTemplate(schemaTemplate)
                .build()) {
            final var connection = ddl.setSchemaAndGetConnection();
            final String templateName = ddl.getSchemaTemplateName();
            final String schemaName = connection.getSchema();

            try (var stmt = connection.createStatement()) {
                stmt.execute("INSERT INTO TS VALUES (1, 'alice')");
                stmt.execute("INSERT INTO TS VALUES (2, 'bob')");
                stmt.execute("INSERT INTO TS VALUES (3, 'carol')");
            }

            final var engineDriver = relationalExtension.getDriver(
                    com.apple.foundationdb.record.provider.foundationdb.FormatVersion.getDefaultFormatVersion());
            final var connectionUtils = new ConnectionUtils(engineDriver);
            // The value-free string parameter warms one plan (declared string / OfType(STRING)).
            Assertions.assertEquals(Long.valueOf(1), connectionUtils.getFromCatalog(c -> countCachedPlans(c, templateName)));

            // Runtime 'name = ?' bound to a string matches OfType(STRING), so the warmed plan is reused (hit): count
            // stays 1 and the right row is returned.
            connectionUtils.runAgainstConnection(dbUri, schemaName, c -> {
                try (var ps = c.prepareStatement("select * from ts where name = ?")) {
                    ps.setString(1, "bob");
                    try (RelationalResultSet rs = ps.executeQuery()) {
                        Assertions.assertTrue(rs.next());
                        Assertions.assertEquals(2, rs.getLong("ID"));
                        Assertions.assertFalse(rs.next());
                    }
                }
            });
            Assertions.assertEquals(Long.valueOf(1), connectionUtils.getFromCatalog(c -> countCachedPlans(c, templateName)));
        }
    }

    @Test
    void storedQueriesBooleanPairConstantFold() throws Exception {
        // A WHERE predicate that is a pure constant comparison of two value-free boolean parameters
        // (?{boolean} = ?{boolean}). There is no column in the predicate, so it is fully constant-foldable — this
        // exercises the value-free folding path: both constants are unbound at warmup, so the plan stays symbolic
        // (warms value-free) and is reused for any bound pair rather than folding a phantom value.
        final String schemaTemplate =
                "CREATE TABLE tb(id bigint, flag boolean, PRIMARY KEY(id))" +
                        " CREATE STORED QUERY by_boolpair3 AS select * from tb where ?{boolean} = ?{boolean}";
        final String dbUri = "/TEST/STOREDQUERIES_BOOLPAIR_DB";
        try (var ddl = Ddl.builder()
                .database(URI.create(dbUri))
                .relationalExtension(relationalExtension)
                .schemaTemplate(schemaTemplate)
                .build()) {
            final var connection = ddl.setSchemaAndGetConnection();
            final String templateName = ddl.getSchemaTemplateName();
            final String schemaName = connection.getSchema();

            try (var stmt = connection.createStatement()) {
                stmt.execute("INSERT INTO TB VALUES (1, true)");
                stmt.execute("INSERT INTO TB VALUES (2, false)");
            }

            // The single value-free query warms exactly one plan, whose literal constraint is IS_NOT_NULL on both
            // constants. The count is 1 deterministically — with a single query there is no dependence on warmup (Map)
            // iteration order. (Mixing this with concrete-literal queries of the same canonical '? = ?' shape would
            // make the count order-dependent: the broad IS_NOT_NULL plan absorbs narrower literal queries warmed after
            // it. That is why this test asserts behavior, not a fragile multi-query plan count.)
            final var engineDriver = relationalExtension.getDriver(
                    com.apple.foundationdb.record.provider.foundationdb.FormatVersion.getDefaultFormatVersion());
            final var connectionUtils = new ConnectionUtils(engineDriver);
            Assertions.assertEquals(Long.valueOf(1), connectionUtils.getFromCatalog(c -> countCachedPlans(c, templateName)));

            // Runtime '? = ?' with true = true -> predicate true -> all rows; reuses the warmed plan (count stays 1).
            connectionUtils.runAgainstConnection(dbUri, schemaName, c -> {
                try (var ps = c.prepareStatement("select * from tb where ? = ?")) {
                    ps.setBoolean(1, true);
                    ps.setBoolean(2, true);
                    try (RelationalResultSet rs = ps.executeQuery()) {
                        Assertions.assertTrue(rs.next());
                        Assertions.assertEquals(1, rs.getLong("ID"));
                        Assertions.assertTrue(rs.next());
                        Assertions.assertEquals(2, rs.getLong("ID"));
                        Assertions.assertFalse(rs.next());
                    }
                }
            });
            Assertions.assertEquals(Long.valueOf(1), connectionUtils.getFromCatalog(c -> countCachedPlans(c, templateName)));

            // Runtime '? = ?' with false = true -> predicate false -> no rows; reuses the same plan (count stays 1).
            connectionUtils.runAgainstConnection(dbUri, schemaName, c -> {
                try (var ps = c.prepareStatement("select * from tb where ? = ?")) {
                    ps.setBoolean(1, false);
                    ps.setBoolean(2, true);
                    try (RelationalResultSet rs = ps.executeQuery()) {
                        Assertions.assertFalse(rs.next());
                    }
                }
            });
            Assertions.assertEquals(Long.valueOf(1), connectionUtils.getFromCatalog(c -> countCachedPlans(c, templateName)));
        }
    }

    @Test
    void storedQueriesBooleanConstraintFork() throws Exception {
        // A single value-free boolean stored query flag = ?{boolean}. It warms one plan whose literal constraint
        // is IS_NOT_NULL, which matches any non-null bound value — so the one warmed plan serves both flag = true
        // and flag = false at runtime, with no per-value fork and no manual enumeration.
        final String schemaTemplate =
                "CREATE TABLE tb(id bigint, flag boolean, PRIMARY KEY(id))" +
                        " CREATE INDEX i_flag AS SELECT flag FROM tb" +
                        " CREATE STORED QUERY by_flag_param AS select * from tb where flag = ?{boolean}";
        final String dbUri = "/TEST/STOREDQUERIES_BOOLFORK_DB";
        try (var ddl = Ddl.builder()
                .database(URI.create(dbUri))
                .relationalExtension(relationalExtension)
                .schemaTemplate(schemaTemplate)
                .build()) {
            final var connection = ddl.setSchemaAndGetConnection();
            final String templateName = ddl.getSchemaTemplateName();
            final String schemaName = connection.getSchema();

            try (var stmt = connection.createStatement()) {
                stmt.execute("INSERT INTO TB VALUES (1, true)");
                stmt.execute("INSERT INTO TB VALUES (2, false)");
                stmt.execute("INSERT INTO TB VALUES (3, true)");
            }

            // The value-free 'flag = ?{boolean}' warms a single plan ISCAN(I_FLAG [EQUALS @cov]) whose literal
            // constraint is IS_NOT_NULL. IS_NOT_NULL matches any non-null bound value, so this one plan serves both
            // true and false at runtime — no per-value fork.
            final var engineDriver = relationalExtension.getDriver(
                    com.apple.foundationdb.record.provider.foundationdb.FormatVersion.getDefaultFormatVersion());
            final var connectionUtils = new ConnectionUtils(engineDriver);
            Assertions.assertEquals(Long.valueOf(1), connectionUtils.getFromCatalog(c -> countCachedPlans(c, templateName)));

            // Runtime 'flag = ?' with true -> id 1, 3. Reuses the value-free plan; no new plan (count stays 1).
            connectionUtils.runAgainstConnection(dbUri, schemaName, c -> {
                try (var ps = c.prepareStatement("select * from tb where flag = ?")) {
                    ps.setBoolean(1, true);
                    try (RelationalResultSet rs = ps.executeQuery()) {
                        Assertions.assertTrue(rs.next());
                        Assertions.assertEquals(1, rs.getLong("ID"));
                        Assertions.assertTrue(rs.next());
                        Assertions.assertEquals(3, rs.getLong("ID"));
                        Assertions.assertFalse(rs.next());
                    }
                }
            });
            Assertions.assertEquals(Long.valueOf(1), connectionUtils.getFromCatalog(c -> countCachedPlans(c, templateName)));

            // A bound false value returns row id 2 and reuses the same warmed plan, so the cache still holds one plan.
            connectionUtils.runAgainstConnection(dbUri, schemaName, c -> {
                try (var ps = c.prepareStatement("select * from tb where flag = ?")) {
                    ps.setBoolean(1, false);
                    try (RelationalResultSet rs = ps.executeQuery()) {
                        Assertions.assertTrue(rs.next());
                        Assertions.assertEquals(2, rs.getLong("ID"));
                        Assertions.assertFalse(rs.next());
                    }
                }
            });
            Assertions.assertEquals(Long.valueOf(1), connectionUtils.getFromCatalog(c -> countCachedPlans(c, templateName)));
        }
    }

    @Test
    void badStoredQuery() {
        // Stored query body has a typo (`select1` rather than `select`) — DDL fails to parse.
        final String schemaTemplate =
                "CREATE TABLE t1(id bigint, col1 bigint, col2 bigint, PRIMARY KEY(id))" +
                        " CREATE STORED QUERY by_col1 AS select1 * from t1 where col1 = 10";
        RelationalAssertions.assertThrowsSqlException(() ->
                Ddl.builder()
                        .database(URI.create("/TEST/BADSTOREDQUERY_DB"))
                        .relationalExtension(relationalExtension)
                        .schemaTemplate(schemaTemplate)
                        .build())
                .hasErrorCode(ErrorCode.SYNTAX_ERROR);
    }

    @Test
    void storedQueryDdl() {
        // Stored query body is itself a DDL statement — rejected by the grammar.
        final String schemaTemplate =
                "CREATE TABLE t1(id bigint, col1 bigint, col2 bigint, PRIMARY KEY(id))" +
                        " CREATE STORED QUERY ddl_t AS CREATE TABLE t2(id bigint, col1 bigint, PRIMARY KEY(id))";
        RelationalAssertions.assertThrowsSqlException(() ->
                Ddl.builder()
                        .database(URI.create("/TEST/DDLSTOREDQUERY_DB"))
                        .relationalExtension(relationalExtension)
                        .schemaTemplate(schemaTemplate)
                        .build())
                .hasErrorCode(ErrorCode.SYNTAX_ERROR);
    }

    @Test
    void storedQueryBadColumn() throws Exception {
        // Stored query references a column that does not exist on the table.
        final String schemaTemplate =
                "CREATE TABLE t1(id bigint, col1 bigint, col2 bigint, PRIMARY KEY(id))" +
                        " CREATE INDEX i1 AS SELECT col1 FROM t1" +
                        " CREATE STORED QUERY by_col1 AS select * from t1 where col3 = 10" + // col3 does not exit
                        " CREATE STORED QUERY by_id AS select * from t1 where id = 1";
        final String dbUri = "/TEST/STOREDQUERIES_DB5";
        try (var ddl = Ddl.builder()
                .database(URI.create(dbUri))
                .relationalExtension(relationalExtension)
                .schemaTemplate(schemaTemplate)
                .build()) {
            final var connection = ddl.setSchemaAndGetConnection();
            final String templateName = ddl.getSchemaTemplateName();

            Assertions.assertEquals(0, countCachedPlans(connection, templateName));

            try (var stmt = connection.createStatement()) {
                stmt.execute("INSERT INTO T1 VALUES (1, 10, 1)");
                stmt.execute("INSERT INTO T1 VALUES (2, 20, 2)");
                stmt.execute("INSERT INTO T1 VALUES (3, 30, 3)");
            }
            Assertions.assertEquals(0, countCachedPlans(connection, templateName)); // we do not generate plans at ddl execution for now

            // create a new engine
            final var engineDriver = relationalExtension.getDriver(
                    com.apple.foundationdb.record.provider.foundationdb.FormatVersion.getDefaultFormatVersion());
            final var connectionUtils = new ConnectionUtils(engineDriver);

            // OfflineStoredQueriesProcessor ran during fresh-engine construction and
            // both stored queries attempted to generate plan
            Assertions.assertEquals(2, eventCounterCount(RelationalMetric.RelationalCount.PLAN_CACHE_TERTIARY_MISS));
            Assertions.assertEquals(0, eventCounterCount(RelationalMetric.RelationalCount.PLAN_CACHE_TERTIARY_HIT));

            // but only one query has valid column and was planned
            Assertions.assertEquals(Long.valueOf(1), connectionUtils.getFromCatalog(c -> countCachedPlans(c, templateName)));
        }
    }

    @Test
    void tempFuncIsStored() throws Exception {
        try (var ddl = Ddl.builder()
                .database(URI.create("/TEST/SQ_TF_PERSIST"))
                .relationalExtension(relationalExtension)
                .schemaTemplate(SCHEMA_TEMPLATE_TF_SINGLE)
                .build()) {
            final var connection = ddl.setSchemaAndGetConnection();
            final var embeddedConnection = connection.unwrap(EmbeddedRelationalConnection.class);
            embeddedConnection.setAutoCommit(false);
            embeddedConnection.createNewTransaction();
            final var schemaTemplate = embeddedConnection.getSchemaTemplate().unwrap(RecordLayerSchemaTemplate.class);
            embeddedConnection.rollback();
            embeddedConnection.setAutoCommit(true);

            final var storedQueries = schemaTemplate.getStoredQueries();
            Assertions.assertEquals(1, storedQueries.size());

            final var sq = storedQueries.get("BY_X");
            Assertions.assertNotNull(sq);
            Assertions.assertEquals("SELECT * FROM sq1(10)", sq.getQuery());
            Assertions.assertEquals(1, sq.getTempFunctions().size());
            final var tempFuncSource = sq.getTempFunctions().get(0);
            Assertions.assertTrue(tempFuncSource.startsWith("CREATE TEMPORARY FUNCTION sq1"),
                    "Expected CREATE TEMPORARY FUNCTION text, got: " + tempFuncSource);
        }
    }

    @Test
    void startupPlanGenerationWithTempFunc() throws Exception {
        try (var ddl = Ddl.builder()
                .database(URI.create("/TEST/SQ_TF_SINGLE"))
                .relationalExtension(relationalExtension)
                .schemaTemplate(SCHEMA_TEMPLATE_TF_SINGLE)
                .build()) {
            final String templateName = ddl.getSchemaTemplateName();

            // fresh engine triggers OfflineStoredQueriesProcessor
            final var engineDriver = relationalExtension.getDriver(
                    com.apple.foundationdb.record.provider.foundationdb.FormatVersion.getDefaultFormatVersion());
            final var connectionUtils = new ConnectionUtils(engineDriver);

            // The stored query SELECT (which calls the temp function) is planned and cached.
            Assertions.assertEquals(Long.valueOf(1), connectionUtils.getFromCatalog(c -> countCachedPlans(c, templateName)));
            // exactly one TERTIARY_MISS — for the stored query SELECT (DDL planning of the temp
            // function itself goes through CACHE_BYPASS and does not bump TERTIARY counters).
            Assertions.assertEquals(1, eventCounterCount(RelationalMetric.RelationalCount.PLAN_CACHE_TERTIARY_MISS));
            Assertions.assertEquals(0, eventCounterCount(RelationalMetric.RelationalCount.PLAN_CACHE_TERTIARY_HIT));
        }
    }

    @Test
    void storedQueriesUsageWithTempFunc() throws Exception {
        final String dbUri = "/TEST/SQ_TF_USAGE";
        try (var ddl = Ddl.builder()
                .database(URI.create(dbUri))
                .relationalExtension(relationalExtension)
                .schemaTemplate(SCHEMA_TEMPLATE_TF_SINGLE)
                .build()) {
            final var connection = ddl.setSchemaAndGetConnection();
            final String templateName = ddl.getSchemaTemplateName();
            final String schemaName = connection.getSchema();

            try (var stmt = connection.createStatement()) {
                stmt.execute("INSERT INTO T1 VALUES (1, 10, 1)");
                stmt.execute("INSERT INTO T1 VALUES (2, 20, 2)");
                stmt.execute("INSERT INTO T1 VALUES (3, 30, 3)");
            }

            // fresh engine triggers OfflineStoredQueriesProcessor
            final var engineDriver = relationalExtension.getDriver(
                    com.apple.foundationdb.record.provider.foundationdb.FormatVersion.getDefaultFormatVersion());
            final var connectionUtils = new ConnectionUtils(engineDriver);

            // pre-warmed: 1 stored query (SELECT * FROM sq1(10)) cached.
            Assertions.assertEquals(1, eventCounterCount(RelationalMetric.RelationalCount.PLAN_CACHE_TERTIARY_MISS));
            Assertions.assertEquals(0, eventCounterCount(RelationalMetric.RelationalCount.PLAN_CACHE_TERTIARY_HIT));
            Assertions.assertEquals(Long.valueOf(1), connectionUtils.getFromCatalog(c -> countCachedPlans(c, templateName)));

            // The runtime user installs the same temp function in their session, then runs the
            // canonical SELECT. The cache lookup should hit the pre-warmed plan.
            connectionUtils.runAgainstConnection(dbUri, schemaName, c -> {
                c.setAutoCommit(false);
                try (var stmt = c.createStatement()) {
                    stmt.execute("CREATE TEMPORARY FUNCTION sq1(in x bigint) ON COMMIT DROP FUNCTION " +
                            "AS SELECT * FROM t1 WHERE col1 < 40 + x");
                    try (RelationalResultSet rs = stmt.executeQuery("SELECT * FROM sq1(10)")) {
                        // sq1(10) → SELECT * FROM t1 WHERE col1 < 50 → all three rows.
                        Assertions.assertTrue(rs.next());
                        Assertions.assertEquals(1, rs.getLong("ID"));
                        Assertions.assertTrue(rs.next());
                        Assertions.assertEquals(2, rs.getLong("ID"));
                        Assertions.assertTrue(rs.next());
                        Assertions.assertEquals(3, rs.getLong("ID"));
                        Assertions.assertFalse(rs.next());
                    }
                }
                c.rollback();
            });

            // SELECT hit the pre-warmed cache: hit +1, miss unchanged, cache size unchanged.
            Assertions.assertEquals(1, eventCounterCount(RelationalMetric.RelationalCount.PLAN_CACHE_TERTIARY_HIT));
            Assertions.assertEquals(1, eventCounterCount(RelationalMetric.RelationalCount.PLAN_CACHE_TERTIARY_MISS));
            Assertions.assertEquals(Long.valueOf(1), connectionUtils.getFromCatalog(c -> countCachedPlans(c, templateName)));
        }
    }

    @Test
    void startupPlanGenerationChained() throws Exception {
        // Chained temp functions.
        final String schemaTemplate =
                "CREATE TABLE t1(id bigint, col1 bigint, col2 bigint, PRIMARY KEY(id))" +
                        " CREATE INDEX i1 AS SELECT col1 FROM t1" +
                        " CREATE STORED QUERY by_chained" +
                        "   DECLARE" +
                        "       FUNCTION sq1(in x bigint) AS (SELECT * FROM t1 WHERE col1 < x);" +
                        "       FUNCTION sq2(in x bigint) AS (SELECT * FROM sq1(x + 1))" +
                        " AS SELECT * FROM sq2(50)";
        try (var ddl = Ddl.builder()
                .database(URI.create("/TEST/SQ_TF_CHAINED"))
                .relationalExtension(relationalExtension)
                .schemaTemplate(schemaTemplate)
                .build()) {
            final String templateName = ddl.getSchemaTemplateName();

            final var engineDriver = relationalExtension.getDriver(
                    com.apple.foundationdb.record.provider.foundationdb.FormatVersion.getDefaultFormatVersion());
            final var connectionUtils = new ConnectionUtils(engineDriver);

            // sq2 references sq1; both must install correctly for the SELECT to plan.
            Assertions.assertEquals(Long.valueOf(1), connectionUtils.getFromCatalog(c -> countCachedPlans(c, templateName)));
            Assertions.assertEquals(1, eventCounterCount(RelationalMetric.RelationalCount.PLAN_CACHE_TERTIARY_MISS));
            Assertions.assertEquals(0, eventCounterCount(RelationalMetric.RelationalCount.PLAN_CACHE_TERTIARY_HIT));
        }
    }

    @Test
    void badTempFunc() throws Exception {
        // The first stored query's temp function references a column that does not exist.
        final String schemaTemplate =
                "CREATE TABLE t1(id bigint, col1 bigint, col2 bigint, PRIMARY KEY(id))" +
                        " CREATE INDEX i1 AS SELECT col1 FROM t1" +
                        " CREATE STORED QUERY by_bad" +
                        "   DECLARE" +
                        "       FUNCTION sq_bad() AS (SELECT * FROM t1 WHERE col_does_not_exist = 1)" +
                        " AS SELECT * FROM sq_bad()" +
                        " CREATE STORED QUERY by_good" +
                        "   DECLARE" +
                        "       FUNCTION sq_good() AS (SELECT * FROM t1 WHERE col1 = 10)" +
                        " AS SELECT * FROM sq_good()";
        try (var ddl = Ddl.builder()
                .database(URI.create("/TEST/SQ_TF_BAD"))
                .relationalExtension(relationalExtension)
                .schemaTemplate(schemaTemplate)
                .build()) {
            final String templateName = ddl.getSchemaTemplateName();

            final var engineDriver = relationalExtension.getDriver(
                    com.apple.foundationdb.record.provider.foundationdb.FormatVersion.getDefaultFormatVersion());
            final var connectionUtils = new ConnectionUtils(engineDriver);

            // The stored query whose temp function fails to compile is skipped; the good one still plans.
            Assertions.assertEquals(Long.valueOf(1), connectionUtils.getFromCatalog(c -> countCachedPlans(c, templateName)));
        }
    }

    @Test
    void tempFuncBadSyntax() {
        // Typo in the temp-function keyword  — DDL fails to parse.
        final String schemaTemplate =
                "CREATE TABLE t1(id bigint, col1 bigint, col2 bigint, PRIMARY KEY(id))" +
                        " CREATE STORED QUERY by_x" +
                        "   DECLARE" +
                        "       FUNCTION1 sq1(in x bigint) AS (SELECT * FROM t1 WHERE col1 < x)" +
                        " AS SELECT * FROM sq1(10)";
        RelationalAssertions.assertThrowsSqlException(() ->
                        Ddl.builder()
                                .database(URI.create("/TEST/SQ_TF_BAD_SYNTAX_DB"))
                                .relationalExtension(relationalExtension)
                                .schemaTemplate(schemaTemplate)
                                .build())
                .hasErrorCode(ErrorCode.SYNTAX_ERROR);
    }

}
