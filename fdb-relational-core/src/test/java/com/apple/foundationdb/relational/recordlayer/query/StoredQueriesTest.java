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

public class StoredQueriesTest {

    private static final String SCHEMA_TEMPLATE =
            "CREATE TABLE t1(id bigint, col1 bigint, col2 bigint, PRIMARY KEY(id))" +
                    " CREATE INDEX i1 AS SELECT col1 FROM t1" +
                    " CREATE QUERY by_col1 AS select * from t1 where col1 = 10" +
                    " CREATE QUERY by_id AS select * from t1 where id = 1";

    /** Stored query body has a typo (`select1` rather than `select`) — DDL fails to parse. */
    private static final String SCHEMA_TEMPLATE_BAD_SYNTAX =
            "CREATE TABLE t1(id bigint, col1 bigint, col2 bigint, PRIMARY KEY(id))" +
                    " CREATE QUERY by_col1 AS select1 * from t1 where col1 = 10";

    /** Stored query body is itself a DDL statement — rejected by the grammar. */
    private static final String SCHEMA_TEMPLATE_DDL_IN_QUERY =
            "CREATE TABLE t1(id bigint, col1 bigint, col2 bigint, PRIMARY KEY(id))" +
                    " CREATE QUERY ddl_t AS CREATE TABLE t2(id bigint, col1 bigint, PRIMARY KEY(id))";

    /** Stored query references a column that does not exist on the table. */
    private static final String SCHEMA_TEMPLATE_BAD_COLUMN =
            "CREATE TABLE t1(id bigint, col1 bigint, col2 bigint, PRIMARY KEY(id))" +
                    " CREATE INDEX i1 AS SELECT col1 FROM t1" +
                    " CREATE QUERY by_col1 AS select * from t1 where col3 = 10" + // col3 does not exit
                    " CREATE QUERY by_id AS select * from t1 where id = 1";

    /** One stored query whose body calls a single temp function. */
    private static final String SCHEMA_TEMPLATE_TF_SINGLE =
            "CREATE TABLE t1(id bigint, col1 bigint, col2 bigint, PRIMARY KEY(id))" +
                    " CREATE INDEX i1 AS SELECT col1 FROM t1" +
                    " CREATE QUERY by_x" +
                    "   WITH CREATE TEMPORARY FUNCTION sq1(in x bigint) ON COMMIT DROP FUNCTION" +
                    "       AS SELECT * FROM t1 WHERE col1 < 40 + x" +
                    " AS SELECT * FROM sq1(10)";

    /**
     * Chained temp functions.
     */
    private static final String SCHEMA_TEMPLATE_TF_CHAINED =
            "CREATE TABLE t1(id bigint, col1 bigint, col2 bigint, PRIMARY KEY(id))" +
                    " CREATE INDEX i1 AS SELECT col1 FROM t1" +
                    " CREATE QUERY by_chained" +
                    "   WITH CREATE TEMPORARY FUNCTION sq1(in x bigint) ON COMMIT DROP FUNCTION" +
                    "       AS SELECT * FROM t1 WHERE col1 < x" +
                    "   WITH CREATE TEMPORARY FUNCTION sq2(in x bigint) ON COMMIT DROP FUNCTION" +
                    "       AS SELECT * FROM sq1(x + 1)" +
                    " AS SELECT * FROM sq2(50)";

    /** The first stored query's temp function references a column that does not exist. */
    private static final String SCHEMA_TEMPLATE_TF_BAD =
            "CREATE TABLE t1(id bigint, col1 bigint, col2 bigint, PRIMARY KEY(id))" +
                    " CREATE INDEX i1 AS SELECT col1 FROM t1" +
                    " CREATE QUERY by_bad" +
                    "   WITH CREATE TEMPORARY FUNCTION sq_bad() ON COMMIT DROP FUNCTION" +
                    "       AS SELECT * FROM t1 WHERE col_does_not_exist = 1" +
                    " AS SELECT * FROM sq_bad()" +
                    " CREATE QUERY by_good" +
                    "   WITH CREATE TEMPORARY FUNCTION sq_good() ON COMMIT DROP FUNCTION" +
                    "       AS SELECT * FROM t1 WHERE col1 = 10" +
                    " AS SELECT * FROM sq_good()";

    /** Typo in the temp-function keyword  — DDL fails to parse. */
    private static final String SCHEMA_TEMPLATE_TF_BAD_SYNTAX =
            "CREATE TABLE t1(id bigint, col1 bigint, col2 bigint, PRIMARY KEY(id))" +
                    " CREATE QUERY by_x" +
                    "   WITH CREATE TEMPORARY FUNCTION1 sq1(in x bigint) ON COMMIT DROP FUNCTION" +
                    "       AS SELECT * FROM t1 WHERE col1 < x" +
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
    void badStoredQuery() {
        RelationalAssertions.assertThrowsSqlException(() ->
                Ddl.builder()
                        .database(URI.create("/TEST/BADSTOREDQUERY_DB"))
                        .relationalExtension(relationalExtension)
                        .schemaTemplate(SCHEMA_TEMPLATE_BAD_SYNTAX)
                        .build())
                .hasErrorCode(ErrorCode.SYNTAX_ERROR);
    }

    @Test
    void storedQueryDdl() {
        RelationalAssertions.assertThrowsSqlException(() ->
                Ddl.builder()
                        .database(URI.create("/TEST/DDLSTOREDQUERY_DB"))
                        .relationalExtension(relationalExtension)
                        .schemaTemplate(SCHEMA_TEMPLATE_DDL_IN_QUERY)
                        .build())
                .hasErrorCode(ErrorCode.SYNTAX_ERROR);
    }

    @Test
    void storedQueryBadColumn() throws Exception {
        final String dbUri = "/TEST/STOREDQUERIES_DB5";
        try (var ddl = Ddl.builder()
                .database(URI.create(dbUri))
                .relationalExtension(relationalExtension)
                .schemaTemplate(SCHEMA_TEMPLATE_BAD_COLUMN)
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
            final var freshDriver = relationalExtension.getDriver(
                    com.apple.foundationdb.record.provider.foundationdb.FormatVersion.getDefaultFormatVersion());
            final var freshUtils = new ConnectionUtils(freshDriver);

            // The stored query SELECT (which calls the temp function) is planned and cached.
            Assertions.assertEquals(Long.valueOf(1), freshUtils.getFromCatalog(c -> countCachedPlans(c, templateName)));
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
            final var freshDriver = relationalExtension.getDriver(
                    com.apple.foundationdb.record.provider.foundationdb.FormatVersion.getDefaultFormatVersion());
            final var freshUtils = new ConnectionUtils(freshDriver);

            // pre-warmed: 1 stored query (SELECT * FROM sq1(10)) cached.
            Assertions.assertEquals(1, eventCounterCount(RelationalMetric.RelationalCount.PLAN_CACHE_TERTIARY_MISS));
            Assertions.assertEquals(0, eventCounterCount(RelationalMetric.RelationalCount.PLAN_CACHE_TERTIARY_HIT));
            Assertions.assertEquals(Long.valueOf(1), freshUtils.getFromCatalog(c -> countCachedPlans(c, templateName)));

            // The runtime user installs the same temp function in their session, then runs the
            // canonical SELECT. The cache lookup should hit the pre-warmed plan.
            freshUtils.runAgainstConnection(dbUri, schemaName, c -> {
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
            Assertions.assertEquals(Long.valueOf(1), freshUtils.getFromCatalog(c -> countCachedPlans(c, templateName)));
        }
    }

    @Test
    void startupPlanGenerationChained() throws Exception {
        try (var ddl = Ddl.builder()
                .database(URI.create("/TEST/SQ_TF_CHAINED"))
                .relationalExtension(relationalExtension)
                .schemaTemplate(SCHEMA_TEMPLATE_TF_CHAINED)
                .build()) {
            final String templateName = ddl.getSchemaTemplateName();

            final var freshDriver = relationalExtension.getDriver(
                    com.apple.foundationdb.record.provider.foundationdb.FormatVersion.getDefaultFormatVersion());
            final var freshUtils = new ConnectionUtils(freshDriver);

            // sq2 references sq1; both must install correctly for the SELECT to plan.
            Assertions.assertEquals(Long.valueOf(1), freshUtils.getFromCatalog(c -> countCachedPlans(c, templateName)));
            Assertions.assertEquals(1, eventCounterCount(RelationalMetric.RelationalCount.PLAN_CACHE_TERTIARY_MISS));
            Assertions.assertEquals(0, eventCounterCount(RelationalMetric.RelationalCount.PLAN_CACHE_TERTIARY_HIT));
        }
    }

    @Test
    void badTempFunc() throws Exception {
        try (var ddl = Ddl.builder()
                .database(URI.create("/TEST/SQ_TF_BAD"))
                .relationalExtension(relationalExtension)
                .schemaTemplate(SCHEMA_TEMPLATE_TF_BAD)
                .build()) {
            final String templateName = ddl.getSchemaTemplateName();

            final var freshDriver = relationalExtension.getDriver(
                    com.apple.foundationdb.record.provider.foundationdb.FormatVersion.getDefaultFormatVersion());
            final var freshUtils = new ConnectionUtils(freshDriver);

            // The stored query whose temp function fails to compile is skipped; the good one still plans.
            Assertions.assertEquals(Long.valueOf(1), freshUtils.getFromCatalog(c -> countCachedPlans(c, templateName)));
        }
    }

    @Test
    void tempFuncBadSyntax() {
        RelationalAssertions.assertThrowsSqlException(() ->
                        Ddl.builder()
                                .database(URI.create("/TEST/SQ_TF_BAD_SYNTAX_DB"))
                                .relationalExtension(relationalExtension)
                                .schemaTemplate(SCHEMA_TEMPLATE_TF_BAD_SYNTAX)
                                .build())
                .hasErrorCode(ErrorCode.SYNTAX_ERROR);
    }

}
