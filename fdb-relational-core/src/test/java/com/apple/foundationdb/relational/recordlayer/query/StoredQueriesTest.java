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

import javax.annotation.Nonnull;
import java.net.URI;
import java.sql.SQLException;
import java.util.Map;

public class StoredQueriesTest {

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

    // ---------------------------------------------------------------------------------------------------------------
    // Basic stored queries (literal bodies)
    // ---------------------------------------------------------------------------------------------------------------

    private static final String SCHEMA_TEMPLATE =
            "CREATE TABLE t1(id bigint, col1 bigint, col2 bigint, PRIMARY KEY(id))" +
                    " CREATE INDEX i1 AS SELECT col1 FROM t1" +
                    " CREATE STORED QUERY by_col1 AS select * from t1 where col1 = 10" +
                    " CREATE STORED QUERY by_id AS select * from t1 where id = 1";

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

    // ---------------------------------------------------------------------------------------------------------------
    // Error cases
    // ---------------------------------------------------------------------------------------------------------------

    /** Stored query body has a typo (`select1` rather than `select`) — DDL fails to parse. */
    private static final String SCHEMA_TEMPLATE_BAD_SYNTAX =
            "CREATE TABLE t1(id bigint, col1 bigint, col2 bigint, PRIMARY KEY(id))" +
                    " CREATE STORED QUERY by_col1 AS select1 * from t1 where col1 = 10";

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

    /** Stored query body is itself a DDL statement — rejected by the grammar. */
    private static final String SCHEMA_TEMPLATE_DDL_IN_QUERY =
            "CREATE TABLE t1(id bigint, col1 bigint, col2 bigint, PRIMARY KEY(id))" +
                    " CREATE STORED QUERY ddl_t AS CREATE TABLE t2(id bigint, col1 bigint, PRIMARY KEY(id))";

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

    /** Stored query references a column that does not exist on the table. */
    private static final String SCHEMA_TEMPLATE_BAD_COLUMN =
            "CREATE TABLE t1(id bigint, col1 bigint, col2 bigint, PRIMARY KEY(id))" +
                    " CREATE INDEX i1 AS SELECT col1 FROM t1" +
                    " CREATE STORED QUERY by_col1 AS select * from t1 where col3 = 10" + // col3 does not exit
                    " CREATE STORED QUERY by_id AS select * from t1 where id = 1";

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

    // ---------------------------------------------------------------------------------------------------------------
    // Declared (temporary) functions
    // ---------------------------------------------------------------------------------------------------------------

    /** One stored query whose body calls a single temp function. */
    private static final String SCHEMA_TEMPLATE_TF_SINGLE =
            "CREATE TABLE t1(id bigint, col1 bigint, col2 bigint, PRIMARY KEY(id))" +
                    " CREATE INDEX i1 AS SELECT col1 FROM t1" +
                    " CREATE STORED QUERY by_x" +
                    "   DECLARE" +
                    "       FUNCTION sq1(in x bigint) AS (SELECT * FROM t1 WHERE col1 < 40 + x)" +
                    " AS SELECT * FROM sq1(10)";

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

    /** Chained temp functions. */
    private static final String SCHEMA_TEMPLATE_TF_CHAINED =
            "CREATE TABLE t1(id bigint, col1 bigint, col2 bigint, PRIMARY KEY(id))" +
                    " CREATE INDEX i1 AS SELECT col1 FROM t1" +
                    " CREATE STORED QUERY by_chained" +
                    "   DECLARE" +
                    "       FUNCTION sq1(in x bigint) AS (SELECT * FROM t1 WHERE col1 < x);" +
                    "       FUNCTION sq2(in x bigint) AS (SELECT * FROM sq1(x + 1))" +
                    " AS SELECT * FROM sq2(50)";

    @Test
    void startupPlanGenerationChained() throws Exception {
        try (var ddl = Ddl.builder()
                .database(URI.create("/TEST/SQ_TF_CHAINED"))
                .relationalExtension(relationalExtension)
                .schemaTemplate(SCHEMA_TEMPLATE_TF_CHAINED)
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

    /** The first stored query's temp function references a column that does not exist. */
    private static final String SCHEMA_TEMPLATE_TF_BAD =
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

    @Test
    void badTempFunc() throws Exception {
        try (var ddl = Ddl.builder()
                .database(URI.create("/TEST/SQ_TF_BAD"))
                .relationalExtension(relationalExtension)
                .schemaTemplate(SCHEMA_TEMPLATE_TF_BAD)
                .build()) {
            final String templateName = ddl.getSchemaTemplateName();

            final var engineDriver = relationalExtension.getDriver(
                    com.apple.foundationdb.record.provider.foundationdb.FormatVersion.getDefaultFormatVersion());
            final var connectionUtils = new ConnectionUtils(engineDriver);

            // The stored query whose temp function fails to compile is skipped; the good one still plans.
            Assertions.assertEquals(Long.valueOf(1), connectionUtils.getFromCatalog(c -> countCachedPlans(c, templateName)));
        }
    }

    /** Typo in the temp-function keyword  — DDL fails to parse. */
    private static final String SCHEMA_TEMPLATE_TF_BAD_SYNTAX =
            "CREATE TABLE t1(id bigint, col1 bigint, col2 bigint, PRIMARY KEY(id))" +
                    " CREATE STORED QUERY by_x" +
                    "   DECLARE" +
                    "       FUNCTION1 sq1(in x bigint) AS (SELECT * FROM t1 WHERE col1 < x)" +
                    " AS SELECT * FROM sq1(10)";

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

    // ---------------------------------------------------------------------------------------------------------------
    // Typed-named-parameter signatures
    // ---------------------------------------------------------------------------------------------------------------

    /**
     * Stored query with a typed-named-parameter signature. {@code param_a} is captured inside the declared function
     * body (it is not the function's own parameter); {@code param_b} is passed as the function's argument in the outer
     * SELECT. At warm-up both are planned value-free from their declared types; at runtime the client re-issues the
     * equivalent SQL binding {@code ?param_a}/{@code ?param_b} by name.
     */
    private static final String SCHEMA_TEMPLATE_SIGNATURE =
            "CREATE TABLE t1(id bigint, col1 bigint, col2 bigint, PRIMARY KEY(id))" +
                    " CREATE INDEX i1 AS SELECT col1 FROM t1" +
                    " CREATE STORED QUERY by_sig(param_a bigint, param_b bigint)" +
                    "   DECLARE" +
                    "       FUNCTION f1(in p bigint) AS (SELECT * FROM t1 WHERE (p IS NULL OR col1 = p) AND col2 = param_a)" +
                    " AS SELECT id FROM f1(param_b)";

    @Test
    void signatureStoredQueryIsStored() throws Exception {
        try (var ddl = Ddl.builder()
                .database(URI.create("/TEST/SQ_SIGNATURE_PERSIST"))
                .relationalExtension(relationalExtension)
                .schemaTemplate(SCHEMA_TEMPLATE_SIGNATURE)
                .build()) {
            final var connection = ddl.setSchemaAndGetConnection();
            final var embeddedConnection = connection.unwrap(EmbeddedRelationalConnection.class);
            embeddedConnection.setAutoCommit(false);
            embeddedConnection.createNewTransaction();
            final var schemaTemplate = embeddedConnection.getSchemaTemplate().unwrap(RecordLayerSchemaTemplate.class);
            embeddedConnection.rollback();
            embeddedConnection.setAutoCommit(true);

            final var sq = schemaTemplate.getStoredQueries().get("BY_SIG");
            Assertions.assertNotNull(sq);
            // signature parameters were resolved to their record-layer type codes, keyed by name.
            Assertions.assertEquals(Map.of("param_a", "LONG", "param_b", "LONG"), sq.getParameters());
            // the argument reference param_b was rewritten to a named parameter ?param_b.
            Assertions.assertEquals("SELECT id FROM f1(?param_b)", sq.getQuery());
            // param_a, captured inside the function body, was rewritten to ?param_a; the function's own parameter p and
            // the columns are untouched.
            Assertions.assertEquals(1, sq.getTempFunctions().size());
            final var tempFunc = sq.getTempFunctions().get(0);
            Assertions.assertEquals(
                    "CREATE TEMPORARY FUNCTION f1(in p bigint) ON COMMIT DROP FUNCTION AS " +
                            "SELECT * FROM t1 WHERE (p IS NULL OR col1 = p) AND col2 = ?param_a",
                    tempFunc);
        }
    }

    /**
     * A parameter reference is recognised from its position in the parse tree, not from its spelling: only an
     * identifier used as a value is rewritten. These are the cases a text-level rewrite would corrupt.
     */
    @Test
    void signatureRewriteOnlyTouchesValuePositions() throws Exception {
        final var schemaTemplate =
                "CREATE TABLE t1(id bigint, col1 bigint, col2 bigint, PRIMARY KEY(id))" +
                        // col1 is also the name of a signature parameter, and is referenced qualified, as an alias,
                        // and bare.
                        " CREATE STORED QUERY by_pos(col1 bigint)" +
                        " AS SELECT t1.col1 AS col1, col2 AS aliased FROM t1 WHERE col2 = col1";
        try (var ddl = Ddl.builder()
                .database(URI.create("/TEST/SQ_SIGNATURE_POSITIONS"))
                .relationalExtension(relationalExtension)
                .schemaTemplate(schemaTemplate)
                .build()) {
            final var connection = ddl.setSchemaAndGetConnection();
            final var embeddedConnection = connection.unwrap(EmbeddedRelationalConnection.class);
            embeddedConnection.setAutoCommit(false);
            embeddedConnection.createNewTransaction();
            final var template = embeddedConnection.getSchemaTemplate().unwrap(RecordLayerSchemaTemplate.class);
            embeddedConnection.rollback();
            embeddedConnection.setAutoCommit(true);

            final var sq = template.getStoredQueries().get("BY_POS");
            Assertions.assertNotNull(sq);
            // t1.col1 stays qualified (it can only be a column), AS col1 stays an alias, and only the bare reference
            // in the WHERE predicate becomes a parameter.
            Assertions.assertEquals("SELECT t1.col1 AS col1, col2 AS aliased FROM t1 WHERE col2 = ?col1",
                    sq.getQuery());
        }
    }

    @Test
    void signatureParameterCollidingWithFunctionParameterIsRejected() throws Exception {
        // p names both a signature parameter and the declared function's own parameter. In the body the two are
        // indistinguishable, so this is rejected rather than silently capturing the function's parameter.
        assertSchemaTemplateRejected(
                "CREATE TABLE t1(id bigint, col1 bigint, PRIMARY KEY(id))" +
                        " CREATE STORED QUERY by_collision(p bigint)" +
                        "   DECLARE FUNCTION f1(in p bigint) AS (SELECT * FROM t1 WHERE col1 = p)" +
                        " AS SELECT id FROM f1(p)",
                "/TEST/SQ_SIGNATURE_COLLISION");
    }

    @Test
    void signatureParameterCollidingWithFunctionParameterInDifferentCaseIsRejected() throws Exception {
        // x and X are the same identifier unless the connection is case-sensitive, so this is the same collision as
        // above even though the rewrite itself matches parameter names exactly.
        assertSchemaTemplateRejected(
                "CREATE TABLE t1(id bigint, col1 bigint, PRIMARY KEY(id))" +
                        " CREATE STORED QUERY by_case_collision(x bigint)" +
                        "   DECLARE FUNCTION f1(in X bigint) AS (SELECT * FROM t1 WHERE col1 = x)" +
                        " AS SELECT id FROM f1(x)",
                "/TEST/SQ_SIGNATURE_CASE_COLLISION");
    }

    @Test
    void duplicateSignatureParameterIsRejected() throws Exception {
        assertSchemaTemplateRejected(
                "CREATE TABLE t1(id bigint, col1 bigint, PRIMARY KEY(id))" +
                        " CREATE STORED QUERY by_dup(param_a bigint, param_a bigint)" +
                        " AS SELECT id FROM t1 WHERE col1 = param_a",
                "/TEST/SQ_SIGNATURE_DUPLICATE");
    }

    @Test
    void nonPrimitiveSignatureParameterIsRejected() throws Exception {
        assertSchemaTemplateRejected(
                "CREATE TABLE t1(id bigint, col1 bigint, PRIMARY KEY(id))" +
                        " CREATE STORED QUERY by_array(param_a bigint array)" +
                        " AS SELECT id FROM t1 WHERE col1 = param_a",
                "/TEST/SQ_SIGNATURE_ARRAY");
    }

    @Test
    void quotedSignatureParameterIsRejected() throws Exception {
        // A quoted name could never be matched as a reference, so it is refused rather than silently ignored.
        assertSchemaTemplateRejected(
                "CREATE TABLE t1(id bigint, col1 bigint, PRIMARY KEY(id))" +
                        " CREATE STORED QUERY by_quoted(\"param_a\" bigint)" +
                        " AS SELECT id FROM t1 WHERE col1 = col1",
                "/TEST/SQ_SIGNATURE_QUOTED");
    }

    private void assertSchemaTemplateRejected(@Nonnull final String schemaTemplate,
                                              @Nonnull final String databaseUri) {
        Assertions.assertThrows(SQLException.class, () -> {
            try (var ddl = Ddl.builder()
                    .database(URI.create(databaseUri))
                    .relationalExtension(relationalExtension)
                    .schemaTemplate(schemaTemplate)
                    .build()) {
                ddl.setSchemaAndGetConnection();
            }
        });
    }

    @Test
    void startupPlanGenerationWithSignature() throws Exception {
        try (var ddl = Ddl.builder()
                .database(URI.create("/TEST/SQ_SIGNATURE_WARMUP"))
                .relationalExtension(relationalExtension)
                .schemaTemplate(SCHEMA_TEMPLATE_SIGNATURE)
                .build()) {
            final String templateName = ddl.getSchemaTemplateName();

            // fresh engine triggers OfflineStoredQueriesProcessor, which plans the signature query value-free.
            final var engineDriver = relationalExtension.getDriver(
                    com.apple.foundationdb.record.provider.foundationdb.FormatVersion.getDefaultFormatVersion());
            final var connectionUtils = new ConnectionUtils(engineDriver);

            // The stored query SELECT (calling the temp function, both signature params value-free) is planned + cached.
            Assertions.assertEquals(Long.valueOf(1), connectionUtils.getFromCatalog(c -> countCachedPlans(c, templateName)));
            Assertions.assertEquals(1, eventCounterCount(RelationalMetric.RelationalCount.PLAN_CACHE_TERTIARY_MISS));
            Assertions.assertEquals(0, eventCounterCount(RelationalMetric.RelationalCount.PLAN_CACHE_TERTIARY_HIT));
        }
    }

    @Test
    void storedQueriesUsageWithSignature() throws Exception {
        final String dbUri = "/TEST/SQ_SIGNATURE_USAGE";
        try (var ddl = Ddl.builder()
                .database(URI.create(dbUri))
                .relationalExtension(relationalExtension)
                .schemaTemplate(SCHEMA_TEMPLATE_SIGNATURE)
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

            // pre-warmed: 1 value-free plan for the signature query.
            Assertions.assertEquals(1, eventCounterCount(RelationalMetric.RelationalCount.PLAN_CACHE_TERTIARY_MISS));
            Assertions.assertEquals(0, eventCounterCount(RelationalMetric.RelationalCount.PLAN_CACHE_TERTIARY_HIT));
            Assertions.assertEquals(Long.valueOf(1), connectionUtils.getFromCatalog(c -> countCachedPlans(c, templateName)));

            // The runtime user re-declares the temp function (binding ?param_a by name) and runs the canonical SELECT
            // (binding ?param_b by name). The cache lookup should hit the pre-warmed value-free plan.
            connectionUtils.runAgainstConnection(dbUri, schemaName, c -> {
                c.setAutoCommit(false);
                try (var ps = c.prepareStatement(
                        "CREATE TEMPORARY FUNCTION f1(in p bigint) ON COMMIT DROP FUNCTION AS " +
                                "SELECT * FROM t1 WHERE (p IS NULL OR col1 = p) AND col2 = ?param_a")) {
                    ps.setLong("param_a", 1L);
                    ps.execute();
                }
                try (var ps = c.prepareStatement("SELECT id FROM f1(?param_b)")) {
                    ps.setLong("param_b", 10L);
                    try (RelationalResultSet rs = ps.executeQuery()) {
                        // f1(10): (10 IS NULL OR col1 = 10) AND col2 = 1 → row (1, 10, 1) → id = 1.
                        Assertions.assertTrue(rs.next());
                        Assertions.assertEquals(1, rs.getLong("ID"));
                        Assertions.assertFalse(rs.next());
                    }
                }
                c.rollback();
            });

            // SELECT hit the pre-warmed value-free plan: hit +1, miss unchanged, cache size unchanged.
            Assertions.assertEquals(1, eventCounterCount(RelationalMetric.RelationalCount.PLAN_CACHE_TERTIARY_HIT));
            Assertions.assertEquals(1, eventCounterCount(RelationalMetric.RelationalCount.PLAN_CACHE_TERTIARY_MISS));
            Assertions.assertEquals(Long.valueOf(1), connectionUtils.getFromCatalog(c -> countCachedPlans(c, templateName)));
        }
    }

    /**
     * A sparse index can only be matched by proving its predicate covers the query range, which needs the parameter's
     * concrete value. A value-free parameter has none, so the query fails to warm rather than silently warming to a
     * scan — a scan plan would satisfy the parameter's {@code IS_NOT_NULL} constraint and so be reused at runtime in
     * place of the index plan, leaving the query permanently worse off than if it had never been warmed. The failure is
     * per stored query: the sibling below, which does not touch the sparse index, still warms.
     */
    private static final String SCHEMA_TEMPLATE_SIGNATURE_SPARSE_INDEX =
            "CREATE TABLE t1(id bigint, col1 bigint, col2 bigint, PRIMARY KEY(id))" +
                    " CREATE INDEX sparse1 AS SELECT col1 FROM t1 WHERE col1 > 42" +
                    " CREATE STORED QUERY needs_value(param_a bigint)" +
                    "   AS SELECT id FROM t1 WHERE col1 > param_a" +
                    " CREATE STORED QUERY by_other(param_b bigint)" +
                    "   AS SELECT id FROM t1 WHERE col2 = param_b";

    @Test
    void signatureParameterAgainstFilteredIndexFailsThatQueryOnly() throws Exception {
        try (var ddl = Ddl.builder()
                .database(URI.create("/TEST/SQ_SIGNATURE_SPARSE"))
                .relationalExtension(relationalExtension)
                .schemaTemplate(SCHEMA_TEMPLATE_SIGNATURE_SPARSE_INDEX)
                .build()) {
            final var connection = ddl.setSchemaAndGetConnection();
            final String templateName = ddl.getSchemaTemplateName();

            // fresh engine triggers OfflineStoredQueriesProcessor
            final var engineDriver = relationalExtension.getDriver(
                    com.apple.foundationdb.record.provider.foundationdb.FormatVersion.getDefaultFormatVersion());
            final var connectionUtils = new ConnectionUtils(engineDriver);

            // needs_value could only be matched against the sparse index with a concrete value, so it is skipped and
            // logged; by_other has nothing to prove and warms normally. One plan cached, not two.
            Assertions.assertEquals(Long.valueOf(1), connectionUtils.getFromCatalog(c -> countCachedPlans(c, templateName)));
        }
    }

    @Test
    void booleanSignatureParameterIsRejected() throws Exception {
        // A boolean's value is plan-determining, so it belongs in the body as a literal where the planner specializes on
        // it and the IS_TRUE/IS_FALSE constraint keeps the two plans distinct. As a value-free parameter it could carry
        // only "is not null", giving one unspecialized plan for both values.
        assertSchemaTemplateRejected(
                "CREATE TABLE t1(id bigint, flag boolean, PRIMARY KEY(id))" +
                        " CREATE STORED QUERY by_flag(param_flag boolean)" +
                        " AS SELECT id FROM t1 WHERE flag = param_flag",
                "/TEST/SQ_SIGNATURE_BOOLEAN");
    }

    /**
     * Same as {@link #SCHEMA_TEMPLATE_SIGNATURE} but {@code param_b} is declared exactly-NULL. Warm-up plans it
     * value-free as the NULL type, so the planner specializes the plan for {@code param_b IS NULL} (the {@code col1 = p}
     * probe folds away). At runtime the client binds {@code param_b} with {@code setNull} to hit this plan.
     */
    private static final String SCHEMA_TEMPLATE_SIGNATURE_NULL =
            "CREATE TABLE t1(id bigint, col1 bigint, col2 bigint, PRIMARY KEY(id))" +
                    " CREATE INDEX i1 AS SELECT col1 FROM t1" +
                    " CREATE STORED QUERY by_sig_null(param_a bigint, param_b null)" +
                    "   DECLARE" +
                    "       FUNCTION f1(in p bigint) AS (SELECT * FROM t1 WHERE (p IS NULL OR col1 = p) AND col2 = param_a)" +
                    " AS SELECT id FROM f1(param_b)";

    @Test
    void nullSignatureStoredQueryIsStored() throws Exception {
        try (var ddl = Ddl.builder()
                .database(URI.create("/TEST/SQ_SIGNATURE_NULL_PERSIST"))
                .relationalExtension(relationalExtension)
                .schemaTemplate(SCHEMA_TEMPLATE_SIGNATURE_NULL)
                .build()) {
            final var connection = ddl.setSchemaAndGetConnection();
            final var embeddedConnection = connection.unwrap(EmbeddedRelationalConnection.class);
            embeddedConnection.setAutoCommit(false);
            embeddedConnection.createNewTransaction();
            final var schemaTemplate = embeddedConnection.getSchemaTemplate().unwrap(RecordLayerSchemaTemplate.class);
            embeddedConnection.rollback();
            embeddedConnection.setAutoCommit(true);

            final var sq = schemaTemplate.getStoredQueries().get("BY_SIG_NULL");
            Assertions.assertNotNull(sq);
            // param_b was declared exactly-NULL → type code NULL; param_a keeps its primitive type.
            Assertions.assertEquals(Map.of("param_a", "LONG", "param_b", "NULL"), sq.getParameters());
            Assertions.assertEquals("SELECT id FROM f1(?param_b)", sq.getQuery());
        }
    }

    @Test
    void storedQueriesUsageWithNullSignature() throws Exception {
        final String dbUri = "/TEST/SQ_SIGNATURE_NULL_USAGE";
        try (var ddl = Ddl.builder()
                .database(URI.create(dbUri))
                .relationalExtension(relationalExtension)
                .schemaTemplate(SCHEMA_TEMPLATE_SIGNATURE_NULL)
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

            // pre-warmed: 1 null-specialized plan (param_b IS NULL).
            Assertions.assertEquals(1, eventCounterCount(RelationalMetric.RelationalCount.PLAN_CACHE_TERTIARY_MISS));
            Assertions.assertEquals(0, eventCounterCount(RelationalMetric.RelationalCount.PLAN_CACHE_TERTIARY_HIT));
            Assertions.assertEquals(Long.valueOf(1), connectionUtils.getFromCatalog(c -> countCachedPlans(c, templateName)));

            // Runtime: re-declare the temp function binding ?param_a by name, run the SELECT binding ?param_b to NULL.
            connectionUtils.runAgainstConnection(dbUri, schemaName, c -> {
                c.setAutoCommit(false);
                try (var ps = c.prepareStatement(
                        "CREATE TEMPORARY FUNCTION f1(in p bigint) ON COMMIT DROP FUNCTION AS " +
                                "SELECT * FROM t1 WHERE (p IS NULL OR col1 = p) AND col2 = ?param_a")) {
                    ps.setLong("param_a", 2L);
                    ps.execute();
                }
                try (var ps = c.prepareStatement("SELECT id FROM f1(?param_b)")) {
                    ps.setNull("param_b", java.sql.Types.BIGINT);
                    try (RelationalResultSet rs = ps.executeQuery()) {
                        // f1(NULL): (NULL IS NULL) short-circuits the p-filter → col2 = 2 → row (2, 20, 2) → id = 2.
                        Assertions.assertTrue(rs.next());
                        Assertions.assertEquals(2, rs.getLong("ID"));
                        Assertions.assertFalse(rs.next());
                    }
                }
                c.rollback();
            });

            // SELECT hit the pre-warmed null-specialized plan: hit +1, miss unchanged, cache size unchanged.
            Assertions.assertEquals(1, eventCounterCount(RelationalMetric.RelationalCount.PLAN_CACHE_TERTIARY_HIT));
            Assertions.assertEquals(1, eventCounterCount(RelationalMetric.RelationalCount.PLAN_CACHE_TERTIARY_MISS));
            Assertions.assertEquals(Long.valueOf(1), connectionUtils.getFromCatalog(c -> countCachedPlans(c, templateName)));
        }
    }

    /**
     * Two stored queries with <em>identical</em> bodies (same SELECT, same declared function) whose signatures differ
     * only in {@code param_b}: typed BIGINT in one, exactly-NULL in the other. Both rewrite to the same canonical SQL
     * and temp function, so they warm into the <em>same</em> secondary cache key as two distinct tertiary entries —
     * distinguished only by the {@code param_b IS NOT NULL} vs {@code IS NULL} constraint. At runtime a value binding
     * selects the first, a {@code setNull} binding the second.
     */
    private static final String SCHEMA_TEMPLATE_SIGNATURE_BOTH =
            "CREATE TABLE t1(id bigint, col1 bigint, col2 bigint, PRIMARY KEY(id))" +
                    " CREATE INDEX i1 AS SELECT col1 FROM t1" +
                    " CREATE STORED QUERY sq_val(param_a bigint, param_b bigint)" +
                    "   DECLARE" +
                    "       FUNCTION f1(in p bigint) AS (SELECT * FROM t1 WHERE (p IS NULL OR col1 = p) AND col2 = param_a)" +
                    " AS SELECT id FROM f1(param_b)" +
                    " CREATE STORED QUERY sq_null(param_a bigint, param_b null)" +
                    "   DECLARE" +
                    "       FUNCTION f1(in p bigint) AS (SELECT * FROM t1 WHERE (p IS NULL OR col1 = p) AND col2 = param_a)" +
                    " AS SELECT id FROM f1(param_b)";

    @Test
    void storedQueriesUsageWithBothSignatureVariants() throws Exception {
        final String dbUri = "/TEST/SQ_SIGNATURE_BOTH_USAGE";
        try (var ddl = Ddl.builder()
                .database(URI.create(dbUri))
                .relationalExtension(relationalExtension)
                .schemaTemplate(SCHEMA_TEMPLATE_SIGNATURE_BOTH)
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

            // Both stored queries warm value-free. They share one secondary cache key (identical canonical SQL and
            // temp function) but hold two tertiary entries — param_b IS NOT NULL and param_b IS NULL.
            Assertions.assertEquals(2, eventCounterCount(RelationalMetric.RelationalCount.PLAN_CACHE_TERTIARY_MISS));
            Assertions.assertEquals(0, eventCounterCount(RelationalMetric.RelationalCount.PLAN_CACHE_TERTIARY_HIT));
            Assertions.assertEquals(Long.valueOf(2), connectionUtils.getFromCatalog(c -> countCachedPlans(c, templateName)));

            // Runtime, value case: bind param_b to a value → selects the IS NOT NULL plan.
            connectionUtils.runAgainstConnection(dbUri, schemaName, c -> {
                c.setAutoCommit(false);
                try (var ps = c.prepareStatement(
                        "CREATE TEMPORARY FUNCTION f1(in p bigint) ON COMMIT DROP FUNCTION AS " +
                                "SELECT * FROM t1 WHERE (p IS NULL OR col1 = p) AND col2 = ?param_a")) {
                    ps.setLong("param_a", 1L);
                    ps.execute();
                }
                try (var ps = c.prepareStatement("SELECT id FROM f1(?param_b)")) {
                    ps.setLong("param_b", 10L);
                    try (RelationalResultSet rs = ps.executeQuery()) {
                        // f1(10): col1 = 10 AND col2 = 1 → row (1, 10, 1) → id = 1.
                        Assertions.assertTrue(rs.next());
                        Assertions.assertEquals(1, rs.getLong("ID"));
                        Assertions.assertFalse(rs.next());
                    }
                }
                c.rollback();
            });

            // Value case hit one of the two warmed plans.
            Assertions.assertEquals(1, eventCounterCount(RelationalMetric.RelationalCount.PLAN_CACHE_TERTIARY_HIT));
            Assertions.assertEquals(2, eventCounterCount(RelationalMetric.RelationalCount.PLAN_CACHE_TERTIARY_MISS));
            Assertions.assertEquals(Long.valueOf(2), connectionUtils.getFromCatalog(c -> countCachedPlans(c, templateName)));

            // Runtime, null case: same SQL, but bind param_b to NULL → selects the IS NULL plan.
            connectionUtils.runAgainstConnection(dbUri, schemaName, c -> {
                c.setAutoCommit(false);
                try (var ps = c.prepareStatement(
                        "CREATE TEMPORARY FUNCTION f1(in p bigint) ON COMMIT DROP FUNCTION AS " +
                                "SELECT * FROM t1 WHERE (p IS NULL OR col1 = p) AND col2 = ?param_a")) {
                    ps.setLong("param_a", 2L);
                    ps.execute();
                }
                try (var ps = c.prepareStatement("SELECT id FROM f1(?param_b)")) {
                    ps.setNull("param_b", java.sql.Types.BIGINT);
                    try (RelationalResultSet rs = ps.executeQuery()) {
                        // f1(NULL): col2 = 2 → row (2, 20, 2) → id = 2.
                        Assertions.assertTrue(rs.next());
                        Assertions.assertEquals(2, rs.getLong("ID"));
                        Assertions.assertFalse(rs.next());
                    }
                }
                c.rollback();
            });

            // Null case hit the other warmed plan: two hits total, still two misses, still two cached plans.
            Assertions.assertEquals(2, eventCounterCount(RelationalMetric.RelationalCount.PLAN_CACHE_TERTIARY_HIT));
            Assertions.assertEquals(2, eventCounterCount(RelationalMetric.RelationalCount.PLAN_CACHE_TERTIARY_MISS));
            Assertions.assertEquals(Long.valueOf(2), connectionUtils.getFromCatalog(c -> countCachedPlans(c, templateName)));
        }
    }

}
