/*
 * SchemaTemplateStoredQueriesTest.java
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

public class SchemaTemplateStoredQueriesTest {

    private static final String SCHEMA_TEMPLATE =
            "CREATE TABLE t1(id bigint, col1 bigint, col2 bigint, PRIMARY KEY(id))" +
                    " CREATE INDEX i1 AS SELECT col1 FROM t1" +
                    " CREATE QUERY by_col1 AS select * from t1 where col1 = 10" +
                    " CREATE QUERY by_id AS select * from t1 where id = 1";

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

    private void showCache(RelationalConnection connection) throws SQLException {
        final var embeddedConnection = connection.unwrap(EmbeddedRelationalConnection.class);
        final RelationalPlanCache cache = embeddedConnection.getRecordLayerDatabase().getPlanCache();
        if (cache == null) {
            System.out.println("[CACHE] no plan cache");
            return;
        }
        for (String key : cache.getStats().getAllKeys()) {
            System.out.println("[CACHE] template: " + key);
            for (QueryCacheKey secondaryKey : cache.getStats().getAllSecondaryKeys(key)) {
                System.out.println("[CACHE]   query: " + secondaryKey.getCanonicalQueryString()
                        + " (version=" + secondaryKey.getSchemaTemplateVersion() + ")");
                var tertiaryMappings = cache.getStats().getAllTertiaryMappings(key, secondaryKey);
                for (var entry : tertiaryMappings.entrySet()) {
                    System.out.println("[CACHE]     key: " + entry.getKey().toString());
                    System.out.println("[CACHE]         plan: " + entry.getValue().explain());
                }
            }
        }
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
            Assertions.assertEquals("select * from t1 where col1 = 10", storedQueries.get("BY_COL1"));
            Assertions.assertEquals("select * from t1 where id = 1", storedQueries.get("BY_ID"));
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
            final var freshDriver = relationalExtension.getDriver(
                    com.apple.foundationdb.record.provider.foundationdb.FormatVersion.getDefaultFormatVersion());

            // Connect via the fresh driver and verify the fresh engine's cache has the plans
            Assertions.assertEquals(Long.valueOf(2), new ConnectionUtils(freshDriver).getFromCatalog(
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
            final var freshDriver = relationalExtension.getDriver(
                    com.apple.foundationdb.record.provider.foundationdb.FormatVersion.getDefaultFormatVersion());
            final var freshUtils = new ConnectionUtils(freshDriver);

            // OfflineStoredQueriesProcessor ran during fresh-engine construction and
            // warmed both stored queries: 2 L3 cache misses.
            Assertions.assertEquals(2, eventCounterCount(RelationalMetric.RelationalCount.PLAN_CACHE_TERTIARY_MISS));
            Assertions.assertEquals(0, eventCounterCount(RelationalMetric.RelationalCount.PLAN_CACHE_TERTIARY_HIT));
            // Connect via the fresh driver and verify the fresh engine's cache has 2 plans
            Assertions.assertEquals(Long.valueOf(2), freshUtils.getFromCatalog(c -> countCachedPlans(c, templateName)));

            // select statement should hit the cache, no new entries
            freshUtils.runAgainstConnection(dbUri, schemaName, c -> {
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
            Assertions.assertEquals(Long.valueOf(2), freshUtils.getFromCatalog(c -> countCachedPlans(c, templateName)));

            // select statement should hit another cache, no new entries
            freshUtils.runAgainstConnection(dbUri, schemaName, c -> {
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
            Assertions.assertEquals(Long.valueOf(2), freshUtils.getFromCatalog(c -> countCachedPlans(c, templateName)));

            // non-stored query, new record in the cache
            freshUtils.runAgainstConnection(dbUri, schemaName, c -> {
                try (var stmt = c.createStatement(); RelationalResultSet rs = stmt.executeQuery("select * from t1 where col2 = 1")) {
                    Assertions.assertTrue(rs.next());
                    Assertions.assertEquals(1, rs.getLong("ID"));
                    Assertions.assertFalse(rs.next());
                }
            });
            // new (3) plan in the cache
            Assertions.assertEquals(Long.valueOf(3), freshUtils.getFromCatalog(c -> countCachedPlans(c, templateName)));
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
            final var freshDriver = relationalExtension.getDriver(
                    com.apple.foundationdb.record.provider.foundationdb.FormatVersion.getDefaultFormatVersion());
            final var freshUtils = new ConnectionUtils(freshDriver);

            // Connect via the fresh driver and verify the fresh engine's cache has the plans
            Assertions.assertEquals(Long.valueOf(2), freshUtils.getFromCatalog(c -> countCachedPlans(c, templateName)));

            // select with different literal than stored — canonical SQL matches, cache hit
            freshUtils.runAgainstConnection(dbUri, schemaName, c -> {
                try (var stmt = c.createStatement(); RelationalResultSet rs = stmt.executeQuery("select * from t1 where col1 = 20")) {
                    Assertions.assertTrue(rs.next());
                    Assertions.assertEquals(2, rs.getLong("ID"));
                    Assertions.assertFalse(rs.next());
                }
            });
            Assertions.assertEquals(Long.valueOf(2), freshUtils.getFromCatalog(c -> countCachedPlans(c, templateName)));

            // select with different literal than stored — canonical SQL matches, cache hit
            freshUtils.runAgainstConnection(dbUri, schemaName, c -> {
                try (var stmt = c.createStatement(); RelationalResultSet rs = stmt.executeQuery("select * from t1 where id = 2")) {
                    Assertions.assertTrue(rs.next());
                    Assertions.assertEquals(2, rs.getLong("ID"));
                    Assertions.assertFalse(rs.next());
                }
            });
            Assertions.assertEquals(Long.valueOf(2), freshUtils.getFromCatalog(c -> countCachedPlans(c, templateName)));
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
            final var freshDriver = relationalExtension.getDriver(
                    com.apple.foundationdb.record.provider.foundationdb.FormatVersion.getDefaultFormatVersion());
            final var freshUtils = new ConnectionUtils(freshDriver);

            // Connect via the fresh driver and verify the fresh engine's cache has the plans
            Assertions.assertEquals(Long.valueOf(2), freshUtils.getFromCatalog(c -> countCachedPlans(c, templateName)));

            // JDBC PreparedStatement on col1 with bound parameter — canonical SQL matches stored BY_COL1, cache hit
            freshUtils.runAgainstConnection(dbUri, schemaName, c -> {
                try (var ps = c.prepareStatement("select * from t1 where col1 = ?")) {
                    ps.setInt(1, 20);
                    try (RelationalResultSet rs = ps.executeQuery()) {
                        Assertions.assertTrue(rs.next());
                        Assertions.assertEquals(2, rs.getLong("ID"));
                        Assertions.assertFalse(rs.next());
                    }
                }
            });
            Assertions.assertEquals(Long.valueOf(2), freshUtils.getFromCatalog(c -> countCachedPlans(c, templateName)));

            // JDBC PreparedStatement on id with bound parameter — canonical SQL matches stored BY_ID, cache hit
            freshUtils.runAgainstConnection(dbUri, schemaName, c -> {
                try (var ps = c.prepareStatement("select * from t1 where id = ?")) {
                    ps.setInt(1, 2);
                    try (RelationalResultSet rs = ps.executeQuery()) {
                        Assertions.assertTrue(rs.next());
                        Assertions.assertEquals(2, rs.getLong("ID"));
                        Assertions.assertFalse(rs.next());
                    }
                }
            });
            Assertions.assertEquals(Long.valueOf(2), freshUtils.getFromCatalog(c -> countCachedPlans(c, templateName)));
        }
    }

    @Test
    void badStoredQuery() {
        final String badTemplate =
                "CREATE TABLE t1(id bigint, col1 bigint, col2 bigint, PRIMARY KEY(id))" +
                        " CREATE QUERY by_col1 AS select1 * from t1 where col1 = 10";
        RelationalAssertions.assertThrowsSqlException(() ->
                Ddl.builder()
                        .database(URI.create("/TEST/BADSTOREDQUERY_DB"))
                        .relationalExtension(relationalExtension)
                        .schemaTemplate(badTemplate)
                        .build())
                .hasErrorCode(ErrorCode.SYNTAX_ERROR);
    }

    @Test
    void storedQueryDdl() {
        final String badTemplate =
                "CREATE TABLE t1(id bigint, col1 bigint, col2 bigint, PRIMARY KEY(id))" +
                        " CREATE QUERY ddl_t AS CREATE TABLE t2(id bigint, col1 bigint, PRIMARY KEY(id))";
        RelationalAssertions.assertThrowsSqlException(() ->
                Ddl.builder()
                        .database(URI.create("/TEST/DDLSTOREDQUERY_DB"))
                        .relationalExtension(relationalExtension)
                        .schemaTemplate(badTemplate)
                        .build())
                .hasErrorCode(ErrorCode.SYNTAX_ERROR);
    }

    @Test
    void storedQueryBadColumn() throws Exception {
        final String template =
                "CREATE TABLE t1(id bigint, col1 bigint, col2 bigint, PRIMARY KEY(id))" +
                        " CREATE INDEX i1 AS SELECT col1 FROM t1" +
                        " CREATE QUERY by_col1 AS select * from t1 where col3 = 10" + // col3 does not exit
                        " CREATE QUERY by_id AS select * from t1 where id = 1";
        final String dbUri = "/TEST/STOREDQUERIES_DB5";
        try (var ddl = Ddl.builder()
                .database(URI.create(dbUri))
                .relationalExtension(relationalExtension)
                .schemaTemplate(template)
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
            final var freshDriver = relationalExtension.getDriver(
                    com.apple.foundationdb.record.provider.foundationdb.FormatVersion.getDefaultFormatVersion());
            final var freshUtils = new ConnectionUtils(freshDriver);

            // OfflineStoredQueriesProcessor ran during fresh-engine construction and
            // both stored queries attempted to generate plan
            Assertions.assertEquals(2, eventCounterCount(RelationalMetric.RelationalCount.PLAN_CACHE_TERTIARY_MISS));
            Assertions.assertEquals(0, eventCounterCount(RelationalMetric.RelationalCount.PLAN_CACHE_TERTIARY_HIT));

            // but only one query has valid column and was planned
            Assertions.assertEquals(Long.valueOf(1), freshUtils.getFromCatalog(c -> countCachedPlans(c, templateName)));
        }
    }
}
