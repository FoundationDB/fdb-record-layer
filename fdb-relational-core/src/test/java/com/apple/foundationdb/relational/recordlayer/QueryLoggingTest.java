/*
 * QueryLoggingTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2025 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.recordlayer;

import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.relational.api.Continuation;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.RelationalConnection;
import com.apple.foundationdb.relational.api.RelationalDriver;
import com.apple.foundationdb.relational.api.RelationalPreparedStatement;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.recordlayer.query.AstNormalizer;
import com.apple.foundationdb.relational.recordlayer.query.PlanContext;
import com.apple.foundationdb.relational.recordlayer.query.PlanGenerator;
import com.apple.foundationdb.relational.utils.SimpleDatabaseRule;
import com.apple.foundationdb.relational.utils.TestSchemas;
import org.apache.logging.log4j.Level;
import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

/**
 * Testing basic query logging: plan, time, cache hits, etc.
 */
public class QueryLoggingTest {
    @RegisterExtension
    @Order(0)
    public final EmbeddedRelationalExtension relationalExtension = new EmbeddedRelationalExtension();

    @RegisterExtension
    @Order(1)
    public final SimpleDatabaseRule database = new SimpleDatabaseRule(QueryLoggingTest.class, TestSchemas.restaurantWithCoveringIndex());

    @RegisterExtension
    @Order(2)
    public final RelationalConnectionRule connection = new RelationalConnectionRule(database::getConnectionUri)
            .withOptions(Options.NONE)
            .withSchema("TEST_SCHEMA");

    @RegisterExtension
    @Order(3)
    public final RelationalStatementRule statement = new RelationalStatementRule(connection);

    @RegisterExtension
    @Order(4)
    public final LogAppenderRule logAppender = new LogAppenderRule("QueryLoggingTestLogAppender", PlanGenerator.class, Level.INFO);

    @Test
    void testLogCache() throws Exception {
        try (final RelationalResultSet resultSet = statement.executeQuery("SELECT * FROM RESTAURANT OPTIONS(LOG QUERY)")) {
            resultSet.next();
        }
        Assertions.assertThat(logAppender.getLastLogEventMessage()).contains("planCache=\"miss\"");

        try (final RelationalResultSet resultSet = statement.executeQuery("SELECT * FROM RESTAURANT OPTIONS(LOG QUERY)")) {
            resultSet.next();
        }
        Assertions.assertThat(logAppender.getLastLogEventMessage()).contains("planCache=\"hit\"");

        try (final RelationalResultSet resultSet = statement.executeQuery("SELECT * FROM RESTAURANT OPTIONS(LOG QUERY, NOCACHE)")) {
            resultSet.next();
        }
        Assertions.assertThat(logAppender.getLastLogEventMessage()).contains("planCache=\"skip\"");
    }

    @Test
    void testLogPlan() throws Exception {
        try (final RelationalResultSet resultSet = statement.executeQuery("SELECT name FROM RESTAURANT OPTIONS(LOG QUERY)")) {
            resultSet.next();
        }
        Assertions.assertThat(logAppender.getLastLogEventMessage()).contains(List.of("plan=", "NAME", "COVERING"));
    }

    @Test
    void testLogQuery() throws Exception {
        try (final RelationalResultSet resultSet = statement.executeQuery("SELECT * FROM Restaurant OPTIONS(LOG QUERY)")) {
            resultSet.next();
        }
        Assertions.assertThat(logAppender.getLastLogEventMessage()).contains("query=\"SELECT * FROM 'RESTAURANT'\"");
    }

    @Test
    void testLogTimes() throws Exception {
        try (final RelationalResultSet resultSet = statement.executeQuery("SELECT * FROM RESTAURANT OPTIONS(LOG QUERY)")) {
            resultSet.next();
        }
        Assertions.assertThat(logAppender.getLastLogEventMessage()).contains(List.of("normalizeQueryTime", "generatePhysicalPlanTime", "totalPlanTime"));

        try (final RelationalResultSet resultSet = statement.executeQuery("SELECT * FROM RESTAURANT OPTIONS(LOG QUERY)")) {
            resultSet.next();
        }
        // We don't get a `generatePhysicalPlanTime` because we hit the cache
        Assertions.assertThat(logAppender.getLastLogEventMessage()).contains(List.of("normalizeQueryTime", "totalPlanTime"));
    }

    @Test
    void testNoLogIfMissing() throws Exception {
        try (final RelationalResultSet resultSet = statement.executeQuery("SELECT * FROM RESTAURANT")) {
            resultSet.next();
        }
        Assertions.assertThat(logAppender.getLogEvents()).isEmpty();
    }

    @Test
    void testRelationalConnectionOptionPreparedStatement() throws Exception {
        final var driver = (RelationalDriver) DriverManager.getDriver(database.getConnectionUri().toString());
        try (RelationalConnection conn = driver.connect(database.getConnectionUri(), Options.builder().withOption(Options.Name.LOG_QUERY, true).build())) {
            conn.setSchema(database.getSchemaName());
            try (PreparedStatement ps = conn.prepareStatement("SELECT name from restaurant where rest_no = ?")) {
                ps.setLong(1, 0);
                try (ResultSet rs = ps.executeQuery()) {
                    rs.next();
                }
                Assertions.assertThat(logAppender.getLastLogEventMessage()).contains("query=\"SELECT 'NAME' from 'RESTAURANT' where 'REST_NO' = ?\"");
            }
            try (Statement ps = conn.createStatement()) {
                try (ResultSet rs = ps.executeQuery("SELECT name from restaurant")) {
                    rs.next();
                }
                Assertions.assertThat(logAppender.getLastLogEventMessage()).contains("query=\"SELECT 'NAME' from 'RESTAURANT'\"");
            }
        }
    }

    @Test
    void testRelationalConnectionOptionExplicitlyDisabled() throws Exception {
        final var driver = (RelationalDriver) DriverManager.getDriver(database.getConnectionUri().toString());
        try (RelationalConnection conn = driver.connect(database.getConnectionUri(), Options.builder().withOption(Options.Name.LOG_QUERY, false).build())) {
            conn.setSchema(database.getSchemaName());
            try (PreparedStatement ps = conn.prepareStatement("SELECT name from restaurant where rest_no = ?")) {
                ps.setLong(1, 0);
                try (ResultSet rs = ps.executeQuery()) {
                    rs.next();
                }
                Assertions.assertThat(logAppender.getLogEvents()).isEmpty();
            }
            try (Statement stmt = conn.createStatement()) {
                try (ResultSet rs = stmt.executeQuery("select name from restaurant")) {
                    rs.next();
                }
                Assertions.assertThat(logAppender.getLogEvents()).isEmpty();
            }
        }
    }

    @Test
    void testRelationalConnectionSetLogOnThenOff() throws Exception {
        final var driver = (RelationalDriver) DriverManager.getDriver(database.getConnectionUri().toString());
        try (RelationalConnection conn = driver.connect(database.getConnectionUri(), Options.NONE)) {
            conn.setSchema(database.getSchemaName());
            try (Statement stmt = conn.createStatement()) {
                try (ResultSet rs = stmt.executeQuery("select name from restaurant")) {
                    rs.next();
                }
                Assertions.assertThat(logAppender.getLogEvents()).isEmpty();
            }
            // set logging to true
            conn.setOption(Options.Name.LOG_QUERY, true);
            try (PreparedStatement ps = conn.prepareStatement("SELECT name from restaurant where rest_no = ?")) {
                ps.setLong(1, 0);
                try (ResultSet rs = ps.executeQuery()) {
                    rs.next();
                }
                Assertions.assertThat(logAppender.getLastLogEventMessage()).contains("query=\"SELECT 'NAME' from 'RESTAURANT' where 'REST_NO' = ?\"");
                Assertions.assertThat(logAppender.getLogEvents()).hasSize(1);
            }
            logAppender.getLogEvents().clear();
            // ... and then explicitly to false
            conn.setOption(Options.Name.LOG_QUERY, false);
            try (Statement stmt = conn.createStatement()) {
                try (ResultSet rs = stmt.executeQuery("select name from restaurant")) {
                    rs.next();
                }
                Assertions.assertThat(logAppender.getLogEvents()).isEmpty();
            }
        }
    }

    @Test
    void testRelationalConnectionSetLogIsOverriddenByQueryOption() throws Exception {
        final var driver = (RelationalDriver) DriverManager.getDriver(database.getConnectionUri().toString());
        try (RelationalConnection conn = driver.connect(database.getConnectionUri(), Options.NONE)) {
            conn.setSchema(database.getSchemaName());
            conn.setOption(Options.Name.LOG_QUERY, false);
            try (PreparedStatement ps = conn.prepareStatement("SELECT name from restaurant where rest_no = ? OPTIONS(LOG QUERY)")) {
                ps.setLong(1, 0);
                try (ResultSet rs = ps.executeQuery()) {
                    rs.next();
                }
                Assertions.assertThat(logAppender.getLastLogEventMessage()).contains("query=\"SELECT 'NAME' from 'RESTAURANT' where 'REST_NO' = ?\"");
                Assertions.assertThat(logAppender.getLogEvents()).hasSize(1);
            }
        }
    }

    @Test
    void testRelationalConnectionSetLogIsOverriddenByExecuteContinuationQueryOption() throws Exception {
        insertRows();
        final var driver = (RelationalDriver) DriverManager.getDriver(database.getConnectionUri().toString());
        try (RelationalConnection conn = driver.connect(database.getConnectionUri(), Options.NONE)) {
            Continuation continuation;
            conn.setSchema(database.getSchemaName());
            try (RelationalPreparedStatement ps = conn.prepareStatement("SELECT name from restaurant")) {
                ps.setMaxRows(1);
                try (RelationalResultSet rs = ps.executeQuery()) {
                    rs.next();
                    continuation = rs.getContinuation();
                }
                Assertions.assertThat(logAppender.getLogEvents()).isEmpty();
            }
            conn.setOption(Options.Name.LOG_QUERY, false);
            try (RelationalPreparedStatement ps = conn.prepareStatement("EXECUTE CONTINUATION ?continuation OPTIONS(LOG QUERY)")) {
                ps.setBytes("continuation", continuation.serialize());
                try (RelationalResultSet rs = ps.executeQuery()) {
                    rs.next();
                }
                Assertions.assertThat(logAppender.getLogEvents()).isNotEmpty();
                Assertions.assertThat(logAppender.getLastLogEventMessage()).contains("plan=\"COVERING(RECORD_TYPE_COVERING <,> -> [NAME: VALUE:[0], REST_NO: KEY:[0]]) | MAP (_.NAME AS NAME)\"");
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testRelationalConnectionSetLogWithExecuteContinuation(boolean setLogging) throws Exception {
        insertRows();
        final var driver = (RelationalDriver) DriverManager.getDriver(database.getConnectionUri().toString());
        try (RelationalConnection conn = driver.connect(database.getConnectionUri(), Options.NONE)) {
            Continuation continuation;
            conn.setSchema(database.getSchemaName());
            try (RelationalPreparedStatement ps = conn.prepareStatement("SELECT name from restaurant")) {
                ps.setMaxRows(1);
                try (RelationalResultSet rs = ps.executeQuery()) {
                    rs.next();
                    continuation = rs.getContinuation();
                }
                Assertions.assertThat(logAppender.getLogEvents()).isEmpty();
            }
            conn.setOption(Options.Name.LOG_QUERY, setLogging);
            try (RelationalPreparedStatement ps = conn.prepareStatement("EXECUTE CONTINUATION ?continuation")) {
                ps.setBytes("continuation", continuation.serialize());
                try (RelationalResultSet rs = ps.executeQuery()) {
                    rs.next();
                }
                if (!setLogging) {
                    Assertions.assertThat(logAppender.getLogEvents()).isEmpty();
                } else {
                    Assertions.assertThat(logAppender.getLogEvents()).isNotEmpty();
                    Assertions.assertThat(logAppender.getLastLogEventMessage()).contains("plan=\"COVERING(RECORD_TYPE_COVERING <,> -> [NAME: VALUE:[0], REST_NO: KEY:[0]]) | MAP (_.NAME AS NAME)\"");
                }
            }
        }
    }

    @Test
    void testLogQueryBecauseLoggerIsSetToDebug() throws Exception {
        try (LogAppenderRule debugRule = LogAppenderRule.of("DebugLogAppender", PlanGenerator.class, Level.DEBUG)) {
            try (final RelationalResultSet resultSet = statement.executeQuery("SELECT * FROM RESTAURANT")) {
                resultSet.next();
            }
            Assertions.assertThat(debugRule.getLastLogEventMessage()).contains("query=\"SELECT * FROM 'RESTAURANT'\"");
        }
    }

    @Test
    void testLogQueryBecauseQueryIsSlow() throws Exception {
        try (final RelationalResultSet resultSet = statement.executeQuery("SELECT * FROM RESTAURANT")) {
            resultSet.next();
        }
        Assertions.assertThat(logAppender.getLogEvents()).isEmpty();
        final var driver = (RelationalDriver) DriverManager.getDriver(database.getConnectionUri().toString());
        try (RelationalConnection conn = driver.connect(database.getConnectionUri(), Options.builder().withOption(Options.Name.LOG_SLOW_QUERY_THRESHOLD_MICROS, 1L).build())) {
            conn.setSchema(database.getSchemaName());
            try (PreparedStatement ps = conn.prepareStatement("SELECT NAME FROM RESTAURANT")) {
                try (ResultSet rs = ps.executeQuery()) {
                    rs.next();
                }
                Assertions.assertThat(logAppender.getLastLogEventMessage()).contains("SELECT 'NAME' FROM 'RESTAURANT'");
            }
        }
    }

    @Test
    void testLogQueryBecauseQueryIsSlowRemovesLiterals() throws Exception {
        try (final RelationalResultSet resultSet = statement.executeQuery("SELECT * FROM RESTAURANT WHERE \"NAME\" = 'restaurant 1'")) {
            resultSet.next();
        }
        Assertions.assertThat(logAppender.getLogEvents()).isEmpty();
        final var driver = (RelationalDriver) DriverManager.getDriver(database.getConnectionUri().toString());
        try (RelationalConnection conn = driver.connect(database.getConnectionUri(), Options.builder().withOption(Options.Name.LOG_SLOW_QUERY_THRESHOLD_MICROS, 1L).build())) {
            conn.setSchema(database.getSchemaName());
            try (PreparedStatement ps = conn.prepareStatement("SELECT * FROM RESTAURANT WHERE \"NAME\" = 'restaurant 1'")) {
                try (ResultSet rs = ps.executeQuery()) {
                    rs.next();
                }
                Assertions.assertThat(logAppender.getLastLogEventMessage()).contains("SELECT * FROM 'RESTAURANT' WHERE 'NAME' = ?");
                Assertions.assertThat(logAppender.getLastLogEventMessage()).doesNotContain("restaurant 1");
            }
        }
    }

    @Test
    void testLogPlanHash() throws Exception {
        final var query1 = "SELECT * FROM RESTAURANT where rest_no = 34 OPTIONS (LOG QUERY)";
        final var conn = (EmbeddedRelationalConnection) connection.connection;
        int queryHash = 0;
        conn.setAutoCommit(false);
        conn.createNewTransaction();
        try (var schema = conn.getRecordLayerDatabase().loadSchema(conn.getSchema())) {
            final var store = schema.loadStore().unwrap(FDBRecordStoreBase.class);
            final var planContext = PlanContext.Builder.create()
                    .fromRecordStore(store, conn.getOptions())
                    .fromDatabase(conn.getRecordLayerDatabase())
                    .withMetricsCollector(conn.getMetricCollector())
                    .withSchemaTemplate(conn.getSchemaTemplate())
                    .build();
            queryHash = AstNormalizer.normalizeQuery(planContext, query1, false, PlanHashable.PlanHashMode.VC0).getQueryCacheKey().getHash();
        }
        conn.commit();
        try (final RelationalResultSet resultSet = statement.executeQuery("SELECT * FROM RESTAURANT where rest_no = 0 OPTIONS (LOG QUERY)")) {
            resultSet.next();
        }
        Assertions.assertThat(logAppender.getLastLogEventMessage()).contains("queryHash=\"" + queryHash + "\"");
        Assertions.assertThat(logAppender.getLastLogEventMessage()).contains("planHash=\"-1179946538\"");
        try (final RelationalResultSet resultSet = statement.executeQuery("SELECT * FROM RESTAURANT where rest_no = 34 OPTIONS (LOG QUERY)")) {
            resultSet.next();
        }
        Assertions.assertThat(logAppender.getLastLogEventMessage()).contains("queryHash=\"" + queryHash + "\"");
        Assertions.assertThat(logAppender.getLastLogEventMessage()).contains("planHash=\"-1179946538\"");
    }

    @Test
    void testLogException() {
        Assert.assertThrows(Exception.class, () -> statement.executeQuery("SELECT * FROM REST where rest_no = 0 OPTIONS (LOG QUERY)"));
        final var thrown = logAppender.getLastLogEvent().getThrown();
        org.junit.jupiter.api.Assertions.assertNotNull(thrown);
        Assertions.assertThat(thrown).hasMessage("Unknown table REST")
                .hasNoCause();
    }

    @Test
    void testLogInsert() throws Exception {
        statement.executeUpdate("INSERT INTO RESTAURANT(REST_NO) VALUES (45) OPTIONS (LOG QUERY)");
        Assertions.assertThat(logAppender.getLastLogEventMessage()).contains("query=\"INSERT INTO 'RESTAURANT' ( 'REST_NO' ) VALUES ( ? )\"");
    }

    @Test
    void testLogUpdate() throws Exception {
        statement.executeUpdate("UPDATE RESTAURANT SET NAME = 'restau' WHERE REST_NO = 3 OPTIONS (LOG QUERY)");
        Assertions.assertThat(logAppender.getLastLogEventMessage()).contains("query=\"UPDATE 'RESTAURANT' SET 'NAME' = ? WHERE 'REST_NO' = ?\"");
    }

    @Test
    void testLogDelete() throws Exception {
        statement.executeUpdate("DELETE FROM RESTAURANT WHERE rest_no = 54 OPTIONS (LOG QUERY)");
        Assertions.assertThat(logAppender.getLastLogEventMessage()).contains("query=\"DELETE FROM 'RESTAURANT' WHERE 'REST_NO' = ?\"");
    }

    // insert to the table and closes the running statement.
    private void insertRows() throws SQLException {
        statement.executeUpdate("INSERT INTO RESTAURANT(REST_NO) VALUES (1), (2), (3)");
        statement.close();
    }
}
