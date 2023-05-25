/*
 * QueryLoggingTest.java
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

package com.apple.foundationdb.relational.recordlayer;

import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.Relational;
import com.apple.foundationdb.relational.api.RelationalConnection;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.recordlayer.query.PlanGenerator;
import com.apple.foundationdb.relational.utils.SimpleDatabaseRule;
import com.apple.foundationdb.relational.utils.TestSchemas;

import org.apache.logging.log4j.Level;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
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
    public final SimpleDatabaseRule database = new SimpleDatabaseRule(relationalExtension, QueryLoggingTest.class, TestSchemas.restaurantWithCoveringIndex());

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
        Assertions.assertThat(logAppender.getLastLogEntry()).contains("planCache=\"miss\"");

        try (final RelationalResultSet resultSet = statement.executeQuery("SELECT * FROM RESTAURANT OPTIONS(LOG QUERY)")) {
            resultSet.next();
        }
        Assertions.assertThat(logAppender.getLastLogEntry()).contains("planCache=\"hit\"");

        try (final RelationalResultSet resultSet = statement.executeQuery("SELECT * FROM RESTAURANT OPTIONS(LOG QUERY, NOCACHE)")) {
            resultSet.next();
        }
        Assertions.assertThat(logAppender.getLastLogEntry()).contains("planCache=\"skip\"");
    }

    @Test
    void testLogPlan() throws Exception {
        try (final RelationalResultSet resultSet = statement.executeQuery("SELECT name FROM RESTAURANT OPTIONS(LOG QUERY)")) {
            resultSet.next();
        }
        Assertions.assertThat(logAppender.getLastLogEntry()).contains(List.of("plan=", "NAME", "Index", "Covering"));
    }

    @Test
    void testLogQuery() throws Exception {
        try (final RelationalResultSet resultSet = statement.executeQuery("SELECT * FROM RESTAURANT OPTIONS(LOG QUERY)")) {
            resultSet.next();
        }
        Assertions.assertThat(logAppender.getLastLogEntry()).contains("query=\"SELECT * FROM RESTAURANT OPTIONS(LOG QUERY)\"");
    }

    @Test
    void testLogTimes() throws Exception {
        try (final RelationalResultSet resultSet = statement.executeQuery("SELECT * FROM RESTAURANT OPTIONS(LOG QUERY)")) {
            resultSet.next();
        }
        Assertions.assertThat(logAppender.getLastLogEntry()).contains(List.of("normalizeQueryTime", "generatePhysicalPlanTime", "totalPlanTime"));

        try (final RelationalResultSet resultSet = statement.executeQuery("SELECT * FROM RESTAURANT OPTIONS(LOG QUERY)")) {
            resultSet.next();
        }
        // We don't get a `generatePhysicalPlanTime` because we hit the cache
        Assertions.assertThat(logAppender.getLastLogEntry()).contains(List.of("normalizeQueryTime", "totalPlanTime"));
    }

    @Test
    void testNoLogIfMissing() throws Exception {
        try (final RelationalResultSet resultSet = statement.executeQuery("SELECT * FROM RESTAURANT")) {
            resultSet.next();
        }
        Assertions.assertThat(logAppender.getLogs()).isEmpty();
    }

    @Test
    void testRelationalConnectionOptionPreparedStatement() throws Exception {
        try (RelationalConnection conn = Relational.connect(database.getConnectionUri(), Options.builder().withOption(Options.Name.LOG_QUERY, true).build())) {
            conn.setSchema(database.getSchemaName());
            try (PreparedStatement ps = conn.prepareStatement("SELECT name from restaurant where rest_no = ?")) {
                ps.setLong(1, 0);
                try (ResultSet rs = ps.executeQuery()) {
                    rs.next();
                }
                Assertions.assertThat(logAppender.getLastLogEntry()).contains("query=\"SELECT name from restaurant where rest_no = ?\"");
            }
            try (Statement ps = conn.createStatement()) {
                try (ResultSet rs = ps.executeQuery("SELECT name from restaurant")) {
                    rs.next();
                }
                Assertions.assertThat(logAppender.getLastLogEntry()).contains("query=\"SELECT name from restaurant\"");
            }
        }
    }

    @Test
    void testRelationalConnectionOptionExplicitlyDisabled() throws Exception {
        try (RelationalConnection conn = Relational.connect(database.getConnectionUri(), Options.builder().withOption(Options.Name.LOG_QUERY, false).build())) {
            conn.setSchema(database.getSchemaName());
            try (PreparedStatement ps = conn.prepareStatement("SELECT name from restaurant where rest_no = ?")) {
                ps.setLong(1, 0);
                try (ResultSet rs = ps.executeQuery()) {
                    rs.next();
                }
                Assertions.assertThat(logAppender.getLogs()).isEmpty();
            }
            try (Statement stmt = conn.createStatement()) {
                try (ResultSet rs = stmt.executeQuery("select name from restaurant")) {
                    rs.next();
                }
                Assertions.assertThat(logAppender.getLogs()).isEmpty();
            }
        }
    }

    @Test
    void testLogQueryBecauseLoggerIsSetToDebug() throws Exception {
        try (LogAppenderRule debugRule = LogAppenderRule.of("DebugLogAppender", PlanGenerator.class, Level.DEBUG)) {
            try (final RelationalResultSet resultSet = statement.executeQuery("SELECT * FROM RESTAURANT")) {
                resultSet.next();
            }
            Assertions.assertThat(debugRule.getLastLogEntry()).contains("query=\"SELECT * FROM RESTAURANT\"");
        }
    }
}
