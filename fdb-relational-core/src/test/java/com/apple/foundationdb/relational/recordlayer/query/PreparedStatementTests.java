/*
 * PreparedStatementTests.java
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

package com.apple.foundationdb.relational.recordlayer.query;

import com.apple.foundationdb.relational.api.Continuation;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.recordlayer.EmbeddedRelationalExtension;
import com.apple.foundationdb.relational.recordlayer.LogAppenderRule;
import com.apple.foundationdb.relational.recordlayer.Utils;
import com.apple.foundationdb.relational.utils.Ddl;
import com.apple.foundationdb.relational.utils.ResultSetAssert;
import com.apple.foundationdb.relational.utils.RelationalAssertions;

import org.apache.logging.log4j.Level;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;

public class PreparedStatementTests {

    private static final String schemaTemplate =
            "CREATE TYPE AS STRUCT Location (address string, latitude string, longitude string)" +
                    " CREATE TYPE AS STRUCT \"ReviewerEndorsements\" (\"endorsementId\" bigint, \"endorsementText\" string)" +
                    " CREATE TYPE AS STRUCT RestaurantComplexReview (reviewer bigint, rating bigint, endorsements \"ReviewerEndorsements\" array)" +
                    " CREATE TYPE AS STRUCT RestaurantTag (tag string, weight bigint)" +
                    " CREATE TYPE AS STRUCT ReviewerStats (start_date bigint, school_name string, hometown string)" +
                    " CREATE TABLE RestaurantComplexRecord (rest_no bigint, name string, location Location, reviews RestaurantComplexReview ARRAY, tags RestaurantTag array, customer string array, encoded_bytes bytes, PRIMARY KEY(rest_no))" +
                    " CREATE TABLE RestaurantReviewer (id bigint, name string, email string, stats ReviewerStats, PRIMARY KEY(id))" +
                    " CREATE INDEX record_name_idx as select name from RestaurantComplexRecord" +
                    " CREATE INDEX reviewer_name_idx as select name from RestaurantReviewer" +
                    " CREATE INDEX mv1 AS SELECT R.rating from RestaurantComplexRecord AS Rec, (select rating from Rec.reviews) R" +
                    " CREATE INDEX mv2 AS SELECT endo.\"endorsementText\" FROM RestaurantComplexRecord rec, (SELECT X.\"endorsementText\" FROM rec.reviews rev, (SELECT \"endorsementText\" from rev.endorsements) X) endo";

    @RegisterExtension
    @Order(0)
    public final EmbeddedRelationalExtension relationalExtension = new EmbeddedRelationalExtension();

    @RegisterExtension
    @Order(1)
    public final LogAppenderRule logAppender = new LogAppenderRule("PreparedStatementsTestLogAppender", PlanGenerator.class, Level.INFO);

    public PreparedStatementTests() {
        Utils.enableCascadesDebugger();
    }

    @Test
    void failsToQueryWithoutASchema() throws Exception {
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (Connection conn = ddl.getConnection()) {
                conn.setSchema(null);

                try (PreparedStatement ps = conn.prepareStatement("select * from RestaurantComplexRecord")) {
                    RelationalAssertions.assertThrowsSqlException(ps::executeQuery)
                            .hasErrorCode(ErrorCode.UNDEFINED_SCHEMA);
                }
            }
        }
    }

    @Test
    void simpleSelect() throws Exception {
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                statement.execute("INSERT INTO RestaurantComplexRecord(rest_no) VALUES (10)");
            }
            try (var ps = ddl.getConnection().prepareStatement("SELECT * FROM RestaurantComplexRecord")) {
                try (final RelationalResultSet resultSet = ps.executeQuery()) {
                    ResultSetAssert.assertThat(resultSet).hasNextRow();
                }
            }
        }
    }

    @Test
    void basicParameterizedQuery() throws Exception {
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                statement.execute("INSERT INTO RestaurantComplexRecord(rest_no) VALUES (10)");
            }
            try (var ps = ddl.setSchemaAndGetConnection().prepareStatement("SELECT * FROM RestaurantComplexRecord WHERE REST_NO = ?")) {
                ps.setLong(1, 10);
                try (final RelationalResultSet resultSet = ps.executeQuery()) {
                    ResultSetAssert.assertThat(resultSet).hasNextRow();
                }
                ps.setLong(1, 0);
                try (final RelationalResultSet resultSet = ps.executeQuery()) {
                    ResultSetAssert.assertThat(resultSet).hasNoNextRow();
                }
            }
        }
    }

    @Test
    void parameterizedQueryMultipleParameters() throws Exception {
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                statement.execute("INSERT INTO RestaurantComplexRecord(rest_no, name) VALUES (10, 'testName')");
            }
            try (var ps = ddl.setSchemaAndGetConnection().prepareStatement("SELECT * FROM RestaurantComplexRecord WHERE REST_NO = ? AND NAME = ?")) {
                ps.setLong(1, 10);
                ps.setString(2, "testName");
                try (final RelationalResultSet resultSet = ps.executeQuery()) {
                    ResultSetAssert.assertThat(resultSet).hasNextRow();
                }
                ps.setLong(1, 10);
                ps.setString(2, "TEST");
                try (final RelationalResultSet resultSet = ps.executeQuery()) {
                    ResultSetAssert.assertThat(resultSet).hasNoNextRow();
                }
                ps.setLong(1, 0);
                ps.setString(2, "testName");
                try (final RelationalResultSet resultSet = ps.executeQuery()) {
                    ResultSetAssert.assertThat(resultSet).hasNoNextRow();
                }
            }
        }
    }

    @Test
    void parameterizedQueryNamedParameters() throws Exception {
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                statement.execute("INSERT INTO RestaurantComplexRecord(rest_no, name) VALUES (10, 'testName')");
            }
            try (var ps = ddl.setSchemaAndGetConnection().prepareStatement("SELECT * FROM RestaurantComplexRecord WHERE REST_NO = ?rest_no AND NAME = ?name")) {
                ps.setLong("rest_no", 10);
                ps.setString("name", "testName");
                try (final RelationalResultSet resultSet = ps.executeQuery()) {
                    ResultSetAssert.assertThat(resultSet).hasNextRow();
                }
                ps.setLong("rest_no", 10);
                ps.setString("name", "TEST");
                try (final RelationalResultSet resultSet = ps.executeQuery()) {
                    ResultSetAssert.assertThat(resultSet).hasNoNextRow();
                }
                ps.setLong("rest_no", 0);
                ps.setString("name", "testName");
                try (final RelationalResultSet resultSet = ps.executeQuery()) {
                    ResultSetAssert.assertThat(resultSet).hasNoNextRow();
                }
            }
        }
    }

    @Test
    void parameterizedQueryNamedAndUnnamedParameters() throws Exception {
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                statement.execute("INSERT INTO RestaurantComplexRecord(rest_no, name) VALUES (10, 'testName')");
            }
            try (var ps = ddl.setSchemaAndGetConnection().prepareStatement("SELECT * FROM RestaurantComplexRecord WHERE REST_NO = ? AND NAME = ?name")) {
                ps.setLong(1, 10);
                ps.setString("name", "testName");
                try (final RelationalResultSet resultSet = ps.executeQuery()) {
                    ResultSetAssert.assertThat(resultSet).hasNextRow();
                }
                ps.setLong(1, 10);
                ps.setString("name", "TEST");
                try (final RelationalResultSet resultSet = ps.executeQuery()) {
                    ResultSetAssert.assertThat(resultSet).hasNoNextRow();
                }
                ps.setLong(1, 0);
                ps.setString("name", "testName");
                try (final RelationalResultSet resultSet = ps.executeQuery()) {
                    ResultSetAssert.assertThat(resultSet).hasNoNextRow();
                }
            }
        }
    }

    @Test
    void parameterizedQueryMissingNamedParameters() throws Exception {
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                statement.execute("INSERT INTO RestaurantComplexRecord(rest_no) VALUES (10)");
            }
            try (var ps = ddl.setSchemaAndGetConnection().prepareStatement("SELECT * FROM RestaurantComplexRecord WHERE REST_NO = ?rest_no AND NAME = ?name")) {
                ps.setLong("rest_no", 10);
                RelationalAssertions.assertThrowsSqlException(ps::executeQuery)
                        .hasErrorCode(ErrorCode.UNDEFINED_PARAMETER);
            }
        }
    }

    @Test
    void executeWithoutNeededParameterShouldThrow() throws Exception {
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var ps = ddl.setSchemaAndGetConnection().prepareStatement("SELECT * FROM RestaurantComplexRecord WHERE REST_NO = ?")) {
                RelationalAssertions.assertThrowsSqlException(ps::executeQuery)
                        .hasErrorCode(ErrorCode.UNDEFINED_PARAMETER);
                ps.setLong(0, 10);
                RelationalAssertions.assertThrowsSqlException(ps::executeQuery)
                        .hasErrorCode(ErrorCode.UNDEFINED_PARAMETER);
                ps.setLong(2, 10);
                RelationalAssertions.assertThrowsSqlException(ps::executeQuery)
                        .hasErrorCode(ErrorCode.UNDEFINED_PARAMETER);
            }
        }
    }

    @Test
    void limit() throws Exception {
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                statement.execute("INSERT INTO RestaurantComplexRecord(rest_no) VALUES (10), (11), (12), (13), (14), (15)");
            }
            try (var ps = ddl.setSchemaAndGetConnection().prepareStatement("SELECT * FROM RestaurantComplexRecord LIMIT ?limit")) {
                ps.setInt("limit", 2);
                try (final RelationalResultSet resultSet = ps.executeQuery()) {
                    ResultSetAssert.assertThat(resultSet)
                            .hasNextRow().hasColumn("REST_NO", 10L)
                            .hasNextRow().hasColumn("REST_NO", 11L)
                            .hasNoNextRow();
                }
            }
            try (var ps = ddl.setSchemaAndGetConnection().prepareStatement("SELECT * FROM RestaurantComplexRecord LIMIT ?limit")) {
                ps.setInt("limit", 1);
                try (final RelationalResultSet resultSet = ps.executeQuery()) {
                    ResultSetAssert.assertThat(resultSet)
                            .hasNextRow().hasColumn("REST_NO", 10L)
                            .hasNoNextRow();
                }
            }
            try (var ps = ddl.setSchemaAndGetConnection().prepareStatement("SELECT * FROM RestaurantComplexRecord LIMIT ?")) {
                ps.setInt(1, 1);
                try (final RelationalResultSet resultSet = ps.executeQuery()) {
                    ResultSetAssert.assertThat(resultSet)
                            .hasNextRow().hasColumn("REST_NO", 10L)
                            .hasNoNextRow();
                }
            }
            try (var ps = ddl.setSchemaAndGetConnection().prepareStatement("SELECT * FROM RestaurantComplexRecord LIMIT ?")) {
                ps.setInt(1, 2);
                try (final RelationalResultSet resultSet = ps.executeQuery()) {
                    ResultSetAssert.assertThat(resultSet)
                            .hasNextRow().hasColumn("REST_NO", 10L)
                            .hasNextRow().hasColumn("REST_NO", 11L)
                            .hasNoNextRow();
                }
            }
        }
    }

    @Test
    void continuation() throws Exception {
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                statement.execute("INSERT INTO RestaurantComplexRecord(rest_no) VALUES (10), (11), (12), (13), (14)");
            }
            Continuation continuation;
            try (var ps = ddl.setSchemaAndGetConnection().prepareStatement("SELECT * FROM RestaurantComplexRecord LIMIT 2")) {
                try (final RelationalResultSet resultSet = ps.executeQuery()) {
                    ResultSetAssert.assertThat(resultSet)
                            .hasNextRow().hasColumn("REST_NO", 10L)
                            .hasNextRow().hasColumn("REST_NO", 11L)
                            .hasNoNextRow();
                    continuation = resultSet.getContinuation();
                }
            }
            try (var ps = ddl.setSchemaAndGetConnection().prepareStatement("SELECT * FROM RestaurantComplexRecord LIMIT 2 WITH CONTINUATION ?continuation")) {
                ps.setBytes("continuation", continuation.serialize());
                try (final RelationalResultSet resultSet = ps.executeQuery()) {
                    ResultSetAssert.assertThat(resultSet)
                            .hasNextRow().hasColumn("REST_NO", 12L)
                            .hasNextRow().hasColumn("REST_NO", 13L)
                            .hasNoNextRow();
                    continuation = resultSet.getContinuation();
                }
                ps.setBytes("continuation", continuation.serialize());
                try (final RelationalResultSet resultSet = ps.executeQuery()) {
                    ResultSetAssert.assertThat(resultSet)
                            .hasNextRow().hasColumn("REST_NO", 14L)
                            .hasNoNextRow();
                }
            }

            // Same but with logs
            try (var ps = ddl.setSchemaAndGetConnection().prepareStatement("SELECT * FROM RestaurantComplexRecord LIMIT 2 OPTIONS(LOG QUERY)")) {
                try (final RelationalResultSet resultSet = ps.executeQuery()) {
                    ResultSetAssert.assertThat(resultSet)
                            .hasNextRow().hasColumn("REST_NO", 10L)
                            .hasNextRow().hasColumn("REST_NO", 11L)
                            .hasNoNextRow();
                    continuation = resultSet.getContinuation();
                }
            }
            Assertions.assertThat(logAppender.getLastLogEntry()).contains("planCache=\"hit\"");
            try (var ps = ddl.setSchemaAndGetConnection().prepareStatement("SELECT * FROM RestaurantComplexRecord LIMIT 2 OPTIONS(LOG QUERY) WITH CONTINUATION ?continuation")) {
                ps.setBytes("continuation", continuation.serialize());
                try (final RelationalResultSet resultSet = ps.executeQuery()) {
                    ResultSetAssert.assertThat(resultSet)
                            .hasNextRow().hasColumn("REST_NO", 12L)
                            .hasNextRow().hasColumn("REST_NO", 13L)
                            .hasNoNextRow();
                    continuation = resultSet.getContinuation();
                }
                Assertions.assertThat(logAppender.getLastLogEntry()).contains("planCache=\"hit\"");
                ps.setBytes("continuation", continuation.serialize());
                try (final RelationalResultSet resultSet = ps.executeQuery()) {
                    ResultSetAssert.assertThat(resultSet)
                            .hasNextRow().hasColumn("REST_NO", 14L)
                            .hasNoNextRow();
                }
                Assertions.assertThat(logAppender.getLastLogEntry()).contains("planCache=\"hit\"");
            }
        }
    }

    @Test
    void prepareInList() throws Exception {
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                statement.execute("INSERT INTO RestaurantComplexRecord(rest_no) VALUES (10), (11), (12), (13), (14)");
            }

            try (var ps = ddl.setSchemaAndGetConnection().prepareStatement("SELECT * FROM RestaurantComplexRecord WHERE rest_no in (?, ?) or rest_no in (?, ?)")) {
                ps.setLong(1, 10);
                ps.setLong(2, 11);
                ps.setLong(3, 12);
                ps.setLong(4, 13);
                try (final RelationalResultSet resultSet = ps.executeQuery()) {
                    ResultSetAssert.assertThat(resultSet)
                            .hasNextRow().hasColumn("REST_NO", 10L)
                            .hasNextRow().hasColumn("REST_NO", 11L)
                            .hasNextRow().hasColumn("REST_NO", 12L)
                            .hasNextRow().hasColumn("REST_NO", 13L)
                            .hasNoNextRow();
                }
            }

            try (var ps = ddl.setSchemaAndGetConnection().prepareStatement("SELECT * FROM RestaurantComplexRecord WHERE rest_no in ?")) {
                ps.setArray(1, ddl.getConnection().createArrayOf("BIGINT", new Object[]{10, 11}));
                try (final RelationalResultSet resultSet = ps.executeQuery()) {
                    ResultSetAssert.assertThat(resultSet)
                            .hasNextRow().hasColumn("REST_NO", 10L)
                            .hasNextRow().hasColumn("REST_NO", 11L)
                            .hasNoNextRow();
                }
            }

            try (var ps = ddl.setSchemaAndGetConnection().prepareStatement("SELECT * FROM RestaurantComplexRecord WHERE rest_no in ? or rest_no in ?param OPTIONS(LOG QUERY)")) {
                ps.setArray(1, ddl.getConnection().createArrayOf("BIGINT", new Object[]{10L, 11L}));
                ps.setArray("param", ddl.getConnection().createArrayOf("BIGINT", new Object[]{12L, 13L}));
                try (final RelationalResultSet resultSet = ps.executeQuery()) {
                    ResultSetAssert.assertThat(resultSet)
                            .hasNextRow().hasColumn("REST_NO", 10L)
                            .hasNextRow().hasColumn("REST_NO", 11L)
                            .hasNextRow().hasColumn("REST_NO", 12L)
                            .hasNextRow().hasColumn("REST_NO", 13L)
                            .hasNoNextRow();
                }
            }
            Assertions.assertThat(logAppender.getLastLogEntry()).contains("planCache=\"miss\"");

            // Run a second time with different parameters
            try (var ps = ddl.setSchemaAndGetConnection().prepareStatement("SELECT * FROM RestaurantComplexRecord WHERE rest_no in ? or rest_no in ?param OPTIONS(LOG QUERY)")) {
                ps.setArray(1, ddl.getConnection().createArrayOf("BIGINT", new Object[]{10L, 100L}));
                ps.setArray("param", ddl.getConnection().createArrayOf("BIGINT", new Object[]{12L, 130L}));
                try (final RelationalResultSet resultSet = ps.executeQuery()) {
                    ResultSetAssert.assertThat(resultSet)
                            .hasNextRow().hasColumn("REST_NO", 10L)
                            .hasNextRow().hasColumn("REST_NO", 12L)
                            .hasNoNextRow();
                }
            }
            Assertions.assertThat(logAppender.getLastLogEntry()).contains("planCache=\"hit\"");
        }
    }

    @Test
    void prepareInListWrongTypeInArray() throws Exception {
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var ps = ddl.setSchemaAndGetConnection().prepareStatement("SELECT * FROM RestaurantComplexRecord WHERE rest_no in ?")) {
                ps.setArray(1, ddl.getConnection().createArrayOf("BIGINT", new Object[]{10L, "FOO"}));
                RelationalAssertions.assertThrowsSqlException(ps::executeQuery)
                        .hasErrorCode(ErrorCode.CANNOT_CONVERT_TYPE);
            }
            try (var ps = ddl.setSchemaAndGetConnection().prepareStatement("SELECT * FROM RestaurantComplexRecord WHERE rest_no in ?")) {
                ps.setArray(1, ddl.getConnection().createArrayOf("BIGINT", new Object[]{"FOO", "BAR"}));
                RelationalAssertions.assertThrowsSqlException(ps::executeQuery)
                        .hasErrorCode(ErrorCode.CANNOT_CONVERT_TYPE);
            }
        }
    }

    @Test
    void prepareInListWrongTypeShouldThrow() throws Exception {
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                statement.execute("INSERT INTO RestaurantComplexRecord(rest_no) VALUES (10), (11), (12), (13), (14)");
            }
            try (var ps = ddl.setSchemaAndGetConnection().prepareStatement("SELECT * FROM RestaurantComplexRecord WHERE rest_no in ?")) {
                ps.setLong(1, 42);
                RelationalAssertions.assertThrowsSqlException(ps::executeQuery)
                        .hasErrorCode(ErrorCode.CANNOT_CONVERT_TYPE);
            }
        }
    }

    @Test
    void prepareEmptyInList() throws Exception {
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                statement.execute("INSERT INTO RestaurantComplexRecord(rest_no) VALUES (10), (11), (12), (13), (14)");
            }
            try (var ps = ddl.setSchemaAndGetConnection().prepareStatement("SELECT * FROM RestaurantComplexRecord WHERE rest_no in ?")) {
                final var arr = ddl.getConnection().createArrayOf("NULL", new Object[]{});
                ps.setArray(1, arr);
                try (final RelationalResultSet resultSet = ps.executeQuery()) {
                    ResultSetAssert.assertThat(resultSet)
                            .hasNoNextRow();
                }
            }
        }
    }

    @Test
    void withPlanCache() throws Exception {
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                statement.execute("INSERT INTO RestaurantComplexRecord(rest_no) VALUES (10), (11), (12), (13), (14)");
            }
            try (var ps = ddl.setSchemaAndGetConnection().prepareStatement("SELECT * FROM RestaurantComplexRecord WHERE rest_no > ?val OPTIONS(LOG QUERY)")) {
                ps.setLong("val", 12);
                try (final RelationalResultSet resultSet = ps.executeQuery()) {
                    ResultSetAssert.assertThat(resultSet)
                            .hasNextRow().hasColumn("REST_NO", 13L)
                            .hasNextRow().hasColumn("REST_NO", 14L)
                            .hasNoNextRow();
                }
            }
            Assertions.assertThat(logAppender.getLastLogEntry()).contains("planCache=\"miss\"");
            try (var ps = ddl.setSchemaAndGetConnection().prepareStatement("SELECT * FROM RestaurantComplexRecord WHERE rest_no > ?val OPTIONS(LOG QUERY)")) {
                ps.setLong("val", 12);
                try (final RelationalResultSet resultSet = ps.executeQuery()) {
                    ResultSetAssert.assertThat(resultSet)
                            .hasNextRow().hasColumn("REST_NO", 13L)
                            .hasNextRow().hasColumn("REST_NO", 14L)
                            .hasNoNextRow();
                }
            }
            Assertions.assertThat(logAppender.getLastLogEntry()).contains("planCache=\"hit\"");
        }
    }

    @Disabled
    @Test
    // TODO (Prepared Statement does not cast fields if set with the wrong types)
    void setWrongTypeForQuestionMarkParameter() throws Exception {
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                statement.execute("INSERT INTO RestaurantComplexRecord(rest_no) VALUES (10)");
            }
            try (var ps = ddl.setSchemaAndGetConnection().prepareStatement("SELECT * FROM RestaurantComplexRecord WHERE REST_NO = ?")) {
                ps.setInt(1, 10);
                try (final RelationalResultSet resultSet = ps.executeQuery()) {
                    ResultSetAssert.assertThat(resultSet).hasNextRow();
                }
                ps.setString(1, "10");
                try (final RelationalResultSet resultSet = ps.executeQuery()) {
                    ResultSetAssert.assertThat(resultSet).hasNextRow();
                }
                ps.setDouble(1, 10.0);
                try (final RelationalResultSet resultSet = ps.executeQuery()) {
                    ResultSetAssert.assertThat(resultSet).hasNextRow();
                }
                ps.setFloat(1, 10);
                try (final RelationalResultSet resultSet = ps.executeQuery()) {
                    ResultSetAssert.assertThat(resultSet).hasNextRow();
                }
                ps.setBytes(1, "10".getBytes(StandardCharsets.UTF_8));
                try (final RelationalResultSet resultSet = ps.executeQuery()) {
                    ResultSetAssert.assertThat(resultSet).hasNextRow();
                }
                ps.setBoolean(1, true);
                try (final RelationalResultSet resultSet = ps.executeQuery()) {
                    ResultSetAssert.assertThat(resultSet).hasNextRow();
                }
            }
        }
    }

    @Disabled
    @Test
    // TODO (Prepared Statement does not cast fields if set with the wrong types)
    void setWrongTypeForQuestionMarkNamedParameter() throws Exception {
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                statement.execute("INSERT INTO RestaurantComplexRecord(rest_no) VALUES (10)");
            }
            try (var ps = ddl.setSchemaAndGetConnection().prepareStatement("SELECT * FROM RestaurantComplexRecord WHERE REST_NO = ?rest_no")) {
                ps.setInt("rest_no", 10);
                try (final RelationalResultSet resultSet = ps.executeQuery()) {
                    ResultSetAssert.assertThat(resultSet).hasNextRow();
                }
                ps.setString("rest_no", "10");
                try (final RelationalResultSet resultSet = ps.executeQuery()) {
                    ResultSetAssert.assertThat(resultSet).hasNextRow();
                }
                ps.setDouble("rest_no", 10.0);
                try (final RelationalResultSet resultSet = ps.executeQuery()) {
                    ResultSetAssert.assertThat(resultSet).hasNextRow();
                }
                ps.setFloat("rest_no", 10);
                try (final RelationalResultSet resultSet = ps.executeQuery()) {
                    ResultSetAssert.assertThat(resultSet).hasNextRow();
                }
                ps.setBytes("rest_no", "10".getBytes(StandardCharsets.UTF_8));
                try (final RelationalResultSet resultSet = ps.executeQuery()) {
                    ResultSetAssert.assertThat(resultSet).hasNextRow();
                }
                ps.setBoolean("rest_no", true);
                try (final RelationalResultSet resultSet = ps.executeQuery()) {
                    ResultSetAssert.assertThat(resultSet).hasNextRow();
                }
            }
        }
    }
}
