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

import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.relational.api.Continuation;
import com.apple.foundationdb.relational.api.FieldDescription;
import com.apple.foundationdb.relational.api.ImmutableRowStruct;
import com.apple.foundationdb.relational.api.RowArray;
import com.apple.foundationdb.relational.api.SqlTypeNamesSupport;
import com.apple.foundationdb.relational.api.SqlTypeSupport;
import com.apple.foundationdb.relational.api.RelationalArrayMetaData;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.RelationalStructMetaData;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.recordlayer.ArrayRow;
import com.apple.foundationdb.relational.recordlayer.EmbeddedRelationalExtension;
import com.apple.foundationdb.relational.recordlayer.LogAppenderRule;
import com.apple.foundationdb.relational.recordlayer.Utils;
import com.apple.foundationdb.relational.utils.Ddl;
import com.apple.foundationdb.relational.utils.ResultSetAssert;
import com.apple.foundationdb.relational.utils.RelationalAssertions;
import org.apache.logging.log4j.Level;
import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class PreparedStatementTests {

    private static final String schemaTemplate =
            "CREATE TYPE AS STRUCT LatLong (latitude double, longitude double)" +
                    "CREATE TYPE AS STRUCT Location (address string, pin bigint, coords LatLong)" +
                    " CREATE TYPE AS STRUCT \"ReviewerEndorsements\" (\"endorsementId\" bigint, \"endorsementText\" string)" +
                    " CREATE TYPE AS STRUCT RestaurantComplexReview (reviewer bigint, rating bigint, endorsements \"ReviewerEndorsements\" array)" +
                    " CREATE TYPE AS STRUCT RestaurantTag (tag string, weight bigint)" +
                    " CREATE TYPE AS STRUCT ReviewerStats (start_date bigint, school_name string, hometown string)" +
                    " CREATE TABLE RestaurantComplexRecord (rest_no bigint, name string, location Location, reviews RestaurantComplexReview ARRAY, tags RestaurantTag array, customer string array, encoded_bytes bytes, key bytes, PRIMARY KEY(rest_no))" +
                    " CREATE TABLE RestaurantReviewer (id bigint, name string, email string, stats ReviewerStats, secrets bytes array, PRIMARY KEY(id))" +
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
    void parameterizedQueryQuestionAndDollarParameter() throws Exception {
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                statement.execute("INSERT INTO RestaurantComplexRecord(rest_no, name) VALUES (10, 'testName')");
            }
            try (var ps = ddl.setSchemaAndGetConnection().prepareStatement("SELECT * FROM RestaurantComplexRecord WHERE REST_NO = ?rest_no AND NAME = $name")) {
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
    void longTypeLimitParameter() throws Exception {
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                statement.execute("INSERT INTO RestaurantComplexRecord(rest_no) VALUES (10), (11), (12), (13), (14), (15)");
            }
            // setLong
            try (var ps = ddl.setSchemaAndGetConnection().prepareStatement("SELECT * FROM RestaurantComplexRecord LIMIT ?limit")) {
                ps.setLong("limit", 2);
                try (final RelationalResultSet resultSet = ps.executeQuery()) {
                    ResultSetAssert.assertThat(resultSet)
                            .hasNextRow().hasColumn("REST_NO", 10L)
                            .hasNextRow().hasColumn("REST_NO", 11L)
                            .hasNoNextRow();
                }
            }
            // setLong > Integer.MAX_VALUE
            try (var ps = ddl.setSchemaAndGetConnection().prepareStatement("SELECT * FROM RestaurantComplexRecord LIMIT ?limit")) {
                ps.setLong("limit", 3147483647L);
                RelationalAssertions.assertThrowsSqlException(ps::executeQuery).hasErrorCode(ErrorCode.INVALID_ROW_COUNT_IN_LIMIT_CLAUSE);
            }
            // setLong = 0
            try (var ps = ddl.setSchemaAndGetConnection().prepareStatement("SELECT * FROM RestaurantComplexRecord LIMIT ?limit")) {
                ps.setLong("limit", 0);
                RelationalAssertions.assertThrowsSqlException(ps::executeQuery).hasErrorCode(ErrorCode.INVALID_ROW_COUNT_IN_LIMIT_CLAUSE);
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
            Assertions.assertThat(logAppender.getLastLogEventMessage()).contains("planCache=\"hit\"");
            try (var ps = ddl.setSchemaAndGetConnection().prepareStatement("SELECT * FROM RestaurantComplexRecord LIMIT 2 OPTIONS(LOG QUERY) WITH CONTINUATION ?continuation")) {
                ps.setBytes("continuation", continuation.serialize());
                try (final RelationalResultSet resultSet = ps.executeQuery()) {
                    ResultSetAssert.assertThat(resultSet)
                            .hasNextRow().hasColumn("REST_NO", 12L)
                            .hasNextRow().hasColumn("REST_NO", 13L)
                            .hasNoNextRow();
                    continuation = resultSet.getContinuation();
                }
                Assertions.assertThat(logAppender.getLastLogEventMessage()).contains("planCache=\"hit\"");
                ps.setBytes("continuation", continuation.serialize());
                try (final RelationalResultSet resultSet = ps.executeQuery()) {
                    ResultSetAssert.assertThat(resultSet)
                            .hasNextRow().hasColumn("REST_NO", 14L)
                            .hasNoNextRow();
                }
                Assertions.assertThat(logAppender.getLastLogEventMessage()).contains("planCache=\"hit\"");
            }
        }
    }

    @Test
    void setArrayTypeOfByte() throws Exception {
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                final var count = statement.executeUpdate("INSERT INTO RestaurantReviewer(id) VALUES (1)");
                Assertions.assertThat(count).isEqualTo(1);
            }
            // "new" should not be quoted. TODO ([Post] Fix identifiers case-sensitivity matching in plan generator)
            try (var ps = ddl.setSchemaAndGetConnection().prepareStatement("UPDATE RestaurantReviewer SET secrets = ?param WHERE id = 1 RETURNING \"new\".*")) {
                final var array = List.of(new byte[]{1, 2, 3, 4}, new byte[]{5, 6, 7, 8});
                final var arrayObject = ddl.getConnection().createArrayOf("BINARY", array.toArray());
                ps.setArray("param", arrayObject);
                try (final var resultSet = ps.executeQuery()) {
                    final var expected = new RowArray(array, RelationalArrayMetaData.ofPrimitive(
                            SqlTypeSupport.recordTypeToSqlType(Type.TypeCode.BYTES), DatabaseMetaData.columnNoNulls));
                    ResultSetAssert.assertThat(resultSet)
                            .hasNextRow().hasColumn("ID", 1L).hasColumn("SECRETS", expected)
                            .hasNoNextRow();
                }
            }
        }
    }

    @Test
    void setByteType() throws Exception {
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                final var count = statement.executeUpdate("INSERT INTO RestaurantComplexRecord(rest_no) VALUES (1)");
                Assertions.assertThat(count).isEqualTo(1);
            }
            // "new" should not be quoted. TODO ([Post] Fix identifiers case-sensitivity matching in plan generator)
            try (var ps = ddl.setSchemaAndGetConnection().prepareStatement("UPDATE RestaurantComplexRecord SET key = ?param WHERE rest_no = 1 RETURNING \"new\".*")) {
                ps.setBytes("param", new byte[]{1, 2, 3, 4});
                try (final var resultSet = ps.executeQuery()) {
                    ResultSetAssert.assertThat(resultSet)
                            .hasNextRow().hasColumn("REST_NO", 1L).hasColumn("key", new byte[]{1, 2, 3, 4})
                            .hasNoNextRow();
                }
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
                ps.setArray(1, ddl.getConnection().createArrayOf("BIGINT", new Object[]{10L, 11L}));
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
            Assertions.assertThat(logAppender.getLastLogEventMessage()).contains("planCache=\"miss\"");

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
            Assertions.assertThat(logAppender.getLastLogEventMessage()).contains("planCache=\"hit\"");
        }
    }

    @Test
    void prepareUpdateWithStruct() throws Exception {
        final var statsAttributes = new Object[]{3L, "c", "d"};
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                statement.execute("INSERT INTO RestaurantReviewer(id, stats) VALUES (1, (2, 'a', 'b')), (2, (3, 'b', 'c')), (3, (4, 'c', 'd')), (4, (5, 'd', 'e')), (5, (6, 'e', 'f'))");
            }
            // "new" should not be quoted. TODO ([Post] Fix identifiers case-sensitivity matching in plan generator)
            final var query = "UPDATE RestaurantReviewer SET stats = ?param WHERE id = 1 RETURNING \"new\".stats";
            try (var ps = ddl.setSchemaAndGetConnection().prepareStatement(query)) {
                ps.setObject("param", ddl.getConnection().createStruct("blah", statsAttributes));
                final var expectedStats = new ImmutableRowStruct(new ArrayRow(statsAttributes), new RelationalStructMetaData(
                        FieldDescription.primitive("START_DATE", SqlTypeNamesSupport.getSqlTypeCode("BIGINT"), DatabaseMetaData.columnNoNulls),
                        FieldDescription.primitive("SCHOOL_NAME", SqlTypeNamesSupport.getSqlTypeCode("STRING"), DatabaseMetaData.columnNoNulls),
                        FieldDescription.primitive("HOMETOWN", SqlTypeNamesSupport.getSqlTypeCode("STRING"), DatabaseMetaData.columnNoNulls)
                ));
                try (final RelationalResultSet resultSet = ps.executeQuery()) {
                    ResultSetAssert.assertThat(resultSet)
                            .hasNextRow().hasColumn("STATS", expectedStats)
                            .hasNoNextRow();
                }
            }
        }
    }

    static Stream<Object> prepareUpdateWithNestedStructMethodSource() {
        return Stream.of(
                Arguments.of(new Object[]{100.0, 200.0}, true),
                // All the ones below require casting the relevant struct
                Arguments.of(new Object[]{100, 200}, true),
                Arguments.of(new Object[]{100L, 200L}, true),
                Arguments.of(new Object[]{100.0f, 200.0f}, true),
                Arguments.of(new Object[]{"100", "200"}, false),
                Arguments.of(new Object[]{100.0, 200.0, 300.0}, false)
        );
    }

    @ParameterizedTest
    @MethodSource("prepareUpdateWithNestedStructMethodSource")
    void prepareUpdateWithNestedStruct(Object[] attributes, boolean succeed) throws Exception {
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                statement.execute("INSERT INTO RestaurantComplexRecord(rest_no, name) VALUES (1, 'mango & miso'), (2, 'basil & brawn'), (3, 'peach & pepper'), (4, 'smoky skillet'), (5, 'the tin pot')");
            }
            // "new" should not be quoted. TODO ([Post] Fix identifiers case-sensitivity matching in plan generator)
            final var query = "UPDATE RestaurantComplexRecord SET location = ?param WHERE rest_no = 1 RETURNING \"new\".location";
            try (var ps = ddl.setSchemaAndGetConnection().prepareStatement(query)) {
                final var latLong = ddl.getConnection().createStruct("LATLONG", attributes);
                final var location = ddl.getConnection().createStruct("LOCATION", new Object[]{"next door", 217, latLong});
                ps.setObject("param", location);
                if (succeed) {
                    final var expectedLatLong = new ImmutableRowStruct(new ArrayRow(100.00, 200.00), new RelationalStructMetaData(
                            FieldDescription.primitive("LATITUDE", SqlTypeNamesSupport.getSqlTypeCode("DOUBLE"), DatabaseMetaData.columnNoNulls),
                            FieldDescription.primitive("LONGITUDE", SqlTypeNamesSupport.getSqlTypeCode("DOUBLE"), DatabaseMetaData.columnNoNulls)
                    ));
                    final var expectedLocation = new ImmutableRowStruct(new ArrayRow("next door", 217, expectedLatLong), new RelationalStructMetaData(
                            FieldDescription.primitive("ADDRESS", SqlTypeNamesSupport.getSqlTypeCode("STRING"), DatabaseMetaData.columnNoNulls),
                            FieldDescription.primitive("PIN", SqlTypeNamesSupport.getSqlTypeCode("BIGINT"), DatabaseMetaData.columnNoNulls),
                            FieldDescription.primitive("COORDS", SqlTypeNamesSupport.getSqlTypeCode("STRUCT"), DatabaseMetaData.columnNoNulls)
                    ));
                    try (final RelationalResultSet resultSet = ps.executeQuery()) {
                        ResultSetAssert.assertThat(resultSet)
                                .hasNextRow().hasColumn("LOCATION", expectedLocation)
                                .hasNoNextRow();
                    }
                } else {
                    Assert.assertThrows(SQLException.class, ps::executeQuery);
                }
            }
        }
    }

    @Test
    void prepareUpdateWithArrayOfPrimitives() throws Exception {
        final var customerAttributes = new Object[]{"george", "adam", "billy"};
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                statement.execute("INSERT INTO RestaurantComplexRecord(rest_no, name) VALUES (1, 'mango & miso'), (2, 'basil & brawn'), (3, 'peach & pepper'), (4, 'smoky skillet'), (5, 'the tin pot')");
            }
            // "new" should not be quoted. TODO ([Post] Fix identifiers case-sensitivity matching in plan generator)
            final var query = "UPDATE RestaurantComplexRecord SET customer = ?param WHERE rest_no = 1 RETURNING \"new\".customer";
            try (var ps = ddl.setSchemaAndGetConnection().prepareStatement(query)) {

                final var customer = ddl.getConnection().createArrayOf("STRING", customerAttributes);
                ps.setArray("param", customer);
                final var expectedCustomer = new RowArray(
                        Arrays.stream(customerAttributes).collect(Collectors.toList()), RelationalArrayMetaData.ofPrimitive(
                                Types.VARCHAR, DatabaseMetaData.columnNoNulls));
                try (final RelationalResultSet resultSet = ps.executeQuery()) {
                    ResultSetAssert.assertThat(resultSet)
                            .hasNextRow().hasColumn("CUSTOMER", expectedCustomer)
                            .hasNoNextRow();
                }
            }
        }
    }

    @Disabled
    // owing to: TODO (Array parameter in Relational does not work with nested types)
    @Test
    void prepareUpdateWithArrayOfStructs() throws Exception {
        final var restaurantTagAttributes = new Object[][]{{"chinese", 343}, {"top-rated", 2356}, {"exotic", 10}};
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                statement.execute("INSERT INTO RestaurantComplexRecord(rest_no, name) VALUES (1, 'mango & miso'), (2, 'basil & brawn'), (3, 'peach & pepper'), (4, 'smoky skillet'), (5, 'the tin pot')");
            }
            // "new" should not be quoted. TODO ([Post] Fix identifiers case-sensitivity matching in plan generator)
            final var query = "UPDATE RestaurantComplexRecord SET customer = ?param WHERE rest_no = 1 RETURNING \"new\".customer";
            try (var ps = ddl.setSchemaAndGetConnection().prepareStatement(query)) {
                final var restaurantTags = ddl.getConnection().createArrayOf("STRUCT",
                        Arrays.stream(restaurantTagAttributes)
                                .map(o -> {
                                    try {
                                        return ddl.getConnection().createStruct("RESTAURANTTAG", o);
                                    } catch (SQLException e) {
                                        throw new RuntimeException(e);
                                    }
                                }).toArray());
                ps.setArray("param", restaurantTags);
                final var expectedRestaurantTags = new RowArray(
                        Arrays.stream(restaurantTagAttributes).map(ArrayRow::new).collect(Collectors.toList()),
                        RelationalArrayMetaData.ofPrimitive(Types.VARCHAR, DatabaseMetaData.columnNoNulls));
                try (final RelationalResultSet resultSet = ps.executeQuery()) {
                    ResultSetAssert.assertThat(resultSet)
                            .hasNextRow().hasColumn("TAGS", expectedRestaurantTags)
                            .hasNoNextRow();
                }
            }
        }
    }

    @Test
    void prepareInListWrongTypeInArray() throws Exception {
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {

            // IN list parameter is an array of Long, but has some non-Long elements.
            try (var ps = ddl.setSchemaAndGetConnection().prepareStatement("SELECT * FROM RestaurantComplexRecord WHERE rest_no in ?")) {
                ps.setArray(1, ddl.getConnection().createArrayOf("BIGINT", new Object[]{10L, "FOO"}));
                RelationalAssertions.assertThrowsSqlException(ps::executeQuery)
                        .hasErrorCode(ErrorCode.DATATYPE_MISMATCH);
            }

            // IN list parameter is an array of Long, but has no Long elements.
            try (var ps = ddl.setSchemaAndGetConnection().prepareStatement("SELECT * FROM RestaurantComplexRecord WHERE rest_no in ?")) {
                ps.setArray(1, ddl.getConnection().createArrayOf("BIGINT", new Object[]{"FOO", "BAR"}));
                RelationalAssertions.assertThrowsSqlException(ps::executeQuery)
                        .hasErrorCode(ErrorCode.DATATYPE_MISMATCH);
            }

            // IN list parameter is an array of structs, but has no elements.
            try (var ps = ddl.setSchemaAndGetConnection().prepareStatement("SELECT * FROM RestaurantComplexRecord WHERE (rest_no, name) in ?")) {
                RelationalAssertions.assertThrowsSqlException(() -> ps.setArray(1, ddl.getConnection().createArrayOf("STRUCT", new Object[]{})))
                        .hasMessage("Cannot determine the complete component type of array of struct since it has no elements!")
                        .hasErrorCode(ErrorCode.INTERNAL_ERROR);
            }

            // IN list parameter is an array of structs, but constituent is not a struct.
            try (var ps = ddl.setSchemaAndGetConnection().prepareStatement("SELECT * FROM RestaurantComplexRecord WHERE (rest_no, name) in ?")) {
                RelationalAssertions.assertThrowsSqlException(() -> ps.setArray(1, ddl.getConnection().createArrayOf("STRUCT", new Object[]{100L})))
                        .hasMessage("Element of the array of struct is not of struct type!")
                        .hasErrorCode(ErrorCode.DATATYPE_MISMATCH);
            }

            // IN list parameter is an array of structs, but the shape of constituent structs are different.
            try (var ps = ddl.setSchemaAndGetConnection().prepareStatement("SELECT * FROM RestaurantComplexRecord WHERE (rest_no, name) in ?")) {
                final var structA = ddl.getConnection().createStruct("na", new Object[]{10L, "a"});
                final var structB = ddl.getConnection().createStruct("na", new Object[]{20L, "b", 100L, "c"});
                ps.setArray(1, ddl.getConnection().createArrayOf("STRUCT", new Object[]{structA, structB}));
                RelationalAssertions.assertThrowsSqlException(ps::executeQuery)
                        .hasMessage("Elements of struct array literal are not of identical shape!")
                        .hasErrorCode(ErrorCode.DATATYPE_MISMATCH);
            }
        }
    }

    @Test
    void prepareInListOfTuple() throws Exception {
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                statement.execute("INSERT INTO RestaurantComplexRecord(rest_no, name) VALUES (1, 'mango & miso'), (2, 'basil & brawn'), (3, 'peach & pepper'), (4, 'smoky skillet'), (5, 'the tin pot')");
            }
            try (var ps = ddl.setSchemaAndGetConnection().prepareStatement("SELECT * FROM RestaurantComplexRecord WHERE (rest_no, name) in ?")) {
                final var structA = ddl.getConnection().createStruct("na", new Object[]{1L, "mango & miso"});
                final var structB = ddl.getConnection().createStruct("na", new Object[]{2L, "basil & brawn"});
                ps.setArray(1, ddl.getConnection().createArrayOf("STRUCT", new Object[]{structA, structB}));

                try (var rs = ps.executeQuery()) {
                    ResultSetAssert.assertThat(rs)
                            .hasNextRow()
                            .hasColumn("REST_NO", 2L)
                            .hasNextRow()
                            .hasColumn("REST_NO", 1L)
                            .hasNoNextRow();
                }
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
            Assertions.assertThat(logAppender.getLastLogEventMessage()).contains("planCache=\"miss\"");
            try (var ps = ddl.setSchemaAndGetConnection().prepareStatement("SELECT * FROM RestaurantComplexRecord WHERE rest_no > ?val OPTIONS(LOG QUERY)")) {
                ps.setLong("val", 12);
                try (final RelationalResultSet resultSet = ps.executeQuery()) {
                    ResultSetAssert.assertThat(resultSet)
                            .hasNextRow().hasColumn("REST_NO", 13L)
                            .hasNextRow().hasColumn("REST_NO", 14L)
                            .hasNoNextRow();
                }
            }
            Assertions.assertThat(logAppender.getLastLogEventMessage()).contains("planCache=\"hit\"");
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

    @Test
    void setNull() throws Exception {
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().prepareStatement("INSERT INTO RestaurantComplexRecord(rest_no, name) VALUES (10, ?), (11, ?named), (12, ?)")) {
                statement.setNull(1, Types.NULL);
                statement.setNull("named", Types.NULL);
                statement.setString(2, "not null");
                Assertions.assertThat(statement.executeUpdate()).isEqualTo(3);
            }
            try (var ps = ddl.setSchemaAndGetConnection().prepareStatement("SELECT rest_no, name FROM RestaurantComplexRecord")) {
                try (final RelationalResultSet resultSet = ps.executeQuery()) {
                    ResultSetAssert.assertThat(resultSet)
                            .hasNextRow().hasColumn("name", null)
                            .hasNextRow().hasColumn("name", null)
                            .hasNextRow().hasColumn("name", "not null")
                            .hasNoNextRow();
                }
            }
            try (var statement = ddl.setSchemaAndGetConnection().prepareStatement("UPDATE RestaurantComplexRecord set name = ? where name is not null")) {
                statement.setNull(1, Types.NULL);
                Assertions.assertThat(statement.executeUpdate()).isEqualTo(1);
            }
            try (var ps = ddl.setSchemaAndGetConnection().prepareStatement("SELECT rest_no, name FROM RestaurantComplexRecord")) {
                try (final RelationalResultSet resultSet = ps.executeQuery()) {
                    ResultSetAssert.assertThat(resultSet)
                            .hasNextRow().hasColumn("name", null)
                            .hasNextRow().hasColumn("name", null)
                            .hasNextRow().hasColumn("name", null)
                            .hasNoNextRow();
                }
            }
        }
    }

    static Stream<Object> listParameterProvider() {
        return Stream.of(
                Arguments.of("empty bigint list", "a1", (BiConsumer<PreparedStatement, Connection>) (preparedStatement, connection) -> {
                    try {
                        preparedStatement.setArray(1, connection.createArrayOf("BIGINT", new Object[]{}));
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                }),
                Arguments.of("bigint list containing three elements", "a1", (BiConsumer<PreparedStatement, Connection>) (preparedStatement, connection) -> {
                    try {
                        preparedStatement.setArray(1, connection.createArrayOf("BIGINT", new Object[]{1L, 2L, 3L}));
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                }),
                Arguments.of("empty double list", "a2", (BiConsumer<PreparedStatement, Connection>) (preparedStatement, connection) -> {
                    try {
                        preparedStatement.setArray(1, connection.createArrayOf("DOUBLE", new Object[]{}));
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                }),
                Arguments.of("double list containing three elements", "a2", (BiConsumer<PreparedStatement, Connection>) (preparedStatement, connection) -> {
                    try {
                        preparedStatement.setArray(1, connection.createArrayOf("DOUBLE", new Object[]{1.0d, 2.0d, 3.0d}));
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                }),
                Arguments.of("empty string list", "a3", (BiConsumer<PreparedStatement, Connection>) (preparedStatement, connection) -> {
                    try {
                        preparedStatement.setArray(1, connection.createArrayOf("STRING", new Object[]{}));
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                }),
                Arguments.of("string list containing three elements", "a3", (BiConsumer<PreparedStatement, Connection>) (preparedStatement, connection) -> {
                    try {
                        preparedStatement.setArray(1, connection.createArrayOf("STRING", new Object[]{"a", "b", "c"}));
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                }),
                //                enable once TODO (Make createArrayOf support custom types) is fixed.
                //                Arguments.of("STRUCT", "a4", (BiConsumer<PreparedStatement, Connection>) (preparedStatement, connection) -> {
                //                    try {
                //                        preparedStatement.setArray(1, connection.createArrayOf("STRUCT", new Object[]{}));
                //                    } catch (SQLException e) {
                //                        throw new RuntimeException(e);
                //                    }
                //                }),
                Arguments.of("empty binary list", "a5", (BiConsumer<PreparedStatement, Connection>) (preparedStatement, connection) -> {
                    try {
                        preparedStatement.setArray(1, connection.createArrayOf("BINARY", new Object[]{}));
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                }),
                Arguments.of("binary list containing two elements", "a5", (BiConsumer<PreparedStatement, Connection>) (preparedStatement, connection) -> {
                    try {
                        final var array = List.of(new byte[]{1, 2, 3, 4}, new byte[]{5, 6, 7, 8});
                        preparedStatement.setArray(1, connection.createArrayOf("BINARY", array.stream().map(t -> Arrays.copyOf(t, t.length)).toArray()));
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                }));
    }

    @ParameterizedTest(name = "Test prepared {0}")
    @MethodSource("listParameterProvider")
    void emptyParametersInTheInList(String ignored, String column, BiConsumer<PreparedStatement, Connection> consumer) throws Exception {
        final String schemaTemplate = "CREATE TYPE AS STRUCT nested (a bigint)" +
                " CREATE TABLE T1(pk bigint, a1 bigint, a2 double, a3 string, a4 nested, a5 bytes, PRIMARY KEY(pk))";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().prepareStatement("select * from t1 where " + column + " in ?")) {
                consumer.accept(statement, ddl.getConnection());
                statement.execute();
            }
        }
    }

    @ParameterizedTest(name = "Test plan cache with {0}")
    @MethodSource("listParameterProvider")
    void cachingQueryWithEmptyList(String ignored, String column, BiConsumer<PreparedStatement, Connection> consumer) throws Exception {
        final String schemaTemplate = "CREATE TYPE AS STRUCT nested (a bigint)" +
                " CREATE TABLE T1(pk bigint, a1 bigint, a2 double, a3 string, a4 nested, a5 bytes, PRIMARY KEY(pk))";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().prepareStatement("select * from t1 where " + column + " in ? OPTIONS(LOG QUERY)")) {
                consumer.accept(statement, ddl.getConnection());
                statement.execute();
            }
            Assertions.assertThat(logAppender.getLastLogEventMessage()).contains("planCache=\"miss\"");
            try (var statement = ddl.setSchemaAndGetConnection().prepareStatement("select * from t1 where " + column + " in ? OPTIONS(LOG QUERY)")) {
                consumer.accept(statement, ddl.getConnection());
                statement.execute();
            }
            Assertions.assertThat(logAppender.getLastLogEventMessage()).contains("planCache=\"hit\"");

            try (var statement = ddl.setSchemaAndGetConnection().prepareStatement("select * from t1 where " + column + " in ? OPTIONS(LOG QUERY)")) {
                consumer.accept(statement, ddl.getConnection());
                statement.execute();
            }

            Assertions.assertThat(logAppender.getLastLogEventMessage()).contains("planCache=\"hit\"");
        }
    }

    @Test
    void cacheWithPromotion() throws Exception {
        final String schemaTemplate = "CREATE TYPE AS STRUCT nested (a bigint)" +
                " CREATE TABLE T1(pk bigint, a1 bigint, PRIMARY KEY(pk))";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            String query = "select * from t1 where a1 in ? OPTIONS(LOG QUERY)";
            runQueryWithArrayBinding(ddl, query, "BIGINT", new Object[]{}, false, List.of());
            runQueryWithArrayBinding(ddl, query, "INTEGER", new Object[]{1, 2, 3}, false, List.of());
            runQueryWithArrayBinding(ddl, query, "BIGINT", new Object[]{1L, 2L, 3L}, true, List.of());
        }
    }

    @Test
    void nullArrayBindingsThenStringArrayBindings() throws Exception {
        final String schemaTemplate = "CREATE TYPE AS STRUCT nested (a bigint)" +
                " CREATE TABLE T1(pk bigint, a1 string, PRIMARY KEY(pk))";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                statement.execute("INSERT INTO T1 VALUES (1, 'a'), (2, 'b'), (3, 'b')");
            }
            String query = "select * from t1 where a1 in ? OPTIONS(LOG QUERY)";
            runQueryWithArrayBinding(ddl, query, "NULL", new Object[]{}, false, List.of());
            runQueryWithArrayBinding(ddl, query, "NULL", new Object[]{}, true, List.of());
            runQueryWithArrayBinding(ddl, query, "STRING", new String[]{"a"}, false, Collections.singletonList(new Object[]{1, "a"}));
            runQueryWithArrayBinding(ddl, query, "STRING", new String[]{"b"}, true, List.of(new Object[]{2, "b"}, new Object[]{3, "b"}));
            runQueryWithArrayBinding(ddl, query, "NULL", new Object[]{}, true, List.of());
            runQueryWithArrayBinding(ddl, query, "STRING", new String[]{"d"}, true, List.of());
            runQueryWithArrayBinding(ddl, query, "STRING", new String[]{"a"}, true, Collections.singletonList(new Object[]{1, "a"}));
        }
    }

    @Test
    void stringArrayBindingThenNullArrayBinding() throws Exception {
        final String schemaTemplate = "CREATE TYPE AS STRUCT nested (a bigint)" +
                " CREATE TABLE T1(pk bigint, a1 string, PRIMARY KEY(pk))";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                statement.execute("INSERT INTO T1 VALUES (1, 'a'), (2, 'b'), (3, 'b')");
            }
            String query = "select * from t1 where a1 in ? OPTIONS(LOG QUERY)";
            runQueryWithArrayBinding(ddl, query, "STRING", new String[]{"a"}, false, Collections.singletonList(new Object[]{1, "a"}));
            runQueryWithArrayBinding(ddl, query, "STRING", new String[]{"b"}, true, List.of(new Object[]{2, "b"}, new Object[]{3, "b"}));
            runQueryWithArrayBinding(ddl, query, "NULL", new Object[]{}, true, List.of());
        }
    }

    private void runQueryWithArrayBinding(Ddl ddl, String query, String arrayType, Object[] binding, boolean hit, List<Object[]> results) throws SQLException {
        try (var statement = ddl.setSchemaAndGetConnection().prepareStatement(query)) {
            statement.setArray(1, ddl.getConnection().createArrayOf(arrayType, binding));
            ResultSetAssert.assertThat(statement.executeQuery())
                    .containsRowsExactly(results);
        }
        if (hit) {
            Assertions.assertThat(logAppender.getLastLogEventMessage()).contains("planCache=\"hit\"");
        } else {
            Assertions.assertThat(logAppender.getLastLogEventMessage()).contains("planCache=\"miss\"");
        }
    }
}
