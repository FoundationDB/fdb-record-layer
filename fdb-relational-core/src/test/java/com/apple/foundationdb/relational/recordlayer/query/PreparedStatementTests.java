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

import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.RelationalStatement;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.recordlayer.EmbeddedRelationalExtension;
import com.apple.foundationdb.relational.recordlayer.Utils;
import com.apple.foundationdb.relational.utils.Ddl;
import com.apple.foundationdb.relational.utils.ResultSetAssert;
import com.apple.foundationdb.relational.utils.RelationalAssertions;

import com.google.protobuf.Message;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import javax.annotation.Nonnull;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

public class PreparedStatementTests {

    private static final String schemaTemplate =
            "CREATE TYPE AS STRUCT Location (address string, latitude string, longitude string)" +
                    " CREATE TYPE AS STRUCT \"ReviewerEndorsements\" (\"endorsementId\" int64, \"endorsementText\" string)" +
                    " CREATE TYPE AS STRUCT RestaurantComplexReview (reviewer int64, rating int64, endorsements \"ReviewerEndorsements\" array)" +
                    " CREATE TYPE AS STRUCT RestaurantTag (tag string, weight int64)" +
                    " CREATE TYPE AS STRUCT ReviewerStats (start_date int64, school_name string, hometown string)" +
                    " CREATE TABLE RestaurantComplexRecord (rest_no int64, name string, location Location, reviews RestaurantComplexReview ARRAY, tags RestaurantTag array, customer string array, encoded_bytes bytes, PRIMARY KEY(rest_no))" +
                    " CREATE TABLE RestaurantReviewer (id int64, name string, email string, stats ReviewerStats, PRIMARY KEY(id))" +
                    " CREATE INDEX record_name_idx as select name from RestaurantComplexRecord" +
                    " CREATE INDEX reviewer_name_idx as select name from RestaurantReviewer" +
                    " CREATE INDEX mv1 AS SELECT R.rating from RestaurantComplexRecord AS Rec, (select rating from Rec.reviews) R" +
                    " CREATE INDEX mv2 AS SELECT endo.\"endorsementText\" FROM RestaurantComplexRecord rec, (SELECT X.\"endorsementText\" FROM rec.reviews rev, (SELECT \"endorsementText\" from rev.endorsements) X) endo";

    @RegisterExtension
    @Order(0)
    public final EmbeddedRelationalExtension relationalExtension = new EmbeddedRelationalExtension();

    public PreparedStatementTests() {
        Utils.enableCascadesDebugger();
    }

    @Test
    void failsToQueryWithoutASchema() throws Exception {
        try (var ddl = Ddl.builder().database("QT").relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
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
        try (var ddl = Ddl.builder().database("QT").relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            Message insertedRecord;
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                insertedRecord = insertRestaurantComplexRecord(statement);
            }
            try (var ps = ddl.getConnection().prepareStatement("SELECT * FROM RestaurantComplexRecord")) {
                try (final RelationalResultSet resultSet = ps.executeQuery()) {
                    ResultSetAssert.assertThat(resultSet).hasNextRow()
                            .hasRow(insertedRecord);
                    // explicitly test when nullable array is set to empty list, the RowArray object holds an empty iterable
                    Assertions.assertEquals("[]", resultSet.getArray("REVIEWS").toString());
                    // explicitly test unset Nullable array is NULL
                    Assertions.assertNull(resultSet.getArray("TAGS"));
                    Assertions.assertNull(resultSet.getArray("CUSTOMER"));
                    Assertions.assertFalse(resultSet.next());
                }
            }
        }
    }

    @Test
    void basicParameterizedQuery() throws Exception {
        try (var ddl = Ddl.builder().database("QT").relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                insertRestaurantComplexRecord(statement);
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
        try (var ddl = Ddl.builder().database("QT").relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                insertRestaurantComplexRecord(statement);
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
        try (var ddl = Ddl.builder().database("QT").relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                insertRestaurantComplexRecord(statement);
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
        try (var ddl = Ddl.builder().database("QT").relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                insertRestaurantComplexRecord(statement);
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
        try (var ddl = Ddl.builder().database("QT").relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                insertRestaurantComplexRecord(statement);
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
        try (var ddl = Ddl.builder().database("QT").relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
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

    @Disabled
    @Test
    // TODO (Prepared Statement does not cast fields if set with the wrong types)
    void setWrongTypeForQuestionMarkParameter() throws Exception {
        try (var ddl = Ddl.builder().database("QT").relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                insertRestaurantComplexRecord(statement);
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
        try (var ddl = Ddl.builder().database("QT").relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                insertRestaurantComplexRecord(statement);
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

    private Message insertRestaurantComplexRecord(RelationalStatement s) throws SQLException {
        return insertRestaurantComplexRecord(s, 10L);
    }

    private Message insertRestaurantComplexRecord(RelationalStatement s, Long recordNumber) throws SQLException {
        return insertRestaurantComplexRecord(s, recordNumber, "testName");
    }

    private Message insertRestaurantComplexRecord(RelationalStatement s, Long recordNumber, @Nonnull final String recordName) throws SQLException {
        return insertRestaurantComplexRecord(s, recordNumber, recordName, List.of(), false);
    }

    private Message insertRestaurantComplexRecord(RelationalStatement s, Long recordNumber, @Nonnull final String recordName, @Nonnull final List<Triple<Long, Long, List<Pair<Long, String>>>> reviews, boolean containsNonNullableArray) throws SQLException {
        final var recBuilder2 = s.getDataBuilder("RESTAURANTCOMPLEXRECORD")
                .setField("REST_NO", recordNumber)
                .setField("NAME", recordName)
                .setField("LOCATION", s.getDataBuilder("RESTAURANTCOMPLEXRECORD", List.of("LOCATION"))
                        .setField("ADDRESS", "address")
                        .setField("LATITUDE", 1)
                        .setField("LONGITUDE", 1)
                        .build());
        if (containsNonNullableArray) {
            for (final Triple<Long, Long, List<Pair<Long, String>>> review : reviews) {
                recBuilder2.addRepeatedField("REVIEWS", s.getDataBuilder("RESTAURANTCOMPLEXRECORD", List.of("REVIEWS"))
                        .setField("REVIEWER", review.getLeft())
                        .setField("RATING", review.getMiddle())
                        .addRepeatedFields("ENDORSEMENTS", review.getRight().stream().map(endo -> {
                            try {
                                return s.getDataBuilder("RESTAURANTCOMPLEXRECORD", List.of("REVIEWS", "ENDORSEMENTS")).setField("endorsementId", endo.getLeft()).setField("endorsementText", endo.getRight()).build();
                            } catch (SQLException e) {
                                throw new RuntimeException(e);
                            }
                        }).collect(Collectors.toList()), false)
                        .build());
            }
        } else {
            List<Message> reviewList = new LinkedList<>();
            for (final Triple<Long, Long, List<Pair<Long, String>>> review : reviews) {
                reviewList.add(s.getDataBuilder("RESTAURANTCOMPLEXRECORD", List.of("REVIEWS"))
                        .setField("REVIEWER", review.getLeft())
                        .setField("RATING", review.getMiddle())
                        .addRepeatedFields("ENDORSEMENTS", review.getRight().stream().map(endo -> {
                            try {
                                return s.getDataBuilder("RESTAURANTCOMPLEXRECORD", List.of("REVIEWS", "ENDORSEMENTS")).setField("endorsementId", endo.getLeft()).setField("endorsementText", endo.getRight()).build();
                            } catch (SQLException e) {
                                throw new RuntimeException(e);
                            }
                        }).collect(Collectors.toList()))
                        .build());
            }
            recBuilder2.addRepeatedFields("REVIEWS", reviewList);
        }

        final Message rec = recBuilder2.build();
        int cnt = s.executeInsert("RESTAURANTCOMPLEXRECORD", rec);
        Assertions.assertEquals(1, cnt, "Incorrect insertion count");
        return rec;
    }
}
