/*
 * ExplainTests.java
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

package com.apple.foundationdb.relational.recordlayer.query;

import com.apple.foundationdb.relational.api.Continuation;
import com.apple.foundationdb.relational.api.RelationalPreparedStatement;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.RelationalStatement;
import com.apple.foundationdb.relational.recordlayer.EmbeddedRelationalExtension;
import com.apple.foundationdb.relational.recordlayer.Utils;
import com.apple.foundationdb.relational.utils.Ddl;
import com.apple.foundationdb.relational.utils.RelationalStructAssert;
import com.apple.foundationdb.relational.utils.ResultSetAssert;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.net.URI;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;

public class ExplainTests {

    private static final String schemaTemplate =
            "CREATE TYPE AS STRUCT Location (address string, latitude string, longitude string)" +
                    " CREATE TYPE AS STRUCT \"ReviewerEndorsements\" (\"endorsementId\" bigint, \"endorsementText\" string)" +
                    " CREATE TYPE AS STRUCT RestaurantComplexReview (reviewer bigint, rating bigint, endorsements \"ReviewerEndorsements\" array)" +
                    " CREATE TYPE AS STRUCT RestaurantTag (tag string, weight bigint)" +
                    " CREATE TYPE AS STRUCT ReviewerStats (start_date bigint, school_name string, hometown string)" +
                    " CREATE TABLE RestaurantComplexRecord (rest_no bigint, name string, location Location, reviews RestaurantComplexReview ARRAY, tags RestaurantTag array, customer string array, encoded_bytes bytes, PRIMARY KEY(rest_no))" +
                    " CREATE TABLE RestaurantReviewer (id bigint, name string, email string, stats ReviewerStats, PRIMARY KEY(id))" +
                    " CREATE INDEX record_name_idx ON RestaurantComplexRecord(name)" +
                    " CREATE INDEX reviewer_name_idx ON RestaurantReviewer(name)" +
                    " CREATE INDEX mv1 AS SELECT R.rating from RestaurantComplexRecord AS Rec, (select rating from Rec.reviews) R" +
                    " CREATE INDEX mv2 AS SELECT endo.\"endorsementText\" FROM RestaurantComplexRecord rec, (SELECT X.\"endorsementText\" FROM rec.reviews rev, (SELECT \"endorsementText\" from rev.endorsements) X) endo";

    @RegisterExtension
    @Order(0)
    public final EmbeddedRelationalExtension relationalExtension = new EmbeddedRelationalExtension();

    public ExplainTests() {
        Utils.enableCascadesDebugger();
    }

    @Test
    void explainResultSetMetadataTest() throws Exception {
        final var expectedLabels = List.of("PLAN", "PLAN_HASH", "PLAN_DOT", "PLAN_GML", "PLAN_CONTINUATION", "PLANNER_METRICS");
        final var expectedTypes = List.of(Types.VARCHAR, Types.INTEGER, Types.VARCHAR, Types.VARCHAR, Types.STRUCT, Types.STRUCT);
        final var expectedContLabels = List.of(
                "EXECUTION_STATE",
                "VERSION",
                "PLAN_HASH_MODE",
                "PLAN_HASH",
                "SERIALIZED_PLAN_COMPLEXITY"
        );
        final var expectedContTypes = List.of(Types.BINARY, Types.INTEGER, Types.VARCHAR, Types.INTEGER, Types.INTEGER);
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            executeInsert(ddl);
            try (RelationalPreparedStatement ps = ddl.setSchemaAndGetConnection().prepareStatement("EXPLAIN SELECT * FROM RestaurantComplexRecord")) {
                try (final RelationalResultSet resultSet = ps.executeQuery()) {
                    final var actualMetadata = resultSet.getMetaData();
                    org.junit.jupiter.api.Assertions.assertEquals(expectedLabels.size(), actualMetadata.getColumnCount());
                    for (int i = 0; i < expectedLabels.size(); i++) {
                        org.junit.jupiter.api.Assertions.assertEquals(expectedLabels.get(i), actualMetadata.getColumnLabel(i + 1));
                        org.junit.jupiter.api.Assertions.assertEquals(expectedTypes.get(i), actualMetadata.getColumnType(i + 1));
                    }
                    final var actualContinuationMetadata = actualMetadata.getStructMetaData(5);
                    org.junit.jupiter.api.Assertions.assertEquals(expectedContLabels.size(), actualContinuationMetadata.getColumnCount());
                    for (int i = 0; i < expectedContLabels.size(); i++) {
                        org.junit.jupiter.api.Assertions.assertEquals(expectedContLabels.get(i), actualContinuationMetadata.getColumnLabel(i + 1));
                        org.junit.jupiter.api.Assertions.assertEquals(expectedContTypes.get(i), actualContinuationMetadata.getColumnType(i + 1));
                    }

                }
            }
        }
    }

    @Test
    void explainWithNoContinuationTest() throws Exception {
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            executeInsert(ddl);
            try (RelationalPreparedStatement ps = ddl.setSchemaAndGetConnection().prepareStatement("EXPLAIN SELECT * FROM RestaurantComplexRecord")) {
                ps.setMaxRows(2);
                try (final RelationalResultSet resultSet = ps.executeQuery()) {
                    final var assertResult = ResultSetAssert.assertThat(resultSet);
                    assertResult.hasNextRow()
                            .hasColumn("PLAN", "ISCAN(RECORD_NAME_IDX <,>)")
                            .hasColumn("PLAN_HASH", -1635569052)
                            .hasColumn("PLAN_CONTINUATION", null);
                    assertResult.hasNoNextRow();
                }
            }
        }
    }

    @Test
    void explainWithContinuationSerializedPlanTest() throws Exception {
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            executeInsert(ddl);
            Continuation continuation;
            try (final var connection = ddl.setSchemaAndGetConnection()) {
                try (RelationalPreparedStatement ps = ddl.setSchemaAndGetConnection().prepareStatement("SELECT * FROM RestaurantComplexRecord")) {
                    ps.setMaxRows(2);
                    continuation = consumeResultAndGetContinuation(ps, 2);
                }

                try (RelationalPreparedStatement ps = connection.prepareStatement("EXPLAIN SELECT * FROM RestaurantComplexRecord WITH CONTINUATION ?cont")) {
                    ps.setObject("cont", continuation.serialize());
                    try (final RelationalResultSet resultSet = ps.executeQuery()) {
                        final var assertResult = ResultSetAssert.assertThat(resultSet);
                        assertResult.hasNextRow()
                                .hasColumn("PLAN", "ISCAN(RECORD_NAME_IDX <,>)")
                                .hasColumn("PLAN_HASH", -1635569052);
                        final var continuationInfo = resultSet.getStruct(5);
                        org.junit.jupiter.api.Assertions.assertNotNull(continuationInfo);
                        final var assertStruct = RelationalStructAssert.assertThat(continuationInfo);
                        assertStruct.hasValue("EXECUTION_STATE", new byte[]{10, 5, 0, 21, 1, 21, 11, 17, -84, -51, 115, -104, -35, 66, 0, 94});
                        assertStruct.hasValue("VERSION", 1);
                        assertStruct.hasValue("PLAN_HASH_MODE", "VC0");
                        assertStruct.hasValue("PLAN_HASH", -1635569052);
                        assertStruct.hasValue("SERIALIZED_PLAN_COMPLEXITY", 1);
                    }
                }
            }
        }
    }

    @Test
    void explainWithContinuationSerializedPlanWithDifferentQueryTest() throws Exception {
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            executeInsert(ddl);
            Continuation continuation;
            try (final var connection = ddl.setSchemaAndGetConnection()) {
                try (RelationalPreparedStatement ps = ddl.setSchemaAndGetConnection().prepareStatement("SELECT * FROM RestaurantComplexRecord")) {
                    ps.setMaxRows(2);
                    continuation = consumeResultAndGetContinuation(ps, 2);
                }

                try (RelationalPreparedStatement ps = connection.prepareStatement("EXPLAIN SELECT rest_no FROM RestaurantComplexRecord WITH CONTINUATION ?cont")) {
                    ps.setObject("cont", continuation.serialize());
                    try (final RelationalResultSet resultSet = ps.executeQuery()) {
                        final var assertResult = ResultSetAssert.assertThat(resultSet);
                        assertResult.hasNextRow()
                                .hasColumn("PLAN", "COVERING(RECORD_NAME_IDX <,> -> [NAME: KEY[0], REST_NO: KEY[2]]) | MAP (_.REST_NO AS REST_NO)")
                                .hasColumn("PLAN_HASH", 4759756);
                        final var continuationInfo = resultSet.getStruct(5);
                        org.junit.jupiter.api.Assertions.assertNotNull(continuationInfo);
                        final var assertStruct = RelationalStructAssert.assertThat(continuationInfo);
                        assertStruct.hasValue("EXECUTION_STATE", new byte[]{10, 5, 0, 21, 1, 21, 11, 17, -84, -51, 115, -104, -35, 66, 0, 94});
                        assertStruct.hasValue("VERSION", 1);
                        assertStruct.hasValue("PLAN_HASH_MODE", "VC0");
                        assertStruct.hasValue("PLAN_HASH", -1635569052);
                        assertStruct.hasValue("SERIALIZED_PLAN_COMPLEXITY", 1);
                    }
                }
            }
        }
    }

    @Test
    void explainExecuteStatementTest() throws Exception {
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            executeInsert(ddl);
            Continuation continuation;
            try (final var connection = ddl.setSchemaAndGetConnection()) {
                try (RelationalPreparedStatement ps = ddl.setSchemaAndGetConnection().prepareStatement("SELECT * FROM RestaurantComplexRecord")) {
                    ps.setMaxRows(2);
                    continuation = consumeResultAndGetContinuation(ps, 2);
                }

                try (RelationalPreparedStatement ps = connection.prepareStatement("EXPLAIN EXECUTE CONTINUATION ?cont")) {
                    ps.setObject("cont", continuation.serialize());
                    try (final RelationalResultSet resultSet = ps.executeQuery()) {
                        final var assertResult = ResultSetAssert.assertThat(resultSet);
                        assertResult.hasNextRow()
                                .hasColumn("PLAN", "ISCAN(RECORD_NAME_IDX <,>)")
                                .hasColumn("PLAN_HASH", -1635569052);
                        final var continuationInfo = resultSet.getStruct(5);
                        org.junit.jupiter.api.Assertions.assertNotNull(continuationInfo);
                        final var assertStruct = RelationalStructAssert.assertThat(continuationInfo);
                        assertStruct.hasValue("EXECUTION_STATE", new byte[]{10, 5, 0, 21, 1, 21, 11, 17, -84, -51, 115, -104, -35, 66, 0, 94});
                        assertStruct.hasValue("VERSION", 1);
                        assertStruct.hasValue("PLAN_HASH_MODE", "VC0");
                        assertStruct.hasValue("PLAN_HASH", -1635569052);
                        assertStruct.hasValue("SERIALIZED_PLAN_COMPLEXITY", 1);
                    }
                }

            }
        }
    }

    private Continuation consumeResultAndGetContinuation(RelationalPreparedStatement ps, int numRows) throws SQLException {
        Continuation continuation;
        try (final RelationalResultSet resultSet = ps.executeQuery()) {
            for (int ignored = 0; ignored < numRows; ignored++) {
                resultSet.next();
            }
            Assertions.assertThat(resultSet.next()).isFalse();
            org.junit.jupiter.api.Assertions.assertSame(Continuation.Reason.QUERY_EXECUTION_LIMIT_REACHED, resultSet.getContinuation().getReason());
            continuation = resultSet.getContinuation();
        }
        return continuation;
    }

    private void executeInsert(Ddl ddl) throws SQLException {
        try (RelationalStatement statement = ddl.setSchemaAndGetConnection().createStatement()) {
            final int updateCount = statement.executeUpdate("INSERT INTO RestaurantComplexRecord(rest_no) VALUES (10), (11), (12), (13), (14)");
            Assertions.assertThat(updateCount).isEqualTo(5);
        }
    }
}
