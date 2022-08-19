/*
 * QueryTest.java
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

import com.apple.foundationdb.record.query.plan.cascades.debug.Debugger;
import com.apple.foundationdb.record.query.plan.debug.DebuggerWithSymbolTables;
import com.apple.foundationdb.relational.api.Continuation;
import com.apple.foundationdb.relational.api.FieldDescription;
import com.apple.foundationdb.relational.api.RowArray;
import com.apple.foundationdb.relational.api.StructMetaData;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.RelationalStatement;
import com.apple.foundationdb.relational.api.RelationalStructMetaData;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.recordlayer.utils.Assert;
import com.apple.foundationdb.relational.utils.Ddl;
import com.apple.foundationdb.relational.utils.ResultSetAssert;
import com.apple.foundationdb.relational.utils.RelationalAssertions;

import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.nio.charset.StandardCharsets;
import java.sql.Array;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

public class QueryTest {

    private static final String schemaTemplate =
            "CREATE STRUCT Location (address string, latitude string, longitude string)" +
                    " CREATE STRUCT \"ReviewerEndorsements\" (\"endorsementId\" int64, \"endorsementText\" string)" +
                    " CREATE STRUCT RestaurantComplexReview (reviewer int64, rating int64, endorsements \"ReviewerEndorsements\" array)" +
                    " CREATE STRUCT RestaurantTag (tag string, weight int64)" +
                    " CREATE STRUCT ReviewerStats (start_date int64, school_name string, hometown string)" +
                    " CREATE TABLE RestaurantComplexRecord (rest_no int64, name string, location Location, reviews RestaurantComplexReview ARRAY, tags RestaurantTag array, customer string array, encoded_bytes bytes, PRIMARY KEY(rest_no))" +
                    " CREATE TABLE RestaurantReviewer (id int64, name string, email string, stats ReviewerStats, PRIMARY KEY(id))" +
                    " CREATE VALUE INDEX record_name_idx on RestaurantComplexRecord(name)" +
                    " CREATE VALUE INDEX reviewer_name_idx on RestaurantReviewer(name)" +
                    " CREATE INDEX mv1 AS SELECT R.rating from RestaurantComplexRecord AS Rec, (select rating from Rec.reviews) R" +
                    " CREATE INDEX mv2 AS SELECT endo.\"endorsementText\" FROM RestaurantComplexRecord rec, (SELECT X.\"endorsementText\" FROM rec.reviews rev, (SELECT \"endorsementText\" from rev.endorsements) X) endo";

    @RegisterExtension
    @Order(0)
    public final EmbeddedRelationalExtension relationalExtension = new EmbeddedRelationalExtension();

    public QueryTest() {
        if (Debugger.getDebugger() == null) {
            Debugger.setDebugger(new DebuggerWithSymbolTables());
        }
        Debugger.setup();
    }

    @Test
    void failsToQueryWithoutASchema() throws Exception {
        try (var ddl = Ddl.builder().database("QT").relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (Connection conn = ddl.getConnection()) {
                conn.setSchema(null);

                try (Statement s = conn.createStatement()) {
                    RelationalAssertions.assertThrowsSqlException(() -> s.executeQuery("select * from RestaurantComplexRecord"))
                            .hasErrorCode(ErrorCode.UNDEFINED_SCHEMA);
                }
            }
        }
    }

    @Test
    void simpleSelect() throws Exception {
        try (var ddl = Ddl.builder().database("QT").relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                var insertedRecord = insertRestaurantComplexRecord(statement);
                Assertions.assertTrue(statement.execute("SELECT * FROM RestaurantComplexRecord"), "Did not return a result set from a select statement!");
                try (final RelationalResultSet resultSet = statement.getResultSet()) {
                    ResultSetAssert.assertThat(resultSet).hasNextRow()
                            .hasRow(insertedRecord)
                            .hasNoNextRow();
                }
            }
        }
    }

    @Test
    void canQueryPKZero() throws Exception {
        try (var ddl = Ddl.builder().database("QT").relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                var insertedRecord = insertRestaurantComplexRecord(statement, 0L, "");
                Assertions.assertTrue(statement.execute("SELECT * FROM RestaurantComplexRecord"), "Did not return a result set from a select statement!");
                try (final RelationalResultSet resultSet = statement.getResultSet()) {
                    ResultSetAssert.assertThat(resultSet).hasNextRow()
                            .hasRow(insertedRecord)
                            .hasNoNextRow();
                }
            }
        }
    }

    @Test
    void selectWithPredicateVariants() throws Exception {
        try (var ddl = Ddl.builder().database("QT").relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                insertRestaurantComplexRecord(statement);
                Message r11 = insertRestaurantComplexRecord(statement, 11L);

                try (final RelationalResultSet resultSet = statement.executeQuery("SELECT * FROM RestaurantComplexRecord WHERE rest_no > 10")) {
                    ResultSetAssert.assertThat(resultSet).hasNextRow()
                            .hasRow(r11)
                            .hasNoNextRow();
                }

                try (final RelationalResultSet resultSet = statement.executeQuery("SELECT * FROM RestaurantComplexRecord WHERE rest_no >= 11")) {
                    ResultSetAssert.assertThat(resultSet).hasNextRow()
                            .hasRow(r11)
                            .hasNoNextRow();
                }

                try (final RelationalResultSet resultSet = statement.executeQuery("SELECT * FROM RestaurantComplexRecord WHERE 10 < rest_no")) {
                    ResultSetAssert.assertThat(resultSet).hasNextRow()
                            .hasRow(r11)
                            .hasNoNextRow();
                }

                try (final RelationalResultSet resultSet = statement.executeQuery("SELECT * FROM RestaurantComplexRecord WHERE 11 <= rest_no")) {
                    ResultSetAssert.assertThat(resultSet).hasNextRow()
                            .hasRow(r11)
                            .hasNoNextRow();
                }
            }
        }
    }

    @Test
    void explainTableScan() throws Exception {
        try (var ddl = Ddl.builder().database("QT").relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                try (final RelationalResultSet resultSet = statement.executeQuery("EXPLAIN SELECT * FROM RestaurantComplexRecord WHERE rest_no > 10")) {
                    resultSet.next();
                    String plan = resultSet.getString(1);
                    assertThat(plan).matches(".*Scan.*RESTAURANTCOMPLEXRECORD.*REST_NO GREATER_THAN 10.* as REST_NO, .* as NAME, .* as LOCATION, .* as REVIEWS, .* as TAGS, .* as CUSTOMER, .* as ENCODED_BYTES.*");
                }
            }
        }
    }

    @Test
    void explainHintedIndexScan() throws Exception {
        try (var ddl = Ddl.builder().database("QT").relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                try (final RelationalResultSet resultSet = statement.executeQuery("EXPLAIN SELECT * FROM RestaurantComplexRecord USE INDEX (record_name_idx) WHERE rest_no > 10")) {
                    resultSet.next();
                    String plan = resultSet.getString(1);
                    assertThat(plan).matches(".*Fetch.*Covering.*Index.*RECORD_NAME_IDX.*REST_NO GREATER_THAN 10.* as REST_NO, .* as NAME, .* as LOCATION, .* as REVIEWS, .* as TAGS, .* as CUSTOMER, .* as ENCODED_BYTES.*");
                }
            }
        }
    }

    @Test
    void explainUnhintedIndexScan() throws Exception {
        try (var ddl = Ddl.builder().database("QT").relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                try (final RelationalResultSet resultSet = statement.executeQuery("EXPLAIN SELECT * FROM RestaurantComplexRecord AS R WHERE EXISTS (SELECT * FROM R.reviews AS RE WHERE RE.rating >= 9)")) {
                    resultSet.next();
                    String plan = resultSet.getString(1);
                    assertThat(plan).matches(".*Index.*MV1.*\\[9\\],>.* as REST_NO, .* as NAME, .* as LOCATION, .* as REVIEWS, .* as TAGS, .* as CUSTOMER, .* as ENCODED_BYTES.*");
                }
            }
        }
    }

    @Test
    void selectWithPredicateCompositionVariants() throws Exception {
        try (var ddl = Ddl.builder().database("QT").relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                insertRestaurantComplexRecord(statement);
                Message l42 = insertRestaurantComplexRecord(statement, 42L, "rest1");
                Message l43 = insertRestaurantComplexRecord(statement, 43L, "rest1");
                Message l44 = insertRestaurantComplexRecord(statement, 44L, "rest1");
                Message l45 = insertRestaurantComplexRecord(statement, 45L, "rest2");
                try (RelationalResultSet resultSet = statement.executeQuery("SELECT * FROM RestaurantComplexRecord WHERE rest_no > 42 AND name = 'rest1'")) {
                    ResultSetAssert.assertThat(resultSet).describedAs("where rest_no > 42 AND name = 'rest1'")
                            .containsRowsExactly(l43, l44);
                }
                try (RelationalResultSet resultSet = statement.executeQuery("SELECT * FROM RestaurantComplexRecord WHERE name = 'rest2' OR name = 'rest1'")) {
                    ResultSetAssert.assertThat(resultSet).describedAs("where name = 'rest2' OR name = 'rest1'")
                            .containsRowsExactly(l42, l43, l44, l45);
                }
                try (RelationalResultSet resultSet = statement.executeQuery("SELECT * FROM RestaurantComplexRecord WHERE rest_no = (40+2)")) {
                    ResultSetAssert.assertThat(resultSet).describedAs("where rest_no = (40+2)")
                            .containsRowsExactly(l42);
                }
                try (RelationalResultSet resultSet = statement.executeQuery("SELECT * FROM RestaurantComplexRecord WHERE (40+2) = rest_no")) {
                    ResultSetAssert.assertThat(resultSet).describedAs("where (40+2) = rest_no")
                            .containsRowsExactly(l42);
                }
                try (RelationalResultSet resultSet = statement.executeQuery("SELECT * FROM RestaurantComplexRecord WHERE (44-2) = rest_no")) {
                    ResultSetAssert.assertThat(resultSet).describedAs("where (44-2) = rest_no")
                            .containsRowsExactly(l42);
                }
                try (RelationalResultSet resultSet = statement.executeQuery("SELECT * FROM RestaurantComplexRecord WHERE 0X2A = rest_no")) {
                    ResultSetAssert.assertThat(resultSet).describedAs("where 0X2A = rest_no")
                            .containsRowsExactly(l42);
                }
                try (RelationalResultSet resultSet = statement.executeQuery("SELECT * FROM RestaurantComplexRecord WHERE rest_no < -1")) {
                    ResultSetAssert.assertThat(resultSet).describedAs("where rest_no < -1").isEmpty();
                }
                try (RelationalResultSet resultSet = statement.executeQuery("SELECT * FROM RestaurantComplexRecord WHERE 10 < -3.9")) {
                    ResultSetAssert.assertThat(resultSet).describedAs("where 10 < -3.9").isEmpty();
                }
            }
        }
    }

    @Test
    void selectWithNullInComparisonOperator() throws Exception {
        try (var ddl = Ddl.builder().database("QT").relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                var insertedRecord = insertRestaurantComplexRecord(statement);
                try (final RelationalResultSet resultSet = statement.executeQuery("SELECT * FROM RestaurantComplexRecord WHERE 1 is null")) {
                    ResultSetAssert.assertThat(resultSet).isEmpty();
                }
                try (final RelationalResultSet resultSet = statement.executeQuery("SELECT * FROM RestaurantComplexRecord WHERE 1 is not null")) {
                    ResultSetAssert.assertThat(resultSet).hasNextRow().hasRow(insertedRecord);
                }
                try (final RelationalResultSet resultSet = statement.executeQuery("SELECT * FROM RestaurantComplexRecord WHERE 1 != null")) {
                    ResultSetAssert.assertThat(resultSet).isEmpty();
                }
                try (final RelationalResultSet resultSet = statement.executeQuery("SELECT * FROM RestaurantComplexRecord WHERE 1 < null")) {
                    ResultSetAssert.assertThat(resultSet).isEmpty();
                }
                try (final RelationalResultSet resultSet = statement.executeQuery("SELECT * FROM RestaurantComplexRecord WHERE null > 1")) {
                    ResultSetAssert.assertThat(resultSet).isEmpty();
                }
                try (final RelationalResultSet resultSet = statement.executeQuery("SELECT * FROM RestaurantComplexRecord WHERE 1 <= null")) {
                    ResultSetAssert.assertThat(resultSet).isEmpty();
                }
                try (final RelationalResultSet resultSet = statement.executeQuery("SELECT * FROM RestaurantComplexRecord WHERE null >= 1")) {
                    ResultSetAssert.assertThat(resultSet).isEmpty();
                }
            }
        }
    }

    @Test
    void selectWithFalsePredicate() throws Exception {
        try (var ddl = Ddl.builder().database("QT").relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                insertRestaurantComplexRecord(statement);
                insertRestaurantComplexRecord(statement, 11L);
                try (final RelationalResultSet resultSet = statement.executeQuery("select * from RestaurantComplexRecord where 42 is null AND 11 = rest_no")) {
                    ResultSetAssert.assertThat(resultSet).isEmpty();
                }
            }
        }
    }

    @Test
    void selectWithFalsePredicate2() throws Exception {
        try (var ddl = Ddl.builder().database("QT").relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                insertRestaurantComplexRecord(statement);
                insertRestaurantComplexRecord(statement, 11L);
                try (final RelationalResultSet resultSet = statement.executeQuery("select * from RestaurantComplexRecord where true = false")) {
                    ResultSetAssert.assertThat(resultSet).isEmpty();
                }
            }
        }
    }

    @Test
    void selectWithContinuation() throws Exception {
        try (var ddl = Ddl.builder().database("QT").relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                insertRestaurantComplexRecord(statement);
                Message l42 = insertRestaurantComplexRecord(statement, 42L, "rest1");
                Message l43 = insertRestaurantComplexRecord(statement, 43L, "rest1");
                Message l44 = insertRestaurantComplexRecord(statement, 44L, "rest1");
                Message l45 = insertRestaurantComplexRecord(statement, 45L, "rest2");
                final String initialQuery = "select * from RestaurantComplexRecord where rest_no > 40";
                Continuation continuation = Continuation.BEGIN;
                final List<Message> expected = List.of(l42, l43, l44, l45);
                int i = 0;

                while (!continuation.atEnd()) {
                    String query = initialQuery;
                    if (!continuation.atBeginning()) {
                        query += " WITH CONTINUATION '" + Base64.getEncoder().encodeToString(continuation.getBytes()) + "'";
                    }
                    try (final RelationalResultSet resultSet = statement.executeQuery(query)) {
                        // assert result matches expected
                        Assertions.assertNotNull(resultSet, "Did not return a result set!");
                        ResultSetAssert.assertThat(resultSet).hasNextRow()
                                .hasRow(expected.get(i));
                        // get continuation for the next query
                        continuation = resultSet.getContinuation();
                        i += 1;
                    }
                }
            }
        }
    }

    @Test
    void testSelectWithIndexHint() throws Exception {
        try (var ddl = Ddl.builder().database("QT").relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                insertRestaurantComplexRecord(statement);
                // successfully execute a query with hinted index
                try (final RelationalResultSet resultSet = statement.executeQuery("SELECT name FROM RestaurantComplexRecord USE INDEX (record_name_idx)")) {
                    ResultSetAssert.assertThat(resultSet)
                            .meetsForAllRows(ResultSetAssert.perRowCondition(rs -> "testName".equals(rs.getString(1)), "Name should = testName"));
                }
                // successfully execute a query with multiple hinted indexes
                try (final RelationalResultSet resultSet = statement.executeQuery("SELECT name FROM RestaurantComplexRecord USE INDEX (record_name_idx, reviewer_name_idx)")) {
                    ResultSetAssert.assertThat(resultSet)
                            .meetsForAllRows(ResultSetAssert.perRowCondition(rs -> "testName".equals(rs.getString(1)), "name should equals 'testName'"));
                }
                // successfully execute a query with multiple hinted indexes, different syntax
                try (final RelationalResultSet resultSet = statement.executeQuery("SELECT name FROM RestaurantComplexRecord USE INDEX (record_name_idx), USE INDEX (reviewer_name_idx)")) {
                    ResultSetAssert.assertThat(resultSet)
                            .meetsForAllRows(ResultSetAssert.perRowCondition(rs -> "testName".equals(rs.getString(1)), "name should equals 'testName'"));
                }
                // exception is thrown when hinted indexes don't exist
                RelationalAssertions.assertThrowsSqlException(() -> statement.executeQuery("SELECT * FROM RestaurantRecord USE INDEX (name) WHERE 11 <= rest_no"))
                        .hasErrorCode(ErrorCode.UNDEFINED_INDEX)
                        .hasMessage("Unknown index(es) NAME");
            }
        }
    }

    @Test
    void testSelectWithCoveringIndexHint() throws Exception {
        final String schema = "CREATE TABLE T1(COL1 int64, COL2 int64, COL3 int64, PRIMARY KEY(COL1, COL3))" +
                " CREATE VALUE INDEX T1_IDX on T1(COL1, COL3) INCLUDE (COL2)";
        try (var ddl = Ddl.builder().database("QT").relationalExtension(relationalExtension).schemaTemplate(schema).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                final Message row1 = statement.getDataBuilder("T1").setField("COL1", 42L).setField("COL2", 100L).setField("COL3", 200L).build();
                int cnt = statement.executeInsert("T1", row1);
                Assertions.assertEquals(1, cnt, "Incorrect insertion count");

                final Message row2 = statement.getDataBuilder("T1").setField("COL1", 43L).setField("COL2", 101L).setField("COL3", 201L).build();
                cnt = statement.executeInsert("T1", row2);
                Assertions.assertEquals(1, cnt, "Incorrect insertion count");

                Assertions.assertTrue(statement.execute("SELECT * from T1 USE INDEX (T1_IDX)"), "Did not return a result set from a select statement!");
                try (final RelationalResultSet resultSet = statement.getResultSet()) {
                    ResultSetAssert.assertThat(resultSet).containsRowsExactly(row1, row2);
                }
            }
        }
    }

    @Test
    void projectIndividualColumns() throws Exception {
        try (var ddl = Ddl.builder().database("QT").relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                insertRestaurantComplexRecord(statement);
                try (final RelationalResultSet resultSet = statement.executeQuery("SELECT name FROM RestaurantComplexRecord WHERE 11 <= rest_no")) {
                    ResultSetAssert.assertThat(resultSet)
                            .meetsForAllRows(ResultSetAssert.perRowCondition(rs -> "testName".equals(rs.getString(1)), "name should equals 'testName'"));
                }
            }
        }
    }

    @Test
    void projectIndividualQualifiedColumns() throws Exception {
        try (var ddl = Ddl.builder().database("QT").relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                insertRestaurantComplexRecord(statement);
                try (final RelationalResultSet resultSet = statement.executeQuery("SELECT RestaurantComplexRecord.name FROM RestaurantComplexRecord WHERE 11 <= rest_no")) {
                    ResultSetAssert.assertThat(resultSet)
                            .meetsForAllRows(ResultSetAssert.perRowCondition(rs -> "testName".equals(rs.getString(1)), "name should equals 'testName'"));
                }
            }
        }
    }

    @Test
    void projectIndividualQualifiedColumnsOverAlias() throws Exception {
        try (var ddl = Ddl.builder().database("QT").relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                insertRestaurantComplexRecord(statement);
                try (final RelationalResultSet resultSet = statement.executeQuery("SELECT name FROM RestaurantComplexRecord AS X WHERE 11 <= rest_no")) {
                    ResultSetAssert.assertThat(resultSet)
                            .meetsForAllRows(ResultSetAssert.perRowCondition(rs -> "testName".equals(rs.getString(1)), "name should equals 'testName'"));
                }
            }
        }
    }

    @Test
    void projectIndividualQualifiedColumnsOverAlias2() throws Exception {
        try (var ddl = Ddl.builder().database("QT").relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                insertRestaurantComplexRecord(statement);
                try (final RelationalResultSet resultSet = statement.executeQuery("SELECT X.name FROM RestaurantComplexRecord AS X WHERE 11 <= rest_no")) {
                    ResultSetAssert.assertThat(resultSet)
                            .meetsForAllRows(ResultSetAssert.perRowCondition(rs -> "testName".equals(rs.getString(1)), "name should equals 'testName'"));
                }
            }
        }
    }

    @Test
    void getBytes() throws Exception {
        try (var ddl = Ddl.builder().database("QT").relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                insertRestaurantComplexRecord(statement);
                insertRestaurantComplexRecord(statement, 1, "getBytes", "blob1".getBytes(StandardCharsets.UTF_8));
                insertRestaurantComplexRecord(statement, 2, "getBytes", "".getBytes(StandardCharsets.UTF_8));

                try (final RelationalResultSet resultSet = statement.executeQuery("SELECT * FROM RestaurantComplexRecord WHERE name = 'getBytes'")) {
                    ResultSetAssert.assertThat(resultSet)
                            .meetsForAllRows(ResultSetAssert.perRowCondition(rs -> {
                                byte[] bytes = rs.getBytes("ENCODED_BYTES");
                                switch ((int) resultSet.getLong("REST_NO")) {
                                    case 1:
                                        return Arrays.equals(bytes, "blob1".getBytes(StandardCharsets.UTF_8));
                                    case 2:
                                        return Arrays.equals(bytes, "".getBytes(StandardCharsets.UTF_8));
                                    default:
                                        return false;
                                }
                            }, "Should find correct encoded_bytes"));
                }
            }
        }
    }

    @Test
    void partiqlNestingWorks() throws Exception {
        final String schema = "CREATE STRUCT A ( b B )" +
                " CREATE STRUCT B ( c C )" +
                " CREATE STRUCT C ( d D )" +
                " CREATE STRUCT D ( e E )" +
                " CREATE STRUCT E ( f int64 )" +
                " CREATE TABLE tbl1 (id int64, c C, a A, PRIMARY KEY(id))";
        try (var ddl = Ddl.builder().database("QT").relationalExtension(relationalExtension).schemaTemplate(schema).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                final Message result = statement.getDataBuilder("TBL1")
                        .setField("ID", 42L)
                        .setField("C", statement.getDataBuilder("C")
                                .setField("D", statement.getDataBuilder("D")
                                        .setField("E", statement.getDataBuilder("E")
                                                .setField("F", 128L)
                                                .build())
                                        .build())
                                .build())
                        .setField("A", statement.getDataBuilder("A")
                                .setField("B", statement.getDataBuilder("B")
                                        .setField("C", statement.getDataBuilder("C")
                                                .setField("D", statement.getDataBuilder("D")
                                                        .setField("E", statement.getDataBuilder("E")
                                                                .setField("F", 128L)
                                                                .build())
                                                        .build())
                                                .build())
                                        .build())
                                .build())
                        .build();
                final int cnt = statement.executeInsert("TBL1", result);
                Assertions.assertEquals(1, cnt, "Incorrect insertion count");
                Assertions.assertTrue(statement.execute("SELECT id, c.d.e.f, a.b.c.d.e.f FROM tbl1"), "Did not return a result set from a select statement!");
                try (final RelationalResultSet resultSet = statement.getResultSet()) {
                    ResultSetAssert.assertThat(resultSet).hasNextRow()
                            .hasRowExactly(42L, 128L, 128L)
                            .hasNoNextRow();
                }
                Assertions.assertTrue(statement.execute("SELECT c.d.e FROM tbl1"), "Did not return a result set from a select statement!");
                try (final RelationalResultSet resultSet = statement.getResultSet()) {
                    final Message expected = statement.getDataBuilder("E")
                            .setField("F", 128L)
                            .build();
                    ResultSetAssert.assertThat(resultSet).hasNextRow()
                            .hasColumn("E", expected)
                            .hasNoNextRow();
                }
            }
        }
    }

    @Test
    void partiqlNestingWorksWithRepeatedLeafWork() throws Exception {
        final String schema = "CREATE STRUCT A ( b B )" +
                " CREATE STRUCT B ( c C )" +
                " CREATE STRUCT C ( d D )" +
                " CREATE STRUCT D ( e E )" +
                " CREATE STRUCT E ( f int64 array )" +
                " CREATE TABLE tbl1 (id int64, c C, a A, PRIMARY KEY(id))";
        try (var ddl = Ddl.builder().database("QT").relationalExtension(relationalExtension).schemaTemplate(schema).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                final Message result = statement.getDataBuilder("TBL1")
                        .setField("ID", 42L)
                        .setField("C", statement.getDataBuilder("C")
                                .setField("D", statement.getDataBuilder("D")
                                        .setField("E", statement.getDataBuilder("E")
                                                .addRepeatedField("F", 128L)
                                                .build())
                                        .build())
                                .build())
                        .setField("A", statement.getDataBuilder("A")
                                .setField("B", statement.getDataBuilder("B")
                                        .setField("C", statement.getDataBuilder("C")
                                                .setField("D", statement.getDataBuilder("D")
                                                        .setField("E", statement.getDataBuilder("E")
                                                                .addRepeatedField("F", 128L)
                                                                .build())
                                                        .build())
                                                .build())
                                        .build())
                                .build())
                        .build();
                final int cnt = statement.executeInsert("TBL1", result);
                Assertions.assertEquals(1, cnt, "Incorrect insertion count");
                Assertions.assertTrue(statement.execute("SELECT id, c.d.e.f, a.b.c.d.e.f FROM tbl1"), "Did not return a result set from a select statement!");
                try (final RelationalResultSet resultSet = statement.getResultSet()) {
                    StructMetaData col2Meta = new RelationalStructMetaData(
                            FieldDescription.primitive("_1", Types.BIGINT, true)
                    );
                    StructMetaData col3Meta = new RelationalStructMetaData(
                            FieldDescription.primitive("_2", Types.BIGINT, true)
                    );
                    Array expectedCol2 = new RowArray(List.of(new ArrayRow(new Object[]{128L})), col2Meta);
                    Array expectedCol3 = new RowArray(List.of(new ArrayRow(new Object[]{128L})), col3Meta);
                    ResultSetAssert.assertThat(resultSet).hasNextRow()
                            .hasRowExactly(42L, expectedCol2, expectedCol3)
                            .hasNoNextRow();
                }
            }
        }
    }

    @Test
    void partiqlNestingNestedPathCollisionWithAlias() throws Exception {
        final String schema = "CREATE STRUCT A ( b B )" +
                " CREATE STRUCT B ( c C )" +
                " CREATE STRUCT C ( d D )" +
                " CREATE STRUCT D ( e E )" +
                " CREATE STRUCT E ( f int64 )" +
                " CREATE TABLE tbl1 (id int64, c C, a A, PRIMARY KEY(id))" +
                " CREATE TABLE tbl2 (id int64, x int64, PRIMARY KEY(id))";
        try (var ddl = Ddl.builder().database("QT").relationalExtension(relationalExtension).schemaTemplate(schema).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                RelationalAssertions.assertThrowsSqlException(() -> statement.execute("select * from tbl1 x, tbl2 where x = 42"))
                        .hasErrorCode(ErrorCode.AMBIGUOUS_COLUMN)
                        .hasMessage("ambiguous column name 'X'");
            }
        }
    }

    @Test
    void partiqlAccessingNestedFieldWithInnerRepeatedFieldsFails() throws Exception {
        final String schema = "CREATE STRUCT A ( b B )" +
                " CREATE STRUCT B ( c C )" +
                " CREATE STRUCT C ( d D )" +
                " CREATE STRUCT D ( e E array )" +
                " CREATE STRUCT E ( f int64 array )" +
                " CREATE TABLE tbl1 (id int64, c C, a A, PRIMARY KEY(id))";
        try (var ddl = Ddl.builder().database("QT").relationalExtension(relationalExtension).schemaTemplate(schema).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                try {
                    statement.execute("SELECT id, c.d.e.f, a.b.c.d.e.f FROM tbl1");
                    fail("expected an exception to be thrown by running 'SELECT id, c.d.e.f, a.b.c.d.e.f FROM tbl1'");
                } catch (SQLException cse) {
                    cse.getMessage().contains("field type 'f' can only be resolved on records");
                }
            }
        }
    }

    @Disabled
    // until we fix the implicit fetch operator in record layer.
    void projectIndividualPredicateColumns() throws Exception {
        try (var ddl = Ddl.builder().database("QT").relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                insertRestaurantComplexRecord(statement);
                try (final RelationalResultSet resultSet = statement.executeQuery("SELECT rest_no FROM RestaurantComplexRecord WHERE 11 <= rest_no")) {
                    ResultSetAssert.assertThat(resultSet).meetsForAllRows(ResultSetAssert.perRowCondition(rs -> resultSet.getLong(1) == 11L, "rest_no should be 11L"));
                }
            }
        }
    }

    @Disabled
    // until we implement1 operators for type promotion and casts in record layer.
    void predicateWithImplicitCast() throws Exception {
        try (var ddl = Ddl.builder().database("QT").relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                insertRestaurantComplexRecord(statement);
                Message l42 = insertRestaurantComplexRecord(statement, 42L, "rest1");
                Message l43 = insertRestaurantComplexRecord(statement, 43L, "rest1");
                Message l44 = insertRestaurantComplexRecord(statement, 44L, "rest1");
                Message l45 = insertRestaurantComplexRecord(statement, 45L, "rest2");
                try (final RelationalResultSet resultSet = statement.executeQuery("SELECT * FROM RestaurantComplexRecord WHERE rest_no > 40.5")) {
                    ResultSetAssert.assertThat(resultSet).containsRowsExactly(l42, l43, l44, l45);
                }
            }
        }
    }

    @Test
    void existsPredicateWorks() throws Exception {
        try (var ddl = Ddl.builder().database("QT").relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                insertRestaurantComplexRecord(statement);
                insertRestaurantComplexRecord(statement, 42L, "rest1", List.of(Triple.of(1L, 4L, List.of()), Triple.of(2L, 5L, List.of())));
                Message l43 = insertRestaurantComplexRecord(statement, 43L, "rest2", List.of(Triple.of(3L, 9L, List.of()), Triple.of(4L, 8L, List.of())));
                Message l44 = insertRestaurantComplexRecord(statement, 44L, "rest3", List.of(Triple.of(3L, 10L, List.of())));
                try (final RelationalResultSet resultSet = statement.executeQuery("SELECT * FROM RestaurantComplexRecord AS R WHERE EXISTS (SELECT * FROM R.reviews AS RE WHERE RE.rating >= 9)")) {
                    ResultSetAssert.assertThat(resultSet).containsRowsExactly(l43, l44);
                }
            }
        }
    }

    @Test
    void existsPredicateNestedWorks() throws Exception {
        try (var ddl = Ddl.builder().database("QT").relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                insertRestaurantComplexRecord(statement);
                Message l42 = insertRestaurantComplexRecord(statement, 42L, "rest1",
                        List.of(Triple.of(1L, 4L, List.of(
                                        Pair.of(400L, "good"),
                                        Pair.of(401L, "meh"))),
                                Triple.of(2L, 5L, List.of(
                                        Pair.of(402L, "awesome"),
                                        Pair.of(401L, "wow")))));
                try (final RelationalResultSet resultSet = statement.executeQuery("SELECT * FROM RestaurantComplexRecord AS R WHERE EXISTS (SELECT * FROM R.reviews AS RE WHERE EXISTS(SELECT * FROM RE.endorsements AS REE WHERE REE.\"endorsementText\"='wow'))")) {
                    ResultSetAssert.assertThat(resultSet).containsRowsExactly(l42);
                }
            }
        }
    }

    @Test
    void testSubquery() throws Exception {
        final String schema = "CREATE STRUCT customer_detail(name string, phone_number string, address string) " +
                "CREATE STRUCT messages(\"TEXT\" string, timestamp int64,sent boolean) " +
                "CREATE TABLE conversations(id int64, other_party CONTACT_DETAIL, messages MESSAGES ARRAY,primary key(id))";
        try (var ddl = Ddl.builder().database("QT").relationalExtension(relationalExtension).schemaTemplate(schema).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                final Message row1 = statement.getDataBuilder("CONVERSATIONS")
                        .setField("ID", 0L)
                        .setField("OTHER_PARTY", statement.getDataBuilder("CONTACT_DETAIL")
                                .setField("NAME", "Arnaud")
                                .setField("PHONE_NUMBER", 12345)
                                .setField("ADDRESS", "6 Part Road")
                                .build())
                        .addRepeatedField("MESSAGES", statement.getDataBuilder("MESSAGES")
                                .setField("TEXT", "Hello there!")
                                .setField("TIMESTAMP", 10000)
                                .setField("SENT", true)
                                .build())
                        .addRepeatedField("MESSAGES", statement.getDataBuilder("MESSAGES")
                                .setField("TEXT", "Hi Scott!")
                                .setField("TIMESTAMP", 20000)
                                .setField("SENT", false)
                                .build())
                        .build();
                int cnt = statement.executeInsert("CONVERSATIONS", row1);
                Assertions.assertEquals(1, cnt, "Incorrect insertion count");

                final Message row2 = statement.getDataBuilder("CONVERSATIONS")
                        .setField("ID", 1L)
                        .setField("OTHER_PARTY", statement.getDataBuilder("CONTACT_DETAIL")
                                .setField("NAME", "Bri")
                                .setField("PHONE_NUMBER", 9876543)
                                .setField("ADDRESS", "10 Chancery Lane")
                                .build())
                        .addRepeatedField("MESSAGES", statement.getDataBuilder("MESSAGES")
                                .setField("TEXT", "Hello there")
                                .setField("TIMESTAMP", 30000)
                                .setField("SENT", true)
                                .build())
                        .addRepeatedField("MESSAGES", statement.getDataBuilder("MESSAGES")
                                .setField("TEXT", "What a nice weather today!")
                                .setField("TIMESTAMP", 40000)
                                .setField("SENT", true)
                                .build())
                        .build();
                cnt = statement.executeInsert("CONVERSATIONS", row2);
                Assertions.assertEquals(1, cnt, "Incorrect insertion count");

                var query = "select other_party.name, msg_texts.text, msg_texts.timestamp from conversations c, (select text, timestamp from c.messages where timestamp > 25000) as msg_texts";
                try (final RelationalResultSet resultSet = statement.executeQuery(query)) {
                    Assert.that(resultSet.next());
                    Assertions.assertEquals("Bri", resultSet.getString(1));
                    Assertions.assertEquals("Hello there", resultSet.getString(2));
                    Assertions.assertEquals("30000", resultSet.getString(3)); // no support yet for getInt

                    Assert.that(resultSet.next());
                    Assertions.assertEquals("Bri", resultSet.getString(1));
                    Assertions.assertEquals("What a nice weather today!", resultSet.getString(2));
                    Assertions.assertEquals("40000", resultSet.getString(3)); // no support yet for getInt
                    
                    Assertions.assertFalse(resultSet.next());
                }

                query = "select other_party.name, msg_texts.text, msg_texts.timestamp from conversations c, (select text, timestamp from c.messages where timestamp > 19000 and timestamp < 32000) as msg_texts";
                try (final RelationalResultSet resultSet = statement.executeQuery(query)) {
                    Assert.that(resultSet.next());
                    Assertions.assertEquals("Arnaud", resultSet.getString(1));
                    Assertions.assertEquals("Hi Scott!", resultSet.getString(2));
                    Assertions.assertEquals("20000", resultSet.getString(3)); // no support yet for getInt

                    Assert.that(resultSet.next());
                    Assertions.assertEquals("Bri", resultSet.getString(1));
                    Assertions.assertEquals("Hello there", resultSet.getString(2));
                    Assertions.assertEquals("30000", resultSet.getString(3)); // no support yet for getInt

                    Assertions.assertFalse(resultSet.next());
                }

                query = "select other_party.name from conversations c where exists (select * from c.messages where text = 'What a nice weather today!')";
                try (final RelationalResultSet resultSet = statement.executeQuery(query)) {
                    Assert.that(resultSet.next());
                    Assertions.assertEquals("Bri", resultSet.getString(1));
                    Assertions.assertFalse(resultSet.next());
                }
            }
        }
    }

    // todo (yhatem) add more tests for queries w and w/o index definition.

    private Message insertRestaurantComplexRecord(RelationalStatement s) throws RelationalException {
        return insertRestaurantComplexRecord(s, 10L);
    }

    private Message insertRestaurantComplexRecord(RelationalStatement s, Long recordNumber) throws RelationalException {
        return insertRestaurantComplexRecord(s, recordNumber, "testName");
    }

    private Message insertRestaurantComplexRecord(RelationalStatement s, Long recordNumber, @Nonnull final String recordName) throws RelationalException {
        return insertRestaurantComplexRecord(s, recordNumber, recordName, List.of());
    }

    private Message insertRestaurantComplexRecord(RelationalStatement s, Long recordNumber, @Nonnull final String recordName, @Nonnull final List<Triple<Long, Long, List<Pair<Long, String>>>> reviews) throws RelationalException {
        final var recBuilder2 = s.getDataBuilder("RESTAURANTCOMPLEXRECORD")
                .setField("REST_NO", recordNumber)
                .setField("NAME", recordName)
                .setField("LOCATION", s.getDataBuilder("LOCATION")
                        .setField("ADDRESS", "address")
                        .setField("LATITUDE", 1)
                        .setField("LONGITUDE", 1)
                        .build());

        for (final Triple<Long, Long, List<Pair<Long, String>>> review : reviews) {
            recBuilder2.addRepeatedField("REVIEWS", s.getDataBuilder("RESTAURANTCOMPLEXREVIEW")
                    .setField("REVIEWER", review.getLeft())
                    .setField("RATING", review.getMiddle())
                    .addRepeatedFields("ENDORSEMENTS", review.getRight().stream().map(endo -> {
                        try {
                            return s.getDataBuilder("ReviewerEndorsements").setField("endorsementId", endo.getLeft()).setField("endorsementText", endo.getRight()).build();
                        } catch (RelationalException e) {
                            throw new RuntimeException(e);
                        }
                    }).collect(Collectors.toList()))
                    .build()
            );
        }

        final Message rec = recBuilder2.build();
        int cnt = s.executeInsert("RESTAURANTCOMPLEXRECORD", rec);
        Assertions.assertEquals(1, cnt, "Incorrect insertion count");
        return rec;
    }

    private Message insertRestaurantComplexRecord(RelationalStatement s, int recordNumber, @Nonnull final String recordName, byte[] blob) throws RelationalException {
        Message result = s.getDataBuilder("RESTAURANTCOMPLEXRECORD")
                .setField("REST_NO", recordNumber)
                .setField("NAME", recordName)
                .setField("ENCODED_BYTES", ByteString.copyFrom(blob))
                .setField("LOCATION", s.getDataBuilder("LOCATION")
                        .setField("ADDRESS", "address")
                        .build()).build();

        int cnt = s.executeInsert("RESTAURANTCOMPLEXRECORD", result);
        Assertions.assertEquals(1, cnt, "Incorrect insertion count");
        return result;
    }

    private void failsWith(@Nonnull final Statement statement, @Nonnull final String query, @Nonnull final String errorMessage) {
        try {
            statement.execute(query);
            fail(String.format("expected an exception to be thrown by running %s", query));
        } catch (SQLException e) {
            Assertions.assertTrue(e.getMessage().contains(errorMessage));
        }
    }
}
