/*
 * StandardQueryTests.java
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
import com.apple.foundationdb.relational.api.EmbeddedRelationalArray;
import com.apple.foundationdb.relational.api.EmbeddedRelationalStruct;
import com.apple.foundationdb.relational.api.ImmutableRowStruct;
import com.apple.foundationdb.relational.api.RelationalArrayMetaData;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.RelationalStatement;
import com.apple.foundationdb.relational.api.RelationalStruct;
import com.apple.foundationdb.relational.api.RelationalStructMetaData;
import com.apple.foundationdb.relational.api.RowArray;
import com.apple.foundationdb.relational.api.exceptions.ContextualSQLException;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.metadata.DataType;
import com.apple.foundationdb.relational.recordlayer.ArrayRow;
import com.apple.foundationdb.relational.recordlayer.ContinuationImpl;
import com.apple.foundationdb.relational.recordlayer.EmbeddedRelationalExtension;
import com.apple.foundationdb.relational.recordlayer.EmbeddedRelationalStatement;
import com.apple.foundationdb.relational.recordlayer.Utils;
import com.apple.foundationdb.relational.util.Assert;
import com.apple.foundationdb.relational.utils.Ddl;
import com.apple.foundationdb.relational.utils.RelationalAssertions;
import com.apple.foundationdb.relational.utils.ResultSetAssert;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.sql.Array;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

public class StandardQueryTests {

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

    private static final String schemaTemplateWithNonNullableArrays =
            "CREATE TYPE AS STRUCT Location (address string, latitude string, longitude string)" +
                    " CREATE TYPE AS STRUCT \"ReviewerEndorsements\" (\"endorsementId\" bigint, \"endorsementText\" string)" +
                    " CREATE TYPE AS STRUCT RestaurantComplexReview (reviewer bigint, rating bigint, endorsements \"ReviewerEndorsements\" array NOT NULL)" +
                    " CREATE TYPE AS STRUCT RestaurantTag (tag string, weight bigint)" +
                    " CREATE TYPE AS STRUCT ReviewerStats (start_date bigint, school_name string, hometown string)" +
                    " CREATE TABLE RestaurantComplexRecord (rest_no bigint, name string, location Location, reviews RestaurantComplexReview ARRAY NOT NULL, tags RestaurantTag array NOT NULL, customer string array NOT NULL, encoded_bytes bytes, PRIMARY KEY(rest_no))" +
                    " CREATE TABLE RestaurantReviewer (id bigint, name string, email string, stats ReviewerStats, PRIMARY KEY(id))" +
                    " CREATE INDEX record_name_idx as select name from RestaurantComplexRecord" +
                    " CREATE INDEX reviewer_name_idx as select name from RestaurantReviewer" +
                    " CREATE INDEX mv1 AS SELECT R.rating from RestaurantComplexRecord AS Rec, (select rating from Rec.reviews) R" +
                    " CREATE INDEX mv2 AS SELECT endo.\"endorsementText\" FROM RestaurantComplexRecord rec, (SELECT X.\"endorsementText\" FROM rec.reviews rev, (SELECT \"endorsementText\" from rev.endorsements) X) endo";

    @RegisterExtension
    @Order(0)
    public final EmbeddedRelationalExtension relationalExtension = new EmbeddedRelationalExtension();

    public StandardQueryTests() {
        Utils.enableCascadesDebugger();
    }

    @Test
    void failsToQueryWithoutASchema() throws Exception {
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
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
    void testTypeConflictFields() throws Exception {
        String typeConflictFieldsTemplate = "CREATE TYPE AS STRUCT StudentA (name string, id bigint)" +
                " CREATE TYPE AS STRUCT StudentB (name string, id string)" +
                " CREATE TABLE CLASSA (student StudentA, name string, PRIMARY KEY(name))" +
                " CREATE TABLE CLASSB (student StudentB, name bigint, PRIMARY KEY(name))";

        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(typeConflictFieldsTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                insertTypeConflictRecords(statement);
                Assertions.assertTrue(statement.execute("SELECT * FROM CLASSA"), "Did not return a result set from a select statement!");
                try (final RelationalResultSet resultSet = statement.getResultSet()) {
                    ResultSetAssert.assertThat(resultSet).hasNextRow()
                            .hasColumn("NAME", "Sophia");
                }
            }
        }
    }

    @Test
    void simpleSelect() throws Exception {
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                var insertedRecord = insertRestaurantComplexRecord(statement);
                Assertions.assertTrue(statement.execute("SELECT * FROM RestaurantComplexRecord"), "Did not return a result set from a select statement!");
                try (final RelationalResultSet resultSet = statement.getResultSet()) {
                    ResultSetAssert.assertThat(resultSet).hasNextRow()
                            .isRowPartly(insertedRecord);
                    // explicitly test when nullable array is set to empty list, the RelationalArray object holds an empty iterable
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
    void simpleSelectWithNonNullableArrays() throws Exception {
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplateWithNonNullableArrays).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                var insertedRecord = insertRestaurantComplexRecord(statement);
                Assertions.assertTrue(statement.execute("SELECT * FROM RestaurantComplexRecord"), "Did not return a result set from a select statement!");
                try (final RelationalResultSet resultSet = statement.getResultSet()) {
                    ResultSetAssert.assertThat(resultSet).hasNextRow()
                            .isRowPartly(insertedRecord);
                    // explicitly test when a Non-nullable array is unset, the RelationalArray object holds an empty iterable
                    Assertions.assertEquals("[]", resultSet.getArray("REVIEWS").toString());
                    Assertions.assertEquals("[]", resultSet.getArray("TAGS").toString());
                    Assertions.assertEquals("[]", resultSet.getArray("CUSTOMER").toString());
                    Assertions.assertFalse(resultSet.next());
                }
            }
        }
    }

    @Test
    void canQueryPrimaryKeyZero() throws Exception {
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                var insertedRecord = insertRestaurantComplexRecord(statement, 0L, "");
                Assertions.assertTrue(statement.execute("SELECT * FROM RestaurantComplexRecord"), "Did not return a result set from a select statement!");
                try (final RelationalResultSet resultSet = statement.getResultSet()) {
                    ResultSetAssert.assertThat(resultSet).hasNextRow()
                            .isRowPartly(insertedRecord)
                            .hasNoNextRow();
                }
            }
        }
    }

    @Test
    void selectWithPredicateVariants() throws Exception {
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                insertRestaurantComplexRecord(statement);
                RelationalStruct r11 = insertRestaurantComplexRecord(statement, 11L);

                try (final RelationalResultSet resultSet = statement.executeQuery("SELECT * FROM RestaurantComplexRecord WHERE rest_no > 10")) {
                    ResultSetAssert.assertThat(resultSet).hasNextRow()
                            .isRowPartly(r11)
                            .hasNoNextRow();
                }

                try (final RelationalResultSet resultSet = statement.executeQuery("SELECT * FROM RestaurantComplexRecord WHERE rest_no >= 11")) {
                    ResultSetAssert.assertThat(resultSet).hasNextRow()
                            .isRowPartly(r11)
                            .hasNoNextRow();
                }

                try (final RelationalResultSet resultSet = statement.executeQuery("SELECT * FROM RestaurantComplexRecord WHERE 10 < rest_no")) {
                    ResultSetAssert.assertThat(resultSet).hasNextRow()
                            .isRowPartly(r11)
                            .hasNoNextRow();
                }

                try (final RelationalResultSet resultSet = statement.executeQuery("SELECT * FROM RestaurantComplexRecord WHERE 11 <= rest_no")) {
                    ResultSetAssert.assertThat(resultSet).hasNextRow()
                            .isRowPartly(r11)
                            .hasNoNextRow();
                }
            }
        }
    }

    @Test
    void explainTableScan() throws Exception {
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                try (final RelationalResultSet resultSet = statement.executeQuery("EXPLAIN SELECT * FROM RestaurantComplexRecord WHERE rest_no > 10")) {
                    resultSet.next();
                    String plan = resultSet.getString(1);
                    assertThat(plan).matches("(.*SCAN.*RESTAURANTCOMPLEXRECORD|.*COVERING.* <,>).*REST_NO GREATER_THAN promote\\(@c8 AS LONG\\).*");
                }
            }
        }
    }

    @Test
    void explainHintedIndexScan() throws Exception {
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                try (final RelationalResultSet resultSet = statement.executeQuery("EXPLAIN SELECT * FROM RestaurantComplexRecord USE INDEX (record_name_idx) WHERE rest_no > 10")) {
                    resultSet.next();
                    String plan = resultSet.getString(1);
                    assertThat(plan).matches(".*COVERING.*RECORD_NAME_IDX.*REST_NO GREATER_THAN promote\\(@c13 AS LONG\\).*FETCH.*");
                }
            }
        }
    }

    @Test
    void explainUnhintedIndexScan() throws Exception {
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                try (final RelationalResultSet resultSet = statement.executeQuery("EXPLAIN SELECT * FROM RestaurantComplexRecord AS R WHERE EXISTS (SELECT * FROM R.reviews AS RE WHERE RE.rating >= 9)")) {
                    resultSet.next();
                    String plan = resultSet.getString(1);
                    assertThat(plan).matches(".*ISCAN.*MV1.*\\[\\[GREATER_THAN_OR_EQUALS promote\\(@c24 AS LONG\\)\\]\\].*");
                }
            }
        }
    }

    @Test
    void selectWithPredicateCompositionVariants() throws Exception {
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                insertRestaurantComplexRecord(statement);
                final var l42 = insertRestaurantComplexRecord(statement, 42L, "rest1");
                final var l43 = insertRestaurantComplexRecord(statement, 43L, "rest1");
                final var l44 = insertRestaurantComplexRecord(statement, 44L, "rest1");
                final var l45 = insertRestaurantComplexRecord(statement, 45L, "rest2");
                try (RelationalResultSet resultSet = statement.executeQuery("SELECT * FROM RestaurantComplexRecord WHERE rest_no > 42 AND name = 'rest1'")) {
                    ResultSetAssert.assertThat(resultSet).describedAs("where rest_no > 42 AND name = 'rest1'")
                            .containsRowsPartly(l43, l44);
                }
                try (RelationalResultSet resultSet = statement.executeQuery("SELECT * FROM RestaurantComplexRecord WHERE name = 'rest2' OR name = 'rest1'")) {
                    ResultSetAssert.assertThat(resultSet).describedAs("where name = 'rest2' OR name = 'rest1'")
                            .containsRowsPartly(l42, l43, l44, l45);
                }
                try (RelationalResultSet resultSet = statement.executeQuery("SELECT * FROM RestaurantComplexRecord WHERE rest_no = (40+2)")) {
                    ResultSetAssert.assertThat(resultSet).describedAs("where rest_no = (40+2)")
                            .containsRowsPartly(l42);
                }
                try (RelationalResultSet resultSet = statement.executeQuery("SELECT * FROM RestaurantComplexRecord WHERE (40+2) = rest_no")) {
                    ResultSetAssert.assertThat(resultSet).describedAs("where (40+2) = rest_no")
                            .containsRowsPartly(l42);
                }
                try (RelationalResultSet resultSet = statement.executeQuery("SELECT * FROM RestaurantComplexRecord WHERE (44-2) = rest_no")) {
                    ResultSetAssert.assertThat(resultSet).describedAs("where (44-2) = rest_no")
                            .containsRowsPartly(l42);
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
    @Disabled("(yhatem) until https://github.com/FoundationDB/fdb-record-layer/issues/1945 is fixed")
    void selectWithNullInComparisonOperator() throws Exception {
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                var insertedRecord = insertRestaurantComplexRecord(statement);
                try (final RelationalResultSet resultSet = statement.executeQuery("SELECT * FROM RestaurantComplexRecord WHERE 1 is null")) {
                    ResultSetAssert.assertThat(resultSet).isEmpty();
                }
                try (final RelationalResultSet resultSet = statement.executeQuery("SELECT * FROM RestaurantComplexRecord WHERE 1 is not null")) {
                    ResultSetAssert.assertThat(resultSet).hasNextRow().isRowExactly(insertedRecord);
                }
                try (final RelationalResultSet resultSet = statement.executeQuery("SELECT * FROM RestaurantComplexRecord WHERE TRUE is null")) {
                    ResultSetAssert.assertThat(resultSet).isEmpty();
                }
                try (final RelationalResultSet resultSet = statement.executeQuery("SELECT * FROM RestaurantComplexRecord WHERE TRUE is not null")) {
                    ResultSetAssert.assertThat(resultSet).hasNextRow().isRowExactly(insertedRecord);
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
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
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
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
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
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                statement.setMaxRows(1);
                insertRestaurantComplexRecord(statement);
                RelationalStruct l42 = insertRestaurantComplexRecord(statement, 42L, "rest1");
                RelationalStruct l43 = insertRestaurantComplexRecord(statement, 43L, "rest1");
                RelationalStruct l44 = insertRestaurantComplexRecord(statement, 44L, "rest1");
                RelationalStruct l45 = insertRestaurantComplexRecord(statement, 45L, "rest2");
                final String initialQuery = "select * from RestaurantComplexRecord where rest_no > 40";
                Continuation continuation = ContinuationImpl.BEGIN;
                final List<RelationalStruct> expected = List.of(l42, l43, l44, l45);
                int i = 0;

                while (!continuation.atEnd()) {
                    String query = initialQuery;
                    if (!continuation.atBeginning()) {
                        query += " WITH CONTINUATION B64'" + Base64.getEncoder().encodeToString(continuation.serialize()) + "'";
                    }
                    try (final RelationalResultSet resultSet = statement.executeQuery(query)) {
                        // assert result matches expected
                        Assertions.assertNotNull(resultSet, "Did not return a result set!");
                        if (i < expected.size()) {
                            ResultSetAssert.assertThat(resultSet).hasNextRow()
                                    .isRowPartly(expected.get(i));
                        } else {
                            ResultSetAssert.assertThat(resultSet).hasNoNextRow();
                        }
                        // get continuation for the next query
                        continuation = resultSet.getContinuation();
                        i += 1;
                    }
                }
            }
        }
    }

    @Test
    void selectWithContinuationBeginEndShouldFail() throws Exception {
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                insertRestaurantComplexRecord(statement);
                insertRestaurantComplexRecord(statement, 42L, "rest1");
                final String begin = "select * from RestaurantComplexRecord where rest_no > 40 with continuation null";
                RelationalAssertions.assertThrowsSqlException(() -> statement.executeQuery(begin))
                        .hasErrorCode(ErrorCode.SYNTAX_ERROR);
                final String end = "select * from RestaurantComplexRecord where rest_no > 40 with continuation b64''";
                RelationalAssertions.assertThrowsSqlException(() -> statement.executeQuery(end))
                        .hasErrorCode(ErrorCode.INVALID_CONTINUATION);
            }
        }
    }

    @Test
    void testSelectWithIndexHint() throws Exception {
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                insertRestaurantComplexRecord(statement);
                // successfully execute a query with hinted index
                try (final RelationalResultSet resultSet = statement.executeQuery("SELECT name FROM RestaurantComplexRecord USE INDEX (record_name_idx)")) {
                    ResultSetAssert.assertThat(resultSet)
                            .meetsForAllRows(ResultSetAssert.perRowCondition(rs -> "testName".equals(rs.getString(1)), "Name should = testName"));
                }
                // successfully execute a query with multiple hinted indexes
                try (final RelationalResultSet resultSet = statement.executeQuery("SELECT name FROM RestaurantComplexRecord USE INDEX (record_name_idx, mv1, mv2)")) {
                    ResultSetAssert.assertThat(resultSet)
                            .meetsForAllRows(ResultSetAssert.perRowCondition(rs -> "testName".equals(rs.getString(1)), "name should equals 'testName'"));
                }
                // successfully execute a query with multiple hinted indexes, different syntax
                try (final RelationalResultSet resultSet = statement.executeQuery("SELECT name FROM RestaurantComplexRecord USE INDEX (record_name_idx), USE INDEX (mv1)")) {
                    ResultSetAssert.assertThat(resultSet)
                            .meetsForAllRows(ResultSetAssert.perRowCondition(rs -> "testName".equals(rs.getString(1)), "name should equals 'testName'"));
                }
                // exception is thrown when hinted indexes don't exist
                RelationalAssertions.assertThrowsSqlException(() -> statement.executeQuery("SELECT * FROM RestaurantComplexRecord USE INDEX (name) WHERE 11 <= rest_no"))
                        .hasErrorCode(ErrorCode.UNDEFINED_INDEX)
                        .hasMessage("Unknown index(es) NAME");
            }
        }
    }

    @Test
    void testSelectWithCoveringIndexHint() throws Exception {
        final String schema = "CREATE TABLE T1(COL1 bigint, COL2 bigint, COL3 bigint, PRIMARY KEY(COL1, COL3))" +
                " CREATE INDEX T1_IDX as select col1, col3, col2 from t1 order by col1, col3";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schema).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                final var row1 = EmbeddedRelationalStruct.newBuilder().addLong("COL1", 42L).addLong("COL2", 100L).addLong("COL3", 200L).build();
                int cnt = statement.executeInsert("T1", row1);
                Assertions.assertEquals(1, cnt, "Incorrect insertion count");

                final var row2 = EmbeddedRelationalStruct.newBuilder().addLong("COL1", 43L).addLong("COL2", 101L).addLong("COL3", 201L).build();
                cnt = statement.executeInsert("T1", row2);
                Assertions.assertEquals(1, cnt, "Incorrect insertion count");

                Assertions.assertTrue(statement.execute("SELECT * from T1 USE INDEX (T1_IDX)"), "Did not return a result set from a select statement!");
                try (final RelationalResultSet resultSet = statement.getResultSet()) {
                    final var expected = new ArrayList<Object[]>();
                    expected.add(new Object[]{42L, 100L, 200L});
                    expected.add(new Object[]{43L, 101L, 201L});
                    ResultSetAssert.assertThat(resultSet).containsRowsExactly(expected);
                }
            }
        }
    }

    @Test
    void projectIndividualColumns() throws Exception {
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
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
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
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
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
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
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
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
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
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
        final String schema = "CREATE TYPE AS STRUCT A ( b B )" +
                " CREATE TYPE AS STRUCT B ( c C )" +
                " CREATE TYPE AS STRUCT C ( d D )" +
                " CREATE TYPE AS STRUCT D ( e E )" +
                " CREATE TYPE AS STRUCT E ( f bigint )" +
                " CREATE TABLE tbl1 (id bigint, c C, a A, PRIMARY KEY(id))";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schema).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                final var result = EmbeddedRelationalStruct.newBuilder()
                        .addLong("ID", 42L)
                        .addStruct("C", EmbeddedRelationalStruct.newBuilder()
                                .addStruct("D", EmbeddedRelationalStruct.newBuilder()
                                        .addStruct("E", EmbeddedRelationalStruct.newBuilder()
                                                .addLong("F", 128L)
                                                .build())
                                        .build())
                                .build())
                        .addStruct("A", EmbeddedRelationalStruct.newBuilder()
                                .addStruct("B", EmbeddedRelationalStruct.newBuilder()
                                        .addStruct("C", EmbeddedRelationalStruct.newBuilder()
                                                .addStruct("D", EmbeddedRelationalStruct.newBuilder()
                                                        .addStruct("E", EmbeddedRelationalStruct.newBuilder()
                                                                .addLong("F", 128L)
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
                            .isRowExactly(42L, 128L, 128L)
                            .hasNoNextRow();
                }
                Assertions.assertTrue(statement.execute("SELECT c.d.e FROM tbl1"), "Did not return a result set from a select statement!");
                try (final RelationalResultSet resultSet = statement.getResultSet()) {
                    final var struct = EmbeddedRelationalStruct.newBuilder()
                            .addLong("F", 128L)
                            .build();
                    ResultSetAssert.assertThat(resultSet).hasNextRow()
                            .hasColumn("E", struct)
                            .hasNoNextRow();
                }
            }
        }
    }

    @Test
    void partiqlNestingWorksWithRepeatedLeaf() throws Exception {
        final String schema = "CREATE TYPE AS STRUCT A ( b B )" +
                " CREATE TYPE AS STRUCT B ( c C )" +
                " CREATE TYPE AS STRUCT C ( d D )" +
                " CREATE TYPE AS STRUCT D ( e E )" +
                " CREATE TYPE AS STRUCT E ( f bigint array )" +
                " CREATE TABLE tbl1 (id bigint, c C, a A, PRIMARY KEY(id))";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schema).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                final var result = EmbeddedRelationalStruct.newBuilder()
                        .addLong("ID", 42L)
                        .addStruct("C", EmbeddedRelationalStruct.newBuilder()
                                .addStruct("D", EmbeddedRelationalStruct.newBuilder()
                                        .addStruct("E", EmbeddedRelationalStruct.newBuilder()
                                                .addArray("F", EmbeddedRelationalArray.newBuilder().addLong(128L).build())
                                                .build())
                                        .build())
                                .build())
                        .addStruct("A", EmbeddedRelationalStruct.newBuilder()
                                .addStruct("B", EmbeddedRelationalStruct.newBuilder()
                                        .addStruct("C", EmbeddedRelationalStruct.newBuilder()
                                                .addStruct("D", EmbeddedRelationalStruct.newBuilder()
                                                        .addStruct("E", EmbeddedRelationalStruct.newBuilder()
                                                                .addArray("F", EmbeddedRelationalArray.newBuilder().addLong(128L).build())
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
                    Array expectedCol2 = EmbeddedRelationalArray.newBuilder().addLong(128L).build();
                    Array expectedCol3 = EmbeddedRelationalArray.newBuilder().addLong(128L).build();
                    ResultSetAssert.assertThat(resultSet).hasNextRow()
                            .isRowExactly(42L, expectedCol2, expectedCol3)
                            .hasNoNextRow();
                }
            }
        }
    }

    @Test
    void partiqlAccessingNestedFieldWithInnerRepeatedFieldsFails() throws Exception {
        final String schema = "CREATE TYPE AS STRUCT A ( b B )" +
                " CREATE TYPE AS STRUCT B ( c C )" +
                " CREATE TYPE AS STRUCT C ( d D )" +
                " CREATE TYPE AS STRUCT D ( e E array )" +
                " CREATE TYPE AS STRUCT E ( f bigint array )" +
                " CREATE TABLE tbl1 (id bigint, c C, a A, PRIMARY KEY(id))";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schema).build()) {
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
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
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
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                insertRestaurantComplexRecord(statement);
                RelationalStruct l42 = insertRestaurantComplexRecord(statement, 42L, "rest1");
                RelationalStruct l43 = insertRestaurantComplexRecord(statement, 43L, "rest1");
                RelationalStruct l44 = insertRestaurantComplexRecord(statement, 44L, "rest1");
                RelationalStruct l45 = insertRestaurantComplexRecord(statement, 45L, "rest2");
                try (final RelationalResultSet resultSet = statement.executeQuery("SELECT * FROM RestaurantComplexRecord WHERE rest_no > 40.5")) {
                    ResultSetAssert.assertThat(resultSet).containsRowsPartly(l42, l43, l44, l45);
                }
            }
        }
    }

    @Test
    void existsPredicateWorks() throws Exception {
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                insertRestaurantComplexRecord(statement);
                insertRestaurantComplexRecord(statement, 42L, "rest1", List.of(Triple.of(1L, 4L, List.of()), Triple.of(2L, 5L, List.of())));
                RelationalStruct l43 = insertRestaurantComplexRecord(statement, 43L, "rest2", List.of(Triple.of(3L, 9L, List.of()), Triple.of(4L, 8L, List.of())));
                RelationalStruct l44 = insertRestaurantComplexRecord(statement, 44L, "rest3", List.of(Triple.of(3L, 10L, List.of())));
                try (final RelationalResultSet resultSet = statement.executeQuery("SELECT * FROM RestaurantComplexRecord AS R WHERE EXISTS (SELECT * FROM R.reviews AS RE WHERE RE.rating >= 9)")) {
                    ResultSetAssert.assertThat(resultSet).containsRowsPartly(l43, l44);
                }
            }
        }
    }

    @Test
    void existsPredicateWorksWithNonNullableArray() throws Exception {
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplateWithNonNullableArrays).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                insertRestaurantComplexRecord(statement, 42L, "rest1", List.of(Triple.of(1L, 4L, List.of()), Triple.of(2L, 5L, List.of())));
                RelationalStruct l43 = insertRestaurantComplexRecord(statement, 43L, "rest2", List.of(Triple.of(3L, 9L, List.of()), Triple.of(4L, 8L, List.of())));
                RelationalStruct l44 = insertRestaurantComplexRecord(statement, 44L, "rest3", List.of(Triple.of(3L, 10L, List.of())));
                try (final RelationalResultSet resultSet = statement.executeQuery("SELECT * FROM RestaurantComplexRecord AS R WHERE EXISTS (SELECT * FROM R.reviews AS RE WHERE RE.rating >= 9)")) {
                    ResultSetAssert.assertThat(resultSet).containsRowsPartly(l43, l44);
                }
            }
        }
    }

    @Test
    void existsPredicateNestedWorks() throws Exception {
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                insertRestaurantComplexRecord(statement);
                RelationalStruct l42 = insertRestaurantComplexRecord(statement, 42L, "rest1",
                        List.of(Triple.of(1L, 4L, List.of(
                                        Pair.of(400L, "good"),
                                        Pair.of(401L, "meh"))),
                                Triple.of(2L, 5L, List.of(
                                        Pair.of(402L, "awesome"),
                                        Pair.of(401L, "wow")))));
                try (final RelationalResultSet resultSet = statement.executeQuery("SELECT * FROM RestaurantComplexRecord AS R WHERE EXISTS (SELECT * FROM R.reviews AS RE WHERE EXISTS(SELECT * FROM RE.endorsements AS REE WHERE REE.\"endorsementText\"='wow'))")) {
                    ResultSetAssert.assertThat(resultSet).containsRowsPartly(l42);
                }
            }
        }
    }

    @Test
    void testSubquery() throws Exception {
        final String schema = "CREATE TYPE AS STRUCT contact_detail(name string, phone_number string, address string) " +
                "CREATE TYPE AS STRUCT messages(\"TEXT\" string, timestamp bigint,sent boolean) " +
                "CREATE TABLE conversations(id bigint, other_party CONTACT_DETAIL, messages MESSAGES ARRAY,primary key(id))";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schema).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                final var row1 = EmbeddedRelationalStruct.newBuilder()
                        .addLong("ID", 0L)
                        .addStruct("OTHER_PARTY", EmbeddedRelationalStruct.newBuilder()
                                .addString("NAME", "Arnaud")
                                .addString("PHONE_NUMBER", "12345")
                                .addString("ADDRESS", "6 Part Road")
                                .build())
                        .addArray("MESSAGES", EmbeddedRelationalArray.newBuilder()
                                .addStruct(EmbeddedRelationalStruct.newBuilder()
                                        .addString("TEXT", "Hello there!")
                                        .addLong("TIMESTAMP", 10000)
                                        .addBoolean("SENT", true)
                                        .build())
                                .addStruct(EmbeddedRelationalStruct.newBuilder()
                                        .addString("TEXT", "Hi Scott!")
                                        .addLong("TIMESTAMP", 20000)
                                        .addBoolean("SENT", false)
                                        .build())
                                .build())
                        .build();
                int cnt = statement.executeInsert("CONVERSATIONS", row1);
                Assertions.assertEquals(1, cnt, "Incorrect insertion count");

                final var row2 = EmbeddedRelationalStruct.newBuilder()
                        .addLong("ID", 1L)
                        .addStruct("OTHER_PARTY", EmbeddedRelationalStruct.newBuilder()
                                .addString("NAME", "Bri")
                                .addString("PHONE_NUMBER", "9876543")
                                .addString("ADDRESS", "10 Chancery Lane")
                                .build())
                        .addArray("MESSAGES", EmbeddedRelationalArray.newBuilder()
                                .addStruct(EmbeddedRelationalStruct.newBuilder()
                                        .addString("TEXT", "Hello there")
                                        .addLong("TIMESTAMP", 30000)
                                        .addBoolean("SENT", true)
                                        .build())
                                .addStruct(EmbeddedRelationalStruct.newBuilder()
                                        .addString("TEXT", "What a nice weather today!")
                                        .addLong("TIMESTAMP", 40000)
                                        .addBoolean("SENT", true)
                                        .build())
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

    @Test
    void aliasingColumnsWorks() throws Exception {
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                insertRestaurantComplexRecord(statement);
                try (final RelationalResultSet resultSet = statement.executeQuery("SELECT Y.M FROM (SELECT X.N AS M FROM (SELECT name AS N FROM RestaurantComplexRecord WHERE 11 <= rest_no) X) Y")) {
                    ResultSetAssert.assertThat(resultSet)
                            .meetsForAllRows(ResultSetAssert.perRowCondition(rs -> "testName".equals(rs.getString(1)), "name should equals 'testName'"));
                }
            }
        }
    }

    @Test
    void aliasingTableToResolveAmbiguityWorks() throws Exception {
        final String schema = "CREATE TABLE FOO(FOO bigint, PRIMARY KEY(FOO))";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schema).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                final var row1 = EmbeddedRelationalStruct.newBuilder().addLong("FOO", 42L).build();
                int cnt = statement.executeInsert("FOO", row1);
                Assertions.assertEquals(1, cnt, "Incorrect insertion count");

                final var row2 = EmbeddedRelationalStruct.newBuilder().addLong("FOO", 43L).build();
                cnt = statement.executeInsert("FOO", row2);
                Assertions.assertEquals(1, cnt, "Incorrect insertion count");
                Assertions.assertTrue(statement.execute("SELECT * from FOO f WHERE f.FOO > 42"), "Did not return a result set from a select statement!");
                try (final RelationalResultSet resultSet = statement.getResultSet()) {
                    ResultSetAssert.assertThat(resultSet)
                            .hasNextRow()
                            .hasColumn("FOO", 43L)
                            .hasNoNextRow();
                }
                Assertions.assertTrue(statement.execute("SELECT * from FOO f WHERE FOO > 42"), "Did not return a result set from a select statement!");
                try (final RelationalResultSet resultSet = statement.getResultSet()) {
                    ResultSetAssert.assertThat(resultSet)
                            .hasNextRow()
                            .hasColumn("FOO", 43L)
                            .hasNoNextRow();
                }
            }
        }
    }

    @Test
    void testBitmap() throws Exception {
        final String query = "SELECT BITMAP_CONSTRUCT_AGG(BITMAP_BIT_POSITION(uid)) as bitmap, category, BITMAP_BUCKET_OFFSET(uid) as offset FROM T1\n" +
                "GROUP BY category, BITMAP_BUCKET_OFFSET(uid)\n";
        final String schemaTemplate = "CREATE TABLE T1(uid bigint, category string, PRIMARY KEY(uid))\n" +
                "create index bitmapIndex as\n" +
                query;

        testBitmapResult(schemaTemplate, query);
    }

    @Test
    void testBitmapWrongGroupByOrder() {
        final String query = "SELECT bitmap_construct_agg(bitmap_bit_position(uid)) as bitmap, bitmap_bucket_offset(uid) as offset, category FROM T1\n" +
                "GROUP BY bitmap_bucket_offset(uid), category\n";
        final String schemaTemplate = "CREATE TABLE T1(uid bigint, category string, PRIMARY KEY(uid))\n" +
                "create index bitmapIndex as\n" +
                query;
        org.junit.Assert.assertThrows(ContextualSQLException.class, () -> testBitmapResult(schemaTemplate, query));
    }

    @Test
    void testBitmapWithEmptyGroup() throws Exception {
        final String schemaTemplate = "CREATE TABLE T1(uid bigint, category string, PRIMARY KEY(uid))\n" +
                "create index bitmapIndex as\n" +
                "select bitmap_construct_agg(bitmap_bit_position(uid)), bitmap_bucket_offset(uid)\n" +
                "from T1\n" +
                "group by bitmap_bucket_offset(uid)";
        testBitmapResultWithEmptyGroup(schemaTemplate);
    }

    @Test
    void testBitmapNoBitmapIndex() throws Exception {
        final String query = "SELECT bitmap_construct_agg(bitmap_bit_position(uid)) as bitmap, category, bitmap_bucket_offset(uid) as offset FROM T1\n" +
                "GROUP BY category, bitmap_bucket_offset(uid)\n";
        final String schemaTemplate = "CREATE TABLE T1(uid bigint, category string, PRIMARY KEY(uid))\n" +
                "create index category_index as\n" +
                "select category, bitmap_bucket_offset(uid) from T1 order by category, bitmap_bucket_offset(uid)";
        testBitmapResult(schemaTemplate, query);
    }

    private void testBitmapResult(String schemaTemplate, String query) throws Exception {
        int expectedByteArrayLength = 1250;
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                statement.executeUpdate("insert into t1 values (42, 'world')");
                statement.executeUpdate("insert into t1 values (2, 'world')");
                statement.executeUpdate("insert into t1 values (19999, 'world')");
                statement.executeUpdate("insert into t1 values (30, 'hello')");
                statement.executeUpdate("insert into t1 values (1, 'hello')");
                statement.executeUpdate("insert into t1 values (20030, 'hello')");

                Assertions.assertTrue(statement.execute(query), "Did not return a result set from a select statement!");
                try (final RelationalResultSet resultSet = statement.getResultSet()) {
                    Assert.that(resultSet.next());
                    byte[] bytes1 = resultSet.getBytes("BITMAP");
                    Assertions.assertEquals(List.of(1L, 30L), collectOnBits(bytes1, expectedByteArrayLength));
                    Assertions.assertEquals("hello", resultSet.getString("CATEGORY"));
                    Assertions.assertEquals(0, resultSet.getLong("OFFSET"));

                    Assert.that(resultSet.next());
                    byte[] bytes2 = resultSet.getBytes("BITMAP");
                    // 20030 -> [0, 0, 0, 01000000, ...]
                    Assertions.assertEquals(List.of(30L), collectOnBits(bytes2, expectedByteArrayLength));
                    Assertions.assertEquals("hello", resultSet.getString("CATEGORY"));
                    Assertions.assertEquals(20000, resultSet.getLong("OFFSET"));

                    Assert.that(resultSet.next());
                    byte[] bytes3 = resultSet.getBytes("BITMAP");
                    // 2, 42 -> [00000100, 0, 0, 0, 0, 00000100, 0...]
                    Assertions.assertEquals(List.of(2L, 42L), collectOnBits(bytes3, expectedByteArrayLength));
                    Assertions.assertEquals("world", resultSet.getString("CATEGORY"));
                    Assertions.assertEquals(0, resultSet.getLong("OFFSET"));

                    Assert.that(resultSet.next());
                    byte[] bytes4 = resultSet.getBytes("BITMAP");
                    Assertions.assertEquals(List.of(9999L), collectOnBits(bytes4, expectedByteArrayLength));
                    Assertions.assertEquals("world", resultSet.getString("CATEGORY"));
                    Assertions.assertEquals(10000, resultSet.getLong("OFFSET"));

                    Assert.that(!resultSet.next());
                }
            }
        }
    }

    @Nullable
    private static List<Long> collectOnBits(@Nullable byte[] bitmap, int expectedArrayLength) {
        if (bitmap == null) {
            return null;
        }
        Assertions.assertEquals(expectedArrayLength, bitmap.length);
        final List<Long> result = new ArrayList<>();
        for (int i = 0; i < bitmap.length; i++) {
            if (bitmap[i] != 0) {
                for (int j = 0; j < 8; j++) {
                    if ((bitmap[i] & (1 << j)) != 0) {
                        result.add(i * 8L + j);
                    }
                }
            }
        }
        return result;
    }

    private void testBitmapResultWithEmptyGroup(String schemaTemplate) throws Exception {
        int expectedByteArrayLength = 1250;
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                statement.executeUpdate("insert into t1 values (42, 'world')");
                statement.executeUpdate("insert into t1 values (2, 'world')");
                statement.executeUpdate("insert into t1 values (19999, 'world')");
                statement.executeUpdate("insert into t1 values (30, 'hello')");
                statement.executeUpdate("insert into t1 values (1, 'hello')");
                statement.executeUpdate("insert into t1 values (20030, 'hello')");

                String query = "SELECT bitmap_construct_agg(bitmap_bit_position(uid)), bitmap_bucket_offset(uid) FROM T1\n" +
                        "GROUP BY bitmap_bucket_offset(uid)\n";
                Assertions.assertTrue(statement.execute(query), "Did not return a result set from a select statement!");
                try (final RelationalResultSet resultSet = statement.getResultSet()) {
                    Assert.that(resultSet.next());
                    byte[] bytes1 = resultSet.getBytes(1);
                    Assertions.assertEquals(List.of(1L, 2L, 30L, 42L), collectOnBits(bytes1, expectedByteArrayLength));
                    Assertions.assertEquals(0, resultSet.getLong(2));

                    Assert.that(resultSet.next());
                    byte[] bytes2 = resultSet.getBytes(1);
                    Assertions.assertEquals(List.of(9999L), collectOnBits(bytes2, expectedByteArrayLength));
                    Assertions.assertEquals(10000L, resultSet.getLong(2));

                    Assert.that(resultSet.next());
                    byte[] bytes3 = resultSet.getBytes(1);
                    Assertions.assertEquals(List.of(30L), collectOnBits(bytes3, expectedByteArrayLength));
                    Assertions.assertEquals(20000L, resultSet.getLong(2));

                    Assert.that(!resultSet.next());
                }
            }
        }
    }

    @Test
    void queryJavaCallFunctionLocallyCreatedUdf() throws Exception {
        final String schemaTemplate = "CREATE TABLE T1(pk bigint, a string, PRIMARY KEY(pk))";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                statement.executeUpdate("insert into t1 values (42, 'world')");
                Assertions.assertTrue(statement.execute("SELECT java_call('com.apple.foundationdb.relational.recordlayer.query.udf.SumUdf', pk, 42) + 100 FROM T1"), "Did not return a result set from a select statement!");
                try (final RelationalResultSet resultSet = statement.getResultSet()) {
                    ResultSetAssert.assertThat(resultSet).hasNextRow()
                            .isRowExactly(100 + 42 + 42L)
                            .hasNoNextRow();
                }
            }
        }
    }

    @Test
    void queryJavaCallSimulatecustomerFunction() throws Exception {
        final var expected = EmbeddedRelationalArray.newBuilder().addBytes(new byte[]{0xA, 0xB}).build();
        final String schemaTemplate = "CREATE TABLE T1(pk bigint, a bytes, b bytes array, PRIMARY KEY(pk))";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                statement.executeUpdate("insert into t1 values (42, X'0A', [ X'0B' ])");
                Assertions.assertTrue(statement.execute("SELECT java_call('com.apple.foundationdb.relational.recordlayer.query.udf.ByteOperationsUdf', a, b) FROM T1"), "Did not return a result set from a select statement!");
                try (final RelationalResultSet resultSet = statement.getResultSet()) {
                    ResultSetAssert.assertThat(resultSet).hasNextRow()
                            .isRowExactly(expected)
                            .hasNoNextRow();
                }
            }
        }
    }

    @Test
    void selectStarStatement() throws Exception {
        final String schemaTemplate = "CREATE TABLE T1(pk bigint, a bigint, b bigint, PRIMARY KEY(pk))";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                statement.executeUpdate("insert into t1 values (42, 100, 101)");
                Assertions.assertTrue(statement.execute("select * from t1"));
                try (final RelationalResultSet resultSet = statement.getResultSet()) {
                    ResultSetAssert.assertThat(resultSet).hasNextRow()
                            .isRowExactly(42L, 100L, 101L)
                            .hasNoNextRow();
                }
            }
        }
    }

    @Test
    void selectWithEmptyListAsPredicate() throws Exception {
        final String schemaTemplate = "CREATE TABLE T1(pk bigint, a string, PRIMARY KEY(pk))";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                statement.executeUpdate("insert into t1 values (42, 'bla')");
            }
            try (var statement = ddl.setSchemaAndGetConnection().prepareStatement("select * from t1 where a in ?")) {
                statement.setArray(1, ddl.getConnection().createArrayOf("STRING", new Object[]{}));
                statement.execute();
                statement.execute();
            }
        }
    }

    @Test
    void deleteLimit() throws Exception {
        final String schemaTemplate = "CREATE TABLE simple (rest_no bigint, name string, primary key(rest_no))";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                statement.executeUpdate("insert into simple values (1,'testRecord1'), (2, 'testRecord2')");
                Assertions.assertTrue(statement.execute("select * from simple"));
                try (final RelationalResultSet resultSet = statement.getResultSet()) {
                    Assert.that(resultSet.next());
                    Assertions.assertEquals(1L, resultSet.getLong(1));
                    Assertions.assertEquals("testRecord1", resultSet.getString(2));

                    Assert.that(resultSet.next());
                    Assertions.assertEquals(2L, resultSet.getLong(1));
                    Assertions.assertEquals("testRecord2", resultSet.getString(2));

                    Assert.that(!resultSet.next());
                }
                final var message = Assertions.assertThrows(SQLException.class, () -> statement.execute("delete from simple limit 1 returning rest_no, name")).getMessage();
                Assertions.assertEquals("LIMIT clause is not supported.", message);
            }
        }
    }

    @Test
    void selectNestedStarWorks() throws Exception {
        final String schemaTemplate = "CREATE TABLE T1(pk bigint, a bigint, b bigint, PRIMARY KEY(pk))";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                statement.executeUpdate("insert into t1 values (42, 100, 101)");
                Assertions.assertTrue(statement.execute("select (*) from t1"));
                try (final RelationalResultSet resultSet = statement.getResultSet()) {
                    Assertions.assertTrue(resultSet.next());
                    final var struct = resultSet.getStruct(1);
                    Assertions.assertEquals(42, struct.getInt(1));
                    Assertions.assertEquals(100, struct.getInt(2));
                    Assertions.assertEquals(101, struct.getInt(3));
                    Assertions.assertFalse(resultSet.next());
                }
                Assertions.assertTrue(statement.execute("select ((*)) from t1"));
                try (final RelationalResultSet resultSet = statement.getResultSet()) {
                    Assertions.assertTrue(resultSet.next());
                    final var struct = resultSet.getStruct(1);
                    final var nestedStruct = struct.getStruct(1);
                    Assertions.assertEquals(42, nestedStruct.getInt(1));
                    Assertions.assertEquals(100, nestedStruct.getInt(2));
                    Assertions.assertEquals(101, nestedStruct.getInt(3));
                    Assertions.assertFalse(resultSet.next());
                }
            }
        }
    }

    @Test
    void testNamingStruct() throws Exception {
        final String schemaTemplate = "CREATE TABLE T1(pk bigint, a bigint, b bigint, c bigint, PRIMARY KEY(pk))";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                statement.executeUpdate("insert into t1 values (42, 100, 500, 101)");
                Assertions.assertTrue(statement.execute("select struct asd (a, 42, struct def (b, c)) as X from t1"));
                try (final RelationalResultSet resultSet = statement.getResultSet()) {
                    Assertions.assertTrue(resultSet.next());
                    Assertions.assertEquals("ASD", resultSet.getStruct(1).getMetaData().getTypeName());
                    final var thirdCol = resultSet.getStruct(1).getStruct(3);
                    Assertions.assertEquals("DEF", thirdCol.getMetaData().getTypeName());
                    Assertions.assertFalse(resultSet.next());
                }
            }
        }
    }

    @Test
    void testNamingStructsSameType() throws Exception {
        final String schemaTemplate = "CREATE TABLE T1(pk bigint, a bigint, b bigint, c bigint, PRIMARY KEY(pk))";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                statement.executeUpdate("insert into t1 values (42, 100, 500, 101)");
                Assertions.assertTrue(statement.execute("select struct asd (a, 42, struct def (b, c), struct def(b, c)) as X from t1"));
                try (final RelationalResultSet resultSet = statement.getResultSet()) {
                    Assertions.assertTrue(resultSet.next());
                    Assertions.assertEquals("ASD", resultSet.getStruct(1).getMetaData().getTypeName());
                    Assertions.assertEquals("X", resultSet.getMetaData().getColumnLabel(1));
                    final var thirdCol = resultSet.getStruct(1).getStruct(3);
                    Assertions.assertEquals("DEF", thirdCol.getMetaData().getTypeName());
                    final var fourthCol = resultSet.getStruct(1).getStruct(4);
                    Assertions.assertEquals("DEF", fourthCol.getMetaData().getTypeName());
                    Assertions.assertFalse(resultSet.next());
                }
            }
        }
    }

    @Test
    void testNamingStructsDifferentTypesThrows() throws Exception {
        final String schemaTemplate = "CREATE TABLE T1(pk bigint, a bigint, b bigint, c bigint, PRIMARY KEY(pk))";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                statement.executeUpdate("insert into t1 values (42, 100, 500, 101)");
                final var message = Assertions.assertThrows(SQLException.class, () -> statement.execute("select struct asd (a, 42, struct def (b, c), struct def(b, c, a)) as X from t1")).getMessage();
                Assertions.assertTrue(message.contains("value already present: DEF")); // we could improve this error message.
            }
        }
    }

    @Test
    void testNamingStructsSameTypeDifferentNestingLevels() throws Exception {
        final String schemaTemplate = "CREATE TABLE T1(pk bigint, a bigint, b bigint, c bigint, PRIMARY KEY(pk))";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                statement.executeUpdate("insert into t1 values (42, 100, 500, 101)");
                Assertions.assertTrue(statement.execute("select a, 42, struct def (b, c), (a, b, c, struct def(b, c)) as X from t1"));
                try (final RelationalResultSet resultSet = statement.getResultSet()) {
                    Assertions.assertTrue(resultSet.next());
                    final var col3 = resultSet.getStruct(3);
                    Assertions.assertEquals("DEF", col3.getMetaData().getTypeName());
                    final var col44 = resultSet.getStruct(4).getStruct(4);
                    Assertions.assertEquals("X", resultSet.getMetaData().getColumnLabel(4));
                    Assertions.assertEquals("DEF", col44.getMetaData().getTypeName());
                    Assertions.assertFalse(resultSet.next());
                }
            }
        }
    }

    @Test
    void testNamingStructWithNameOfTableIsPermitted() throws Exception {
        final String schemaTemplate = "CREATE TABLE T1(pk bigint, a bigint, b bigint, c bigint, PRIMARY KEY(pk))";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                statement.executeUpdate("insert into t1 values (42, 100, 500, 101)");
                Assertions.assertTrue(statement.execute("select a, 42, struct T1 (b, c) as X from t1"));
                try (final RelationalResultSet resultSet = statement.getResultSet()) {
                    Assertions.assertTrue(resultSet.next());
                    final var col3 = resultSet.getStruct(3);
                    Assertions.assertEquals("T1", col3.getMetaData().getTypeName());
                    Assertions.assertEquals("X", resultSet.getMetaData().getColumnLabel(3));
                    Assertions.assertFalse(resultSet.next());
                }
            }
        }
    }

    @Test
    void tupleInListAsPredicate() throws Exception {
        final String schemaTemplate = "CREATE TABLE T1(pk bigint, a string, PRIMARY KEY(pk))" +
                " CREATE INDEX a_index as select pk, a from T1 order by pk, a";

        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                statement.executeUpdate("insert into t1 values (42, 'bla')");
                statement.executeUpdate("insert into t1 values (40, 'foo')");
            }

            try (var statement = ddl.setSchemaAndGetConnection().prepareStatement("select * from t1 where (pk, a) in ((?l1, 'bla'), (?l2, 'bar'))")) {
                statement.setLong("l1", 42L);
                statement.setLong("l2", 40L);
                statement.execute();
                try (final RelationalResultSet resultSet = statement.getResultSet()) {
                    Assert.that(resultSet.next());
                    Assertions.assertEquals(42L, resultSet.getLong(1));
                    Assertions.assertEquals("bla", resultSet.getString(2));
                    Assert.that(!resultSet.next());
                }
            }
            try (var statement = ddl.setSchemaAndGetConnection().prepareStatement("select * from t1 where (pk, a) in ((?l1, 'foo'), (?l2, 'bar'))")) {
                statement.setLong("l1", 42L);
                statement.setLong("l2", 40L);
                statement.execute();
                try (final RelationalResultSet resultSet = statement.getResultSet()) {
                    Assert.that(!resultSet.next());
                }
            }
            try (var statement = ddl.setSchemaAndGetConnection().prepareStatement("select * from t1 where (pk, a) in ((?l1, 'foo', 'foo2'), (?l2, 'bar', 'bar2'))")) {
                statement.setLong("l1", 42L);
                statement.setLong("l2", 40L);
                Assertions.assertThrows(SQLException.class, statement::execute);
            }
            try (var statement = ddl.setSchemaAndGetConnection().prepareStatement("select * from t1 where (pk, a) in (?l1, 'foo', 'foo2')")) {
                statement.setLong("l1", 42L);
                Assertions.assertThrows(SQLException.class, statement::execute);
            }
        }
    }

    @Test
    void tupleInListAsPredicate2() throws Exception {
        final String schemaTemplate = "CREATE TABLE T1(pk bigint, a string, b bigint, PRIMARY KEY(pk))" +
                " CREATE INDEX pk_a as select pk, a from T1 order by pk, a" +
                " CREATE INDEX b_a as select b, a from T1 order by b, a";

        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                statement.executeUpdate("insert into t1 values (42, 'bla', 1)");
                statement.executeUpdate("insert into t1 values (40, 'foo', 2)");
            }
            try (var statement = ddl.setSchemaAndGetConnection().prepareStatement("select * from t1 where (b, a) in ((?l1, 'bla'), (?l2, 'bar'), (?l3, 'bar')) and pk = ?pk")) {
                statement.setLong("pk", 42L);
                statement.setLong("l1", 1L);
                statement.setLong("l2", 2L);
                statement.setLong("l3", 3L);

                statement.execute();
                try (final RelationalResultSet resultSet = statement.getResultSet()) {
                    Assert.that(resultSet.next());
                    Assertions.assertEquals(42L, resultSet.getLong(1));
                    Assertions.assertEquals("bla", resultSet.getString(2));
                    Assertions.assertEquals(1L, resultSet.getLong(3));
                    Assert.that(!resultSet.next());
                }
            }

            try (var statement = ddl.setSchemaAndGetConnection().prepareStatement("select * from t1 where (b, a) in ((?l1, 'foo'), (?l2, 'bar'))")) {
                statement.setLong("l1", 42L);
                statement.setLong("l2", 40L);
                statement.execute();
                try (final RelationalResultSet resultSet = statement.getResultSet()) {
                    Assert.that(!resultSet.next());
                }
            }
        }
    }

    @Test
    void tupleThreeInListAsPredicate() throws Exception {
        final String schemaTemplate = "CREATE TABLE T1(pk bigint, a string, b bigint, PRIMARY KEY(pk))" +
                " CREATE INDEX pk_a_b as select pk, a, b from T1 order by pk, a, b";

        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                statement.executeUpdate("insert into t1 values (42, 'bla', 1)");
                statement.executeUpdate("insert into t1 values (40, 'foo', 2)");
            }
            try (var statement = ddl.setSchemaAndGetConnection().prepareStatement("select * from t1 where (pk, a, b) in ((?pk1, 'bla', ?b1), (?pk2, 'bar', ?b2))")) {
                statement.setLong("pk1", 42L);
                statement.setLong("pk2", 40L);
                statement.setLong("b1", 1L);
                statement.setLong("b2", 2L);
                statement.execute();
                try (final RelationalResultSet resultSet = statement.getResultSet()) {
                    Assert.that(resultSet.next());
                    Assertions.assertEquals(42L, resultSet.getLong(1));
                    Assertions.assertEquals("bla", resultSet.getString(2));
                    Assertions.assertEquals(1L, resultSet.getLong(3));
                    Assert.that(!resultSet.next());
                }
            }
            try (var statement = ddl.setSchemaAndGetConnection().prepareStatement("select * from t1 where (pk, a, b) in ((?pk1, 'bar', ?b1), (?pk2, 'bla', ?b2))")) {
                statement.setLong("pk1", 42L);
                statement.setLong("pk2", 40L);
                statement.setLong("b1", 1L);
                statement.setLong("b2", 2L);
                statement.execute();

                try (final RelationalResultSet resultSet = statement.getResultSet()) {
                    Assert.that(!resultSet.next());
                }
            }
        }
    }

    @Test
    void unionIsNotSupported() throws Exception {
        final String schemaTemplate = "CREATE TABLE T1(pk bigint, a string, b bigint, PRIMARY KEY(pk))";

        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                RelationalAssertions.assertThrows(() ->
                        ((EmbeddedRelationalStatement) statement)
                                .executeInternal("select * from t1 union select * from t1"))
                        .hasErrorCode(ErrorCode.UNSUPPORTED_QUERY);
            }
        }
    }

    @Test
    void structArrayContains() throws Exception {
        final String schemaTemplate = "CREATE TYPE AS STRUCT A(col2 string, col3 bigint, col4 bigint) " +
                "CREATE TABLE T1(col1 bigint, a A Array, col5 bigint, primary key(col1))";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                statement.executeUpdate("insert into t1 values (42, [('Apple', 1, 100), ('Orange', 2, 200)], 142), (44, [('Grape', 3, 300), ('Pear', 4, 400)], 144)");
                Assertions.assertTrue(statement.execute("SELECT T1.col5 FROM T1 where exists (SELECT 1 FROM T1.A r where r.col2 = 'Grape') "));
                try (final RelationalResultSet resultSet = statement.getResultSet()) {
                    ResultSetAssert.assertThat(resultSet).hasNextRow()
                            .isRowExactly(144L)
                            .hasNoNextRow();
                }
                // another way of query
                Assertions.assertTrue(statement.execute("SELECT T1.col5 FROM T1, (SELECT col2, col3 FROM T1.A) X where X.col2 = 'Grape'"));
                try (final RelationalResultSet resultSet = statement.getResultSet()) {
                    ResultSetAssert.assertThat(resultSet).hasNextRow()
                            .isRowExactly(144L)
                            .hasNoNextRow();
                }
                Assertions.assertTrue(statement.execute("SELECT T1.col5 FROM T1 where exists (SELECT 1 FROM T1.A r where r.col2 in ('Grape', 'Orange'))"));
                try (final RelationalResultSet resultSet = statement.getResultSet()) {
                    ResultSetAssert.assertThat(resultSet).hasNextRow()
                            .isRowExactly(142L)
                            .hasNextRow()
                            .isRowExactly(144L)
                            .hasNoNextRow();
                }
            }
        }
    }

    @Test
    void primitiveArrayContains() throws Exception {
        final String schemaTemplate = "CREATE TABLE T1(col1 bigint, a string Array, primary key(col1))";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                statement.executeUpdate("insert into t1 values (42, ['Apple', 'Orange']), (44, ['Grape', 'Pear'])");
                /*
                Assertions.assertTrue(statement.execute("SELECT * FROM T1 where exists (SELECT 1 FROM T1.A r where r = 'Grape')"));
                try (final RelationalResultSet resultSet = statement.getResultSet()) {
                    ResultSetAssert.assertThat(resultSet).hasNextRow()
                            .isRowExactly(44L, EmbeddedRelationalArray.newBuilder().addString("Grape").addString("Pear").build())
                            .hasNoNextRow();
                }
                Assertions.assertTrue(statement.execute("SELECT * FROM T1 where exists (SELECT 1 FROM T1.A r where r in ('Grape', 'Orange'))"));
                try (final RelationalResultSet resultSet = statement.getResultSet()) {
                    ResultSetAssert.assertThat(resultSet).hasNextRow()
                            .isRowExactly(42L, EmbeddedRelationalArray.newBuilder().addString("Apple").addString("Orange").build())
                            .hasNextRow()
                            .isRowExactly(44L, EmbeddedRelationalArray.newBuilder().addString("Grape").addString("Pear").build())
                            .hasNoNextRow();
                }

                 */

                Assertions.assertTrue(statement.execute("SELECT * FROM T1 where 'Grape' in (a)"));
                try (final RelationalResultSet resultSet = statement.getResultSet()) {
                    ResultSetAssert.assertThat(resultSet).hasNextRow()
                            .isRowExactly(44L, EmbeddedRelationalArray.newBuilder().addString("Grape").addString("Pear").build())
                            .hasNoNextRow();
                }
            }
        }
    }

    @Test
    void cteWorksCorrectly() throws Exception {
        final String schemaTemplate = "CREATE TABLE T1(pk bigint, a bigint, b bigint, c bigint, PRIMARY KEY(pk))";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                statement.executeUpdate("insert into t1 values (42, 100, 500, 101), (44, 101, 501, 102)");
                //Assertions.assertTrue(statement.execute("with C1 (X, Y, Z) as (SELECT a, b, c from T1) select Y, Z from C1"));
                Assertions.assertTrue(statement.execute("with C1 as (SELECT a, b, c from T1) select b, c from C1"));
                //Assertions.assertTrue(statement.execute("select b, c from (select a, b, c from t1) as x "));
                try (final RelationalResultSet resultSet = statement.getResultSet()) {
                    ResultSetAssert.assertThat(resultSet).hasNextRow()
                            .isRowExactly(500L, 101L)
                            .hasNextRow()
                            .isRowExactly(501L, 102L)
                            .hasNoNextRow();
                }
            }
        }
    }

    @Test
    void cteWorksCorrectly2() throws Exception {
        final String schemaTemplate = "CREATE TABLE T1(pk bigint, a bigint, b bigint, c bigint, PRIMARY KEY(pk))";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                statement.executeUpdate("insert into t1 values (42, 100, 500, 101), (44, 101, 501, 102)");
                //Assertions.assertTrue(statement.execute("with C1 (X, Y, Z) as (SELECT a, b, c from T1) select Y, Z from C1"));
                Assertions.assertTrue(statement.execute("with C1 as (SELECT a, b, c from T1) select b, c from C1"));
                //Assertions.assertTrue(statement.execute("select b, c from (select a, b, c from t1) as x "));
                try (final RelationalResultSet resultSet = statement.getResultSet()) {
                    ResultSetAssert.assertThat(resultSet).hasNextRow()
                            .isRowExactly(500L, 101L)
                            .hasNextRow()
                            .isRowExactly(501L, 102L)
                            .hasNoNextRow();
                }
            }
        }
    }

    @Test
    void cteWithColumnAliasesWorksCorrectly() throws Exception {
        final String schemaTemplate = "CREATE TABLE T1(pk bigint, a bigint, b bigint, c bigint, PRIMARY KEY(pk))";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                statement.executeUpdate("insert into t1 values (42, 100, 500, 101), (44, 101, 501, 102)");
                Assertions.assertTrue(statement.execute("with C1 (X, Y, Z) as (SELECT a, b, c from T1) select Y, Z from C1"));
                try (final RelationalResultSet resultSet = statement.getResultSet()) {
                    ResultSetAssert.assertThat(resultSet).hasNextRow()
                            .isRowExactly(500L, 101L)
                            .hasNextRow()
                            .isRowExactly(501L, 102L)
                            .hasNoNextRow();
                }
            }
        }
    }

    @Test
    void unionParenthesisIsNotSupported() throws Exception {
        final String schemaTemplate = "CREATE TABLE T1(pk bigint, a string, b bigint, PRIMARY KEY(pk))";

        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                RelationalAssertions.assertThrows(() ->
                        ((EmbeddedRelationalStatement) statement)
                                .executeInternal("(select * from t1) union (select * from t1)"))
                        .hasErrorCode(ErrorCode.UNSUPPORTED_QUERY);
            }
        }
    }

    @Test
    void selfJoinTest() throws Exception {
        final String schemaTemplate = "CREATE TABLE T1(pk bigint, a bigint, PRIMARY KEY(pk))";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                statement.executeUpdate("insert into t1 values (1, 10), (2, 20)");
                Assertions.assertTrue(statement.execute("select * from t1 as x, t1 as y"));
                try (final RelationalResultSet resultSet = statement.getResultSet()) {
                    ResultSetAssert.assertThat(resultSet).hasNextRow()
                            .isRowExactly(1L, 10L, 1L, 10L)
                            .hasNextRow()
                            .isRowExactly(1L, 10L, 2L, 20L)
                            .hasNextRow()
                            .isRowExactly(2L, 20L, 1L, 10L)
                            .hasNextRow()
                            .isRowExactly(2L, 20L, 2L, 20L)
                            .hasNoNextRow();
                }
            }
        }
    }

    @Test
    void testInsertUuidTest() throws Exception {
        final String schemaTemplate = "CREATE TABLE T1(pk bigint, a UUID, PRIMARY KEY(pk))";
        final var actualUuidValue = UUID.fromString("14b387cd-79ad-4860-9588-9c4e81588af0");
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var insert = ddl.setSchemaAndGetConnection().prepareStatement("insert into t1 values (1, '14b387cd-79ad-4860-9588-9c4e81588af0')")) {
                final var numActualInserted = insert.executeUpdate();
                Assertions.assertEquals(1, numActualInserted);
            }
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                Assertions.assertTrue(statement.execute("select * from t1"));
                try (final RelationalResultSet resultSet = statement.getResultSet()) {
                    ResultSetAssert.assertThat(resultSet)
                            .hasNextRow()
                            .isRowExactly(1L, UUID.fromString("14b387cd-79ad-4860-9588-9c4e81588af0"))
                            .hasNoNextRow();
                }
            }
        }
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var insert = ddl.setSchemaAndGetConnection().prepareStatement("insert into t1 values (?pk, ?a)")) {
                insert.setLong("pk", 1L);
                insert.setUUID("a", actualUuidValue);
                final var numActualInserted = insert.executeUpdate();
                Assertions.assertEquals(1, numActualInserted);
            }
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                Assertions.assertTrue(statement.execute("select * from t1"));
                try (final RelationalResultSet resultSet = statement.getResultSet()) {
                    ResultSetAssert.assertThat(resultSet)
                            .hasNextRow()
                            .isRowExactly(1L, actualUuidValue)
                            .hasNoNextRow();
                }
            }
        }
    }

    @Test
    void testInsertStructWithUuidTest() throws Exception {
        final String schemaTemplate = "CREATE TYPE AS STRUCT S1(a bigint, b uuid) " +
                "CREATE TABLE T1(pk bigint, a UUID, b s1, PRIMARY KEY(pk))";
        final var actualUuidValue1 = UUID.randomUUID();
        final var actualUuidValue2 = UUID.randomUUID();
        final var structWithUuid = EmbeddedRelationalStruct.newBuilder()
                .addLong("A", 2L)
                .addUuid("B", actualUuidValue2)
                .build();
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var insert = ddl.setSchemaAndGetConnection().prepareStatement("insert into t1 values (?pk, ?a, ?b)")) {
                insert.setLong("pk", 1L);
                insert.setUUID("a", actualUuidValue1);
                insert.setObject("b", structWithUuid);
                final var numActualInserted = insert.executeUpdate();
                Assertions.assertEquals(1, numActualInserted);
            }
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                Assertions.assertTrue(statement.execute("select * from t1"));
                try (final RelationalResultSet resultSet = statement.getResultSet()) {
                    ResultSetAssert.assertThat(resultSet)
                            .hasNextRow()
                            .hasColumn("PK", 1L)
                            .hasColumn("A", actualUuidValue1)
                            .hasColumn("B", structWithUuid)
                            .hasNoNextRow();
                }
            }
        }
    }

    // todo (yhatem) add more tests for queries w and w/o index definition.

    private void insertTypeConflictRecords(RelationalStatement s) throws SQLException {
        final var recBuilder = EmbeddedRelationalStruct.newBuilder()
                .addString("NAME", "Sophia");
        final var rec = recBuilder.build();
        int cnt = s.executeInsert("CLASSA", rec);
        Assertions.assertEquals(1, cnt, "Incorrect insertion count");
    }

    private RelationalStruct insertRestaurantComplexRecord(RelationalStatement s) throws SQLException {
        return insertRestaurantComplexRecord(s, 10L);
    }

    private RelationalStruct insertRestaurantComplexRecord(RelationalStatement s, Long recordNumber) throws SQLException {
        return insertRestaurantComplexRecord(s, recordNumber, "testName");
    }

    private RelationalStruct insertRestaurantComplexRecord(RelationalStatement s, Long recordNumber, @Nonnull final String recordName) throws SQLException {
        return insertRestaurantComplexRecord(s, recordNumber, recordName, List.of());
    }

    private RelationalStruct insertRestaurantComplexRecord(RelationalStatement s, Long recordNumber, @Nonnull final String recordName, @Nonnull final List<Triple<Long, Long, List<Pair<Long, String>>>> reviews) throws SQLException {
        final var recBuilder2 = EmbeddedRelationalStruct.newBuilder()
                .addLong("REST_NO", recordNumber)
                .addString("NAME", recordName)
                .addStruct("LOCATION", EmbeddedRelationalStruct.newBuilder()
                        .addString("ADDRESS", "address")
                        .addString("LATITUDE", "1")
                        .addString("LONGITUDE", "1")
                        .build());
        final var reviewsArrayBuilder = EmbeddedRelationalArray.newBuilder();
        for (final Triple<Long, Long, List<Pair<Long, String>>> review : reviews) {
            final var reviewBuilder = EmbeddedRelationalStruct.newBuilder()
                    .addLong("REVIEWER", review.getLeft())
                    .addLong("RATING", review.getMiddle());
            final var endorsementsArrayBuilder = EmbeddedRelationalArray.newBuilder();
            for (var endorsement : review.getRight()) {
                endorsementsArrayBuilder.addStruct(EmbeddedRelationalStruct.newBuilder()
                        .addLong("endorsementId", endorsement.getLeft())
                        .addString("endorsementText", endorsement.getRight())
                        .build());
            }
            reviewBuilder.addArray("ENDORSEMENTS", endorsementsArrayBuilder.build());
            reviewsArrayBuilder.addStruct(reviewBuilder.build());
        }
        recBuilder2.addArray("REVIEWS", reviewsArrayBuilder.build());
        final RelationalStruct structToInsert = recBuilder2.build();
        int cnt = s.executeInsert("RESTAURANTCOMPLEXRECORD", structToInsert);
        Assertions.assertEquals(1, cnt, "Incorrect insertion count");
        return getExpected(recordNumber, recordName, reviews);
    }

    private static RelationalStruct getExpected(Long recordNumber, @Nonnull final String recordName, @Nonnull final List<Triple<Long, Long, List<Pair<Long, String>>>> reviews) {
        final var locationType = DataType.StructType.from("LOCATION", List.of(
                DataType.StructType.Field.from("ADDRESS", DataType.Primitives.STRING.type(), 1),
                DataType.StructType.Field.from("LATITUDE", DataType.Primitives.STRING.type(), 2),
                DataType.StructType.Field.from("LONGITUDE", DataType.Primitives.STRING.type(), 3)
        ), false);
        final var endorsementType = DataType.StructType.from("ENDORSEMENT", List.of(
                DataType.StructType.Field.from("endorsementId", DataType.Primitives.LONG.type(), 1),
                DataType.StructType.Field.from("endorsementText", DataType.Primitives.STRING.type(), 2)
        ), false);
        final var reviewType = DataType.StructType.from("LOCATION", List.of(
                DataType.StructType.Field.from("REVIEWER", DataType.Primitives.LONG.type(), 1),
                DataType.StructType.Field.from("RATING", DataType.Primitives.LONG.type(), 2),
                DataType.StructType.Field.from("ENDORSEMENTS", DataType.ArrayType.from(endorsementType, false), 3)
        ), false);
        final var restaurantComplexRecordType = DataType.StructType.from("RESTAURANTCOMPLEXRECORD", List.of(
                DataType.StructType.Field.from("REST_NO", DataType.Primitives.LONG.type(), 1),
                DataType.StructType.Field.from("NAME", DataType.Primitives.STRING.type(), 2),
                DataType.StructType.Field.from("LOCATION", locationType, 3),
                DataType.StructType.Field.from("REVIEWS", DataType.ArrayType.from(reviewType), 4)
        ), false);
        final var locationStruct = new ImmutableRowStruct(new ArrayRow("address", 1, 1), RelationalStructMetaData.of(locationType));
        final var reviewsList = new ArrayList<RelationalStruct>();
        reviews.forEach(review -> {
            final var endorsementsList = review.getRight().stream()
                    .map(e -> new ImmutableRowStruct(new ArrayRow(e.getLeft(), e.getRight()), RelationalStructMetaData.of(endorsementType)))
                    .collect(Collectors.toList());
            reviewsList.add(new ImmutableRowStruct(new ArrayRow(
                    review.getLeft(),
                    review.getMiddle(),
                    new RowArray(endorsementsList, RelationalArrayMetaData.of(DataType.ArrayType.from(endorsementType, false)))
            ), RelationalStructMetaData.of(reviewType)));
        });
        final var reviewsArray = new RowArray(reviewsList, RelationalArrayMetaData.of(DataType.ArrayType.from(reviewType)));
        return new ImmutableRowStruct(new ArrayRow(recordNumber, recordName, locationStruct, reviewsArray), RelationalStructMetaData.of(restaurantComplexRecordType));
    }

    private void insertRestaurantComplexRecord(RelationalStatement s, int recordNumber, @Nonnull final String recordName, byte[] blob) throws SQLException {
        var struct = EmbeddedRelationalStruct.newBuilder()
                .addLong("REST_NO", recordNumber)
                .addString("NAME", recordName)
                .addBytes("ENCODED_BYTES", blob)
                .addStruct("LOCATION", EmbeddedRelationalStruct.newBuilder()
                        .addString("ADDRESS", "address")
                        .build())
                .build();
        int cnt = s.executeInsert("RESTAURANTCOMPLEXRECORD", struct);
        Assertions.assertEquals(1, cnt, "Incorrect insertion count");
    }
}
