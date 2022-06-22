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
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.RelationalStatement;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.utils.Ddl;
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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Base64;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

public class QueryTest {

    private static final String schemaTemplate =
            "CREATE STRUCT Location (address string, latitude string, longitude string);" +
                    "CREATE STRUCT ReviewerEndorsements (endorsementId int64, endorsementText string);" +
                    "CREATE STRUCT RestaurantComplexReview (reviewer int64, rating int64, endorsements ReviewerEndorsements array);" +
                    "CREATE STRUCT RestaurantTag (tag string, weight int64);" +
                    "CREATE STRUCT ReviewerStats (start_date int64, school_name string, hometown string);" +
                    "CREATE TABLE RestaurantComplexRecord (rest_no int64, name string, location Location, reviews RestaurantComplexReview ARRAY, tags RestaurantTag array, customer string array, encoded_bytes bytes, PRIMARY KEY(rest_no));" +
                    "CREATE TABLE RestaurantReviewer (id int64, name string, email string, stats ReviewerStats, PRIMARY KEY(id));" +
                    "CREATE VALUE INDEX record_name_idx on RestaurantComplexRecord(name);" +
                    "CREATE VALUE INDEX reviewer_name_idx on RestaurantReviewer(name);" +
                    "CREATE MATERIALIZED VIEW mv1 AS SELECT R.rating from RestaurantComplexRecord AS Rec, (select rating from Rec.reviews) R;" +
                    "CREATE MATERIALIZED VIEW mv2 AS SELECT endo.endorsementText FROM RestaurantComplexRecord rec, (SELECT X.endorsementText FROM rec.reviews rev, (SELECT endorsementText from rev.endorsements) X) endo";

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
    void simpleSelect() throws Exception {
        try (var ddl = Ddl.builder().database("QT").relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                var insertedRecord = insertRestaurantComplexRecord(statement);
                Assertions.assertTrue(statement.execute("SELECT * FROM RestaurantComplexRecord"), "Did not return a result set from a select statement!");
                try (final ResultSet resultSet = statement.getResultSet()) {
                    RelationalAssertions.assertThat(resultSet).hasExactly(new MessageTuple(insertedRecord));
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

                try (final ResultSet resultSet = statement.executeQuery("SELECT * FROM RestaurantComplexRecord WHERE rest_no > 10")) {
                    RelationalAssertions.assertThat(resultSet).hasExactly(new MessageTuple(r11));
                }

                try (final ResultSet resultSet = statement.executeQuery("SELECT * FROM RestaurantComplexRecord WHERE rest_no >= 11")) {
                    RelationalAssertions.assertThat(resultSet).hasExactly(new MessageTuple(r11));
                }

                try (final ResultSet resultSet = statement.executeQuery("SELECT * FROM RestaurantComplexRecord WHERE 10 < rest_no")) {
                    RelationalAssertions.assertThat(resultSet).hasExactly(new MessageTuple(r11));
                }

                try (final ResultSet resultSet = statement.executeQuery("SELECT * FROM RestaurantComplexRecord WHERE 11 <= rest_no")) {
                    RelationalAssertions.assertThat(resultSet).hasExactly(new MessageTuple(r11));
                }
            }
        }
    }

    @Test
    void explainTableScan() throws Exception {
        try (var ddl = Ddl.builder().database("QT").relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                try (final ResultSet resultSet = statement.executeQuery("EXPLAIN SELECT * FROM RestaurantComplexRecord WHERE rest_no > 10")) {
                    resultSet.next();
                    String plan = resultSet.getString(1);
                    assertThat(plan).matches(".*Scan.*RestaurantComplexRecord.*rest_no GREATER_THAN 10.* as rest_no, .* as name, .* as location, .* as reviews, .* as tags, .* as customer, .* as encoded_bytes.*");
                }
            }
        }
    }

    @Test
    void explainHintedIndexScan() throws Exception {
        try (var ddl = Ddl.builder().database("QT").relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                try (final ResultSet resultSet = statement.executeQuery("EXPLAIN SELECT * FROM RestaurantComplexRecord USE INDEX (record_name_idx) WHERE rest_no > 10")) {
                    resultSet.next();
                    String plan = resultSet.getString(1);
                    assertThat(plan).matches(".*Fetch.*Covering.*Index.*record_name_idx.*rest_no GREATER_THAN 10.* as rest_no, .* as name, .* as location, .* as reviews, .* as tags, .* as customer, .* as encoded_bytes.*");
                }
            }
        }
    }

    @Test
    void explainUnhintedIndexScan() throws Exception {
        try (var ddl = Ddl.builder().database("QT").relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                try (final ResultSet resultSet = statement.executeQuery("EXPLAIN SELECT * FROM RestaurantComplexRecord AS R WHERE EXISTS (SELECT * FROM R.reviews AS RE WHERE RE.rating >= 9)")) {
                    resultSet.next();
                    String plan = resultSet.getString(1);
                    assertThat(plan).matches(".*Index.*mv1.*\\[9\\],>.* as rest_no, .* as name, .* as location, .* as reviews, .* as tags, .* as customer, .* as encoded_bytes.*");
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
                try (final ResultSet resultSet = statement.executeQuery("SELECT * FROM RestaurantComplexRecord WHERE rest_no > 42 AND name = 'rest1'")) {
                    assertMatches(resultSet, List.of(l43, l44));
                }
                try (final ResultSet resultSet = statement.executeQuery("SELECT * FROM RestaurantComplexRecord WHERE name = 'rest2' OR name = 'rest1'")) {
                    assertMatches(resultSet, List.of(l42, l43, l44, l45));
                }
                try (final ResultSet resultSet = statement.executeQuery("SELECT * FROM RestaurantComplexRecord WHERE rest_no = (40+2)")) {
                    assertMatches(resultSet, List.of(l42));
                }
                try (final ResultSet resultSet = statement.executeQuery("SELECT * FROM RestaurantComplexRecord WHERE (40+2) = rest_no")) {
                    assertMatches(resultSet, List.of(l42));
                }
                try (final ResultSet resultSet = statement.executeQuery("SELECT * FROM RestaurantComplexRecord WHERE (44-2) = rest_no")) {
                    assertMatches(resultSet, List.of(l42));
                }
                try (final ResultSet resultSet = statement.executeQuery("SELECT * FROM RestaurantComplexRecord WHERE 0X2A = rest_no")) {
                    assertMatches(resultSet, List.of(l42));
                }
                try (final ResultSet resultSet = statement.executeQuery("SELECT * FROM RestaurantComplexRecord WHERE rest_no < -1")) {
                    assertMatches(resultSet, List.of());
                }
                try (final ResultSet resultSet = statement.executeQuery("SELECT * FROM RestaurantComplexRecord WHERE 10 < -3.9")) {
                    assertMatches(resultSet, List.of());
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
                try (final ResultSet resultSet = statement.executeQuery("select * from RestaurantComplexRecord where 42 is null AND 11 = rest_no")) {
                    RelationalAssertions.assertThat(resultSet).hasNoNextRow();
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
                try (final ResultSet resultSet = statement.executeQuery("select * from RestaurantComplexRecord where true = false")) {
                    RelationalAssertions.assertThat(resultSet).hasNoNextRow();
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
                        query += " WITH CONTINUATION \"" + Base64.getEncoder().encodeToString(continuation.getBytes()) + "\"";
                    }
                    try (final ResultSet resultSet = statement.executeQuery(query)) {
                        // assert result matches expected
                        Assertions.assertNotNull(resultSet, "Did not return a result set!");
                        RelationalAssertions.assertThat(resultSet).nextRowMatches(new MessageTuple(expected.get(i)));
                        // get continuation for the next query
                        Assertions.assertTrue(resultSet instanceof RelationalResultSet);
                        continuation = ((RelationalResultSet) resultSet).getContinuation();
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
                try (final ResultSet resultSet = statement.executeQuery("SELECT name FROM RestaurantComplexRecord USE INDEX (record_name_idx)")) {
                    while (resultSet.next()) {
                        Assertions.assertEquals("testName", resultSet.getString(1));
                    }
                }
                // successfully execute a query with multiple hinted indexes
                try (final ResultSet resultSet = statement.executeQuery("SELECT name FROM RestaurantComplexRecord USE INDEX (record_name_idx, reviewer_name_idx)")) {
                    while (resultSet.next()) {
                        Assertions.assertEquals("testName", resultSet.getString(1));
                    }
                }
                // successfully execute a query with multiple hinted indexes, different syntax
                try (final ResultSet resultSet = statement.executeQuery("SELECT name FROM RestaurantComplexRecord USE INDEX (record_name_idx), USE INDEX (reviewer_name_idx)")) {
                    while (resultSet.next()) {
                        Assertions.assertEquals("testName", resultSet.getString(1));
                    }
                }
                // exception is thrown when hinted indexes don't exist
                Exception exception = Assertions.assertThrows(SQLException.class, () -> {
                    statement.executeQuery("SELECT * FROM RestaurantComplexRecord USE INDEX (name) WHERE 11 <= rest_no");
                });
                Assertions.assertEquals("Unknown index(es) name", exception.getMessage());
            }
        }
    }

    @Test
    void projectIndividualColumns() throws Exception {
        try (var ddl = Ddl.builder().database("QT").relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                insertRestaurantComplexRecord(statement);
                try (final ResultSet resultSet = statement.executeQuery("SELECT name FROM RestaurantComplexRecord WHERE 11 <= rest_no")) {

                    while (resultSet.next()) {
                        Assertions.assertEquals("testName", resultSet.getString(1));
                    }
                }
            }
        }
    }

    @Test
    void projectIndividualQualifiedColumns() throws Exception {
        try (var ddl = Ddl.builder().database("QT").relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                insertRestaurantComplexRecord(statement);
                try (final ResultSet resultSet = statement.executeQuery("SELECT RestaurantComplexRecord.name FROM RestaurantComplexRecord WHERE 11 <= rest_no")) {

                    while (resultSet.next()) {
                        Assertions.assertEquals("testName", resultSet.getString(1));
                    }
                }
            }
        }
    }

    @Test
    void projectIndividualQualifiedColumnsOverAlias() throws Exception {
        try (var ddl = Ddl.builder().database("QT").relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                insertRestaurantComplexRecord(statement);
                try (final ResultSet resultSet = statement.executeQuery("SELECT name FROM RestaurantComplexRecord AS X WHERE 11 <= rest_no")) {

                    while (resultSet.next()) {
                        Assertions.assertEquals("testName", resultSet.getString(1));
                    }
                }
            }
        }
    }

    @Test
    void projectIndividualQualifiedColumnsOverAlias2() throws Exception {
        try (var ddl = Ddl.builder().database("QT").relationalExtension(relationalExtension).schemaTemplate(schemaTemplate).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                insertRestaurantComplexRecord(statement);
                try (final ResultSet resultSet = statement.executeQuery("SELECT X.name FROM RestaurantComplexRecord AS X WHERE 11 <= rest_no")) {

                    while (resultSet.next()) {
                        Assertions.assertEquals("testName", resultSet.getString(1));
                    }
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

                try (final ResultSet resultSet = statement.executeQuery("SELECT * FROM RestaurantComplexRecord WHERE name = 'getBytes'")) {
                    while (resultSet.next()) {
                        byte[] bytes = resultSet.getBytes("encoded_bytes");
                        switch ((int) resultSet.getLong("rest_no")) {
                            case 1:
                                assertThat(bytes).isEqualTo("blob1".getBytes(StandardCharsets.UTF_8));
                                break;
                            case 2:
                                assertThat(bytes).isEqualTo("".getBytes(StandardCharsets.UTF_8));
                                break;
                            default:
                                Assertions.fail("Unknown record returned by query");
                                break;
                        }
                    }
                }
            }
        }
    }

    @Test
    void partiqlNestingWorks() throws Exception {
        final String schema = "CREATE STRUCT A ( b B );" +
                "CREATE STRUCT B ( c C );" +
                "CREATE STRUCT C ( d D );" +
                "CREATE STRUCT D ( e E );" +
                "CREATE STRUCT E ( f int64 );" +
                "CREATE TABLE tbl1 (id int64, c C, a A, PRIMARY KEY(id));";
        try (var ddl = Ddl.builder().database("QT").relationalExtension(relationalExtension).schemaTemplate(schema).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                final Message result = statement.getDataBuilder("tbl1")
                        .setField("id", 42L)
                        .setField("c", statement.getDataBuilder("C")
                                .setField("d", statement.getDataBuilder("D")
                                        .setField("e", statement.getDataBuilder("E")
                                                .setField("f", 128L)
                                                .build())
                                        .build())
                                .build())
                        .setField("a", statement.getDataBuilder("A")
                                .setField("b", statement.getDataBuilder("B")
                                        .setField("c", statement.getDataBuilder("C")
                                                .setField("d", statement.getDataBuilder("D")
                                                        .setField("e", statement.getDataBuilder("E")
                                                                .setField("f", 128L)
                                                                .build())
                                                        .build())
                                                .build())
                                        .build())
                                .build())
                        .build();
                final int cnt = statement.executeInsert("tbl1", result);
                Assertions.assertEquals(1, cnt, "Incorrect insertion count");
                Assertions.assertTrue(statement.execute("SELECT id, c.d.e.f, a.b.c.d.e.f FROM tbl1"), "Did not return a result set from a select statement!");
                try (final ResultSet resultSet = statement.getResultSet()) {
                    Assertions.assertTrue(resultSet.next());
                    Assertions.assertEquals(resultSet.getLong(1), 42L);
                    Assertions.assertEquals(resultSet.getLong(2), 128L);
                    Assertions.assertEquals(resultSet.getLong(3), 128L);
                    Assertions.assertFalse(resultSet.next());
                }
                Assertions.assertTrue(statement.execute("SELECT c.d.e FROM tbl1"), "Did not return a result set from a select statement!");
                try (final ResultSet resultSet = statement.getResultSet()) {
                    Assertions.assertTrue(resultSet.next());
                    final Message expected = statement.getDataBuilder("E")
                            .setField("f", 128L)
                            .build();
                    Assertions.assertEquals(resultSet.getObject(1), expected);
                    Assertions.assertFalse(resultSet.next());
                }
            }
        }
    }

    @Test
    void partiqlNestingWorksWithRepeatedLeafWork() throws Exception {
        final String schema = "CREATE STRUCT A ( b B );" +
                "CREATE STRUCT B ( c C );" +
                "CREATE STRUCT C ( d D );" +
                "CREATE STRUCT D ( e E );" +
                "CREATE STRUCT E ( f int64 array );" +
                "CREATE TABLE tbl1 (id int64, c C, a A, PRIMARY KEY(id));";
        try (var ddl = Ddl.builder().database("QT").relationalExtension(relationalExtension).schemaTemplate(schema).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                final Message result = statement.getDataBuilder("tbl1")
                        .setField("id", 42L)
                        .setField("c", statement.getDataBuilder("C")
                                .setField("d", statement.getDataBuilder("D")
                                        .setField("e", statement.getDataBuilder("E")
                                                .addRepeatedField("f", 128L)
                                                .build())
                                        .build())
                                .build())
                        .setField("a", statement.getDataBuilder("A")
                                .setField("b", statement.getDataBuilder("B")
                                        .setField("c", statement.getDataBuilder("C")
                                                .setField("d", statement.getDataBuilder("D")
                                                        .setField("e", statement.getDataBuilder("E")
                                                                .addRepeatedField("f", 128L)
                                                                .build())
                                                        .build())
                                                .build())
                                        .build())
                                .build())
                        .build();
                final int cnt = statement.executeInsert("tbl1", result);
                Assertions.assertEquals(1, cnt, "Incorrect insertion count");
                Assertions.assertTrue(statement.execute("SELECT id, c.d.e.f, a.b.c.d.e.f FROM tbl1"), "Did not return a result set from a select statement!");
                try (final ResultSet resultSet = statement.getResultSet()) {
                    Assertions.assertTrue(resultSet.next());
                    Assertions.assertEquals(resultSet.getLong(1), 42L);
                    Assertions.assertEquals(((List) resultSet.getObject(2)).get(0), 128L);
                    Assertions.assertEquals(((List) resultSet.getObject(3)).get(0), 128L);
                    Assertions.assertFalse(resultSet.next());
                }
            }
        }
    }

    @Test
    void partiqlNestingNestedPathCollisionWithAlias() throws Exception {
        final String schema = "CREATE STRUCT A ( b B );" +
                "CREATE STRUCT B ( c C );" +
                "CREATE STRUCT C ( d D );" +
                "CREATE STRUCT D ( e E );" +
                "CREATE STRUCT E ( f int64 );" +
                "CREATE TABLE tbl1 (id int64, c C, a A, PRIMARY KEY(id));" +
                "CREATE TABLE tbl2 (id int64, x int64, PRIMARY KEY(id));";
        try (var ddl = Ddl.builder().database("QT").relationalExtension(relationalExtension).schemaTemplate(schema).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                failsWith(statement, "select * from tbl1 x, tbl2 where x = 42", "ambiguous field name 'x'");
            }
        }
    }

    @Test
    void partiqlAccessingNestedFieldWithInnerRepeatedFieldsFails() throws Exception {
        final String schema = "CREATE STRUCT A ( b B );" +
                "CREATE STRUCT B ( c C );" +
                "CREATE STRUCT C ( d D );" +
                "CREATE STRUCT D ( e E array );" +
                "CREATE STRUCT E ( f int64 array );" +
                "CREATE TABLE tbl1 (id int64, c C, a A, PRIMARY KEY(id));";
        try (var ddl = Ddl.builder().database("QT").relationalExtension(relationalExtension).schemaTemplate(schema).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                try {
                    statement.execute("SELECT id, c.d.e.f, a.b.c.d.e.f FROM tbl1");
                    fail("expected an exception to be thrown by running 'SELECT id, c.d.e.f, a.b.c.d.e.f FROM tbl1'");
                    // todo refactor once https://github.com/FoundationDB/fdb-record-layer/pull/1742 is in a milestone.
                } catch (ClassCastException cce) {
                    cce.getMessage().contains("class com.apple.foundationdb.record.query.plan.cascades.typing.Type$Array cannot be cast to " +
                            "class com.apple.foundationdb.record.query.plan.cascades.typing.Type$Record");
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
                try (final ResultSet resultSet = statement.executeQuery("SELECT rest_no FROM RestaurantComplexRecord WHERE 11 <= rest_no")) {
                    while (resultSet.next()) {
                        Assertions.assertEquals(11L, resultSet.getLong(1));
                    }
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
                try (final ResultSet resultSet = statement.executeQuery("SELECT * FROM RestaurantComplexRecord WHERE rest_no > 40.5")) {
                    assertMatches(resultSet, List.of(l42, l43, l44, l45));
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
                try (final ResultSet resultSet = statement.executeQuery("SELECT * FROM RestaurantComplexRecord AS R WHERE EXISTS (SELECT * FROM R.reviews AS RE WHERE RE.rating >= 9)")) {
                    assertMatches(resultSet, List.of(l43, l44));
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
                try (final ResultSet resultSet = statement.executeQuery("SELECT * FROM RestaurantComplexRecord AS R WHERE EXISTS (SELECT * FROM R.reviews AS RE WHERE EXISTS(SELECT * FROM RE.endorsements AS REE WHERE REE.endorsementText='wow'))")) {
                    assertMatches(resultSet, List.of(l42));
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
        final var recBuilder2 = s.getDataBuilder("RestaurantComplexRecord")
                .setField("rest_no", recordNumber)
                .setField("name", recordName)
                .setField("location", s.getDataBuilder("Location")
                        .setField("address", "address")
                        .setField("latitude", 1)
                        .setField("longitude", 1)
                        .build());

        for (final Triple<Long, Long, List<Pair<Long, String>>> review : reviews) {
            recBuilder2.addRepeatedField("reviews", s.getDataBuilder("RestaurantComplexReview")
                    .setField("reviewer", review.getLeft())
                    .setField("rating", review.getMiddle())
                    .addRepeatedFields("endorsements", review.getRight().stream().map(endo -> {
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
        int cnt = s.executeInsert("RestaurantComplexRecord", rec);
        Assertions.assertEquals(1, cnt, "Incorrect insertion count");
        return rec;
    }

    private Message insertRestaurantComplexRecord(RelationalStatement s, int recordNumber, @Nonnull final String recordName, byte[] blob) throws RelationalException {
        Message result = s.getDataBuilder("RestaurantComplexRecord")
                .setField("rest_no", recordNumber)
                .setField("name", recordName)
                .setField("encoded_bytes", ByteString.copyFrom(blob))
                .setField("location", s.getDataBuilder("Location")
                        .setField("address", "address")
                        .build()).build();

        int cnt = s.executeInsert("RestaurantComplexRecord", result);
        Assertions.assertEquals(1, cnt, "Incorrect insertion count");
        return result;
    }

    private <M extends Message> void assertMatches(ResultSet resultSet, Collection<M> rec) throws SQLException, RelationalException {
        Assertions.assertNotNull(resultSet, "Did not return a result set!");
        RelationalAssertions.assertThat(resultSet).hasExactlyInAnyOrder(
                rec.stream().map(MessageTuple::new).collect(Collectors.toList())
        );
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
