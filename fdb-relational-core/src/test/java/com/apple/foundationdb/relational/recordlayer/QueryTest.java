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

import com.apple.foundationdb.record.Restaurant;
import com.apple.foundationdb.relational.api.Continuation;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.RelationalStatement;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.utils.SimpleDatabaseRule;
import com.apple.foundationdb.relational.utils.TestSchemas;
import com.apple.foundationdb.relational.utils.RelationalAssertions;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Base64;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import static org.assertj.core.api.Assertions.assertThat;

public class QueryTest {
    @RegisterExtension
    @Order(0)
    public final EmbeddedRelationalExtension relationalExtension = new EmbeddedRelationalExtension();

    @RegisterExtension
    @Order(1)
    public final SimpleDatabaseRule database = new SimpleDatabaseRule(relationalExtension, QueryTest.class, TestSchemas.restaurant());

    @RegisterExtension
    @Order(2)
    public final RelationalConnectionRule connection = new RelationalConnectionRule(database::getConnectionUri)
            .withOptions(Options.NONE)
            .withSchema("testSchema");

    @RegisterExtension
    @Order(3)
    public final RelationalStatementRule statement = new RelationalStatementRule(connection);

    private Restaurant.RestaurantRecord insertedRecord;

    @BeforeEach
    public final void setup() throws RelationalException {
        insertedRecord = insertRestaurantRecord(statement);
    }

    @Test
    void simpleSelect() throws RelationalException, SQLException {
        Assertions.assertTrue(statement.execute("SELECT * FROM RestaurantRecord"), "Did not return a result set from a select statement!");
        try (final ResultSet resultSet = statement.getResultSet()) {
            RelationalAssertions.assertThat(resultSet).hasExactly(new MessageTuple(insertedRecord));
        }
    }

    @Test
    void selectWithPredicateVariants() throws RelationalException, SQLException {
        Restaurant.RestaurantRecord r11 = insertRestaurantRecord(statement, 11);
        try (final ResultSet resultSet = statement.executeQuery("SELECT * FROM RestaurantRecord WHERE rest_no > 10")) {
            RelationalAssertions.assertThat(resultSet).hasExactly(new MessageTuple(r11));
        }

        try (final ResultSet resultSet = statement.executeQuery("SELECT * FROM RestaurantRecord WHERE rest_no >= 11")) {
            RelationalAssertions.assertThat(resultSet).hasExactly(new MessageTuple(r11));
        }

        try (final ResultSet resultSet = statement.executeQuery("SELECT * FROM RestaurantRecord WHERE 10 < rest_no")) {
            RelationalAssertions.assertThat(resultSet).hasExactly(new MessageTuple(r11));
        }

        try (final ResultSet resultSet = statement.executeQuery("SELECT * FROM RestaurantRecord WHERE 11 <= rest_no")) {
            RelationalAssertions.assertThat(resultSet).hasExactly(new MessageTuple(r11));
        }
    }

    @Test
    void selectWithPredicateCompositionVariants() throws RelationalException, SQLException {
        Restaurant.RestaurantRecord l42 = insertRestaurantRecord(statement, 42, "rest1");
        Restaurant.RestaurantRecord l43 = insertRestaurantRecord(statement, 43, "rest1");
        Restaurant.RestaurantRecord l44 = insertRestaurantRecord(statement, 44, "rest1");
        Restaurant.RestaurantRecord l45 = insertRestaurantRecord(statement, 45, "rest2");
        try (final ResultSet resultSet = statement.executeQuery("SELECT * FROM RestaurantRecord WHERE rest_no > 42 AND name = 'rest1'")) {
            assertMatches(resultSet, List.of(l43, l44), this::convert);
        }
        try (final ResultSet resultSet = statement.executeQuery("SELECT * FROM RestaurantRecord WHERE name = 'rest2' OR name = 'rest1'")) {
            assertMatches(resultSet, List.of(l42, l43, l44, l45), this::convert);
        }
        try (final ResultSet resultSet = statement.executeQuery("SELECT * FROM RestaurantRecord WHERE rest_no = (40+2)")) {
            assertMatches(resultSet, List.of(l42), this::convert);
        }
        try (final ResultSet resultSet = statement.executeQuery("SELECT * FROM RestaurantRecord WHERE (40+2) = rest_no")) {
            assertMatches(resultSet, List.of(l42), this::convert);
        }
        try (final ResultSet resultSet = statement.executeQuery("SELECT * FROM RestaurantRecord WHERE (44-2) = rest_no")) {
            assertMatches(resultSet, List.of(l42), this::convert);
        }
        try (final ResultSet resultSet = statement.executeQuery("SELECT * FROM RestaurantRecord WHERE 0X2A = rest_no")) {
            assertMatches(resultSet, List.of(l42), this::convert);
        }
        try (final ResultSet resultSet = statement.executeQuery("SELECT * FROM RestaurantRecord WHERE rest_no < -1")) {
            assertMatches(resultSet, List.of(), this::convert);
        }
        try (final ResultSet resultSet = statement.executeQuery("SELECT * FROM RestaurantRecord WHERE 10 < -3.9")) {
            assertMatches(resultSet, List.of(), this::convert);
        }
    }

    @Test
    void selectWithFalsePredicate() throws RelationalException, SQLException {
        insertRestaurantRecord(statement, 11);
        try (final ResultSet resultSet = statement.executeQuery("select * from RestaurantRecord where 42 is null AND 11 = rest_no")) {
            RelationalAssertions.assertThat(resultSet).hasNoNextRow();
        }
    }

    @Test
    void selectWithFalsePredicate2() throws RelationalException, SQLException {
        insertRestaurantRecord(statement, 11);
        try (final ResultSet resultSet = statement.executeQuery("select * from RestaurantRecord where true = false")) {
            RelationalAssertions.assertThat(resultSet).hasNoNextRow();
        }
    }

    @Test
    void selectWithContinuation() throws RelationalException, SQLException {
        Restaurant.RestaurantRecord l42 = insertRestaurantRecord(statement, 42, "rest1");
        Restaurant.RestaurantRecord l43 = insertRestaurantRecord(statement, 43, "rest1");
        Restaurant.RestaurantRecord l44 = insertRestaurantRecord(statement, 44, "rest1");
        Restaurant.RestaurantRecord l45 = insertRestaurantRecord(statement, 45, "rest2");
        final String initialQuery = "select * from RestaurantRecord where rest_no > 40";
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

    @Test
    void testSelectWithIndexHint() throws SQLException {
        // successfully execute a query with hinted index
        try (final ResultSet resultSet = statement.executeQuery("SELECT name FROM RestaurantRecord USE INDEX (record_name_idx)")) {
            while (resultSet.next()) {
                Assertions.assertEquals("testName", resultSet.getString(1));
            }
        }
        // successfully execute a query with multiple hinted indexes
        try (final ResultSet resultSet = statement.executeQuery("SELECT name FROM RestaurantRecord USE INDEX (record_name_idx, reviewer_name_idx)")) {
            while (resultSet.next()) {
                Assertions.assertEquals("testName", resultSet.getString(1));
            }
        }
        // successfully execute a query with multiple hinted indexes, different syntax
        try (final ResultSet resultSet = statement.executeQuery("SELECT name FROM RestaurantRecord USE INDEX (record_name_idx), USE INDEX (reviewer_name_idx)")) {
            while (resultSet.next()) {
                Assertions.assertEquals("testName", resultSet.getString(1));
            }
        }
        // exception is thrown when hinted indexes don't exist
        Exception exception = Assertions.assertThrows(SQLException.class, () -> {
            statement.executeQuery("SELECT * FROM RestaurantRecord USE INDEX (name) WHERE 11 <= rest_no");
        });
        Assertions.assertEquals("Unknown index(es) name", exception.getMessage());
    }

    @Test
    void projectIndividualColumns() throws RelationalException, SQLException {
        try (final ResultSet resultSet = statement.executeQuery("SELECT name FROM RestaurantRecord WHERE 11 <= rest_no")) {

            while (resultSet.next()) {
                Assertions.assertEquals("testName", resultSet.getString(1));
            }
        }
    }

    @Test
    void projectIndividualQualifiedColumns() throws RelationalException, SQLException {
        try (final ResultSet resultSet = statement.executeQuery("SELECT RestaurantRecord.name FROM RestaurantRecord WHERE 11 <= rest_no")) {

            while (resultSet.next()) {
                Assertions.assertEquals("testName", resultSet.getString(1));
            }
        }
    }

    @Test
    void projectIndividualQualifiedColumnsOverAlias() throws RelationalException, SQLException {
        try (final ResultSet resultSet = statement.executeQuery("SELECT name FROM RestaurantRecord AS X WHERE 11 <= rest_no")) {

            while (resultSet.next()) {
                Assertions.assertEquals("testName", resultSet.getString(1));
            }
        }
    }

    @Test
    void projectIndividualQualifiedColumnsOverAlias2() throws RelationalException, SQLException {
        try (final ResultSet resultSet = statement.executeQuery("SELECT X.name FROM RestaurantRecord AS X WHERE 11 <= rest_no")) {

            while (resultSet.next()) {
                Assertions.assertEquals("testName", resultSet.getString(1));
            }
        }
    }

    @Test
    void getBytes() throws RelationalException, SQLException {
        insertRestaurantRecord(statement, 1, "getBytes", "blob1".getBytes(StandardCharsets.UTF_8));
        insertRestaurantRecord(statement, 2, "getBytes", "".getBytes(StandardCharsets.UTF_8));

        try (final ResultSet resultSet = statement.executeQuery("SELECT * FROM RestaurantRecord WHERE name = 'getBytes'")) {
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

    @Disabled
    // until we fix the implicit fetch operator in record layer.
    void projectIndividualPredicateColumns() throws RelationalException, SQLException {
        try (final ResultSet resultSet = statement.executeQuery("SELECT rest_no FROM RestaurantRecord WHERE 11 <= rest_no")) {
            while (resultSet.next()) {
                Assertions.assertEquals(11L, resultSet.getLong(1));
            }
        }
    }

    @Disabled
    // until we implement operators for type promotion and casts in record layer.
    void predicateWithImplicitCast() throws RelationalException, SQLException {
        Restaurant.RestaurantRecord l42 = insertRestaurantRecord(statement, 42, "rest1");
        Restaurant.RestaurantRecord l43 = insertRestaurantRecord(statement, 43, "rest1");
        Restaurant.RestaurantRecord l44 = insertRestaurantRecord(statement, 44, "rest1");
        Restaurant.RestaurantRecord l45 = insertRestaurantRecord(statement, 45, "rest2");
        try (final ResultSet resultSet = statement.executeQuery("SELECT * FROM RestaurantRecord WHERE rest_no > 40.5")) {
            assertMatches(resultSet, List.of(l42, l43, l44, l45), this::convert);
        }
    }

    private Restaurant.RestaurantRecord insertRestaurantRecord(RelationalStatement s) throws RelationalException {
        return insertRestaurantRecord(s, 10);
    }

    private Restaurant.RestaurantRecord insertRestaurantRecord(RelationalStatement s, int recordNumber) throws RelationalException {
        return insertRestaurantRecord(s, recordNumber, "testName");
    }

    private Restaurant.RestaurantRecord insertRestaurantRecord(RelationalStatement s, int recordNumber, @Nonnull final String recordName) throws RelationalException {
        Restaurant.RestaurantRecord rec = Restaurant.RestaurantRecord.newBuilder().setRestNo(recordNumber).setName(recordName).setLocation(Restaurant.Location.newBuilder().setAddress("address").build()).build();
        int cnt = s.executeInsert("RestaurantRecord", s.getDataBuilder("RestaurantRecord").convertMessage(rec));
        Assertions.assertEquals(1, cnt, "Incorrect insertion count");
        return rec;
    }

    private Restaurant.RestaurantRecord insertRestaurantRecord(RelationalStatement s, int recordNumber, @Nonnull final String recordName, byte[] blob) throws RelationalException {
        Restaurant.RestaurantRecord rec = Restaurant.RestaurantRecord.newBuilder()
                .setRestNo(recordNumber)
                .setName(recordName)
                .setEncodedBytes(ByteString.copyFrom(blob))
                .setLocation(Restaurant.Location.newBuilder().setAddress("address"))
                .build();
        int cnt = s.executeInsert("RestaurantRecord", s.getDataBuilder("RestaurantRecord").convertMessage(rec));
        Assertions.assertEquals(1, cnt, "Incorrect insertion count");
        return rec;
    }

    private <M extends Message> void assertMatches(ResultSet resultSet, Collection<M> rec, Function<M, M> adapter) throws SQLException, RelationalException {
        Assertions.assertNotNull(resultSet, "Did not return a result set!");
        RelationalAssertions.assertThat(resultSet).hasExactlyInAnyOrder(
                rec.stream().map(MessageTuple::new).collect(Collectors.toList())
        );
    }

    private Message convert(Message message) {
        try {
            return Restaurant.RestaurantRecord.parseFrom(message.toByteArray());
        } catch (InvalidProtocolBufferException e) {
            Assertions.fail("message conversion failed!");
            return null;
        }
    }
}
