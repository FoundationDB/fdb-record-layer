/*
 * InsertAutoQATest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2026 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.relational.api.EmbeddedRelationalStruct;
import com.apple.foundationdb.relational.api.RelationalConnection;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.RelationalStatement;
import com.apple.foundationdb.relational.api.RelationalStruct;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.utils.RelationalAssertions;
import com.apple.foundationdb.relational.utils.SimpleDatabaseRule;
import com.apple.foundationdb.relational.utils.TestSchemas;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import javax.annotation.Nonnull;
import java.sql.Array;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
public class InsertAutoQATest {
    @RegisterExtension
    @Order(0)
    public final EmbeddedRelationalExtension relationalExtension = new EmbeddedRelationalExtension();

    @RegisterExtension
    @Order(1)
    public final SimpleDatabaseRule database = new SimpleDatabaseRule(InsertAutoQATest.class, TestSchemas.restaurant());

    @Nonnull
    private RelationalConnection connect() throws SQLException {
        final RelationalConnection conn = DriverManager.getConnection(database.getConnectionUri().toString())
                .unwrap(RelationalConnection.class);
        conn.setSchema("TEST_SCHEMA");
        return conn;
    }

    @Nonnull
    private static List<Long> restaurantKeys(@Nonnull RelationalStatement s) throws SQLException {
        final List<Long> keys = new ArrayList<>();
        try (RelationalResultSet rs = s.executeQuery("SELECT rest_no FROM restaurant ORDER BY rest_no")) {
            while (rs.next()) {
                keys.add(rs.getLong(1));
            }
        }
        return keys;
    }

    @Test
    void insertSelectFromSameTableInsertsEachSourceRowOnce() throws SQLException {
        // INSERT ... SELECT from the target table must read the source rows before inserting (no Halloween problem)
        try (RelationalConnection conn = connect(); RelationalStatement s = conn.createStatement()) {
            assertThat(s.executeUpdate("INSERT INTO restaurant(rest_no, name) VALUES (1, 'a'), (2, 'b'), (3, 'c')"))
                    .isEqualTo(3);
            final int count = s.executeUpdate(
                    "INSERT INTO restaurant SELECT rest_no + 100, name, location, reviews, tags, customer, encoded_bytes "
                            + "FROM restaurant WHERE rest_no < 1000");
            assertThat(restaurantKeys(s)).containsExactly(1L, 2L, 3L, 101L, 102L, 103L);
            assertThat(count).isEqualTo(3);
        }
    }

    @Test
    void sqlMultiRowInsertWithDuplicateKeyInsertsNoRows() throws SQLException {
        // a multi-row VALUES whose last row repeats a key of the same statement fails as a whole
        try (RelationalConnection conn = connect(); RelationalStatement s = conn.createStatement()) {
            RelationalAssertions.assertThrowsSqlException(
                    () -> s.executeUpdate("INSERT INTO restaurant(rest_no, name) VALUES (10, 'a'), (11, 'b'), (10, 'c')"))
                    .hasErrorCode(ErrorCode.UNIQUE_CONSTRAINT_VIOLATION);
            assertThat(restaurantKeys(s)).isEmpty();
        }
    }

    @Test
    void directAccessInsertListWithDuplicateKeyInsertsNoRows() throws SQLException {
        // executeInsert of a list where one struct collides with another fails and leaves nothing behind in auto-commit
        try (RelationalConnection conn = connect(); RelationalStatement s = conn.createStatement()) {
            final RelationalStruct first = EmbeddedRelationalStruct.newBuilder().addLong("REST_NO", 20L).addString("NAME", "a").build();
            final RelationalStruct second = EmbeddedRelationalStruct.newBuilder().addLong("REST_NO", 21L).addString("NAME", "b").build();
            final RelationalStruct duplicate = EmbeddedRelationalStruct.newBuilder().addLong("REST_NO", 20L).addString("NAME", "c").build();
            RelationalAssertions.assertThrowsSqlException(
                    () -> s.executeInsert("RESTAURANT", List.of(first, second, duplicate)))
                    .hasErrorCode(ErrorCode.UNIQUE_CONSTRAINT_VIOLATION);
            assertThat(restaurantKeys(s)).isEmpty();
        }
    }

    @Test
    void sqlInsertNestedStructAndArrayRoundTrips() throws SQLException {
        // STRUCT and ARRAY-of-STRUCT values inserted through SQL are read back unchanged, and the update count is per row
        try (RelationalConnection conn = connect(); RelationalStatement s = conn.createStatement()) {
            final int count = s.executeUpdate("INSERT INTO restaurant(rest_no, name, location, reviews, customer) VALUES "
                    + "(30, 'n30', ('addr', '1.5', '2.5'), [(7, 5), (8, 4)], ['x', 'y']), "
                    + "(31, 'n31', ('addr2', '3', '4'), [], [])");
            assertThat(count).isEqualTo(2);
            try (RelationalResultSet rs = s.executeQuery("SELECT location, reviews, customer FROM restaurant WHERE rest_no = 30")) {
                assertThat(rs.next()).isTrue();
                final RelationalStruct location = rs.getStruct(1);
                assertThat(location.getString(1)).isEqualTo("addr");
                assertThat(location.getString(2)).isEqualTo("1.5");
                assertThat(location.getString(3)).isEqualTo("2.5");
                final Object[] reviews = (Object[]) rs.getArray(2).getArray();
                assertThat(reviews).hasSize(2);
                assertThat(((RelationalStruct) reviews[0]).getLong(1)).isEqualTo(7L);
                assertThat(((RelationalStruct) reviews[0]).getLong(2)).isEqualTo(5L);
                assertThat(((RelationalStruct) reviews[1]).getLong(1)).isEqualTo(8L);
                assertThat(((RelationalStruct) reviews[1]).getLong(2)).isEqualTo(4L);
                final Array customer = rs.getArray(3);
                assertThat((Object[]) customer.getArray()).containsExactly("x", "y");
                assertThat(rs.next()).isFalse();
            }
        }
    }

    @Test
    void sqlMultiRowInsertWithDuplicateKeyInExplicitTransactionLeavesNoRows() throws SQLException {
        // BUG AQA-27: with autoCommit=false a failed multi-row INSERT keeps its earlier rows in the transaction (rollback
        // only happens when canCommit()). Spec: ISO/IEC 9075-2 SQL-statement atomicity.
        // a failed multi-row INSERT inside an explicit transaction must not leave its earlier rows for the commit
        try (RelationalConnection conn = connect(); RelationalStatement s = conn.createStatement()) {
            conn.setAutoCommit(false);
            RelationalAssertions.assertThrowsSqlException(
                    () -> s.executeUpdate("INSERT INTO restaurant(rest_no, name) VALUES (60, 'a'), (60, 'b')"))
                    .hasErrorCode(ErrorCode.UNIQUE_CONSTRAINT_VIOLATION);
            conn.commit();
            conn.setAutoCommit(true);
            assertThat(restaurantKeys(s)).isEmpty();
        }
    }
}
