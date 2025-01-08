/*
 * InsertTest.java
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

import com.apple.foundationdb.annotation.API;

import com.apple.foundationdb.relational.api.EmbeddedRelationalArray;
import com.apple.foundationdb.relational.api.EmbeddedRelationalStruct;
import com.apple.foundationdb.relational.api.FieldDescription;
import com.apple.foundationdb.relational.api.KeySet;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.RelationalArrayMetaData;
import com.apple.foundationdb.relational.api.RelationalConnection;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.RelationalStatement;
import com.apple.foundationdb.relational.api.RelationalStruct;
import com.apple.foundationdb.relational.api.RelationalStructMetaData;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.utils.ResultSetAssert;
import com.apple.foundationdb.relational.utils.SimpleDatabaseRule;
import com.apple.foundationdb.relational.utils.TestSchemas;
import com.apple.foundationdb.relational.utils.RelationalAssertions;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Map;

@API(API.Status.EXPERIMENTAL)
public class InsertTest {
    @RegisterExtension
    @Order(0)
    public final EmbeddedRelationalExtension relationalExtension = new EmbeddedRelationalExtension();

    @RegisterExtension
    @Order(1)
    public final SimpleDatabaseRule database = new SimpleDatabaseRule(relationalExtension, InsertTest.class, TestSchemas.restaurant());

    @Test
    void canInsertWithMultipleRecordTypes() throws RelationalException, SQLException {
        /*
         * We want to make sure that we don't accidentally pick up data from different tables
         */
        try (RelationalConnection conn = DriverManager.getConnection(database.getConnectionUri().toString()).unwrap(RelationalConnection.class)) {
            conn.setSchema("TEST_SCHEMA");
            try (RelationalStatement s = conn.createStatement()) {
                long id = System.currentTimeMillis();
                RelationalStruct restaurant = EmbeddedRelationalStruct.newBuilder()
                        .addLong("REST_NO", id)
                        .addString("NAME", "restRecord" + id)
                        .build();
                int inserted = s.executeInsert("RESTAURANT", restaurant);
                Assertions.assertEquals(1, inserted, "Did not insert properly!");

                RelationalStruct reviewer = EmbeddedRelationalStruct.newBuilder()
                        .addString("NAME", "reviewerName" + id)
                        .addLong("ID", id - 1)
                        .build();
                inserted = s.executeInsert("RESTAURANT_REVIEWER", reviewer);
                Assertions.assertEquals(1, inserted, "Did not insert reviewers properly!");

                //now prove we can get them back out
                try (RelationalResultSet relationalResultSet = s.executeGet("RESTAURANT", new KeySet().setKeyColumns(Map.of("REST_NO", id)), Options.NONE)) {
                    ResultSetAssert.assertThat(relationalResultSet).hasNextRow()
                            .hasColumns(Map.of("NAME", "restRecord" + id, "REST_NO", id))
                            .hasNoNextRow();
                }

                try (RelationalResultSet relationalResultSet = s.executeGet("RESTAURANT_REVIEWER", new KeySet().setKeyColumn("ID", id - 1), Options.NONE)) {
                    ResultSetAssert.assertThat(relationalResultSet).hasNextRow()
                            .hasColumns(Map.of("NAME", "reviewerName" + id, "ID", id - 1))
                            .hasNoNextRow();
                }

                //now make sure that they don't show up in the other table
                try (RelationalResultSet relationalResultSet = s.executeGet("RESTAURANT",
                        new KeySet().setKeyColumn("NAME", "reviewerName" + id),
                        Options.builder().withOption(Options.Name.INDEX_HINT, "RECORD_NAME_IDX").build())) {
                    ResultSetAssert.assertThat(relationalResultSet).isEmpty();
                }

                try (RelationalResultSet relationalResultSet = s.executeGet("RESTAURANT_REVIEWER",
                        new KeySet().setKeyColumn("NAME", "restRecord" + id),
                        Options.builder().withOption(Options.Name.INDEX_HINT, "REVIEWER_NAME_IDX").build())) {
                    ResultSetAssert.assertThat(relationalResultSet).isEmpty();
                }

                /*
                 * now try to scan the entirety of each table and validate the results
                 * we can have data from multiple test runs contaminating this scan, because it's unbounded. This is
                 * actually OK, because what we really care about is that the scan doesn't return data from
                 * other tables. So all we do here is check the returned message type
                 */
                try (final RelationalResultSet recordScan = s.executeScan("RESTAURANT", new KeySet(), Options.NONE)) {
                    Assertions.assertNotNull(recordScan, "Did not return a valid result set!");

                    while (recordScan.next()) {
                        Assertions.assertDoesNotThrow(() -> {
                            //make sure that the correct fields are returned
                            recordScan.getString("NAME");
                            recordScan.getLong("REST_NO");
                        });
                        org.assertj.core.api.Assertions.assertThatThrownBy(() -> recordScan.getLong("id"))
                                .isInstanceOf(SQLException.class)
                                .extracting("SQLState")
                                .isEqualTo(ErrorCode.INVALID_COLUMN_REFERENCE.getErrorCode());
                    }
                }

                try (final RelationalResultSet reviewerScan = s.executeScan("RESTAURANT_REVIEWER", new KeySet(), Options.NONE)) {
                    Assertions.assertNotNull(reviewerScan, "Did not return a valid result set!");
                    while (reviewerScan.next()) {
                        Assertions.assertDoesNotThrow(() -> {
                            //make sure that the correct fields are returned
                            reviewerScan.getString("NAME");
                            reviewerScan.getLong("ID");
                        });

                        org.assertj.core.api.Assertions.assertThatThrownBy(() -> reviewerScan.getLong("REST_NO"))
                                .isInstanceOf(SQLException.class)
                                .extracting("SQLState")
                                .isEqualTo(ErrorCode.INVALID_COLUMN_REFERENCE.getErrorCode());
                    }
                }
            }
        }
    }

    @Test
    void cannotInsertWithIncorrectTypeForRecord() throws SQLException {
        /*
         * We want to make sure that we don't accidentally pick up data from different tables
         */
        try (RelationalConnection conn = DriverManager.getConnection(database.getConnectionUri().toString()).unwrap(RelationalConnection.class)) {
            conn.setSchema("TEST_SCHEMA");
            try (RelationalStatement s = conn.createStatement()) {
                long id = System.currentTimeMillis();
                final var record = EmbeddedRelationalStruct.newBuilder().addLong("REST_NO", id).addString("NAME", "restRecord" + id).build();
                RelationalAssertions.assertThrowsSqlException(
                        () -> s.executeInsert("RESTAURANT_REVIEWER", record))
                        .hasErrorCode(ErrorCode.INVALID_PARAMETER);
            }
        }
    }

    @Test
    void canDifferentiateNullAndDefaultValue() throws SQLException {
        try (RelationalConnection conn = DriverManager.getConnection(database.getConnectionUri().toString()).unwrap(RelationalConnection.class)) {
            conn.setSchema("TEST_SCHEMA");
            try (RelationalStatement s = conn.createStatement()) {
                long id = System.currentTimeMillis();
                // string type field NAME is unset
                final var restaurant1 = EmbeddedRelationalStruct.newBuilder()
                        .addLong("REST_NO", id)
                        .build();
                int inserted1 = s.executeInsert("RESTAURANT", restaurant1);
                Assertions.assertEquals(1, inserted1, "Did not insert properly!");
                // string type field NAME is set to empty string
                final var restaurant2 = EmbeddedRelationalStruct.newBuilder()
                        .addLong("REST_NO", id + 1)
                        .addString("NAME", "")
                        .build();
                int inserted2 = s.executeInsert("RESTAURANT", restaurant2);
                Assertions.assertEquals(1, inserted2, "Did not insert properly!");

                //now prove we can get them back out
                try (RelationalResultSet relationalResultSet = s.executeGet("RESTAURANT", new KeySet().setKeyColumns(Map.of("REST_NO", id)), Options.NONE)) {
                    ResultSetAssert.assertThat(relationalResultSet).hasNextRow()
                            .hasColumns(Map.of("REST_NO", id))
                            .hasNoNextRow();
                }

                try (RelationalResultSet relationalResultSet = s.executeGet("RESTAURANT", new KeySet().setKeyColumns(Map.of("REST_NO", id + 1)), Options.NONE)) {
                    ResultSetAssert.assertThat(relationalResultSet).hasNextRow()
                            .hasColumns(Map.of("REST_NO", id + 1, "NAME", ""));
                }
            }
        }
    }

    @Test
    void cannotInsertDuplicatePrimaryKey() throws SQLException {
        try (RelationalConnection conn = DriverManager.getConnection(database.getConnectionUri().toString()).unwrap(RelationalConnection.class)) {
            conn.setSchema("TEST_SCHEMA");
            try (RelationalStatement s = conn.createStatement()) {
                final var struct = EmbeddedRelationalStruct.newBuilder().addLong("REST_NO", 0).build();
                s.executeInsert("RESTAURANT", struct);
                RelationalAssertions.assertThrowsSqlException(() -> s.executeInsert("RESTAURANT", struct))
                        .hasErrorCode(ErrorCode.UNIQUE_CONSTRAINT_VIOLATION);
            }
        }
    }

    @Test
    void canNotAddNullElementToArray() {
        final var builder = EmbeddedRelationalArray.newBuilder();
        final var throwAssert = Assertions.assertThrows(SQLException.class, () -> builder.addAll(1, 2, 3, null, 5, 6));
        Assertions.assertTrue(throwAssert.getMessage().contains("Cannot add NULL to an array"));
    }

    @Test
    void canInsertNullableArray() throws SQLException, RelationalException {
        final var itemMetadata = new RelationalStructMetaData(
                FieldDescription.primitive("NAME", Types.VARCHAR, DatabaseMetaData.columnNullable),
                FieldDescription.primitive("PRICE", Types.FLOAT, DatabaseMetaData.columnNullable)
        );
        final var itemsMetadata = RelationalArrayMetaData.ofStruct(itemMetadata, DatabaseMetaData.columnNoNulls);
        try (RelationalConnection conn = DriverManager.getConnection(database.getConnectionUri().toString()).unwrap(RelationalConnection.class)) {
            conn.setSchema("TEST_SCHEMA");
            try (RelationalStatement s = conn.createStatement()) {

                // with nullable array as null
                var restMenu = EmbeddedRelationalStruct.newBuilder()
                        .addLong("ID", 1L)
                        .addLong("REST_NO", 23L)
                        .addObject("CUISINE", "japanese", Types.OTHER)
                        .addArray("ITEMS", EmbeddedRelationalArray.newBuilder()
                                .addStruct(EmbeddedRelationalStruct.newBuilder()
                                        .addString("NAME", "katsu curry")
                                        .addFloat("PRICE", 8.95f)
                                        .build())
                                .addStruct(EmbeddedRelationalStruct.newBuilder()
                                        .addString("NAME", "karaage chicken")
                                        .addFloat("PRICE", 7.5f)
                                        .build())
                                .build())
                        .build();
                s.executeInsert("RESTAURANT_MENU", restMenu, Options.NONE);
                //now prove we can get them back out
                try (RelationalResultSet relationalResultSet = s.executeGet("RESTAURANT_MENU", new KeySet().setKeyColumns(Map.of("ID", 1L, "REST_NO", 23L)), Options.NONE)) {
                    ResultSetAssert.assertThat(relationalResultSet)
                            .hasNextRow()
                            .hasColumn("ID", restMenu.getLong(1))
                            .hasColumn("REST_NO", restMenu.getLong(2))
                            .hasColumn("CUISINE", restMenu.getString(3))
                            .hasColumn("ITEMS", restMenu.getArray(4))
                            .hasColumn("REVIEWS", null)
                            .hasNoNextRow();
                }

                // with non nullable array as empty
                restMenu = EmbeddedRelationalStruct.newBuilder()
                        .addLong("ID", 2L)
                        .addLong("REST_NO", 23L)
                        .addObject("CUISINE", "japanese", Types.OTHER)
                        .build();
                s.executeInsert("RESTAURANT_MENU", restMenu, Options.NONE);

                //now prove we can get them back out
                try (RelationalResultSet relationalResultSet = s.executeGet("RESTAURANT_MENU", new KeySet().setKeyColumns(Map.of("ID", 2L, "REST_NO", 23L)), Options.NONE)) {
                    ResultSetAssert.assertThat(relationalResultSet)
                            .hasNextRow()
                            .hasColumn("ID", restMenu.getLong(1))
                            .hasColumn("REST_NO", restMenu.getLong(2))
                            .hasColumn("CUISINE", restMenu.getString(3))
                            .hasColumn("ITEMS", EmbeddedRelationalArray.newBuilder(itemsMetadata).build())
                            .hasColumn("REVIEWS", null)
                            .hasNoNextRow();
                }
            }
        }
    }

    @Test
    void replaceOnInsert() throws SQLException, RelationalException {
        try (RelationalConnection conn = DriverManager.getConnection(database.getConnectionUri().toString()).unwrap(RelationalConnection.class)) {
            conn.setSchema("TEST_SCHEMA");
            try (RelationalStatement s = conn.createStatement()) {
                RelationalStruct record = EmbeddedRelationalStruct.newBuilder()
                        .addLong("REST_NO", 0)
                        .addString("NAME", "before")
                        .addArray("CUSTOMER", EmbeddedRelationalArray.newBuilder()
                                .addString("cust1")
                                .build())
                        .build();
                s.executeInsert("RESTAURANT", record);
                record = EmbeddedRelationalStruct.newBuilder()
                        .addLong("REST_NO", 0)
                        .addString("NAME", "after")
                        .build();
                s.executeInsert("RESTAURANT", record, Options.builder().withOption(Options.Name.REPLACE_ON_DUPLICATE_PK, true).build());
                try (RelationalResultSet rs = s.executeGet("RESTAURANT", new KeySet().setKeyColumn("REST_NO", 0), Options.NONE)) {
                    ResultSetAssert.assertThat(rs)
                            .hasNextRow()
                            .hasColumn("NAME", "after")
                            .hasNoNextRow();
                }
            }
        }
    }

    @Test
    void replaceOnFirstInsert() throws SQLException {
        try (RelationalConnection conn = DriverManager.getConnection(database.getConnectionUri().toString()).unwrap(RelationalConnection.class)) {
            conn.setSchema("TEST_SCHEMA");
            try (RelationalStatement s = conn.createStatement()) {
                RelationalStruct record = EmbeddedRelationalStruct.newBuilder().addLong("REST_NO", 0).build();
                s.executeInsert("RESTAURANT", record, Options.builder().withOption(Options.Name.REPLACE_ON_DUPLICATE_PK, true).build());
                try (RelationalResultSet rs = s.executeGet("RESTAURANT", new KeySet().setKeyColumn("REST_NO", 0), Options.NONE)) {
                    ResultSetAssert.assertThat(rs)
                            .hasNextRow()
                            .hasColumn("REST_NO", 0L)
                            .hasNoNextRow();
                }
            }
        }
    }
}
