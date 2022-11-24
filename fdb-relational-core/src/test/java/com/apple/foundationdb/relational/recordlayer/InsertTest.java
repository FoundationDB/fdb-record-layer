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

import com.apple.foundationdb.relational.api.KeySet;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.Relational;
import com.apple.foundationdb.relational.api.RelationalConnection;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.RelationalStatement;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.utils.ResultSetAssert;
import com.apple.foundationdb.relational.utils.SimpleDatabaseRule;
import com.apple.foundationdb.relational.utils.TestSchemas;
import com.apple.foundationdb.relational.utils.RelationalAssertions;

import com.google.protobuf.Message;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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
        try (RelationalConnection conn = Relational.connect(database.getConnectionUri(), Options.NONE)) {
            conn.setSchema("TEST_SCHEMA");
            conn.beginTransaction();
            try (RelationalStatement s = conn.createStatement()) {
                long id = System.currentTimeMillis();
                Message restaurant = s.getDataBuilder("RESTAURANT")
                        .setField("REST_NO", id)
                        .setField("NAME", "restRecord" + id)
                        .build();
                int inserted = s.executeInsert("RESTAURANT", restaurant);
                Assertions.assertEquals(1, inserted, "Did not insert properly!");

                Message reviewer = s.getDataBuilder("RESTAURANT_REVIEWER")
                        .setField("NAME", "reviewerName" + id)
                        .setField("ID", id - 1)
                        .build();
                inserted = s.executeInsert("RESTAURANT_REVIEWER", reviewer);
                Assertions.assertEquals(1, inserted, "Did not insert reviewers properly!");

                //now prove we can get them back out
                try (RelationalResultSet relationalResultSet = s.executeGet("RESTAURANT", new KeySet().setKeyColumns(Map.of("REST_NO", id)), Options.NONE)) {
                    ResultSetAssert.assertThat(relationalResultSet).hasNextRow()
                            .hasRow(Map.of("NAME", "restRecord" + id, "REST_NO", id))
                            .hasNoNextRow();
                }

                try (RelationalResultSet relationalResultSet = s.executeGet("RESTAURANT_REVIEWER", new KeySet().setKeyColumn("ID", id - 1), Options.NONE)) {
                    ResultSetAssert.assertThat(relationalResultSet).hasNextRow()
                            .hasRow(Map.of("NAME", "reviewerName" + id, "ID", id - 1))
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
    void cannotInsertWithIncorrectTypeForRecord() throws RelationalException, SQLException {
        /*
         * We want to make sure that we don't accidentally pick up data from different tables
         */
        try (RelationalConnection conn = Relational.connect(database.getConnectionUri(), Options.NONE)) {
            conn.setSchema("TEST_SCHEMA");
            conn.beginTransaction();
            try (RelationalStatement s = conn.createStatement()) {
                long id = System.currentTimeMillis();
                Message record = s.getDataBuilder("RESTAURANT").setField("REST_NO", id).setField("NAME", "restRecord" + id).build();
                RelationalAssertions.assertThrowsSqlException(
                        () -> s.executeInsert("RESTAURANT_REVIEWER", record))
                        .hasErrorCode(ErrorCode.INVALID_PARAMETER);
            }
        }
    }

    @Test
    void canDifferentiateNullAndDefaultValue() throws RelationalException, SQLException {
        try (RelationalConnection conn = Relational.connect(database.getConnectionUri(), Options.NONE)) {
            conn.setSchema("TEST_SCHEMA");
            conn.beginTransaction();
            try (RelationalStatement s = conn.createStatement()) {
                long id = System.currentTimeMillis();
                // string type field NAME is unset
                Message restaurant1 = s.getDataBuilder("RESTAURANT")
                        .setField("REST_NO", id)
                        .build();
                int inserted1 = s.executeInsert("RESTAURANT", restaurant1);
                Assertions.assertEquals(1, inserted1, "Did not insert properly!");
                // string type field NAME is set to empty string
                Message restaurant2 = s.getDataBuilder("RESTAURANT")
                        .setField("REST_NO", id + 1)
                        .setField("NAME", "")
                        .build();
                int inserted2 = s.executeInsert("RESTAURANT", restaurant2);
                Assertions.assertEquals(1, inserted2, "Did not insert properly!");

                //now prove we can get them back out
                try (RelationalResultSet relationalResultSet = s.executeGet("RESTAURANT", new KeySet().setKeyColumns(Map.of("REST_NO", id)), Options.NONE)) {
                    ResultSetAssert.assertThat(relationalResultSet).hasNextRow()
                            .hasRow(Map.of("REST_NO", id))
                            .hasNoNextRow();
                }

                try (RelationalResultSet relationalResultSet = s.executeGet("RESTAURANT", new KeySet().setKeyColumns(Map.of("REST_NO", id + 1)), Options.NONE)) {
                    ResultSetAssert.assertThat(relationalResultSet).hasNextRow()
                            .hasRow(Map.of("REST_NO", id + 1, "NAME", ""));
                }
            }
        }
    }

    @Test
    void cannotInsertDuplicatePrimaryKey() throws SQLException, RelationalException {
        try (RelationalConnection conn = Relational.connect(database.getConnectionUri(), Options.NONE)) {
            conn.setSchema("TEST_SCHEMA");
            conn.beginTransaction();
            try (RelationalStatement s = conn.createStatement()) {
                Message record = s.getDataBuilder("RESTAURANT").setField("REST_NO", 0).build();
                s.executeInsert("RESTAURANT", record);
                RelationalAssertions.assertThrowsSqlException(() -> s.executeInsert("RESTAURANT", record))
                        .hasErrorCode(ErrorCode.UNIQUE_CONSTRAINT_VIOLATION);
            }
        }
    }

    @Test
    void canNotAddNullElementToArray() throws SQLException, RelationalException {
        try (RelationalConnection conn = Relational.connect(database.getConnectionUri(), Options.NONE)) {
            conn.setSchema("TEST_SCHEMA");
            conn.beginTransaction();
            List<String> customers = new ArrayList<>();
            customers.add("A");
            customers.add(null);
            try (RelationalStatement s = conn.createStatement()) {
                RelationalAssertions.assertThrowsSqlException(() -> s.getDataBuilder("RESTAURANT")
                        .setField("REST_NO", 0)
                        .addRepeatedFields("CUSTOMER", customers)
                        .build()).hasErrorCode(ErrorCode.NOT_NULL_VIOLATION);
            }
        }
    }

    @Test
    void replaceOnInsert() throws SQLException, RelationalException {
        try (RelationalConnection conn = Relational.connect(database.getConnectionUri(), Options.NONE)) {
            conn.setSchema("TEST_SCHEMA");
            conn.beginTransaction();
            try (RelationalStatement s = conn.createStatement()) {
                Message record = s.getDataBuilder("RESTAURANT").setField("REST_NO", 0).setField("NAME", "before").addRepeatedFields("CUSTOMER", List.of("cust1")).build();
                s.executeInsert("RESTAURANT", record);
                record = s.getDataBuilder("RESTAURANT").setField("REST_NO", 0).setField("NAME", "after").build();
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
    void replaceOnFirstInsert() throws SQLException, RelationalException {
        try (RelationalConnection conn = Relational.connect(database.getConnectionUri(), Options.NONE)) {
            conn.setSchema("TEST_SCHEMA");
            conn.beginTransaction();
            try (RelationalStatement s = conn.createStatement()) {
                Message record = s.getDataBuilder("RESTAURANT").setField("REST_NO", 0).build();
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
