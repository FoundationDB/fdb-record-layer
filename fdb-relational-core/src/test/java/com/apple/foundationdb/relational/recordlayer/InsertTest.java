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

import com.apple.foundationdb.record.Restaurant;
import com.apple.foundationdb.relational.api.KeySet;
import com.apple.foundationdb.relational.api.OperationOption;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.TableScan;
import com.apple.foundationdb.relational.api.Relational;
import com.apple.foundationdb.relational.api.RelationalConnection;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.RelationalStatement;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.utils.SimpleDatabaseRule;
import com.apple.foundationdb.relational.utils.TestSchemas;
import com.apple.foundationdb.relational.utils.RelationalAssertions;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.sql.SQLException;
import java.util.Collections;
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
        try (RelationalConnection conn = Relational.connect(database.getConnectionUri(), Options.create())) {
            conn.setSchema("testSchema");
            conn.beginTransaction();
            try (RelationalStatement s = conn.createStatement()) {
                long id = System.currentTimeMillis();
                Restaurant.RestaurantRecord record = Restaurant.RestaurantRecord.newBuilder().setRestNo(id).setName("restRecord" + id).build();
                int inserted = s.executeInsert("RestaurantRecord", s.getDataBuilder("RestaurantRecord").convertMessage(record), Options.create());
                Assertions.assertEquals(1, inserted, "Did not insert properly!");

                Restaurant.RestaurantReviewer reviewer = Restaurant.RestaurantReviewer.newBuilder().setName("reviewerName" + id).setId(id - 1).build();
                inserted = s.executeInsert("RestaurantReviewer", s.getDataBuilder("RestaurantReviewer").convertMessage(reviewer), Options.create());
                Assertions.assertEquals(1, inserted, "Did not insert reviewers properly!");

                //now prove we can get them back out
                try (RelationalResultSet relationalResultSet = s.executeGet("RestaurantRecord", new KeySet().setKeyColumns(Map.of("rest_no", record.getRestNo())), Options.create())) {
                    RelationalAssertions.assertThat(relationalResultSet).hasExactly(Map.of("name", record.getName(), "rest_no", record.getRestNo()));
                }

                try (RelationalResultSet relationalResultSet = s.executeGet("RestaurantReviewer", new KeySet().setKeyColumn("id", reviewer.getId()), Options.create())) {
                    RelationalAssertions.assertThat(relationalResultSet).hasExactly(Map.of("name", reviewer.getName(), "id", reviewer.getId()));
                }

                //now make sure that they don't show up in the other table
                try (RelationalResultSet relationalResultSet = s.executeGet("RestaurantRecord",
                        new KeySet().setKeyColumn("name", reviewer.getName()),
                        Options.create().withOption(OperationOption.index("record_name_idx")))) {
                    RelationalAssertions.assertThat(relationalResultSet).hasNoNextRow();
                }

                try (RelationalResultSet relationalResultSet = s.executeGet("RestaurantReviewer",
                        new KeySet().setKeyColumn("name", record.getName()),
                        Options.create().withOption(OperationOption.index("reviewer_name_idx")))) {
                    RelationalAssertions.assertThat(relationalResultSet).hasNoNextRow();
                }

                /*
                 * now try to scan the entirety of each table and validate the results
                 * we can have data from multiple test runs contaminating this scan, because it's unbounded. This is
                 * actually OK, because wwhat we really care about is that the scan doesn't return data from
                 * other tables. So all we do here is check the returned message type
                 */
                try (final RelationalResultSet recordScan = s.executeScan(TableScan.newBuilder().withTableName("RestaurantRecord").build(), Options.create())) {
                    Assertions.assertNotNull(recordScan, "Did not return a valid result set!");

                    while (recordScan.next()) {
                        Assertions.assertDoesNotThrow(() -> {
                            //make sure that the correct fields are returned
                            recordScan.getString("name");
                            recordScan.getLong("rest_no");
                        });
                        RelationalAssertions.assertThrowsSqlException(() -> recordScan.getLong("id")).hasErrorCode(ErrorCode.INVALID_COLUMN_REFERENCE);
                    }
                }

                try (final RelationalResultSet reviewerScan = s.executeScan(TableScan.newBuilder().withTableName("RestaurantReviewer").build(), Options.create())) {
                    Assertions.assertNotNull(reviewerScan, "Did not return a valid result set!");
                    while (reviewerScan.next()) {
                        Assertions.assertDoesNotThrow(() -> {
                            //make sure that the correct fields are returned
                            reviewerScan.getString("name");
                            reviewerScan.getLong("id");
                        });

                        RelationalAssertions.assertThrowsSqlException(() -> reviewerScan.getLong("rest_no")).hasErrorCode(ErrorCode.INVALID_COLUMN_REFERENCE);
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
        try (RelationalConnection conn = Relational.connect(database.getConnectionUri(), Options.create())) {
            conn.setSchema("testSchema");
            conn.beginTransaction();
            try (RelationalStatement s = conn.createStatement()) {
                long id = System.currentTimeMillis();
                Restaurant.RestaurantRecord record = Restaurant.RestaurantRecord.newBuilder().setRestNo(id).setName("restRecord" + id).build();
                RelationalAssertions.assertThrows(
                        () -> s.executeInsert("RestaurantReviewer", Collections.singleton(record), Options.create()))
                        .hasErrorCode(ErrorCode.INVALID_PARAMETER);
            }
        }
    }

    @Test
    void cannotInsertWithMissingSchema() throws RelationalException, SQLException {
        /*
         * We want to make sure that we don't accidentally pick up data from different tables
         */
        try (RelationalConnection conn = Relational.connect(database.getConnectionUri(), Options.create())) {
            conn.setSchema("doesNotExist");
            conn.beginTransaction();
            try (RelationalStatement s = conn.createStatement()) {
                long id = System.currentTimeMillis();
                Restaurant.RestaurantRecord record = Restaurant.RestaurantRecord.newBuilder().setRestNo(id).setName("restRecord" + id).build();
                RelationalAssertions.assertThrows(() -> {
                    int inserted = s.executeInsert("RestaurantReviewer", Collections.singleton(record), Options.create());
                    Assertions.fail("did not throw an exception on insertion(inserted=" + inserted + ")");
                }).hasErrorCode(ErrorCode.SCHEMA_NOT_FOUND);
            }
        }
    }
}
