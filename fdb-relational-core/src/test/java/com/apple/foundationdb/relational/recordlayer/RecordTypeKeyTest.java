/*
 * RecordTypeKeyTest.java
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
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.relational.api.KeySet;
import com.apple.foundationdb.relational.api.OperationOption;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.TableScan;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;

import com.google.protobuf.Message;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.sql.SQLException;
import java.util.Collections;

public class RecordTypeKeyTest {
    @RegisterExtension
    @Order(0)
    public final RecordLayerCatalogRule catalog = new RecordLayerCatalogRule();

    @RegisterExtension
    @Order(1)
    public final RecordLayerTemplateRule template = new RecordLayerTemplateRule("Restaurant", catalog)
            .setRecordFile(Restaurant.getDescriptor())
            .configureTable("RestaurantRecord", table -> {
                table.setRecordTypeKey(0);
                table.setPrimaryKey(Key.Expressions.recordType());
            })
            .addIndex("RestaurantRecord",
                    new Index("record_rt_covering_idx", Key.Expressions.keyWithValue(
                            Key.Expressions.concat(Key.Expressions.recordType(), Key.Expressions.field("rest_no"), Key.Expressions.field("name")), 2), IndexTypes.VALUE))
            .configureTable("RestaurantReviewer", table -> {
                table.setRecordTypeKey(1);
                table.setPrimaryKey(Key.Expressions.concat(Key.Expressions.recordType(), Key.Expressions.field("id")));
            });

    @RegisterExtension
    @Order(2)
    public final DatabaseRule database = new DatabaseRule("type_key_db", catalog)
            .withSchema("main", template);

    @RegisterExtension
    @Order(3)
    public final RelationalConnectionRule connection = new RelationalConnectionRule(database)
            .withOptions(Options.create().withOption(OperationOption.forceVerifyDdl()))
            .withSchema("main");

    @RegisterExtension
    @Order(4)
    public final RelationalStatementRule statement = new RelationalStatementRule(connection);

    @Test
    void testPrimaryKeyWithOnlyRecordTypeKey() throws RelationalException, SQLException {
        long id = System.currentTimeMillis();
        Restaurant.RestaurantRecord record = Restaurant.RestaurantRecord.newBuilder().setRestNo(id).setName("Awesome Burgers").addCustomer("Scott").build();
        int count = statement.executeInsert("RestaurantRecord", Collections.singleton(record), Options.create());
        Assertions.assertEquals(1, count, "Incorrect returned insertion count");

        Restaurant.RestaurantReviewer reviewer = Restaurant.RestaurantReviewer.newBuilder().setId(id + 1).setName("review").build();
        count = statement.executeInsert("RestaurantReviewer", Collections.singleton(reviewer), Options.create());
        Assertions.assertEquals(1, count, "Incorrect returned insertion count");

        // Only scan the "RestaurantRecord" table
        TableScan scan = TableScan.newBuilder()
                .withTableName("RestaurantRecord")
                .build();
        try (final RelationalResultSet resultSet = statement.executeScan(scan, Options.create())) {
            // Only 1 RestaurantRecord is expected to be returned
            Assertions.assertNotNull(resultSet, "No result set returned!");
            Assertions.assertTrue(resultSet.next(), "No records returned from scanning");
            Assertions.assertTrue(resultSet.supportsMessageParsing(), "Does not support message parsing!");
            Message m = resultSet.parseMessage();
            Assertions.assertEquals(record, m, "Did not return the correct message!");
            Assertions.assertFalse(resultSet.next(), "More than 1 record returned from scanning, which is unexpected");
        }
    }

    @Test
    void testScanningWithUnknownKeys() throws RelationalException, SQLException {
        long id = System.currentTimeMillis();
        Restaurant.RestaurantRecord record = Restaurant.RestaurantRecord.newBuilder().setRestNo(id).setName("Awesome Burgers").addCustomer("Scott").build();
        int count = statement.executeInsert("RestaurantRecord", Collections.singleton(record), Options.create());
        Assertions.assertEquals(1, count, "Incorrect returned insertion count");

        TableScan scan = TableScan.newBuilder()
                .withTableName("RestaurantRecord")
                .setStartKey("rest_no", id)
                .setEndKey("rest_no", id + 1)
                .build();
        // Scan is expected to rejected because it uses fields which are not included in primary key
        RelationalException exception = Assertions.assertThrows(RelationalException.class, () -> statement.executeScan(scan, Options.create()));
        Assertions.assertEquals("Unknown keys for primary key of <RestaurantRecord>, unknown keys: <REST_NO>", exception.getMessage());
        Assertions.assertEquals(ErrorCode.INVALID_PARAMETER, exception.getErrorCode());
    }

    @Test
    void canGetWithRecordTypeInPrimaryKey() throws RelationalException, SQLException {
        long id = System.currentTimeMillis();
        Restaurant.RestaurantReviewer reviewer = Restaurant.RestaurantReviewer.newBuilder().setId(id).setName("review_1").build();
        int count = statement.executeInsert("RestaurantReviewer", Collections.singleton(reviewer), Options.create());
        Assertions.assertEquals(1, count, "Incorrect returned insertion count");

        try (final RelationalResultSet rrs = statement.executeGet("RestaurantReviewer", new KeySet().setKeyColumn("id", id), Options.create())) {
            Assertions.assertTrue(rrs.next(), "Did not return a record from a GET");
            //this should return the full protobuf, so it should support message parsing
            Assertions.assertTrue(rrs.supportsMessageParsing(), "Does not support message parsing!");
            Message m = rrs.parseMessage();
            Assertions.assertEquals(reviewer, m, "Did not return the correct message!");
        }
    }

    @Test
    void canGetWithRecordTypeKeyIndex() throws RelationalException, SQLException {
        long id = System.currentTimeMillis();
        Restaurant.RestaurantRecord record = Restaurant.RestaurantRecord.newBuilder().setRestNo(id).setName("Awesome Burgers").addCustomer("Scott").build();
        int count = statement.executeInsert("RestaurantRecord", Collections.singleton(record), Options.create());
        Assertions.assertEquals(1, count, "Incorrect returned insertion count");

        try (final RelationalResultSet rrs = statement.executeGet("RestaurantRecord",
                new KeySet().setKeyColumn("rest_no", id),
                Options.create().withOption(OperationOption.index("record_rt_covering_idx")))) {
            Assertions.assertTrue(rrs.next(), "Did not return a record from a GET");
            //this should be doing an index fetch, so it's not the full protobuf
            if (rrs.supportsMessageParsing()) {
                Message m = rrs.parseMessage();
                Assertions.assertEquals(record, m, "Did not return the correct message!");
            } else {
                //match the records returned
                Assertions.assertEquals(record.getName(), rrs.getString("name"), "Incorrect returned name!");
                Assertions.assertEquals(record.getRestNo(), rrs.getLong("rest_no"), "Incorrect returned Rest no!");
            }
        }
    }
}
