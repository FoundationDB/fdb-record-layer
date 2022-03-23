/*
 * RecordLayerTableTest.java
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
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.relational.api.KeySet;
import com.apple.foundationdb.relational.api.OperationOption;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.TableScan;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.RelationalStatement;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;

import com.google.common.collect.Iterators;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Basic integration tests outlining the full process of using Relational to insert and scan records from a database.
 */
public class RecordLayerTableTest {
    @RegisterExtension
    @Order(0)
    public final RecordLayerCatalogRule catalog = new RecordLayerCatalogRule();

    @RegisterExtension
    @Order(1)
    public final RecordLayerTemplateRule template = new RecordLayerTemplateRule("RestaurantRecord", catalog)
            .setRecordFile(Restaurant.getDescriptor())
            .configureTable("RestaurantRecord", table -> table.setPrimaryKey(Key.Expressions.field("rest_no")));

    @RegisterExtension
    @Order(2)
    public final DatabaseRule database = new DatabaseRule("record_layer_table_test", catalog)
            .withSchema("test", template);

    @RegisterExtension
    @Order(3)
    public final RelationalConnectionRule connection = new RelationalConnectionRule(database)
            .withOptions(Options.create().withOption(OperationOption.forceVerifyDdl()))
            .withSchema("test");

    @RegisterExtension
    @Order(4)
    public final RelationalStatementRule statement = new RelationalStatementRule(connection);

    private final AtomicLong restNo = new AtomicLong(System.currentTimeMillis());

    @Test
    void canInsertAndGetASingleRecord() throws RelationalException, SQLException {
        Restaurant.RestaurantRecord inserted = insertRestaurantRecord(statement);

        KeySet keys = new KeySet()
                .setKeyColumn("rest_no", inserted.getRestNo());

        final RelationalResultSet resultSet = statement.executeGet("RestaurantRecord", keys, Options.create());
        assertMatches(resultSet, inserted);
    }

    @Test
    void canDeleteASingleRecord() throws RelationalException, SQLException {
        Restaurant.RestaurantRecord inserted = insertRestaurantRecord(statement);

        KeySet keys = new KeySet().setKeyColumn("rest_no", inserted.getRestNo());

        RelationalResultSet resultSet = statement.executeGet("RestaurantRecord", keys, Options.create());
        assertMatches(resultSet, inserted);

        // Now delete the record and check again
        int recordsDeleted = statement.executeDelete("RestaurantRecord", Collections.singleton(keys), Options.create());
        Assertions.assertEquals(1, recordsDeleted, "Incorrect number of records deletes");

        // Now it shouldn't be there
        resultSet = statement.executeGet("RestaurantRecord", keys, Options.create());
        Assertions.assertNotNull(resultSet, "No result set returned!");
        Assertions.assertFalse(resultSet.next(), "No records returned!");
    }

    @Test
    void deleteNoRecord() throws RelationalException, SQLException {
        Restaurant.RestaurantRecord inserted = insertRestaurantRecord(statement);

        KeySet keys = new KeySet().setKeyColumn("rest_no", inserted.getRestNo());

        RelationalResultSet resultSet = statement.executeGet("RestaurantRecord", keys, Options.create());
        assertMatches(resultSet, inserted);

        // Pretend to delete the record
        int recordsDeleted = statement.executeDelete("RestaurantRecord", List.of(), Options.create());
        Assertions.assertEquals(0, recordsDeleted, "Incorrect number of records deletes");

        // It's still there
        resultSet = statement.executeGet("RestaurantRecord", keys, Options.create());
        assertMatches(resultSet, inserted);
    }

    @Test
    void canDeleteMultipleRecord() throws RelationalException, SQLException {
        Restaurant.RestaurantRecord[] inserted = {insertRestaurantRecord(statement), insertRestaurantRecord(statement)};
        List<KeySet> keys = new ArrayList<>();
        for (Restaurant.RestaurantRecord record : inserted) {
            keys.add(new KeySet().setKeyColumn("rest_no", record.getRestNo()));
            RelationalResultSet resultSet = statement.executeGet("RestaurantRecord", keys.get(keys.size() - 1), Options.create());
            assertMatches(resultSet, record);
        }

        //now delete the record and check again
        final int recordsDeleted = statement.executeDelete("RestaurantRecord", keys, Options.create());
        Assertions.assertEquals(2, recordsDeleted, "Incorrect number of records deletes");

        //now it shouldn't be there
        for (KeySet key : keys) {
            RelationalResultSet resultSet = statement.executeGet("RestaurantRecord", key, Options.create());
            Assertions.assertNotNull(resultSet, "No result set returned!");
            Assertions.assertFalse(resultSet.next(), "No records returned!");
        }
    }

    @Test
    void canInsertAndScanASingleRecordFromIndex() throws Exception {
        Restaurant.RestaurantRecord r = insertRestaurantRecord(statement);

        KeySet keys = new KeySet().setKeyColumn("name", r.getName());

        final RelationalResultSet resultSet = statement.executeGet("RestaurantRecord", keys, Options.create().withOption(OperationOption.index("RestaurantRecord$name")));
        assertMatches(resultSet, r);
    }

    @Test
    void wrongIndex() throws Exception {
        Restaurant.RestaurantRecord r = insertRestaurantRecord(statement);

        KeySet keys = new KeySet().setKeyColumn("name", r.getName());

        assertThatThrownBy(() -> statement.executeGet("RestaurantRecord", keys, Options.create().withOption(OperationOption.index("RestaurantRecord$NOT_AN_INDEX"))))
                .isInstanceOf(RelationalException.class)
                .extracting("errorCode")
                .isEqualTo(ErrorCode.UNKNOWN_INDEX);
    }

    @Test
    void canInsertAndScanASingleRecord() throws Exception {
        try {
            Restaurant.RestaurantRecord r = insertRestaurantRecord(statement);

            TableScan scan = TableScan.newBuilder()
                    .withTableName("RestaurantRecord")
                    .setStartKey("rest_no", r.getRestNo())
                    .setEndKey("rest_no", r.getRestNo() + 1)
                    .build();
            final RelationalResultSet resultSet = statement.executeScan(scan, Options.create());
            assertMatches(resultSet, r);
        } catch (Throwable t) {
            try {
                connection.rollback();
            } catch (Throwable suppressable) {
                t.addSuppressed(suppressable);
            }
            throw t;
        }
    }

    @Test
    void get() throws RelationalException, SQLException {
        Restaurant.RestaurantRecord r = insertRestaurantRecord(statement);

        //verify that it's in there with a nice GET against the key
        KeySet keys = new KeySet()
                .setKeyColumn("rest_no", r.getRestNo());

        try (RelationalResultSet resultSet = statement.executeGet("RestaurantRecord", keys, Options.create())) {
            assertMatches(resultSet, r); //make sure that the record is there
        }
    }

    @Test
    void getViaIndex() throws RelationalException, SQLException {
        Restaurant.RestaurantRecord r = insertRestaurantRecord(statement);

        //get via index
        KeySet keys = new KeySet()
                .setKeyColumn("name", r.getName());
        try (RelationalResultSet resultSet = statement.executeGet("RestaurantRecord", keys, Options.create().withOption(OperationOption.index("RestaurantRecord$name")))) {
            assertMatches(resultSet, r); //make sure that the record is there
        }
    }

    @Test
    void scanPrimaryKey() throws RelationalException, SQLException {
        Restaurant.RestaurantRecord r = insertRestaurantRecord(statement);

        //scan on the primary key
        TableScan scan = TableScan.newBuilder()
                .withTableName("RestaurantRecord")
                .setStartKey("rest_no", r.getRestNo())
                .setEndKey("rest_no", r.getRestNo() + 1)
                .build();
        try (RelationalResultSet resultSet = statement.executeScan(scan, Options.create())) {
            assertMatches(resultSet, r);
        }
    }

    @Test
    void scanIndex() throws RelationalException, SQLException {
        Restaurant.RestaurantRecord r = insertRestaurantRecord(statement);

        //scan on the index
        TableScan scan = TableScan.newBuilder()
                .withTableName("RestaurantRecord")
                .setStartKey("name", r.getName())
                .setEndKey("name", r.getName() + 1)
                .build();
        try (RelationalResultSet resultSet = statement.executeScan(scan, Options.create().withOption(OperationOption.index("RestaurantRecord$name")))) {
            assertMatches(resultSet, r);
        }
    }

    @Test
    void delete() throws RelationalException, SQLException {
        Restaurant.RestaurantRecord r = insertRestaurantRecord(statement);

        //now delete the record
        KeySet keys = new KeySet()
                .setKeyColumn("rest_no", r.getRestNo());
        int numDeleted = statement.executeDelete("RestaurantRecord", Collections.singleton(keys), Options.create());
        Assertions.assertEquals(1, numDeleted, "Incorrect number of keys deleted");

        //now see that it's not there any more

        try (RelationalResultSet resultSet = statement.executeGet("RestaurantRecord", keys, Options.create())) {
            Assertions.assertFalse(resultSet.next(), "Incorrectly returned a row!");
        }
    }

    private void assertMatches(RelationalResultSet resultSet, Restaurant.RestaurantRecord...r) throws SQLException {
        Assertions.assertNotNull(resultSet, "No result set returned!");
        //TODO(bfines) do Set operations here instead
        for (Restaurant.RestaurantRecord record : r) {
            Assertions.assertTrue(resultSet.next(), "No records returned!");
            if (resultSet.supportsMessageParsing()) {
                Message m = resultSet.parseMessage();
                Assertions.assertEquals(Restaurant.RestaurantRecord.getDescriptor(), m.getDescriptorForType(), "Incorrect message type returned");
                final Object nameField = m.getField(Restaurant.RestaurantRecord.getDescriptor().findFieldByName("name"));
                Assertions.assertEquals(record.getName(), nameField, "Incorrect restaurant record returned!");
                Assertions.assertEquals(record, m, "Incorrect message returned");
            } else {
                Assertions.assertEquals(record.getName(), resultSet.getString("name"), "Incorrect name!");
            }
        }
    }

    private Restaurant.RestaurantRecord insertRestaurantRecord(RelationalStatement s) throws RelationalException {
        long id = restNo.incrementAndGet();
        Restaurant.RestaurantRecord r = Restaurant.RestaurantRecord.newBuilder().setName("testRest" + id).setRestNo(id).build();
        int insertCount = s.executeInsert("RestaurantRecord", Iterators.singletonIterator(r), Options.create());
        Assertions.assertEquals(1, insertCount, "Did not count insertions correctly!");
        return r;
    }
}
