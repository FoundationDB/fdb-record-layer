/*
 * TableTest.java
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
import com.apple.foundationdb.relational.api.TableScan;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.RelationalStatement;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.utils.Ddl;
import com.apple.foundationdb.relational.utils.ResultSetAssert;
import com.apple.foundationdb.relational.utils.SimpleDatabaseRule;
import com.apple.foundationdb.relational.utils.TestSchemas;

import com.google.protobuf.Message;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;


/**
 * Basic integration tests outlining the full process of using Relational to insert and scan records from a database.
 */
public class TableTest {
    @RegisterExtension
    @Order(0)
    public final EmbeddedRelationalExtension relationalExtension = new EmbeddedRelationalExtension();

    @RegisterExtension
    @Order(1)
    public final SimpleDatabaseRule database = new SimpleDatabaseRule(relationalExtension, TableTest.class, TestSchemas.restaurantWithCoveringIndex());

    @RegisterExtension
    @Order(2)
    public final RelationalConnectionRule connection = new RelationalConnectionRule(database::getConnectionUri)
            .withOptions(Options.NONE)
            .withSchema("TEST_SCHEMA");

    @RegisterExtension
    @Order(3)
    public final RelationalStatementRule statement = new RelationalStatementRule(connection);

    @Test
    void canInsertAndGetASingleRecord() throws RelationalException, SQLException {
        long restNo = newRestNo();
        Message inserted = insertRestaurantRecord(statement, restNo);

        KeySet keys = new KeySet()
                .setKeyColumn("REST_NO", restNo);

        try (final RelationalResultSet resultSet = statement.executeGet("RESTAURANT", keys, Options.NONE)) {
            ResultSetAssert.assertThat(resultSet)
                    .hasNextRow()
                    .hasRow(inserted)
                    .hasNoNextRow();
        }
    }

    @Test
    void canDeleteASingleRecord() throws RelationalException, SQLException {
        long restNo = newRestNo();
        Message inserted = insertRestaurantRecord(statement, restNo);

        KeySet keys = new KeySet().setKeyColumn("REST_NO", restNo);

        try (RelationalResultSet resultSet = statement.executeGet("RESTAURANT", keys, Options.NONE)) {
            ResultSetAssert.assertThat(resultSet)
                    .hasNextRow()
                    .hasRow(inserted)
                    .hasNoNextRow();
        }

        // Now delete the record and check again
        int recordsDeleted = statement.executeDelete("RESTAURANT", Collections.singleton(keys));
        Assertions.assertThat(recordsDeleted).describedAs("Incorrect number of records deleted").isEqualTo(1);

        // Now it shouldn't be there
        try (RelationalResultSet resultSet = statement.executeGet("RESTAURANT", keys, Options.NONE)) {
            ResultSetAssert.assertThat(resultSet).isEmpty();
        }
    }

    @Test
    void deleteNoRecord() throws RelationalException, SQLException {
        long restNo = newRestNo();
        Message inserted = insertRestaurantRecord(statement, restNo);

        KeySet keys = new KeySet().setKeyColumn("REST_NO", restNo);

        try (RelationalResultSet resultSet = statement.executeGet("RESTAURANT", keys, Options.NONE)) {
            ResultSetAssert.assertThat(resultSet)
                    .hasNextRow()
                    .hasRow(inserted)
                    .hasNoNextRow();
        }

        // Pretend to delete the record
        int recordsDeleted = statement.executeDelete("RESTAURANT", List.of());
        Assertions.assertThat(recordsDeleted).describedAs("Incorrect number of records deleted").isEqualTo(0);

        // It's still there
        try (RelationalResultSet resultSet = statement.executeGet("RESTAURANT", keys, Options.NONE)) {
            ResultSetAssert.assertThat(resultSet)
                    .hasNextRow()
                    .hasRow(inserted)
                    .hasNoNextRow();
        }
    }

    @Test
    void canDeleteMultipleRecord() throws RelationalException, SQLException {
        long[] restNo = {newRestNo(), newRestNo()};
        Message[] inserted = {
                insertRestaurantRecord(statement, restNo[0]),
                insertRestaurantRecord(statement, restNo[1])
        };
        List<KeySet> keys = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            keys.add(new KeySet().setKeyColumn("REST_NO", restNo[i]));
            try (RelationalResultSet resultSet = statement.executeGet("RESTAURANT", keys.get(keys.size() - 1), Options.NONE)) {
                ResultSetAssert.assertThat(resultSet)
                        .hasNextRow()
                        .hasRow(inserted[i])
                        .hasNoNextRow();
            }
        }

        //now delete the record and check again
        final int recordsDeleted = statement.executeDelete("RESTAURANT", keys);
        Assertions.assertThat(recordsDeleted).describedAs("Incorrect number of records deleted").isEqualTo(2);

        //now it shouldn't be there
        for (KeySet key : keys) {
            try (RelationalResultSet resultSet = statement.executeGet("RESTAURANT", key, Options.NONE)) {
                ResultSetAssert.assertThat(resultSet).isEmpty();
            }
        }
    }

    @Test
    void canInsertAndScanASingleRecordFromIndex() throws Exception {
        long restNo = newRestNo();
        Message r = insertRestaurantRecord(statement, restNo);

        KeySet keys = new KeySet().setKeyColumn("name", restName(restNo));

        try (final RelationalResultSet resultSet = statement.executeGet("RESTAURANT", keys,
                Options.builder().withOption(Options.Name.INDEX_HINT, "RECORD_NAME_IDX").build())) {
            ResultSetAssert.assertThat(resultSet)
                    .hasNextRow()
                    .hasRow(r)
                    .hasNoNextRow();
        }
    }

    @Test
    void canGetFieldNamesFromCoveringIndex() throws Exception {
        long restNo = newRestNo();
        Message r = insertRestaurantRecord(statement, restNo);

        KeySet keys = new KeySet().setKeyColumn("REST_NO", restNo);

        try (final RelationalResultSet resultSet = statement.executeGet("RESTAURANT", keys,
                Options.builder().withOption(Options.Name.INDEX_HINT, "RECORD_TYPE_COVERING").build())) {
            ResultSetAssert.assertThat(resultSet).hasNextRow()
                    .hasRow(r)
                    .hasNoNextRow();
        }
    }

    @Test
    void wrongIndex() throws Exception {
        long restNo = newRestNo();

        KeySet keys = new KeySet().setKeyColumn("name", restName(restNo));

        org.assertj.core.api.Assertions.assertThatThrownBy(() -> statement.executeGet("RESTAURANT", keys, Options.builder().withOption(Options.Name.INDEX_HINT, "RestaurantRecord$NOT_AN_INDEX").build()))
                .isInstanceOf(RelationalException.class)
                .extracting("errorCode")
                .isEqualTo(ErrorCode.UNDEFINED_INDEX);
    }

    @Test
    void canInsertAndScanASingleRecord() throws Exception {
        try {
            long restNo = newRestNo();
            Message r = insertRestaurantRecord(statement, restNo);

            TableScan scan = TableScan.newBuilder()
                    .withTableName("RESTAURANT")
                    .setStartKey("REST_NO", restNo)
                    .setEndKey("REST_NO", restNo + 1)
                    .build();
            try (final RelationalResultSet resultSet = statement.executeScan(scan, Options.NONE)) {
                ResultSetAssert.assertThat(resultSet).hasNextRow()
                        .hasRow(r)
                        .hasNoNextRow();
            }
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
        long restNo = newRestNo();
        insertRestaurantRecord(statement, restNo);

        //verify that it's in there with a nice GET against the key
        KeySet keys = new KeySet()
                .setKeyColumn("REST_NO", restNo);

        try (RelationalResultSet resultSet = statement.executeGet("RESTAURANT", keys, Options.NONE)) {
            ResultSetAssert.assertThat(resultSet)
                    .hasNextRow()
                    .row()
                    .hasValue("REST_NO", restNo)
                    .hasValue("NAME", restName(restNo));
            org.assertj.core.api.Assertions.assertThat(resultSet.next()).isFalse();
        }
    }

    @Test
    void getViaIndex() throws RelationalException, SQLException {
        long restNo = newRestNo();
        Message r = insertRestaurantRecord(statement, restNo);

        //get via index
        KeySet keys = new KeySet()
                .setKeyColumn("name", restName(restNo));
        try (RelationalResultSet resultSet = statement.executeGet("RESTAURANT", keys,
                Options.builder().withOption(Options.Name.INDEX_HINT, "RECORD_NAME_IDX").build())) {
            ResultSetAssert.assertThat(resultSet).hasNextRow()
                    .hasRow(r)
                    .hasNoNextRow();
        }
    }

    @Test
    void scanPrimaryKey() throws RelationalException, SQLException {
        long restNo = newRestNo();
        Message r = insertRestaurantRecord(statement, restNo);

        //scan on the primary key
        TableScan scan = TableScan.newBuilder()
                .withTableName("RESTAURANT")
                .setStartKey("REST_NO", restNo)
                .setEndKey("REST_NO", restNo + 1)
                .build();
        try (RelationalResultSet resultSet = statement.executeScan(scan, Options.NONE)) {
            ResultSetAssert.assertThat(resultSet).hasNextRow()
                    .hasRow(r)
                    .hasNoNextRow();
        }
    }

    @Test
    void scanIndex() throws RelationalException, SQLException {
        long restNo = newRestNo();
        insertRestaurantRecord(statement, restNo);

        //scan on the index
        TableScan scan = TableScan.newBuilder()
                .withTableName("RESTAURANT")
                .setStartKey("NAME", restName(restNo))
                .setEndKey("NAME", restName(restNo) + 1)
                .build();
        try (RelationalResultSet resultSet = statement.executeScan(scan, Options.builder().withOption(Options.Name.INDEX_HINT, "RECORD_NAME_IDX").build())) {
            //because we are scanning the index only, the returned result only contains what's in the record_name_idx (name)
            ResultSetAssert.assertThat(resultSet).hasNextRow()
                    .hasRowExactly(Map.of("NAME", restName(restNo)))
                    .hasNoNextRow();
        }
    }

    @Test
    void testIndexCreatedUsingLastColumn() throws Exception {
        final String schema =
                " CREATE TABLE tbl1 (id int64, a string, b string, c string, PRIMARY KEY(id))" +
                        " CREATE VALUE INDEX c_name_idx ON tbl1(c)";
        try (var ddl = Ddl.builder().database("QT").relationalExtension(relationalExtension).schemaTemplate(schema).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {

                Message result = statement.getDataBuilder("TBL1").setField("ID", 42L).setField("A", "valuea1").setField("B", "valueb1").setField("C", "valuec1").build();
                int cnt = statement.executeInsert("TBL1", result);
                org.junit.jupiter.api.Assertions.assertEquals(1, cnt, "Incorrect insertion count");

                result = statement.getDataBuilder("TBL1").setField("ID", 43L).setField("A", "valuea2").setField("B", "valueb2").setField("C", "valuec2").build();
                cnt = statement.executeInsert("TBL1", result);
                org.junit.jupiter.api.Assertions.assertEquals(1, cnt, "Incorrect insertion count");

                result = statement.getDataBuilder("TBL1").setField("ID", 44L).setField("A", "valuea3").setField("B", "valueb3").setField("C", "valuec3").build();
                cnt = statement.executeInsert("TBL1", result);
                org.junit.jupiter.api.Assertions.assertEquals(1, cnt, "Incorrect insertion count");

                //scan on the index
                TableScan scan = TableScan.newBuilder()
                        .withTableName("TBL1")
                        .setStartKey("C", "valuec2")
                        .setEndKey("C", "valuec2" + 1) //??
                        .build();
                try (RelationalResultSet resultSet = statement.executeScan(scan, Options.builder().withOption(Options.Name.INDEX_HINT, "C_NAME_IDX").build())) {
                    //because we are scanning the index only, the returned result only contains what's in the record_name_idx (name)
                    ResultSetAssert.assertThat(resultSet).hasNextRow()
                            .hasRowExactly(Map.of("C", "valuec2"))
                            .hasNoNextRow();
                }
            }
        }
    }

    @Test
    void testIndexCreatedUsingMultipleColumns() throws Exception {
        final String schema =
                " CREATE TABLE tbl1 (id int64, a string, b string, c string, d string, PRIMARY KEY(id))" +
                        " CREATE VALUE INDEX c_name_idx ON tbl1(c, d)";
        try (var ddl = Ddl.builder().database("QT").relationalExtension(relationalExtension).schemaTemplate(schema).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {

                Message result = statement.getDataBuilder("TBL1").setField("ID", 42L).setField("A", "valuea1").setField("B", "valueb1").setField("C", "valuec1").setField("D", "valued1").build();
                int cnt = statement.executeInsert("TBL1", result);
                org.junit.jupiter.api.Assertions.assertEquals(1, cnt, "Incorrect insertion count");

                result = statement.getDataBuilder("TBL1").setField("ID", 43L).setField("A", "valuea2").setField("B", "valueb2").setField("C", "valuec2").setField("D", "valued2").build();
                cnt = statement.executeInsert("TBL1", result);
                org.junit.jupiter.api.Assertions.assertEquals(1, cnt, "Incorrect insertion count");

                result = statement.getDataBuilder("TBL1").setField("ID", 44L).setField("A", "valuea3").setField("B", "valueb3").setField("C", "valuec3").setField("D", "valued3").build();
                cnt = statement.executeInsert("TBL1", result);
                org.junit.jupiter.api.Assertions.assertEquals(1, cnt, "Incorrect insertion count");

                //scan on the index
                TableScan scan = TableScan.newBuilder()
                        .withTableName("TBL1")
                        .setStartKey("C", "valuec2")
                        .setEndKey("C", "valuec2" + 1) //??
                        .build();
                try (RelationalResultSet resultSet = statement.executeScan(scan, Options.builder().withOption(Options.Name.INDEX_HINT, "C_NAME_IDX").build())) {
                    //because we are scanning the index only, the returned result only contains what's in the record_name_idx (name)
                    ResultSetAssert.assertThat(resultSet).hasNextRow()
                            .hasRowExactly(Map.of("C", "valuec2", "D", "valued2"))
                            .hasNoNextRow();
                }
            }
        }
    }

    @Test
    void delete() throws RelationalException, SQLException {
        long restNo = newRestNo();
        insertRestaurantRecord(statement, restNo);

        //now delete the record
        KeySet keys = new KeySet()
                .setKeyColumn("REST_NO", restNo);
        int numDeleted = statement.executeDelete("RESTAURANT", Collections.singleton(keys));
        Assertions.assertThat(numDeleted).describedAs("Incorrect number of records deleted").isEqualTo(1);

        //now see that it's not there any more

        try (RelationalResultSet resultSet = statement.executeGet("RESTAURANT", keys, Options.NONE)) {
            ResultSetAssert.assertThat(resultSet).isEmpty();
        }
    }

    private Message insertRestaurantRecord(RelationalStatement s, long id) throws RelationalException {
        Message restaurant = statement.getDataBuilder("RESTAURANT")
                .setField("NAME", restName(id))
                .setField("REST_NO", id)
                .build();
        int insertCount = s.executeInsert("RESTAURANT", restaurant);
        Assertions.assertThat(insertCount).describedAs("Did not count insertions correctly!").isEqualByComparingTo(1);
        return restaurant;
    }

    private long newRestNo() {
        return restNo.incrementAndGet();
    }

    private String restName(long id) {
        return "testRest" + id;
    }

    private final AtomicLong restNo = new AtomicLong();
}
