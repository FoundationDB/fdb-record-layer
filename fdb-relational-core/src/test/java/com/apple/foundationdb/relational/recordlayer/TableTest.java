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

import com.apple.foundationdb.relational.api.EmbeddedRelationalStruct;
import com.apple.foundationdb.relational.api.KeySet;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.RelationalStatement;
import com.apple.foundationdb.relational.api.RelationalStruct;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.utils.Ddl;
import com.apple.foundationdb.relational.utils.ResultSetAssert;
import com.apple.foundationdb.relational.utils.SimpleDatabaseRule;
import com.apple.foundationdb.relational.utils.TestSchemas;
import com.apple.foundationdb.relational.utils.RelationalAssertions;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.net.URI;
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
    void canInsertAndGetASingleRecord() throws Exception {
        long restNo = newRestNo();
        insertRestaurantRecord(statement, restNo);

        KeySet keys = new KeySet()
                .setKeyColumn("REST_NO", restNo);

        try (final RelationalResultSet resultSet = statement.executeGet("RESTAURANT", keys, Options.NONE)) {
            ResultSetAssert.assertThat(resultSet)
                    .hasNextRow()
                    .hasColumn("REST_NO", restNo)
                    .hasColumn("NAME", restName(restNo))
                    .hasNoNextRow();
        }
    }

    @Test
    void wrongSizeOfPrimaryKeyInGet() {
        RelationalAssertions.assertThrowsSqlException(
                () -> statement.executeGet("RESTAURANT", new KeySet(), Options.NONE))
                .hasErrorCode(ErrorCode.INVALID_PARAMETER);
    }

    @Test
    void wrongSizeOfPrimaryKeyInGetLongerKey() throws Exception {
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate("CREATE TABLE FOO(A bigint, B bigint, C bigint, PRIMARY KEY(C, A))").build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                RelationalAssertions.assertThrowsSqlException(
                        () -> statement.executeGet("FOO", new KeySet().setKeyColumn("C", 5), Options.NONE))
                        .hasErrorCode(ErrorCode.INVALID_PARAMETER);
                RelationalAssertions.assertThrowsSqlException(
                        () -> statement.executeGet("FOO", new KeySet().setKeyColumn("A", 5), Options.NONE))
                        .hasErrorCode(ErrorCode.INVALID_PARAMETER);
            }
        }
    }

    @Test
    void canDeleteASingleRecord() throws Exception {
        long restNo = newRestNo();
        insertRestaurantRecord(statement, restNo);

        KeySet keys = new KeySet().setKeyColumn("REST_NO", restNo);

        try (RelationalResultSet resultSet = statement.executeGet("RESTAURANT", keys, Options.NONE)) {
            ResultSetAssert.assertThat(resultSet)
                    .hasNextRow()
                    .hasColumn("REST_NO", restNo)
                    .hasColumn("NAME", restName(restNo))
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
    void deleteNoRecord() throws Exception {
        long restNo = newRestNo();
        insertRestaurantRecord(statement, restNo);

        KeySet keys = new KeySet().setKeyColumn("REST_NO", restNo);

        try (RelationalResultSet resultSet = statement.executeGet("RESTAURANT", keys, Options.NONE)) {
            ResultSetAssert.assertThat(resultSet)
                    .hasNextRow()
                    .hasColumn("REST_NO", restNo)
                    .hasColumn("NAME", restName(restNo))
                    .hasNoNextRow();
        }

        // Pretend to delete the record
        int recordsDeleted = statement.executeDelete("RESTAURANT", List.of());
        Assertions.assertThat(recordsDeleted).describedAs("Incorrect number of records deleted").isEqualTo(0);

        // It's still there
        try (RelationalResultSet resultSet = statement.executeGet("RESTAURANT", keys, Options.NONE)) {
            ResultSetAssert.assertThat(resultSet)
                    .hasNextRow()
                    .hasColumn("REST_NO", restNo)
                    .hasColumn("NAME", restName(restNo))
                    .hasNoNextRow();
        }
    }

    @Test
    void canDeleteMultipleRecord() throws Exception {
        long[] restNo = {newRestNo(), newRestNo()};
        insertRestaurantRecord(statement, restNo[0]);
        insertRestaurantRecord(statement, restNo[1]);
        List<KeySet> keys = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            keys.add(new KeySet().setKeyColumn("REST_NO", restNo[i]));
            try (RelationalResultSet resultSet = statement.executeGet("RESTAURANT", keys.get(keys.size() - 1), Options.NONE)) {
                ResultSetAssert.assertThat(resultSet)
                        .hasNextRow()
                        .hasColumn("REST_NO", restNo[i])
                        .hasColumn("NAME", restName(restNo[i]))
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
        insertRestaurantRecord(statement, restNo);

        KeySet keys = new KeySet().setKeyColumn("NAME", restName(restNo));

        try (final RelationalResultSet resultSet = statement.executeGet("RESTAURANT", keys,
                Options.builder().withOption(Options.Name.INDEX_HINT, "RECORD_NAME_IDX").build())) {
            ResultSetAssert.assertThat(resultSet)
                    .hasNextRow()
                    .hasColumn("REST_NO", restNo)
                    .hasColumn("NAME", restName(restNo))
                    .hasNoNextRow();
        }
    }

    @Test
    void canGetFieldNamesFromCoveringIndex() throws Exception {
        long restNo = newRestNo();
        insertRestaurantRecord(statement, restNo);

        KeySet keys = new KeySet().setKeyColumn("REST_NO", restNo);

        try (final RelationalResultSet resultSet = statement.executeGet("RESTAURANT", keys,
                Options.builder().withOption(Options.Name.INDEX_HINT, "RECORD_TYPE_COVERING").build())) {
            ResultSetAssert.assertThat(resultSet).hasNextRow()
                    .hasColumn("REST_NO", restNo)
                    .hasColumn("NAME", restName(restNo))
                    .hasNoNextRow();
        }
    }

    @Test
    void wrongIndex() throws Exception {
        long restNo = newRestNo();

        KeySet keys = new KeySet().setKeyColumn("name", restName(restNo));

        RelationalAssertions.assertThrowsSqlException(() -> statement.executeGet("RESTAURANT", keys, Options.builder().withOption(Options.Name.INDEX_HINT, "RestaurantRecord$NOT_AN_INDEX").build()))
                .hasErrorCode(ErrorCode.UNDEFINED_INDEX);
    }

    @Test
    void canInsertAndScanASingleRecord() throws Exception {
        try {
            long restNo = newRestNo();
            insertRestaurantRecord(statement, restNo);

            KeySet keySet = new KeySet().setKeyColumn("REST_NO", restNo);
            try (final RelationalResultSet resultSet = statement.executeScan("RESTAURANT", keySet, Options.NONE)) {
                ResultSetAssert.assertThat(resultSet).hasNextRow()
                        .hasColumn("REST_NO", restNo)
                        .hasColumn("NAME", restName(restNo))
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
    void get() throws Exception {
        long restNo = newRestNo();
        insertRestaurantRecord(statement, restNo);

        //verify that it's in there with a nice GET against the key
        KeySet keys = new KeySet()
                .setKeyColumn("REST_NO", restNo);

        try (RelationalResultSet resultSet = statement.executeGet("RESTAURANT", keys, Options.NONE)) {
            ResultSetAssert.assertThat(resultSet)
                    .hasNextRow()
                    .hasColumn("REST_NO", restNo)
                    .hasColumn("NAME", restName(restNo));
            org.assertj.core.api.Assertions.assertThat(resultSet.next()).isFalse();
        }
    }

    @Test
    void getViaIndex() throws Exception {
        long restNo = newRestNo();
        insertRestaurantRecord(statement, restNo);

        //get via index
        KeySet keys = new KeySet()
                .setKeyColumn("NAME", restName(restNo));
        try (RelationalResultSet resultSet = statement.executeGet("RESTAURANT", keys,
                Options.builder().withOption(Options.Name.INDEX_HINT, "RECORD_NAME_IDX").build())) {
            ResultSetAssert.assertThat(resultSet).hasNextRow()
                    .hasColumn("REST_NO", restNo)
                    .hasColumn("NAME", restName(restNo))
                    .hasNoNextRow();
        }
    }

    @Test
    void scanPrimaryKey() throws Exception {
        long restNo = newRestNo();
        insertRestaurantRecord(statement, restNo);

        //scan on the primary key
        KeySet keySet = new KeySet().setKeyColumn("REST_NO", restNo);
        try (RelationalResultSet resultSet = statement.executeScan("RESTAURANT", keySet, Options.NONE)) {
            ResultSetAssert.assertThat(resultSet).hasNextRow()
                    .hasColumn("REST_NO", restNo)
                    .hasColumn("NAME", restName(restNo))
                    .hasNoNextRow();
        }
    }

    @Test
    void scanIndex() throws Exception {
        long restNo = newRestNo();
        insertRestaurantRecord(statement, restNo);

        //scan on the index
        KeySet keySet = new KeySet().setKeyColumn("NAME", restName(restNo));
        try (RelationalResultSet resultSet = statement.executeScan("RESTAURANT", keySet, Options.builder().withOption(Options.Name.INDEX_HINT, "RECORD_NAME_IDX").build())) {
            //because we are scanning the index only, the returned result only contains what's in the record_name_idx (name)
            ResultSetAssert.assertThat(resultSet).hasNextRow()
                    .hasColumns(Map.of("NAME", restName(restNo)))
                    .hasNoNextRow();
        }
    }

    @Test
    void testIndexCreatedUsingLastColumn() throws Exception {
        final String schema =
                " CREATE TABLE tbl1 (id bigint, a string, b string, c string, PRIMARY KEY(id))" +
                        " CREATE INDEX c_name_idx as select c from tbl1";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schema).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {

                var result = EmbeddedRelationalStruct.newBuilder().addLong("ID", 42L).addString("A", "valuea1").addString("B", "valueb1").addString("C", "valuec1").build();
                int cnt = statement.executeInsert("TBL1", result);
                org.junit.jupiter.api.Assertions.assertEquals(1, cnt, "Incorrect insertion count");

                result = EmbeddedRelationalStruct.newBuilder().addLong("ID", 43L).addString("A", "valuea2").addString("B", "valueb2").addString("C", "valuec2").build();
                cnt = statement.executeInsert("TBL1", result);
                org.junit.jupiter.api.Assertions.assertEquals(1, cnt, "Incorrect insertion count");

                result = EmbeddedRelationalStruct.newBuilder().addLong("ID", 44L).addString("A", "valuea3").addString("B", "valueb3").addString("C", "valuec3").build();
                cnt = statement.executeInsert("TBL1", result);
                org.junit.jupiter.api.Assertions.assertEquals(1, cnt, "Incorrect insertion count");

                //scan on the index
                KeySet keySet = new KeySet().setKeyColumn("C", "valuec2");
                try (RelationalResultSet resultSet = statement.executeScan("TBL1", keySet,
                        Options.builder().withOption(Options.Name.INDEX_HINT, "C_NAME_IDX").build())) {
                    //because we are scanning the index only, the returned result only contains what's in the record_name_idx (name)
                    ResultSetAssert.assertThat(resultSet).hasNextRow()
                            .hasColumns(Map.of("C", "valuec2"))
                            .hasNoNextRow();
                }
            }
        }
    }

    @Test
    void testIndexCreatedUsingMultipleColumns() throws Exception {
        final String schema =
                " CREATE TABLE tbl1 (id bigint, a string, b string, c string, d string, PRIMARY KEY(id))" +
                        " CREATE INDEX c_name_idx as select c, d from tbl1 order by c, d";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/QT")).relationalExtension(relationalExtension).schemaTemplate(schema).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {

                var result = EmbeddedRelationalStruct.newBuilder().addLong("ID", 42L).addString("A", "valuea1").addString("B", "valueb1").addString("C", "valuec1").addString("D", "valued1").build();
                int cnt = statement.executeInsert("TBL1", result);
                org.junit.jupiter.api.Assertions.assertEquals(1, cnt, "Incorrect insertion count");

                result = EmbeddedRelationalStruct.newBuilder().addLong("ID", 43L).addString("A", "valuea2").addString("B", "valueb2").addString("C", "valuec2").addString("D", "valued2").build();
                cnt = statement.executeInsert("TBL1", result);
                org.junit.jupiter.api.Assertions.assertEquals(1, cnt, "Incorrect insertion count");

                result = EmbeddedRelationalStruct.newBuilder().addLong("ID", 44L).addString("A", "valuea3").addString("B", "valueb3").addString("C", "valuec3").addString("D", "valued3").build();
                cnt = statement.executeInsert("TBL1", result);
                org.junit.jupiter.api.Assertions.assertEquals(1, cnt, "Incorrect insertion count");

                //scan on the index
                KeySet keySet = new KeySet().setKeyColumn("C", "valuec2");
                try (RelationalResultSet resultSet = statement.executeScan("TBL1", keySet,
                        Options.builder().withOption(Options.Name.INDEX_HINT, "C_NAME_IDX").build())) {
                    //because we are scanning the index only, the returned result only contains what's in the record_name_idx (name)
                    ResultSetAssert.assertThat(resultSet).hasNextRow()
                            .hasColumns(Map.of("C", "valuec2", "D", "valued2"))
                            .hasNoNextRow();
                }
            }
        }
    }

    @Test
    void delete() throws Exception {
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

    private RelationalStruct insertRestaurantRecord(RelationalStatement s, long id) throws Exception {
        var restaurant = EmbeddedRelationalStruct.newBuilder()
                .addString("NAME", restName(id))
                .addLong("REST_NO", id)
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
