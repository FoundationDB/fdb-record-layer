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
import com.apple.foundationdb.relational.api.KeySet;
import com.apple.foundationdb.relational.api.OperationOption;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.TableScan;
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Basic integration tests outlining the full process of using Relational to insert and scan records from a database.
 */
public class RecordLayerTableTest {
    @RegisterExtension
    public static final EmbeddedRelationalExtension relational = new EmbeddedRelationalExtension();

    @RegisterExtension
    @Order(0)
    public final SimpleDatabaseRule database = new SimpleDatabaseRule(relational.getEngine(), RecordLayerTableTest.class, TestSchemas.restaurant());

    @RegisterExtension
    @Order(3)
    public final RelationalConnectionRule connection = new RelationalConnectionRule(database::getConnectionUri)
            .withOptions(Options.create())
            .withSchema("testSchema");

    @RegisterExtension
    @Order(4)
    public final RelationalStatementRule statement = new RelationalStatementRule(connection);

    @Test
    void canInsertAndGetASingleRecord() throws RelationalException, SQLException {
        Restaurant.RestaurantRecord inserted = insertRestaurantRecord(statement);

        KeySet keys = new KeySet()
                .setKeyColumn("rest_no", inserted.getRestNo());

        try (final RelationalResultSet resultSet = statement.executeGet("RestaurantRecord", keys, Options.create())) {
            RelationalAssertions.assertThat(resultSet).hasExactly(new MessageTuple(inserted));
        }
    }

    @Test
    void canDeleteASingleRecord() throws RelationalException, SQLException {
        Restaurant.RestaurantRecord inserted = insertRestaurantRecord(statement);

        KeySet keys = new KeySet().setKeyColumn("rest_no", inserted.getRestNo());

        try (RelationalResultSet resultSet = statement.executeGet("RestaurantRecord", keys, Options.create())) {
            RelationalAssertions.assertThat(resultSet).hasExactly(new MessageTuple(inserted));
        }

        // Now delete the record and check again
        int recordsDeleted = statement.executeDelete("RestaurantRecord", Collections.singleton(keys), Options.create());
        Assertions.assertEquals(1, recordsDeleted, "Incorrect number of records deletes");

        // Now it shouldn't be there
        try (RelationalResultSet resultSet = statement.executeGet("RestaurantRecord", keys, Options.create())) {
            RelationalAssertions.assertThat(resultSet).hasNoNextRow();
        }
    }

    @Test
    void deleteNoRecord() throws RelationalException, SQLException {
        Restaurant.RestaurantRecord inserted = insertRestaurantRecord(statement);

        KeySet keys = new KeySet().setKeyColumn("rest_no", inserted.getRestNo());

        try (RelationalResultSet resultSet = statement.executeGet("RestaurantRecord", keys, Options.create())) {
            RelationalAssertions.assertThat(resultSet).hasExactly(new MessageTuple(inserted));
        }

        // Pretend to delete the record
        int recordsDeleted = statement.executeDelete("RestaurantRecord", List.of(), Options.create());
        Assertions.assertEquals(0, recordsDeleted, "Incorrect number of records deletes");

        // It's still there
        try (RelationalResultSet resultSet = statement.executeGet("RestaurantRecord", keys, Options.create())) {
            RelationalAssertions.assertThat(resultSet).hasExactly(new MessageTuple(inserted));
        }
    }

    @Test
    void canDeleteMultipleRecord() throws RelationalException, SQLException {
        Restaurant.RestaurantRecord[] inserted = {insertRestaurantRecord(statement), insertRestaurantRecord(statement)};
        List<KeySet> keys = new ArrayList<>();
        for (Restaurant.RestaurantRecord record : inserted) {
            keys.add(new KeySet().setKeyColumn("rest_no", record.getRestNo()));
            try (RelationalResultSet resultSet = statement.executeGet("RestaurantRecord", keys.get(keys.size() - 1), Options.create())) {
                RelationalAssertions.assertThat(resultSet).hasExactly(new MessageTuple(record));
            }
        }

        //now delete the record and check again
        final int recordsDeleted = statement.executeDelete("RestaurantRecord", keys, Options.create());
        Assertions.assertEquals(2, recordsDeleted, "Incorrect number of records deletes");

        //now it shouldn't be there
        for (KeySet key : keys) {
            try (RelationalResultSet resultSet = statement.executeGet("RestaurantRecord", key, Options.create())) {
                RelationalAssertions.assertThat(resultSet).hasNoNextRow();
            }
        }
    }

    @Test
    void canInsertAndScanASingleRecordFromIndex() throws Exception {
        Restaurant.RestaurantRecord r = insertRestaurantRecord(statement);

        KeySet keys = new KeySet().setKeyColumn("name", r.getName());

        try (final RelationalResultSet resultSet = statement.executeGet("RestaurantRecord", keys,
                Options.create().withOption(OperationOption.index("record_name_idx")))) {
            RelationalAssertions.assertThat(resultSet).hasExactly(new MessageTuple(r));
        }
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
            try (final RelationalResultSet resultSet = statement.executeScan(scan, Options.create())) {
                RelationalAssertions.assertThat(resultSet).hasExactly(new MessageTuple(r));
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
        Restaurant.RestaurantRecord r = insertRestaurantRecord(statement);

        //verify that it's in there with a nice GET against the key
        KeySet keys = new KeySet()
                .setKeyColumn("rest_no", r.getRestNo());

        try (RelationalResultSet resultSet = statement.executeGet("RestaurantRecord", keys, Options.create())) {
            RelationalAssertions.assertThat(resultSet).hasExactly(new MessageTuple(r));
        }
    }

    @Test
    void getViaIndex() throws RelationalException, SQLException {
        Restaurant.RestaurantRecord r = insertRestaurantRecord(statement);

        //get via index
        KeySet keys = new KeySet()
                .setKeyColumn("name", r.getName());
        try (RelationalResultSet resultSet = statement.executeGet("RestaurantRecord", keys,
                Options.create().withOption(OperationOption.index("record_name_idx")))) {
            RelationalAssertions.assertThat(resultSet).hasExactly(new MessageTuple(r));
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
            RelationalAssertions.assertThat(resultSet).hasExactly(new MessageTuple(r));
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
        try (RelationalResultSet resultSet = statement.executeScan(scan, Options.create().withOption(OperationOption.index("record_name_idx")))) {
            //because we are scanning the index only, the returned result only contains what's in the record_name_idx (name)
            RelationalAssertions.assertThat(resultSet).hasExactly(Map.of("name", r.getName()));
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
            RelationalAssertions.assertThat(resultSet).hasNoNextRow();
        }
    }

    private Restaurant.RestaurantRecord insertRestaurantRecord(RelationalStatement s) throws RelationalException {
        long id = System.currentTimeMillis();
        Restaurant.RestaurantRecord r = Restaurant.RestaurantRecord.newBuilder().setName("testRest" + id).setRestNo(id).build();
        int insertCount = s.executeInsert("RestaurantRecord", statement.getDataBuilder("RestaurantRecord").convertMessage(r), Options.create());
        Assertions.assertEquals(1, insertCount, "Did not count insertions correctly!");
        return r;
    }
}
