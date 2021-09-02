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

import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.Restaurant;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.relational.api.DatabaseConnection;
import com.apple.foundationdb.relational.api.Relational;
import com.apple.foundationdb.relational.api.KeySet;
import com.apple.foundationdb.relational.api.OperationOption;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.Statement;
import com.apple.foundationdb.relational.api.TableScan;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.catalog.DatabaseTemplate;
import com.google.common.collect.Iterators;
import com.google.protobuf.Message;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.net.URI;
import java.util.Collections;

/**
 * Basic integration tests outlining the full process of using Relational to insert and scan records from a database.
 */
public class RecordLayerTableTest {
    @RegisterExtension
    public final RecordLayerCatalogRule catalog = new RecordLayerCatalogRule();

    @BeforeEach
    public final void setupCatalog(){
        final RecordMetaDataBuilder builder = RecordMetaData.newBuilder().setRecords(Restaurant.getDescriptor());
        builder.getRecordType("RestaurantRecord").setPrimaryKey(Key.Expressions.field("rest_no"));
        catalog.createSchemaTemplate(new RecordLayerTemplate("RestaurantRecord", builder.build()));

        catalog.createDatabase(URI.create("//record_layer_table_test"),
                DatabaseTemplate.newBuilder()
                        .withSchema("test","RestaurantRecord")
                        .build());
    }

    @AfterEach
    public final void tearDown(){
        catalog.deleteDatabase(URI.create("//record_layer_table_test"));
    }

    @Test
    void canInsertAndGetASingleRecord() {
        final URI dbUrl = URI.create("rlsc:embed://record_layer_table_test");
        try(DatabaseConnection conn = Relational.connect(dbUrl, Options.create().withOption(OperationOption.forceVerifyDdl()))) {
            conn.beginTransaction();
            conn.setSchema("test");
            try(Statement s = conn.createStatement()) {
                long id = System.currentTimeMillis();
                Restaurant.RestaurantRecord r = Restaurant.RestaurantRecord.newBuilder().setName("testRest" + id).setRestNo(id).build();
                int insertCount = s.executeInsert("RestaurantRecord", Iterators.singletonIterator(r), Options.create());
                Assertions.assertEquals(1, insertCount, "Did not count insertions correctly!");

                KeySet keys = new KeySet()
                        .setKeyColumn("rest_no",r.getRestNo());

                final RelationalResultSet resultSet = s.executeGet("RestaurantRecord",keys, Options.create());
                Assertions.assertNotNull(resultSet, "No result set returned!");
                Assertions.assertTrue(resultSet.next(), "No records returned!");
                if (resultSet.supportsMessageParsing()) {
                    Message m = resultSet.parseMessage();
                    Assertions.assertEquals(Restaurant.RestaurantRecord.getDescriptor(), m.getDescriptorForType(), "Incorrect message type returned");
                    final Object nameField = m.getField(Restaurant.RestaurantRecord.getDescriptor().findFieldByName("name"));
                    Assertions.assertEquals(r.getName(), nameField, "Incorrect restaurant record returned!");
                    Assertions.assertEquals(r, m, "Incorrect message returned");
                } else {
                    Assertions.assertEquals(r.getName(), resultSet.getString("name"), "Incorrect name!");
                    Assertions.assertEquals(r.getRestNo(), resultSet.getLong("rest_no"), "Incorrect rest_no!");
                }
            }
        }
    }

    @Test
    void canDeleteASingleRecord() {
        final URI dbUrl = URI.create("rlsc:embed://record_layer_table_test");
        try(DatabaseConnection conn = Relational.connect(dbUrl, Options.create().withOption(OperationOption.forceVerifyDdl()))) {
            conn.beginTransaction();
            conn.setSchema("test");
            try(Statement s = conn.createStatement()) {
                long id = System.currentTimeMillis();
                Restaurant.RestaurantRecord r = Restaurant.RestaurantRecord.newBuilder().setName("testRest" + id).setRestNo(id).build();
                int insertCount = s.executeInsert("RestaurantRecord", Iterators.singletonIterator(r), Options.create());
                Assertions.assertEquals(1, insertCount, "Did not count insertions correctly!");

                KeySet keys = new KeySet()
                        .setKeyColumn("rest_no",r.getRestNo());

                RelationalResultSet resultSet = s.executeGet("RestaurantRecord",keys, Options.create());
                Assertions.assertNotNull(resultSet, "No result set returned!");
                Assertions.assertTrue(resultSet.next(), "No records returned!");
                if (resultSet.supportsMessageParsing()) {
                    Message m = resultSet.parseMessage();
                    Assertions.assertEquals(Restaurant.RestaurantRecord.getDescriptor(), m.getDescriptorForType(), "Incorrect message type returned");
                    final Object nameField = m.getField(Restaurant.RestaurantRecord.getDescriptor().findFieldByName("name"));
                    Assertions.assertEquals(r.getName(), nameField, "Incorrect restaurant record returned!");
                    Assertions.assertEquals(r, m, "Incorrect message returned");
                } else {
                    Assertions.assertEquals(r.getName(), resultSet.getString("name"), "Incorrect name!");
                    Assertions.assertEquals(r.getRestNo(), resultSet.getLong("rest_no"), "Incorrect rest_no!");
                }

                //now delete the record and check again
                final int recordsDeleted = s.executeDelete("RestaurantRecord", Collections.singleton(keys), Options.create());
                Assertions.assertEquals(1,recordsDeleted,"Incorrect number of records deletes");

                //now it shouldn't be there
                resultSet = s.executeGet("RestaurantRecord",keys, Options.create());
                Assertions.assertNotNull(resultSet, "No result set returned!");
                Assertions.assertFalse(resultSet.next(), "No records returned!");
            }
        }
    }

    @Test
    void canInsertAndScanASingleRecordFromIndex() throws Exception {
        URI uri = URI.create("rlsc:embed://record_layer_table_test");
        try(DatabaseConnection conn = Relational.connect(uri, Options.create().withOption(OperationOption.forceVerifyDdl()))){
            conn.beginTransaction();
            conn.setSchema("test");
            try(Statement s = conn.createStatement()) {
                long id = System.currentTimeMillis();
                Restaurant.RestaurantRecord r = Restaurant.RestaurantRecord.newBuilder().setName("testRest" + id).setRestNo(id).build();
                int insertCount = s.executeInsert("RestaurantRecord", Iterators.singletonIterator(r), Options.create());
                Assertions.assertEquals(1, insertCount, "Did not count insertions correctly!");

                KeySet keys = new KeySet().setKeyColumn("name",r.getName());

                final RelationalResultSet resultSet = s.executeGet("RestaurantRecord",keys, Options.create().withOption(OperationOption.index("RestaurantRecord$name")));
                Assertions.assertNotNull(resultSet, "No result set returned!");
                Assertions.assertTrue(resultSet.next(), "No records returned!");
                if (resultSet.supportsMessageParsing()) {
                    Message m = resultSet.parseMessage();
                    Assertions.assertEquals(Restaurant.RestaurantRecord.getDescriptor(), m.getDescriptorForType(), "Incorrect message type returned");
                    final Object nameField = m.getField(Restaurant.RestaurantRecord.getDescriptor().findFieldByName("name"));
                    Assertions.assertEquals(r.getName(), nameField, "Incorrect restaurant record returned!");
                    Assertions.assertEquals(r, m, "Incorrect message returned");
                } else {
                    Assertions.assertEquals(r.getName(), resultSet.getString("name"), "Incorrect name!");
                    Assertions.assertEquals(r.getRestNo(), resultSet.getLong("rest_no"), "Incorrect rest_no!");
                }
            }
        }
    }

    @Test
    void canInsertAndScanASingleRecord() throws Exception {
        final URI dbUrl = URI.create("rlsc:embed://record_layer_table_test");
        try(DatabaseConnection conn = Relational.connect(dbUrl, Options.create().withOption(OperationOption.forceVerifyDdl()))){
            conn.beginTransaction();
            conn.setSchema("test");
            try(Statement s = conn.createStatement()){
                long id = System.currentTimeMillis();
                Restaurant.RestaurantRecord r = Restaurant.RestaurantRecord.newBuilder().setName("testRest"+id).setRestNo(id).build();
                int insertCount = s.executeInsert("RestaurantRecord", Iterators.singletonIterator(r),Options.create());
                Assertions.assertEquals(1, insertCount, "Did not count insertions correctly!");


                TableScan scan = TableScan.newBuilder()
                        .withTableName("RestaurantRecord")
                        .setStartKey("rest_no",r.getRestNo())
                        .setEndKey("rest_no",r.getRestNo()+1)
                        .build();
                final RelationalResultSet resultSet = s.executeScan(scan, Options.create());
                assertMatches(resultSet,r);
            } catch (Throwable t) {
                try {
                    conn.rollback();
                }catch(Throwable suppressable){
                    t.addSuppressed(suppressable);
                }
                throw t;
            }
        }
    }


    @Test
    void demo() {
        final URI dbUrl = URI.create("rlsc:embed://record_layer_table_test");

        try (DatabaseConnection conn = Relational.connect(dbUrl, null,Options.create().withOption(OperationOption.forceVerifyDdl()))) {
            conn.beginTransaction();
            conn.setSchema("test");
            //create a statement to execute against
            try (Statement s = conn.createStatement()) {
                long id = System.currentTimeMillis();
                Restaurant.RestaurantRecord r = Restaurant.RestaurantRecord.newBuilder().setName("testRest" + id).setRestNo(id).build();
                int insertCount = s.executeInsert("RestaurantRecord", Iterators.singletonIterator(r), Options.create());
                Assertions.assertEquals(1, insertCount, "Did not count insertions correctly!");

                //verify that it's in there with a nice GET against the key
                KeySet keys = new KeySet()
                        .setKeyColumn("rest_no", r.getRestNo());

                try (RelationalResultSet resultSet = s.executeGet("RestaurantRecord", keys, Options.create())) {
                    assertMatches(resultSet, r); //make sure that the record is there
                }

                //get via index
                keys = new KeySet()
                        .setKeyColumn("name",r.getName());
                try (RelationalResultSet resultSet = s.executeGet("RestaurantRecord", keys, Options.create().withOption(OperationOption.index("RestaurantRecord$name")))) {
                    assertMatches(resultSet, r); //make sure that the record is there
                }

                //scan on the primary key
                TableScan scan = TableScan.newBuilder()
                        .withTableName("RestaurantRecord")
                        .setStartKey("rest_no",r.getRestNo())
                        .setEndKey("rest_no",r.getRestNo()+1)
                        .build();
                try(RelationalResultSet resultSet = s.executeScan(scan, Options.create())) {
                    assertMatches(resultSet, r);
                }

                //scan on the index
                scan = TableScan.newBuilder()
                        .withTableName("RestaurantRecord")
                        .setStartKey("name",r.getName())
                        .setEndKey("name",r.getName()+1)
                        .build();
                try(RelationalResultSet resultSet = s.executeScan(scan, Options.create().withOption(OperationOption.index("RestaurantRecord$name")))) {
                    assertMatches(resultSet, r);
                }

                //now delete the record
                keys = new KeySet()
                        .setKeyColumn("rest_no", r.getRestNo());
                int numDeleted = s.executeDelete("RestaurantRecord",Collections.singleton(keys),Options.create());
                Assertions.assertEquals(1,numDeleted,"Incorrect number of keys deleted");

                //now see that it's not there any more

                try (RelationalResultSet resultSet = s.executeGet("RestaurantRecord", keys, Options.create())) {
                    Assertions.assertFalse(resultSet.next(),"Incorrectly returned a row!");
                }
            }
        }
    }

        private void assertMatches( RelationalResultSet resultSet,Restaurant.RestaurantRecord...r) {
            Assertions.assertNotNull(resultSet, "No result set returned!");
            //TODO(bfines) do Set operations here instead
            for(Restaurant.RestaurantRecord record : r) {
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
}
