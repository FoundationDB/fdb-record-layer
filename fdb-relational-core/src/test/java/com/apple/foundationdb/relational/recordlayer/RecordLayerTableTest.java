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
import com.apple.foundationdb.relational.api.NestableTuple;
import com.apple.foundationdb.relational.api.OperationOption;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.Statement;
import com.apple.foundationdb.relational.api.TableScan;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.catalog.DatabaseTemplate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Basic integration tests outlining the full process of using Relational to insert and scan records from a database.
 */
public class RecordLayerTableTest {
    @RegisterExtension
    public final RecordLayerCatalogRule catalog = new RecordLayerCatalogRule("/",
            ((oldUserVersion, oldMetaDataVersion, metaData) -> CompletableFuture.completedFuture(oldUserVersion)),
            new TestSerializerRegistry());

    @BeforeEach
    public final void setupCatalog(){
        final RecordMetaDataBuilder builder = RecordMetaData.newBuilder().setRecords(Restaurant.getDescriptor());
        builder.getRecordType("RestaurantRecord").setPrimaryKey(Key.Expressions.field("name"));
        catalog.setSchemaTemplate(new RecordLayerTemplate("RestaurantRecord",builder.build()));

        catalog.loadDatabase("record_layer_table_test", DatabaseTemplate.newBuilder().withSchema("test","RestaurantRecord").build());
    }

    @Test
    public void canInsertAndScanASingleRecord() {
        RecordLayerDriver driver = new RecordLayerDriver(catalog,catalog.fdbDatabase);
        try(DatabaseConnection conn = driver.connect(ImmutableList.of("record_layer_table_test"), Options.create().withOption(OperationOption.forceVerifyDdl()))){
            conn.beginTransaction();
            conn.setSchema("test");
            try(Statement s = conn.createStatement()){
                Restaurant.RestaurantRecord r = Restaurant.RestaurantRecord.newBuilder().setName("testRest"+System.currentTimeMillis()).setRestNo(1L).build();
                int insertCount = s.executeInsert("RestaurantRecord", Iterators.singletonIterator(r),Options.create());
                Assertions.assertEquals(1, insertCount, "Did not count insertions correctly!");


                TableScan scan = TableScan.newBuilder()
                        .withTableName("RestaurantRecord")
                        .setStartKey("name",r.getName())
                        .setEndKey("name",r.getName()+"1")
                        .build();
                final RelationalResultSet resultSet = s.executeScan(scan, Options.create());
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
                }
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

}
