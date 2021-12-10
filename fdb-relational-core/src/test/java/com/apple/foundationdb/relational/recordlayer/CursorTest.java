/*
 * CursorTest.java
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
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.RecordTypeBuilder;
import com.apple.foundationdb.relational.api.DatabaseConnection;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.Statement;
import com.apple.foundationdb.relational.api.TableScan;
import com.apple.foundationdb.relational.api.Relational;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.catalog.DatabaseTemplate;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class CursorTest {

    @RegisterExtension
    RecordLayerCatalogRule catalog = new RecordLayerCatalogRule();

    @BeforeEach
    public final void setupCatalog() {
        final RecordMetaDataBuilder builder = RecordMetaData.newBuilder().setRecords(Restaurant.getDescriptor());
        RecordTypeBuilder recordBuilder = builder.getRecordType("RestaurantRecord");
        recordBuilder.setRecordTypeKey(0);

        builder.addIndex("RestaurantRecord", new Index("record_type_covering",
                Key.Expressions.keyWithValue(
                        Key.Expressions.concat(
                                Key.Expressions.recordType(), Key.Expressions.field("rest_no"),
                                Key.Expressions.field("name")), 2),
                IndexTypes.VALUE));
        catalog.createSchemaTemplate(new RecordLayerTemplate("Restaurant", builder.build()));

        catalog.createDatabase(URI.create("/insert_test"),
                DatabaseTemplate.newBuilder()
                        .withSchema("main", "Restaurant")
                        .build());
    }

    @AfterEach
    void tearDown() {
        catalog.deleteDatabase(URI.create("/insert_test"));
    }

    @Test
    public void canIterateOverAllResults() throws InvalidProtocolBufferException {
        try (DatabaseConnection conn = Relational.connect(URI.create("rlsc:embed:/insert_test"), Options.create())) {
            conn.setSchema("main");
            conn.beginTransaction();
            try (Statement s = conn.createStatement()) {

                // 1/3 add all records to table insert_test.main.Restaurant.RestaurantRecord
                Iterable<Restaurant.RestaurantRecord> records = Utils.generateRestaurantRecords(10);
                int count = s.executeInsert("RestaurantRecord", records, Options.create());
                Assertions.assertEquals(10, count);

                // 2/3 scan all records
                List<Restaurant.RestaurantRecord> actual = new ArrayList<>();
                try (RelationalResultSet resultSet = s.executeScan(TableScan.newBuilder().withTableName("RestaurantRecord").build(),
                        Options.create())) {
                    while (resultSet.next()) {
                        Assertions.assertTrue(resultSet.supportsMessageParsing());
                        Message m = resultSet.parseMessage();
                        Restaurant.RestaurantRecord r = Restaurant.RestaurantRecord.parseFrom(m.toByteArray());
                        actual.add(r);
                    }
                }

                // 3/3 make sure we've received everything
                Collection<Restaurant.RestaurantRecord> expected = ImmutableList.copyOf(records);
                Assertions.assertEquals(expected.size(), actual.size());
                Assertions.assertTrue(actual.containsAll(expected)); // no dups
            }
        }
    }

}
