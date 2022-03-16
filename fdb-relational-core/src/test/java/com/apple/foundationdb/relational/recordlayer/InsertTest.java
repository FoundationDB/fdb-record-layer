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

import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.Restaurant;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.RecordTypeBuilder;
import com.apple.foundationdb.relational.api.KeySet;
import com.apple.foundationdb.relational.api.OperationOption;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.TableScan;
import com.apple.foundationdb.relational.api.Relational;
import com.apple.foundationdb.relational.api.RelationalConnection;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.RelationalStatement;
import com.apple.foundationdb.relational.api.catalog.DatabaseTemplate;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;

import com.google.protobuf.Message;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.net.URI;
import java.sql.SQLException;
import java.util.Collections;
import java.util.function.Consumer;

public class InsertTest {
    @RegisterExtension
    public final RecordLayerCatalogRule catalog = new RecordLayerCatalogRule();

    @BeforeEach
    public final void setupCatalog() throws RelationalException {
        final RecordMetaDataBuilder builder = RecordMetaData.newBuilder().setRecords(Restaurant.getDescriptor());
        RecordTypeBuilder recordBuilder = builder.getRecordType("RestaurantRecord");
        recordBuilder.setRecordTypeKey(0);

        builder.addIndex("RestaurantRecord", new Index("record_type_covering",
                Key.Expressions.keyWithValue(
                        Key.Expressions.concat(Key.Expressions.recordType(), Key.Expressions.field("rest_no"), Key.Expressions.field("name")), 2), IndexTypes.VALUE));
        catalog.createSchemaTemplate(new RecordLayerTemplate("Restaurant", builder.build()));

        catalog.createDatabase(URI.create("/insert_test"),
                DatabaseTemplate.newBuilder()
                        .withSchema("main", "Restaurant")
                        .build());
    }

    @AfterEach
    void tearDown() throws RelationalException {
        catalog.deleteDatabase(URI.create("/insert_test"));
    }

    @Test
    void canInsertWithMultipleRecordTypes() throws RelationalException, SQLException {
        /*
         * We want to make sure that we don't accidentally pick up data from different tables
         */
        try (RelationalConnection conn = Relational.connect(URI.create("jdbc:embed:/insert_test"), Options.create())) {
            conn.setSchema("main");
            conn.beginTransaction();
            try (RelationalStatement s = conn.createStatement()) {
                long id = System.currentTimeMillis();
                Restaurant.RestaurantRecord record = Restaurant.RestaurantRecord.newBuilder().setRestNo(id).setName("restRecord" + id).build();
                int inserted = s.executeInsert("RestaurantRecord", Collections.singleton(record), Options.create());
                Assertions.assertEquals(1, inserted, "Did not insert properly!");

                Restaurant.RestaurantReviewer reviewer = Restaurant.RestaurantReviewer.newBuilder().setName("reviewerName" + id).setId(id - 1).build();
                inserted = s.executeInsert("RestaurantReviewer", Collections.singleton(reviewer), Options.create());
                Assertions.assertEquals(1, inserted, "Did not insert reviewers properly!");

                //now prove we can get them back out
                RelationalResultSet relationalResultSet = s.executeGet("RestaurantRecord", new KeySet().setKeyColumn("rest_no", record.getRestNo()), Options.create());
                assertGetMatches(record, relationalResultSet, vry -> {
                    try {
                        Assertions.assertEquals(record.getName(), vry.getString("name"), "Incorrect name!");
                        Assertions.assertEquals(record.getRestNo(), vry.getLong("rest_no"), "Incorrect rest_no!");
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                });

                relationalResultSet = s.executeGet("RestaurantReviewer", new KeySet().setKeyColumn("id", reviewer.getId()), Options.create());
                assertGetMatches(reviewer, relationalResultSet, vry -> {
                    try {
                        Assertions.assertEquals(reviewer.getName(), vry.getString("name"), "Incorrect reviewer name");
                        Assertions.assertEquals(reviewer.getId(), vry.getLong("id"), "Incorrect reviewer id");
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                });

                //now make sure that they don't show up in the other table
                relationalResultSet = s.executeGet("RestaurantRecord", new KeySet().setKeyColumn("name", reviewer.getName()), Options.create().withOption(OperationOption.index("RestaurantRecord$name")));
                Assertions.assertNotNull(relationalResultSet, "Did not return a valid result set!");
                Assertions.assertFalse(relationalResultSet.next(), "Records should not be returned!");

                relationalResultSet = s.executeGet("RestaurantReviewer", new KeySet().setKeyColumn("name", record.getName()), Options.create().withOption(OperationOption.index("RestaurantReviewer$name")));
                Assertions.assertNotNull(relationalResultSet, "Did not return a valid result set!");
                Assertions.assertFalse(relationalResultSet.next(), "Records should not be returned!");

                /*
                 * now try to scan the entirety of each table and validate the results
                 * we can have data from multiple test runs contaminating this scan, because it's unbounded. This is
                 * actually OK, because wwhat we really care about is that the scan doesn't return data from
                 * other tables. So all we do here is check the returned message type
                 */
                final RelationalResultSet recordScan = s.executeScan(TableScan.newBuilder().withTableName("RestaurantRecord").build(), Options.create().withOption(OperationOption.index("record_type_covering")));
                Assertions.assertNotNull(recordScan, "Did not return a valid result set!");
                while (recordScan.next()) {
                    if (recordScan.supportsMessageParsing()) {
                        Message row = recordScan.parseMessage();
                        Assertions.assertEquals(record.getDescriptorForType().getFullName(), row.getDescriptorForType().getFullName(), "Unexpected descriptor type");
                    }
                    Assertions.assertDoesNotThrow(() -> {
                        //make sure that the correct fields are returned
                        recordScan.getString("name");
                        recordScan.getLong("rest_no");
                    });
                    RelationalAssertions.assertThrowsSqlException(() -> recordScan.getLong("id"), ErrorCode.INVALID_COLUMN_REFERENCE);
                }

                final RelationalResultSet reviewerScan = s.executeScan(TableScan.newBuilder().withTableName("RestaurantReviewer").build(), Options.create());
                Assertions.assertNotNull(reviewerScan, "Did not return a valid result set!");
                while (reviewerScan.next()) {
                    if (reviewerScan.supportsMessageParsing()) {
                        Message row = reviewerScan.parseMessage();
                        Assertions.assertEquals(reviewer.getDescriptorForType().getFullName(), row.getDescriptorForType().getFullName(), "Unexpected descriptor type");
                    }
                    Assertions.assertDoesNotThrow(() -> {
                        //make sure that the correct fields are returned
                        reviewerScan.getString("name");
                        reviewerScan.getLong("id");
                    });

                    RelationalAssertions.assertThrowsSqlException(() -> reviewerScan.getLong("rest_no"), ErrorCode.INVALID_COLUMN_REFERENCE);
                }
            }
        }
    }

    private void assertGetMatches(Message expected, RelationalResultSet queryResult, Consumer<RelationalResultSet> nonMessageCheck) throws SQLException {
        Assertions.assertNotNull(queryResult, "Did not return a valid result set!");
        Assertions.assertTrue(queryResult.next(), "Did not return a record!");
        if (queryResult.supportsMessageParsing()) {
            Message m = queryResult.parseMessage();
            Assertions.assertEquals(expected, m, "Incorrect returned record!");
        } else {
            nonMessageCheck.accept(queryResult);
        }
        Assertions.assertFalse(queryResult.next(), "Too many records returned!");
    }

    @Test
    void cannotInsertWithIncorrectTypeForRecord() throws RelationalException, SQLException {
        /*
         * We want to make sure that we don't accidentally pick up data from different tables
         */
        try (RelationalConnection conn = Relational.connect(URI.create("jdbc:embed:/insert_test"), Options.create())) {
            conn.setSchema("main");
            conn.beginTransaction();
            try (RelationalStatement s = conn.createStatement()) {
                long id = System.currentTimeMillis();
                Restaurant.RestaurantRecord record = Restaurant.RestaurantRecord.newBuilder().setRestNo(id).setName("restRecord" + id).build();
                RelationalAssertions.assertThrowsRelationalException(
                        () -> s.executeInsert("RestaurantReviewer", Collections.singleton(record), Options.create()),
                        ErrorCode.INVALID_PARAMETER);
            }
        }
    }

    @Test
    void cannotInsertWithMissingSchema() throws RelationalException, SQLException {
        /*
         * We want to make sure that we don't accidentally pick up data from different tables
         */
        try (RelationalConnection conn = Relational.connect(URI.create("jdbc:embed:/insert_test"), Options.create())) {
            conn.setSchema("doesNotExist");
            conn.beginTransaction();
            try (RelationalStatement s = conn.createStatement()) {
                long id = System.currentTimeMillis();
                Restaurant.RestaurantRecord record = Restaurant.RestaurantRecord.newBuilder().setRestNo(id).setName("restRecord" + id).build();
                RelationalAssertions.assertThrowsRelationalException(() -> {
                    int inserted = s.executeInsert("RestaurantReviewer", Collections.singleton(record), Options.create());
                    Assertions.fail("did not throw an exception on insertion(inserted=" + inserted + ")");
                }, ErrorCode.UNKNOWN_SCHEMA);
            }
        }
    }
}
