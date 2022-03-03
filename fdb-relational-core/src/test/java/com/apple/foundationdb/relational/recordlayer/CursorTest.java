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
import com.apple.foundationdb.relational.api.Continuation;
import com.apple.foundationdb.relational.api.OperationOption;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.QueryProperties;
import com.apple.foundationdb.relational.api.TableScan;
import com.apple.foundationdb.relational.api.Relational;
import com.apple.foundationdb.relational.api.RelationalConnection;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.RelationalStatement;
import com.apple.foundationdb.relational.api.catalog.DatabaseTemplate;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.net.URI;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.BiConsumer;

public class CursorTest {

    @RegisterExtension
    RecordLayerCatalogRule catalog = new RecordLayerCatalogRule();

    @BeforeEach
    public final void setupCatalog() throws RelationalException {
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
    void tearDown() throws RelationalException {
        catalog.deleteDatabase(URI.create("/insert_test"));
    }

    @Test
    public void canIterateOverAllResults() throws RelationalException, SQLException {
        havingInsertedRecordsDo(10, (Iterable<Restaurant.RestaurantRecord> records, RelationalStatement s) -> {
            // 1/2 scan all records
            List<Restaurant.RestaurantRecord> actual = new ArrayList<>();
            try (RelationalResultSet resultSet = s.executeScan(TableScan.newBuilder().withTableName("RestaurantRecord").build(),
                    Options.create())) {
                while (resultSet.next()) {
                    Assertions.assertTrue(resultSet.supportsMessageParsing());
                    Message m = resultSet.parseMessage();
                    Restaurant.RestaurantRecord r = null;
                    try {
                        r = Restaurant.RestaurantRecord.parseFrom(m.toByteArray());
                    } catch (InvalidProtocolBufferException e) {
                        Assertions.fail("failed to parse ");
                    }
                    actual.add(r);
                }
            } catch (SQLException | RelationalException e) {
                throw new RuntimeException(e);
            }
            // 2/2 make sure we've received everything
            Collection<Restaurant.RestaurantRecord> expected = ImmutableList.copyOf(records);
            Assertions.assertEquals(expected.size(), actual.size());
            Assertions.assertTrue(actual.containsAll(expected)); // no dups
        });
    }

    @Test
    public void canIterateWithContinuation() throws RelationalException, SQLException {
        havingInsertedRecordsDo(10, (Iterable<Restaurant.RestaurantRecord> records, RelationalStatement s) -> {
            // 1/2 scan all records
            List<Restaurant.RestaurantRecord> actual = new ArrayList<>();
            RelationalResultSet resultSet = null;
            try {
                TableScan scan = TableScan.newBuilder().withTableName("RestaurantRecord")
                        .setScanProperties(QueryProperties.newBuilder().setRowLimit(1).build()).build();
                resultSet = s.executeScan(scan, Options.create());
                while (true) {
                    while (resultSet.next()) {
                        Assertions.assertTrue(resultSet.supportsMessageParsing());
                        Message m = resultSet.parseMessage();
                        Restaurant.RestaurantRecord r = null;
                        try {
                            r = Restaurant.RestaurantRecord.parseFrom(m.toByteArray());
                        } catch (InvalidProtocolBufferException e) {
                            Assertions.fail("failed to parse ");
                        }
                        actual.add(r);
                    }
                    if (resultSet.terminatedEarly()) {
                        resultSet.close();
                        resultSet = s.executeScan(scan,
                                Options.create().withOption(OperationOption.continuation(resultSet.getContinuation())));
                    } else {
                        resultSet.close();
                        break;
                    }
                }
            } catch (SQLException | RelationalException e) {
                throw new RuntimeException(e);
            } finally {
                if (resultSet != null) {
                    try {
                        resultSet.close();
                    } catch (SQLException e) {
                        Assertions.fail("Could not close resultSet", e);
                    }
                }
            }
            // 2/2 make sure we've received everything
            Collection<Restaurant.RestaurantRecord> expected = ImmutableList.copyOf(records);
            Assertions.assertEquals(expected.size(), actual.size());
            Assertions.assertTrue(actual.containsAll(expected)); // no dups
        });
    }

    @Test
    public void continuationOnEdgesOfRecordCollection() throws RelationalException, SQLException {

        havingInsertedRecordsDo(3, (Iterable<Restaurant.RestaurantRecord> records, RelationalStatement s) -> {
            RelationalResultSet resultSet = null;
            try {
                TableScan scan = TableScan.newBuilder().withTableName("RestaurantRecord").build();
                resultSet = s.executeScan(scan, Options.create());

                // get continuation before iterating on the result set (should point to the first record).
                Continuation beginContinuation = resultSet.getContinuation();

                resultSet.next();
                Restaurant.RestaurantRecord firstRecord = Restaurant.RestaurantRecord.parseFrom(resultSet.parseMessage().toByteArray());
                // get continuation at the first (should point to the second record).
                final Continuation firstContinuation = resultSet.getContinuation();

                resultSet.next();
                Restaurant.RestaurantRecord secondRecord = Restaurant.RestaurantRecord.parseFrom(resultSet.parseMessage().toByteArray());
                // get continuation at the second element (should point to third).
                final Continuation secondContinuation = resultSet.getContinuation();

                resultSet.next();
                Restaurant.RestaurantRecord lastRecord = Restaurant.RestaurantRecord.parseFrom(resultSet.parseMessage().toByteArray());
                // get continuation at the last record (should point to FINISHED).
                Continuation lastContinuation = resultSet.getContinuation();

                // verify
                Restaurant.RestaurantRecord resumedFirstRecord = readFirstRecordWithContinuation(s, beginContinuation);
                Assertions.assertEquals(firstRecord, resumedFirstRecord);

                Restaurant.RestaurantRecord resumedSecondRecord = readFirstRecordWithContinuation(s, firstContinuation);
                Assertions.assertEquals(secondRecord, resumedSecondRecord);

                Restaurant.RestaurantRecord resumedThirdRecord = readFirstRecordWithContinuation(s, secondContinuation);
                Assertions.assertEquals(lastRecord, resumedThirdRecord);

                Assertions.assertNull(lastContinuation.getBytes());
                Assertions.assertTrue(lastContinuation.atEnd());

            } catch (InvalidProtocolBufferException | RelationalException | SQLException e) {
                Assertions.fail("failed to parse ", e);
            } finally {

                if (resultSet != null) {
                    try {
                        resultSet.close();
                    } catch (SQLException e) {
                        Assertions.fail("Could not close resultSet", e);
                    }
                }
            }
        });
    }

    @Test
    public void continuationOnEmptyCollection() throws RelationalException, SQLException {
        havingInsertedRecordsDo(0, (Iterable<Restaurant.RestaurantRecord> records, RelationalStatement s) -> {
            RelationalResultSet resultSet = null;
            try {
                TableScan scan = TableScan.newBuilder().withTableName("RestaurantRecord").build();
                resultSet = s.executeScan(scan, Options.create());
                Continuation continuation = resultSet.getContinuation();
                Assertions.assertNull(continuation.getBytes());
                Assertions.assertTrue(continuation.atEnd());
                Assertions.assertTrue(continuation.atBeginning());
                Assertions.assertFalse(resultSet.next());
            } catch (RelationalException | SQLException e) {
                throw new RuntimeException(e);
            } finally {
                if (resultSet != null) {
                    try {
                        resultSet.close();
                    } catch (SQLException e) {
                        Assertions.fail("Could not close resultSet", e);
                    }
                }
            }
        });
    }

    // helper methods

    private void havingInsertedRecordsDo(int numRecords,
                                         BiConsumer<Iterable<Restaurant.RestaurantRecord>, RelationalStatement> test) throws RelationalException, SQLException {
        try (RelationalConnection conn = Relational.connect(URI.create("rlsc:embed:/insert_test"), Options.create())) {
            conn.setSchema("main");
            conn.beginTransaction();
            try (RelationalStatement s = conn.createStatement()) {

                // 1/2 add all records to table insert_test.main.Restaurant.RestaurantRecord
                Iterable<Restaurant.RestaurantRecord> records = Utils.generateRestaurantRecords(numRecords);
                int count = s.executeInsert("RestaurantRecord", records, Options.create());
                Assertions.assertEquals(numRecords, count);

                // 2/2 test logic follows
                test.accept(records, s);
            }
        }
    }

    private Restaurant.RestaurantRecord readFirstRecordWithContinuation(RelationalStatement s, Continuation c) throws SQLException, RelationalException {
        RelationalResultSet resultSet = null;
        try {
            TableScan scan = TableScan.newBuilder().withTableName("RestaurantRecord").build();
            resultSet = s.executeScan(scan, Options.create().withOption(OperationOption.continuation(c)));
            resultSet.next();
            return Restaurant.RestaurantRecord.parseFrom(resultSet.parseMessage().toByteArray());
        } catch (InvalidProtocolBufferException | SQLException e) {
            Assertions.fail("failed to parse ", e);
        } finally {
            if (resultSet != null) {
                resultSet.close();
            }
        }
        return null;
    }

}
