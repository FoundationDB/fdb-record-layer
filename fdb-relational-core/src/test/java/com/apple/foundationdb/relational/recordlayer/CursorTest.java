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

import com.apple.foundationdb.relational.api.Continuation;
import com.apple.foundationdb.relational.api.DynamicMessageBuilder;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.Row;
import com.apple.foundationdb.relational.api.TableScan;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.RelationalStatement;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.utils.SimpleDatabaseRule;
import com.apple.foundationdb.relational.utils.TestSchemas;

import com.google.protobuf.Message;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.net.URI;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static com.apple.foundationdb.relational.utils.RelationalAssertions.assertThat;

public class CursorTest {
    @RegisterExtension
    @Order(0)
    public final EmbeddedRelationalExtension relationalExtension = new EmbeddedRelationalExtension();

    @RegisterExtension
    @Order(1)
    public final SimpleDatabaseRule database = new SimpleDatabaseRule(relationalExtension,
            URI.create("/" + CursorTest.class.getSimpleName()),
            TestSchemas.restaurant());

    @RegisterExtension
    @Order(2)
    public final RelationalConnectionRule connection = new RelationalConnectionRule(database::getConnectionUri)
            .withOptions(Options.NONE)
            .withSchema("testSchema");

    @RegisterExtension
    @Order(3)
    public final RelationalStatementRule statement = new RelationalStatementRule(connection);

    @Test
    public void canIterateOverAllResults() throws RelationalException {
        havingInsertedRecordsDo(10, (Iterable<Message> records, RelationalStatement s) -> {
            // 1/2 scan all records
            try (RelationalResultSet resultSet = s.executeScan(TableScan.newBuilder().withTableName("RestaurantRecord").build(),
                    Options.NONE)) {
                assertThat(resultSet).hasExactlyInAnyOrder(records);
            } catch (SQLException | RelationalException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Test
    public void canIterateWithContinuation() throws RelationalException {
        havingInsertedRecordsDo(10, (Iterable<Message> records, RelationalStatement s) -> {
            // 1/2 scan all records
            List<Row> actual = new ArrayList<>();
            RelationalResultSet resultSet = null;
            try {
                TableScan scan = TableScan.newBuilder().withTableName("RestaurantRecord").build();
                resultSet = s.executeScan(scan, Options.builder().withOption(Options.Name.ROW_LIMIT, 1).build());
                while (true) {
                    while (resultSet.next()) {
                        actual.add(resultSet.asRow());
                    }
                    if (!resultSet.getContinuation().atEnd()) {
                        resultSet.close();
                        resultSet = s.executeScan(scan, Options.builder().withOption(Options.Name.CONTINUATION, resultSet.getContinuation()).build());
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
            Collection<Row> expected = StreamSupport.stream(records.spliterator(), false).map(MessageTuple::new).collect(Collectors.toList());
            Assertions.assertEquals(expected.size(), actual.size());
            Assertions.assertTrue(actual.containsAll(expected)); // no dups
        });
    }

    @Test
    public void continuationOnEdgesOfRecordCollection() throws RelationalException, SQLException {

        havingInsertedRecordsDo(3, (Iterable<Message> records, RelationalStatement s) -> {
            RelationalResultSet resultSet = null;
            try {
                TableScan scan = TableScan.newBuilder().withTableName("RestaurantRecord").build();
                resultSet = s.executeScan(scan, Options.NONE);

                // get continuation before iterating on the result set (should point to the first record).
                Continuation beginContinuation = resultSet.getContinuation();

                resultSet.next();
                Row firstRecord = resultSet.asRow();
                // get continuation at the first (should point to the second record).
                final Continuation firstContinuation = resultSet.getContinuation();

                resultSet.next();
                Row secondRecord = resultSet.asRow();
                // get continuation at the second element (should point to third).
                final Continuation secondContinuation = resultSet.getContinuation();

                resultSet.next();
                Row lastRecord = resultSet.asRow();
                // get continuation at the last record (should point to FINISHED).
                Continuation lastContinuation = resultSet.getContinuation();

                // verify
                Row resumedFirstRecord = readFirstRecordWithContinuation(s, beginContinuation);
                Assertions.assertEquals(firstRecord, resumedFirstRecord);

                Row resumedSecondRecord = readFirstRecordWithContinuation(s, firstContinuation);
                Assertions.assertEquals(secondRecord, resumedSecondRecord);

                Row resumedThirdRecord = readFirstRecordWithContinuation(s, secondContinuation);
                Assertions.assertEquals(lastRecord, resumedThirdRecord);

                Assertions.assertNull(lastContinuation.getBytes());
                Assertions.assertTrue(lastContinuation.atEnd());

            } catch (RelationalException | SQLException e) {
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
        havingInsertedRecordsDo(0, (Iterable<Message> records, RelationalStatement s) -> {
            RelationalResultSet resultSet = null;
            try {
                TableScan scan = TableScan.newBuilder().withTableName("RestaurantRecord").build();
                resultSet = s.executeScan(scan, Options.NONE);
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
                                         BiConsumer<Iterable<Message>, RelationalStatement> test) throws RelationalException {
        // 1/2 add all records to table insert_test.main.Restaurant.RestaurantRecord
        Iterable<Message> records = Utils.generateRestaurantRecords(numRecords, statement);
        final DynamicMessageBuilder dataBuilder = statement.getDataBuilder("RestaurantRecord");
        Iterable<Message> convertedRecords = StreamSupport.stream(records.spliterator(), false)
                .map(m -> {
                    try {
                        return dataBuilder.convertMessage(m);
                    } catch (RelationalException e) {
                        throw new RuntimeException(e);
                    }
                })
                .collect(Collectors.toList());
        int count = statement.executeInsert("RestaurantRecord", convertedRecords);
        Assertions.assertEquals(numRecords, count);

        // 2/2 test logic follows
        test.accept(records, statement);
    }

    private Row readFirstRecordWithContinuation(RelationalStatement s, Continuation c) throws SQLException, RelationalException {
        RelationalResultSet resultSet = null;
        try {
            TableScan scan = TableScan.newBuilder().withTableName("RestaurantRecord").build();
            resultSet = s.executeScan(scan, Options.builder().withOption(Options.Name.CONTINUATION, c).build());
            resultSet.next();
            return resultSet.asRow();
        } catch (SQLException e) {
            Assertions.fail("failed to parse ", e);
        } finally {
            if (resultSet != null) {
                resultSet.close();
            }
        }
        return null;
    }
}
