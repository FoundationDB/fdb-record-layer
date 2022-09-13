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
import com.apple.foundationdb.relational.api.ImmutableRowStruct;
import com.apple.foundationdb.relational.api.KeySet;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.Row;
import com.apple.foundationdb.relational.api.StructMetaData;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.RelationalStatement;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.utils.ResultSetAssert;
import com.apple.foundationdb.relational.utils.ResultSetTestUtils;
import com.apple.foundationdb.relational.utils.SimpleDatabaseRule;
import com.apple.foundationdb.relational.utils.TestSchemas;
import com.apple.foundationdb.relational.utils.RelationalStructAssert;

import com.google.protobuf.Message;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.net.URI;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

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
            .withSchema("TEST_SCHEMA");

    @RegisterExtension
    @Order(3)
    public final RelationalStatementRule statement = new RelationalStatementRule(connection);

    @Test
    public void canIterateOverAllResults() throws RelationalException {
        havingInsertedRecordsDo(10, (Iterable<Message> records, RelationalStatement s) -> {
            // 1/2 scan all records
            try (RelationalResultSet resultSet = s.executeScan("RESTAURANT", new KeySet(), Options.NONE)) {
                ResultSetAssert.assertThat(resultSet).containsRowsExactly(records);
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
            StructMetaData metaData = null;
            try {
                Continuation cont = Continuation.BEGIN;
                while (!cont.atEnd()) {
                    try (RelationalResultSet resultSet = s.executeScan("RESTAURANT", new KeySet(),
                            Options.builder().withOption(Options.Name.CONTINUATION, cont).withOption(Options.Name.CONTINUATION_PAGE_SIZE, 1).build())) {
                        metaData = resultSet.getMetaData().unwrap(StructMetaData.class);
                        while (resultSet.next()) {
                            actual.add(ResultSetTestUtils.currentRow(resultSet));
                        }

                        cont = resultSet.getContinuation();
                    }
                }
            } catch (SQLException | RelationalException e) {
                throw new RuntimeException(e);
            }
            RelationalResultSet actualResults = new IteratorResultSet(metaData, actual.iterator(), 0);
            ResultSetAssert.assertThat(actualResults).containsRowsExactly(records);
        });
    }

    @Test
    public void continuationOnEdgesOfRecordCollection() throws RelationalException {

        havingInsertedRecordsDo(3, (Iterable<Message> records, RelationalStatement s) -> {
            try (RelationalResultSet resultSet = s.executeScan("RESTAURANT", new KeySet(), Options.NONE)) {
                // get continuation before iterating on the result set (should point to the first record).
                Continuation continuation = resultSet.getContinuation();
                Assertions.assertEquals(Continuation.BEGIN, continuation, "Incorrect starting continuation!");

                StructMetaData smd = resultSet.getMetaData().unwrap(StructMetaData.class);
                boolean called = false;
                while (resultSet.next()) {
                    called = true;
                    Row resumedRow = readFirstRecordWithContinuation(s, continuation);
                    Row mainRow = ResultSetTestUtils.currentRow(resultSet);
                    RelationalStructAssert.assertThat(new ImmutableRowStruct(mainRow, smd)).isEqualTo(new ImmutableRowStruct(resumedRow, smd));

                    continuation = resultSet.getContinuation();
                }

                Assertions.assertTrue(called, "Did not return any records!");

                // get continuation at the last record (should point to FINISHED).
                Continuation lastContinuation = resultSet.getContinuation();

                // verify
                Assertions.assertTrue(lastContinuation.atEnd());
                Assertions.assertEquals(lastContinuation.getBytes(), Continuation.END.getBytes());

            } catch (RelationalException | SQLException e) {
                Assertions.fail("failed to parse ", e);
            }
        });
    }

    @Test
    public void continuationOnEmptyCollection() throws RelationalException {
        havingInsertedRecordsDo(0, (Iterable<Message> records, RelationalStatement s) -> {
            RelationalResultSet resultSet = null;
            try {
                resultSet = s.executeScan("RESTAURANT", new KeySet(), Options.NONE);
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
        // 1/2 add all records to table insert_test.main.Restaurant.RESTAURANT
        Iterable<Message> records = Utils.generateRestaurantRecords(numRecords, statement);
        final DynamicMessageBuilder dataBuilder = statement.getDataBuilder("RESTAURANT");
        Iterable<Message> convertedRecords = StreamSupport.stream(records.spliterator(), false)
                .map(m -> {
                    try {
                        return dataBuilder.convertMessage(m);
                    } catch (RelationalException e) {
                        throw new RuntimeException(e);
                    }
                })
                .collect(Collectors.toList());
        int count = statement.executeInsert("RESTAURANT", convertedRecords);
        Assertions.assertEquals(numRecords, count);

        // 2/2 test logic follows
        test.accept(records, statement);
    }

    private Row readFirstRecordWithContinuation(RelationalStatement s, Continuation c) throws SQLException, RelationalException {
        try (RelationalResultSet resultSet = s.executeScan("RESTAURANT", new KeySet(),
                Options.builder().withOption(Options.Name.CONTINUATION, c).build())) {
            resultSet.next();
            return ResultSetTestUtils.currentRow(resultSet);
        }
    }
}
