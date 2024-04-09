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
import com.apple.foundationdb.relational.api.Relational;
import com.apple.foundationdb.relational.api.RelationalConnection;
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
            CursorTest.class,
            TestSchemas.restaurant());

    @Test
    public void canIterateOverAllResults() throws SQLException, RelationalException {
        insertRecordsAndTest(10, (Iterable<Message> records, RelationalConnection conn) -> {
            // 1/2 scan all records
            try (RelationalResultSet resultSet = conn.createStatement().executeScan("RESTAURANT", new KeySet(), Options.NONE)) {
                ResultSetAssert.assertThat(resultSet).containsRowsExactly(records);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Test
    public void canIterateWithContinuation() throws SQLException, RelationalException {
        insertRecordsAndTest(10, (Iterable<Message> records, RelationalConnection conn) -> {
            // 1/2 scan all records
            List<Row> actual = new ArrayList<>();
            StructMetaData metaData = null;
            try {
                Continuation cont = ContinuationImpl.BEGIN;
                while (!cont.atEnd()) {
                    try (RelationalResultSet resultSet = conn.createStatement().executeScan("RESTAURANT", new KeySet(),
                            Options.builder().withOption(Options.Name.CONTINUATION, cont).withOption(Options.Name.CONTINUATION_PAGE_SIZE, 1).build())) {
                        metaData = resultSet.getMetaData().unwrap(StructMetaData.class);
                        while (resultSet.next()) {
                            actual.add(ResultSetTestUtils.currentRow(resultSet));
                        }

                        cont = resultSet.getContinuation();
                    }
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
            RelationalResultSet actualResults = new IteratorResultSet(metaData, actual.iterator(), 0);
            ResultSetAssert.assertThat(actualResults).containsRowsExactly(records);
        });
    }

    @Test
    public void continuationOnEdgesOfRecordCollection() throws SQLException, RelationalException {
        insertRecordsAndTest(3, (Iterable<Message> records, RelationalConnection conn) -> {
            try (RelationalResultSet resultSet = conn.createStatement().executeScan("RESTAURANT", new KeySet(), Options.NONE)) {
                // get continuation before iterating on the result set (should point to the first record).
                Continuation continuation = resultSet.getContinuation();
                Assertions.assertEquals(ContinuationImpl.BEGIN, continuation, "Incorrect starting continuation!");

                StructMetaData smd = resultSet.getMetaData().unwrap(StructMetaData.class);
                boolean called = false;
                while (resultSet.next()) {
                    called = true;
                    Row resumedRow = readFirstRecordWithContinuation(conn.createStatement(), continuation);
                    Row mainRow = ResultSetTestUtils.currentRow(resultSet);
                    RelationalStructAssert.assertThat(new ImmutableRowStruct(mainRow, smd)).isEqualTo(new ImmutableRowStruct(resumedRow, smd));

                    continuation = resultSet.getContinuation();
                }

                Assertions.assertTrue(called, "Did not return any records!");

                // get continuation at the last record (should point to FINISHED).
                Continuation lastContinuation = resultSet.getContinuation();

                // verify
                Assertions.assertTrue(lastContinuation.atEnd());
                Assertions.assertEquals(lastContinuation.getExecutionState(), ContinuationImpl.END.getExecutionState());

            } catch (RelationalException | SQLException e) {
                Assertions.fail("failed to parse ", e);
            }
        });
    }

    @Test
    public void continuationOnEmptyCollection() throws SQLException, RelationalException {
        insertRecordsAndTest(0, (Iterable<Message> records, RelationalConnection conn) -> {
            RelationalResultSet resultSet = null;
            try {
                resultSet = conn.createStatement().executeScan("RESTAURANT", new KeySet(), Options.NONE);
                Assertions.assertFalse(resultSet.next());
                Continuation continuation = resultSet.getContinuation();
                Assertions.assertEquals(0, continuation.getExecutionState().length);
                Assertions.assertTrue(continuation.atEnd());
                Assertions.assertFalse(continuation.atBeginning());
                Assertions.assertFalse(resultSet.next());
            } catch (SQLException e) {
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

    @Test
    public void continuationWithReturnRowLimit() throws SQLException, RelationalException {
        insertRecordsAndTest(10, (Iterable<Message> records, RelationalConnection conn) -> {
            Continuation continuation;
            try (final var resultSet = conn.createStatement().executeQuery("select * from RESTAURANT limit 5")) {
                Assertions.assertTrue(resultSet.getContinuation().atBeginning());
                final var resultSetAssert = ResultSetAssert.assertThat(resultSet);
                for (int i = 0; i < 5; i++) {
                    resultSetAssert.hasNextRow();
                }
                resultSetAssert.hasNoNextRow().hasNoNextRowReasonAsNoMoreRows();
                continuation = resultSet.getContinuation();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
            try (final var preparedStatement = conn.prepareStatement("select * from RESTAURANT with continuation ?param")) {
                preparedStatement.setBytes("param", continuation.serialize());
                try (final var resultSet = preparedStatement.executeQuery()) {
                    Assertions.assertTrue(resultSet.getContinuation().atBeginning());
                    final var resultSetAssert = ResultSetAssert.assertThat(resultSet);
                    for (int i = 0; i < 5; i++) {
                        resultSetAssert.hasNextRow();
                    }
                    resultSetAssert.hasNoNextRow().hasNoNextRowReasonAsNoMoreRows();
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Test
    public void continuationWithScanRowLimit() throws SQLException, RelationalException {
        insertAndReturnRecords(10);
        Continuation continuation;
        int numRowsReturned = 0;
        // 1. Iterate over and count the rows returned before the scan rows limit is hit
        try (final var conn = Relational.connect(database.getConnectionUri(), Options.builder().withOption(Options.Name.EXECUTION_SCANNED_ROWS_LIMIT, 3).build())) {
            conn.setSchema(database.getSchemaName());
            conn.beginTransaction();
            try (final var resultSet = conn.createStatement().executeQuery("select * from RESTAURANT")) {
                Assertions.assertTrue(resultSet.getContinuation().atBeginning());
                while (true) {
                    if (resultSet.next()) {
                        numRowsReturned++;
                    } else {
                        Assertions.assertEquals(resultSet.noNextRowReason(), RelationalResultSet.NoNextRowReason.EXEC_LIMIT_REACHED);
                        continuation = resultSet.getContinuation();
                        break;
                    }
                }
            }
        }
        // 2. Further count the rows in other execution without limits and see if total number of rows is 10
        try (final var conn = Relational.connect(database.getConnectionUri(), Options.NONE)) {
            conn.setSchema(database.getSchemaName());
            conn.beginTransaction();
            try (final var preparedStatement = conn.prepareStatement("select * from RESTAURANT with continuation ?param")) {
                preparedStatement.setBytes("param", continuation.serialize());
                try (final var resultSet = preparedStatement.executeQuery()) {
                    Assertions.assertTrue(resultSet.getContinuation().atBeginning());
                    while (true) {
                        if (resultSet.next()) {
                            numRowsReturned++;
                        } else {
                            Assertions.assertEquals(resultSet.noNextRowReason(), RelationalResultSet.NoNextRowReason.NO_MORE_ROWS);
                            Assertions.assertEquals(10, numRowsReturned);
                            break;
                        }
                    }
                }
            }
        }
    }

    // helper methods

    private void insertRecordsAndTest(int numRecords,
                                      BiConsumer<Iterable<Message>, RelationalConnection> test) throws SQLException, RelationalException {
        final var records = insertAndReturnRecords(numRecords);
        try (final var con = Relational.connect(database.getConnectionUri(), Options.NONE)) {
            con.setSchema(database.getSchemaName());
            con.beginTransaction();
            test.accept(records, con);
        }
    }

    private Iterable<Message> insertAndReturnRecords(int numRecords) throws SQLException, RelationalException {
        Iterable<Message> records;
        try (final var con = Relational.connect(database.getConnectionUri(), Options.NONE)) {
            con.setSchema(database.getSchemaName());
            con.beginTransaction();
            final var statement = con.createStatement();
            records = Utils.generateRestaurantRecords(numRecords, statement);
            final DynamicMessageBuilder dataBuilder = statement.getDataBuilder("RESTAURANT");
            Iterable<Message> convertedRecords = StreamSupport.stream(records.spliterator(), false)
                    .map(m -> {
                        try {
                            return dataBuilder.convertMessage(m);
                        } catch (SQLException e) {
                            throw new RuntimeException(e);
                        }
                    })
                    .collect(Collectors.toList());
            int count = statement.executeInsert("RESTAURANT", convertedRecords);
            Assertions.assertEquals(numRecords, count);
        }
        return records;
    }

    private Row readFirstRecordWithContinuation(RelationalStatement s, Continuation c) throws SQLException, RelationalException {
        try (RelationalResultSet resultSet = s.executeScan("RESTAURANT", new KeySet(),
                Options.builder().withOption(Options.Name.CONTINUATION, c).build())) {
            resultSet.next();
            return ResultSetTestUtils.currentRow(resultSet);
        }
    }
}
