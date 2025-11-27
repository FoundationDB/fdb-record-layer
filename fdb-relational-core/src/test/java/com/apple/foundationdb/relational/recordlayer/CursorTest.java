/*
 * CursorTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2025 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.relational.api.KeySet;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.Row;
import com.apple.foundationdb.relational.api.StructMetaData;
import com.apple.foundationdb.relational.api.RelationalConnection;
import com.apple.foundationdb.relational.api.RelationalDriver;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.RelationalStatement;
import com.apple.foundationdb.relational.api.RelationalStruct;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.utils.ResultSetAssert;
import com.apple.foundationdb.relational.utils.ResultSetTestUtils;
import com.apple.foundationdb.relational.utils.SimpleDatabaseRule;
import com.apple.foundationdb.relational.utils.TestSchemas;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;

public class CursorTest {
    @RegisterExtension
    @Order(0)
    public final EmbeddedRelationalExtension relationalExtension = new EmbeddedRelationalExtension();

    @RegisterExtension
    @Order(1)
    public final SimpleDatabaseRule database = new SimpleDatabaseRule(
            CursorTest.class,
            TestSchemas.restaurant());

    @Test
    public void canIterateOverAllResults() throws SQLException, RelationalException {
        insertRecordsAndTest(10, (List<RelationalStruct> records, RelationalConnection conn) -> {
            // 1/2 scan all records
            try (RelationalResultSet resultSet = conn.createStatement().executeScan("RESTAURANT", new KeySet(), Options.NONE)) {
                ResultSetAssert.assertThat(resultSet).containsRowsPartly(records.toArray(new RelationalStruct[]{}));
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Test
    public void canIterateWithContinuation() throws SQLException, RelationalException {
        insertRecordsAndTest(10, (List<RelationalStruct> records, RelationalConnection conn) -> {
            // 1/2 scan all records
            List<Row> actual = new ArrayList<>();
            StructMetaData metaData = null;
            try {
                Continuation cont = ContinuationImpl.BEGIN;
                while (!cont.atEnd()) {
                    try (RelationalStatement statement = conn.createStatement()) {
                        statement.setMaxRows(1);
                        try (RelationalResultSet resultSet = statement.executeScan("RESTAURANT", new KeySet(),
                                Options.builder().withOption(Options.Name.CONTINUATION, cont).build())) {
                            metaData = resultSet.getMetaData();
                            while (resultSet.next()) {
                                actual.add(ResultSetTestUtils.currentRow(resultSet));
                            }

                            cont = resultSet.getContinuation();
                        }
                    }
                }
                RelationalResultSet actualResults = new IteratorResultSet(metaData, actual.iterator(), 0);
                ResultSetAssert.assertThat(actualResults).containsRowsPartly(records.toArray(new RelationalStruct[]{}));
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Test
    public void continuationOnEdgesOfRecordCollection() throws SQLException, RelationalException {
        final int numRecords = 3;
        insertRecordsAndTest(numRecords, (List<RelationalStruct> records, RelationalConnection conn) -> {
            try (RelationalResultSet resultSet = conn.createStatement().executeScan("RESTAURANT", new KeySet(), Options.NONE)) {
                // get continuation before iterating on the result set (should fail).
                Assertions.assertThrows(SQLException.class, resultSet::getContinuation);

                StructMetaData smd = resultSet.getMetaData();
                int called = 0;
                while (resultSet.next()) {
                    // Continuation is not available until the result set is exhausted
                    if (++called < numRecords) {
                        Assertions.assertThrows(SQLException.class, resultSet::getContinuation);
                    }
                }

                Assertions.assertTrue(called > 0, "Did not return any records!");

                // get continuation at the last record (should point to FINISHED).
                Continuation lastContinuation = resultSet.getContinuation();

                // verify
                Assertions.assertTrue(lastContinuation.atEnd());
                Assertions.assertEquals(lastContinuation.getExecutionState(), ContinuationImpl.END.getExecutionState());

            } catch (SQLException e) {
                Assertions.fail("failed to parse ", e);
            }
        });
    }

    @Test
    public void continuationOnEmptyCollection() throws SQLException, RelationalException {
        insertRecordsAndTest(0, (List<RelationalStruct> records, RelationalConnection conn) -> {
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
        insertRecordsAndTest(10, (List<RelationalStruct> records, RelationalConnection conn) -> {
            Continuation continuation;
            try (final var s = conn.createStatement()) {
                s.setMaxRows(5);
                try (final var resultSet = s.executeQuery("select * from RESTAURANT")) {
                    Assertions.assertThrows(SQLException.class, resultSet::getContinuation);
                    final var resultSetAssert = ResultSetAssert.assertThat(resultSet);
                    for (int i = 0; i < 5; i++) {
                        resultSetAssert.hasNextRow();
                    }
                    resultSetAssert.hasNoNextRow().continuationReasonIs(Continuation.Reason.QUERY_EXECUTION_LIMIT_REACHED);
                    continuation = resultSet.getContinuation();
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
            try (final var preparedStatement = conn.prepareStatement("EXECUTE CONTINUATION ?param")) {
                preparedStatement.setBytes("param", continuation.serialize());
                try (final var resultSet = preparedStatement.executeQuery()) {
                    Assertions.assertThrows(SQLException.class, resultSet::getContinuation);
                    final var resultSetAssert = ResultSetAssert.assertThat(resultSet);
                    for (int i = 0; i < 5; i++) {
                        resultSetAssert.hasNextRow();
                    }
                    resultSetAssert.hasNoNextRow();
                    Assertions.assertEquals(Continuation.Reason.CURSOR_AFTER_LAST, resultSet.getContinuation().getReason());
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
        final var driver = (RelationalDriver) DriverManager.getDriver(database.getConnectionUri().toString());
        try (final var conn = driver.connect(database.getConnectionUri(), Options.builder().withOption(Options.Name.EXECUTION_SCANNED_ROWS_LIMIT, 3).build())) {
            conn.setSchema(database.getSchemaName());
            try (final var resultSet = conn.createStatement().executeQuery("select * from RESTAURANT")) {
                Assertions.assertThrows(SQLException.class, resultSet::getContinuation);
                while (true) {
                    if (resultSet.next()) {
                        numRowsReturned++;
                    } else {
                        continuation = resultSet.getContinuation();
                        Assertions.assertEquals(Continuation.Reason.TRANSACTION_LIMIT_REACHED, continuation.getReason());
                        break;
                    }
                }
            }
        }
        // 2. Further count the rows in other execution without limits and see if total number of rows is 10
        try (final var conn = DriverManager.getConnection(database.getConnectionUri().toString()).unwrap(RelationalConnection.class)) {
            conn.setSchema(database.getSchemaName());
            try (final var preparedStatement = conn.prepareStatement("EXECUTE CONTINUATION ?param")) {
                preparedStatement.setBytes("param", continuation.serialize());
                try (final var resultSet = preparedStatement.executeQuery()) {
                    Assertions.assertThrows(SQLException.class, resultSet::getContinuation);
                    while (true) {
                        if (resultSet.next()) {
                            numRowsReturned++;
                        } else {
                            Assertions.assertEquals(10, numRowsReturned);
                            Assertions.assertEquals(Continuation.Reason.CURSOR_AFTER_LAST, resultSet.getContinuation().getReason());
                            break;
                        }
                    }
                }
            }
        }
    }

    // helper methods

    private void insertRecordsAndTest(int numRecords,
                                      BiConsumer<List<RelationalStruct>, RelationalConnection> test) throws SQLException, RelationalException {
        final var records = insertAndReturnRecords(numRecords);
        try (final var con = DriverManager.getConnection(database.getConnectionUri().toString()).unwrap(RelationalConnection.class)) {
            con.setSchema(database.getSchemaName());
            test.accept(records, con);
        }
    }

    private List<RelationalStruct> insertAndReturnRecords(int numRecords) throws SQLException {
        List<RelationalStruct> records;
        try (final var con = DriverManager.getConnection(database.getConnectionUri().toString()).unwrap(RelationalConnection.class)) {
            con.setSchema(database.getSchemaName());
            final var statement = con.createStatement();
            records = Utils.generateRestaurantRecords(numRecords);
            int count = statement.executeInsert("RESTAURANT", records);
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
