/*
 * UpdateTest.java
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

package com.apple.foundationdb.relational.recordlayer.query;

import com.apple.foundationdb.relational.api.Continuation;
import com.apple.foundationdb.relational.api.EmbeddedRelationalArray;
import com.apple.foundationdb.relational.api.EmbeddedRelationalStruct;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.RelationalConnection;
import com.apple.foundationdb.relational.api.RelationalDriver;
import com.apple.foundationdb.relational.api.RelationalPreparedStatement;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.recordlayer.ContinuationImpl;
import com.apple.foundationdb.relational.recordlayer.EmbeddedRelationalConnection;
import com.apple.foundationdb.relational.recordlayer.EmbeddedRelationalExtension;
import com.apple.foundationdb.relational.recordlayer.LogAppenderRule;
import com.apple.foundationdb.relational.utils.ResultSetAssert;
import com.apple.foundationdb.relational.utils.SchemaTemplateRule;
import com.apple.foundationdb.relational.utils.SimpleDatabaseRule;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.Level;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.opentest4j.AssertionFailedError;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

public class UpdateTest {

    private static final String schemaTemplate =
            "CREATE TYPE AS STRUCT LatLong (latitude double, longitude double)" +
                    " CREATE TYPE AS STRUCT ReviewerStats (start_date bigint, school_name string, hometown string)" +
                    " CREATE TABLE RestaurantReviewer (id bigint, name string, email string, stats ReviewerStats, secrets bytes array, PRIMARY KEY(id))";

    @RegisterExtension
    @Order(0)
    public final EmbeddedRelationalExtension relationalExtension = new EmbeddedRelationalExtension();

    @RegisterExtension
    @Order(1)
    public final SimpleDatabaseRule database = new SimpleDatabaseRule(UpdateTest.class, schemaTemplate, new SchemaTemplateRule.SchemaTemplateOptions(true, true));

    @RegisterExtension
    @Order(4)
    public final LogAppenderRule logAppender = new LogAppenderRule("UpdateTestLogAppender", PlanGenerator.class, Level.DEBUG);

    @BeforeEach
    public void beforeEach() throws SQLException, RelationalException {
        insertRecords(10);
    }

    @Test
    void updateSimpleFieldWithContinuationTest() throws Exception {
        final var fieldToUpdate = "name";
        final Function<RelationalConnection, Object> updateValue = conn -> "blahText";
        final var expectedValue = updateValue.apply(null);
        testUpdateWithContinuationInternal(fieldToUpdate, updateValue, expectedValue);
    }

    @Test
    void updateSimpleFieldVerifyCacheTest() throws Exception {
        final var fieldToUpdate = "name";
        final Function<RelationalConnection, Object> updateValue = conn -> "blahText";
        final var expectedValue = updateValue.apply(null);
        testUpdateVerifyCacheInternal(fieldToUpdate, updateValue, expectedValue);
    }

    @Test
    void updateStructFieldWithContinuationTest() throws Exception {
        final var fieldToUpdate = "stats";
        final var expectedValue = EmbeddedRelationalStruct.newBuilder()
                .addLong("START_DATE", 123L)
                .addString("SCHOOL_NAME", "blah")
                .addString("HOMETOWN", "blah2")
                .build();
        final Function<RelationalConnection, Object> updateValue = conn -> {
            try {
                return conn.createStruct("ReviewerStats", new Object[]{123L, "blah", "blah2"});
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        };
        testUpdateWithContinuationInternal(fieldToUpdate, updateValue, expectedValue);
    }

    @Test
    void updateStructFieldVerifyCacheTest() throws Exception {
        final var fieldToUpdate = "stats";
        final var expectedValue = EmbeddedRelationalStruct.newBuilder()
                .addLong("START_DATE", 123L)
                .addString("SCHOOL_NAME", "blah")
                .addString("HOMETOWN", "blah2")
                .build();
        final Function<RelationalConnection, Object> updateValue = conn -> {
            try {
                return conn.createStruct("ReviewerStats", new Object[]{123L, "blah", "blah2"});
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        };
        testUpdateVerifyCacheInternal(fieldToUpdate, updateValue, expectedValue);
    }

    @Test
    void updateArrayFieldWithContinuationTest() throws Exception {
        final var fieldToUpdate = "secrets";
        final var array = List.of(new byte[]{1, 2, 3, 4}, new byte[]{5, 6, 7, 8});
        final Function<RelationalConnection, Object> updateValue = conn -> {
            try {
                return conn.createArrayOf("BINARY", array.stream().map(t -> Arrays.copyOf(t, t.length)).toArray());
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        };
        final var expectedValue = EmbeddedRelationalArray.newBuilder()
                .addBytes(array.get(0))
                .addBytes(array.get(1))
                .build();
        testUpdateWithContinuationInternal(fieldToUpdate, updateValue, expectedValue);
    }

    @Test
    void updateArrayFieldVerifyCacheTest() throws Exception {
        final var fieldToUpdate = "secrets";
        final var array = List.of(new byte[]{1, 2, 3, 4}, new byte[]{5, 6, 7, 8});
        final Function<RelationalConnection, Object> updateValue = conn -> {
            try {
                return conn.createArrayOf("BINARY", array.stream().map(t -> Arrays.copyOf(t, t.length)).toArray());
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        };
        final var expectedValue = EmbeddedRelationalArray.newBuilder()
                .addBytes(array.get(0))
                .addBytes(array.get(1))
                .build();
        testUpdateVerifyCacheInternal(fieldToUpdate, updateValue, expectedValue);

    }

    public void insertRecords(int numRecords) throws RelationalException, SQLException {
        try (final var con = DriverManager.getConnection(database.getConnectionUri().toString())) {
            con.setSchema(database.getSchemaName());
            final var builder = new StringBuilder("INSERT INTO RestaurantReviewer(id) VALUES");
            for (int i = 0; i < numRecords; i++) {
                builder.append(" (").append(i).append(")");
                if (i != numRecords - 1) {
                    builder.append(",");
                }
            }
            con.createStatement().execute(builder.toString());
        }
    }

    private static RelationalPreparedStatement prepareUpdate(RelationalConnection conn, String updateField, Object param, Continuation continuation) throws SQLException {
        if (continuation.atBeginning()) {
            final var statement = conn.prepareStatement("UPDATE RestaurantReviewer SET " + updateField + " = ?param WHERE id >= 0 RETURNING NEW." + updateField + ", NEW.id");
            statement.setObject("param", param);
            return statement;
        } else {
            final var statement = conn.prepareStatement("EXECUTE CONTINUATION ?cont");
            statement.setObject("cont", continuation.serialize());
            return statement;
        }
    }

    private void verifyUpdates(String updatedField, Object expectedValue, int updatedUpTill) throws SQLException {
        try (final var con = DriverManager.getConnection(database.getConnectionUri().toString()).unwrap(RelationalConnection.class)) {
            con.setSchema(database.getSchemaName());
            final var statement = con.prepareStatement("SELECT id, " + updatedField + " from RestaurantReviewer WHERE id >= 0");
            try (final var resultSet = statement.executeQuery()) {
                final var resultSetAssert = ResultSetAssert.assertThat(resultSet);
                for (long i = 0; i < 10; i++) {
                    if (i < updatedUpTill) {
                        resultSetAssert.hasNextRow().hasColumn("ID", i).hasColumn(updatedField.toUpperCase(), expectedValue);
                    } else {
                        resultSetAssert.hasNextRow().hasColumn(updatedField.toUpperCase(), null);
                    }
                }
            }
        }
    }

    private Pair<Continuation, Integer> updateWithScanRowLimit(final String fieldToUpdate, final Function<RelationalConnection, Object> updateValue,
                                                               Object expectedValue) throws SQLException, RelationalException {
        return updateWithScanRowLimit(fieldToUpdate, updateValue, expectedValue, Options.NONE);
    }

    private Pair<Continuation, Integer> updateWithScanRowLimit(final String fieldToUpdate, final Function<RelationalConnection, Object> updateValue,
                                                               Object expectedValue, Options options) throws SQLException, RelationalException {
        return updateWithScanRowLimit(fieldToUpdate, updateValue, expectedValue, Pair.of(ContinuationImpl.BEGIN, 0), options);
    }

    private Pair<Continuation, Integer> updateWithScanRowLimit(final String fieldToUpdate, final Function<RelationalConnection, Object> updateValue,
                                                               Object expectedValue, Pair<Continuation, Integer> continuationAndNumUpdated,
                                                               Options options) throws SQLException, RelationalException {
        var continuation = continuationAndNumUpdated.getLeft();
        var updatedUpTill = continuationAndNumUpdated.getRight();
        final var driver = (RelationalDriver) DriverManager.getDriver(database.getConnectionUri().toString());
        try (final var con = (EmbeddedRelationalConnection) driver.connect(database.getConnectionUri(), options)) {
            con.setSchema(database.getSchemaName());
            final var statement = prepareUpdate(con, fieldToUpdate, updateValue.apply(con), continuation);
            try (final var resultSet = statement.executeQuery()) {
                final var resultSetAssert = ResultSetAssert.assertThat(resultSet);
                while (true) {
                    try {
                        resultSetAssert.hasNextRow().hasColumn("ID", (long) updatedUpTill++).hasColumn(fieldToUpdate.toUpperCase(), expectedValue);
                    } catch (AssertionFailedError e) {
                        resultSetAssert.hasNoNextRow();
                        continuation = resultSet.getContinuation();
                        break;
                    }
                }
            }
        }
        return Pair.of(continuation, updatedUpTill);
    }

    private void testUpdateWithContinuationInternal(String fieldToUpdate, Function<RelationalConnection, Object> updateValue, Object expectedValue)
            throws SQLException, RelationalException {
        var continuationAndNumUpdated = updateWithScanRowLimit(fieldToUpdate, updateValue, expectedValue,
                Options.builder().withOption(Options.Name.EXECUTION_SCANNED_ROWS_LIMIT, 2).build());
        verifyUpdates(fieldToUpdate, expectedValue, continuationAndNumUpdated.getRight());
        continuationAndNumUpdated = updateWithScanRowLimit(fieldToUpdate, updateValue, expectedValue, continuationAndNumUpdated, Options.NONE);
        Assertions.assertThat(continuationAndNumUpdated.getRight()).isEqualTo(10);
        verifyUpdates(fieldToUpdate, expectedValue, 10);
    }

    private void testUpdateVerifyCacheInternal(String fieldToUpdate, Function<RelationalConnection, Object> updateValue, Object expectedValue)
            throws SQLException, RelationalException {
        updateWithScanRowLimit(fieldToUpdate, updateValue, expectedValue);
        Assertions.assertThat(logAppender.getLastLogEventMessage()).contains("planCache=\"miss\"");
        updateWithScanRowLimit(fieldToUpdate, updateValue, expectedValue);
        Assertions.assertThat(logAppender.getLastLogEventMessage()).contains("planCache=\"hit\"");
    }
}
