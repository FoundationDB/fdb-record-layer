/*
 * ExecutePropertyTests.java
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
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.RelationalDriver;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.exceptions.ContextualSQLException;
import com.apple.foundationdb.relational.recordlayer.ContinuationImpl;
import com.apple.foundationdb.relational.recordlayer.EmbeddedRelationalExtension;
import com.apple.foundationdb.relational.recordlayer.Utils;
import com.apple.foundationdb.relational.recordlayer.RelationalConnectionRule;
import com.apple.foundationdb.relational.recordlayer.RelationalStatementRule;
import com.apple.foundationdb.relational.utils.SimpleDatabaseRule;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.sql.DriverManager;
import java.util.List;

public class ExecutePropertyTests {

    /**
     * Tests here primarily evaluates the {@link RelationalResultSet} in light of the execution limits imposed on the
     * connection and query. The execution limits can be the limit on time, scanned number of bytes and scanned number
     * rows. In the current setup, these limits are not strictly enforced by some of the plan execution cursors, for
     * instance see {@link com.apple.foundationdb.record.provider.foundationdb.SplitHelper.KeyValueUnsplitter}. Hence,
     * an execution that has a scanned number of rows limit set can actually end up returning slightly more than that.
     * <p>
     * Some of the tests here are based on that fact and hard-code the mapping between prescribed limit and actual
     * returned number of result.
     * <p>
     * TODO (Sanitize ExecutePropertyTest in Relational)
     */

    private static final String schemaTemplate = "CREATE TABLE FOO(a bigint, name string, PRIMARY KEY(A))";

    @RegisterExtension
    @Order(0)
    public final EmbeddedRelationalExtension relationalExtension = new EmbeddedRelationalExtension();

    @RegisterExtension
    @Order(1)
    public final SimpleDatabaseRule database = new SimpleDatabaseRule(
            ExecutePropertyTests.class,
            schemaTemplate);

    @RegisterExtension
    @Order(2)
    public final RelationalConnectionRule connection = new RelationalConnectionRule(database::getConnectionUri)
            .withOptions(Options.NONE)
            .withSchema("TEST_SCHEMA");

    @RegisterExtension
    @Order(3)
    public final RelationalStatementRule statement = new RelationalStatementRule(connection);

    public ExecutePropertyTests() {
        Utils.enableCascadesDebugger();
    }

    private static List<Arguments> hitLimitOptions() {
        return List.of(
                Arguments.of(Options.Name.EXECUTION_SCANNED_ROWS_LIMIT, 1, 2),
                Arguments.of(Options.Name.EXECUTION_SCANNED_BYTES_LIMIT, 5L, 2),
                Arguments.of(Options.Name.EXECUTION_SCANNED_ROWS_LIMIT, 2, 3),
                Arguments.of(Options.Name.EXECUTION_SCANNED_ROWS_LIMIT, 7, 7)
        );
    }

    @ParameterizedTest(name = "[{0}:{1}], {2}")
    @MethodSource("hitLimitOptions")
    void hitLimitEveryRow(Options.Name optionName, Object optionValue, int expectedRowCountPerQuery) throws Exception {
        statement.executeUpdate("INSERT INTO FOO VALUES (10, '10'), (11, '11'), (12, '12'), (13, '13'), (14, '14'), (15, '15'), (16, '16')");
        Continuation continuation = ContinuationImpl.BEGIN;
        long nextCorrectResult = 10L;
        final var driver = (RelationalDriver) DriverManager.getDriver(database.getConnectionUri().toString());
        try (var conn = driver.connect(database.getConnectionUri(), Options.builder().withOption(optionName, optionValue).build())) {
            conn.setSchema("TEST_SCHEMA");
            while (!continuation.atEnd()) {
                String query = continuation.atBeginning() ? "SELECT * FROM FOO" : "EXECUTE CONTINUATION ?";
                try (var ps = conn.prepareStatement(query)) {
                    if (!continuation.atBeginning()) {
                        ps.setBytes(1, continuation.serialize());
                    }
                    try (final RelationalResultSet rs = ps.executeQuery()) {
                        for (int currentRowCount = 0; currentRowCount < expectedRowCountPerQuery; currentRowCount++) {
                            if (nextCorrectResult == 17L) {
                                break;
                            }
                            Assertions.assertThat(rs.next()).isTrue();
                            Assertions.assertThat(rs.getLong(1)).isEqualTo(nextCorrectResult);
                            Assertions.assertThat(rs.getString(2)).isEqualTo(String.valueOf(nextCorrectResult));
                            nextCorrectResult++;
                        }
                        try {
                            boolean hasNext = rs.next();
                            Assertions.assertThat(hasNext).isFalse().withFailMessage("We should either have exhausted the result set or we should fail because of a limit reached");
                            continuation = rs.getContinuation();
                        } catch (ContextualSQLException ex) {
                            continuation = rs.getContinuation();
                        } catch (Exception ex) {
                            Assertions.fail("Wrong type of exception: " + ex);
                        }
                    }
                }
            }
        }
        Assertions.assertThat(nextCorrectResult).isEqualTo(17L);
    }

    @Test
    public void multipleConnectionsDoNotAffectEachOthersLimit() throws Exception {
        statement.executeUpdate("INSERT INTO FOO VALUES (10, '10'), (11, '11'), (12, '12'), (13, '13'), (14, '14'), (15, '15'), (16, '16')");
        final var driver = (RelationalDriver) DriverManager.getDriver(database.getConnectionUri().toString());
        try (var conn1 = driver.connect(database.getConnectionUri(), Options.builder().withOption(Options.Name.EXECUTION_SCANNED_ROWS_LIMIT, 2).build());
                var conn2 = driver.connect(database.getConnectionUri(), Options.builder().withOption(Options.Name.EXECUTION_SCANNED_ROWS_LIMIT, 3).build())) {
            conn1.setSchema("TEST_SCHEMA");
            conn2.setSchema("TEST_SCHEMA");
            try (var ps1 = conn1.prepareStatement("SELECT * FROM FOO");
                    var ps2 = conn2.prepareStatement("SELECT * FROM FOO")) {
                try (final RelationalResultSet rs1 = ps1.executeQuery();
                        final RelationalResultSet rs2 = ps2.executeQuery()) {
                    Assertions.assertThat(rs1.next()).isTrue();
                    Assertions.assertThat(rs1.next()).isTrue();
                    Assertions.assertThat(rs1.next()).isTrue();
                    Assertions.assertThat(rs2.next()).isTrue();
                    Assertions.assertThat(rs2.next()).isTrue();
                    Assertions.assertThat(rs1.next()).isFalse();
                    Assertions.assertThat(rs2.next()).isTrue();
                    Assertions.assertThat(rs2.next()).isTrue();
                    Assertions.assertThat(rs2.next()).isFalse();
                    Assertions.assertThat(rs1.getContinuation().getReason()).isEqualTo(Continuation.Reason.TRANSACTION_LIMIT_REACHED);
                    Assertions.assertThat(rs2.getContinuation().getReason()).isEqualTo(Continuation.Reason.TRANSACTION_LIMIT_REACHED);
                }
            }
        }
    }

    @Test
    public void limitIsKeptAcrossMultipleQueriesWithinTheSameTransaction() throws Exception {
        statement.executeUpdate("INSERT INTO FOO VALUES (10, '10'), (11, '11'), (12, '12')");
        final var driver = (RelationalDriver) DriverManager.getDriver(database.getConnectionUri().toString());
        try (var conn = driver.connect(database.getConnectionUri(), Options.builder().withOption(Options.Name.EXECUTION_SCANNED_ROWS_LIMIT, 5).build())) {
            conn.setSchema("TEST_SCHEMA");
            conn.setAutoCommit(false);
            try (var ps = conn.prepareStatement("SELECT * FROM FOO")) {
                try (final RelationalResultSet rs = ps.executeQuery()) {
                    Assertions.assertThat(rs.next()).isTrue();
                    Assertions.assertThat(rs.next()).isTrue();
                    Assertions.assertThat(rs.next()).isTrue();
                }
                try (final RelationalResultSet rs = ps.executeQuery()) {
                    Assertions.assertThat(rs.next()).isTrue();
                    Assertions.assertThat(rs.next()).isTrue();
                }
                try (final RelationalResultSet rs = ps.executeQuery()) {
                    Assertions.assertThat(rs.next()).isTrue();
                    Assertions.assertThat(rs.next()).isTrue();
                    Assertions.assertThat(rs.next()).isFalse();
                    Assertions.assertThat(rs.getContinuation().getReason()).isEqualTo(Continuation.Reason.TRANSACTION_LIMIT_REACHED);
                }
            }
        }
    }

    @Test
    public void limitIsKeptAcrossMultipleQueriesWithinTheSameTransactionSecondQueryFailsRightAway() throws Exception {
        statement.executeUpdate("INSERT INTO FOO VALUES (10, '10'), (11, '11'), (12, '12')");
        final var driver = (RelationalDriver) DriverManager.getDriver(database.getConnectionUri().toString());
        try (var conn = driver.connect(database.getConnectionUri(), Options.builder().withOption(Options.Name.EXECUTION_SCANNED_ROWS_LIMIT, 1).build())) {
            conn.setSchema("TEST_SCHEMA");
            conn.setAutoCommit(false);
            try (var ps = conn.prepareStatement("SELECT * FROM FOO")) {
                try (final RelationalResultSet rs = ps.executeQuery()) {
                    Assertions.assertThat(rs.next()).isTrue();
                    Assertions.assertThat(rs.next()).isTrue();
                }
                try (final RelationalResultSet rs = ps.executeQuery()) {
                    Assertions.assertThat(rs.next()).isTrue();
                    Assertions.assertThat(rs.next()).isTrue();
                    Assertions.assertThat(rs.next()).isFalse();
                    Assertions.assertThat(rs.getContinuation().getReason()).isEqualTo(Continuation.Reason.TRANSACTION_LIMIT_REACHED);
                }
            }
        }
    }

    @Test
    public void limitIsResetWithNewTransaction() throws Exception {
        statement.executeUpdate("INSERT INTO FOO VALUES (10, '10'), (11, '11')");
        final var driver = (RelationalDriver) DriverManager.getDriver(database.getConnectionUri().toString());
        try (var conn = driver.connect(database.getConnectionUri(), Options.builder().withOption(Options.Name.EXECUTION_SCANNED_ROWS_LIMIT, 5).build())) {
            conn.setSchema("TEST_SCHEMA");
            conn.setAutoCommit(false);
            try (var ps = conn.prepareStatement("SELECT * FROM FOO")) {
                try (final RelationalResultSet rs = ps.executeQuery()) {
                    Assertions.assertThat(rs.next()).isTrue();
                    Assertions.assertThat(rs.next()).isTrue();
                }
                try (final RelationalResultSet rs = ps.executeQuery()) {
                    Assertions.assertThat(rs.next()).isTrue();
                    Assertions.assertThat(rs.next()).isTrue();
                }
            }
            conn.rollback();
            try (var ps = conn.prepareStatement("SELECT * FROM FOO")) {
                try (final RelationalResultSet rs = ps.executeQuery()) {
                    Assertions.assertThat(rs.next()).isTrue();
                    Assertions.assertThat(rs.next()).isTrue();
                }
                try (final RelationalResultSet rs = ps.executeQuery()) {
                    Assertions.assertThat(rs.next()).isTrue();
                    Assertions.assertThat(rs.next()).isTrue();
                }
            }
        }
    }
}
