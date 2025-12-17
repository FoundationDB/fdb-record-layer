/*
 * AutoCommitTests.java
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

import com.apple.foundationdb.relational.api.EmbeddedRelationalDriver;
import com.apple.foundationdb.relational.api.EmbeddedRelationalStruct;
import com.apple.foundationdb.relational.api.KeySet;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.RelationalConnection;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.transactionbound.TransactionBoundEmbeddedRelationalEngine;
import com.apple.foundationdb.relational.utils.ResultSetAssert;
import com.apple.foundationdb.relational.utils.SimpleDatabaseRule;
import com.apple.foundationdb.relational.utils.TestSchemas;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import javax.annotation.Nonnull;
import java.net.URI;
import java.sql.SQLException;
import java.util.List;
import java.util.function.Consumer;

public class AutoCommitTests {
    @RegisterExtension
    @Order(0)
    public final EmbeddedRelationalExtension relationalExtension = new EmbeddedRelationalExtension();

    @RegisterExtension
    @Order(1)
    public final SimpleDatabaseRule database = new SimpleDatabaseRule(AutoCommitTests.class, TestSchemas.restaurant());

    @RegisterExtension
    @Order(2)
    public final RelationalConnectionRule connection = new RelationalConnectionRule(database::getConnectionUri)
            .withOptions(Options.NONE)
            .withSchema("TEST_SCHEMA");

    private EmbeddedRelationalDriver alternateDriver = new EmbeddedRelationalDriver(new TransactionBoundEmbeddedRelationalEngine());

    public AutoCommitTests() throws SQLException {
    }

    @BeforeEach
    public void setup() throws SQLException {
        try (final var statement = connection.createStatement()) {
            statement.execute("INSERT INTO RESTAURANT(REST_NO, NAME) VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e')");
        }
        try (final var statement = connection.createStatement()) {
            statement.executeUpdate("DROP DATABASE IF EXISTS /TEST/DB23434");
        }

    }

    public enum TransactionType {
        AUTO_COMMIT_ON,
        /**
         * AutoCommit is set to off. Tests call explicit commit after each statement is executed.
         */
        AUTO_COMMIT_OFF_WITH_EXPLICIT_COMMIT,
        /**
         * AutoCommit is set to off. Tests do not call commit ever.
         */
        AUTO_COMMIT_OFF_WITH_NO_COMMIT,
        /**
         * This mode makes use of {@link RecordStoreAndRecordContextTransaction} that creates a connection with an
         * ongoing transaction.
         */
        EXISTING_TRANSACTION
    }

    @ParameterizedTest
    @EnumSource
    public void simpleSelectExhaustedResultSet(TransactionType transactionType) throws SQLException, RelationalException {
        EmbeddedRelationalConnection conn = connection.getUnderlyingEmbeddedConnection();
        Assertions.assertFalse(conn.inActiveTransaction());
        if (transactionType == TransactionType.EXISTING_TRANSACTION) {
            conn = getConnectionWithExistingTransaction(conn, database.getConnectionUri(), alternateDriver);
        }
        setAutoCommit(conn, transactionType);
        try (final var statement = conn.createStatement()) {
            try (final var rs = statement.executeQuery("SELECT REST_NO, NAME FROM RESTAURANT")) {
                Assertions.assertTrue(conn.inActiveTransaction());
                ResultSetAssert.assertThat(rs)
                        .hasNextRow()
                        .hasNextRow()
                        .hasNextRow()
                        .hasNextRow()
                        .hasNextRow()
                        .hasNoNextRow();
            }
            checkOpenTransaction(conn, transactionType);
        }
        checkOpenTransaction(conn, transactionType);
        checkAutoCommitAndCommitIfRequired(conn, transactionType);
    }

    @ParameterizedTest
    @EnumSource
    public void simpleSelectNonExhaustedResultSet(TransactionType transactionType) throws SQLException, RelationalException {
        EmbeddedRelationalConnection conn = connection.getUnderlyingEmbeddedConnection();
        Assertions.assertFalse(conn.inActiveTransaction());
        if (transactionType == TransactionType.EXISTING_TRANSACTION) {
            conn = getConnectionWithExistingTransaction(conn, database.getConnectionUri(), alternateDriver);
        }
        setAutoCommit(conn, transactionType);
        try (final var statement = conn.createStatement()) {
            try (final var rs = statement.executeQuery("SELECT REST_NO, NAME FROM RESTAURANT")) {
                Assertions.assertTrue(conn.inActiveTransaction());
                ResultSetAssert.assertThat(rs)
                        .hasNextRow()
                        .hasNextRow();
            }
            checkOpenTransaction(conn, transactionType);
        }
        checkOpenTransaction(conn, transactionType);
        checkAutoCommitAndCommitIfRequired(conn, transactionType);
    }

    @ParameterizedTest
    @EnumSource
    public void simpleInsert(TransactionType transactionType) throws SQLException, RelationalException {
        EmbeddedRelationalConnection conn = connection.getUnderlyingEmbeddedConnection();
        Assertions.assertFalse(conn.inActiveTransaction());
        if (transactionType == TransactionType.EXISTING_TRANSACTION) {
            conn = getConnectionWithExistingTransaction(conn, database.getConnectionUri(), alternateDriver);
        }
        setAutoCommit(conn, transactionType);
        try (final var statement = conn.createStatement()) {
            final var hasResultSet = statement.execute("INSERT INTO RESTAURANT(REST_NO, NAME) VALUES (6, 'f')");
            Assertions.assertFalse(hasResultSet);
            checkOpenTransaction(conn, transactionType);
        }
        checkOpenTransaction(conn, transactionType);
        checkAutoCommitAndCommitIfRequired(conn, transactionType);
        // check operation succeeds
        try (final var statement = conn.createStatement()) {
            try (final var resultSet = statement.executeQuery("SELECT * FROM RESTAURANT WHERE REST_NO = 6")) {
                ResultSetAssert.assertThat(resultSet)
                        .hasNextRow()
                        .hasNoNextRow();
            }
        }
        checkOpenTransaction(conn, transactionType);
        checkAutoCommitAndCommitIfRequired(conn, transactionType);
    }

    @ParameterizedTest
    @EnumSource
    public void simpleUpdate(TransactionType transactionType) throws SQLException, RelationalException {
        EmbeddedRelationalConnection conn = connection.getUnderlyingEmbeddedConnection();
        Assertions.assertFalse(conn.inActiveTransaction());
        if (transactionType == TransactionType.EXISTING_TRANSACTION) {
            conn = getConnectionWithExistingTransaction(conn, database.getConnectionUri(), alternateDriver);
        }
        setAutoCommit(conn, transactionType);
        try (final var statement = conn.createStatement()) {
            final var hasResultSet = statement.execute("UPDATE RESTAURANT SET NAME = 'aa' WHERE REST_NO = 1");
            Assertions.assertFalse(hasResultSet);
            Assertions.assertEquals(1, statement.getUpdateCount());
            checkOpenTransaction(conn, transactionType);
        }
        checkOpenTransaction(conn, transactionType);
        checkAutoCommitAndCommitIfRequired(conn, transactionType);
        // check operation succeeds
        try (final var statement = conn.createStatement()) {
            try (final var resultSet = statement.executeQuery("SELECT * FROM RESTAURANT WHERE REST_NO = 1")) {
                ResultSetAssert.assertThat(resultSet)
                        .hasNextRow().hasColumn("NAME", "aa")
                        .hasNoNextRow();
            }
        }
        checkOpenTransaction(conn, transactionType);
        checkAutoCommitAndCommitIfRequired(conn, transactionType);
    }

    @ParameterizedTest
    @EnumSource
    public void simpleUpdateWithReturningExhaustedResultSet(TransactionType transactionType) throws SQLException, RelationalException {
        EmbeddedRelationalConnection conn = connection.getUnderlyingEmbeddedConnection();
        Assertions.assertFalse(conn.inActiveTransaction());
        if (transactionType == TransactionType.EXISTING_TRANSACTION) {
            conn = getConnectionWithExistingTransaction(conn, database.getConnectionUri(), alternateDriver);
        }
        setAutoCommit(conn, transactionType);
        try (final var statement = conn.createStatement()) {
            final var hasResultSet = statement.execute("UPDATE RESTAURANT SET NAME = 'aa' WHERE REST_NO = 1 RETURNING NEW.* ");
            Assertions.assertTrue(hasResultSet);
            try (final var resultSet = statement.getResultSet()) {
                ResultSetAssert.assertThat(resultSet)
                        .hasNextRow().hasColumn("NAME", "aa")
                        .hasNoNextRow();
            }
            checkOpenTransaction(conn, transactionType);
        }
        checkOpenTransaction(conn, transactionType);
        checkAutoCommitAndCommitIfRequired(conn, transactionType);
        // check operation succeeds
        try (final var statement = conn.createStatement()) {
            try (final var resultSet = statement.executeQuery("SELECT * FROM RESTAURANT WHERE REST_NO = 1")) {
                ResultSetAssert.assertThat(resultSet)
                        .hasNextRow().hasColumn("NAME", "aa")
                        .hasNoNextRow();
            }
        }
        checkOpenTransaction(conn, transactionType);
        checkAutoCommitAndCommitIfRequired(conn, transactionType);
    }

    @Disabled // TODO (UPDATE RETURNING updates wrong number of rows when the ResultSet is not fully consumed.)
    @ParameterizedTest
    @EnumSource
    public void simpleUpdateWithReturningNonExhaustedResultSet(TransactionType transactionType) throws SQLException, RelationalException {
        EmbeddedRelationalConnection conn = connection.getUnderlyingEmbeddedConnection();
        Assertions.assertFalse(conn.inActiveTransaction());
        if (transactionType == TransactionType.EXISTING_TRANSACTION) {
            conn = getConnectionWithExistingTransaction(conn, database.getConnectionUri(), alternateDriver);
        }
        setAutoCommit(conn, transactionType);
        try (final var statement = conn.createStatement()) {
            final var hasResultSet = statement.execute("UPDATE RESTAURANT SET NAME = 'aa' WHERE REST_NO < 3 RETURNING NEW.* ");
            Assertions.assertTrue(hasResultSet);
            // resultSet not retrieved, hence it is still owned by the statement and will be closed.
        }
        checkOpenTransaction(conn, transactionType);
        checkAutoCommitAndCommitIfRequired(conn, transactionType);
        // check operation succeeds
        try (final var statement = conn.createStatement()) {
            final var resultSet = statement.executeQuery("SELECT * FROM RESTAURANT WHERE REST_NO < 3");
            ResultSetAssert.assertThat(resultSet)
                    .hasNextRow().hasColumn("NAME", "aa").hasColumn("REST_NO", 1L)
                    .hasNextRow().hasColumn("NAME", "b").hasColumn("REST_NO", 2L)
                    .hasNoNextRow();
        }
        checkOpenTransaction(conn, transactionType);
        checkAutoCommitAndCommitIfRequired(conn, transactionType);
    }

    @ParameterizedTest
    @EnumSource
    public void directAccessScan(TransactionType transactionType) throws SQLException, RelationalException {
        EmbeddedRelationalConnection conn = connection.getUnderlyingEmbeddedConnection();
        Assertions.assertFalse(conn.inActiveTransaction());
        if (transactionType == TransactionType.EXISTING_TRANSACTION) {
            conn = getConnectionWithExistingTransaction(conn, database.getConnectionUri(), alternateDriver);
        }
        setAutoCommit(conn, transactionType);
        try (final var statement = conn.createStatement()) {
            try (var resultSet = statement.executeScan("RESTAURANT", new KeySet(), Options.NONE)) {
                ResultSetAssert.assertThat(resultSet)
                        .hasNextRow().hasColumn("REST_NO", 1L)
                        .hasNextRow().hasColumn("REST_NO", 2L)
                        .hasNextRow().hasColumn("REST_NO", 3L)
                        .hasNextRow().hasColumn("REST_NO", 4L)
                        .hasNextRow().hasColumn("REST_NO", 5L)
                        .hasNoNextRow();
            }
            checkOpenTransaction(conn, transactionType);
        }
        checkOpenTransaction(conn, transactionType);
        checkAutoCommitAndCommitIfRequired(conn, transactionType);
    }

    @ParameterizedTest
    @EnumSource
    public void directAccessGet(TransactionType transactionType) throws SQLException, RelationalException {
        EmbeddedRelationalConnection conn = connection.getUnderlyingEmbeddedConnection();
        Assertions.assertFalse(conn.inActiveTransaction());
        if (transactionType == TransactionType.EXISTING_TRANSACTION) {
            conn = getConnectionWithExistingTransaction(conn, database.getConnectionUri(), alternateDriver);
        }
        setAutoCommit(conn, transactionType);
        try (final var statement = conn.createStatement()) {
            final var resultSet = statement.executeGet("RESTAURANT", new KeySet().setKeyColumn("REST_NO", 1), Options.NONE);
            ResultSetAssert.assertThat(resultSet)
                    .hasNextRow().hasColumn("REST_NO", 1L)
                    .hasNoNextRow();
        }
        checkOpenTransaction(conn, transactionType);
        checkAutoCommitAndCommitIfRequired(conn, transactionType);
    }

    @ParameterizedTest
    @EnumSource
    public void directAccessInsert(TransactionType transactionType) throws SQLException, RelationalException {
        EmbeddedRelationalConnection conn = connection.getUnderlyingEmbeddedConnection();
        Assertions.assertFalse(conn.inActiveTransaction());
        if (transactionType == TransactionType.EXISTING_TRANSACTION) {
            conn = getConnectionWithExistingTransaction(conn, database.getConnectionUri(), alternateDriver);
        }
        setAutoCommit(conn, transactionType);
        try (final var statement = conn.createStatement()) {
            final var num = statement.executeInsert("RESTAURANT", EmbeddedRelationalStruct.newBuilder()
                    .addLong("REST_NO", 6L)
                    .addString("NAME", "f")
                    .build());
            Assertions.assertEquals(1, num);
        }
        checkOpenTransaction(conn, transactionType);
        checkAutoCommitAndCommitIfRequired(conn, transactionType);
        // check operation succeeds
        try (final var statement = conn.createStatement()) {
            try (var resultSet = statement.executeQuery("SELECT * FROM RESTAURANT WHERE REST_NO = 6")) {
                ResultSetAssert.assertThat(resultSet)
                        .hasNextRow().hasColumn("NAME", "f")
                        .hasNoNextRow();
            }
        }
        checkOpenTransaction(conn, transactionType);
        checkAutoCommitAndCommitIfRequired(conn, transactionType);
    }

    @ParameterizedTest
    @EnumSource
    public void directAccessDelete(TransactionType transactionType) throws SQLException, RelationalException {
        EmbeddedRelationalConnection conn = connection.getUnderlyingEmbeddedConnection();
        Assertions.assertFalse(conn.inActiveTransaction());
        if (transactionType == TransactionType.EXISTING_TRANSACTION) {
            conn = getConnectionWithExistingTransaction(conn, database.getConnectionUri(), alternateDriver);
        }
        setAutoCommit(conn, transactionType);
        try (final var statement = conn.createStatement()) {
            final var keySets = List.of(new KeySet().setKeyColumn("REST_NO", 1), new KeySet().setKeyColumn("REST_NO", 2));
            final var num = statement.executeDelete("RESTAURANT", keySets);
            Assertions.assertEquals(2, num);
        }
        checkOpenTransaction(conn, transactionType);
        checkAutoCommitAndCommitIfRequired(conn, transactionType);
        // check operation succeeds
        try (final var statement = conn.createStatement()) {
            try (var resultSet = statement.executeQuery("SELECT * FROM RESTAURANT WHERE REST_NO < 3 ")) {
                ResultSetAssert.assertThat(resultSet)
                        .hasNoNextRow();
            }
        }
        checkOpenTransaction(conn, transactionType);
        checkAutoCommitAndCommitIfRequired(conn, transactionType);
    }

    @ParameterizedTest
    @EnumSource
    public void directAccessDeleteRange(TransactionType transactionType) throws SQLException, RelationalException {
        EmbeddedRelationalConnection conn = connection.getUnderlyingEmbeddedConnection();
        Assertions.assertFalse(conn.inActiveTransaction());
        if (transactionType == TransactionType.EXISTING_TRANSACTION) {
            conn = getConnectionWithExistingTransaction(conn, database.getConnectionUri(), alternateDriver);
        }
        setAutoCommit(conn, transactionType);
        try (final var statement = conn.createStatement()) {
            statement.executeDeleteRange("RESTAURANT", new KeySet(), Options.NONE);
        }
        checkOpenTransaction(conn, transactionType);
        checkAutoCommitAndCommitIfRequired(conn, transactionType);
        // check operation succeeds
        try (final var statement = conn.createStatement()) {
            try (final var resultSet = statement.executeQuery("SELECT * FROM RESTAURANT")) {
                ResultSetAssert.assertThat(resultSet)
                        .hasNoNextRow();
            }
        }
        checkOpenTransaction(conn, transactionType);
        checkAutoCommitAndCommitIfRequired(conn, transactionType);
    }

    @ParameterizedTest
    @EnumSource
    public void commitWithAutoCommitOnOff(TransactionType transactionType) throws SQLException, RelationalException {
        tryCommitOrRollback(transactionType, conn -> {
            try {
                conn.commit();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @ParameterizedTest
    @EnumSource
    public void rollbackWithAutoCommitOnOff(TransactionType transactionType) throws SQLException, RelationalException {
        tryCommitOrRollback(transactionType, conn -> {
            try {
                conn.rollback();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });
    }

    private void tryCommitOrRollback(TransactionType transactionType, Consumer<EmbeddedRelationalConnection> commitOrRollback) throws SQLException, RelationalException {
        EmbeddedRelationalConnection conn = connection.getUnderlyingEmbeddedConnection();
        Assertions.assertFalse(conn.inActiveTransaction());
        if (transactionType == TransactionType.EXISTING_TRANSACTION) {
            conn = getConnectionWithExistingTransaction(conn, database.getConnectionUri(), alternateDriver);
        }
        setAutoCommit(conn, transactionType);
        try (final var statement = conn.createStatement()) {
            final var rs = statement.executeQuery("SELECT REST_NO, NAME FROM RESTAURANT");
            Assertions.assertTrue(conn.inActiveTransaction());
            ResultSetAssert.assertThat(rs)
                    .hasNextRow()
                    .hasNextRow();
            final var finalConn = conn;
            if (transactionType == TransactionType.AUTO_COMMIT_OFF_WITH_EXPLICIT_COMMIT || transactionType == TransactionType.AUTO_COMMIT_OFF_WITH_NO_COMMIT) {
                Assertions.assertDoesNotThrow(() -> commitOrRollback.accept(finalConn));
            } else {
                final var cause = Assertions.assertThrows(RuntimeException.class, () -> commitOrRollback.accept(finalConn)).getCause();
                Assertions.assertInstanceOf(SQLException.class, cause);
            }
        }
    }

    @ParameterizedTest
    @EnumSource
    public void ddl(TransactionType transactionType) throws SQLException, RelationalException {
        EmbeddedRelationalConnection conn = connection.getUnderlyingEmbeddedConnection();
        Assertions.assertFalse(conn.inActiveTransaction());
        if (transactionType == TransactionType.EXISTING_TRANSACTION) {
            conn = getConnectionWithExistingTransaction(conn, database.getConnectionUri(), alternateDriver);
        }
        try (final var statement = conn.createStatement()) {
            setAutoCommit(conn, transactionType);
            statement.executeUpdate("CREATE DATABASE /TEST/DB23434");
            checkOpenTransaction(conn, transactionType);
        }
        checkOpenTransaction(conn, transactionType);
        checkAutoCommitAndCommitIfRequired(conn, transactionType);
    }

    @ParameterizedTest
    @EnumSource
    public void catalogMetadata(TransactionType transactionType) throws SQLException, RelationalException {
        EmbeddedRelationalConnection conn = connection.getUnderlyingEmbeddedConnection();
        Assertions.assertFalse(conn.inActiveTransaction());
        if (transactionType == TransactionType.EXISTING_TRANSACTION) {
            conn = getConnectionWithExistingTransaction(conn, database.getConnectionUri(), alternateDriver);
        }
        setAutoCommit(conn, transactionType);
        try (final var rs = conn.getMetaData().getTables("/TEST/AutoCommitTests", "TEST_SCHEMA", null, null)) {
            ResultSetAssert.assertThat(rs)
                    .hasNextRow()
                    .hasNextRow()
                    .hasNextRow()
                    .hasNoNextRow();
        }
        // Metadata operations should not persist the transaction even when the autoCommit is off,
        // unless if the connection is created with an existing transaction.
        if (transactionType == TransactionType.EXISTING_TRANSACTION) {
            Assertions.assertTrue(conn.inActiveTransaction());
        } else {
            Assertions.assertFalse(conn.inActiveTransaction());
        }
    }

    @Test
    public void changeAutoCommitBetweenTransactions() throws SQLException {
        EmbeddedRelationalConnection conn = connection.getUnderlyingEmbeddedConnection();

        Assertions.assertFalse(conn.inActiveTransaction());
        Assertions.assertTrue(conn.getAutoCommit());
        try (final var statement = conn.createStatement()) {
            try (final var rs = statement.executeQuery("SELECT REST_NO, NAME FROM RESTAURANT")) {
                Assertions.assertTrue(conn.inActiveTransaction());
                ResultSetAssert.assertThat(rs)
                        .hasNextRow()
                        .hasNextRow()
                        .hasNextRow()
                        .hasNextRow()
                        .hasNextRow()
                        .hasNoNextRow();

                // since the resultSet is still open, transaction should be opened.
                Assertions.assertTrue(conn.inActiveTransaction());
            }
            // since the resultSet is closed, transaction is closed.
            Assertions.assertFalse(conn.inActiveTransaction());
        }

        conn.setAutoCommit(false);
        Assertions.assertFalse(conn.getAutoCommit());
        try (final var statement = conn.createStatement()) {
            try (final var rs = statement.executeQuery("SELECT REST_NO, NAME FROM RESTAURANT")) {
                Assertions.assertTrue(conn.inActiveTransaction());
                ResultSetAssert.assertThat(rs)
                        .hasNextRow()
                        .hasNextRow()
                        .hasNextRow()
                        .hasNextRow()
                        .hasNextRow()
                        .hasNoNextRow();

                // since the resultSet is still open, transaction should be opened.
                Assertions.assertTrue(conn.inActiveTransaction());
            }
            // since the resultSet is closed but autoCommit is off, transaction will remain open.
            Assertions.assertTrue(conn.inActiveTransaction());
        }
        Assertions.assertDoesNotThrow(conn::commit);
    }

    @Test
    public void switchOffAutoCommitBetweenOngoingTransaction() throws SQLException {
        EmbeddedRelationalConnection conn = connection.getUnderlyingEmbeddedConnection();

        Assertions.assertFalse(conn.inActiveTransaction());
        Assertions.assertTrue(conn.getAutoCommit());
        try (final var statement = conn.createStatement()) {
            try (final var rs = statement.executeQuery("UPDATE RESTAURANT SET name = 'aa' WHERE REST_NO < 3 RETURNING NEW.* ")) {
                Assertions.assertTrue(conn.inActiveTransaction());
                ResultSetAssert.assertThat(rs)
                        .hasNextRow()
                        .hasNextRow()
                        .hasNoNextRow();

                // since the resultSet is still open, transaction should be opened.
                Assertions.assertTrue(conn.inActiveTransaction());

                // this should cause the ongoing transaction to commit
                conn.setAutoCommit(false);
                // transaction is closed.
                Assertions.assertFalse(conn.inActiveTransaction());
            }
            Assertions.assertFalse(conn.getAutoCommit());
        }

        try (final var statement = conn.createStatement()) {
            try (final var rs = statement.executeQuery("SELECT NAME FROM RESTAURANT WHERE REST_NO < 3")) {
                Assertions.assertTrue(conn.inActiveTransaction());
                ResultSetAssert.assertThat(rs)
                        .hasNextRow().hasColumn("NAME", "aa")
                        .hasNextRow().hasColumn("NAME", "aa")
                        .hasNoNextRow();
            }
            // transaction stays active since the autoCommit is off.
            Assertions.assertTrue(conn.inActiveTransaction());
        }
        // transaction stays active even after the statement closes.
        Assertions.assertTrue(conn.inActiveTransaction());
    }

    @Test
    public void switchOnAutoCommitBetweenOngoingTransaction() throws SQLException {
        EmbeddedRelationalConnection conn = connection.getUnderlyingEmbeddedConnection();

        Assertions.assertFalse(conn.inActiveTransaction());
        conn.setAutoCommit(false);
        Assertions.assertFalse(conn.getAutoCommit());
        try (final var statement = conn.createStatement()) {
            try (final var rs = statement.executeQuery("UPDATE RESTAURANT SET name = 'aa' WHERE REST_NO < 3 RETURNING NEW.* ")) {
                Assertions.assertTrue(conn.inActiveTransaction());
                ResultSetAssert.assertThat(rs)
                        .hasNextRow()
                        .hasNextRow()
                        .hasNoNextRow();

                // since the resultSet is still open, transaction should be opened.
                Assertions.assertTrue(conn.inActiveTransaction());

                // this should cause the ongoing transaction to commit
                conn.setAutoCommit(true);
                // transaction is closed.
                Assertions.assertFalse(conn.inActiveTransaction());
            }
            Assertions.assertTrue(conn.getAutoCommit());
        }

        try (final var statement = conn.createStatement()) {
            try (final var rs = statement.executeQuery("SELECT NAME FROM RESTAURANT WHERE REST_NO < 3")) {
                Assertions.assertTrue(conn.inActiveTransaction());
                ResultSetAssert.assertThat(rs)
                        .hasNextRow().hasColumn("NAME", "aa")
                        .hasNextRow().hasColumn("NAME", "aa")
                        .hasNoNextRow();
            }
            // transaction closes since we are in autoCommit mode
            Assertions.assertFalse(conn.inActiveTransaction());
        }
        // transaction closes since we are in autoCommit mode
        Assertions.assertFalse(conn.inActiveTransaction());
    }

    @Test
    public void transactionClosesWithStatement() throws SQLException {
        EmbeddedRelationalConnection conn = connection.getUnderlyingEmbeddedConnection();

        Assertions.assertFalse(conn.inActiveTransaction());
        Assertions.assertTrue(conn.getAutoCommit());

        RelationalResultSet rs;
        try (final var statement = conn.createStatement()) {
            rs = statement.executeQuery("UPDATE RESTAURANT SET name = 'aa' WHERE REST_NO < 3 RETURNING NEW.* ");
            Assertions.assertTrue(conn.inActiveTransaction());
            ResultSetAssert.assertThat(rs).hasNextRow();

            // since the resultSet is still open, transaction should be opened.
            Assertions.assertTrue(conn.inActiveTransaction());
            ResultSetAssert.assertThat(rs).hasNextRow();
        }
        // since the statement closes, the associated result set should close as well.
        Assertions.assertTrue(rs.isClosed());
        // ...and the connection
        Assertions.assertFalse(conn.inActiveTransaction());

        // explicit close
        rs.close();
        Assertions.assertFalse(conn.inActiveTransaction());
        Assertions.assertThrows(SQLException.class, rs::next);
    }

    @Test
    public void newTransactionForEachExecutionOfStatement() throws SQLException, RelationalException {
        EmbeddedRelationalConnection conn = connection.getUnderlyingEmbeddedConnection();

        Assertions.assertFalse(conn.inActiveTransaction());
        Assertions.assertTrue(conn.getAutoCommit());

        try (final var stmt = conn.createStatement()) {
            final var rs1 = stmt.executeQuery("SELECT NAME FROM RESTAURANT WHERE REST_NO < 3");
            ResultSetAssert.assertThat(rs1).hasNextRow();
            // since the resultSet and statement is still open, transaction should be opened.
            Assertions.assertTrue(conn.inActiveTransaction());
            final var txn1 = conn.getTransaction();

            final var rs2 = stmt.executeQuery("SELECT NAME FROM RESTAURANT WHERE REST_NO < 3");
            ResultSetAssert.assertThat(rs2).hasNextRow();
            // previous resultSet is closed.
            Assertions.assertTrue(rs1.isClosed());
            Assertions.assertTrue(conn.inActiveTransaction());
            final var txn2 = conn.getTransaction();
            Assertions.assertNotSame(txn1, txn2);
        }
    }

    private static void checkOpenTransaction(@Nonnull EmbeddedRelationalConnection connection, TransactionType transactionType) {
        if (transactionType == TransactionType.AUTO_COMMIT_ON) {
            Assertions.assertFalse(connection.inActiveTransaction());
        } else {
            Assertions.assertTrue(connection.inActiveTransaction());
        }
    }

    private static void checkAutoCommitAndCommitIfRequired(@Nonnull EmbeddedRelationalConnection connection, TransactionType transactionType) throws SQLException {
        if (transactionType == TransactionType.AUTO_COMMIT_ON || transactionType == TransactionType.EXISTING_TRANSACTION) {
            Assertions.assertTrue(connection.getAutoCommit());
        } else {
            Assertions.assertFalse(connection.getAutoCommit());
            if (transactionType == TransactionType.AUTO_COMMIT_OFF_WITH_EXPLICIT_COMMIT) {
                Assertions.assertDoesNotThrow(connection::commit);
            }
        }
    }

    private static void setAutoCommit(@Nonnull RelationalConnection connection, TransactionType transactionType) throws SQLException {
        if (transactionType == TransactionType.AUTO_COMMIT_ON) {
            connection.setAutoCommit(true);
        } else if (transactionType == TransactionType.AUTO_COMMIT_OFF_WITH_EXPLICIT_COMMIT || transactionType == TransactionType.AUTO_COMMIT_OFF_WITH_NO_COMMIT) {
            connection.setAutoCommit(false);
        }
    }

    private static EmbeddedRelationalConnection getConnectionWithExistingTransaction(@Nonnull EmbeddedRelationalConnection connection, @Nonnull URI uri, @Nonnull EmbeddedRelationalDriver alternateDriver) throws SQLException, RelationalException {
        final var store = TransactionBoundDatabaseTest.getStore(connection);
        final var schemaTemplate = TransactionBoundDatabaseTest.getSchemaTemplate(connection);

        final var context = TransactionBoundDatabaseTest.createNewContext(connection);
        final var newStore = store.asBuilder().setMetaDataProvider(store.getMetaDataProvider()).setContext(context).open();
        final var transaction = new RecordStoreAndRecordContextTransaction(newStore, context, schemaTemplate);
        final var newConnection = alternateDriver.connect(uri, transaction, Options.NONE).unwrap(EmbeddedRelationalConnection.class);
        newConnection.setSchema("TEST_SCHEMA");
        return newConnection;
    }
}
