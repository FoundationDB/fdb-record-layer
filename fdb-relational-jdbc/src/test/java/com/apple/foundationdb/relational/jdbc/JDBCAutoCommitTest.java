/*
 * JDBCAutoCommitTest.java
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

package com.apple.foundationdb.relational.jdbc;

import com.apple.foundationdb.relational.api.RelationalConnection;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.RelationalStatement;
import com.apple.foundationdb.relational.api.RelationalStruct;
import com.apple.foundationdb.relational.recordlayer.RelationalKeyspaceProvider;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Run tests with AutoCommit=OFF against a remote Relational DB.
 * Note: This test is running with embedded gRPC (this would make it easier to debug client-server end-to-end).
 */
public class JDBCAutoCommitTest {
    private static final String SYSDBPATH = "/" + RelationalKeyspaceProvider.SYS;
    private static final String TESTDB = "/FRL/jdbc_test_db";
    public static final String TEST_SCHEMA = "test_schema";

    @BeforeAll
    public static void beforeAll() throws Exception {
        // Load driver.
        JDBCRelationalDriverTest.getDriver();
    }

    @AfterAll
    public static void afterAll() throws IOException {
    }

    @BeforeEach
    public void setup() throws Exception {
        createDatabase();
    }

    @AfterEach
    public void tearDown() throws Exception {
        cleanup();
    }

    /**
     * Run a test with the default (auto-commit on) for sanity.
     */
    @Test
    void autoCommitOn() throws SQLException {
        try (RelationalConnection connection = DriverManager.getConnection(getTestDbUri()).unwrap(RelationalConnection.class)) {
            try (RelationalStatement statement = connection.createStatement()) {
                insertOneRow(statement);
                RelationalResultSet resultSet = statement.executeQuery("SELECT * FROM test_table");
                assertNextResult(resultSet, 100, "one hundred");
                assertNoNextResult(resultSet);
            }
        }
    }

    @Test
    void autoCommitWithNoTransactionInBetween() throws SQLException {
        try (RelationalConnection connection = DriverManager.getConnection(getTestDbUri()).unwrap(RelationalConnection.class)) {
            connection.setAutoCommit(false);
            connection.setAutoCommit(true);
        }
    }

    @Test
    void autoCommitStayOn() throws SQLException {
        try (RelationalConnection connection = DriverManager.getConnection(getTestDbUri()).unwrap(RelationalConnection.class)) {
            connection.setAutoCommit(true);
        }
    }

    @Test
    void autoCommitStayOff() throws SQLException {
        try (RelationalConnection connection = DriverManager.getConnection(getTestDbUri()).unwrap(RelationalConnection.class)) {
            connection.setAutoCommit(false);
            connection.setAutoCommit(false);
        }
    }

    @Test
    void commitEnableAutoCommit() throws SQLException {
        try (RelationalConnection connection = DriverManager.getConnection(getTestDbUri()).unwrap(RelationalConnection.class)) {
            connection.setAutoCommit(false);

            try (RelationalStatement statement = connection.createStatement()) {
                insertOneRow(statement);
            }
            connection.commit();
            connection.setAutoCommit(true);
            try (RelationalStatement statement = connection.createStatement()) {
                insertOneRow(statement, 101);
            }
            try (RelationalStatement statement = connection.createStatement()) {
                RelationalResultSet resultSet = statement.executeQuery("SELECT * FROM test_table");
                assertNextResult(resultSet, 100, "one hundred");
                assertNextResult(resultSet, 101, "one hundred");
                assertNoNextResult(resultSet);
            }
        }
    }

    @Test
    void rollbackThenEnableAutoCommit() throws SQLException {
        try (RelationalConnection connection = DriverManager.getConnection(getTestDbUri()).unwrap(RelationalConnection.class)) {
            connection.setAutoCommit(false);

            try (RelationalStatement statement = connection.createStatement()) {
                insertOneRow(statement);
            }
            connection.rollback();
            connection.setAutoCommit(true);
            try (RelationalStatement statement = connection.createStatement()) {
                RelationalResultSet resultSet = statement.executeQuery("SELECT * FROM test_table");
                assertNoNextResult(resultSet);
                assertNoNextResult(resultSet);
            }
        }
    }

    /**
     * Run a test with commit and then read.
     */
    @Test
    void commitThenRead() throws SQLException, IOException {
        try (RelationalConnection connection = DriverManager.getConnection(getTestDbUri()).unwrap(RelationalConnection.class)) {
            connection.setAutoCommit(false);

            try (RelationalStatement statement = connection.createStatement()) {
                insertOneRow(statement);
                connection.commit();

                RelationalResultSet resultSet = statement.executeQuery("SELECT * FROM test_table");
                assertNextResult(resultSet, 100, "one hundred");
                assertNoNextResult(resultSet);
                connection.commit();
            }
        }
    }

    @Test
    void insertMultiCommitRead() throws SQLException, IOException {
        try (RelationalConnection connection = DriverManager.getConnection(getTestDbUri()).unwrap(RelationalConnection.class)) {
            connection.setAutoCommit(false);

            try (RelationalStatement statement = connection.createStatement()) {
                insertOneRow(statement);
                insert2ndRow(statement);
                connection.commit();

                RelationalResultSet resultSet = statement.executeQuery("SELECT * FROM test_table");
                assertNextResult(resultSet, 100, "one hundred");
                assertNextResult(resultSet, 200, "two hundred");
                assertNoNextResult(resultSet);
                connection.commit();
            }
        }
    }

    /**
     * Run a test with rollback and then read.
     */
    @Test
    void rollbackThenRead() throws SQLException, IOException {
        try (RelationalConnection connection = DriverManager.getConnection(getTestDbUri()).unwrap(RelationalConnection.class)) {
            connection.setAutoCommit(false);

            try (RelationalStatement statement = connection.createStatement()) {
                insertOneRow(statement);
                connection.rollback();

                RelationalResultSet resultSet = statement.executeQuery("SELECT * FROM test_table");
                assertNoNextResult(resultSet);
                connection.commit();
            }
        }
    }

    /**
     * Run a test with reverting to auto-commit on.
     */
    @Test
    void revertToAutoCommitOn() throws Exception {
        try (RelationalConnection connection = DriverManager.getConnection(getTestDbUri()).unwrap(RelationalConnection.class)) {
            connection.setAutoCommit(false);

            try (RelationalStatement statement = connection.createStatement()) {
                insertOneRow(statement);
                connection.setAutoCommit(true); // this should commit

                RelationalResultSet resultSet = statement.executeQuery("SELECT * FROM test_table");
                assertNextResult(resultSet, 100, "one hundred");
                assertNoNextResult(resultSet);
            }
        }
    }

    @Test
    void insertError() throws Exception {
        try (RelationalConnection connection = DriverManager.getConnection(getTestDbUri()).unwrap(RelationalConnection.class)) {
            connection.setAutoCommit(false);
            try (RelationalStatement statement = connection.createStatement()) {
                RelationalStruct insert = JDBCRelationalStruct.newBuilder()
                        .addLong("BLAH", 100)
                        .build();
                Assertions.assertThrows(SQLException.class, () -> statement.executeInsert("TEST_TABLE", insert));
            }
        }
    }

    @Test
    void insertErrorAfterSuccess() throws Exception {
        try (RelationalConnection connection = DriverManager.getConnection(getTestDbUri()).unwrap(RelationalConnection.class)) {
            connection.setAutoCommit(false);
            try (RelationalStatement statement = connection.createStatement()) {
                insertOneRow(statement);
                RelationalStruct insert = JDBCRelationalStruct.newBuilder()
                        .addLong("BLAH", 100)
                        .build();
                Assertions.assertThrows(SQLException.class, () -> statement.executeInsert("TEST_TABLE", insert));

                connection.commit();
                RelationalResultSet resultSet = statement.executeQuery("SELECT * FROM test_table");
                assertNextResult(resultSet, 100, "one hundred");
                assertNoNextResult(resultSet);
            }
        }
    }

    @Test
    void queryError() throws Exception {
        try (RelationalConnection connection = DriverManager.getConnection(getTestDbUri()).unwrap(RelationalConnection.class)) {
            connection.setAutoCommit(false);
            try (RelationalStatement statement = connection.createStatement()) {
                Assertions.assertThrows(SQLException.class, () -> statement.executeQuery("BLAH"));
            }
        }
    }

    @Test
    void continueAfterSqlError() throws Exception {
        try (RelationalConnection connection = DriverManager.getConnection(getTestDbUri()).unwrap(RelationalConnection.class)) {
            connection.setAutoCommit(false);
            try (RelationalStatement statement = connection.createStatement()) {
                insertOneRow(statement);
                Assertions.assertThrows(SQLException.class, () -> statement.executeQuery("BLAH"));
                connection.commit();
                // Connection should remain open after an error
                RelationalResultSet resultSet = statement.executeQuery("SELECT * FROM test_table");
                assertNextResult(resultSet, 100, "one hundred");
            }
        }
    }

    @Test
    void commitError() throws Exception {
        try (RelationalConnection connection = DriverManager.getConnection(getTestDbUri()).unwrap(RelationalConnection.class)) {
            connection.setAutoCommit(false);
            try (RelationalStatement statement = connection.createStatement()) {
                // commit with no SQL statement is an error
                Assertions.assertThrows(SQLException.class, () -> connection.commit());
                // Connection should remain open after an error
                insertOneRow(statement);
                connection.commit();
                RelationalResultSet resultSet = statement.executeQuery("SELECT * FROM test_table");
                assertNextResult(resultSet, 100, "one hundred");
            }
        }
    }

    @Test
    void rollbackError() throws Exception {
        try (RelationalConnection connection = DriverManager.getConnection(getTestDbUri()).unwrap(RelationalConnection.class)) {
            connection.setAutoCommit(false);
            try (RelationalStatement statement = connection.createStatement()) {
                // rollback with no SQL statement is an error
                Assertions.assertThrows(SQLException.class, () -> connection.rollback());
                // Connection should remain open after an error
                insertOneRow(statement);
                connection.commit();
                RelationalResultSet resultSet = statement.executeQuery("SELECT * FROM test_table");
                assertNextResult(resultSet, 100, "one hundred");
            }
        }
    }

    @Test
    void rollbackOnConnectionClose() throws Exception {
        try (RelationalConnection connection = DriverManager.getConnection(getTestDbUri()).unwrap(RelationalConnection.class)) {
            connection.setAutoCommit(false);
            try (RelationalStatement statement = connection.createStatement()) {
                // no commit, closing session should roll back
                insertOneRow(statement);
            }
        }
        try (RelationalConnection connection = DriverManager.getConnection(getTestDbUri()).unwrap(RelationalConnection.class)) {
            try (RelationalStatement statement = connection.createStatement()) {
                RelationalResultSet resultSet = statement.executeQuery("SELECT * FROM test_table");
                assertNoNextResult(resultSet);
            }
        }
    }

    @Test
    void twoConnectionsInSequence() throws Exception {
        try (RelationalConnection connection = DriverManager.getConnection(getTestDbUri()).unwrap(RelationalConnection.class)) {
            connection.setAutoCommit(false);
            try (RelationalStatement statement = connection.createStatement()) {
                insertOneRow(statement);
                connection.commit();
            }
        }
        try (RelationalConnection connection = DriverManager.getConnection(getTestDbUri()).unwrap(RelationalConnection.class)) {
            connection.setAutoCommit(false);
            try (RelationalStatement statement = connection.createStatement()) {
                insert2ndRow(statement);
                connection.commit();
                RelationalResultSet resultSet = statement.executeQuery("SELECT * FROM test_table");
                assertNextResult(resultSet, 100, "one hundred");
                assertNextResult(resultSet, 200, "two hundred");
                assertNoNextResult(resultSet);
            }
        }
    }

    @Test
    void twoConcurrentConnections() throws Exception {
        try (RelationalConnection connection1 = DriverManager.getConnection(getTestDbUri()).unwrap(RelationalConnection.class);
                RelationalConnection connection2 = DriverManager.getConnection(getTestDbUri()).unwrap(RelationalConnection.class)) {
            connection1.setAutoCommit(false);
            connection2.setAutoCommit(false);
            try (RelationalStatement statement1 = connection1.createStatement();
                    RelationalStatement statement2 = connection2.createStatement()) {
                insertOneRow(statement1);
                insert2ndRow(statement2);
                connection1.commit();
                connection2.commit();
            }
        }
        try (RelationalConnection connection = DriverManager.getConnection(getTestDbUri()).unwrap(RelationalConnection.class)) {
            connection.setAutoCommit(false);
            try (RelationalStatement statement = connection.createStatement()) {
                RelationalResultSet resultSet = statement.executeQuery("SELECT * FROM test_table");
                assertNextResult(resultSet, 100, "one hundred");
                assertNextResult(resultSet, 200, "two hundred");
                assertNoNextResult(resultSet);
            }
        }
    }

    protected String getSysDbUri() {
        return "jdbc:relational://" + getHostPort() + SYSDBPATH + "?schema=" + RelationalKeyspaceProvider.CATALOG;
    }

    protected String getTestDbUri() {
        return "jdbc:relational://" + getHostPort() + TESTDB + "?schema=" + TEST_SCHEMA;
    }

    protected String getHostPort() {
        return "";
    }

    private static void insertOneRow(final RelationalStatement statement) throws SQLException {
        insertOneRow(statement, 100);
    }

    private static void insertOneRow(final RelationalStatement statement, int restNo) throws SQLException {
        RelationalStruct insert = JDBCRelationalStruct.newBuilder()
                .addLong("REST_NO", restNo)
                .addString("NAME", "one hundred")
                .build();
        int res = statement.executeInsert("TEST_TABLE", insert);
        Assertions.assertEquals(1, res);
    }

    private static void insert2ndRow(final RelationalStatement statement) throws SQLException {
        RelationalStruct insert = JDBCRelationalStruct.newBuilder()
                .addLong("REST_NO", 200)
                .addString("NAME", "two hundred")
                .build();
        int res = statement.executeInsert("TEST_TABLE", insert);
        Assertions.assertEquals(1, res);
    }

    private void createDatabase() throws SQLException {
        try (RelationalConnection connection = DriverManager.getConnection(getSysDbUri())
                .unwrap(RelationalConnection.class)) {
            try (RelationalStatement statement = connection.createStatement()) {
                statement.executeUpdate("Drop database if exists \"" + TESTDB + "\"");
                statement.executeUpdate("Drop schema template if exists test_template");
                statement.executeUpdate("CREATE SCHEMA TEMPLATE test_template " +
                        "CREATE TABLE test_table (rest_no bigint, name string, PRIMARY KEY(rest_no))");
                statement.executeUpdate("create database \"" + TESTDB + "\"");
                statement.executeUpdate("create schema \"" + TESTDB + "/test_schema\" with template test_template");
                Assertions.assertNull(statement.getWarnings());
            }
        }
    }

    private void cleanup() throws SQLException {
        try (RelationalConnection connection = DriverManager.getConnection(getSysDbUri())
                .unwrap(RelationalConnection.class)) {
            try (RelationalStatement statement = connection.createStatement()) {
                statement.executeUpdate("Drop database \"" + TESTDB + "\"");
            }
        }
    }


    private static void assertNextResult(final RelationalResultSet resultSet, final int longValue, final String stringValue) throws SQLException {
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals(longValue, resultSet.getLong(1));
        Assertions.assertEquals(stringValue, resultSet.getString(2));
    }

    private static void assertNoNextResult(final RelationalResultSet resultSet) throws SQLException {
        Assertions.assertFalse(resultSet.next());
    }
}
