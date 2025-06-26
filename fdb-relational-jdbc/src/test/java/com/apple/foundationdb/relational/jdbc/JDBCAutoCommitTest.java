/*
 * JDBCSimpleStatementTest.java
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
    public static final String SYS_DB_URI = "jdbc:relational://" + SYSDBPATH + "?schema=" + RelationalKeyspaceProvider.CATALOG;
    public static final String TEST_DB_URI = "jdbc:relational://" + TESTDB + "?schema=" + TEST_SCHEMA;

    /**
     * Load our JDBCDriver via ServiceLoader so available to test.
     */
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
    public void autoCommitOn() throws SQLException, IOException {
        try (RelationalConnection connection = DriverManager.getConnection(TEST_DB_URI).unwrap(RelationalConnection.class)) {
            try (RelationalStatement statement = connection.createStatement()) {
                insertOneRow(statement);
                RelationalResultSet resultSet = statement.executeQuery("SELECT * FROM test_table");
                Assertions.assertTrue(resultSet.next());
                Assertions.assertEquals(100, resultSet.getLong(1));
                Assertions.assertEquals("one hundred", resultSet.getString(2));
                Assertions.assertFalse(resultSet.next());
            }
        }
    }

    /**
     * Run a test with commit and then read.
     */
    @Test
    public void commitThenRead() throws SQLException, IOException {
        try (RelationalConnection connection = DriverManager.getConnection(TEST_DB_URI).unwrap(RelationalConnection.class)) {
            connection.setAutoCommit(false);

            try (RelationalStatement statement = connection.createStatement()) {
                insertOneRow(statement);
                connection.commit();

                RelationalResultSet resultSet = statement.executeQuery("SELECT * FROM test_table");
                Assertions.assertTrue(resultSet.next());
                Assertions.assertEquals(100, resultSet.getLong(1));
                Assertions.assertEquals("one hundred", resultSet.getString(2));
                Assertions.assertFalse(resultSet.next());
                connection.commit();
            }
        }
    }

    /**
     * Run a test with rollback and then read.
     */
    @Test
    public void rollbackThenRead() throws SQLException, IOException {
        try (RelationalConnection connection = DriverManager.getConnection(TEST_DB_URI).unwrap(RelationalConnection.class)) {
            connection.setAutoCommit(false);

            try (RelationalStatement statement = connection.createStatement()) {
                insertOneRow(statement);
                connection.rollback();

                RelationalResultSet resultSet = statement.executeQuery("SELECT * FROM test_table");
                Assertions.assertFalse(resultSet.next());
                connection.commit();
            }
        }
    }

    /**
     * Run a test with reverting to auto-commit on.
     */
    @Test
    public void revertToAutoCommitOn() throws SQLException, IOException {
        try (RelationalConnection connection = DriverManager.getConnection(TEST_DB_URI).unwrap(RelationalConnection.class)) {
            connection.setAutoCommit(false);

            try (RelationalStatement statement = connection.createStatement()) {
                insertOneRow(statement);
                connection.setAutoCommit(true);

                RelationalResultSet resultSet = statement.executeQuery("SELECT * FROM test_table");
                Assertions.assertTrue(resultSet.next());
                Assertions.assertEquals(100, resultSet.getLong(1));
                Assertions.assertEquals("one hundred", resultSet.getString(2));
                Assertions.assertFalse(resultSet.next());
            }
        }
    }

    private static void insertOneRow(final RelationalStatement statement) throws SQLException {
        RelationalStruct insert = JDBCRelationalStruct.newBuilder()
                .addLong("REST_NO", 100)
                .addString("NAME", "one hundred")
                .build();
        int res = statement.executeInsert("TEST_TABLE", insert);
        Assertions.assertEquals(1, res);
    }

    private void createDatabase() throws SQLException {
        try (RelationalConnection connection = DriverManager.getConnection(SYS_DB_URI)
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
        try (RelationalConnection connection = DriverManager.getConnection(SYS_DB_URI)
                .unwrap(RelationalConnection.class)) {
            try (RelationalStatement statement = connection.createStatement()) {
                statement.executeUpdate("Drop database \"" + TESTDB + "\"");
            }
        }
    }
}
