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
import com.apple.foundationdb.relational.api.RelationalPreparedStatement;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.RelationalStatement;
import com.apple.foundationdb.relational.jdbc.grpc.GrpcConstants;
import com.apple.foundationdb.relational.recordlayer.RelationalKeyspaceProvider;
import com.apple.foundationdb.relational.server.ServerTestUtil;
import com.apple.foundationdb.relational.server.RelationalServer;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.Types;

/**
 * Run some simple Statement updates/executes against a remote Relational DB.
 */
public class JDBCSimpleStatementTest {
    private static final String SYSDBPATH = "/" + RelationalKeyspaceProvider.SYS;
    private static final String TESTDB = "/FRL/jdbc_test_db";

    private static RelationalServer relationalServer;

    /**
     * Load our JDBCDriver via ServiceLoader so available to test.
     */
    @BeforeAll
    public static void beforeAll() throws SQLException, IOException {
        // Load driver.
        JDBCRelationalDriverTest.getDriver();
        relationalServer = ServerTestUtil.createAndStartRelationalServer(GrpcConstants.DEFAULT_SERVER_PORT);
    }

    @AfterAll
    public static void afterAll() throws IOException {
        if (relationalServer != null) {
            relationalServer.close();
        }
        // Don't deregister once registered; service loading runs once only it seems.
        // Joys of static initializations.
        // DriverManager.deregisterDriver(driver);
    }

    @Test
    public void simpleStatement() throws SQLException, IOException {
        var jdbcStr = "jdbc:relational://localhost:" + relationalServer.getGrpcPort() + SYSDBPATH + "?schema=" + RelationalKeyspaceProvider.CATALOG;
        try (RelationalConnection connection = JDBCRelationalDriverTest.getDriver().connect(jdbcStr, null)
                .unwrap(RelationalConnection.class)) {
            try (RelationalStatement statement = connection.createStatement()) {
                // Exercise some methods to up our test coverage metrics
                Assertions.assertEquals(connection, statement.getConnection());
                // Make this better... currently returns zero how ever many rows we touch.
                Assertions.assertEquals(0, statement.executeUpdate("Drop database if exists \"" + TESTDB + "\""));
                Assertions.assertEquals(0, statement.executeUpdate("Drop schema template if exists test_template"));
                Assertions.assertEquals(0,
                        statement.executeUpdate("CREATE SCHEMA TEMPLATE test_template " +
                                "CREATE TABLE test_table (rest_no bigint, name string, PRIMARY KEY(rest_no))"));
                Assertions.assertEquals(0, statement.executeUpdate("create database \"" + TESTDB + "\""));
                Assertions.assertEquals(0, statement.executeUpdate("create schema \"" + TESTDB +
                        "/test_schema\" with template test_template"));
                // Call some of the statement methods for the sake of exercising coverage.
                Assertions.assertNull(statement.getWarnings());
                // Does nothing.
                statement.clearWarnings();
                // Cancel currently does nothing.
                statement.cancel();
                Assertions.assertFalse(statement.isClosed());
                try (RelationalResultSet resultSet = statement.executeQuery("select * from databases")) {
                    checkSelectStarFromDatabasesResultSet(resultSet);
                }
                try (RelationalPreparedStatement preparedStatement =
                        connection.prepareStatement("select * from databases")) {
                    try (RelationalResultSet resultSet = preparedStatement.executeQuery()) {
                        checkSelectStarFromDatabasesResultSet(resultSet);
                    }
                }
                // Simple test of parameters in prepared statement.
                String columnName = "DATABASE_ID";
                String columnValue = "/__SYS";
                try (RelationalPreparedStatement preparedStatement =
                        connection.prepareStatement("select * from databases where " + columnName + " = ?")) {
                    preparedStatement.setString(1, columnValue);
                    try (RelationalResultSet resultSet = preparedStatement.executeQuery()) {
                        // Should return one column only in a one row resultset.
                        Assertions.assertEquals(columnName, resultSet.getMetaData().getColumnName(1));
                        Assertions.assertEquals(Types.VARCHAR, resultSet.getMetaData().getColumnType(1));
                        Assertions.assertTrue(resultSet.next());
                        Assertions.assertEquals(columnValue, resultSet.getString(1));
                        Assertions.assertFalse(resultSet.next());
                    }
                }
            } finally {
                try (RelationalStatement statement = connection.createStatement()) {
                    statement.executeUpdate("Drop database \"" + TESTDB + "\"");
                }
            }
        }
    }

    private static void checkSelectStarFromDatabasesResultSet(RelationalResultSet resultSet) throws SQLException {
        Assertions.assertNotNull(resultSet);
        Assertions.assertTrue(resultSet.isWrapperFor(RelationalResultSetFacade.class));
        // Exercise some metadata methods to get our jacoco coverage up.
        Assertions.assertEquals(1, resultSet.getMetaData().getColumnCount());
        String columnName = "DATABASE_ID";
        Assertions.assertEquals(columnName, resultSet.getMetaData().getColumnName(1));
        // Label == name for now.
        Assertions.assertEquals(columnName, resultSet.getMetaData().getColumnLabel(1));
        Assertions.assertEquals(Types.VARCHAR, resultSet.getMetaData().getColumnType(1));
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals(TESTDB, resultSet.getString(1));
        Assertions.assertEquals(TESTDB, resultSet.getString(columnName));
        // This should work too.
        Assertions.assertEquals(TESTDB, resultSet.getString(columnName.toLowerCase()));
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals(SYSDBPATH, resultSet.getString(1));
        Assertions.assertEquals(SYSDBPATH, resultSet.getString(columnName));
        Assertions.assertFalse(resultSet.next());
        resultSet.clearWarnings(); // Does nothing.
        // For now they are empty.
        Assertions.assertNull(resultSet.getWarnings());
    }
}
