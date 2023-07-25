/*
 * JDBCEmbedDriverTest.java
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
import com.apple.foundationdb.relational.recordlayer.RelationalKeyspaceProvider;
import com.apple.foundationdb.relational.util.BuildVersion;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Types;

/**
 * Run some simple Statement updates/executes against a the JDBC Embed JDBC Driver.
 * Like the JDBCSimpleStatementTest from over in fdb-relational-jdbc only different around
 * the setup and teardown and less stringent since lots of JDBC is not implemented in
 * the fdb-relational-core.
 */
public class JDBCEmbedDriverTest {
    static {
        // Load the below JDBC driver class and it will register itself with the JDBC DriverManager.
        new JDBCEmbedDriver();
    }

    static Driver getDriver() throws SQLException {
        // Use ANY valid URl to get hold of the driver. When we 'connect' we'll
        // more specific about where we want to connect to.
        return DriverManager.getDriver("jdbc:embed:" + SYSDBPATH);
    }

    private static final String SYSDBPATH = "/" + RelationalKeyspaceProvider.SYS;
    private static final String TESTDB = "/FRL/jdbc_test_db";

    @AfterAll
    public static void afterAll() throws IOException, SQLException {
        DriverManager.deregisterDriver(getDriver());
    }

    @Test
    public void testGetPropertyInfo() throws SQLException {
        Assertions.assertNotNull(getDriver().getPropertyInfo(JDBCEmbedDriver.JDBC_URL_PREFIX, null));
    }

    public void testGetMajorVersion() throws SQLException {
        Assertions.assertEquals(getDriver().getMajorVersion(), BuildVersion.getInstance().getMajorVersion());
    }

    public void testGetMininVersion() throws SQLException {
        Assertions.assertEquals(getDriver().getMinorVersion(), BuildVersion.getInstance().getMinorVersion());
    }

    public void testJDBCCompliant() throws SQLException {
        Assertions.assertFalse(getDriver().jdbcCompliant());
    }

    public void testGetParentLogger() throws SQLException {
        Assertions.assertThrows(SQLFeatureNotSupportedException.class,
                () -> getDriver().getParentLogger());
    }

    @Test
    public void simpleStatement() throws SQLException, IOException {
        var jdbcStr = "jdbc:embed:" + SYSDBPATH + "?schema=" + RelationalKeyspaceProvider.CATALOG;
        try (RelationalConnection connection = getDriver().connect(jdbcStr, null)
                .unwrap(RelationalConnection.class)) {
            try (RelationalStatement statement = connection.createStatement().unwrap(RelationalStatement.class)) {
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
                try (RelationalResultSet resultSet = statement.executeQuery("select * from databases;")
                        .unwrap(RelationalResultSet.class)) {
                    checkSelectStarFromDatabasesResultSet(resultSet);
                }
                try (RelationalPreparedStatement preparedStatement = connection
                        .prepareStatement("select * from databases;").unwrap(RelationalPreparedStatement.class)) {
                    try (RelationalResultSet resultSet = preparedStatement.executeQuery()) {
                        checkSelectStarFromDatabasesResultSet(resultSet);
                    }
                }
                // Simple test of parameters in prepared statement.
                String columnName = "DATABASE_ID";
                String columnValue = "/__SYS";
                try (RelationalPreparedStatement preparedStatement =
                        connection.prepareStatement("select * from databases where " + columnName + " = ?;")) {
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
                    statement.executeUpdate("Drop database if exists \"" + TESTDB + "\"");
                    statement.executeUpdate("Drop schema template if exists test_template");
                }
            }
        }
    }

    private void checkSelectStarFromDatabasesResultSet(RelationalResultSet resultSet) throws SQLException {
        Assertions.assertNotNull(resultSet);
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
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals(SYSDBPATH, resultSet.getString(1));
        Assertions.assertEquals(SYSDBPATH, resultSet.getString(columnName));
        Assertions.assertFalse(resultSet.next());
    }
}
