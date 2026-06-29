/*
 * JDBCEmbedDriverTest.java
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

package com.apple.foundationdb.relational.jdbc;

import com.apple.foundationdb.relational.api.RelationalDriver;
import com.apple.foundationdb.relational.recordlayer.EmbeddedRelationalExtension;
import com.apple.foundationdb.relational.recordlayer.RelationalKeyspaceProvider;
import com.apple.foundationdb.relational.util.BuildVersion;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.sql.Types;

/**
 * Run some simple Statement updates/executes against the JDBC Embed JDBC Driver.
 * Like the JDBCSimpleStatementTest from over in fdb-relational-jdbc only different around
 * the setup and teardown and less stringent since lots of JDBC is not implemented in
 * the fdb-relational-core.
 */
public class JDBCEmbedDriverTest {

    @RegisterExtension
    @Order(0)
    static final EmbeddedRelationalExtension relationalExtension = new EmbeddedRelationalExtension();

    private static final String SYSDBPATH = "/" + RelationalKeyspaceProvider.SYS;
    private static final String TESTDB = "/FRL/jdbc_test_db";

    private static RelationalDriver getDriver() {
        return relationalExtension.getDriver();
    }

    @Test
    public void testGetPropertyInfo() throws SQLException {
        Assertions.assertThrows(SQLException.class, () -> getDriver().getPropertyInfo("blah", null));
    }

    @Test
    public void testGetMajorVersion() throws Exception {
        Assertions.assertEquals(getDriver().getMajorVersion(), BuildVersion.getInstance().getMajorVersion());
    }

    @Test
    public void testGetMinorVersion() throws Exception {
        Assertions.assertEquals(getDriver().getMinorVersion(), BuildVersion.getInstance().getMinorVersion());
    }

    @Test
    public void testJDBCCompliant() {
        Assertions.assertFalse(getDriver().jdbcCompliant());
    }

    @Test
    public void testGetParentLogger() {
        Assertions.assertThrows(SQLFeatureNotSupportedException.class,
                () -> getDriver().getParentLogger());
    }

    @Test
    public void simpleStatement() throws SQLException {
        // Register the FRL domain for this test's CREATE DATABASE call below.
        RelationalKeyspaceProvider.instance().registerDomainIfNotExists("FRL");
        var jdbcStr = "jdbc:embed:" + SYSDBPATH + "?schema=" + RelationalKeyspaceProvider.CATALOG;
        try (final var connection = getDriver().connect(jdbcStr, null)) {
            try (Statement statement = connection.createStatement()) {
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
                try (final var resultSet = statement.executeQuery("select * from databases where database_id = '" + TESTDB + "'")) {
                    checkSelectStarFromDatabasesResultSet(resultSet);
                }
                try (final var preparedStatement = connection.prepareStatement("select * from databases where database_id = ?")) {
                    preparedStatement.setString(1, TESTDB);
                    try (final var resultSet = preparedStatement.executeQuery()) {
                        checkSelectStarFromDatabasesResultSet(resultSet);
                    }
                }
                // Simple test of parameters in prepared statement.
                String columnName = "DATABASE_ID";
                String columnValue = "/__SYS";
                try (final var preparedStatement =
                        connection.prepareStatement("select * from databases where " + columnName + " = ?")) {
                    preparedStatement.setString(1, columnValue);
                    try (final var resultSet = preparedStatement.executeQuery()) {
                        // Should return one column only in a one row resultset.
                        Assertions.assertEquals(columnName, resultSet.getMetaData().getColumnName(1));
                        Assertions.assertEquals(Types.VARCHAR, resultSet.getMetaData().getColumnType(1));
                        Assertions.assertTrue(resultSet.next());
                        Assertions.assertEquals(columnValue, resultSet.getString(1));
                        Assertions.assertFalse(resultSet.next());
                    }
                }
            } finally {
                try (final var statement = connection.createStatement()) {
                    statement.executeUpdate("Drop database if exists \"" + TESTDB + "\"");
                    statement.executeUpdate("Drop schema template if exists test_template");
                }
            }
        }
    }

    private void checkSelectStarFromDatabasesResultSet(ResultSet resultSet) throws SQLException {
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
        Assertions.assertFalse(resultSet.next());
    }
}
