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
import com.apple.foundationdb.relational.utils.CatalogOperations;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.Types;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Run some simple Statement updates/executes against a remote Relational DB.
 */
public class JDBCSimpleStatementTest {
    private static final String SYSDBPATH = "/" + RelationalKeyspaceProvider.SYS;

    private static RelationalServer relationalServer;

    // Per-instance DB and template names so parallel test classes don't collide on the catalog.
    private final String testDb;
    private final String schemaTemplate;

    public JDBCSimpleStatementTest() {
        final String suffix = Long.toHexString(ThreadLocalRandom.current().nextLong());
        this.testDb = "/FRL/jdbc_simple_test_db_" + suffix;
        this.schemaTemplate = "test_template_" + suffix;
    }

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
            // Catalog setup runs under the JVM-wide catalog lock (via CatalogOperations) so we
            // don't race with other test classes doing their own CREATE/DROP DATABASE on the
            // same FDB cluster.
            CatalogOperations.runLockedWithRetry(() -> {
                try (RelationalStatement setupStmt = connection.createStatement()) {
                    Assertions.assertEquals(0, setupStmt.executeUpdate("Drop database if exists \"" + testDb + "\""));
                    Assertions.assertEquals(0, setupStmt.executeUpdate("Drop schema template if exists " + schemaTemplate));
                    Assertions.assertEquals(0,
                            setupStmt.executeUpdate("CREATE SCHEMA TEMPLATE " + schemaTemplate + " " +
                                    "CREATE TABLE test_table (rest_no bigint, name string, PRIMARY KEY(rest_no))"));
                    Assertions.assertEquals(0, setupStmt.executeUpdate("create database \"" + testDb + "\""));
                    Assertions.assertEquals(0, setupStmt.executeUpdate("create schema \"" + testDb +
                            "/test_schema\" with template " + schemaTemplate));
                }
            });
            try (RelationalStatement statement = connection.createStatement()) {
                // Exercise some methods to up our test coverage metrics
                Assertions.assertEquals(connection, statement.getConnection());
                // Call some of the statement methods for the sake of exercising coverage.
                Assertions.assertNull(statement.getWarnings());
                // Does nothing.
                statement.clearWarnings();
                // Cancel currently does nothing.
                statement.cancel();
                Assertions.assertFalse(statement.isClosed());
                // Filter to just our own database + SYS so other concurrent test classes'
                // leftover DBs don't pollute the assertion.
                try (RelationalResultSet resultSet = statement.executeQuery(
                        "select * from databases where database_id in ('" + testDb + "', '" + SYSDBPATH + "')")) {
                    checkSelectStarFromDatabasesResultSet(resultSet);
                }
                try (RelationalPreparedStatement preparedStatement =
                        connection.prepareStatement("select * from databases where database_id in ('" + testDb + "', '" + SYSDBPATH + "')")) {
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
                CatalogOperations.runLockedWithRetry(() -> {
                    try (RelationalStatement cleanupStmt = connection.createStatement()) {
                        cleanupStmt.executeUpdate("Drop database if exists \"" + testDb + "\"");
                        cleanupStmt.executeUpdate("Drop schema template if exists " + schemaTemplate);
                    }
                });
            }
        }
    }

    private void checkSelectStarFromDatabasesResultSet(RelationalResultSet resultSet) throws SQLException {
        Assertions.assertNotNull(resultSet);
        Assertions.assertTrue(resultSet.isWrapperFor(RelationalResultSetFacade.class));
        // Exercise some metadata methods to get our jacoco coverage up.
        Assertions.assertEquals(1, resultSet.getMetaData().getColumnCount());
        String columnName = "DATABASE_ID";
        Assertions.assertEquals(columnName, resultSet.getMetaData().getColumnName(1));
        // Label == name for now.
        Assertions.assertEquals(columnName, resultSet.getMetaData().getColumnLabel(1));
        Assertions.assertEquals(Types.VARCHAR, resultSet.getMetaData().getColumnType(1));
        // The result-set ordering isn't guaranteed relative to our test's DB and /__SYS, so
        // collect into a set and assert set equality. Also exercises getString(columnName) and
        // its lower-cased variant on each row.
        final Set<String> ids = new HashSet<>();
        while (resultSet.next()) {
            final String id = resultSet.getString(1);
            Assertions.assertEquals(id, resultSet.getString(columnName));
            Assertions.assertEquals(id, resultSet.getString(columnName.toLowerCase()));
            ids.add(id);
        }
        Assertions.assertEquals(Set.of(testDb, SYSDBPATH), ids);
        resultSet.clearWarnings(); // Does nothing.
        // For now they are empty.
        Assertions.assertNull(resultSet.getWarnings());
    }
}
