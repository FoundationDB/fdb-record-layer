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

import com.apple.foundationdb.relational.grpc.GrpcConstants;
import com.apple.foundationdb.relational.server.ServerTestUtil;
import com.apple.foundationdb.relational.server.RelationalServer;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;

/**
 * Run some simple Statement updates/executes against a remote Relational DB.
 */
public class JDBCSimpleStatementTest {
    private static Driver driver;
    private static final String SYSDB = "/__SYS";
    private static final String SCHEMA = "CATALOG";
    private static final String TESTDB = "/test_db";

    private static RelationalServer relationalServer;

    /**
     * Load our JDBCDriver via ServiceLoader so available to test.
     */
    @BeforeAll
    public static void beforeAll() throws SQLException, IOException {
        driver = JDBCRelationalDriverTest.getDriver();
        relationalServer = ServerTestUtil.createAndStartRelationalServer(GrpcConstants.DEFAULT_SERVER_PORT);
    }

    @AfterAll
    public static void afterAll() throws IOException, SQLException {
        if (relationalServer != null) {
            relationalServer.close();
        }
        // Don't deregister once registered; service loading runs once only it seems.
        // Joys of static initializations.
        // DriverManager.deregisterDriver(driver);
    }

    @Test
    public void simpleStatement() throws SQLException, IOException {
        var jdbcStr = "jdbc:relational://localhost:" + relationalServer.getPort() + SYSDB + "?schema=" + SCHEMA;
        try (Connection connection = JDBCRelationalDriverTest.getDriver().connect(jdbcStr, null)) {
            try (Statement statement = connection.createStatement()) {
                JDBCRelationalStatement jdbcRelationalStatement = statement.unwrap(JDBCRelationalStatement.class);
                // Make this better... currently returns zero how ever many rows we touch.
                Assertions.assertEquals(0,
                        jdbcRelationalStatement.executeUpdate("Drop database \"" + TESTDB + "\""));
                Assertions.assertEquals(0,
                        jdbcRelationalStatement.executeUpdate("CREATE SCHEMA TEMPLATE test_template " +
                                "CREATE TABLE test_table (rest_no int64, name string, PRIMARY KEY(rest_no))"));
                Assertions.assertEquals(0,
                        jdbcRelationalStatement.executeUpdate("create database \"" + TESTDB + "\""));
                Assertions.assertEquals(0,
                        jdbcRelationalStatement.executeUpdate("create schema \"" + TESTDB +
                                "/test_schema\" with template test_template"));
                try (ResultSet resultSet = jdbcRelationalStatement.executeQuery("select * from databases;")) {
                    Assertions.assertNotNull(resultSet);
                    var jdbcRelationalResultSet = resultSet.unwrap(JDBCRelationalResultSet.class);
                    // Exercise some metadata methods to get our jacoco coverage up.
                    Assertions.assertFalse(jdbcRelationalResultSet.isClosed());
                    Assertions.assertEquals(1, jdbcRelationalResultSet.getMetaData().getColumnCount());
                    Assertions.assertEquals("DATABASE_ID",
                            jdbcRelationalResultSet.getMetaData().getColumnName(1));
                    Assertions.assertEquals(Types.VARCHAR, jdbcRelationalResultSet.getMetaData().getColumnType(1));
                    Assertions.assertTrue(jdbcRelationalResultSet.next());
                    Assertions.assertEquals(SYSDB, jdbcRelationalResultSet.getString(1));
                    Assertions.assertTrue(jdbcRelationalResultSet.next());
                    Assertions.assertEquals(TESTDB, jdbcRelationalResultSet.getString(1));
                    Assertions.assertFalse(jdbcRelationalResultSet.next());
                    Assertions.assertEquals(0,
                            jdbcRelationalStatement.executeUpdate("Drop database \"" + TESTDB + "\""));
                }
            }
        }
    }
}
