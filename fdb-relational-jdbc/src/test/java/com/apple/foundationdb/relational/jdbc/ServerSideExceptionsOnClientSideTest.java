/*
 * ServerSideExceptionsOnClientSideTest.java
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

import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.jdbc.grpc.GrpcConstants;
import com.apple.foundationdb.relational.recordlayer.RelationalKeyspaceProvider;
import com.apple.foundationdb.relational.server.ServerTestUtil;
import com.apple.foundationdb.relational.server.RelationalServer;

import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Run some simple Statement updates/executes against a remote Relational DB.
 */
public class ServerSideExceptionsOnClientSideTest {
    private static final String SYSDBPATH = "/" + RelationalKeyspaceProvider.SYS;
    private static final String TESTDB = "/test_db";

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
    public static void afterAll() throws IOException, SQLException {
        if (relationalServer != null) {
            relationalServer.close();
        }
        // Don't deregister once registered; service loading runs once only it seems.
        // Joys of static initializations.
        // DriverManager.deregisterDriver(driver);
    }

    @Test
    public void simpleStatementProvokesSQLException() throws SQLException {
        var jdbcStr = "jdbc:relational://localhost:" + relationalServer.getGrpcPort() + SYSDBPATH + "?schema=" + RelationalKeyspaceProvider.CATALOG;
        try (Connection connection = JDBCRelationalDriverTest.getDriver().connect(jdbcStr, null)) {
            try (Statement statement = connection.createStatement()) {
                String badSql = "BAD SQL";
                try (JDBCRelationalStatement jdbcRelationalStatement = statement.unwrap(JDBCRelationalStatement.class)) {
                    try (ResultSet resultSet = jdbcRelationalStatement.executeQuery(badSql + ";")) {
                        throw new RuntimeException("Should not get to here!");
                    }
                } catch (SQLException sqlException) {
                    Assertions.assertEquals(sqlException.getSQLState(), ErrorCode.SYNTAX_ERROR.getErrorCode());
                    Assertions.assertTrue(sqlException.getMessage().contains(badSql));
                    Assertions.assertTrue(sqlException.getMessage().contains("syntax error"));
                }
            }
            try (Statement statement = connection.createStatement()) {
                String emptySql = "";
                try (JDBCRelationalStatement jdbcRelationalStatement = statement.unwrap(JDBCRelationalStatement.class)) {
                    try (ResultSet resultSet = jdbcRelationalStatement.executeQuery(emptySql)) {
                        throw new RuntimeException("Should not get to here!");
                    }
                } catch (StatusRuntimeException statusRuntimeException) {
                    Assertions.assertTrue(statusRuntimeException.getMessage().contains("Empty sql"));
                }
            }
        }
    }
}
