/*
 * JDBCRelationalDatabaseMetaDataTest.java
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

import com.apple.foundationdb.relational.api.RelationalDatabaseMetaData;
import com.apple.foundationdb.relational.grpc.jdbc.v1.DatabaseMetaDataResponse;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;

public class JDBCRelationalDatabaseMetaDataTest {
    private static DatabaseMetaData databaseMetaData;
    private static Connection connection;

    @BeforeAll
    public static void beforeAll() {
        DatabaseMetaDataResponse response = DatabaseMetaDataResponse.newBuilder().build();
        connection = new JDBCRelationalConnection(JDBCRelationalDriver.JDBC_BASE_URL + "example.com");
        databaseMetaData = new JDBCRelationalDatabaseMetaData(connection, response);
    }

    @AfterAll
    public static void afterAll() throws SQLException {
        if (databaseMetaData != null) {
            if (databaseMetaData.getConnection() != null) {
                databaseMetaData.getConnection().close();
            }
        }
    }

    @Test
    public void testUnwrap() throws SQLException {
        Assertions.assertTrue(databaseMetaData.isWrapperFor(RelationalDatabaseMetaData.class));
        databaseMetaData.unwrap(RelationalDatabaseMetaData.class);
        Assertions.assertTrue(databaseMetaData.isWrapperFor(JDBCRelationalDatabaseMetaData.class));
        databaseMetaData.unwrap(JDBCRelationalDatabaseMetaData.class);
        Assertions.assertFalse(databaseMetaData.isWrapperFor(String.class));
    }

    /**
     * Silly little test just to get some jacoco relief.
     */
    @Test
    public void testVersion() throws SQLException {
        Assertions.assertEquals(databaseMetaData.getDriverVersion(), databaseMetaData.getDriverVersion());
        // For now, jdbc version == driver version; at least in test context.
        Assertions.assertEquals(databaseMetaData.getDriverMinorVersion(), databaseMetaData.getJDBCMinorVersion());
        Assertions.assertEquals(databaseMetaData.getDriverMajorVersion(), databaseMetaData.getJDBCMajorVersion());
    }

    /**
     * Silly little test just to get some jacoco relief.
     */
    @Test
    public void testTransactionSupport() throws SQLException {
        Assertions.assertTrue(databaseMetaData.supportsTransactions());
        Assertions.assertEquals(databaseMetaData.getDefaultTransactionIsolation(),
                connection.getTransactionIsolation());
        Assertions.assertTrue(
                databaseMetaData.supportsTransactionIsolationLevel(databaseMetaData.getDefaultTransactionIsolation()));
    }

    @Test
    public void testNames() throws SQLException {
        Assertions.assertEquals(databaseMetaData.getDriverName(), JDBCRelationalDriver.DRIVER_NAME);
        Assertions.assertEquals(databaseMetaData.getDatabaseProductName(),
                databaseMetaData.unwrap(RelationalDatabaseMetaData.class).DATABASE_PRODUCT_NAME);
    }
}
