/*
 * RelationalJDBCDriverTest.java
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

import com.apple.foundationdb.relational.server.ServerTestUtil;
import com.apple.foundationdb.relational.server.RelationalServer;

import org.apple.relational.grpc.GrpcConstants;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.ServiceLoader;

public class RelationalJDBCDriverTest {
    /**
     * Set by beforeAll initialization.
     */
    private static Driver driver;

    /**
     * Load our JDBCDriver via ServiceLoader so available to test.
     */
    @BeforeAll
    public static void beforeAll() throws SQLException {
        for (Iterator<Driver> it = ServiceLoader.load(Driver.class).iterator();
                it.hasNext(); it.next()) {
        }
        driver = DriverManager.getDriver("jdbc:relational://localhost");
        Assertions.assertNotNull(driver);
    }

    @Test
    public void acceptsURL() throws SQLException {
        Assertions.assertTrue(driver.acceptsURL("jdbc:relational://127.0.0.1/db"));
        Assertions.assertTrue(driver.acceptsURL("jdbc:relational://example.org/db"));
        Assertions.assertTrue(driver.acceptsURL("jdbc:relational://example.org:1234/db"));
        Assertions.assertFalse(driver.acceptsURL("jdbc:rubbish://example.org:1234/db"));
        Assertions.assertFalse(driver.acceptsURL("jdbc:relational:WAH"));
    }

    @Test
    public void connect() throws SQLException, IOException {
        try (RelationalServer relationalServer =
                ServerTestUtil.createAndStartRelationalServer(GrpcConstants.DEFAULT_SERVER_PORT)) {
            try (Connection connection =
                    driver.connect("jdbc:relational://localhost:" + relationalServer.getPort(), null)) {
                Assertions.assertFalse(connection.isClosed());
                DatabaseMetaData databaseMetaData = connection.getMetaData();
                Assertions.assertEquals(GrpcConstants.PRODUCT_NAME,
                        databaseMetaData.getDatabaseProductName());
                Assertions.assertEquals(GrpcConstants.PRODUCT_VERSION,
                        databaseMetaData.getDatabaseProductVersion());
            }
        }
    }
}
