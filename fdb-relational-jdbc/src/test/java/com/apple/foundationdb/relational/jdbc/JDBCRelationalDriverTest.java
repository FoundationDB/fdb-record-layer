/*
 * JDBCRelationalDriverTest.java
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
import com.apple.foundationdb.relational.util.BuildVersion;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.ServiceLoader;

public class JDBCRelationalDriverTest {
    /**
     * Set by beforeAll initialization.
     */
    private static Driver driver;

    static Driver getDriver() throws SQLException {
        // Load ServiceLoader Services.
        for (Driver value : ServiceLoader.load(Driver.class)) {
            // Intentionally empty
        }
        // Use ANY valid URl to get hold of the driver. When we 'connect' we'll
        // more specific about where we want to connect to.
        var driver = DriverManager.getDriver("jdbc:relational://localhost/__SYS");
        Assertions.assertNotNull(driver);
        return driver;
    }

    /**
     * Load our JDBCDriver via ServiceLoader so available to test.
     */
    @BeforeAll
    public static void beforeAll() throws SQLException {
        driver = getDriver();
    }

    @AfterEach
    public void afterAll() throws SQLException {
        // Don't deregister once registered; service loading runs once only it seems.
        // Joys of static initializations.
        // DriverManager.deregisterDriver(driver);
    }

    @Test
    public void acceptsURL() throws SQLException {
        Assertions.assertTrue(driver.acceptsURL("jdbc:relational://127.0.0.1/db"));
        Assertions.assertTrue(driver.acceptsURL("jdbc:relational://example.org/db"));
        Assertions.assertTrue(driver.acceptsURL("jdbc:relational://example.org:1234/db"));
        Assertions.assertFalse(driver.acceptsURL("jdbc:rubbish://example.org:1234/db"));
        Assertions.assertFalse(driver.acceptsURL("jdbc:relational:WAH"));
        SQLException sqlException = null;
        // Assert bad connection url throws.
        try {
            driver.connect("jdbc:bad_url://", null);
        } catch (SQLException e) {
            sqlException = e;
        }
        Assertions.assertNotNull(sqlException);
    }

    public void testDriverMiscellaneous() throws SQLException {
        Assertions.assertFalse(driver.jdbcCompliant());
        // Currently getPropertyInfo doesn't do anything.
        DriverPropertyInfo[] driverPropertyInfos = driver.getPropertyInfo("anyOldString", null);
        Assertions.assertEquals(0, driverPropertyInfos.length);
    }

    @Test
    public void connectAndGetDatabaseMetaData() throws SQLException, IOException {
        try (RelationalServer relationalServer =
                ServerTestUtil.createAndStartRelationalServer(GrpcConstants.DEFAULT_SERVER_PORT)) {
            try (Connection connection =
                    driver.connect("jdbc:relational://localhost:" + relationalServer.getGrpcPort() + "/__SYS", null)) {
                Assertions.assertFalse(connection.isClosed());
                DatabaseMetaData databaseMetaData = connection.getMetaData();
                // These should be the same. One version is read from the server, the
                // other is read by looking at the classpath. Ditto for the URL.
                // They'll be the same in test context. They will likely not be the same in production,
                // at least sometimes.
                Assertions.assertEquals(BuildVersion.getInstance().getVersion(),
                        databaseMetaData.getDatabaseProductVersion());
                Assertions.assertEquals(BuildVersion.getInstance().getMajorVersion(),
                        databaseMetaData.getDatabaseMajorVersion());
                Assertions.assertEquals(BuildVersion.getInstance().getMinorVersion(),
                        databaseMetaData.getDatabaseMinorVersion());
                Assertions.assertEquals(BuildVersion.getInstance().getURL(),
                        databaseMetaData.getURL());
            }
        }
    }
}
