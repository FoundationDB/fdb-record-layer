/*
 * SimpleDirectAccessInsertionTests.java
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

import com.apple.foundationdb.relational.api.KeySet;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.RelationalConnection;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.grpc.GrpcConstants;
import com.apple.foundationdb.relational.server.ServerTestUtil;
import com.apple.foundationdb.relational.server.RelationalServer;
import com.apple.foundationdb.relational.utils.DirectAccessApiProtobufFactory;
import com.apple.foundationdb.relational.utils.ResultSetAssert;
import com.apple.foundationdb.relational.utils.TestSchemas;

import com.google.protobuf.Message;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URI;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Simple unit tests around direct-access insertion tests.
 * Very much based on the fdb-relational-core com.apple.foundationdb.relational.recordlayer.SimpleDirectAccessInsertionTests with some
 * copying of test extension Rule logic. We copy because test Rules expect embedded jdbc driver where here we want to
 * exercise the remote jdbc client. We make use of the new class, {@link DirectAccessApiProtobufFactory}, so we don't
 * have to interweave embedded and remote jdbc client connections and to save on pollution of these tests with
 * DynamicMessageBuilder, record-layer, etc.
 */
public class SimpleDirectAccessInsertionTests {
    private static RelationalServer relationalServer;
    private static final String SCHEMA_NAME = "TEST_SCHEMA";
    private static final String SYSDB = "/__SYS";
    private static final String SCHEMA = "CATALOG";
    private static URI databasePath;
    private static String templateName;

    /**
     * Load our JDBCDriver via ServiceLoader so available to test.
     */
    @BeforeAll
    public static void beforeAll() throws SQLException, IOException {
        // Load driver.
        JDBCRelationalDriverTest.getDriver();
        relationalServer = ServerTestUtil.createAndStartRelationalServer(GrpcConstants.DEFAULT_SERVER_PORT);
        // Copied from Simple DatabaseRule Construtor.
        databasePath = URI.create("/" + SimpleDirectAccessInsertionTests.class.getSimpleName());
        templateName = databasePath.getPath().substring(databasePath.getPath().lastIndexOf("/") + 1) +
                "_TEMPLATE";
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

    @BeforeEach
    public void beforeEach() throws SQLException {
        // Here we do what is done inside in the test extension SimpleDatabaseRule... before and after each test.
        String jdbcStr = "jdbc:relational://localhost:" + relationalServer.getGrpcPort() + SYSDB + "?schema=" + SCHEMA;
        try (RelationalConnection connection = JDBCRelationalDriverTest.getDriver().connect(jdbcStr, null)
                .unwrap(RelationalConnection.class)) {
            try (Statement statement = connection.createStatement()) {
                final StringBuilder createStatement =
                        new StringBuilder("CREATE SCHEMA TEMPLATE \"").append(templateName).append("\" ");
                createStatement.append(TestSchemas.restaurant());
                statement.executeUpdate(createStatement.toString());
            }
            try (Statement statement = connection.createStatement()) {
                statement.executeUpdate("CREATE DATABASE \"" + databasePath.getPath() + "\"");
            }
            try (Statement statement = connection.createStatement()) {
                statement.executeUpdate("CREATE SCHEMA \"" + databasePath.getPath() + "/" + SCHEMA_NAME +
                        "\" WITH TEMPLATE \"" + templateName + "\"");
            }
        }
    }

    @AfterEach
    public void afterEach() throws SQLException {
        String jdbcStr = "jdbc:relational://localhost:" + relationalServer.getGrpcPort() + SYSDB + "?schema=" + SCHEMA;
        try (RelationalConnection connection = JDBCRelationalDriverTest.getDriver().connect(jdbcStr, null)
                .unwrap(RelationalConnection.class)) {
            try (Statement statement = connection.createStatement()) {
                statement.executeUpdate("DROP SCHEMA \"" + databasePath.getPath() + "/" + SCHEMA_NAME + "\"");
            }
            try (Statement statement = connection.createStatement()) {
                statement.executeUpdate("DROP DATABASE \"" + databasePath.getPath() + "\"");
            }
            try (Statement statement = connection.createStatement()) {
                var dropStatement = new StringBuilder("DROP SCHEMA TEMPLATE \"").append(templateName).append("\"");
                statement.executeUpdate(dropStatement.toString());
            }
        }
    }

    @Test
    void insertNestedFields() throws Exception {
        String jdbcStr = "jdbc:relational://localhost:" + relationalServer.getGrpcPort() + databasePath.getPath() +
                "?schema=" + SCHEMA_NAME;
        try (RelationalConnection connection = JDBCRelationalDriverTest.getDriver().connect(jdbcStr, null)
                .unwrap(RelationalConnection.class)) {
            try (var statement = connection.createStatement()) {
                Message reviewer =
                        DirectAccessApiProtobufFactory.createAnthonyBourdainReviewer(databasePath, SCHEMA_NAME);
                int inserted = statement.executeInsert(DirectAccessApiProtobufFactory.REVIEWER, reviewer);
                Assertions.assertThat(inserted).withFailMessage("incorrect insertion number!")
                        .isEqualTo(1);
                KeySet key = new KeySet().setKeyColumn("ID", 1L);
                try (RelationalResultSet rrs = statement.executeGet(DirectAccessApiProtobufFactory.REVIEWER,
                        key, Options.NONE)) {
                    ResultSetAssert.assertThat(rrs).hasNextRow().hasRow(reviewer).hasNoNextRow();
                }
            }
        }
    }

    @Test
    void insertMultipleTablesDontMix() throws SQLException {
        // Because RecordLayer allows multiple types within the same keyspace, we need to validate that
        // tables are logically separated.
        String jdbcStr = "jdbc:relational://localhost:" + relationalServer.getGrpcPort() + databasePath.getPath() +
                "?schema=" + SCHEMA_NAME;
        try (RelationalConnection connection = JDBCRelationalDriverTest.getDriver().connect(jdbcStr, null)
                .unwrap(RelationalConnection.class)) {
            try (var statement = connection.createStatement()) {
                var reviewer = DirectAccessApiProtobufFactory.createIsabelHawthornReviewer(databasePath, SCHEMA_NAME);
                Assertions.assertThat(statement.executeInsert(DirectAccessApiProtobufFactory.REVIEWER, reviewer))
                        .isEqualTo(1);
                var restaurant =
                        DirectAccessApiProtobufFactory.createBurgersBurgersRestaurant(databasePath, SCHEMA_NAME);
                Assertions.assertThat(statement.executeInsert(DirectAccessApiProtobufFactory.RESTAURANT, restaurant))
                        .isEqualTo(1);

                // Now make sure that you don't get back the other one
                try (var rrs = statement.executeGet(DirectAccessApiProtobufFactory.RESTAURANT,
                        new KeySet().setKeyColumn("REST_NO", 1L), Options.NONE)) {
                    ResultSetAssert.assertThat(rrs).isEmpty();
                }

                try (var rrs = statement.executeGet(DirectAccessApiProtobufFactory.REVIEWER,
                        new KeySet().setKeyColumn("ID", 2L), Options.NONE)) {
                    ResultSetAssert.assertThat(rrs).isEmpty();
                }

                // Make sure you get back the correct rows from the correct tables
                try (var rrs = statement.executeGet(DirectAccessApiProtobufFactory.RESTAURANT,
                        new KeySet().setKeyColumn("REST_NO", 2L), Options.NONE)) {
                    ResultSetAssert.assertThat(rrs)
                            .hasNextRow()
                            .hasRow(restaurant)
                            .hasNoNextRow();
                }
                try (var rrs = statement.executeGet(DirectAccessApiProtobufFactory.REVIEWER,
                        new KeySet().setKeyColumn("ID", 1L), Options.NONE)) {
                    ResultSetAssert.assertThat(rrs)
                            .hasNextRow()
                            .hasRow(reviewer)
                            .hasNoNextRow();
                }

                // Now scan the data and see if too much comes back
                try (var rrs = statement.executeScan(DirectAccessApiProtobufFactory.RESTAURANT,
                        KeySet.EMPTY, Options.NONE)) {
                    ResultSetAssert.assertThat(rrs)
                            .hasNextRow()
                            .hasRow(restaurant)
                            .hasNoNextRow();
                }

                try (var rrs = statement.executeScan(DirectAccessApiProtobufFactory.REVIEWER,
                        KeySet.EMPTY, Options.NONE)) {
                    ResultSetAssert.assertThat(rrs)
                            .hasNextRow()
                            .hasRow(reviewer)
                            .hasNoNextRow();
                }
            }
        }
    }
}
