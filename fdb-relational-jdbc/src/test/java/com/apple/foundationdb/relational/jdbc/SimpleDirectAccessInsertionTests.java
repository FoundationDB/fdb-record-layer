/*
 * SimpleDirectAccessInsertionTests.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2024 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.relational.api.RelationalStruct;
import com.apple.foundationdb.relational.jdbc.grpc.GrpcConstants;
import com.apple.foundationdb.relational.recordlayer.RelationalKeyspaceProvider;
import com.apple.foundationdb.relational.server.ServerTestUtil;
import com.apple.foundationdb.relational.server.RelationalServer;
import com.apple.foundationdb.relational.utils.ResultSetAssert;
import com.apple.foundationdb.relational.utils.TestSchemas;

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
import java.util.HashMap;
import java.util.Map;

/**
 * Simple unit tests around direct-access insertion tests.
 * Very much based on the fdb-relational-core com.apple.foundationdb.relational.recordlayer.SimpleDirectAccessInsertionTests with some
 * copying of test extension Rule logic. We copy because test Rules expect embedded jdbc driver where here we want to
 * exercise the remote jdbc client.
 */
public class SimpleDirectAccessInsertionTests {
    private static RelationalServer relationalServer;
    private static final String SCHEMA_NAME = "TEST_SCHEMA";
    private static final String SYSDBPATH = "/" + RelationalKeyspaceProvider.SYS;
    private static URI databasePath;
    private static String templateName;

    private static final String RESTAURANT = "RESTAURANT";
    private static final String REVIEWER = "RESTAURANT_REVIEWER";

    /**
     * Load our JDBCDriver via ServiceLoader so available to test.
     */
    @BeforeAll
    public static void beforeAll() throws SQLException, IOException {
        // Load driver.
        JDBCRelationalDriverTest.getDriver();
        relationalServer = ServerTestUtil.createAndStartRelationalServer(GrpcConstants.DEFAULT_SERVER_PORT);
        // Copied from Simple DatabaseRule Constructor.
        databasePath = URI.create("/FRL/" + SimpleDirectAccessInsertionTests.class.getSimpleName());
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
        String jdbcStr = "jdbc:relational://localhost:" + relationalServer.getGrpcPort() + SYSDBPATH + "?schema=" + RelationalKeyspaceProvider.CATALOG;
        try (RelationalConnection connection = JDBCRelationalDriverTest.getDriver().connect(jdbcStr, null)
                .unwrap(RelationalConnection.class)) {
            try (Statement statement = connection.createStatement()) {
                String createStatement = "CREATE SCHEMA TEMPLATE \"" + templateName + "\" " +
                        TestSchemas.restaurant();
                statement.executeUpdate(createStatement);
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
        String jdbcStr = "jdbc:relational://localhost:" + relationalServer.getGrpcPort() + SYSDBPATH + "?schema=" + RelationalKeyspaceProvider.CATALOG;
        try (RelationalConnection connection = JDBCRelationalDriverTest.getDriver().connect(jdbcStr, null)
                .unwrap(RelationalConnection.class)) {
            try (Statement statement = connection.createStatement()) {
                statement.executeUpdate("DROP SCHEMA \"" + databasePath.getPath() + "/" + SCHEMA_NAME + "\"");
            }
            try (Statement statement = connection.createStatement()) {
                statement.executeUpdate("DROP DATABASE \"" + databasePath.getPath() + "\"");
            }
            try (Statement statement = connection.createStatement()) {
                statement.executeUpdate("DROP SCHEMA TEMPLATE \"" + templateName + "\"");
            }
        }
    }

    @Test
    void insertNestedFieldsNewAPI() throws Exception {
        String jdbcStr = "jdbc:relational://localhost:" + relationalServer.getGrpcPort() + databasePath.getPath() +
                "?schema=" + SCHEMA_NAME;
        try (RelationalConnection connection = JDBCRelationalDriverTest.getDriver().connect(jdbcStr, null)
                .unwrap(RelationalConnection.class)) {
            try (var statement = connection.createStatement()) {
                var reviewer = JDBCRelationalStruct.newBuilder()
                        .addLong("ID", 1L)
                        .addString("NAME", "Anthony Bourdain")
                        .addString("EMAIL", "abourdain@apple.com")
                        .addStruct("STATS", JDBCRelationalStruct.newBuilder()
                                .addLong("START_DATE", 0L)
                                .addString("SCHOOL_NAME", "Truman High School")
                                .addString("HOMETOWN", "Boise, Indiana")
                                .build())
                        .build();
                int inserted = statement.executeInsert(REVIEWER, reviewer, Options.NONE);
                Assertions.assertThat(inserted).withFailMessage("incorrect insertion number!")
                        .isEqualTo(1);
                KeySet key = new KeySet().setKeyColumn("ID", 1L);
                try (RelationalResultSet rrs = statement.executeGet(REVIEWER,
                        key, Options.NONE)) {
                    ResultSetAssert.assertThat(rrs).hasNextRow().hasColumns(to(reviewer)).hasNoNextRow();
                }
            }
        }
    }

    /**
     * Make a (shallow) Map from a RelationalStruct.
     */
    private Map<String, Object> to(RelationalStruct struct) throws SQLException {
        Map<String, Object> map = new HashMap<>(struct.getMetaData().getColumnCount());
        for (int i = 1; i <= struct.getMetaData().getColumnCount(); i++) {
            map.put(struct.getMetaData().getColumnName(i), struct.getObject(i));
        }
        return map;
    }

    @Test
    void insertMultipleTablesDontMixNewAPI() throws SQLException {
        // Because RecordLayer allows multiple types within the same keyspace, we need to validate that
        // tables are logically separated.
        String jdbcStr = "jdbc:relational://localhost:" + relationalServer.getGrpcPort() + databasePath.getPath() +
                "?schema=" + SCHEMA_NAME;
        try (RelationalConnection connection = JDBCRelationalDriverTest.getDriver().connect(jdbcStr, null)
                .unwrap(RelationalConnection.class)) {
            try (var statement = connection.createStatement()) {
                var reviewer = JDBCRelationalStruct.newBuilder()
                        .addLong("ID", 1L)
                        .addString("NAME", "Jane Doe")
                        .addString("EMAIL", "isabel.hawthowrne@apples.com")
                        .addStruct("STATS", JDBCRelationalStruct.newBuilder()
                                .addLong("START_DATE", 12L)
                                .addString("SCHOOL_NAME", "l'école populaire")
                                .addString("HOMETOWN", "Athens, GA")
                                .build())
                        .build();
                Assertions.assertThat(statement.executeInsert(REVIEWER, reviewer)).isEqualTo(1);
                var restaurant = JDBCRelationalStruct.newBuilder()
                        .addLong("REST_NO", 2L)
                        .addString("NAME", "Burgers Burgers")
                        .addStruct("LOCATION", JDBCRelationalStruct.newBuilder()
                                .addString("ADDRESS", "12345 Easy Street")
                                // Add these fields for now so the ResultSetAssert#hasColumnsExactly will work (source insert
                                // will match the returned ResultSet otherwise, the FRL adds fields to the
                                // ResultSet that were not in the insert... making ResultSetAssert fail.
                                .addString("LATITUDE", "0.000000")
                                .addString("LONGITUDE", "0.000000")
                                .build())
                        .addArray("TAGS", JDBCRelationalArray.newBuilder()
                                .addStruct(JDBCRelationalStruct.newBuilder()
                                        .addString("TAG", "title-123")
                                        .addLong("WEIGHT", 1L)
                                        .build())
                                .build())
                        .addArray("REVIEWS", JDBCRelationalArray.newBuilder()
                                .addStruct(JDBCRelationalStruct.newBuilder()
                                        .addLong("REVIEWER", 1L)
                                        .addLong("RATING", 1L)
                                        .build())
                                .build())
                        .build();
                Assertions.assertThat(statement.executeInsert(RESTAURANT,
                        restaurant)).isEqualTo(1);

                // Now make sure that you don't get back the other one
                try (var rrs = statement.executeGet(RESTAURANT,
                        new KeySet().setKeyColumn("REST_NO", 1L), Options.NONE)) {
                    ResultSetAssert.assertThat(rrs).isEmpty();
                }

                try (var rrs = statement.executeGet(REVIEWER,
                        new KeySet().setKeyColumn("ID", 2L), Options.NONE)) {
                    ResultSetAssert.assertThat(rrs).isEmpty();
                }

                // Make sure you get back the correct rows from the correct tables
                try (var rrs = statement.executeGet(RESTAURANT,
                        new KeySet().setKeyColumn("REST_NO", 2L), Options.NONE)) {
                    ResultSetAssert.assertThat(rrs)
                            .hasNextRow()
                            .hasColumns(to(restaurant))
                            .hasNoNextRow();
                }
                try (var rrs = statement.executeGet(REVIEWER,
                        new KeySet().setKeyColumn("ID", 1L), Options.NONE)) {
                    ResultSetAssert.assertThat(rrs)
                            .hasNextRow()
                            .hasColumns(to(reviewer))
                            .hasNoNextRow();
                }

                // Now scan the data and see if too much comes back
                try (var rrs = statement.executeScan(RESTAURANT,
                        KeySet.EMPTY, Options.NONE)) {
                    ResultSetAssert.assertThat(rrs)
                            .hasNextRow()
                            .hasColumns(to(restaurant))
                            .hasNoNextRow();
                }

                try (var rrs = statement.executeScan(REVIEWER,
                        KeySet.EMPTY, Options.NONE)) {
                    ResultSetAssert.assertThat(rrs)
                            .hasNextRow()
                            .hasColumns(to(reviewer))
                            .hasNoNextRow();
                }
            }
        }
    }
}
