/*
 * DbStateInsertCommandTest.java
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

package com.apple.foundationdb.relational.cli;

import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonObject;
import java.net.URI;
import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Inserts are the most complicated commands to test, so let's use a new class just for them.
 */
public class DbStateInsertCommandTest {

    private DbState dbState;

    @BeforeEach
    void setUp() throws RelationalException {
        dbState = new DbState();
    }

    @AfterEach
    void tearDown() throws Exception {
        dbState.close();
    }

    @Test
    void insertCommandWithoutConnectionFails() {
        DbStateCommandFactory factory = new DbStateCommandFactory(dbState);
        CliCommand<Integer> showComm = factory.getInsertCommand("CATALOG", "SCHEMA", Json.createArrayBuilder().build());
        SQLException sqle = Assertions.assertThrows(SQLException.class, showComm::call);
        Assertions.assertEquals(ErrorCode.CONNECTION_DOES_NOT_EXIST.getErrorCode(), sqle.getSQLState(), "Incorrect error code");
    }

    @Test
    void insertCommandWithoutSchemaFails() throws SQLException {
        DbStateCommandFactory factory = new DbStateCommandFactory(dbState);
        factory.getConnectCommand(URI.create("jdbc:embed:/__SYS")).call(); //set connection
        CliCommand<Integer> showComm = factory.getInsertCommand(null, "schema", Json.createArrayBuilder().build());
        SQLException sqle = Assertions.assertThrows(SQLException.class, showComm::call);
        Assertions.assertEquals(ErrorCode.UNDEFINED_SCHEMA.getErrorCode(), sqle.getSQLState(), "Incorrect error code");
    }

    @Test
    void insertCommandWithDefaultSchemaWorks() throws SQLException {
        DbStateCommandFactory factory = new DbStateCommandFactory(dbState);
        factory.getConnectCommand(URI.create("jdbc:embed:/__SYS")).call(); //set connection

        /*
         * Test weirdness alert: We have to create a table here to make sure that we can insert data, so
         * we go through some effort here to create a table
         */
        factory.getSetSchemaCommand("CATALOG").call(); //set schema

        final String cmd = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TABLE test_table (rest_no bigint, name string, PRIMARY KEY(rest_no));";
        Object o = factory.getQueryCommand(cmd).call();
        Assertions.assertTrue(o instanceof Integer, "Did not return an integer!");
        Assertions.assertEquals(0, (Integer) o, "Did not return the correct modification count");

        factory.getQueryCommand("create database \"/test_db\"").call();
        try {
            factory.getQueryCommand("create schema \"/test_db/test_schema\" with template test_template").call();
            factory.getDisconnectCommand().call();
            factory.getConnectCommand(URI.create("jdbc:embed:/test_db")).call();
            factory.getSetSchemaCommand("test_schema").call();

            int insertCount = factory.getInsertCommand(null, "TEST_TABLE", Json.createArrayBuilder().build()).call();
            Assertions.assertEquals(0, insertCount, "Inserted records when it shouldn't have");
        } finally {
            factory.getDisconnectCommand().call();
            factory.getConnectCommand(URI.create("jdbc:embed:/__SYS")).call(); //set connection
            factory.getSetSchemaCommand("CATALOG").call(); //set schema
            factory.getQueryCommand("Drop database \"/test_db\"").call();
        }
    }

    @Test
    void insertCommandWithSpecifiedSchemaWorks() throws SQLException {
        DbStateCommandFactory factory = new DbStateCommandFactory(dbState);
        factory.getConnectCommand(URI.create("jdbc:embed:/__SYS")).call(); //set connection

        /*
         * Test weirdness alert: We have to create a table here to make sure that we can insert data, so
         * we go through some effort here to create a table
         */
        factory.getSetSchemaCommand("CATALOG").call(); //set schema

        final String cmd = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TABLE test_table (rest_no bigint, name string, PRIMARY KEY(rest_no));";
        Object o = factory.getQueryCommand(cmd).call();
        Assertions.assertTrue(o instanceof Integer, "Did not return an integer!");
        Assertions.assertEquals(0, (Integer) o, "Did not return the correct modification count");

        factory.getQueryCommand("create database \"/test_db\"").call();
        try {
            factory.getQueryCommand("create schema \"/test_db/test_schema\" with template test_template").call();
            factory.getDisconnectCommand().call();
            factory.getConnectCommand(URI.create("jdbc:embed:/test_db")).call();

            int insertCount = factory.getInsertCommand("test_schema", "TEST_TABLE", Json.createArrayBuilder().build()).call();
            Assertions.assertEquals(0, insertCount, "Inserted records when it shouldn't have");
        } finally {
            factory.getDisconnectCommand().call();
            factory.getConnectCommand(URI.create("jdbc:embed:/__SYS")).call(); //set connection
            factory.getSetSchemaCommand("CATALOG").call(); //set schema
            factory.getQueryCommand("Drop database \"/test_db\"").call();
        }
    }

    @Test
    void insertCommandWithValidDataWorks() throws SQLException {
        DbStateCommandFactory factory = new DbStateCommandFactory(dbState);
        factory.getConnectCommand(URI.create("jdbc:embed:/__SYS")).call(); //set connection

        /*
         * Test weirdness alert: We have to create a table here to make sure that we can insert data, so
         * we go through some effort here to create a table
         */
        factory.getSetSchemaCommand("CATALOG").call(); //set schema

        final String cmd = "CREATE SCHEMA TEMPLATE test_template " +
                " CREATE TYPE AS STRUCT nested_type (k string, b boolean)" +
                " CREATE TABLE test_table (rest_no bigint, name string, arr nested_type ARRAY, PRIMARY KEY(rest_no))";
        Object o = factory.getQueryCommand(cmd).call();
        Assertions.assertTrue(o instanceof Integer, "Did not return an integer!");
        Assertions.assertEquals(0, (Integer) o, "Did not return the correct modification count");

        factory.getQueryCommand("create database \"/test_db\"").call();
        try {
            factory.getQueryCommand("create schema \"/test_db/test_schema\" with template test_template").call();
            factory.getDisconnectCommand().call();
            factory.getConnectCommand(URI.create("jdbc:embed:/test_db")).call();

            final JsonObject row = Json.createObjectBuilder()
                    .add("REST_NO", 1)
                    .add("NAME", "testRecord")
                    .add("ARR", Json.createArrayBuilder()
                            .add(Json.createObjectBuilder()
                                    .add("K", "hello")
                                    .add("B", false)
                                    .build())
                            .add(Json.createObjectBuilder()
                                    .add("K", "goodbye")
                                    .add("B", true)
                                    .build())
                            .build())
                    .build();
            JsonArray data = Json.createArrayBuilder()
                    .add(row)
                    .build();
            int insertCount = factory.getInsertCommand("test_schema", "TEST_TABLE", data).call();
            Assertions.assertEquals(1, insertCount, "Inserted records when it shouldn't have");

            factory.getSetSchemaCommand("test_schema").call();
            try (ResultSet rs = (ResultSet) factory.getQueryCommand("select * from test_table").call()) {
                //TODO(bfines) import the ResultSetAssert logic into CLI so that we don't have to do this nonsense
                Assertions.assertTrue(rs.next(), "Did not return records!");
                Assertions.assertEquals(1, rs.getLong("REST_NO"), "Incorrect rest no");
                Assertions.assertEquals("testRecord", rs.getString("NAME"), "Incorrect name!");
                Array arrValue = rs.getArray("ARR");
                //this will only work if you don't add columns to the nested type
                Map<String, Boolean> expectedRows = Map.of("hello", false, "goodbye", true);
                Set<String> visitedTracker = new HashSet<>();
                try (ResultSet arrayRs = arrValue.getResultSet()) {
                    while (arrayRs.next()) {
                        final String keyField = arrayRs.getString(1);
                        boolean boolField = arrayRs.getBoolean(2);
                        final Boolean expectedBoolField = expectedRows.get(keyField);
                        Assertions.assertNotNull(expectedBoolField, "Unexpected array contents! k=" + keyField);
                        Assertions.assertEquals(expectedBoolField, boolField, "Incorrect bool field");
                        Assertions.assertTrue(visitedTracker.add(keyField), "Already seen key " + keyField);
                    }
                    Assertions.assertEquals(expectedRows.size(), visitedTracker.size(), "Did not visit the full array");
                }
                Assertions.assertFalse(rs.next(), "Too many records returned!");
            } finally {
                factory.getSetSchemaCommand(null).call();
            }
        } finally {
            factory.getDisconnectCommand().call();
            factory.getConnectCommand(URI.create("jdbc:embed:/__SYS")).call(); //set connection
            factory.getSetSchemaCommand("CATALOG").call(); //set schema
            factory.getQueryCommand("Drop database \"/test_db\"").call();
        }
    }

    @Test
    void insertCommandWithInvalidDataFails() throws SQLException {
        DbStateCommandFactory factory = new DbStateCommandFactory(dbState);
        factory.getConnectCommand(URI.create("jdbc:embed:/__SYS")).call(); //set connection

        /*
         * Test weirdness alert: We have to create a table here to make sure that we can insert data, so
         * we go through some effort here to create a table
         */
        factory.getSetSchemaCommand("CATALOG").call(); //set schema

        final String cmd = "CREATE SCHEMA TEMPLATE test_template " +
                "CREATE TABLE test_table (rest_no bigint, name string,nt string ARRAY, PRIMARY KEY(rest_no))";
        Object o = factory.getQueryCommand(cmd).call();
        Assertions.assertTrue(o instanceof Integer, "Did not return an integer!");
        Assertions.assertEquals(0, (Integer) o, "Did not return the correct modification count");

        factory.getQueryCommand("create database \"/test_db\"").call();
        try {
            factory.getQueryCommand("create schema \"/test_db/test_schema\" with template test_template").call();
            factory.getDisconnectCommand().call();
            factory.getConnectCommand(URI.create("jdbc:embed:/test_db")).call();

            JsonArray data = Json.createArrayBuilder()
                    .add(Json.createObjectBuilder()
                            .add("REST_NO", "a string?")
                            .build())
                    .build();
            final CliCommand<Integer> insertCommand = factory.getInsertCommand("test_schema", "TEST_TABLE", data);
            Assertions.assertThrows(NumberFormatException.class, insertCommand::call);
        } finally {
            factory.getDisconnectCommand().call();
            factory.getConnectCommand(URI.create("jdbc:embed:/__SYS")).call(); //set connection
            factory.getSetSchemaCommand("CATALOG").call(); //set schema
            factory.getQueryCommand("Drop database \"/test_db\"").call();
        }
    }
}
