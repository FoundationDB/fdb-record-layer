/*
 * TestQueryCommand.java
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


import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * This tests {@link QueryCommand}.
 */
class TestQueryCommand {
    private static final List<String> insertRecords = Arrays.asList(
            "{\"rest_no\":42,\"name\":\"something\",\"location\":{\"address\":\"address1\",\"latitude\":\"44\",\"longitude\":\"45\"},\"reviews\":[],\"tags\":[],\"customer\":[\"customer1\"]}",
            "{\"rest_no\":43,\"name\":\"something\",\"location\":{\"address\":\"address1\",\"latitude\":\"44\",\"longitude\":\"45\"},\"reviews\":[],\"tags\":[],\"customer\":[\"customer1\"]}",
            "{\"rest_no\":44,\"name\":\"something\",\"location\":{\"address\":\"address1\",\"latitude\":\"44\",\"longitude\":\"45\"},\"reviews\":[],\"tags\":[],\"customer\":[\"customer1\"]}");

    @RegisterExtension
    CliRule cli = new CliRule();

    @BeforeEach
    void setUp() throws Exception {
        TestUtils.createRestaurantSchemaTemplate(cli);

        TestUtils.runCommand("connect jdbc:embed:/__SYS", cli);
        TestUtils.runCommand("setschema catalog", cli);
        TestUtils.runQuery("CREATE DATABASE '/test_select_command_db'", cli);
        TestUtils.runQuery("CREATE SCHEMA '/test_select_command_db/test_select_schema' WITH TEMPLATE restaurant_template", cli);

        TestUtils.runCommand("connect jdbc:embed:/test_select_command_db", cli);
        TestUtils.runCommand("config --no-pretty-print", cli);
        TestUtils.runCommand("config --delimiter ####", cli);
        TestUtils.runCommand("config --headers", cli);
        TestUtils.runCommand("setschema test_select_schema", cli);
        TestUtils.insertIntoTable("test_select_command_db", "test_select_schema", "RestaurantRecord",  insertRecords);
    }

    @AfterEach
    void tearDown() throws Exception {
        TestUtils.runCommand("connect jdbc:embed:/__SYS", cli);
        TestUtils.runCommand("setschema catalog", cli);
        TestUtils.runQuery("drop database '/test_select_command_db'", cli);
    }

    @Test
    void testSelect() throws Exception {
        TestUtils.runCommand("config --no-headers", cli);
        String actualRecord = TestUtils.runQueryGetOutput("select * from RestaurantRecord", cli);
        List<String> actualRecords = Arrays.stream(actualRecord.split("####")).filter(s -> !"\n".equals(s)).collect(Collectors.toList()); // poor man's way of getting array of records.
        TestUtils.assertJsonObjects(insertRecords, actualRecords);
    }

    @Test
    void testSelectWithHeaders() throws Exception {
        String actualRecord = TestUtils.runQueryGetOutput("select * from RestaurantRecord", cli);
        List<String> actualRecords = Arrays.stream(actualRecord.split("####")).filter(s -> !"\n".equals(s)).collect(Collectors.toList()); // poor man's way of getting array of records.
        Assertions.assertEquals(4, actualRecords.size());
        JsonElement element = JsonParser.parseString(actualRecords.get(0));
        Assertions.assertTrue(element.isJsonObject());
        JsonObject object = (JsonObject) element;
        Stream.of("rest_no", "name", "location", "reviews", "customer", "tags").forEach(e -> Assertions.assertTrue(object.has(e)));
        TestUtils.assertJsonObjects(insertRecords, actualRecords.stream().skip(1).collect(Collectors.toUnmodifiableList()));
    }

    @Test
    void selectFailsWithNoSchemaSet() {
        TestUtils.runCommand("disconnect", cli);
        TestUtils.runCommand("connect jdbc:embed:/test_select_command_db", cli);
        Assertions.assertThrows(IllegalStateException.class, () -> TestUtils.runQueryGetOutput("select * from RestaurantRecord", cli), "schema is not set");
    }

    @Test
    void selectFailsWithNoConnection() throws Exception {
        TestUtils.runCommand("disconnect", cli);
        Assertions.assertThrows(IllegalStateException.class, () -> TestUtils.runQueryGetOutput("select * from RestaurantRecord", cli), "no open connection");
    }
}
