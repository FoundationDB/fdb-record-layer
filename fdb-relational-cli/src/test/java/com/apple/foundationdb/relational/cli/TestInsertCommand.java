/*
 * TestInsertCommand.java
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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class TestInsertCommand {

    @RegisterExtension
    CliRule cli = new CliRule();

    @BeforeEach
    void setUp() throws Exception {
        TestUtils.createRestaurantSchemaTemplate(cli);
        TestUtils.runCommand("connect jdbc:embed:/__SYS", cli);
        TestUtils.runCommand("setschema catalog", cli);
        TestUtils.runQuery("CREATE DATABASE '/test_insert_db'", cli);
        TestUtils.runQuery("CREATE SCHEMA '/test_insert_db/test_insert_schema' WITH TEMPLATE restaurant_template", cli);

        TestUtils.runCommand("connect jdbc:embed:/test_insert_db", cli);
        TestUtils.runCommand("config --no-pretty-print", cli);
        TestUtils.runCommand("config --no-headers", cli);
        TestUtils.runCommand("setschema test_insert_schema", cli);
    }

    @AfterEach
    void tearDown() throws Exception {
        TestUtils.dropDb("test_insert_db", cli);
    }

    @Test
    void testInsert() throws Exception {
        final String insertRecord = "{\"rest_no\":42,\"name\":\"something\",\"location\":{\"address\":\"address1\",\"latitude\":\"44\",\"longitude\":\"45\"},\"reviews\":[],\"tags\":[],\"customer\":[\"customer1\"]}";
        TestUtils.runCommand("insertinto RestaurantRecord " + insertRecord, cli);
        final String actualRecord = TestUtils.runQueryGetOutput("select * from RestaurantRecord", cli);
        TestUtils.assertJsonObjects(insertRecord, actualRecord);

    }

    @Test
    void testInsertArray() throws Exception {
        final String insertRecord = "{\"rest_no\":51,\"name\":\"something\",\"location\":{\"address\":\"address1\",\"latitude\":\"44\",\"longitude\":\"45\"},\"reviews\":[],\"tags\":[{\"tag\":\"aTag\",\"weight\":\"12\"}],\"customer\":[\"customer1\"]}";
        TestUtils.runCommand("insertinto RestaurantRecord [" + insertRecord + "]", cli);
        final String actualRecord = TestUtils.runQueryGetOutput("select * from RestaurantRecord", cli);
        TestUtils.assertJsonObjects(insertRecord, actualRecord);
    }

    @Test
    void cannotInsertBadType() throws Exception {
        final String insertRecord = "{\"no_such_field\": false}";
        final int exitCode = cli.getCmd().execute(("insertinto RestaurantRecord " + insertRecord).split("\\s+"));
        Assertions.assertNotEquals(0, exitCode, "Did not report an error!");
    }

    @Test
    void invalidArrayColumn() throws Exception {
        final String insertRecord = "{\"rest_no\":51,\"name\":\"something\",\"location\":{\"address\":\"address1\",\"latitude\":\"44\",\"longitude\":\"45\"},\"reviews\":[],\"tags\":[{\"bad_column\":\"aTag\",\"weight\":\"12\"}],\"customer\":[\"customer1\"]}";
        final int exitCode = cli.getCmd().execute(("insertinto RestaurantRecord " + insertRecord).split("\\s+"));
        Assertions.assertNotEquals(0, exitCode, "Did not report an error!");
    }

}
