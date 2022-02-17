/*
 * TestSelectCommand.java
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

import com.apple.foundationdb.relational.api.exceptions.RelationalException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

class TestSelectCommand {

    @RegisterExtension
    CliRule cli = new CliRule();

    @Test
    void testSelect() throws RelationalException {
        try {
            TestUtils.runCommand("createdb --path /test_select_command_db --schema test_select_schema --schema-template com.apple.foundationdb.record.Restaurant", cli);
            List<String> insertRecords = Arrays.asList(
                    "{\"rest_no\":\"42\",\"name\":\"something\",\"location\":{\"address\":\"address1\",\"latitude\":\"44\",\"longitude\":\"45\"},\"reviews\":[],\"tags\":[],\"customer\":[\"customer1\"]}",
                    "{\"rest_no\":\"43\",\"name\":\"something\",\"location\":{\"address\":\"address1\",\"latitude\":\"44\",\"longitude\":\"45\"},\"reviews\":[],\"tags\":[],\"customer\":[\"customer1\"]}",
                    "{\"rest_no\":\"44\",\"name\":\"something\",\"location\":{\"address\":\"address1\",\"latitude\":\"44\",\"longitude\":\"45\"},\"reviews\":[],\"tags\":[],\"customer\":[\"customer1\"]}");
            TestUtils.runCommand("connect rlsc:embed:/test_select_command_db", cli);
            TestUtils.runCommand("config --no-pretty-print", cli);
            TestUtils.runCommand("config --delimiter ####", cli);
            TestUtils.runCommand("setschema test_select_schema", cli);
            TestUtils.insertIntoTable("test_select_command_db", "test_select_schema", "RestaurantRecord", "com.apple.foundationdb.record.Restaurant$RestaurantRecord", insertRecords);
            String actualRecord = TestUtils.runCommandGetOutput("select com.apple.foundationdb.record.Restaurant$RestaurantRecord", cli);
            List<String> actualRecords = Arrays.stream(actualRecord.split("####")).filter(s -> !"\n".equals(s)).collect(Collectors.toList()); // poor man's way of getting array of records.
            TestUtils.assertJsonObjects(insertRecords, actualRecords);
        } finally {
            TestUtils.deleteDb("test_select_command_db", cli);
        }
    }

}
