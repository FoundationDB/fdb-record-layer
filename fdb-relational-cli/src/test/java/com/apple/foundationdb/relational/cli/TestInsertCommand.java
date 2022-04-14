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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class TestInsertCommand {

    @RegisterExtension
    CliRule cli = new CliRule();

    @Test
    void testInsert() throws Exception {
        try {
            TestUtils.runCommand("createdb --path /test_insert_db --schema test_insert_schema --schema-template com.apple.foundationdb.record.Restaurant", cli);
            TestUtils.runCommand("connect jdbc:embed:/test_insert_db", cli);
            TestUtils.runCommand("config --no-pretty-print", cli);
            TestUtils.runCommand("config --no-headers", cli);
            TestUtils.runCommand("setschema test_insert_schema", cli);
            String insertRecord = "{\"rest_no\":42,\"name\":\"something\",\"location\":{\"address\":\"address1\",\"latitude\":\"44\",\"longitude\":\"45\"},\"reviews\":[],\"tags\":[],\"customer\":[\"customer1\"]}";
            TestUtils.runCommand("insertinto com.apple.foundationdb.record.Restaurant$RestaurantRecord " + insertRecord, cli);
            String actualRecord = TestUtils.runQueryGetOutput("select * from RestaurantRecord", cli);
            TestUtils.assertJsonObjects(insertRecord, actualRecord);
        } finally {
            TestUtils.deleteDb("test_insert_db", cli);
        }
    }

}
