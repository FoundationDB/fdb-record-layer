/*
 * TestListSchemasCommand.java
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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

class TestListSchemasCommand {
    private static final Pattern ASCII_TABLE_PATTERN = Pattern.compile("([─└┘│┌┐┤├\\s]+)");

    @RegisterExtension
    CliRule cli = new CliRule();

    @Test
    void testListSchemasWithPrettyPrinting() throws RelationalException, SQLException {
        try {
            TestUtils.runCommand("createdb --path /test_list_schemas_db --schema testSchemaA --schema-template com.apple.foundationdb.record.Restaurant", cli);
            TestUtils.runCommand("createdb --path /test_list_schemas_db --schema testSchemaB --schema-template com.apple.foundationdb.record.Restaurant", cli);
            TestUtils.runCommand("createdb --path /test_list_schemas_db --schema testSchemaC --schema-template com.apple.foundationdb.record.Restaurant", cli);
            TestUtils.databaseHasSchemas("test_list_schemas_db", "testSchemaA", "testSchemaB", "testSchemaC");
            TestUtils.runCommand("config --no-headers", cli);
            TestUtils.schemaHasTables("test_list_schemas_db", "testSchemaA", "RestaurantRecord", "RestaurantReviewer");
            TestUtils.schemaHasTables("test_list_schemas_db", "testSchemaB", "RestaurantRecord", "RestaurantReviewer");
            TestUtils.schemaHasTables("test_list_schemas_db", "testSchemaC", "RestaurantRecord", "RestaurantReviewer");
            TestUtils.runCommand("connect jdbc:embed:/test_list_schemas_db", cli);

            //test with both pretty print and not pretty print, just to make sure that the printer works in both cases

            //run with  pretty print
            Set<String> possibleOutputs = ASCII_TABLE_PATTERN.splitAsStream(TestUtils.runCommandGetOutput("listschemas", cli))
                    .filter(str -> !str.isBlank())
                    .collect(Collectors.toSet());
            Assertions.assertEquals(Set.of("testSchemaA", "testSchemaB", "testSchemaC"), possibleOutputs);
        } finally {
            TestUtils.deleteDb("test_list_schemas_db", cli);
        }
    }

    @Test
    void testListSchemasWithoutPrettyPrinting() throws RelationalException, SQLException {
        try {
            TestUtils.runCommand("createdb --path /test_list_schemas_db --schema testSchemaA --schema-template com.apple.foundationdb.record.Restaurant", cli);
            TestUtils.runCommand("createdb --path /test_list_schemas_db --schema testSchemaB --schema-template com.apple.foundationdb.record.Restaurant", cli);
            TestUtils.runCommand("createdb --path /test_list_schemas_db --schema testSchemaC --schema-template com.apple.foundationdb.record.Restaurant", cli);
            TestUtils.databaseHasSchemas("test_list_schemas_db", "testSchemaA", "testSchemaB", "testSchemaC");
            TestUtils.runCommand("config --no-headers", cli);
            TestUtils.schemaHasTables("test_list_schemas_db", "testSchemaA", "RestaurantRecord", "RestaurantReviewer");
            TestUtils.schemaHasTables("test_list_schemas_db", "testSchemaB", "RestaurantRecord", "RestaurantReviewer");
            TestUtils.schemaHasTables("test_list_schemas_db", "testSchemaC", "RestaurantRecord", "RestaurantReviewer");
            TestUtils.runCommand("connect jdbc:embed:/test_list_schemas_db", cli);

            //test with both pretty print and not pretty print, just to make sure that the printer works in both cases

            //now run again without it
            TestUtils.runCommand("config --no-pretty-print", cli);
            Set<String> possibleOutputs = ASCII_TABLE_PATTERN.splitAsStream(TestUtils.runCommandGetOutput("listschemas", cli))
                    .filter(str -> !str.isBlank())
                    .collect(Collectors.toSet());
            Assertions.assertEquals(Set.of("{\"schemas\":testSchemaB}", "{\"schemas\":testSchemaC}", "{\"schemas\":testSchemaA}"), possibleOutputs);
        } finally {
            TestUtils.deleteDb("test_list_schemas_db", cli);
        }
    }

    @Disabled("this test documents current behavior: if we create a database _after_ connecting, we don't see it when calling listSchemas.")
    void testListSchemasAfterCreateDb() throws RelationalException, IOException {
        try {
            TestUtils.runCommand("createdb --path /test_list_schemas_db --schema testSchemaC --schema-template com.apple.foundationdb.record.Restaurant", cli);
            TestUtils.runCommand("createdb --path /test_list_schemas_db --schema testSchemaD --schema-template com.apple.foundationdb.record.Restaurant", cli);
            TestUtils.runCommand("createdb --path /test_list_schemas_db --schema testSchemaE --schema-template com.apple.foundationdb.record.Restaurant", cli);
            TestUtils.runCommand("connect jdbc:embed:/test_list_schemas_db", cli);
            TestUtils.runCommand("config --no-pretty-print", cli);
            Assertions.assertEquals(Set.of("testSchemaC", "testSchemaD", "testSchemaE"),
                    Arrays.stream(TestUtils.runCommandGetOutput("listschemas", cli).split("\\s+")).collect(Collectors.toSet()));
            // test fails if we create a database _after_ connecting, we don't see it when calling listSchemas.
            TestUtils.runCommand("createdb --path /test_list_schemas_db --schema testSchemaF --schema-template com.apple.foundationdb.record.Restaurant", cli);
            Assertions.assertEquals(Set.of("testSchemaC", "testSchemaD", "testSchemaE", "testSchemaF"),
                    Arrays.stream(TestUtils.runCommandGetOutput("listschemas", cli).split("\\s+")).collect(Collectors.toSet()));
        } finally {
            TestUtils.deleteDb("test_list_schemas_db", cli);
        }
    }

}
