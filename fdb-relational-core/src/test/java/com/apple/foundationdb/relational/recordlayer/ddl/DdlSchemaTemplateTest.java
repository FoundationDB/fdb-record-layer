/*
 * DdlSchemaTemplateTest.java
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

package com.apple.foundationdb.relational.recordlayer.ddl;

import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.ddl.DdlConnection;
import com.apple.foundationdb.relational.api.ddl.DdlStatement;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.recordlayer.EmbeddedRelationalExtension;
import com.apple.foundationdb.relational.utils.DdlPermutationGenerator;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Stream;

/**
 * End-to-end unit tests for Schema Template language in the RecordLayer.
 */
public class DdlSchemaTemplateTest {
    @RegisterExtension
    public static final EmbeddedRelationalExtension relational = new EmbeddedRelationalExtension();

    public static Stream<Arguments> columnTypePermutations() {
        return DdlPermutationGenerator.generateTables("schemaTemplateTest", 2)
                .map(Arguments::of);
    }

    @Test
    void canDropSchemaTemplates() throws Exception {
        String createColumnStatement = "CREATE SCHEMA TEMPLATE drop_template AS {" +
                "CREATE TYPE FOO_TYPE (a int64);" +
                "CREATE TABLE FOO_TBL (b double PRIMARY KEY(b))" +
                "}";

        try (DdlConnection conn = relational.getEngine().getDdlConnection()) {
            try (DdlStatement statement = conn.createStatement()) {
                statement.execute(createColumnStatement);

                //verify that it's there
                Assertions.assertTrue(statement.execute("DESCRIBE SCHEMA TEMPLATE drop_template"));
                try (RelationalResultSet rrs = statement.getNextResultSet()) {
                    Assertions.assertTrue(rrs.next(), "Didn't find created template!");
                    Assertions.assertEquals("drop_template", rrs.getString(1));
                    Assertions.assertFalse(rrs.next(), "too many schema templates!");
                }

                //now drop it
                statement.execute("DROP SCHEMA TEMPLATE drop_template");

                //verify it's not there, and that means that there is an error trying to describe it
                RelationalException ve = Assertions.assertThrows(RelationalException.class, () -> statement.execute("DESCRIBE SCHEMA TEMPLATE drop_template"));
                Assertions.assertEquals(ErrorCode.UNKNOWN_SCHEMA_TEMPLATE, ve.getErrorCode(), "Incorrect error code");
            }
        }

    }

    @ParameterizedTest
    @MethodSource("columnTypePermutations")
    void describeSchemaTemplate(DdlPermutationGenerator.NamedPermutation table) throws Exception {
        String createColumnStatement = "CREATE SCHEMA TEMPLATE " + table.getName() + " AS { " +
                "CREATE TYPE " + table.getTypeDefinition("TYP") + ";" +
                "CREATE TABLE " + table.getTableDefinition("TBL") +
                "}";
        try (DdlConnection conn = relational.getEngine().getDdlConnection()) {
            try (DdlStatement statement = conn.createStatement()) {
                statement.execute(createColumnStatement);

                Assertions.assertTrue(statement.execute("DESCRIBE SCHEMA TEMPLATE " + table.getName()));
                try (RelationalResultSet rrs = statement.getNextResultSet()) {
                    Assertions.assertTrue(rrs.next(), "Missing schema template description!");

                    String name = rrs.getString(1);
                    Assertions.assertEquals(table.getName(), name, "Incorrect template name!");
                    Collection<?> tables = rrs.getRepeated(2);
                    Collection<?> types = rrs.getRepeated(3);
                    Assertions.assertEquals(1, types.size(), "Incorrect number of types!");
                    Assertions.assertEquals(1, tables.size(), "Incorrect number of tables!");
                    //TODO(bfines) String comparison sucks here, but it's a bit easier than overengineering
                    // a whole TableDescriptor type--eventually, we'll need to do that though, probably
                    String expectedTypeJson = table.getTypeJson("TYP");
                    Assertions.assertEquals(expectedTypeJson, types.stream().findFirst().orElseThrow().toString(), "Incorrect type");
                    String expectedTableJson = table.getTableJson("TBL");
                    Assertions.assertEquals(expectedTableJson, tables.stream().findFirst().orElseThrow().toString(), "Incorrect tables");

                    Assertions.assertFalse(rrs.next(), "Too many results returned!");
                }
            }
        }
    }

    @ParameterizedTest
    @MethodSource("columnTypePermutations")
    void listSchemaTemplatesWorks(DdlPermutationGenerator.NamedPermutation table) throws Exception {
        String columnStatement = "CREATE SCHEMA TEMPLATE <TEST_TEMPLATE> AS { " +
                "CREATE TYPE " + table.getTypeDefinition("FOO") +
                "}";

        try (DdlConnection conn = relational.getEngine().getDdlConnection()) {
            try (DdlStatement statement = conn.createStatement()) {
                //do a scan of template names first, to see if there are any in there. This is mostly a protection
                // against test contamination
                Assertions.assertTrue(statement.execute("SHOW SCHEMA TEMPLATES"), "Did not execute a ResultSet!");
                Set<String> oldTemplateNames = new HashSet<>();
                try (RelationalResultSet rrs = statement.getNextResultSet()) {
                    while (rrs.next()) {
                        oldTemplateNames.add(rrs.getString(1));
                    }
                }
                //now find ourselves a nice template name to use
                int templateNum = oldTemplateNames.stream()
                        .filter(name -> name.startsWith(table.getName()))
                        .map(name -> name.substring(name.lastIndexOf("_" + 1)))
                        .mapToInt(name -> Integer.parseInt(name.trim()))
                        .max().orElse(0);
                columnStatement = columnStatement.replace("<TEST_TEMPLATE>", (table.getName() + templateNum));
                statement.execute(columnStatement);

                Assertions.assertTrue(statement.execute("SHOW SCHEMA TEMPLATES"), "Did not execut a ResultSet!");
                Set<String> templateNames = new HashSet<>();
                try (RelationalResultSet rrs = statement.getNextResultSet()) {
                    Assertions.assertTrue(rrs.next(), "No Templates in the returned result set!");
                    boolean next = true;
                    while (next) {
                        templateNames.add(rrs.getString(1));
                        next = rrs.next();
                    }
                }

                oldTemplateNames.add(table.getName() + templateNum);
                Assertions.assertEquals(oldTemplateNames, templateNames, "Incorrect returned Schema template list!");
            }
        }
    }
}
