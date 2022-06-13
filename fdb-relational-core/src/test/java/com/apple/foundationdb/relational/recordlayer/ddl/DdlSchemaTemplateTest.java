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

import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.Relational;
import com.apple.foundationdb.relational.api.RelationalConnection;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.recordlayer.EmbeddedRelationalExtension;
import com.apple.foundationdb.relational.utils.DdlPermutationGenerator;

import com.google.common.base.Strings;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.net.URI;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
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
                "CREATE STRUCT FOO_TYPE (a int64);" +
                "CREATE TABLE FOO_TBL (b double PRIMARY KEY(b))" +
                "}";

        try (RelationalConnection conn = Relational.connect(URI.create("jdbc:embed:/__SYS"), Options.none())) {
            conn.setSchema("catalog");
            try (Statement statement = conn.createStatement()) {
                statement.executeUpdate(createColumnStatement);

                //verify that it's there
                try (ResultSet rs = statement.executeQuery("DESCRIBE SCHEMA TEMPLATE drop_template")) {
                    Assertions.assertTrue(rs.next(), "Didn't find created template!");
                    Assertions.assertEquals("drop_template", rs.getString(1));
                    Assertions.assertFalse(rs.next(), "too many schema templates!");
                }
                //now drop it
                statement.executeUpdate("DROP SCHEMA TEMPLATE drop_template");

                //verify it's not there, and that means that there is an error trying to describe it
                SQLException ve = Assertions.assertThrows(SQLException.class, () -> statement.executeQuery("DESCRIBE SCHEMA TEMPLATE drop_template"));
                Assertions.assertEquals(ErrorCode.UNKNOWN_SCHEMA_TEMPLATE.getErrorCode(), ve.getSQLState(), "Incorrect error code");
            }

        }
    }

    @ParameterizedTest
    @MethodSource("columnTypePermutations")
    void describeSchemaTemplate(DdlPermutationGenerator.NamedPermutation table) throws Exception {
        String createColumnStatement = "CREATE SCHEMA TEMPLATE " + table.getName() + " AS { " +
                "CREATE STRUCT " + table.getTypeDefinition("TYP") + ";" +
                "CREATE TABLE " + table.getTableDefinition("TBL") +
                "}";
        try (RelationalConnection conn = Relational.connect(URI.create("jdbc:embed:/__SYS"), Options.none())) {
            conn.setSchema("catalog");
            try (Statement statement = conn.createStatement()) {
                statement.executeUpdate(createColumnStatement);

                try (ResultSet rs = statement.executeQuery("DESCRIBE SCHEMA TEMPLATE " + table.getName())) {
                    Assertions.assertTrue(rs.next(), "Missing schema template description!");
                    String name = rs.getString(1);
                    Assertions.assertEquals(table.getName(), name, "Incorrect template name!");
                    Assertions.assertTrue(rs instanceof RelationalResultSet);
                    RelationalResultSet rrs = (RelationalResultSet) rs;
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
                "CREATE STRUCT " + table.getTypeDefinition("FOO") +
                "}";

        try (RelationalConnection conn = Relational.connect(URI.create("jdbc:embed:/__SYS"), Options.none())) {
            conn.setSchema("catalog");
            try (Statement statement = conn.createStatement()) {
                //do a scan of template names first, to see if there are any in there. This is mostly a protection
                // against test contamination
                Set<String> oldTemplateNames = new HashSet<>();
                try (ResultSet rs = statement.executeQuery("SHOW SCHEMA TEMPLATES")) {
                    while (rs.next()) {
                        oldTemplateNames.add(rs.getString(1));
                    }
                }
                //now find ourselves a nice template name to use.
                //apply Gödel's diagnolisation
                final var namesAsList = oldTemplateNames
                        .stream().map(s -> s.length() < oldTemplateNames.size() ? s = s + Strings.repeat(" ", oldTemplateNames.size() - s.length() + 1) : s)
                        .collect(Collectors.toList());
                final var unique = IntStream.range(0, namesAsList.size())
                        .mapToObj(i -> namesAsList.get(i).charAt(i) == Character.MAX_VALUE ? (char) i : (char) (namesAsList.get(i).charAt(i) + 1))
                        .map(Object::toString)
                        .collect(Collectors.joining()).concat("NEW_TEMPLATE");
                columnStatement = columnStatement.replace("<TEST_TEMPLATE>", unique);
                statement.executeUpdate(columnStatement);
                Set<String> templateNames = new HashSet<>();
                try (ResultSet rs = statement.executeQuery("SHOW SCHEMA TEMPLATES")) {
                    while (rs.next()) {
                        templateNames.add(rs.getString(1));
                    }
                }

                oldTemplateNames.add(unique);
                Assertions.assertEquals(oldTemplateNames, templateNames, "Incorrect returned Schema template list!");
            }
        }
    }
}
