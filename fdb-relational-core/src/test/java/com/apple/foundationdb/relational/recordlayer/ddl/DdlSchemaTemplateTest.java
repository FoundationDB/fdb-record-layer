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

import com.apple.foundationdb.relational.api.FieldDescription;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.Row;
import com.apple.foundationdb.relational.api.RowArray;
import com.apple.foundationdb.relational.api.StructMetaData;
import com.apple.foundationdb.relational.api.Relational;
import com.apple.foundationdb.relational.api.RelationalConnection;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.RelationalStatement;
import com.apple.foundationdb.relational.api.RelationalStructMetaData;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.recordlayer.EmbeddedRelationalExtension;
import com.apple.foundationdb.relational.utils.DdlPermutationGenerator;
import com.apple.foundationdb.relational.utils.ResultSetAssert;
import com.apple.foundationdb.relational.utils.RelationalAssertions;

import com.google.common.base.Strings;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.net.URI;
import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
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
        return DdlPermutationGenerator.generateTables("SCHEMA_TEMPLATE_TEST", 2)
                .map(Arguments::of);
    }

    @Test
    void canDropSchemaTemplates() throws Exception {
        String createColumnStatement = "CREATE SCHEMA TEMPLATE drop_template " +
                "CREATE STRUCT FOO_TYPE (a int64)" +
                " CREATE TABLE FOO_TBL (b double, PRIMARY KEY(b))";

        try (RelationalConnection conn = Relational.connect(URI.create("jdbc:embed:/__SYS"), Options.NONE)) {
            conn.setSchema("CATALOG");
            try (Statement statement = conn.createStatement()) {
                statement.executeUpdate(createColumnStatement);

                //verify that it's there
                try (ResultSet rs = statement.executeQuery("DESCRIBE SCHEMA TEMPLATE drop_template")) {
                    Assertions.assertTrue(rs.next(), "Didn't find created template!");
                    Assertions.assertEquals("DROP_TEMPLATE", rs.getString(1));
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
        String createColumnStatement = "CREATE SCHEMA TEMPLATE " + table.getName() + "  " +
                "CREATE STRUCT " + table.getTypeDefinition("TYP") +
                " CREATE TABLE " + table.getTableDefinition("TBL");
        try (RelationalConnection conn = Relational.connect(URI.create("jdbc:embed:/__SYS"), Options.NONE)) {
            conn.setSchema("CATALOG");
            try (RelationalStatement statement = conn.createStatement()) {
                statement.executeUpdate(createColumnStatement);

                try (RelationalResultSet rs = statement.executeQuery("DESCRIBE SCHEMA TEMPLATE " + table.getName())) {
                    Collection<Row> expectedTables = List.of(table.getPermutationAsRow("TBL"));
                    StructMetaData expectedTableMetaData = new RelationalStructMetaData(
                            FieldDescription.primitive("TABLE_NAME", Types.VARCHAR, false),
                            FieldDescription.array("COLUMNS", false,
                                    new RelationalStructMetaData(
                                            FieldDescription.primitive("COLUMN_NAME", Types.VARCHAR, false),
                                            FieldDescription.primitive("COLUMN_TYPE", Types.INTEGER, false)
                                    ))
                    );
                    Array expectedTablesArr = new RowArray(expectedTables, expectedTableMetaData);
                    Collection<Row> expectedTypes = List.of(table.getPermutationAsRow("TYP"));
                    StructMetaData expectedTypeMetaData = new RelationalStructMetaData(
                            FieldDescription.primitive("TYPE_NAME", Types.VARCHAR, false),
                            FieldDescription.array("COLUMNS", false,
                                    new RelationalStructMetaData(
                                            FieldDescription.primitive("COLUMN_NAME", Types.VARCHAR, false),
                                            FieldDescription.primitive("COLUMN_TYPE", Types.INTEGER, false)
                                    ))
                    );
                    Array expectedTypesArr = new RowArray(expectedTypes, expectedTypeMetaData);

                    ResultSetAssert.assertThat(rs)
                            .hasNextRow()
                            .hasRowExactly(table.getName(), expectedTypesArr, expectedTablesArr)
                            .hasNoNextRow();
                }
            }
        }
    }

    @ParameterizedTest
    @MethodSource("columnTypePermutations")
    void listSchemaTemplatesWorks(DdlPermutationGenerator.NamedPermutation table) throws Exception {
        String columnStatement = "CREATE SCHEMA TEMPLATE <TEST_TEMPLATE> " +
                "CREATE STRUCT " + table.getTypeDefinition("FOO") +
                " CREATE TABLE the_table(col0 int64, col1 foo, PRIMARY KEY(col0))";

        try (RelationalConnection conn = Relational.connect(URI.create("jdbc:embed:/__SYS"), Options.NONE)) {
            conn.setSchema("CATALOG");
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

    @Test
    void createSchemaTemplateWithNoTable() throws SQLException, RelationalException {
        String createColumnStatement = "CREATE SCHEMA TEMPLATE no_table " +
                "CREATE STRUCT not_a_table(a int64);";

        try (RelationalConnection conn = Relational.connect(URI.create("jdbc:embed:/__SYS"), Options.NONE)) {
            conn.setSchema("CATALOG");
            try (Statement statement = conn.createStatement()) {
                RelationalAssertions.assertThrowsSqlException(() -> statement.executeUpdate(createColumnStatement))
                        .hasErrorCode(ErrorCode.INVALID_SCHEMA_TEMPLATE);
            }
        }
    }

    @Test
    void cyclicDependencyTest() throws RelationalException, SQLException {
        String template = "CREATE SCHEMA TEMPLATE cyclic " +
                "CREATE STRUCT s1 (a s2) " +
                "CREATE STRUCT s2 (a s1) " +
                "CREATE TABLE t1 (id int64, a s1, b s2, PRIMARY KEY(id))";
        try (RelationalConnection conn = Relational.connect(URI.create("jdbc:embed:/__SYS?schema=CATALOG"), Options.NONE)) {
            try (Statement statement = conn.createStatement()) {
                RelationalAssertions.assertThrowsSqlException(() -> statement.executeUpdate(template))
                        .hasErrorCode(ErrorCode.INVALID_SCHEMA_TEMPLATE)
                        .hasMessageContaining("cyclic");
            }
        }
    }

    @Test
    void manyStructsThatDoNotDependOnEachOther() throws RelationalException, SQLException {
        StringBuilder template = new StringBuilder("CREATE SCHEMA TEMPLATE many_structs ");
        for (int i = 0; i < 100; i++) {
            template.append("CREATE STRUCT s").append(i).append("(a int64) ");
        }
        template.append("CREATE TABLE t1 (id int64,");
        for (int i = 0; i < 100; i++) {
            template.append("c").append(i).append(" s").append(i).append(",");
        }
        template.append(" PRIMARY KEY(id))");
        try (RelationalConnection conn = Relational.connect(URI.create("jdbc:embed:/__SYS?schema=CATALOG"), Options.NONE)) {
            try (RelationalStatement statement = conn.createStatement()) {
                statement.executeUpdate(template.toString());
                try (RelationalResultSet resultSet = statement.executeQuery("DESCRIBE SCHEMA TEMPLATE many_structs")) {
                    ResultSetAssert.assertThat(resultSet).hasNextRow();
                    try (RelationalResultSet types = resultSet.getArray("TYPES").getResultSet()) {
                        int count;
                        for (count = 0; types.next(); count++) {
                        }
                        Assertions.assertEquals(100, count);
                    }
                }
            }
        }
    }
}
