/*
 * DdlRecordLayerSchemaTemplateTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2025 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.relational.api.EmbeddedRelationalArray;
import com.apple.foundationdb.relational.api.RelationalConnection;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.RelationalStatement;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.recordlayer.EmbeddedRelationalExtension;
import com.apple.foundationdb.relational.util.Assert;
import com.apple.foundationdb.relational.utils.DdlPermutationGenerator;
import com.apple.foundationdb.relational.utils.ResultSetAssert;
import com.apple.foundationdb.relational.utils.RelationalAssertions;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.function.ThrowingConsumer;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.sql.Array;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Stream;

/**
 * End-to-end unit tests for Schema Template language in the RecordLayer.
 * <br/>
 * <b>Note:</b> this test suite has side effects, FDB should be manually cleaned up after running this test.
 *
 */
public class DdlRecordLayerSchemaTemplateTest {
    @RegisterExtension
    public static final EmbeddedRelationalExtension relational = new EmbeddedRelationalExtension();

    public static Stream<Arguments> columnTypePermutations() {
        return DdlPermutationGenerator.generateTables("SCHEMA_TEMPLATE_TEST", 2)
                .map(Arguments::of);
    }

    private void run(ThrowingConsumer<? super RelationalStatement> operation) throws RelationalException, SQLException {
        try (RelationalConnection conn = DriverManager.getConnection("jdbc:embed:/__SYS").unwrap(RelationalConnection.class)) {
            conn.setSchema("CATALOG");
            try (RelationalStatement statement = conn.createStatement()) {
                operation.accept(statement);
            } catch (RelationalException | SQLException | RuntimeException err) {
                throw err;
            } catch (Throwable throwable) {
                Assertions.fail("unexpected error type", throwable);
            }
        }
    }

    @Test
    void canDropSchemaTemplates() throws Exception {
        String createColumnStatement = "CREATE SCHEMA TEMPLATE drop_template " +
                "CREATE TYPE AS STRUCT FOO_TYPE (a bigint)" +
                " CREATE TABLE FOO_TBL (b double, PRIMARY KEY(b))";

        run(statement -> {
            statement.executeUpdate(createColumnStatement);

            //verify that it's there
            try (final var rs = statement.executeQuery("DESCRIBE SCHEMA TEMPLATE drop_template")) {
                Assertions.assertTrue(rs.next(), "Didn't find created template!");
                Assertions.assertEquals("DROP_TEMPLATE", rs.getString(1));
                Assertions.assertFalse(rs.next(), "too many schema templates!");
            }
            //now drop it
            statement.executeUpdate("DROP SCHEMA TEMPLATE drop_template");

            //verify it's not there, and that means that there is an error trying to describe it
            SQLException ve = Assertions.assertThrows(SQLException.class, () -> statement.executeQuery("DESCRIBE SCHEMA TEMPLATE drop_template"));
            Assertions.assertEquals(ErrorCode.UNKNOWN_SCHEMA_TEMPLATE.getErrorCode(), ve.getSQLState(), "Incorrect error code");
        });
    }

    @ParameterizedTest
    @MethodSource("columnTypePermutations")
    void describeSchemaTemplate(DdlPermutationGenerator.NamedPermutation table) throws Exception {
        String createColumnStatement = "CREATE SCHEMA TEMPLATE " + table.getName() + "  " +
                "CREATE TYPE AS STRUCT " + table.getTypeDefinition("TYP") +
                " CREATE TABLE " + table.getTableDefinition("TBL");

        run(statement -> {
            statement.executeUpdate(createColumnStatement);

            try (RelationalResultSet rs = statement.executeQuery("DESCRIBE SCHEMA TEMPLATE " + table.getName())) {
                final var expectedTables = List.of(table.getPermutation("TBL"));
                Array expectedTablesArr = EmbeddedRelationalArray.newBuilder().addAll(expectedTables.toArray()).build();

                ResultSetAssert.assertThat(rs)
                        .hasNextRow()
                        .isRowExactly(table.getName(), expectedTablesArr)
                        .hasNoNextRow();
            }
        });
    }

    @ParameterizedTest
    @MethodSource("columnTypePermutations")
    void listSchemaTemplatesWorks(DdlPermutationGenerator.NamedPermutation table) throws Exception {
        String columnStatement = "CREATE SCHEMA TEMPLATE <TEST_TEMPLATE> " +
                "CREATE TYPE AS STRUCT " + table.getTypeDefinition("FOO") +
                " CREATE TABLE the_table(col0 bigint, col1 foo, PRIMARY KEY(col0))";

        run(statement -> {
            //do a scan of template names first, to see if there are any in there. This is mostly a protection
            // against test contamination
            Set<String> oldTemplateNames = new HashSet<>();
            try (final var rs = statement.executeQuery("SHOW SCHEMA TEMPLATES")) {
                while (rs.next()) {
                    oldTemplateNames.add(rs.getString(1));
                }
            }
            // Create a unique String, one that doesn't clash w/ what is there already.
            // Use the Thread ThreadLocalRandom rather than create our own -- expensive.
            String uniqueName = ThreadLocalRandom.current().ints('A', 'Z' + 1)
                    .limit(16)
                    .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
                    .append("NEW_TEMPLATE")
                    .toString();
            String uniqueStatement = columnStatement.replace("<TEST_TEMPLATE>", uniqueName);
            statement.executeUpdate(uniqueStatement);
            Set<String> templateNames = new HashSet<>();
            try (final var rs = statement.executeQuery("SHOW SCHEMA TEMPLATES")) {
                while (rs.next()) {
                    templateNames.add(rs.getString(1));
                }
            }

            oldTemplateNames.add(uniqueName);
            Assertions.assertEquals(oldTemplateNames, templateNames, "Incorrect returned Schema template list!");
        });
    }

    @Test
    void createSchemaTemplateWithNoTable() throws SQLException, RelationalException {
        String createColumnStatement = "CREATE SCHEMA TEMPLATE no_table " +
                "CREATE TYPE AS STRUCT not_a_table(a bigint)";

        run(statement -> RelationalAssertions.assertThrowsSqlException(() -> statement.executeUpdate(createColumnStatement))
                .hasErrorCode(ErrorCode.INVALID_SCHEMA_TEMPLATE));
    }

    @Test
    void cyclicDependencyTest() throws RelationalException, SQLException {
        String template = "CREATE SCHEMA TEMPLATE cyclic " +
                "CREATE TYPE AS STRUCT s1 (a s2) " +
                "CREATE TYPE AS STRUCT s2 (a s1) " +
                "CREATE TABLE t1 (id bigint, a s1, b s2, PRIMARY KEY(id))";

        run(statement -> RelationalAssertions.assertThrowsSqlException(() -> statement.executeUpdate(template))
                .hasErrorCode(ErrorCode.INVALID_SCHEMA_TEMPLATE)
                .hasMessageContaining("cyclic"));
    }

    @Test
    void manyStructsThatDoNotDependOnEachOther() throws RelationalException, SQLException {
        StringBuilder template = new StringBuilder("CREATE SCHEMA TEMPLATE many_structs ");
        for (int i = 0; i < 100; i++) {
            template.append("CREATE TYPE AS STRUCT s").append(i).append("(a bigint) ");
        }
        template.append("CREATE TABLE t1 (id bigint,");
        for (int i = 0; i < 100; i++) {
            template.append("c").append(i).append(" s").append(i).append(",");
        }
        template.append(" PRIMARY KEY(id))");

        run(statement -> {
            statement.executeUpdate(template.toString());
            try (RelationalResultSet resultSet = statement.executeQuery("DESCRIBE SCHEMA TEMPLATE many_structs")) {
                ResultSetAssert.assertThat(resultSet).hasNextRow();
                final var type = resultSet.getArray("TABLES").getResultSet();
                Assert.that(type.next());
                Assert.that(!type.next());
            }
        });
    }

    @Test
    void missingTypeTest() throws RelationalException, SQLException {
        String template = "CREATE SCHEMA TEMPLATE missing_type " +
                "CREATE TABLE t1 (id bigint, val unknown_type, PRIMARY KEY(id))";

        run(statement -> RelationalAssertions.assertThrowsSqlException(() -> statement.executeUpdate(template))
                .hasErrorCode(ErrorCode.UNKNOWN_TYPE) // todo: this seems like it should be INVALID_SCHEMA_TEMPLATE
                .hasMessageContaining("could not find type 'UNKNOWN_TYPE'"));
    }

    @Test
    void basicEnumTest() throws RelationalException, SQLException {
        String template = "CREATE SCHEMA TEMPLATE basic_enum_template " +
                "CREATE TYPE AS ENUM basic_enum ('FOO', 'BAR', 'BAZ') " +
                "CREATE TABLE t1 (id bigint, val basic_enum, PRIMARY KEY(id))";

        run(statement -> {
            statement.executeUpdate(template);

            //verify that it's there
            try (ResultSet rs = statement.executeQuery("DESCRIBE SCHEMA TEMPLATE basic_enum_template")) {
                Assertions.assertTrue(rs.next(), "Didn't find created template!");
            }
        });
    }

    @Test
    void twoTypesSameNameTest() throws RelationalException, SQLException {
        String template = "CREATE SCHEMA TEMPLATE same_name " +
                "CREATE TABLE t1 (id bigint, foo string, PRIMARY KEY(id)) " +
                "CREATE TABLE t1 (id bigint, bar string, PRIMARY KEY(id))";

        run(statement -> RelationalAssertions.assertThrowsSqlException(() -> statement.executeUpdate(template))
                .hasErrorCode(ErrorCode.INVALID_SCHEMA_TEMPLATE)
                .hasMessageContaining("table with name 'T1' already exists"));
    }

    @Test
    void twoTypesSameNameMixedCase() throws RelationalException, SQLException {
        String template = "CREATE SCHEMA TEMPLATE same_name_mixed_case " +
                "CREATE TABLE aTypeName (id bigint, foo string, PRIMARY KEY(id)) " +
                "CREATE TABLE AtYPEnAME (id bigint, bar string, PRIMARY KEY(id))";

        run(statement -> RelationalAssertions.assertThrowsSqlException(() -> statement.executeUpdate(template))
                .hasErrorCode(ErrorCode.INVALID_SCHEMA_TEMPLATE)
                .hasMessageContaining("table with name 'ATYPENAME' already exists"));
    }

    @Test
    void typeAndEnumSameNameTest() throws RelationalException, SQLException {
        String template = "CREATE SCHEMA TEMPLATE same_name " +
                "CREATE TABLE foo (id bigint, foo string, PRIMARY KEY(id)) " +
                "CREATE TYPE AS ENUM foo ('A', 'B', 'C')";

        run(statement -> RelationalAssertions.assertThrowsSqlException(() -> statement.executeUpdate(template))
                .hasErrorCode(ErrorCode.INVALID_SCHEMA_TEMPLATE)
                .hasMessageContaining("type with name 'FOO' already exists"));
    }

    @Test
    void notNullNonArrayTypeNotAllowedTest() throws RelationalException, SQLException {
        String template = "CREATE SCHEMA TEMPLATE not_null_non_array_column_type " +
                "CREATE TABLE foo (id bigint, foo string not null, PRIMARY KEY(id))";

        run(statement -> RelationalAssertions.assertThrowsSqlException(() -> statement.executeUpdate(template))
                .hasErrorCode(ErrorCode.UNSUPPORTED_OPERATION)
                .hasMessageContaining("NOT NULL is only allowed for ARRAY column type"));
    }
}
