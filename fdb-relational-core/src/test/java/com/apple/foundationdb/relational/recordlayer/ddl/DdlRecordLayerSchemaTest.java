/*
 * DdlRecordLayerSchemaTest.java
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
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.recordlayer.EmbeddedRelationalExtension;
import com.apple.foundationdb.relational.utils.DatabaseRule;
import com.apple.foundationdb.relational.utils.ResultSetAssert;
import com.apple.foundationdb.relational.utils.SchemaTemplateRule;
import com.apple.foundationdb.relational.utils.TableDefinition;
import com.apple.foundationdb.relational.utils.TypeDefinition;
import com.apple.foundationdb.relational.utils.RelationalAssertions;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.net.URI;
import java.sql.Array;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

public class DdlRecordLayerSchemaTest {
    @RegisterExtension
    @Order(0)
    public static final EmbeddedRelationalExtension relational = new EmbeddedRelationalExtension();

    @RegisterExtension
    @Order(1)
    public final SchemaTemplateRule baseTemplate = new SchemaTemplateRule(relational,
            DdlRecordLayerSchemaTest.class.getSimpleName().toUpperCase(Locale.ROOT) + "_TEMPLATE",
            null, Collections.singleton(new TableDefinition("FOO_TBL", List.of("string", "double"), List.of("col0"))),
            Collections.singleton(new TypeDefinition("FOO_NESTED_TYPE", List.of("string", "bigint"))));

    @RegisterExtension
    @Order(2)
    public final DatabaseRule db = new DatabaseRule(relational, URI.create("/TEST/" + DdlRecordLayerSchemaTest.class.getSimpleName().toUpperCase(Locale.ROOT)));

    @Test
    void canCreateSchema() throws Exception {
        try (final var conn = DriverManager.getConnection("jdbc:embed:/__SYS")) {
            conn.setSchema("CATALOG");
            try (final var statement = conn.createStatement()) {
                //create a schema
                final String createStatement = "CREATE SCHEMA " + db.getDbUri() + "/TEST_SCHEMA WITH TEMPLATE " + baseTemplate.getTemplateName();
                statement.executeUpdate(createStatement);
                //now describe the schema
                try (final var resultSet = statement.executeQuery("DESCRIBE SCHEMA " + db.getDbUri() + "/TEST_SCHEMA")) {
                    while (resultSet.next()) {
                        Assertions.assertEquals(db.getDbUri().getPath(), resultSet.getString("DATABASE_PATH"), "Incorrect database name!");
                        Assertions.assertEquals("TEST_SCHEMA", resultSet.getString("SCHEMA_NAME"), "Incorrect schema name!");
                        Array tableInfoArr = resultSet.getArray("TABLES");
                        try (ResultSet rs = tableInfoArr.getResultSet()) {
                            org.assertj.core.api.Assertions.assertThat(rs).isInstanceOf(RelationalResultSet.class);
                            ResultSetAssert.assertThat((RelationalResultSet) rs).hasNextRow()
                                    .isRowExactly(1, "FOO_TBL")
                                    .hasNoNextRow();
                        }
                    }
                }
            }
        }
    }

    @Test
    void canCreateSchemaTemplateWhenConnectedToNonCatalogSchema() throws Exception {
        try (final var conn = DriverManager.getConnection("jdbc:embed:/__SYS")) {
            conn.setSchema("CATALOG");
            try (Statement statement = conn.createStatement()) {
                //create a schema
                final String createStatement = "CREATE SCHEMA " + db.getDbUri() + "/TEST_SCHEMA WITH TEMPLATE " + baseTemplate.getTemplateName();
                statement.executeUpdate(createStatement);

            }
        }
        //now create a new schema in the same db but using a different connection
        try (final var conn = DriverManager.getConnection("jdbc:embed:" + db.getDbUri())) {
            conn.setSchema("TEST_SCHEMA");
            try (Statement statement = conn.createStatement()) {
                //create a schema
                final String createStatement = "CREATE SCHEMA TEMPLATE FOO CREATE TABLE T(A string, B string, PRIMARY KEY (A))";
                statement.executeUpdate(createStatement);
            }
        }
    }

    @Test
    void cannotCreateSchemaTwice() throws Exception {
        try (final var conn = DriverManager.getConnection("jdbc:embed:/__SYS")) {
            conn.setSchema("CATALOG");
            try (Statement statement = conn.createStatement()) {

                //create a schema
                final String createStatement = "CREATE SCHEMA " + db.getDbUri() + "/TEST_SCHEMA WITH TEMPLATE " + baseTemplate.getTemplateName();
                statement.executeUpdate(createStatement);
                RelationalAssertions.assertThrowsSqlException(() -> statement.executeUpdate(createStatement))
                        .hasErrorCode(ErrorCode.SCHEMA_ALREADY_EXISTS);

            }
        }
    }

    @Test
    void dropSchema() throws Exception {
        try (final var conn = DriverManager.getConnection("jdbc:embed:/__SYS")) {
            conn.setSchema("CATALOG");
            try (final var statement = conn.createStatement()) {

                //create a schema
                final String createStatement = "CREATE SCHEMA \"" + db.getDbUri() + "/TEST_SCHEMA\" WITH TEMPLATE " + baseTemplate.getTemplateName();
                statement.executeUpdate(createStatement);

                //make sure it's there
                //now describe the schema
                try (final var resultSet = statement.executeQuery("DESCRIBE SCHEMA " + db.getDbUri() + "/TEST_SCHEMA")) {
                    while (resultSet.next()) {
                        Assertions.assertEquals(db.getDbUri().getPath(), resultSet.getString("DATABASE_PATH"), "Incorrect database name!");
                        Assertions.assertEquals("TEST_SCHEMA", resultSet.getString("SCHEMA_NAME"), "Incorrect schema name!");

                        Array arr = resultSet.getArray("TABLES");
                        try (ResultSet tableRs = arr.getResultSet()) {
                            org.assertj.core.api.Assertions.assertThat(tableRs).isInstanceOf(RelationalResultSet.class);
                            ResultSetAssert.assertThat((RelationalResultSet) tableRs).hasNextRow()
                                    .isRowExactly(1, "FOO_TBL")
                                    .hasNoNextRow();
                        }
                    }
                }

                //drop the schema
                statement.executeUpdate("DROP SCHEMA \"" + db.getDbUri() + "/TEST_SCHEMA\"");

                //now make sure that it can't be found again
                RelationalAssertions.assertThrowsSqlException(() -> statement.executeQuery("DESCRIBE SCHEMA " + db.getDbUri() + "/TEST_SCHEMA"))
                        .hasErrorCode(ErrorCode.UNDEFINED_SCHEMA);
            }
        }
    }
}
