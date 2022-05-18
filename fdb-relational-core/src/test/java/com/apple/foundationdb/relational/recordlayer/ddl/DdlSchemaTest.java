/*
 * DdlSchemaTest.java
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

import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.Relational;
import com.apple.foundationdb.relational.api.RelationalConnection;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.generated.CatalogData;
import com.apple.foundationdb.relational.recordlayer.EmbeddedRelationalExtension;
import com.apple.foundationdb.relational.utils.DatabaseRule;
import com.apple.foundationdb.relational.utils.SchemaTemplateRule;
import com.apple.foundationdb.relational.utils.TableDefinition;
import com.apple.foundationdb.relational.utils.TypeDefinition;
import com.apple.foundationdb.relational.utils.RelationalAssertions;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.net.URI;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class DdlSchemaTest {
    @RegisterExtension
    @Order(0)
    public static final EmbeddedRelationalExtension relational = new EmbeddedRelationalExtension();

    @RegisterExtension
    @Order(1)
    public final SchemaTemplateRule baseTemplate = new SchemaTemplateRule(relational,
            DdlSchemaTest.class.getSimpleName() + "_TEMPLATE",
            Collections.singleton(new TableDefinition("FOO_TBL", List.of("string", "double"), List.of("col0"))),
            Collections.singleton(new TypeDefinition("FOO_NESTED_TYPE", List.of("string", "int64"))));

    @RegisterExtension
    @Order(2)
    public final DatabaseRule db = new DatabaseRule(relational, URI.create("/" + DdlSchemaTest.class.getSimpleName()));

    @Test
    void cannotCreateSchemaWithNonexistentSchemaTemplate() throws Exception {
        try (RelationalConnection conn = Relational.connect(URI.create("jdbc:embed:/__SYS"), Options.create())) {
            conn.setSchema("catalog");
            try (Statement statement = conn.createStatement()) {
                RelationalAssertions.assertThrowsSqlException(() -> statement.executeUpdate("CREATE SCHEMA bad_schema WITH TEMPLATE missingTemplate"))
                        .hasErrorCode(ErrorCode.UNKNOWN_SCHEMA_TEMPLATE);
            }
        }
    }

    @Test
    void canCreateSchema() throws Exception {
        try (RelationalConnection conn = Relational.connect(URI.create("jdbc:embed:/__SYS"), Options.create())) {
            conn.setSchema("catalog");
            try (Statement statement = conn.createStatement()) {
                //create a schema
                final String createStatement = "CREATE SCHEMA '" + db.getDbUri() + "/testSchema' WITH TEMPLATE " + baseTemplate.getTemplateName();
                statement.executeUpdate(createStatement);
                //now describe the schema
                try (ResultSet resultSet = statement.executeQuery("DESCRIBE SCHEMA '" + db.getDbUri() + "/testSchema'")) {
                    while (resultSet.next()) {
                        Assertions.assertEquals(db.getDbUri().getPath(), resultSet.getString("DATABASE_PATH"), "Incorrect database name!");
                        Assertions.assertEquals("testSchema", resultSet.getString("SCHEMA_NAME"), "Incorrect schema name!");
                        Assertions.assertTrue(resultSet instanceof RelationalResultSet);
                        RelationalResultSet relationalResultSet = (RelationalResultSet) resultSet;
                        Collection<?> tableInfo = relationalResultSet.getRepeated("TABLES");
                        Assertions.assertEquals(1, tableInfo.size(), "Incorrect number of tables!");
                        Object tbl = tableInfo.stream().findFirst().orElseThrow();
                        CatalogData.Table correctData = CatalogData.Table.newBuilder()
                                .setName("FOO_TBL")
                                .setPrimaryKey(Key.Expressions.concat(Key.Expressions.recordType(), Key.Expressions.field("col0")).toKeyExpression().toByteString())
                                .build();
                        Assertions.assertEquals(correctData, tbl, "Incorrect table info!");
                    }
                }
            }
        }
    }

    @Test
    void cannotCreateSchemaTwice() throws Exception {
        try (RelationalConnection conn = Relational.connect(URI.create("jdbc:embed:/__SYS"), Options.create())) {
            conn.setSchema("catalog");
            try (Statement statement = conn.createStatement()) {

                //create a schema
                final String createStatement = "CREATE SCHEMA '" + db.getDbUri() + "/testSchema' WITH TEMPLATE " + baseTemplate.getTemplateName();
                statement.executeUpdate(createStatement);
                RelationalAssertions.assertThrowsSqlException(() -> statement.executeUpdate(createStatement))
                        .hasErrorCode(ErrorCode.SCHEMA_EXISTS);

            }
        }
    }

    @Test
    void dropSchema() throws Exception {
        try (RelationalConnection conn = Relational.connect(URI.create("jdbc:embed:/__SYS"), Options.create())) {
            conn.setSchema("catalog");
            try (Statement statement = conn.createStatement()) {

                //create a schema
                final String createStatement = "CREATE SCHEMA \"" + db.getDbUri() + "/testSchema\" WITH TEMPLATE " + baseTemplate.getTemplateName();
                statement.executeUpdate(createStatement);

                //make sure it's there
                //now describe the schema
                try (ResultSet resultSet = statement.executeQuery("DESCRIBE SCHEMA '" + db.getDbUri() + "/testSchema'")) {
                    while (resultSet.next()) {
                        Assertions.assertEquals(db.getDbUri().getPath(), resultSet.getString("DATABASE_PATH"), "Incorrect database name!");
                        Assertions.assertEquals("testSchema", resultSet.getString("SCHEMA_NAME"), "Incorrect schema name!");
                        Assertions.assertTrue(resultSet instanceof RelationalResultSet);
                        RelationalResultSet relationalResultSet = (RelationalResultSet) resultSet;
                        Collection<?> tableInfo = relationalResultSet.getRepeated("TABLES");
                        Assertions.assertEquals(1, tableInfo.size(), "Incorrect number of tables!");
                        Object tbl = tableInfo.stream().findFirst().orElseThrow();
                        CatalogData.Table correctData = CatalogData.Table.newBuilder()
                                .setName("FOO_TBL")
                                .setPrimaryKey(Key.Expressions.concat(Key.Expressions.recordType(), Key.Expressions.field("col0")).toKeyExpression().toByteString())
                                .build();
                        Assertions.assertEquals(correctData, tbl, "Incorrect table info!");
                    }
                }

                //drop the schema
                statement.executeUpdate("DROP SCHEMA \"" + db.getDbUri() + "/testSchema\"");

                //now make sure that it can't be found again
                RelationalAssertions.assertThrowsSqlException(() -> statement.executeQuery("DESCRIBE SCHEMA '" + db.getDbUri() + "/testSchema'"))
                        .hasErrorCode(ErrorCode.SCHEMA_NOT_FOUND);
            }
        }
    }
}
