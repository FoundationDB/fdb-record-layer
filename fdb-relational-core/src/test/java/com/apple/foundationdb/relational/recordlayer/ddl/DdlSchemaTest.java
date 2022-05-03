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
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.ddl.DdlConnection;
import com.apple.foundationdb.relational.api.ddl.DdlStatement;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
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
            "'" + DdlSchemaTest.class.getName() + "_TEMPLATE'",
            Collections.singleton(new TableDefinition("FOO_TBL", List.of("string", "double"), List.of("col0"))),
            Collections.singleton(new TypeDefinition("FOO_NESTED_TYPE", List.of("string", "int64"))));

    @RegisterExtension
    @Order(2)
    public final DatabaseRule db = new DatabaseRule(relational, URI.create("/" + DdlSchemaTest.class.getSimpleName()));

    @Test
    void cannotCreateSchemaWithNonexistentSchemaTemplate() throws Exception {
        try (DdlConnection conn = relational.getEngine().getDdlConnection()) {
            try (DdlStatement statement = conn.createStatement()) {
                conn.setDatabase(db.getDbUri());

                RelationalAssertions.assertThrows(() -> statement.execute("CREATE SCHEMA bad_schema WITH TEMPLATE missingTemplate"))
                        .hasErrorCode(ErrorCode.UNKNOWN_SCHEMA_TEMPLATE);
            }
        }
    }

    @Test
    void canCreateSchema() throws Exception {
        try (DdlConnection conn = relational.getEngine().getDdlConnection()) {
            try (DdlStatement statement = conn.createStatement()) {
                //set the database
                conn.setDatabase(db.getDbUri());

                //create a schema
                final String createStatement = "CREATE SCHEMA testSchema WITH TEMPLATE " + baseTemplate.getTemplateName();
                Assertions.assertFalse(statement.execute(createStatement), "Should not have gotten a result set!");

                //now describe the schema
                Assertions.assertTrue(statement.execute("DESCRIBE SCHEMA testSchema"));
                try (RelationalResultSet rrs = statement.getNextResultSet()) {
                    while (rrs.next()) {
                        Assertions.assertEquals(db.getDbUri().getPath(), rrs.getString("DATABASE_PATH"), "Incorrect database name!");
                        Assertions.assertEquals("testSchema", rrs.getString("SCHEMA_NAME"), "Incorrect schema name!");
                        Collection<?> tableInfo = rrs.getRepeated("TABLES");
                        Assertions.assertEquals(1, tableInfo.size(), "Incorrect number of tables!");
                        Object tbl = tableInfo.stream().findFirst().orElseThrow();
                        CatalogData.Table correctData = CatalogData.Table.newBuilder()
                                .setName("FOO_TBL")
                                .setPrimaryKey(Key.Expressions.concat(Key.Expressions.recordType(), Key.Expressions.field("col0")).toKeyExpression())
                                .build();
                        Assertions.assertEquals(correctData, tbl, "Incorrect table info!");
                    }
                }
            }
        }
    }

    @Test
    void cannotCreateSchemaTwice() throws Exception {
        try (DdlConnection conn = relational.getEngine().getDdlConnection()) {
            try (DdlStatement statement = conn.createStatement()) {
                //set the database
                conn.setDatabase(db.getDbUri());

                //create a schema
                final String createStatement = "CREATE SCHEMA testSchema WITH TEMPLATE " + baseTemplate.getTemplateName();
                Assertions.assertFalse(statement.execute(createStatement), "Should not have gotten a result set!");

                RelationalException ve = Assertions.assertThrows(RelationalException.class, () -> statement.execute(createStatement));
                Assertions.assertEquals(ErrorCode.SCHEMA_EXISTS, ve.getErrorCode());

            }
        }
    }

    @Test
    void dropSchema() throws Exception {
        try (DdlConnection conn = relational.getEngine().getDdlConnection()) {
            try (DdlStatement statement = conn.createStatement()) {
                //set the database
                conn.setDatabase(db.getDbUri());

                //create a schema
                final String createStatement = "CREATE SCHEMA testSchema WITH TEMPLATE " + baseTemplate.getTemplateName();
                Assertions.assertFalse(statement.execute(createStatement), "Should not have gotten a result set!");

                //make sure it's there
                //now describe the schema
                Assertions.assertTrue(statement.execute("DESCRIBE SCHEMA testSchema"));
                try (RelationalResultSet rrs = statement.getNextResultSet()) {
                    while (rrs.next()) {
                        Assertions.assertEquals(db.getDbUri().getPath(), rrs.getString("DATABASE_PATH"), "Incorrect database name!");
                        Assertions.assertEquals("testSchema", rrs.getString("SCHEMA_NAME"), "Incorrect schema name!");
                        Collection<?> tableInfo = rrs.getRepeated("TABLES");
                        Assertions.assertEquals(1, tableInfo.size(), "Incorrect number of tables!");
                        Object tbl = tableInfo.stream().findFirst().orElseThrow();
                        CatalogData.Table correctData = CatalogData.Table.newBuilder()
                                .setName("FOO_TBL")
                                .setPrimaryKey(Key.Expressions.concat(Key.Expressions.recordType(), Key.Expressions.field("col0")).toKeyExpression())
                                .build();
                        Assertions.assertEquals(correctData, tbl, "Incorrect table info!");
                    }
                }

                //drop the schema
                Assertions.assertFalse(statement.execute("DROP SCHEMA testSchema"), "Should not have gotten a result set");

                //now make sure that it can't be found again
                RelationalException ve = Assertions.assertThrows(RelationalException.class, () -> statement.execute("DESCRIBE SCHEMA testSchema"));
                Assertions.assertEquals(ErrorCode.SCHEMA_NOT_FOUND, ve.getErrorCode());
            }
        }
    }
}
