/*
 * DdlDatabaseTest.java
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
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.recordlayer.EmbeddedRelationalExtension;
import com.apple.foundationdb.relational.utils.SchemaTemplateRule;
import com.apple.foundationdb.relational.utils.TableDefinition;
import com.apple.foundationdb.relational.utils.TypeDefinition;
import com.apple.foundationdb.relational.utils.RelationalAssertions;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.net.URI;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Collections;
import java.util.List;

/**
 * End-to-end unit tests for the database ddl language built over RecordLayer.
 */
public class DdlDatabaseTest {
    @RegisterExtension
    public static final EmbeddedRelationalExtension relational = new EmbeddedRelationalExtension();

    @RegisterExtension
    public final SchemaTemplateRule baseTemplate = new SchemaTemplateRule( relational, DdlDatabaseTest.class.getSimpleName() + "_TEMPLATE",
            Collections.singleton(new TableDefinition("FOO_TBL", List.of("string", "double"), List.of("col1"))),
            Collections.singleton(new TypeDefinition("FOO_NESTED_TYPE", List.of("string", "int64"))));

    @Test
    @Disabled("Awaiting support for listDatabases")
    public void canCreateDatabase() throws Exception {
        try (RelationalConnection conn = Relational.connect(URI.create("jdbc:embed:/__SYS"), Options.create())) {
            conn.setSchema("catalog");
            try (Statement statement = conn.createStatement()) {
                //create a database
                statement.executeUpdate("CREATE DATABASE '/test_db'");

                //look to see if it's in the list
                try (ResultSet rs = statement.executeQuery("SHOW DATABASES WITH PREFIX /test_db")) {
                    Assertions.assertTrue(rs.next(), "Did not find any databases!");
                    Assertions.assertEquals("/test_db", rs.getString(1), "Incorrect database name!");
                    Assertions.assertFalse(rs.next(), "Found too many databases!");
                }
            }
        }
    }

    @Test
    @Disabled("disabled until listDatabases is supported")
    public void dropDatabaseRemovesFromList() throws Exception {
        final String listCommand = "SHOW DATABASES WITH PREFIX '/test_db'";
        try (RelationalConnection conn = Relational.connect(URI.create("jdbc:embed:/__SYS"), Options.create())) {
            conn.setSchema("catalog");
            try (Statement statement = conn.createStatement()) {
                //create a database
                statement.executeUpdate("CREATE DATABASE '/test_db'");

                //look to see if it's in the list
                try (ResultSet rs = statement.executeQuery(listCommand)) {
                    Assertions.assertTrue(rs.next(), "Did not find any databases!");
                    Assertions.assertEquals("/test_db", rs.getString(1), "Incorrect database name!");
                    Assertions.assertFalse(rs.next(), "Found too many databases!");
                }

                //now drop the database
                statement.executeUpdate("DROP DATABASE '/test_db/test_db'");

                //now it should be missing
                try (ResultSet rs = statement.executeQuery(listCommand)) {
                    Assertions.assertFalse(rs.next(), "Found too many databases!");
                }
            }
        }
    }

    @Test
    @Disabled("Catalog behavior w.r.t databases that have no schemas needs to be addressed(TODO)")
    public void cannotCreateSchemaFromDroppedDatabase() throws Exception {
        /*
         * a sort-of-dirty way of verifying that a database was created: if you can create
         * a schema inside the database, then it is created. Then drop the database; create schema should fail.
         *
         * This is a drop database test that doesn't require listing the database
         */
        try (RelationalConnection conn = Relational.connect(URI.create("jdbc:embed:/__SYS"), Options.create())) {
            conn.setSchema("catalog");
            try (Statement statement = conn.createStatement()) {
                //create a database
                statement.executeUpdate("CREATE DATABASE '/test_db'");

                //create a schema --this should just "work" in that it won't throw an error
                statement.executeUpdate("CREATE SCHEMA '/test_db/created_schema' with template " + baseTemplate.getTemplateName());

                //now drop the database
                statement.executeUpdate("DROP DATABASE '/test_db/created_schema'");

                //now creating a new schema should throw a DATABASE_NOT_FOUND error
                RelationalAssertions.assertThrowsSqlException(() -> statement.executeUpdate("CREATE SCHEMA /test_db/should_fail with template " + baseTemplate.getTemplateName()))
                        .hasErrorCode(ErrorCode.DATABASE_NOT_FOUND);
            }
        }
    }

    @Test
    public void cannotCreateSameDatabaseTwice() throws Exception {
        /*
         * a sort-of-dirty way of verifying that a database was created: if you can create
         * a schema inside the database, then it is created.  Then try to create it again, it should fail
         *
         */
        try (RelationalConnection conn = Relational.connect(URI.create("jdbc:embed:/__SYS"), Options.create())) {
            conn.setSchema("catalog");
            try (Statement statement = conn.createStatement()) {
                //create a database
                statement.executeUpdate("CREATE DATABASE '/test_db_two_schemas'");
                //TODO(bfines) the catalog doesn't currently detect a database unless it has a schema, which is probably not right
                statement.executeUpdate("CREATE SCHEMA '/test_db_two_schemas/schema3' with template " + baseTemplate.getTemplateName());

                //creating the database a second time should fail
                RelationalAssertions.assertThrowsSqlException(() -> statement.executeUpdate("CREATE DATABASE '/test_db_two_schemas'"))
                        .hasErrorCode(ErrorCode.DATABASE_ALREADY_EXISTS);
            } finally {
                try (Statement statement = conn.createStatement()) {
                    //try to drop the db for test cleanliness
                    statement.executeUpdate("DROP DATABASE '/test_db_two_schemas'");
                }
            }
        }
    }

}
