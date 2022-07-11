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
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.RelationalStatement;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.recordlayer.EmbeddedRelationalExtension;
import com.apple.foundationdb.relational.utils.ResultSetAssert;
import com.apple.foundationdb.relational.utils.SchemaTemplateRule;
import com.apple.foundationdb.relational.utils.TableDefinition;
import com.apple.foundationdb.relational.utils.TypeDefinition;
import com.apple.foundationdb.relational.utils.RelationalAssertions;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.net.URI;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * End-to-end unit tests for the database ddl language built over RecordLayer.
 */
public class DdlDatabaseTest {
    @RegisterExtension
    public static final EmbeddedRelationalExtension relational = new EmbeddedRelationalExtension();

    @RegisterExtension
    public final SchemaTemplateRule baseTemplate = new SchemaTemplateRule(relational, DdlDatabaseTest.class.getSimpleName() + "_TEMPLATE",
            Collections.singleton(new TableDefinition("FOO_TBL", List.of("string", "double"), List.of("col1"))),
            Collections.singleton(new TypeDefinition("FOO_NESTED_TYPE", List.of("string", "int64"))));

    @AfterEach
    void tearDown() throws Exception {
        try (RelationalConnection conn = Relational.connect(URI.create("jdbc:embed:/__SYS"), Options.NONE)) {
            conn.setSchema("catalog");
            try (Statement statement = conn.createStatement()) {
                statement.execute("DROP DATABASE '/test_db'");
            }
        }
    }

    @Test
    public void canCreateDatabase() throws Exception {
        try (RelationalConnection conn = Relational.connect(URI.create("jdbc:embed:/__SYS"), Options.NONE)) {
            conn.setSchema("catalog");
            try (Statement statement = conn.createStatement()) {
                //create a database
                statement.executeUpdate("CREATE DATABASE '/test_db'");
                statement.executeUpdate("CREATE SCHEMA '/test_db/foo_schem' with template " + baseTemplate.getTemplateName());
            }
        }
        try (RelationalConnection conn = Relational.connect(URI.create("jdbc:embed:/test_db"), Options.NONE)) {
            conn.setSchema("foo_schem");
            try (RelationalStatement statement = conn.createStatement()) {

                //look to see if it's in the list
                Set<String> databases = Set.of("/test_db", "/__SYS");
                try (RelationalResultSet rs = statement.executeQuery("SHOW DATABASES")) {
                    ResultSetAssert.assertThat(rs)
                            .meetsForAllRows(ResultSetAssert.perRowCondition(resultSet -> databases.contains(resultSet.getString(1)), "Should be a valid database"));
                }
            }
        }
    }

    @Test
    @Disabled("TODO")
    public void dropDatabaseRemovesFromList() throws Exception {
        final String listCommand = "SHOW DATABASES WITH PREFIX '/test_db'";
        try (RelationalConnection conn = Relational.connect(URI.create("jdbc:embed:/__SYS"), Options.NONE)) {
            conn.setSchema("catalog");
            try (RelationalStatement statement = conn.createStatement()) {
                //create a database
                statement.executeUpdate("CREATE DATABASE '/test_db'");

                //look to see if it's in the list
                try (RelationalResultSet rs = statement.executeQuery(listCommand)) {
                    ResultSetAssert.assertThat(rs).hasNextRow()
                            .hasColumn("database_id", "/test_db");
                }
                //now drop the database
                statement.executeUpdate("DROP DATABASE '/test_db/test_db'");

                //now it should be missing
                try (RelationalResultSet rs = statement.executeQuery(listCommand)) {
                    ResultSetAssert.assertThat(rs).isEmpty();
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
        try (RelationalConnection conn = Relational.connect(URI.create("jdbc:embed:/__SYS"), Options.NONE)) {
            conn.setSchema("catalog");
            try (Statement statement = conn.createStatement()) {
                //create a database
                statement.executeUpdate("CREATE DATABASE '/test_db'");

                //create a schema --this should just "work" in that it won't throw an error
                statement.executeUpdate("CREATE SCHEMA '/test_db/created_schema' with template " + baseTemplate.getTemplateName());

                //now drop the database
                statement.executeUpdate("DROP DATABASE '/test_db/created_schema'");

                //now creating a new schema should throw a DATABASE_NOT_FOUND error
                Assertions.assertThatThrownBy(() -> statement.executeUpdate("CREATE SCHEMA /test_db/should_fail with template " + baseTemplate.getTemplateName()))
                        .isInstanceOf(SQLException.class)
                        .extracting("SQLState")
                        .isEqualTo(ErrorCode.UNDEFINED_DATABASE.getErrorCode());
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
        try (RelationalConnection conn = Relational.connect(URI.create("jdbc:embed:/__SYS"), Options.NONE)) {
            conn.setSchema("catalog");
            try (Statement statement = conn.createStatement()) {
                //create a database
                statement.executeUpdate("CREATE DATABASE '/test_db_two_schemas'");
                //TODO(bfines) the catalog doesn't currently detect a database unless it has a schema, which is probably not right
                statement.executeUpdate("CREATE SCHEMA '/test_db_two_schemas/schema3' with template " + baseTemplate.getTemplateName());

                //creating the database a second time should fail
                Assertions.assertThatThrownBy(() -> statement.executeUpdate("CREATE DATABASE '/test_db_two_schemas'"))
                        .isInstanceOf(SQLException.class)
                        .extracting("SQLState")
                        .isEqualTo(ErrorCode.DATABASE_ALREADY_EXISTS.getErrorCode());
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
