/*
 * DdlDatabaseTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2024 Apple Inc. and the FoundationDB project authors
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
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.sql.DriverManager;
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
            null, Collections.singleton(new TableDefinition("FOO_TBL", List.of("string", "double"), List.of("col1"))),
            Collections.singleton(new TypeDefinition("FOO_NESTED_TYPE", List.of("string", "bigint"))));

    @Test
    public void canCreateDatabase() throws Exception {
        try {
            try (final var conn = DriverManager.getConnection("jdbc:embed:/__SYS")) {
                conn.setSchema("CATALOG");
                try (final var statement = conn.createStatement()) {
                    //create a database
                    statement.executeUpdate("CREATE DATABASE /test/test_db");
                    statement.executeUpdate("CREATE SCHEMA /test/test_db/foo_schem with template \"" + baseTemplate.getTemplateName() + "\"");
                }
            }
            try (RelationalConnection conn = DriverManager.getConnection("jdbc:embed:/TEST/TEST_DB").unwrap(RelationalConnection.class)) {
                conn.setSchema("FOO_SCHEM");
                try (RelationalStatement statement = conn.createStatement()) {

                    //look to see if it's in the list
                    Set<String> databases = Set.of("/TEST/TEST_DB", "/__SYS");
                    try (RelationalResultSet rs = statement.executeQuery("SHOW DATABASES")) {
                        ResultSetAssert.assertThat(rs)
                                .meetsForAllRows(ResultSetAssert.perRowCondition(resultSet -> databases.contains(resultSet.getString(1)), "Should be a valid database"));
                    }
                }
            }
        } finally {
            try (final var conn = DriverManager.getConnection("jdbc:embed:/__SYS?schema=CATALOG")) {
                try (final var statement = conn.createStatement()) {
                    statement.execute("DROP DATABASE /test/test_db");
                }
            }
        }
    }

    @Test
    @Disabled("TODO")
    public void dropDatabaseRemovesFromList() throws Exception {
        final String listCommand = "SHOW DATABASES WITH PREFIX /test_db";
        try (RelationalConnection conn = DriverManager.getConnection("jdbc:embed:/__SYS").unwrap(RelationalConnection.class)) {
            conn.setSchema("CATALOG");
            try (RelationalStatement statement = conn.createStatement()) {
                //create a database
                statement.executeUpdate("CREATE DATABASE /test_db");

                //look to see if it's in the list
                try (RelationalResultSet rs = statement.executeQuery(listCommand)) {
                    ResultSetAssert.assertThat(rs).hasNextRow()
                            .hasColumn("database_id", "/test_db");
                }
                //now drop the database
                statement.executeUpdate("DROP DATABASE /test_db");

                //now it should be missing
                try (RelationalResultSet rs = statement.executeQuery(listCommand)) {
                    ResultSetAssert.assertThat(rs).isEmpty();
                }
            }
        }
    }

    @Test
    public void cannotCreateSchemaFromDroppedDatabase() throws Exception {
        /*
         * a sort-of-dirty way of verifying that a database was created: if you can create
         * a schema inside the database, then it is created. Then drop the database; create schema should fail.
         *
         * This is a drop database test that doesn't require listing the database
         */
        try (final var conn = DriverManager.getConnection("jdbc:embed:/__SYS")) {
            conn.setSchema("CATALOG");
            try (final var statement = conn.createStatement()) {
                //create a database
                statement.executeUpdate("CREATE DATABASE /test/test_db");

                //create a schema --this should just "work" in that it won't throw an error
                statement.executeUpdate("CREATE SCHEMA /test/test_db/created_schema with template \"" + baseTemplate.getTemplateName() + "\"");

                //now drop the database
                statement.executeUpdate("DROP DATABASE /test/test_db");

                //now creating a new schema should throw a DATABASE_NOT_FOUND error
                Assertions.assertThatThrownBy(() -> statement.executeUpdate("CREATE SCHEMA /test/test_db/should_fail with template " + baseTemplate.getTemplateName()))
                        .isInstanceOf(SQLException.class)
                        .extracting("SQLState")
                        .isEqualTo(ErrorCode.UNDEFINED_DATABASE.getErrorCode());
            }
        }
    }

    @Test
    public void cannotCreateDatabaseIfExists() throws Exception {
        try (RelationalConnection conn = DriverManager.getConnection("jdbc:embed:/__SYS").unwrap(RelationalConnection.class)) {
            conn.setSchema("CATALOG");
            // assure that there is not a database with the same name from before
            try (Statement statement = conn.createStatement()) {
                statement.executeUpdate("DROP DATABASE if exists /test/test_db");
            }
            try (Statement statement = conn.createStatement()) {
                //create a database
                statement.executeUpdate("CREATE DATABASE /test/test_db");
                RelationalAssertions.assertThrowsSqlException(() ->
                        statement.executeUpdate("CREATE DATABASE /test/test_db"))
                        .hasErrorCode(ErrorCode.DATABASE_ALREADY_EXISTS);
            } finally {
                try (Statement statement = conn.createStatement()) {
                    //try to drop the db for test cleanliness
                    statement.executeUpdate("DROP DATABASE /test/test_db");
                }
            }
        }
    }

    @Test
    public void cannotCreateSchemaWithoutDatabase() throws Exception {
        try (RelationalConnection conn = DriverManager.getConnection("jdbc:embed:/__SYS").unwrap(RelationalConnection.class)) {
            conn.setSchema("CATALOG");
            try (Statement statement = conn.createStatement()) {
                //create a database
                RelationalAssertions.assertThrowsSqlException(() ->
                        statement.executeUpdate("CREATE SCHEMA /database_that_does_not_exist/schema_that_cannot_be_created with template " + baseTemplate.getTemplateName()))
                        .hasErrorCode(ErrorCode.UNDEFINED_DATABASE);
            }
        }
    }
}
