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

import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.ddl.DdlConnection;
import com.apple.foundationdb.relational.api.ddl.DdlStatement;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.recordlayer.EmbeddedRelationalExtension;
import com.apple.foundationdb.relational.utils.SchemaTemplateRule;
import com.apple.foundationdb.relational.utils.TableDefinition;
import com.apple.foundationdb.relational.utils.TypeDefinition;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.net.URI;
import java.util.Collections;
import java.util.List;

/**
 * End-to-end unit tests for the database ddl language built over RecordLayer.
 */
public class DdlDatabaseTest {
    @RegisterExtension
    public static final EmbeddedRelationalExtension relational = new EmbeddedRelationalExtension();

    @RegisterExtension
    public final SchemaTemplateRule baseTemplate = new SchemaTemplateRule( DdlDatabaseTest.class.getSimpleName() + "_TEMPLATE",
            Collections.singleton(new TableDefinition("FOO_TBL", List.of("string", "double"), List.of("col1"))),
            Collections.singleton(new TypeDefinition("FOO_NESTED_TYPE", List.of("string", "int64"))),
            () -> relational.getEngine().getDdlConnection());

    @Test
    @Disabled("Awaiting support for listDatabases")
    public void canCreateDatabase() throws Exception {
        try (DdlConnection conn = relational.getEngine().getDdlConnection()) {
            try (DdlStatement statement = conn.createStatement()) {
                //create a database
                statement.execute("CREATE DATABASE /test_db");

                //look to see if it's in the list
                Assertions.assertTrue(statement.execute("SHOW DATABASES WITH PREFIX /test_db"), "Did not return a result set!");
                try (RelationalResultSet rrs = statement.getNextResultSet()) {
                    Assertions.assertTrue(rrs.next(), "Did not find any databases!");
                    Assertions.assertEquals("/test_db", rrs.getString(1), "Incorrect database name!");
                    Assertions.assertFalse(rrs.next(), "Found too many databases!");
                }
            }
        }
    }

    @Test
    @Disabled("disabled until listDatabases is supported")
    public void dropDatabaseRemovesFromList() throws Exception {
        final String listCommand = "SHOW DATABASES WITH PREFIX /test_db";
        try (DdlConnection conn = relational.getEngine().getDdlConnection()) {
            try (DdlStatement statement = conn.createStatement()) {
                //create a database
                statement.execute("CREATE DATABASE /test_db");

                //look to see if it's in the list
                Assertions.assertTrue(statement.execute(listCommand), "Did not return a result set!");
                try (RelationalResultSet rrs = statement.getNextResultSet()) {
                    Assertions.assertTrue(rrs.next(), "Did not find any databases!");
                    Assertions.assertEquals("/test_db", rrs.getString(1), "Incorrect database name!");
                    Assertions.assertFalse(rrs.next(), "Found too many databases!");
                }

                //now drop the database
                Assertions.assertFalse(statement.execute("DROP DATABASE /test_db"), "Should not return a result set");

                //now it should be missing
                Assertions.assertTrue(statement.execute(listCommand), "Did not return a result set!");
                try (RelationalResultSet rrs = statement.getNextResultSet()) {
                    Assertions.assertFalse(rrs.next(), "Found too many databases!");
                }
            }
        }
    }

    @Test
    @Disabled("Catalog behavior w.r.t databses that have no schemas needs to be addressed(TODO)")
    public void cannotCreateSchemaFromDroppedDatabase() throws Exception {
        /*
         * a sort-of-dirty way of verifying that a database was created: if you can create
         * a schema inside the database, then it is created. Then drop the database; create schema should fail.
         *
         * This is a drop database test that doesn't require listing the database
         */
        try (DdlConnection conn = relational.getEngine().getDdlConnection()) {
            try (DdlStatement statement = conn.createStatement()) {
                //create a database
                statement.execute("CREATE DATABASE /test_db");

                conn.setDatabase(URI.create("/test_db"));
                //create a schema --this should just "work" in that it won't throw an error
                statement.execute("CREATE SCHEMA created_schema with template " + baseTemplate.getTemplateName());

                //now drop the database
                Assertions.assertFalse(statement.execute("DROP DATABASE /test_db"), "Should not return a result set");

                //now creating a new schema should throw a DATABASE_NOT_FOUND error
                RelationalException ve = Assertions.assertThrows(RelationalException.class,
                        () -> statement.execute("CREATE SCHEMA /test_db/should_fail with template " + baseTemplate.getTemplateName()));
                Assertions.assertEquals(ErrorCode.DATABASE_NOT_FOUND, ve.getErrorCode());
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
        try (DdlConnection conn = relational.getEngine().getDdlConnection()) {
            try (DdlStatement statement = conn.createStatement()) {
                try {
                    //create a database
                    statement.execute("CREATE DATABASE /test_db_two_schemas");
                    //TODO(bfines) the catalog doesn't currently detect a database unless it has a schema, which is probably not right
                    statement.execute("CREATE SCHEMA /test_db_two_schemas/schema3 with template " + baseTemplate.getTemplateName());

                    //creating the database a second time should fail
                    RelationalException ve = Assertions.assertThrows(RelationalException.class,
                            () -> statement.execute("CREATE DATABASE /test_db_two_schemas"));
                    Assertions.assertEquals(ErrorCode.DATABASE_ALREADY_EXISTS, ve.getErrorCode());
                } finally {
                    //try to drop the db for test cleanliness
                    statement.execute("DROP DATABASE /test_db_two_schemas");
                }
            }
        }
    }

}
