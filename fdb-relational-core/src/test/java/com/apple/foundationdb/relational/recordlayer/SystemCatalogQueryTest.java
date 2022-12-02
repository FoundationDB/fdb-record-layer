/*
 * SystemCatalogQueryTest.java
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

package com.apple.foundationdb.relational.recordlayer;

import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.Relational;
import com.apple.foundationdb.relational.api.RelationalConnection;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.RelationalStatement;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.utils.ResultSetAssert;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import javax.annotation.Nonnull;
import java.net.URI;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Set;

public class SystemCatalogQueryTest {

    @RegisterExtension
    public static final EmbeddedRelationalExtension relationalExtension = new EmbeddedRelationalExtension();

    @BeforeEach
    public void setup() throws Exception {
        runDdl("CREATE SCHEMA TEMPLATE st CREATE TABLE FOO (ID int64, BAR string, PRIMARY KEY(ID));");
        createDb("/DB1");
        createDb("/DB2");
        createDb("/DB3");
        createSchema("/DB1", "/S11");
        createSchema("/DB2", "/S12");
        createSchema("/DB2", "/S21");
        createSchema("/DB3", "/S31");
        createSchema("/DB3", "/S32");
        createSchema("/DB3", "/S33");

    }

    @AfterEach
    public void teardown() throws Exception {
        dropDb("/DB1");
        dropDb("/DB2");
        dropDb("/DB3");
        runDdl("DROP SCHEMA TEMPLATE st");
    }

    private static void runDdl(@Nonnull final String ddl) throws Exception {
        try (RelationalConnection conn = Relational.connect(URI.create("jdbc:embed:/__SYS"), Options.NONE)) {
            conn.setSchema("CATALOG");
            try (Statement statement = conn.createStatement()) {
                statement.executeUpdate(ddl);
            }
        }
    }

    private static void createDb(@Nonnull final String dbName) throws Exception {
        runDdl(String.format("CREATE DATABASE %s", dbName));
    }

    private static void createSchema(@Nonnull final String dbName, @Nonnull final String schemaName) throws Exception {
        runDdl(String.format("CREATE SCHEMA %s%s WITH TEMPLATE st", dbName, schemaName));
    }

    private static void dropDb(@Nonnull final String dbName) throws Exception {
        runDdl(String.format("DROP DATABASE %s", dbName));
    }

    @Test
    @SuppressWarnings("checkstyle:Indentation")
    public void selectSchemasWorks() throws RelationalException, SQLException {
        try (RelationalConnection conn = Relational.connect(URI.create("jdbc:embed:/__SYS"), Options.NONE)) {
            conn.setSchema("CATALOG");
            //we are selective here to make it easier to check the correctness of the row (otherwise we'd have to put
            //MetaData objects in for equality)
            try (RelationalStatement statement = conn.createStatement();
                 RelationalResultSet rs = statement.executeQuery("SELECT schema_name,database_id FROM \"SCHEMAS\"")) {
                ResultSetAssert.assertThat(rs).containsRowsExactly(List.of(
                        new Object[]{"S11", "/DB1"},
                        new Object[]{"S12", "/DB2"},
                        new Object[]{"S21", "/DB2"},
                        new Object[]{"S31", "/DB3"},
                        new Object[]{"S32", "/DB3"},
                        new Object[]{"S33", "/DB3"},
                        new Object[]{"CATALOG", "/__SYS"}
                ));
            }
        }
    }

    @Test
    public void selectSchemasWithPredicateAndProjectionWorks() throws RelationalException, SQLException {
        try (RelationalConnection conn = Relational.connect(URI.create("jdbc:embed:/__SYS"), Options.NONE)) {
            conn.setSchema("CATALOG");
            try (Statement statement = conn.createStatement(); ResultSet rs = statement.executeQuery("SELECT schema_name FROM \"SCHEMAS\" WHERE database_id = '/__SYS'")) {
                shouldBe(rs, Set.of(
                        List.of("CATALOG")
                ));
            }
        }
    }

    @Test
    public void selectDatabaseInfoWorks() throws RelationalException, SQLException {
        try (RelationalConnection conn = Relational.connect(URI.create("jdbc:embed:/__SYS"), Options.NONE)) {
            conn.setSchema("CATALOG");
            try (Statement statement = conn.createStatement(); ResultSet rs = statement.executeQuery("SELECT * FROM \"DATABASES\"")) {
                shouldBe(rs, Set.of(
                        List.of("/DB1"),
                        List.of("/DB2"),
                        List.of("/DB3"),
                        List.of("/__SYS")
                ));
            }
        }
    }

    @Test
    public void selectDatabaseInfoWithPredicateAndProjectionWorks() throws RelationalException, SQLException {
        try (RelationalConnection conn = Relational.connect(URI.create("jdbc:embed:/__SYS"), Options.NONE)) {
            conn.setSchema("CATALOG");
            try (Statement statement = conn.createStatement(); ResultSet rs = statement.executeQuery("SELECT database_id FROM \"DATABASES\" WHERE database_id != '/__SYS'")) {
                shouldBe(rs, Set.of(
                        List.of("/DB1"),
                        List.of("/DB2"),
                        List.of("/DB3")
                ));
            }
        }
    }

    private static void shouldBe(@Nonnull final ResultSet resultSet, @Nonnull final Set<List<String>> expected) throws SQLException {
        assert !expected.isEmpty();
        final int colCount = expected.stream().findFirst().get().size();
        final ImmutableSet.Builder<List<String>> actual = ImmutableSet.builder();
        while (resultSet.next()) {
            ImmutableList.Builder<String> row = ImmutableList.builder();
            for (int i = 1; i <= colCount; i++) {
                row.add(resultSet.getString(i));
            }
            actual.add(row.build());
        }
        Assertions.assertEquals(expected, actual.build());
    }

}
