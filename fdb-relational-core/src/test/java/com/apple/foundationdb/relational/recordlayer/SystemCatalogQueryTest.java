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
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import javax.annotation.Nonnull;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Locale;
import java.util.Set;

public class SystemCatalogQueryTest {

    @RegisterExtension
    public static final EmbeddedRelationalExtension relationalExtension = new EmbeddedRelationalExtension();

    @BeforeEach
    public void setup() throws Exception {
        runDdl("CREATE SCHEMA TEMPLATE st CREATE TABLE FOO (ID bigint, BAR string, PRIMARY KEY(ID))");
        createDb("/TEST/DB1");
        createDb("/TEST/DB2");
        createDb("/TEST/DB3");
        createSchema("/TEST/DB1", "/S11");
        createSchema("/TEST/DB2", "/S12");
        createSchema("/TEST/DB2", "/S21");
        createSchema("/TEST/DB3", "/S31");
        createSchema("/TEST/DB3", "/S32");
        createSchema("/TEST/DB3", "/S33");
    }

    @AfterEach
    public void teardown() throws Exception {
        dropDb("/TEST/DB1");
        dropDb("/TEST/DB2");
        dropDb("/TEST/DB3");
        runDdl("DROP SCHEMA TEMPLATE st");
    }

    private static void runDdl(@Nonnull final String ddl) throws Exception {
        try (final var conn = DriverManager.getConnection("jdbc:embed:/__SYS")) {
            conn.setSchema("CATALOG");
            try (final var statement = conn.createStatement()) {
                statement.executeUpdate(ddl);
            }
        }
    }

    private static void createDb(@Nonnull final String dbName) throws Exception {
        runDdl(String.format(Locale.ROOT, "CREATE DATABASE %s", dbName));
    }

    private static void createSchema(@Nonnull final String dbName, @Nonnull final String schemaName) throws Exception {
        runDdl(String.format(Locale.ROOT, "CREATE SCHEMA %s%s WITH TEMPLATE st", dbName, schemaName));
    }

    private static void dropDb(@Nonnull final String dbName) throws Exception {
        runDdl(String.format(Locale.ROOT, "DROP DATABASE %s", dbName));
    }

    @Disabled // TODO (SystemCatalogQueryTest has fragile tests)
    @Test
    @SuppressWarnings("checkstyle:Indentation")
    public void selectSchemasWorks() throws SQLException {
        try (RelationalConnection conn = DriverManager.getConnection("jdbc:embed:/__SYS").unwrap(RelationalConnection.class)) {
            conn.setSchema("CATALOG");
            //we are selective here to make it easier to check the correctness of the row (otherwise we'd have to put
            //MetaData objects in for equality)
            try (RelationalStatement statement = conn.createStatement();
                 RelationalResultSet rs = statement.executeQuery("SELECT schema_name,database_id FROM \"SCHEMAS\"")) {
                ResultSetAssert.assertThat(rs).containsRowsExactly(List.of(
                        new Object[]{"S11", "/TEST/DB1"},
                        new Object[]{"S12", "/TEST/DB2"},
                        new Object[]{"S21", "/TEST/DB2"},
                        new Object[]{"S31", "/TEST/DB3"},
                        new Object[]{"S32", "/TEST/DB3"},
                        new Object[]{"S33", "/TEST/DB3"},
                        new Object[]{"CATALOG", "/__SYS"}
                ));
            }
        }
    }

    @Test
    public void selectSchemasWithPredicateAndProjectionWorks() throws SQLException {
        try (final var conn = DriverManager.getConnection("jdbc:embed:/__SYS")) {
            conn.setSchema("CATALOG");
            try (final var statement = conn.createStatement(); final var rs = statement.executeQuery("SELECT schema_name FROM \"SCHEMAS\" WHERE database_id = '/__SYS'")) {
                shouldBe(rs, Set.of(
                        List.of("CATALOG")
                ));
            }
        }
    }

    @Disabled // TODO (SystemCatalogQueryTest has fragile tests)
    @Test
    public void selectDatabaseInfoWorks() throws SQLException {
        try (final var conn = DriverManager.getConnection("jdbc:embed:/__SYS")) {
            conn.setSchema("CATALOG");
            try (final var statement = conn.createStatement(); final var rs = statement.executeQuery("SELECT * FROM \"DATABASES\"")) {
                shouldBe(rs, Set.of(
                        List.of("/TEST/DB1"),
                        List.of("/TEST/DB2"),
                        List.of("/TEST/DB3"),
                        List.of("/__SYS")
                ));
            }
        }
    }

    @Disabled // TODO (SystemCatalogQueryTest has fragile tests)
    @Test
    public void selectDatabaseInfoWithPredicateAndProjectionWorks() throws RelationalException, SQLException {
        try (final var conn = DriverManager.getConnection("jdbc:embed:/__SYS")) {
            conn.setSchema("CATALOG");
            try (final var statement = conn.createStatement(); final var rs = statement.executeQuery("SELECT database_id FROM \"DATABASES\" WHERE database_id != '/__SYS'")) {
                shouldBe(rs, Set.of(
                        List.of("/TEST/DB1"),
                        List.of("/TEST/DB2"),
                        List.of("/TEST/DB3")
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
