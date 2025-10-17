/*
 * CaseSensitivityTest.java
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

package com.apple.foundationdb.relational.recordlayer;

import com.apple.foundationdb.relational.api.EmbeddedRelationalStruct;
import com.apple.foundationdb.relational.api.Row;
import com.apple.foundationdb.relational.api.StructMetaData;
import com.apple.foundationdb.relational.api.RelationalConnection;
import com.apple.foundationdb.relational.api.RelationalDatabaseMetaData;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.RelationalStatement;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.utils.Ddl;
import com.apple.foundationdb.relational.utils.ResultSetAssert;
import com.apple.foundationdb.relational.utils.RelationalAssertions;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.net.URI;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

public class CaseSensitivityTest {

    @RegisterExtension
    @Order(0)
    public final EmbeddedRelationalExtension relationalExtension = new EmbeddedRelationalExtension();

    public CaseSensitivityTest() {
    }

    @Test
    void selectFromCaseInsensitiveTable() throws Exception {
        final String schema = "CREATE TABLE tbl1 (id bigint, value bigint, PRIMARY KEY(id))";
        try (var ddl = Ddl.builder().database(URI.create("/TEST/CaseSensitivity")).relationalExtension(relationalExtension).schemaTemplate(schema).build()) {
            try (var statement = ddl.setSchemaAndGetConnection().createStatement()) {
                for (String tableName : List.of("tbl1", "TBL1", "TbL1", "\"TBL1\"")) {
                    Assertions.assertDoesNotThrow(() -> statement.execute("SELECT * FROM " + tableName));
                }
                for (String tableName : List.of("\"tbl1\"", "\"TBl1\"")) {
                    RelationalAssertions.assertThrowsSqlException(() -> statement.execute("SELECT * FROM " + tableName))
                            .hasErrorCode(ErrorCode.UNDEFINED_TABLE);
                }
            }
        }
    }

    @Test
    public void databaseWithSameUpperName() throws Exception {
        try (final var conn = DriverManager.getConnection("jdbc:embed:/__SYS")) {
            conn.setSchema("CATALOG");
            try (final var statement = conn.createStatement()) {
                //create a database
                statement.executeUpdate("CREATE DATABASE /test/upper");
                RelationalAssertions.assertThrowsSqlException(() ->
                        statement.executeUpdate("CREATE DATABASE \"/TEST/UPPER\""))
                        .hasErrorCode(ErrorCode.DATABASE_ALREADY_EXISTS);
            } finally {
                try (final var statement = conn.createStatement()) {
                    //try to drop the db for test cleanliness
                    statement.executeUpdate("DROP DATABASE /test/upper");
                }
            }
        }
    }

    private String quote(String dbObject, boolean quote) {
        if (quote) {
            return "\"" + dbObject + "\"";
        } else {
            return dbObject;
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void variousDatabases(boolean quoted) throws Exception {
        List<String> databases = List.of("/TEST/ABC1", "/TEST/def2", "/TEST/Ghi3", "/TEST/jKL4");
        try (final var conn = DriverManager.getConnection("jdbc:embed:/__SYS")) {
            conn.setSchema("CATALOG");
            try {
                try (final var statement = conn.createStatement()) {
                    // Quoted databases
                    for (String db : databases) {
                        statement.executeUpdate("CREATE DATABASE " + quote(db, quoted));
                    }
                }
                List<String> result = new ArrayList<>();
                try (final var statement = conn.createStatement()) {
                    try (final var rs = statement.executeQuery("SELECT * FROM \"DATABASES\"")) {
                        while (rs.next()) {
                            result.add(rs.getString(1));
                        }
                    }
                }
                if (quoted) {
                    assertThat(result).containsAll(databases);
                } else {
                    assertThat(result).containsAll(databases.stream().map(s -> s.toUpperCase(Locale.ROOT)).collect(Collectors.toList()));
                }
            } finally {
                try (Statement statement = conn.createStatement()) {
                    for (String db : databases) {
                        statement.executeUpdate("DROP DATABASE " + quote(db, quoted));
                    }
                }
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void variousSchemas(boolean quoted) throws Exception {
        List<String> schemas = List.of("ABC2", "def3", "Ghi4", "jKL5");
        try (final var conn = DriverManager.getConnection("jdbc:embed:/__SYS")) {
            conn.setSchema("CATALOG");
            try {
                try (final var statement = conn.createStatement()) {
                    statement.executeUpdate("DROP DATABASE if exists /test/various_schemas");
                    statement.executeUpdate("CREATE DATABASE /test/various_schemas");
                    statement.executeUpdate("CREATE SCHEMA TEMPLATE temp_various_schemas CREATE TABLE foo(a bigint, PRIMARY KEY(a))");
                }
                try (final var statement = conn.createStatement()) {
                    for (String schema : schemas) {
                        statement.executeUpdate("CREATE SCHEMA " + quote("/TEST/VARIOUS_SCHEMAS/" + schema, quoted) + " WITH TEMPLATE temp_various_schemas");
                    }
                }
                try (final var statement = conn.createStatement().unwrap(RelationalStatement.class)) {
                    try (RelationalResultSet rs = statement.executeQuery("SELECT SCHEMA_NAME FROM \"SCHEMAS\" WHERE DATABASE_ID = '/TEST/VARIOUS_SCHEMAS'")) {
                        List<Row> expectedResults = schemas.stream().map(s -> quoted ? s : s.toUpperCase(Locale.ROOT)).map(ValueTuple::new).collect(Collectors.toList());
                        ResultSetAssert.assertThat(rs)
                                .isExactlyInAnyOrder(new IteratorResultSet(rs.getMetaData().unwrap(StructMetaData.class), expectedResults.listIterator(), 0));
                    }
                }
            } finally {
                try (Statement statement = conn.createStatement()) {
                    statement.executeUpdate("DROP DATABASE /test/various_schemas");
                    statement.executeUpdate("DROP SCHEMA TEMPLATE temp_various_schemas");
                }
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    @Disabled("Fails with 'com.apple.foundationdb.record.query.expressions.Query$InvalidExpressionException: A is a " +
            "nested message, but accessed as a scalar' when persisting schema templates instead of in-memory. TODO." +
            "See TODO (CaseSensitivityTests broken by TODO (RecordLayer hosted (persistent) " +
            "SchemaTemplateCatalog))")
    public void variousStructs(boolean quoted) throws Exception {
        List<String> structs = List.of(/*"ABC1",*/ "def2", "Ghi3", "jKL4");
        try (RelationalConnection conn = DriverManager.getConnection("jdbc:embed:/__SYS").unwrap(RelationalConnection.class)) {
            conn.setSchema("CATALOG");
            try {
                try (RelationalStatement statement = conn.createStatement()) {
                    for (String struct : structs) {
                        Assertions.assertDoesNotThrow(() ->
                                statement.executeUpdate(String.format(Locale.ROOT,
                                        "CREATE SCHEMA TEMPLATE temp_various_struct_%s CREATE TYPE AS STRUCT %s (a bigint) CREATE TABLE foo(a %s, PRIMARY KEY(a))",
                                        struct, quote(struct, quoted), quoted ? quote(struct, true) : struct.toLowerCase(Locale.ROOT))));
                    }
                }
            } finally {
                try (Statement statement = conn.createStatement()) {
                    for (String struct : structs) {
                        statement.executeUpdate("DROP SCHEMA TEMPLATE temp_various_struct_" + struct);
                    }
                }
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void variousTables(boolean quoted) throws Exception {
        List<String> tables = List.of("ABC1", "def2", "Ghi3", "jKL4");
        try (RelationalConnection conn = DriverManager.getConnection("jdbc:embed:/__SYS").unwrap(RelationalConnection.class)) {
            conn.setSchema("CATALOG");
            try {
                try (RelationalStatement statement = conn.createStatement()) {
                    statement.executeUpdate("DROP DATABASE if exists /test/various_tables_db");
                    statement.executeUpdate("CREATE DATABASE /test/various_tables_db");
                    for (String table : tables) {
                        statement.executeUpdate(String.format(Locale.ROOT,
                                "CREATE SCHEMA TEMPLATE temp_various_table_%s CREATE TABLE %s (a bigint, PRIMARY KEY(a))",
                                table, quoted ? quote(table, true) : table.toLowerCase(Locale.ROOT)));
                        statement.executeUpdate(String.format(Locale.ROOT,
                                "CREATE SCHEMA /test/various_tables_db/various_table_%s with template temp_various_table_%s",
                                table, table));
                    }
                }
                RelationalDatabaseMetaData md = conn.getMetaData().unwrap(RelationalDatabaseMetaData.class);
                for (String table : tables) {
                    try (RelationalResultSet rs = md.getTables("/TEST/VARIOUS_TABLES_DB", "VARIOUS_TABLE_" + table.toUpperCase(Locale.ROOT), null, null)) {
                        List<Row> row = List.of(new ArrayRow(
                                "/TEST/VARIOUS_TABLES_DB",
                                "VARIOUS_TABLE_" + table.toUpperCase(Locale.ROOT),
                                quoted ? table : table.toUpperCase(Locale.ROOT),
                                null));
                        ResultSetAssert.assertThat(rs).isExactlyInAnyOrder(new IteratorResultSet(rs.getMetaData(), row.iterator(), 0));
                    }
                }
            } finally {
                try (Statement statement = conn.createStatement()) {
                    statement.executeUpdate("DROP DATABASE /test/various_tables_db");
                    for (String table : tables) {
                        statement.executeUpdate("DROP SCHEMA TEMPLATE temp_various_table_" + table);
                    }
                }
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void variousColumns(boolean quoted) throws Exception {
        List<String> columns = List.of("ABC1", "def2", "Ghi3", "jKL4");
        try (RelationalConnection conn = DriverManager.getConnection("jdbc:embed:/__SYS").unwrap(RelationalConnection.class)) {
            conn.setSchema("CATALOG");
            try {
                try (RelationalStatement statement = conn.createStatement()) {
                    statement.executeUpdate("CREATE DATABASE /test/various_columns_db");
                    for (String column : columns) {
                        statement.executeUpdate(String.format(Locale.ROOT,
                                "CREATE SCHEMA TEMPLATE temp_various_column_%s CREATE TABLE tbl_various_columns (%s bigint, PRIMARY KEY(%s))",
                                column,
                                quoted ? quote(column, true) : column.toLowerCase(Locale.ROOT),
                                quoted ? quote(column, true) : column.toLowerCase(Locale.ROOT)));
                        statement.executeUpdate(String.format(Locale.ROOT,
                                "CREATE SCHEMA /test/various_columns_db/various_columns_%s with template temp_various_column_%s",
                                column, column));
                    }
                }
                RelationalDatabaseMetaData md = conn.getMetaData().unwrap(RelationalDatabaseMetaData.class);
                for (String column : columns) {
                    try (RelationalResultSet rs = md.getColumns("/TEST/VARIOUS_COLUMNS_DB", "VARIOUS_COLUMNS_" + column.toUpperCase(Locale.ROOT), "TBL_VARIOUS_COLUMNS", null)) {
                        ResultSetAssert.assertThat(rs)
                                .hasNextRow()
                                .hasColumn("COLUMN_NAME", quoted ? column : column.toUpperCase(Locale.ROOT));
                    }
                }
            } finally {
                try (Statement statement = conn.createStatement()) {
                    statement.executeUpdate("DROP DATABASE /test/various_columns_db");
                    for (String column : columns) {
                        statement.executeUpdate("DROP SCHEMA TEMPLATE temp_various_column_" + column);
                    }
                }
            }
        }
    }

    @Test
    public void overload() throws Exception {
        List<String> databases = List.of("/TEST/database", "/TEST/DATABASE", "/TEST/Database", "/TEST/DaTaBaSe");
        List<String> schemas = List.of("schema", "SCHEMA", "Schema", "ScHeMa");
        List<String> tables = List.of("table", "TABLE", "Table", "TaBlE");
        List<String> columns = List.of("column", "COLUMN", "Column", "CoLuMn");
        try (RelationalConnection conn = DriverManager.getConnection("jdbc:embed:/__SYS").unwrap(RelationalConnection.class)) {
            conn.setSchema("CATALOG");
            try {
                try (RelationalStatement statement = conn.createStatement()) {
                    for (String schema : schemas) {
                        StringBuilder template = new StringBuilder();
                        template.append("CREATE SCHEMA TEMPLATE \"").append(schema).append("_template\" ");
                        for (String table : tables) {
                            template.append("CREATE TABLE \"").append(table).append("\" (");
                            template.append(columns.stream().map(c -> "\"" + c + "\" bigint").collect(Collectors.joining(",")));
                            template.append(", ").append("PRIMARY KEY (\"").append(columns.get(0)).append("\")");
                            template.append(") ");
                        }
                        statement.executeUpdate(template.toString());
                    }
                }
                for (String database : databases) {
                    // DDL
                    try (RelationalStatement statement = conn.createStatement()) {
                        statement.executeUpdate(String.format(Locale.ROOT,
                                "CREATE DATABASE \"%s\"",
                                database)
                        );
                        for (String schema : schemas) {
                            statement.executeUpdate(String.format(Locale.ROOT, "CREATE SCHEMA \"%s/%s\" WITH TEMPLATE \"%s_template\"", database, schema, schema));
                        }
                    }
                    long value = 0;
                    // Data insertion
                    try (RelationalConnection dbConn = DriverManager.getConnection("jdbc:embed:" + database).unwrap(RelationalConnection.class)) {
                        try (RelationalStatement statement = dbConn.createStatement()) {
                            for (String schema : schemas) {
                                dbConn.setSchema(schema);
                                for (String table : tables) {
                                    final var builder = EmbeddedRelationalStruct.newBuilder();
                                    for (String column : columns) {
                                        builder.addLong(column, value++);
                                    }
                                    statement.executeInsert(table, builder.build());
                                }
                            }
                        }
                    }
                    value = 0;
                    // Data retrieval
                    try (RelationalConnection dbConn = DriverManager.getConnection("jdbc:embed:" + database).unwrap(RelationalConnection.class)) {
                        try (RelationalStatement statement = dbConn.createStatement()) {
                            for (String schema : schemas) {
                                dbConn.setSchema(schema);
                                for (String table : tables) {
                                    for (String column : columns) {
                                        try (RelationalResultSet rs = statement.executeQuery(String.format(Locale.ROOT, "SELECT \"%s\" from \"%s\"", column, table))) {
                                            ResultSetAssert.assertThat(rs).hasNextRow().hasColumn(column, value++);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            } finally {
                // Cleanup
                try (RelationalStatement statement = conn.createStatement()) {
                    for (String database : databases) {
                        statement.executeUpdate(String.format(Locale.ROOT, "DROP DATABASE if exists \"%s\"", database));
                    }
                    for (String schema : schemas) {
                        statement.executeUpdate(String.format(Locale.ROOT, "DROP SCHEMA TEMPLATE if exists \"%s_template\"", schema));
                    }
                }
            }
        }
    }
}
