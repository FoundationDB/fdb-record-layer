/*
 * CaseSensitivityTest.java
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

import com.apple.foundationdb.relational.api.DynamicMessageBuilder;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.Row;
import com.apple.foundationdb.relational.api.StructMetaData;
import com.apple.foundationdb.relational.api.Relational;
import com.apple.foundationdb.relational.api.RelationalConnection;
import com.apple.foundationdb.relational.api.RelationalDatabaseMetaData;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.RelationalStatement;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.utils.Ddl;
import com.apple.foundationdb.relational.utils.ResultSetAssert;
import com.apple.foundationdb.relational.utils.RelationalAssertions;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.net.URI;
import java.sql.Statement;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

public class CaseSensitivityTest {

    @RegisterExtension
    @Order(0)
    public final EmbeddedRelationalExtension relationalExtension = new EmbeddedRelationalExtension();

    public CaseSensitivityTest() {
    }

    @Test
    void selectFromCaseInsensitiveTable() throws Exception {
        final String schema = "CREATE TABLE tbl1 (id int64, value int64, PRIMARY KEY(id))";
        try (var ddl = Ddl.builder().database("CaseSensitivity").relationalExtension(relationalExtension).schemaTemplate(schema).build()) {
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
        try (RelationalConnection conn = Relational.connect(URI.create("jdbc:embed:/__SYS"), Options.NONE)) {
            conn.setSchema("CATALOG");
            try (Statement statement = conn.createStatement()) {
                //create a database
                statement.executeUpdate("CREATE DATABASE /upper");
                RelationalAssertions.assertThrowsSqlException(() ->
                        statement.executeUpdate("CREATE DATABASE \"/UPPER\""))
                        .hasErrorCode(ErrorCode.DATABASE_ALREADY_EXISTS);
            } finally {
                try (Statement statement = conn.createStatement()) {
                    //try to drop the db for test cleanliness
                    statement.executeUpdate("DROP DATABASE /upper");
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
        List<String> databases = List.of("/ABC1", "/def2", "/Ghi3", "/jKL4");
        try (RelationalConnection conn = Relational.connect(URI.create("jdbc:embed:/__SYS"), Options.NONE)) {
            conn.setSchema("CATALOG");
            try {
                try (RelationalStatement statement = conn.createStatement()) {
                    // Quoted databases
                    for (String db : databases) {
                        statement.executeUpdate("CREATE DATABASE " + quote(db, quoted));
                    }
                }
                try (RelationalStatement statement = conn.createStatement()) {
                    try (RelationalResultSet rs = statement.executeQuery("SELECT * FROM \"DATABASES\"")) {
                        List<Row> expectedResults = databases.stream().map(db -> quoted ? db : db.toUpperCase(Locale.ROOT)).map(ValueTuple::new).collect(Collectors.toList());
                        expectedResults.add(new ValueTuple("/__SYS"));
                        ResultSetAssert.assertThat(rs)
                                .isExactlyInAnyOrder(new IteratorResultSet(rs.getMetaData().unwrap(StructMetaData.class), expectedResults.listIterator(), 0));
                    }
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
        List<String> schemas = List.of("ABC1", "def2", "Ghi3", "jKL4");
        try (RelationalConnection conn = Relational.connect(URI.create("jdbc:embed:/__SYS"), Options.NONE)) {
            conn.setSchema("CATALOG");
            try {
                try (RelationalStatement statement = conn.createStatement()) {
                    statement.executeUpdate("CREATE DATABASE /various_schemas");
                    statement.executeUpdate("CREATE SCHEMA TEMPLATE temp_various_schemas CREATE TABLE foo(a int64, PRIMARY KEY(a))");
                }
                try (RelationalStatement statement = conn.createStatement()) {
                    for (String schema : schemas) {
                        statement.executeUpdate("CREATE SCHEMA " + quote("/VARIOUS_SCHEMAS/" + schema, quoted) + " WITH TEMPLATE temp_various_schemas");
                    }
                }
                try (RelationalStatement statement = conn.createStatement()) {
                    try (RelationalResultSet rs = statement.executeQuery("SELECT SCHEMA_NAME FROM \"SCHEMAS\" WHERE DATABASE_ID = '/VARIOUS_SCHEMAS'")) {
                        List<Row> expectedResults = schemas.stream().map(s -> quoted ? s : s.toUpperCase(Locale.ROOT)).map(ValueTuple::new).collect(Collectors.toList());
                        ResultSetAssert.assertThat(rs)
                                .isExactlyInAnyOrder(new IteratorResultSet(rs.getMetaData().unwrap(StructMetaData.class), expectedResults.listIterator(), 0));
                    }
                }
            } finally {
                try (Statement statement = conn.createStatement()) {
                    statement.executeUpdate("DROP DATABASE /various_schemas");
                    statement.executeUpdate("DROP SCHEMA TEMPLATE temp_various_schemas");
                }
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void variousStructs(boolean quoted) throws Exception {
        List<String> structs = List.of("ABC1", "def2", "Ghi3", "jKL4");
        try (RelationalConnection conn = Relational.connect(URI.create("jdbc:embed:/__SYS"), Options.NONE)) {
            conn.setSchema("CATALOG");
            try {
                try (RelationalStatement statement = conn.createStatement()) {
                    for (String struct : structs) {
                        Assertions.assertDoesNotThrow(() ->
                                statement.executeUpdate(String.format(
                                        "CREATE SCHEMA TEMPLATE temp_various_struct_%s CREATE STRUCT %s (a int64) CREATE TABLE foo(a %s, PRIMARY KEY(a))",
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
        try (RelationalConnection conn = Relational.connect(URI.create("jdbc:embed:/__SYS"), Options.NONE)) {
            conn.setSchema("CATALOG");
            try {
                try (RelationalStatement statement = conn.createStatement()) {
                    statement.executeUpdate("CREATE DATABASE /various_tables_db");
                    for (String table : tables) {
                        statement.executeUpdate(String.format(
                                "CREATE SCHEMA TEMPLATE temp_various_table_%s CREATE TABLE %s (a int64, PRIMARY KEY(a))",
                                table, quoted ? quote(table, true) : table.toLowerCase(Locale.ROOT)));
                        statement.executeUpdate(String.format(
                                "CREATE SCHEMA /various_tables_db/various_table_%s with template temp_various_table_%s",
                                table, table));
                    }
                }
                RelationalDatabaseMetaData md = conn.getMetaData().unwrap(RelationalDatabaseMetaData.class);
                for (String table : tables) {
                    try (RelationalResultSet rs = md.getTables("/VARIOUS_TABLES_DB", "VARIOUS_TABLE_" + table.toUpperCase(Locale.ROOT), null, null)) {
                        List<Row> row = List.of(new ArrayRow(
                                "/VARIOUS_TABLES_DB",
                                "VARIOUS_TABLE_" + table.toUpperCase(Locale.ROOT),
                                quoted ? table : table.toUpperCase(Locale.ROOT),
                                null));
                        ResultSetAssert.assertThat(rs).isExactlyInAnyOrder(new IteratorResultSet(rs.getMetaData().unwrap(StructMetaData.class), row.iterator(), 0));
                    }
                }
            } finally {
                try (Statement statement = conn.createStatement()) {
                    for (String table : tables) {
                        statement.executeUpdate("DROP DATABASE /various_tables_db");
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
        try (RelationalConnection conn = Relational.connect(URI.create("jdbc:embed:/__SYS"), Options.NONE)) {
            conn.setSchema("CATALOG");
            try {
                try (RelationalStatement statement = conn.createStatement()) {
                    statement.executeUpdate("CREATE DATABASE /various_columns_db");
                    for (String column : columns) {
                        statement.executeUpdate(String.format(
                                "CREATE SCHEMA TEMPLATE temp_various_column_%s CREATE TABLE tbl_various_columns (%s int64, PRIMARY KEY(%s))",
                                column,
                                quoted ? quote(column, true) : column.toLowerCase(Locale.ROOT),
                                quoted ? quote(column, true) : column.toLowerCase(Locale.ROOT)));
                        statement.executeUpdate(String.format(
                                "CREATE SCHEMA /various_columns_db/various_columns_%s with template temp_various_column_%s",
                                column, column));
                    }
                }
                RelationalDatabaseMetaData md = conn.getMetaData().unwrap(RelationalDatabaseMetaData.class);
                for (String column : columns) {
                    try (RelationalResultSet rs = md.getColumns("/VARIOUS_COLUMNS_DB", "VARIOUS_COLUMNS_" + column.toUpperCase(Locale.ROOT), "TBL_VARIOUS_COLUMNS", null)) {
                        ResultSetAssert.assertThat(rs).hasNextRow().hasColumn("COLUMN_NAME", quoted ? column : column.toUpperCase(Locale.ROOT));
                    }
                }
            } finally {
                try (Statement statement = conn.createStatement()) {
                    for (String column : columns) {
                        statement.executeUpdate("DROP DATABASE /various_columns_db");
                        statement.executeUpdate("DROP SCHEMA TEMPLATE temp_various_column_" + column);
                    }
                }
            }
        }
    }

    @Test
    public void overload() throws Exception {
        List<String> databases = List.of("/database", "/DATABASE", "/Database", "/DaTaBaSe");
        List<String> schemas = List.of("schema", "SCHEMA", "Schema", "ScHeMa");
        List<String> tables = List.of("table", "TABLE", "Table", "TaBlE");
        List<String> columns = List.of("column", "COLUMN", "Column", "CoLuMn");
        try (RelationalConnection conn = Relational.connect(URI.create("jdbc:embed:/__SYS"), Options.NONE)) {
            conn.setSchema("CATALOG");
            try {
                for (String database : databases) {
                    // DDL
                    try (RelationalStatement statement = conn.createStatement()) {
                        statement.executeUpdate(String.format(
                                "CREATE DATABASE \"%s\"",
                                database)
                        );
                        for (String schema : schemas) {
                            StringBuilder template = new StringBuilder();
                            template.append("CREATE SCHEMA TEMPLATE \"").append(schema).append("_template\" ");
                            for (String table : tables) {
                                template.append("CREATE TABLE \"").append(table).append("\" (");
                                template.append(columns.stream().map(c -> "\"" + c + "\" int64").collect(Collectors.joining(",")));
                                template.append(", ").append("PRIMARY KEY (\"").append(columns.get(0)).append("\")");
                                template.append(") ");
                            }
                            statement.executeUpdate(template.toString());
                            statement.executeUpdate(String.format("CREATE SCHEMA \"%s/%s\" WITH TEMPLATE \"%s_template\"", database, schema, schema));
                        }
                    }
                    int value = 0;
                    // Data insertion
                    try (RelationalConnection dbConn = Relational.connect(URI.create("jdbc:embed:" + database), Options.NONE)) {
                        try (RelationalStatement statement = dbConn.createStatement()) {
                            for (String schema : schemas) {
                                dbConn.setSchema(schema);
                                for (String table : tables) {
                                    DynamicMessageBuilder rowBuilder = statement.getDataBuilder(table);
                                    for (String column : columns) {
                                        rowBuilder.setField(column, value++);
                                    }
                                    statement.executeInsert(table, rowBuilder.build());
                                }
                            }
                        }
                    }
                    value = 0;
                    // Data retrieval
                    // Disabled until TODO is fixed
                    //try (RelationalConnection dbConn = Relational.connect(URI.create("jdbc:embed:" + database), Options.NONE)) {
                    //    try (RelationalStatement statement = dbConn.createStatement()) {
                    //        for (String schema : schemas) {
                    //            dbConn.setSchema(schema);
                    //            for (String table : tables) {
                    //                for (String column : columns) {
                    //                    try (RelationalResultSet rs = statement.executeQuery(String.format("SELECT \"%s\" from \"%s\"", column, table))) {
                    //                        ResultSetAssert.assertThat(rs).hasNextRow().hasColumn(column, value++);
                    //                    }
                    //                }
                    //            }
                    //        }
                    //    }
                    //}
                }
            } finally {
                // Cleanup
                try (RelationalStatement statement = conn.createStatement()) {
                    for (String database : databases) {
                        statement.executeUpdate(String.format("DROP DATABASE \"%s\"", database));
                    }
                    for (String schema : schemas) {
                        statement.executeUpdate(String.format("DROP SCHEMA TEMPLATE \"%s_template\"", schema));
                    }
                }
            }
        }
    }
}
