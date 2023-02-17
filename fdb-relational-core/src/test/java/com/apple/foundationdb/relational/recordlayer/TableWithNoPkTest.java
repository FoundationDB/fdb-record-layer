/*
 * TableWithNoPkTest.java
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

import com.apple.foundationdb.relational.api.KeySet;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.Relational;
import com.apple.foundationdb.relational.api.RelationalConnection;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.RelationalStatement;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.utils.ResultSetAssert;
import com.apple.foundationdb.relational.utils.SimpleDatabaseRule;
import com.apple.foundationdb.relational.utils.RelationalAssertions;

import com.google.protobuf.Message;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.net.URI;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;

/**
 * A table with no primary key (but with a record-type key can contain only one row.
 */
public class TableWithNoPkTest {
    @RegisterExtension
    @Order(0)
    public final EmbeddedRelationalExtension relationalExtension = new EmbeddedRelationalExtension();

    @RegisterExtension
    @Order(1)
    public final SimpleDatabaseRule db = new SimpleDatabaseRule(relationalExtension, TableWithNoPkTest.class,
            "CREATE TABLE no_pk(a bigint, b bigint, SINGLE ROW ONLY)");

    @Test
    void simpleTest() throws RelationalException, SQLException {
        try (RelationalConnection conn = Relational.connect(URI.create("jdbc:embed://" + db.getDatabasePath().getPath()), Options.NONE)) {
            conn.setSchema(db.getSchemaName());

            try (RelationalStatement s = conn.createStatement()) {
                final Message row = s.getDataBuilder("NO_PK")
                        .setField("A", 12)
                        .setField("B", 18)
                        .build();

                int inserted = s.executeInsert("NO_PK", row);
                Assertions.assertThat(inserted).withFailMessage("incorrect insertion number!").isEqualTo(1);
                KeySet key = new KeySet();
                try (RelationalResultSet rrs = s.executeGet("NO_PK", key, Options.NONE)) {
                    ResultSetAssert.assertThat(rrs).hasNextRow().hasRow(row).hasNoNextRow();
                }
            }
        }
    }

    @Test
    void twoInserts() throws RelationalException, SQLException {
        try (RelationalConnection conn = Relational.connect(URI.create("jdbc:embed://" + db.getDatabasePath().getPath()), Options.NONE)) {
            conn.setSchema(db.getSchemaName());

            try (RelationalStatement s = conn.createStatement()) {
                final Message row1 = s.getDataBuilder("NO_PK")
                        .setField("A", 12)
                        .setField("B", 18)
                        .build();

                int inserted = s.executeInsert("NO_PK", row1);
                Assertions.assertThat(inserted).withFailMessage("incorrect insertion number!").isEqualTo(1);

                final Message row2 = s.getDataBuilder("NO_PK")
                        .setField("A", 14)
                        .setField("B", 19)
                        .build();

                RelationalAssertions.assertThrowsSqlException(() -> s.executeInsert("NO_PK", row2))
                        .hasErrorCode(ErrorCode.UNIQUE_CONSTRAINT_VIOLATION);

                KeySet key = new KeySet();
                try (RelationalResultSet rrs = s.executeGet("NO_PK", key, Options.NONE)) {
                    ResultSetAssert.assertThat(rrs).hasNextRow().hasRow(row1).hasNoNextRow();
                }
            }
        }
    }

    @Test
    void testDelete() throws Exception {
        try (RelationalConnection conn = Relational.connect(URI.create("jdbc:embed://" + db.getDatabasePath().getPath()), Options.NONE)) {
            conn.setSchema(db.getSchemaName());

            try (RelationalStatement s = conn.createStatement()) {
                final Message row1 = s.getDataBuilder("NO_PK")
                        .setField("A", 12)
                        .setField("B", 18)
                        .build();

                s.executeInsert("NO_PK", row1);
                try (RelationalResultSet rrs = s.executeGet("NO_PK", new KeySet(), Options.NONE)) {
                    ResultSetAssert.assertThat(rrs).hasNextRow().hasNoNextRow();
                }

                s.executeDelete("NO_PK", Collections.singleton(new KeySet()));
                try (RelationalResultSet rrs = s.executeGet("NO_PK", new KeySet(), Options.NONE)) {
                    ResultSetAssert.assertThat(rrs).hasNoNextRow();
                }
            }
        }
    }

    @Test
    void testScan() throws Exception {
        try (RelationalConnection conn = Relational.connect(URI.create("jdbc:embed://" + db.getDatabasePath().getPath()), Options.NONE)) {
            conn.setSchema(db.getSchemaName());

            try (RelationalStatement s = conn.createStatement()) {
                final Message row = s.getDataBuilder("NO_PK")
                        .setField("A", 12)
                        .setField("B", 18)
                        .build();

                s.executeInsert("NO_PK", row);
                try (RelationalResultSet rrs = s.executeScan("NO_PK", new KeySet(), Options.NONE)) {
                    ResultSetAssert.assertThat(rrs).hasNextRow().hasRow(row).hasNoNextRow();
                }
            }
        }
    }

    @Test
    void testQuery() throws Exception {
        try (RelationalConnection conn = Relational.connect(URI.create("jdbc:embed://" + db.getDatabasePath().getPath()), Options.NONE)) {
            conn.setSchema(db.getSchemaName());

            try (RelationalStatement s = conn.createStatement()) {
                final Message row = s.getDataBuilder("NO_PK")
                        .setField("A", 12)
                        .setField("B", 18)
                        .build();

                s.executeInsert("NO_PK", row);
                try (RelationalResultSet rrs = s.executeQuery("SELECT * FROM NO_PK")) {
                    ResultSetAssert.assertThat(rrs).hasNextRow().hasRow(row).hasNoNextRow();
                }

                try (RelationalResultSet rrs = s.executeQuery("SELECT A FROM NO_PK")) {
                    ResultSetAssert.assertThat(rrs).hasNextRow().hasRowExactly(12L).hasNoNextRow();
                }

                try (RelationalResultSet rrs = s.executeQuery("SELECT B FROM NO_PK")) {
                    ResultSetAssert.assertThat(rrs).hasNextRow().hasRowExactly(18L).hasNoNextRow();
                }
            }
        }
    }

    @Test
    void testCreateTableWithNoSingleRowOnlyClause() throws Exception {
        try (RelationalConnection conn = Relational.connect(URI.create("jdbc:embed:/__SYS"), Options.NONE)) {
            conn.setSchema("CATALOG");
            try (Statement statement = conn.createStatement()) {
                //create a schema
                final String createStatement = "CREATE SCHEMA TEMPLATE FOO CREATE TABLE T(A string, B string); ";
                RelationalAssertions.assertThrowsSqlException(() -> statement.executeUpdate(createStatement))
                        .hasErrorCode(ErrorCode.SYNTAX_ERROR)
                        .hasMessage("Illegal table definition 'T'. Include either a PRIMARY KEY clause OR A SINGLE ROW ONLY clause.");
            }
        }
    }
}
