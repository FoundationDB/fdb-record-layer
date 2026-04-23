/*
 * IntermingledTablesTest.java
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
import com.apple.foundationdb.relational.api.KeySet;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.utils.ResultSetAssert;
import com.apple.foundationdb.relational.utils.SchemaTemplateRule;
import com.apple.foundationdb.relational.utils.SimpleDatabaseRule;
import com.apple.foundationdb.relational.utils.RelationalAssertions;

import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.sql.SQLException;
import java.util.List;
import java.util.Locale;

/**
 * Tests of some basic operations in a store where each record's primary key is missing the record type key prefix,
 * and thus different types share the same extent.
 */
public class IntermingledTablesTest {
    private static final String SCHEMA_TEMPLATE =
            """
            CREATE TABLE t1 (group bigint, id string, val bigint, PRIMARY KEY(group, id))
            CREATE TABLE t2 (group bigint, id string, val2 string, PRIMARY KEY(group, id))
            """;

    @RegisterExtension
    @Order(0)
    public final EmbeddedRelationalExtension relationalExtension = new EmbeddedRelationalExtension();

    @RegisterExtension
    @Order(1)
    public final SimpleDatabaseRule database = new SimpleDatabaseRule(IntermingledTablesTest.class, SCHEMA_TEMPLATE, new SchemaTemplateRule.SchemaTemplateOptions(true, true));

    @RegisterExtension
    @Order(2)
    public final RelationalConnectionRule connection = new RelationalConnectionRule(database::getConnectionUri)
            .withOptions(Options.NONE)
            .withSchema("TEST_SCHEMA");

    @RegisterExtension
    @Order(3)
    public final RelationalStatementRule statement = new RelationalStatementRule(connection);

    @Test
    void readFromOtherType() throws SQLException {
        final var row = EmbeddedRelationalStruct.newBuilder()
                .addLong("GROUP", 0)
                .addString("ID", "foo")
                .addLong("VAL", 1066)
                .build();
        statement.executeInsert("T1", row, Options.NONE);

        KeySet key = new KeySet()
                .setKeyColumn("GROUP", 0)
                .setKeyColumn("ID", "foo");
        try (RelationalResultSet resultSet = statement.executeGet("T2", key, Options.NONE)) {
            ResultSetAssert.assertThat(resultSet)
                    .hasNextRow()
                    .hasColumn("GROUP", 0L)
                    .hasColumn("ID", "foo")
                    .hasColumn("VAL2", "1066")
                    .hasNoNextRow();
        }
    }

    @Test
    void doNotInsertOnOtherType() throws SQLException {
        final var row1 = EmbeddedRelationalStruct.newBuilder()
                .addLong("GROUP", 1L)
                .addString("ID", "bar")
                .addLong("VAL", 1412L)
                .build();
        statement.executeInsert("T1", row1, Options.NONE);
        final var row2 = EmbeddedRelationalStruct.newBuilder()
                .addLong("GROUP", 1L)
                .addString("ID", "bar")
                .addString("VAL2", "other")
                .build();
        RelationalAssertions.assertThrowsSqlException(() -> statement.executeInsert("T2", row2, Options.NONE))
                .hasErrorCode(ErrorCode.UNIQUE_CONSTRAINT_VIOLATION);
        statement.executeInsert("T2", row2, Options.builder().withOption(Options.Name.REPLACE_ON_DUPLICATE_PK, true).build());

        KeySet key = new KeySet()
                .setKeyColumn("GROUP", 1)
                .setKeyColumn("ID", "bar");
        try (RelationalResultSet resultSet = statement.executeGet("T1", key, Options.NONE)) {
            ResultSetAssert.assertThat(resultSet)
                    .hasNextRow()
                    .isRowExactly(1L, "bar", "other")
                    .hasNoNextRow();
        }
    }

    @Test
    void queryFromSharedExtent() throws SQLException {
        final int groupCount = 4;
        final int idCount = 5;

        for (int group = 0; group < groupCount; group++) {
            for (int id = 0; id < idCount; id++) {
                var t1 = EmbeddedRelationalStruct.newBuilder()
                        .addLong("GROUP", group)
                        .addString("ID", "t1_" + id)
                        .addLong("VAL", id * 10)
                        .build();
                statement.executeInsert("T1", t1, Options.NONE);

                var t2 = EmbeddedRelationalStruct.newBuilder()
                        .addLong("GROUP", group)
                        .addString("ID", "t2_" + id)
                        .addString("VAL2", "val=" + (id * 10))
                        .build();
                statement.executeInsert("T2", t2, Options.NONE);
            }
        }

        for (int group = 0; group < groupCount; group++) {
            try (RelationalResultSet resultSet = statement.executeQuery(String.format(Locale.ROOT, "SELECT * FROM t1 WHERE group = %d", group))) {
                ResultSetAssert.assertThat(resultSet).containsRowsExactly(List.of(
                        new Object[]{group, "t1_0", 0},
                        new Object[]{group, "t1_1", 10},
                        new Object[]{group, "t1_2", 20},
                        new Object[]{group, "t1_3", 30},
                        new Object[]{group, "t1_4", 40}
                ));
            }
            try (RelationalResultSet resultSet = statement.executeQuery(String.format(Locale.ROOT, "SELECT * FROM t2 WHERE group = %d", group))) {
                ResultSetAssert.assertThat(resultSet).containsRowsExactly(List.of(
                        new Object[]{group, "t2_0", "val=0"},
                        new Object[]{group, "t2_1", "val=10"},
                        new Object[]{group, "t2_2", "val=20"},
                        new Object[]{group, "t2_3", "val=30"},
                        new Object[]{group, "t2_4", "val=40"}
                ));
            }
        }
    }
}
