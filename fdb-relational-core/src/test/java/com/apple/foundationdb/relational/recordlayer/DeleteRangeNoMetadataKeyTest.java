/*
 * DeleteRangeNoMetadataKeyTest.java
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
import com.apple.foundationdb.relational.api.RelationalStatement;
import com.apple.foundationdb.relational.api.RelationalStruct;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.utils.Ddl;
import com.apple.foundationdb.relational.utils.ResultSetAssert;
import com.apple.foundationdb.relational.utils.SchemaTemplateRule;
import com.apple.foundationdb.relational.utils.SimpleDatabaseRule;
import com.apple.foundationdb.relational.utils.RelationalAssertions;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.net.URI;
import java.sql.SQLException;
import java.util.List;

/**
 * Basic tests for {@link RelationalStatement#executeDeleteRange(String, KeySet, Options)} when the schema template
 * has records that share a single primary key extent.
 */
public class DeleteRangeNoMetadataKeyTest {
    private static final String SCHEMA_TEMPLATE = " CREATE TABLE t1 (id bigint, a string, b string, c string, d string, PRIMARY KEY(id, a, b)) " +
            "CREATE TABLE t2 (id bigint, a string, b string, e bigint, f boolean, PRIMARY KEY(id, a, b))";

    @RegisterExtension
    @Order(0)
    public final EmbeddedRelationalExtension relationalExtension = new EmbeddedRelationalExtension();

    @RegisterExtension
    @Order(1)
    public final SimpleDatabaseRule database = new SimpleDatabaseRule(DeleteRangeNoMetadataKeyTest.class, SCHEMA_TEMPLATE, new SchemaTemplateRule.SchemaTemplateOptions(true, true));

    @RegisterExtension
    @Order(2)
    public final RelationalConnectionRule connection = new RelationalConnectionRule(database::getConnectionUri)
            .withOptions(Options.NONE)
            .withSchema("TEST_SCHEMA");

    @RegisterExtension
    @Order(3)
    public final RelationalStatementRule statement = new RelationalStatementRule(connection);

    @BeforeEach
    void insertData() throws SQLException {
        insertData(statement);
    }

    private void insertData(RelationalStatement stmt) throws SQLException {
        for (int i = 0; i < 12; i++) {
            RelationalStruct toInsert = EmbeddedRelationalStruct.newBuilder()
                    .addLong("ID", i % 2)
                    .addString("A", Integer.toString(i % 3))
                    .addString("B", Integer.toString(i % 4))
                    .addString("C", Integer.toString(i * 10))
                    .addString("D", Integer.toString(i * 100))
                    .build();
            stmt.executeInsert("T1", toInsert);

            RelationalStruct toInsert2 = EmbeddedRelationalStruct.newBuilder()
                    .addLong("ID", i % 2)
                    .addString("A", Integer.toString(i % 3))
                    .addString("B", Integer.toString(i % 4 + 100))
                    .addLong("E", i * 10)
                    .addBoolean("F", i % 2 == 0)
                    .build();
            stmt.executeInsert("T2", toInsert2);
        }
    }

    @Test
    void deleteNoKeyFailsBecauseNoRecordTypeKeys() throws Exception {
        KeySet toDelete = new KeySet();
        RelationalAssertions.assertThrowsSqlException(() -> statement.executeDeleteRange("T1", toDelete, Options.NONE))
                .hasErrorCode(ErrorCode.INVALID_PARAMETER)
                .hasMessageContaining("Delete range with empty key range is only supported on tables with RecordTypeKeys");
    }

    @Test
    void deletePartialKey() throws Exception {
        KeySet toDelete = new KeySet()
                .setKeyColumn("ID", 0);
        statement.executeDeleteRange("T1", toDelete, Options.NONE);

        try (RelationalResultSet resultSet = statement.executeQuery("SELECT * FROM t1")) {
            ResultSetAssert.assertThat(resultSet)
                    .containsRowsExactly(List.of(
                            new Object[]{1, "1", "1", "10", "100"},
                            new Object[]{1, "0", "3", "30", "300"},
                            new Object[]{1, "2", "1", "50", "500"},
                            new Object[]{1, "1", "3", "70", "700"},
                            new Object[]{1, "0", "1", "90", "900"},
                            new Object[]{1, "2", "3", "110", "1100"}
                    ));
        }

        try (RelationalResultSet resultSet = statement.executeQuery("SELECT * FROM t2")) {
            ResultSetAssert.assertThat(resultSet)
                    .containsRowsExactly(List.of(
                            new Object[]{1, "1", "101", 10, false},
                            new Object[]{1, "0", "103", 30, false},
                            new Object[]{1, "2", "101", 50, false},
                            new Object[]{1, "1", "103", 70, false},
                            new Object[]{1, "0", "101", 90, false},
                            new Object[]{1, "2", "103", 110, false}
                    ));
        }
    }

    @Test
    void deleteLongerKey() throws Exception {
        KeySet toDelete = new KeySet()
                .setKeyColumn("ID", 0)
                .setKeyColumn("A", "0");
        statement.executeDeleteRange("T1", toDelete, Options.NONE);

        try (RelationalResultSet resultSet = statement.executeQuery("SELECT * FROM t1")) {
            ResultSetAssert.assertThat(resultSet)
                    .containsRowsExactly(List.of(
                            new Object[]{1, "1", "1", "10", "100"},
                            new Object[]{0, "2", "2", "20", "200"},
                            new Object[]{1, "0", "3", "30", "300"},
                            new Object[]{0, "1", "0", "40", "400"},
                            new Object[]{1, "2", "1", "50", "500"},
                            new Object[]{1, "1", "3", "70", "700"},
                            new Object[]{0, "2", "0", "80", "800"},
                            new Object[]{1, "0", "1", "90", "900"},
                            new Object[]{0, "1", "2", "100", "1000"},
                            new Object[]{1, "2", "3", "110", "1100"}
                    ));
        }

        try (RelationalResultSet resultSet = statement.executeQuery("SELECT * FROM t2")) {
            ResultSetAssert.assertThat(resultSet)
                    .containsRowsExactly(List.of(
                            new Object[]{1, "1", "101", 10, false},
                            new Object[]{0, "2", "102", 20, true},
                            new Object[]{1, "0", "103", 30, false},
                            new Object[]{0, "1", "100", 40, true},
                            new Object[]{1, "2", "101", 50, false},
                            new Object[]{1, "1", "103", 70, false},
                            new Object[]{0, "2", "100", 80, true},
                            new Object[]{1, "0", "101", 90, false},
                            new Object[]{0, "1", "102", 100, true},
                            new Object[]{1, "2", "103", 110, false}
                    ));
        }
    }

    @Test
    void deleteFullKey() throws Exception {
        KeySet toDelete = new KeySet()
                .setKeyColumn("ID", 0)
                .setKeyColumn("A", "0")
                .setKeyColumn("B", "0");
        statement.executeDeleteRange("T1", toDelete, Options.NONE);

        try (RelationalResultSet resultSet = statement.executeQuery("SELECT * FROM t1")) {
            ResultSetAssert.assertThat(resultSet)
                    .containsRowsExactly(List.of(
                            new Object[]{1, "1", "1", "10", "100"},
                            new Object[]{0, "2", "2", "20", "200"},
                            new Object[]{1, "0", "3", "30", "300"},
                            new Object[]{0, "1", "0", "40", "400"},
                            new Object[]{1, "2", "1", "50", "500"},
                            new Object[]{0, "0", "2", "60", "600"},
                            new Object[]{1, "1", "3", "70", "700"},
                            new Object[]{0, "2", "0", "80", "800"},
                            new Object[]{1, "0", "1", "90", "900"},
                            new Object[]{0, "1", "2", "100", "1000"},
                            new Object[]{1, "2", "3", "110", "1100"}
                    ));
        }

        try (RelationalResultSet resultSet = statement.executeQuery("SELECT * FROM t2")) {
            ResultSetAssert.assertThat(resultSet)
                    .containsRowsExactly(List.of(
                            new Object[]{0, "0", "100", 0, true},
                            new Object[]{1, "1", "101", 10, false},
                            new Object[]{0, "2", "102", 20, true},
                            new Object[]{1, "0", "103", 30, false},
                            new Object[]{0, "1", "100", 40, true},
                            new Object[]{1, "2", "101", 50, false},
                            new Object[]{0, "0", "102", 60, true},
                            new Object[]{1, "1", "103", 70, false},
                            new Object[]{0, "2", "100", 80, true},
                            new Object[]{1, "0", "101", 90, false},
                            new Object[]{0, "1", "102", 100, true},
                            new Object[]{1, "2", "103", 110, false}
                    ));
        }
    }

    @Test
    void deleteNothing() throws Exception {
        KeySet toDelete = new KeySet()
                .setKeyColumn("ID", 19);
        statement.executeDeleteRange("T1", toDelete, Options.NONE);

        try (RelationalResultSet resultSet = statement.executeQuery("SELECT * FROM t1")) {
            ResultSetAssert.assertThat(resultSet)
                    .containsRowsExactly(List.of(
                            new Object[]{0, "0", "0", "0", "0"},
                            new Object[]{1, "1", "1", "10", "100"},
                            new Object[]{0, "2", "2", "20", "200"},
                            new Object[]{1, "0", "3", "30", "300"},
                            new Object[]{0, "1", "0", "40", "400"},
                            new Object[]{1, "2", "1", "50", "500"},
                            new Object[]{0, "0", "2", "60", "600"},
                            new Object[]{1, "1", "3", "70", "700"},
                            new Object[]{0, "2", "0", "80", "800"},
                            new Object[]{1, "0", "1", "90", "900"},
                            new Object[]{0, "1", "2", "100", "1000"},
                            new Object[]{1, "2", "3", "110", "1100"}
                    ));
        }

        try (RelationalResultSet resultSet = statement.executeQuery("SELECT * FROM t2")) {
            ResultSetAssert.assertThat(resultSet)
                    .containsRowsExactly(List.of(
                            new Object[]{0, "0", "100", 0, true},
                            new Object[]{1, "1", "101", 10, false},
                            new Object[]{0, "2", "102", 20, true},
                            new Object[]{1, "0", "103", 30, false},
                            new Object[]{0, "1", "100", 40, true},
                            new Object[]{1, "2", "101", 50, false},
                            new Object[]{0, "0", "102", 60, true},
                            new Object[]{1, "1", "103", 70, false},
                            new Object[]{0, "2", "100", 80, true},
                            new Object[]{1, "0", "101", 90, false},
                            new Object[]{0, "1", "102", 100, true},
                            new Object[]{1, "2", "103", 110, false}
                    ));
        }
    }

    @Test
    void deleteUsingUnknownColumn() throws Exception {
        KeySet toDelete = new KeySet()
                .setKeyColumn("ID", 0)
                .setKeyColumn("whatColumn", "0");
        RelationalAssertions.assertThrowsSqlException(() -> statement.executeDeleteRange("T1", toDelete, Options.NONE))
                .hasErrorCode(ErrorCode.INVALID_PARAMETER)
                .hasMessageContaining("Unknown keys for primary");
    }

    @Test
    void deleteMissingKeyColumn() throws Exception {
        KeySet toDelete = new KeySet()
                .setKeyColumn("ID", 0)
                .setKeyColumn("B", "0");
        RelationalAssertions.assertThrowsSqlException(() -> statement.executeDeleteRange("T1", toDelete, Options.NONE))
                .hasErrorCode(ErrorCode.INVALID_PARAMETER)
                .hasMessageContaining("missing key at position");
    }

    @Test
    void deleteUsingNonKeyColumns() throws Exception {
        KeySet toDelete = new KeySet()
                .setKeyColumn("ID", 0)
                .setKeyColumn("A", "0")
                .setKeyColumn("B", "0")
                .setKeyColumn("C", "0");
        RelationalAssertions.assertThrowsSqlException(() -> statement.executeDeleteRange("T1", toDelete, Options.NONE))
                .hasErrorCode(ErrorCode.INVALID_PARAMETER)
                .hasMessageContaining("Unknown keys for primary key");
    }

    @Test
    void testDeleteWithIndexWithSamePrefix() throws Exception {
        final String schemaTemplateSuffix = " CREATE INDEX idx1 ON t1(id, a) " +
                "CREATE INDEX idx2 ON t2(id, a, e) INCLUDE(f)";
        try (var ddl = getDdl(schemaTemplateSuffix)) {
            try (var stmt = ddl.setSchemaAndGetConnection().createStatement()) {
                insertData(stmt);
                KeySet toDelete = new KeySet()
                        .setKeyColumn("ID", 0)
                        .setKeyColumn("A", "0");
                stmt.executeDeleteRange("T1", toDelete, Options.NONE);

                try (RelationalResultSet resultSet = stmt.executeQuery("SELECT * FROM t1")) {
                    ResultSetAssert.assertThat(resultSet)
                            .containsRowsExactly(List.of(
                                    new Object[]{1, "1", "1", "10", "100"},
                                    new Object[]{0, "2", "2", "20", "200"},
                                    new Object[]{1, "0", "3", "30", "300"},
                                    new Object[]{0, "1", "0", "40", "400"},
                                    new Object[]{1, "2", "1", "50", "500"},
                                    new Object[]{1, "1", "3", "70", "700"},
                                    new Object[]{0, "2", "0", "80", "800"},
                                    new Object[]{1, "0", "1", "90", "900"},
                                    new Object[]{0, "1", "2", "100", "1000"},
                                    new Object[]{1, "2", "3", "110", "1100"}
                            ));
                }

                // Note that because this is able to use deleteRecordsWhere under the hood, the rows from
                // type t2 that match the delete predicate are deleted
                try (RelationalResultSet resultSet = stmt.executeQuery("SELECT * FROM t2")) {
                    ResultSetAssert.assertThat(resultSet)
                            .containsRowsExactly(List.of(
                                    new Object[]{1, "1", "101", 10, false},
                                    new Object[]{0, "2", "102", 20, true},
                                    new Object[]{1, "0", "103", 30, false},
                                    new Object[]{0, "1", "100", 40, true},
                                    new Object[]{1, "2", "101", 50, false},
                                    new Object[]{1, "1", "103", 70, false},
                                    new Object[]{0, "2", "100", 80, true},
                                    new Object[]{1, "0", "101", 90, false},
                                    new Object[]{0, "1", "102", 100, true},
                                    new Object[]{1, "2", "103", 110, false}
                            ));
                }
            }
        }
    }

    @Test
    void testDeleteWithIndexSamePrefixButDeleteGoesBeyondIndex() throws Exception {
        final String schemaTemplateSuffix = " CREATE INDEX idx1 as select id from t1";
        try (var ddl = getDdl(schemaTemplateSuffix)) {
            try (var stmt = ddl.setSchemaAndGetConnection().createStatement()) {
                insertData(stmt);
                KeySet toDelete = new KeySet()
                        .setKeyColumn("ID", 0)
                        .setKeyColumn("A", "0");
                stmt.executeDeleteRange("T1", toDelete, Options.NONE);

                try (RelationalResultSet resultSet = stmt.executeQuery("SELECT * FROM t1")) {
                    ResultSetAssert.assertThat(resultSet)
                            .containsRowsExactly(List.of(
                                    new Object[]{1, "1", "1", "10", "100"},
                                    new Object[]{0, "2", "2", "20", "200"},
                                    new Object[]{1, "0", "3", "30", "300"},
                                    new Object[]{0, "1", "0", "40", "400"},
                                    new Object[]{1, "2", "1", "50", "500"},
                                    new Object[]{1, "1", "3", "70", "700"},
                                    new Object[]{0, "2", "0", "80", "800"},
                                    new Object[]{1, "0", "1", "90", "900"},
                                    new Object[]{0, "1", "2", "100", "1000"},
                                    new Object[]{1, "2", "3", "110", "1100"}
                            ));
                }

                // Note that because this is *not* able to use deleteRecordsWhere under the hood, the rows from
                // type t2 that match the delete predicate are *not* deleted
                try (RelationalResultSet resultSet = stmt.executeQuery("SELECT * FROM t2")) {
                    ResultSetAssert.assertThat(resultSet)
                            .containsRowsExactly(List.of(
                                    new Object[]{0, "0", "100", 0, true},
                                    new Object[]{1, "1", "101", 10, false},
                                    new Object[]{0, "2", "102", 20, true},
                                    new Object[]{1, "0", "103", 30, false},
                                    new Object[]{0, "1", "100", 40, true},
                                    new Object[]{1, "2", "101", 50, false},
                                    new Object[]{0, "0", "102", 60, true},
                                    new Object[]{1, "1", "103", 70, false},
                                    new Object[]{0, "2", "100", 80, true},
                                    new Object[]{1, "0", "101", 90, false},
                                    new Object[]{0, "1", "102", 100, true},
                                    new Object[]{1, "2", "103", 110, false}
                            ));
                }
            }
        }
    }

    private Ddl getDdl(String templateSuffix) throws Exception {
        return Ddl.builder()
                .database(URI.create("/TEST/QT"))
                .relationalExtension(relationalExtension)
                .schemaTemplate(SCHEMA_TEMPLATE + templateSuffix)
                .schemaTemplateOptions(new SchemaTemplateRule.SchemaTemplateOptions(true, true))
                .build();
    }
}
