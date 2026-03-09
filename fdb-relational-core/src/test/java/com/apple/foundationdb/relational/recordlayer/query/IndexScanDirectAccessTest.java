/*
 * IndexScanDirectAccessTest.java
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

package com.apple.foundationdb.relational.recordlayer.query;

import com.apple.foundationdb.relational.api.EmbeddedRelationalStruct;
import com.apple.foundationdb.relational.api.KeySet;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.RelationalConnection;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.RelationalStatement;
import com.apple.foundationdb.relational.api.RelationalStruct;
import com.apple.foundationdb.relational.recordlayer.EmbeddedRelationalExtension;
import com.apple.foundationdb.relational.utils.Ddl;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.net.URI;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies the columns returned by direct-access API methods with INDEX_HINT
 * for the schema used in IndexScanVsQueryBenchmark.
 */
public class IndexScanDirectAccessTest {

    private static final String SCHEMA_TEMPLATE =
            "CREATE TABLE \"IndexBenchTable\" (" +
            "\"id\" bigint, " +
            "\"category\" bigint, " +
            "\"label\" string, " +
            "PRIMARY KEY(\"id\")) " +
            "CREATE INDEX \"idx_label\" ON \"IndexBenchTable\"(\"label\") " +
            "CREATE INDEX \"idx_category\" ON \"IndexBenchTable\"(\"category\")";

    @RegisterExtension
    @Order(0)
    public final EmbeddedRelationalExtension relationalExtension = new EmbeddedRelationalExtension();

    @Test
    void indexApiColumnVerification() throws Exception {
        try (var ddl = Ddl.builder()
                .database(URI.create("/TEST/IndexScanDirectAccess"))
                .relationalExtension(relationalExtension)
                .schemaTemplate(SCHEMA_TEMPLATE)
                .build()) {
            RelationalConnection conn = ddl.setSchemaAndGetConnection();

            // Insert data: 5 categories × 10 rows each
            try (RelationalStatement stmt = conn.createStatement()) {
                List<RelationalStruct> rows = new ArrayList<>();
                for (int g = 0; g < 5; g++) {
                    for (int r = 0; r < 10; r++) {
                        rows.add(EmbeddedRelationalStruct.newBuilder()
                                .addLong("id", (long) g * 10 + r)
                                .addLong("category", g)
                                .addString("label", "lbl-" + g + "-" + r)
                                .build());
                    }
                }
                stmt.executeInsert("IndexBenchTable", rows);
            }

            Options indexHintCategory = Options.builder()
                    .withOption(Options.Name.INDEX_HINT, "idx_category").build();
            Options indexHintLabel = Options.builder()
                    .withOption(Options.Name.INDEX_HINT, "idx_label").build();

            // 1. executeScan with idx_category
            System.out.println("=== executeScan idx_category ===");
            try (RelationalStatement stmt = conn.createStatement()) {
                try (RelationalResultSet rs = stmt.executeScan("IndexBenchTable",
                        new KeySet().setKeyColumn("category", 2L),
                        indexHintCategory)) {
                    ResultSetMetaData meta = rs.getMetaData();
                    System.out.println("Column count: " + meta.getColumnCount());
                    for (int i = 1; i <= meta.getColumnCount(); i++) {
                        System.out.println("  col " + i + ": " + meta.getColumnName(i));
                    }
                    int count = 0;
                    while (rs.next()) {
                        count++;
                    }
                    System.out.println("Row count: " + count);
                    assertThat(count).as("expect 10 rows in category 2").isEqualTo(10);
                }
            }

            // 2. executeGet with idx_label
            System.out.println("=== executeGet idx_label ===");
            try (RelationalStatement stmt = conn.createStatement()) {
                try (RelationalResultSet rs = stmt.executeGet("IndexBenchTable",
                        new KeySet().setKeyColumn("label", "lbl-2-1"),
                        indexHintLabel)) {
                    ResultSetMetaData meta = rs.getMetaData();
                    System.out.println("Column count: " + meta.getColumnCount());
                    for (int i = 1; i <= meta.getColumnCount(); i++) {
                        System.out.println("  col " + i + ": " + meta.getColumnName(i));
                    }
                    assertThat(rs.next()).as("expect 1 row").isTrue();
                    System.out.println("Row found: " + rs.getString(1));
                }
            }

            // 3. executeScan with idx_label (equality, for comparison)
            System.out.println("=== executeScan idx_label ===");
            try (RelationalStatement stmt = conn.createStatement()) {
                try (RelationalResultSet rs = stmt.executeScan("IndexBenchTable",
                        new KeySet().setKeyColumn("label", "lbl-2-1"),
                        indexHintLabel)) {
                    ResultSetMetaData meta = rs.getMetaData();
                    System.out.println("Column count: " + meta.getColumnCount());
                    for (int i = 1; i <= meta.getColumnCount(); i++) {
                        System.out.println("  col " + i + ": " + meta.getColumnName(i));
                    }
                    int count = 0;
                    while (rs.next()) {
                        count++;
                    }
                    System.out.println("Row count: " + count);
                    assertThat(count).as("expect 1 row").isEqualTo(1);
                }
            }
        }
    }
}
