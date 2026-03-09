/*
 * IndexBenchExplainTest.java
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

import com.apple.foundationdb.relational.api.RelationalConnection;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.recordlayer.EmbeddedRelationalExtension;
import com.apple.foundationdb.relational.utils.Ddl;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.net.URI;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Prints EXPLAIN plans for the SQL queries used in IndexScanVsQueryBenchmark.
 * Helps diagnose why secondary index SQL overhead (~4-5ms) is much larger than
 * primary scan SQL overhead (~1ms).
 */
public class IndexBenchExplainTest {

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
    void explainIndexBenchQueries() throws Exception {
        try (var ddl = Ddl.builder()
                .database(URI.create("/TEST/IndexBenchExplain"))
                .relationalExtension(relationalExtension)
                .schemaTemplate(SCHEMA_TEMPLATE)
                .build()) {
            RelationalConnection conn = ddl.setSchemaAndGetConnection();

            String[] queries = {
                // The benchmark queries
                "SELECT \"label\" FROM \"IndexBenchTable\" WHERE \"label\" = 'lbl-2-1'",
                "SELECT \"category\" FROM \"IndexBenchTable\" WHERE \"category\" = 2",
                // For comparison: PK-based queries (same structure as BenchTable)
                "SELECT * FROM \"IndexBenchTable\" WHERE \"id\" = 21",
                // Full scan baseline
                "SELECT * FROM \"IndexBenchTable\"",
            };

            System.out.println("=== IndexBenchTable EXPLAIN results ===");
            for (String q : queries) {
                try (var ps = conn.prepareStatement("EXPLAIN " + q);
                        RelationalResultSet rs = ps.executeQuery()) {
                    assertThat(rs.next()).as("EXPLAIN returned at least one row").isTrue();
                    String plan = rs.getString("PLAN");
                    System.out.println("Query : " + q);
                    System.out.println("Plan  : " + plan);
                    System.out.println();
                }
            }
        }
    }
}
