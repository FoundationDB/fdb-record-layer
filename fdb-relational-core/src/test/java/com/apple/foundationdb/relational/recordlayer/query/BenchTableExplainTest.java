/*
 * BenchTableExplainTest.java
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
 * Verifies the query plans chosen by the SQL planner for the three access patterns exercised
 * by DirectAccessVsQueryBenchmark: PK point-lookup, PK prefix scan, and full table scan.
 *
 * The benchmark showed that get_sqlQuery and scanPrefix_sqlQuery had the same latency as
 * fullScan_sqlQuery, suggesting the planner was not generating PK-based access plans.
 * This test prints the EXPLAIN output to confirm (or refute) that hypothesis.
 */
public class BenchTableExplainTest {

    private static final String SCHEMA_TEMPLATE =
            "CREATE TABLE \"BenchTable\" (" +
            "\"group_id\" bigint, " +
            "\"row_id\" bigint, " +
            "\"val\" string, " +
            "PRIMARY KEY(\"group_id\", \"row_id\"))";

    @RegisterExtension
    @Order(0)
    public final EmbeddedRelationalExtension relationalExtension = new EmbeddedRelationalExtension();

    @Test
    void explainBenchTableQueries() throws Exception {
        try (var ddl = Ddl.builder()
                .database(URI.create("/TEST/BenchTableExplain"))
                .relationalExtension(relationalExtension)
                .schemaTemplate(SCHEMA_TEMPLATE)
                .build()) {
            RelationalConnection conn = ddl.setSchemaAndGetConnection();

            // Insert a small dataset — enough for the planner to have real data to reason about
            try (var stmt = conn.createStatement()) {
                for (int g = 0; g < 5; g++) {
                    for (int r = 0; r < 10; r++) {
                        stmt.executeUpdate(
                                "INSERT INTO \"BenchTable\" (\"group_id\", \"row_id\", \"val\") VALUES ("
                                + g + ", " + r + ", 'v-" + g + "-" + r + "')");
                    }
                }
            }

            String[] queries = {
                // PK point-lookup — full composite PK predicate
                "SELECT * FROM \"BenchTable\" WHERE \"group_id\" = 2 AND \"row_id\" = 1",
                // PK prefix scan — first component only
                "SELECT * FROM \"BenchTable\" WHERE \"group_id\" = 2",
                // Full table scan — no predicate
                "SELECT * FROM \"BenchTable\""
            };

            System.out.println("=== BenchTable EXPLAIN results ===");
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
