/*
 * DirectAccessVsQueryBenchmark.java
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

import com.apple.foundationdb.annotation.API;

import com.apple.foundationdb.relational.api.EmbeddedRelationalStruct;
import com.apple.foundationdb.relational.api.KeySet;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.RelationalConnection;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.RelationalStatement;
import com.apple.foundationdb.relational.api.RelationalStruct;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import com.apple.foundationdb.relational.recordlayer.query.cache.RelationalPlanCache;

import java.net.URI;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Microbenchmark comparing the Direct Access API ({@code executeGet}, {@code executeScan}) against
 * equivalent SQL queries ({@code executeQuery}) for three access patterns:
 * <ol>
 *   <li>Single-row PK lookup</li>
 *   <li>Prefix range scan (partial PK match)</li>
 *   <li>Full table scan</li>
 * </ol>
 *
 * <p>The SQL benchmarks run with a warm plan cache (JMH warmup phase saturates the cache), so
 * any overhead measured is plan-cache-hit + query dispatch, not full planning cost.
 *
 * <p>Table schema: {@code BenchTable(group_id BIGINT, row_id BIGINT, val STRING, PK(group_id, row_id))}.
 * Data is organized as {@code NUM_GROUPS} groups of {@code rowsPerGroup} rows each.
 */
@Fork(1)
@Warmup(iterations = 1, time = 10)
@Measurement(iterations = 5, time = 10)
@BenchmarkMode({Mode.AverageTime, Mode.SampleTime})
@Threads(Threads.MAX)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@API(API.Status.EXPERIMENTAL)
public class DirectAccessVsQueryBenchmark extends EmbeddedRelationalBenchmark {

    static final String schema = "bench";
    private static final String templateName = "DirectAccessVsQueryTemplate";
    private static final String templateDefinition =
            "CREATE TABLE \"BenchTable\" (" +
            "\"group_id\" bigint, " +
            "\"row_id\" bigint, " +
            "\"val\" string, " +
            "PRIMARY KEY(\"group_id\", \"row_id\"))";

    static final URI dbUri = URI.create("/BENCHMARKS/DirectAccessVsQuery");

    private static final int NUM_GROUPS = 5;
    // Fixed lookup target — always group 2, row 1
    private static final long LOOKUP_GROUP = 2L;
    private static final long LOOKUP_ROW = 1L;

    /** Number of rows per group; controls prefix-scan and full-scan result sizes. */
    @Param({"10", "100"})
    int rowsPerGroup;

    Driver driver = new Driver(templateName, templateDefinition, RelationalPlanCache.buildWithDefaults());

    @Setup(Level.Trial)
    public void trialUp() throws SQLException, RelationalException {
        driver.up();
        EmbeddedRelationalBenchmark.createDatabase(dbUri, templateName, schema);
        insertData();
    }

    @TearDown(Level.Trial)
    public void trialDown() throws SQLException, RelationalException {
        try {
            EmbeddedRelationalBenchmark.deleteDatabase(dbUri);
        } finally {
            driver.down();
        }
    }

    private void insertData() throws SQLException {
        try (RelationalConnection conn = DriverManager.getConnection("jdbc:embed:" + dbUri)
                .unwrap(RelationalConnection.class)) {
            conn.setSchema(schema);
            try (RelationalStatement stmt = conn.createStatement()) {
                List<RelationalStruct> rows = new ArrayList<>(NUM_GROUPS * rowsPerGroup);
                for (int g = 0; g < NUM_GROUPS; g++) {
                    for (int r = 0; r < rowsPerGroup; r++) {
                        rows.add(EmbeddedRelationalStruct.newBuilder()
                                .addLong("group_id", g)
                                .addLong("row_id", r)
                                .addString("val", "v-" + g + "-" + r)
                                .build());
                    }
                }
                stmt.executeInsert("BenchTable", rows);
            }
        }
    }

    // ── Single-row PK lookup ──────────────────────────────────────────────────

    @Benchmark
    public void get_directAccess(Blackhole bh, ConnHolder connHolder) throws SQLException {
        try (RelationalStatement stmt = connHolder.connection.createStatement()
                .unwrap(RelationalStatement.class)) {
            try (RelationalResultSet rs = stmt.executeGet("BenchTable",
                    new KeySet().setKeyColumn("group_id", LOOKUP_GROUP)
                                .setKeyColumn("row_id", LOOKUP_ROW),
                    Options.NONE)) {
                if (rs.next()) {
                    bh.consume(rs.getLong("group_id"));
                    bh.consume(rs.getString("val"));
                }
            }
        }
    }

    @Benchmark
    public void get_sqlQuery(Blackhole bh, ConnHolder connHolder) throws SQLException {
        try (RelationalStatement stmt = connHolder.connection.createStatement()
                .unwrap(RelationalStatement.class)) {
            try (RelationalResultSet rs = stmt.executeQuery(
                    "SELECT * FROM \"BenchTable\" WHERE \"group_id\" = " + LOOKUP_GROUP
                    + " AND \"row_id\" = " + LOOKUP_ROW)) {
                if (rs.next()) {
                    bh.consume(rs.getLong("group_id"));
                    bh.consume(rs.getString("val"));
                }
            }
        }
    }

    // ── Prefix range scan (one group = rowsPerGroup rows) ────────────────────

    @Benchmark
    public void scanPrefix_directAccess(Blackhole bh, ConnHolder connHolder) throws SQLException {
        try (RelationalStatement stmt = connHolder.connection.createStatement()
                .unwrap(RelationalStatement.class)) {
            try (RelationalResultSet rs = stmt.executeScan("BenchTable",
                    new KeySet().setKeyColumn("group_id", LOOKUP_GROUP),
                    Options.NONE)) {
                while (rs.next()) {
                    bh.consume(rs.getLong("group_id"));
                    bh.consume(rs.getString("val"));
                }
            }
        }
    }

    @Benchmark
    public void scanPrefix_sqlQuery(Blackhole bh, ConnHolder connHolder) throws SQLException {
        try (RelationalStatement stmt = connHolder.connection.createStatement()
                .unwrap(RelationalStatement.class)) {
            try (RelationalResultSet rs = stmt.executeQuery(
                    "SELECT * FROM \"BenchTable\" WHERE \"group_id\" = " + LOOKUP_GROUP)) {
                while (rs.next()) {
                    bh.consume(rs.getLong("group_id"));
                    bh.consume(rs.getString("val"));
                }
            }
        }
    }

    // ── Full table scan (NUM_GROUPS * rowsPerGroup rows) ─────────────────────

    @Benchmark
    public void fullScan_directAccess(Blackhole bh, ConnHolder connHolder) throws SQLException {
        try (RelationalStatement stmt = connHolder.connection.createStatement()
                .unwrap(RelationalStatement.class)) {
            try (RelationalResultSet rs = stmt.executeScan("BenchTable", new KeySet(), Options.NONE)) {
                while (rs.next()) {
                    bh.consume(rs.getLong("group_id"));
                    bh.consume(rs.getString("val"));
                }
            }
        }
    }

    @Benchmark
    public void fullScan_sqlQuery(Blackhole bh, ConnHolder connHolder) throws SQLException {
        try (RelationalStatement stmt = connHolder.connection.createStatement()
                .unwrap(RelationalStatement.class)) {
            try (RelationalResultSet rs = stmt.executeQuery("SELECT * FROM \"BenchTable\"")) {
                while (rs.next()) {
                    bh.consume(rs.getLong("group_id"));
                    bh.consume(rs.getString("val"));
                }
            }
        }
    }

    // ── Connection holder (one connection per JMH thread) ────────────────────

    @State(Scope.Thread)
    public static class ConnHolder {
        Connection connection;

        @Setup
        public void init() throws SQLException {
            connection = DriverManager.getConnection("jdbc:embed:" + dbUri);
            connection.setSchema(schema);
        }

        @TearDown
        public void stop() throws SQLException {
            connection.close();
        }
    }

    public static void main(String[] args) throws RunnerException {
        org.openjdk.jmh.runner.options.Options opt = new OptionsBuilder()
                .include(DirectAccessVsQueryBenchmark.class.getSimpleName())
                .forks(0)
                .build();
        new Runner(opt).run();
    }
}
