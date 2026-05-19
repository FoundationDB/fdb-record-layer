/*
 * IndexScanVsQueryBenchmark.java
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
 * Microbenchmark comparing the Direct Access API ({@code executeGet}, {@code executeScan} with
 * {@code INDEX_HINT}) against equivalent SQL queries for two secondary-index access patterns:
 * <ol>
 *   <li>Single-row equality lookup via a unique secondary index ({@code idx_label})</li>
 *   <li>Prefix range scan via a non-unique secondary index ({@code idx_category})</li>
 * </ol>
 *
 * <p>Table schema:
 * <pre>
 *   IndexBenchTable(id BIGINT PRIMARY KEY, category BIGINT, label STRING)
 *   INDEX idx_label    ON (label)     -- unique per row
 *   INDEX idx_category ON (category)  -- NUM_CATEGORIES distinct values, rowsPerGroup rows each
 * </pre>
 *
 * <p>The SQL benchmarks run with a warm plan cache so any measured overhead is
 * plan-cache-hit + query dispatch, not full planning cost.
 */
@Fork(1)
@Warmup(iterations = 1, time = 10)
@Measurement(iterations = 5, time = 10)
@BenchmarkMode({Mode.AverageTime, Mode.SampleTime})
@Threads(Threads.MAX)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@API(API.Status.EXPERIMENTAL)
public class IndexScanVsQueryBenchmark extends EmbeddedRelationalBenchmark {

    static final String schema = "bench";
    private static final String templateName = "IndexScanVsQueryTemplate";
    private static final String templateDefinition =
            "CREATE TABLE \"IndexBenchTable\" (" +
            "\"id\" bigint, " +
            "\"category\" bigint, " +
            "\"label\" string, " +
            "PRIMARY KEY(\"id\")) " +
            "CREATE INDEX \"idx_label\" ON \"IndexBenchTable\"(\"label\") " +
            "CREATE INDEX \"idx_category\" ON \"IndexBenchTable\"(\"category\")";

    static final URI dbUri = URI.create("/BENCHMARKS/IndexScanVsQuery");

    private static final int NUM_CATEGORIES = 5;
    // Fixed lookup targets — always category 2, row 1
    private static final long LOOKUP_CATEGORY = 2L;
    private static final String LOOKUP_LABEL = "lbl-2-1";  // requires rowsPerGroup >= 2

    /** Number of rows per category; controls prefix-scan result size. */
    @Param({"10", "100"})
    int rowsPerGroup;

    Driver driver = new Driver(templateName, templateDefinition, RelationalPlanCache.buildWithDefaults());
    Options indexHintLabel;
    Options indexHintCategory;

    @Setup(Level.Trial)
    public void trialUp() throws SQLException, RelationalException {
        indexHintLabel = Options.builder().withOption(Options.Name.INDEX_HINT, "idx_label").build();
        indexHintCategory = Options.builder().withOption(Options.Name.INDEX_HINT, "idx_category").build();
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
                List<RelationalStruct> rows = new ArrayList<>(NUM_CATEGORIES * rowsPerGroup);
                for (int g = 0; g < NUM_CATEGORIES; g++) {
                    for (int r = 0; r < rowsPerGroup; r++) {
                        rows.add(EmbeddedRelationalStruct.newBuilder()
                                .addLong("id", (long) g * rowsPerGroup + r)
                                .addLong("category", g)
                                .addString("label", "lbl-" + g + "-" + r)
                                .build());
                    }
                }
                stmt.executeInsert("IndexBenchTable", rows);
            }
        }
    }

    // ── Equality lookup via unique secondary index (idx_label) ───────────────

    /**
     * Direct API: index-only scan on {@code idx_label} for the matching label value.
     * {@code executeScan} on a secondary index returns only the declared index columns
     * ({@code label}), without fetching the full record from the primary store.
     */
    @Benchmark
    public void lookupByLabel_directAccess(Blackhole bh, ConnHolder connHolder) throws SQLException {
        try (RelationalStatement stmt = connHolder.connection.createStatement()
                .unwrap(RelationalStatement.class)) {
            try (RelationalResultSet rs = stmt.executeScan("IndexBenchTable",
                    new KeySet().setKeyColumn("label", LOOKUP_LABEL),
                    indexHintLabel)) {
                if (rs.next()) {
                    bh.consume(rs.getString("label"));  // index-only: only label is returned
                }
            }
        }
    }

    /**
     * SQL: {@code SELECT "label" WHERE label = ?} — projects only the index column to match
     * the index-only result from {@code lookupByLabel_directAccess}.
     * Planner should choose {@code ISCAN(idx_label [EQUALS ?])}.
     */
    @Benchmark
    public void lookupByLabel_sqlQuery(Blackhole bh, ConnHolder connHolder) throws SQLException {
        try (RelationalStatement stmt = connHolder.connection.createStatement()
                .unwrap(RelationalStatement.class)) {
            try (RelationalResultSet rs = stmt.executeQuery(
                    "SELECT \"label\" FROM \"IndexBenchTable\" WHERE \"label\" = '" + LOOKUP_LABEL + "'")) {
                if (rs.next()) {
                    bh.consume(rs.getString("label"));
                }
            }
        }
    }

    // ── Prefix scan via non-unique secondary index (idx_category) ────────────

    /**
     * Direct API: scans {@code idx_category} for all rows in the given category (rowsPerGroup rows).
     * {@code executeScan} on a secondary index is an index-only scan: it returns only the
     * declared index columns ({@code category}), not the full record.
     */
    @Benchmark
    public void scanByCategory_directAccess(Blackhole bh, ConnHolder connHolder) throws SQLException {
        try (RelationalStatement stmt = connHolder.connection.createStatement()
                .unwrap(RelationalStatement.class)) {
            try (RelationalResultSet rs = stmt.executeScan("IndexBenchTable",
                    new KeySet().setKeyColumn("category", LOOKUP_CATEGORY),
                    indexHintCategory)) {
                while (rs.next()) {
                    bh.consume(rs.getLong("category"));  // index-only scan: only category is returned
                }
            }
        }
    }

    /**
     * SQL: {@code SELECT "category" WHERE category = ?} — projects only the index column to match
     * the index-only result from {@code scanByCategory_directAccess}.
     * Planner should choose {@code ISCAN(idx_category [EQUALS ?])}.
     */
    @Benchmark
    public void scanByCategory_sqlQuery(Blackhole bh, ConnHolder connHolder) throws SQLException {
        try (RelationalStatement stmt = connHolder.connection.createStatement()
                .unwrap(RelationalStatement.class)) {
            try (RelationalResultSet rs = stmt.executeQuery(
                    "SELECT \"category\" FROM \"IndexBenchTable\" WHERE \"category\" = " + LOOKUP_CATEGORY)) {
                while (rs.next()) {
                    bh.consume(rs.getLong("category"));
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
                .include(IndexScanVsQueryBenchmark.class.getSimpleName())
                .forks(0)
                .build();
        new Runner(opt).run();
    }
}
