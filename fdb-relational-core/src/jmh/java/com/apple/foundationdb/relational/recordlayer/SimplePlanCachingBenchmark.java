/*
 * SimplePlanCachingBenchmark.java
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

import com.apple.foundationdb.annotation.API;

import com.apple.foundationdb.relational.api.EmbeddedRelationalStruct;
import com.apple.foundationdb.relational.api.RelationalConnection;
import com.apple.foundationdb.relational.api.RelationalStatement;
import com.apple.foundationdb.relational.api.RelationalStruct;
import com.apple.foundationdb.relational.api.catalog.DatabaseTemplate;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.recordlayer.query.cache.RelationalPlanCache;
import com.apple.foundationdb.relational.recordlayer.util.ExceptionUtil;
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
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.net.URI;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * A microbenchmark to evaluate the performance impact of a plan cache.
 * <p>
 * This performs a simple query repeatedly, and evaluates the overall cost, and relies on subclasses
 * to provide different PlanCache implementations to determine performance advantages
 */
@State(Scope.Benchmark)
@Fork(1)
@Warmup(iterations = 1, time = 10)
@Measurement(iterations = 5, time = 10)
@BenchmarkMode({Mode.AverageTime, Mode.SampleTime})
@Threads(Threads.MAX)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@API(API.Status.EXPERIMENTAL)
public class SimplePlanCachingBenchmark extends EmbeddedRelationalBenchmark {
    static final String dbName = "/BENCHMARKS/SimplePlanCaching";

    static final String cacheSchema = "cacheSchema";

    final int dbSize = 5;

    @Param({"NONE", "MULTI-STAGED"})
    String cacheType;

    int dbCount = 1;

    Driver driver;

    BenchmarkScopedDatabases databases = new BenchmarkScopedDatabases();

    @Setup(Level.Trial)
    public void trialUp() throws SQLException, RelationalException {
        driver = new Driver(getPlanCache());

        driver.up();

        databases.createMultipleDatabases(
                DatabaseTemplate.newBuilder()
                        .withSchema(cacheSchema, schemaTemplateName)
                        .build(),
                dbCount,
                this::dbName,
                this::populateDatabase);
    }

    @TearDown(Level.Trial)
    public void trialDown() throws RelationalException {
        databases.deleteDatabases();
        driver.down();
    }

    @Benchmark
    public void repeatedRead(Blackhole bh) throws SQLException {
        long dbId = ThreadLocalRandom.current().nextInt(0, dbCount);
        try (final var dbConn = DriverManager.getConnection(getUri(dbName(dbId), true).toString())) {
            dbConn.setSchema(cacheSchema);
            long restId = ThreadLocalRandom.current().nextInt(1, dbSize + 1);
            try (final var stmt = dbConn.createStatement();
                    ResultSet resultSet = stmt.executeQuery("EXPLAIN SELECT * from \"RestaurantRecord\" where \"rest_no\" = " + restId)) {
                resultSet.next();
                bh.consume(resultSet.getString(1));
            }
        }
    }

    private RelationalPlanCache getPlanCache() {
        switch (cacheType) {
            case "NONE":
                return null;
            case "MULTI-STAGED":
                return RelationalPlanCache.buildWithDefaults();
            default:
                throw new IllegalArgumentException("Unexpected cache name: " + cacheType);
        }
    }

    private String dbName(long dbId) {
        return dbName + dbId;
    }

    private void populateDatabase(URI uri) {
        try (RelationalConnection dbConn = DriverManager.getConnection(uri.toString()).unwrap(RelationalConnection.class)) {
            dbConn.setSchema(cacheSchema);
            try (RelationalStatement stmt = dbConn.createStatement()) {
                stmt.executeInsert(
                        restaurantRecordTable,
                        createRecords());
            }
        } catch (SQLException e) {
            throw ExceptionUtil.toRelationalException(e).toUncheckedWrappedException();
        }
    }

    private List<RelationalStruct> createRecords() {
        return IntStream.range(1, dbSize + 1).mapToObj(this::newRestaurantRecord).collect(Collectors.toList());
    }

    private RelationalStruct newRestaurantRecord(int recordId) {
        try {
            return EmbeddedRelationalStruct.newBuilder()
                    .addLong("rest_no", recordId)
                    .addString("name", "restaurant #" + recordId)
                    .build();
        } catch (SQLException e) {
            throw ExceptionUtil.toRelationalException(e).toUncheckedWrappedException();
        }
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .shouldFailOnError(true)
                .include(SimplePlanCachingBenchmark.class.getSimpleName())
                .forks(1)
                .threads(1)
                .build();

        new Runner(opt).run();
    }
}
