/*
 * ManyDatabasesBenchmark.java
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

import com.apple.foundationdb.record.Restaurant;
import com.apple.foundationdb.relational.api.QueryProperties;
import com.apple.foundationdb.relational.api.Queryable;
import com.apple.foundationdb.relational.api.Relational;
import com.apple.foundationdb.relational.api.RelationalConnection;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.RelationalStatement;
import com.apple.foundationdb.relational.api.WhereClause;
import com.apple.foundationdb.relational.api.catalog.DatabaseTemplate;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.recordlayer.query.RelationalQuery;
import com.apple.foundationdb.relational.recordlayer.query.ValueComparisonClause;
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
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.net.URI;
import java.sql.SQLException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@State(Scope.Benchmark)
@Fork(1)
@Warmup(iterations = 1, time = 100)
@Measurement(iterations = 5, time = 100)
@BenchmarkMode({Mode.AverageTime, Mode.SampleTime})
@Threads(Threads.MAX)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class ManyDatabasesBenchmark extends EmbeddedRelationalBenchmark {
    final String schema = "schema";
    final int dbSize = 1000;
    final List<Restaurant.RestaurantRecord> records = IntStream.range(1, dbSize + 1).mapToObj(this::newRestaurantRecord).collect(Collectors.toList());

    @Param({"1", "10", "100", "1000", "10000"})
    int dbCount;

    @Setup(Level.Trial)
    public void setUp(Driver driver, BenchmarkScopedDatabases databases) throws RelationalException {
        System.out.printf("Creating %s databases...%n", dbCount);
        long startTime = System.nanoTime();
        databases.createMultipleDatabases(driver,
                DatabaseTemplate.newBuilder()
                        .withSchema(schema, restaurantRecord)
                        .build(),
                dbCount,
                this::dbName,
                this::populateDatabase);
        long endTime = System.nanoTime();
        System.out.printf("Done in %s %n.", Duration.ofNanos(endTime - startTime));
    }

    @Benchmark
    public void singleRead(Blackhole bh) throws SQLException, RelationalException {
        long dbId = ThreadLocalRandom.current().nextInt(0, dbCount);
        try (RelationalConnection dbConn = Relational.connect(getUri(dbName(dbId), true), com.apple.foundationdb.relational.api.Options.create())) {
            dbConn.setSchema(schema);
            long restId = ThreadLocalRandom.current().nextInt(1, dbSize + 1);
            try (RelationalStatement stmt = dbConn.createStatement()) {
                WhereClause clause = new ValueComparisonClause("rest_no", ValueComparisonClause.ComparisonType.EQUALS, restId);
                Queryable query = new RelationalQuery(restaurantRecord, schema, List.of("rest_no", "name"), clause, false, QueryProperties.DEFAULT);
                RelationalResultSet resultSet = stmt.executeQuery(query, com.apple.foundationdb.relational.api.Options.create());
                resultSet.next();
                bh.consume(resultSet.getLong("rest_no"));
                bh.consume(resultSet.getString("name"));
            }
        }
    }

    private void populateDatabase(URI uri) {
        try (RelationalConnection dbConn = Relational.connect(uri, com.apple.foundationdb.relational.api.Options.create())) {
            dbConn.setSchema(schema);
            try (RelationalStatement stmt = dbConn.createStatement()) {
                stmt.executeInsert(
                        restaurantRecord,
                        records,
                        com.apple.foundationdb.relational.api.Options.create());
            }
        } catch (SQLException e) {
            throw ExceptionUtil.toRelationalException(e).toUncheckedWrappedException();
        } catch (RelationalException e) {
            throw e.toUncheckedWrappedException();
        }
    }

    private Restaurant.RestaurantRecord newRestaurantRecord(int recordId) {
        return Restaurant.RestaurantRecord.newBuilder()
                .setRestNo(recordId)
                .setName("restaurant #" + recordId)
                .build();
    }

    private String dbName(long dbId) {
        return "/ManyDatabaseBenchmark" + dbId;
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(ManyDatabasesBenchmark.class.getSimpleName())
                .forks(1)
                .build();

        new Runner(opt).run();
    }
}
