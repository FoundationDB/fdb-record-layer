/*
 * BasicBenchmark.java
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
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
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

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@State(Scope.Benchmark)
@Fork(2)
@Warmup(iterations = 3, time = 10)
@Measurement(iterations = 5, time = 10)
@BenchmarkMode({Mode.AverageTime, Mode.SampleTime})
@Threads(Threads.MAX)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@API(API.Status.EXPERIMENTAL)
public class BasicBenchmark extends EmbeddedRelationalBenchmark {
    static final String dbName = "/BENCHMARKS/BasicBenchmark";
    static final String singleReadSchema = "singleReadSchema";
    static final String singleWriteSchema = "singleWriteSchema";
    AtomicInteger restNo = new AtomicInteger();

    Driver driver = new Driver();

    @Setup(Level.Trial)
    public void trialUp() throws SQLException, RelationalException {
        driver.up();
    }

    @TearDown(Level.Trial)
    public void trialDown() throws RelationalException {
        driver.down();
    }

    @Setup(Level.Iteration)
    public void setUp(ThreadScopedDatabases databases) throws RelationalException, SQLException {
        databases.createDatabase(
                DatabaseTemplate.newBuilder()
                        .withSchema(singleReadSchema, schemaTemplateName)
                        .withSchema(singleWriteSchema, schemaTemplateName)
                        .build(),
                dbName);

        try (RelationalConnection dbConn = DriverManager.getConnection(getUri(dbName, true).toString()).unwrap(RelationalConnection.class)) {
            dbConn.setSchema(singleReadSchema);
            try (RelationalStatement stmt = dbConn.createStatement()) {
                stmt.executeInsert(restaurantRecordTable, newRestaurantRecord(42));
            }
        }
    }

    @Benchmark
    public void singleWrite(Blackhole bh) throws SQLException {
        try (RelationalConnection dbConn = DriverManager.getConnection(getUri(dbName, true).toString()).unwrap(RelationalConnection.class)) {
            dbConn.setSchema(singleWriteSchema);
            try (RelationalStatement stmt = dbConn.createStatement()) {
                bh.consume(stmt.executeInsert(restaurantRecordTable, newRestaurantRecord()));
            }
        }
    }

    @Benchmark
    public void singlePkRead(Blackhole bh) throws SQLException {
        try (RelationalConnection dbConn = DriverManager.getConnection(getUri(dbName, true).toString()).unwrap(RelationalConnection.class)) {
            dbConn.setSchema(singleReadSchema);
            try (RelationalStatement stmt = dbConn.createStatement();
                    ResultSet resultSet = stmt.executeQuery("SELECT * FROM \"RestaurantRecord\" WHERE \"rest_no\" = 42")) {

                resultSet.next();
                bh.consume(resultSet.getLong("rest_no"));
            }
        }
    }

    @Benchmark
    public void singleNonPkRead(Blackhole bh) throws SQLException {
        try (RelationalConnection dbConn = DriverManager.getConnection(getUri(dbName, true).toString()).unwrap(RelationalConnection.class)) {
            dbConn.setSchema(singleReadSchema);
            try (RelationalStatement stmt = dbConn.createStatement();
                    ResultSet resultSet = stmt.executeQuery("SELECT * from \"RestaurantRecord\" WHERE \"name\" = 'testName'")) {
                resultSet.next();
                bh.consume(resultSet.getLong("rest_no"));
            }
        }
    }

    private RelationalStruct newRestaurantRecord() throws SQLException {
        return newRestaurantRecord(restNo.incrementAndGet());
    }

    private RelationalStruct newRestaurantRecord(int recordId) throws SQLException {
        return EmbeddedRelationalStruct.newBuilder()
                .addLong("rest_no", recordId)
                .addString("name", "testName")
                .build();
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(BasicBenchmark.class.getSimpleName())
                .forks(1)
                .build();

        new Runner(opt).run();
    }
}
