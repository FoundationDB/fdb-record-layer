/*
 * CreateDatabaseBenchmark.java
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
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

@Fork(1)
@Warmup(iterations = 1, time = 100)
@Measurement(iterations = 5, time = 100)
@BenchmarkMode({Mode.AverageTime, Mode.SampleTime})
@Threads(Threads.MAX)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
public class CreateDatabaseBenchmark extends EmbeddedRelationalBenchmark {
    static final String schema = "schema";

    Driver driver = new Driver();

    @Setup(Level.Trial)
    public void trialUp() throws SQLException, RelationalException {
        driver.up();
    }

    @TearDown(Level.Trial)
    public void trialDown() throws RelationalException {
        driver.down();
    }

    @State(Scope.Thread)
    public static class DbNameGenerator {
        private int id = 0;

        public String getNextDbName() {
            return "/BENCHMARKS/CreateDatabaseBenchmark_" + Thread.currentThread().getId() + "_" + id++;
        }
    }

    @Benchmark
    public void createDatabase(ThreadScopedDatabases databases, DbNameGenerator dbNameGenerator) throws RelationalException, SQLException {
        databases.createDatabase(
                DatabaseTemplate.newBuilder()
                        .withSchema(schema, schemaTemplateName)
                        .build(),
                dbNameGenerator.getNextDbName());
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(CreateDatabaseBenchmark.class.getSimpleName())
                .forks(1)
                .build();

        new Runner(opt).run();
    }
}
