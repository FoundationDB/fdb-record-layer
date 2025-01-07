/*
 * SetSchemaBenchmark.java
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

package com.apple.foundationdb.relational.api;

import com.apple.foundationdb.annotation.API;

import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.recordlayer.EmbeddedRelationalBenchmark;
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
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.net.URI;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

/**
 * Benchmark the cost of calling `setSchema()` on an open connection.
 */
@Fork(1)
@Warmup(iterations = 1, time = 10)
@Measurement(iterations = 5, time = 10)
@BenchmarkMode({Mode.AverageTime, Mode.SampleTime})
@Threads(Threads.MAX)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@API(API.Status.EXPERIMENTAL)
public class SetSchemaBenchmark extends EmbeddedRelationalBenchmark {
    static final String schema = "setSchema";

    private static final String templateName = "setSchemaTemplate";

    private static final String templateDefinition =
            "CREATE TABLE SetSchema (id STRING, some_bytes BYTES, PRIMARY KEY(id))";

    static final URI dbUri = URI.create("/BENCHMARKS/setSchema");

    Driver driver = new Driver(templateName, templateDefinition);


    @Setup(Level.Trial)
    public void trialUp() throws SQLException, RelationalException {
        driver.up();
        EmbeddedRelationalBenchmark.createDatabase(dbUri, templateName, schema);
    }

    @TearDown(Level.Trial)
    public void trialDown() throws SQLException, RelationalException {
        try {
            EmbeddedRelationalBenchmark.deleteDatabase(dbUri);
        } finally {
            driver.down();
        }
    }

    @Benchmark
    public void setSchema(RelationalConnHolder connHolder) throws SQLException {
        connHolder.connection.setSchema(schema);
    }

    public static void main(String[] args) throws RunnerException {
        org.openjdk.jmh.runner.options.Options opt = new OptionsBuilder()
                .include(SetSchemaBenchmark.class.getSimpleName())
                .forks(0) //run in same JVM
                .build();

        new Runner(opt).run();
    }

    @State(Scope.Thread)
    public static class RelationalConnHolder {
        private Connection connection;

        @Setup
        public void init() throws SQLException {
            connection = DriverManager.getConnection("jdbc:embed:" + dbUri.getPath());
        }

        @TearDown
        public void stop() throws SQLException {
            connection.close();
        }
    }
}

