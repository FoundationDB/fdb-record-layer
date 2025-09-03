/*
 * RelationalConnectionBenchmark.java
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
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.net.URI;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

/**
 * A Simple benchmark for estimating the cost of connecting to Relational.
 */

@Fork(1)
@Warmup(iterations = 1, time = 10)
@Measurement(iterations = 5, time = 10)
@BenchmarkMode({Mode.AverageTime, Mode.SampleTime})
@Threads(Threads.MAX)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@API(API.Status.EXPERIMENTAL)
public class RelationalConnectionBenchmark extends EmbeddedRelationalBenchmark {

    Driver driver = new Driver(); //use the default benchmark template, cause we don't care what our data is here


    @Param(value = {"true", "false"})
    boolean setSchemaAtConnTime;

    final String schemaName = "testSchema";
    URI dbUri = URI.create("/BENCHMARKS/connectionDb");

    @Setup(Level.Trial)
    public void trialUp() throws SQLException, RelationalException {
        driver.up();
        EmbeddedRelationalBenchmark.createDatabase(dbUri, schemaTemplateName, schemaName);
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
    public String connect() throws SQLException {
        String connUri;
        if (setSchemaAtConnTime) {
            connUri = "jdbc:embed:" + dbUri.getPath() + "?schema=" + schemaName;
        } else {
            connUri = "jdbc:embed:" + dbUri.getPath();
        }
        try (final var vc = DriverManager.getConnection(connUri)) {
            if (!setSchemaAtConnTime) {
                vc.setSchema(schemaName);
            }
            return vc.getSchema();
        }
    }

    public static void main(String[] args) throws RunnerException {
        org.openjdk.jmh.runner.options.Options opt = new OptionsBuilder()
                .include(RelationalConnectionBenchmark.class.getSimpleName())
                .forks(0)
                .build();

        new Runner(opt).run();
    }
}
