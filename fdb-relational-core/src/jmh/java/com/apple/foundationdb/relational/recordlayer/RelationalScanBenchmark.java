/*
 * RelationalScanBenchmark.java
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
import com.apple.foundationdb.relational.api.KeySet;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.RelationalConnection;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.RelationalStatement;
import com.apple.foundationdb.relational.api.RelationalStruct;
import com.apple.foundationdb.relational.api.RelationalStructBuilder;
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

import javax.annotation.Nonnull;
import java.net.URI;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

@Fork(1)
@Warmup(iterations = 1, time = 10)
@Measurement(iterations = 5, time = 10)
@BenchmarkMode({Mode.AverageTime, Mode.SampleTime})
@Threads(Threads.MAX)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@API(API.Status.EXPERIMENTAL)
public class RelationalScanBenchmark extends EmbeddedRelationalBenchmark {

    static final String schema = "putAndScan";

    private static final String templateName = "putAndScanTemplate";

    private static final String templateDefinition =
            "CREATE TABLE DirectAccessBenchTable (id STRING, some_bytes BYTES, PRIMARY KEY(id))";

    static final URI dbUri = URI.create("/BENCHMARKS/putAndScan");

    @Param({"1", "10", "100","1000"})
    int rowCount;

    Driver driver = new Driver(templateName, templateDefinition);

    protected RelationalRecordLayerAccessor accessor;

    @Setup(Level.Trial)
    public void trialUp() throws SQLException, RelationalException {
        driver.up();
        accessor = createDatabase();
        insertData(new DataSet(rowCount));
    }


    @Benchmark
    public void scan(Blackhole bh, RelationalConnHolder connHolder) throws RelationalException, SQLException {
        try (final var stmt = connHolder.connection.createStatement().unwrap(RelationalStatement.class)) {

            final Options scanOpts = Options.NONE; //Options.builder().withOption(Options.Name.REQUIRED_METADATA_TABLE_VERSION, -1).build();
            List<Map.Entry<String, byte[]>> data = new ArrayList<>();
            try (RelationalResultSet rs = stmt.executeScan("DIRECTACCESSBENCHTABLE", new KeySet(), scanOpts)) {
                while (rs.next()) {
                    String id = rs.getString("ID");
                    byte[] some_bytes = rs.getBytes("SOME_BYTES");
                    data.add(new AbstractMap.SimpleEntry<>(id, some_bytes));
                }
            }
            bh.consume(data);
        }
    }

    @TearDown(Level.Trial)
    public void trialDown() throws SQLException, RelationalException {
        try {
            EmbeddedRelationalBenchmark.deleteDatabase(dbUri);
        } finally {
            driver.down();
        }
    }

    public static class DataSet {
        private final int numRecords;
        private int pos = 0;

        private final List<RelationalStruct> data = new ArrayList<>();

        public DataSet(int numRecords) {
            this.numRecords = numRecords;
        }

        public boolean hasNext() {
            return pos < numRecords;
        }

        public void next(RelationalStructBuilder builder) throws SQLException {
            //create the data
            builder.addString("ID", "id" + pos);
            byte[] bytes = new byte[20];
            ThreadLocalRandom.current().nextBytes(bytes);
            builder.addBytes("SOME_BYTES", bytes);
            data.add(builder.build());
            pos++;
        }

        public List<RelationalStruct> data() {
            return data;
        }
    }

    public void insertData(@Nonnull DataSet dataSet) throws SQLException {
        //insert about 10 records
        try (RelationalConnection conn = DriverManager.getConnection("jdbc:embed:" + dbUri).unwrap(RelationalConnection.class)) {
            conn.setSchema(schema);
            try (RelationalStatement vs = conn.createStatement()) {
                final RelationalStructBuilder builder = EmbeddedRelationalStruct.newBuilder();
                while (dataSet.hasNext()) {
                    dataSet.next(builder);
                }
                vs.executeInsert("DIRECTACCESSBENCHTABLE", dataSet.data());
            }
        }
    }

    private RelationalRecordLayerAccessor createDatabase() throws RelationalException, SQLException {
        EmbeddedRelationalBenchmark.createDatabase(dbUri, templateName, schema);
        return new RelationalRecordLayerAccessor(driver, dbUri, schema);
    }

    public static void main(String[] args) throws RunnerException {
        org.openjdk.jmh.runner.options.Options opt = new OptionsBuilder()
                .include(RelationalScanBenchmark.class.getSimpleName())
                .forks(0)
                .build();

        new Runner(opt).run();
    }

    @State(Scope.Thread)
    public static class RelationalConnHolder {
        private Connection connection;

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
}
