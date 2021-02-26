/*
 * BasicBenchmark.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.benchmark;

import com.apple.foundationdb.record.BenchmarkRecords1Proto;
import com.apple.foundationdb.tuple.Tuple;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.Random;

/**
 * Very basic read / write test.
 */
public class BasicBenchmark {
    /**
     * Random key selector.
     */
    @State(Scope.Thread)
    public static class KeyGen {
        Random random;
        int mod;

        @Setup
        public void setup(BenchmarkRecordStores.Simple db) {
            random = new Random();
            mod = db.numberOfRecords;
        }

        public Tuple nextKey() {
            return Tuple.from(random.nextInt(mod));
        }
    }

    @Benchmark
    public void loadRecord(BenchmarkRecordStores.Simple fdb, BenchmarkTimer timer, KeyGen keyGen, Blackhole blackhole) {
        final Tuple primaryKey = keyGen.nextKey();
        fdb.run(timer, recordStore -> {
            blackhole.consume(recordStore.loadRecord(primaryKey));
        });
    }

    @Benchmark
    public void saveRecord(BenchmarkRecordStores.Simple fdb, BenchmarkTimer timer, KeyGen keyGen) {
        final Tuple primaryKey = keyGen.nextKey();
        fdb.run(timer, recordStore -> {
            final BenchmarkRecords1Proto.MySimpleRecord.Builder builder = BenchmarkRecords1Proto.MySimpleRecord.newBuilder();
            builder.mergeFrom(recordStore.loadRecord(primaryKey).getRecord());
            builder.setNumValue3Indexed(builder.getNumValue3Indexed() + 1);
            recordStore.saveRecord(builder.build());
        });
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(BasicBenchmark.class.getSimpleName())
                .build();

        new Runner(opt).run();
    }
}
