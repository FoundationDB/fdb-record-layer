/*
 * SimpleBenchmark.java
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
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

/**
 * Load up the benchmark data sets.
 */
@State(Scope.Benchmark)
@Warmup(iterations = 0)
@Measurement(iterations = 1)
@Fork(1)
public class LoadBenchmark {
    @Param({"500"})
    public int commitQuantum;

    @Benchmark
    public void simple(BenchmarkRecordStores.Simple fdb, BenchmarkTimer timer) {
        fdb.deleteAllData(timer);
        for (int recNo = 0; recNo < fdb.numberOfRecords; recNo += commitQuantum) {
            final int baseRecNo = recNo;
            fdb.run(timer, recordStore -> {
                for (int i = 0; i < commitQuantum; i++) {
                    recordStore.saveRecord(simpleRecord1(baseRecNo + i));
                }
            });
        }
    }

    @Benchmark
    public void rank(BenchmarkRecordStores.Rank fdb, BenchmarkTimer timer) {
        fdb.deleteAllData(timer);
        for (int recNo = 0; recNo < fdb.numberOfRecords; recNo += commitQuantum) {
            final int baseRecNo = recNo;
            fdb.run(timer, recordStore -> {
                for (int i = 0; i < commitQuantum; i++) {
                    recordStore.saveRecord(simpleRecord1(baseRecNo + i));
                }
            });
        }
    }

    private BenchmarkRecords1Proto.MySimpleRecord simpleRecord1(int recNo) {
        return BenchmarkRecords1Proto.MySimpleRecord.newBuilder()
                .setRecNo(recNo)
                .setStrValueIndexed(recNo % 2 == 0 ? "even" : "odd")
                .setNumValue2(recNo % 5)
                .setNumValue3Indexed(recNo % 3)
                .setNumValueUnique(recNo + 1000)
                .build();
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(LoadBenchmark.class.getSimpleName())
                .build();

        new Runner(opt).run();
    }
}
