/*
 * BenchmarkRecordStores.java
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

import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

/**
 * Specific store configurations.
 */
public class BenchmarkRecordStores {
    private BenchmarkRecordStores() {
    }

    /**
     * Simple record store.
     * <ul>
     * <li>{@code simple} keyspace</li>
     * <li>{@code benchmark_records_1} with no additional indexes</li>
     * </ul>
     */
    @State(Scope.Benchmark)
    public static class Simple extends BenchmarkRecordStore {
        @Setup
        public void setup() {
            setName("simple");
            setMetaData(BenchmarkMetaData.records1Base());
        }

        @Param("10000")
        public int numberOfRecords;
    }

    /**
     * Rank record store.
     * <ul>
     * <li>{@code rank} keyspace</li>
     * <li>{@code benchmark_records_1} with additional rank index</li>
     * </ul>
     */
    @State(Scope.Benchmark)
    public static class Rank extends BenchmarkRecordStore {
        @Setup
        public void setup() {
            setName("rank");
            setMetaData(BenchmarkMetaData.records1Rank());
        }

        @Param("5000")
        public int numberOfRecords;
    }
}
