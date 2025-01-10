/*
 * RecordLayerScanBenchmark.java
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

import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.provider.common.DynamicMessageRecordSerializer;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabase;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabaseFactory;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoredRecord;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.AbstractMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

@Fork(1)
@Warmup(iterations = 1, time = 10)
@Measurement(iterations = 5, time = 10)
@BenchmarkMode({Mode.AverageTime, Mode.SampleTime})
@Threads(Threads.MAX)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@API(API.Status.EXPERIMENTAL)
public class RecordLayerScanBenchmark extends RelationalScanBenchmark {

    @Override
    public void scan(Blackhole bh, RelationalConnHolder ignored) throws RelationalException {
        FDBDatabase fdbDb = FDBDatabaseFactory.instance().getDatabase();
        try (FDBRecordContext ctx = fdbDb.openContext()) {
            final var path = RelationalKeyspaceProvider.toDatabasePath(dbUri, driver.keySpace).schemaPath(schema);
            FDBRecordStoreBase<Message> store = FDBRecordStore.newBuilder()
                    .setKeySpacePath(path)
                    .setMetaDataProvider(accessor.getProvider(new RecordContextTransaction(ctx)))
                    .setSerializer(DynamicMessageRecordSerializer.instance())
                    .setUserVersionChecker((oldUserVersion, oldMetaDataVersion, metaData) -> CompletableFuture.completedFuture(oldMetaDataVersion))
                    .setFormatVersion(1)
                    .setContext(ctx)
                    .open();

            try (RecordCursor<FDBStoredRecord<Message>> cursor =
                         store.scanRecords(TupleRange.ALL, null, ScanProperties.FORWARD_SCAN)) {
                List<Map.Entry<String, ByteString>> results = cursor.map(FDBStoredRecord::getRecord)
                        .map(rec -> {
                            Descriptors.Descriptor d = rec.getDescriptorForType();
                            d.findFieldByName("id");
                            String id = (String) rec.getField(d.findFieldByName("id"));
                            ByteString bytes = (ByteString) rec.getField(d.findFieldByName("some_bytes"));
                            return (Map.Entry<String, ByteString>) new AbstractMap.SimpleEntry<>(id, bytes);
                        })
                        .asList()
                        .get();
                bh.consume(results);
            } catch (ExecutionException | InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static void main(String[] args) throws RunnerException {
        org.openjdk.jmh.runner.options.Options opt = new OptionsBuilder()
                .include(RecordLayerScanBenchmark.class.getSimpleName())
                .forks(1)
                .build();

        new Runner(opt).run();
    }
}
