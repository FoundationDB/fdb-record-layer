/*
 * FDBRecordStorePerformanceTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2018 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.provider.foundationdb;

import com.apple.foundationdb.record.EndpointType;
import com.apple.foundationdb.record.FunctionNames;
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.PipelineOperation;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexRecordFunction;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.expressions.EmptyKeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpacePath;
import com.apple.foundationdb.record.test.FDBDatabaseExtension;
import com.apple.foundationdb.record.test.TestKeySpace;
import com.apple.foundationdb.test.FDBTestEnvironment;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.util.StringUtils;
import com.apple.test.Tags;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.converters.CommaParameterSplitter;
import org.apache.commons.math3.stat.descriptive.rank.Percentile;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * Performance tests for {@link FDBRecordStore}.
 *
 * Run as a standalone command-line program.
 */
@Tag(Tags.RequiresFDB)
@Tag(Tags.Performance)
public class FDBRecordStorePerformanceTest {
    private static final Logger logger = LoggerFactory.getLogger(FDBRecordStorePerformanceTest.class);
    @RegisterExtension
    final FDBDatabaseExtension dbExtension = new FDBDatabaseExtension();

    static class DatabaseParameters {
        public Object[] path = {"record-test", "performance", "recordStore"};

        @Parameter(names = "trace", description = "Write FDB trace files", arity = 1)
        public boolean trace = true;

        @Parameter(names = "--record-count", description = "Number of records in database")
        public int recordCount = 10000;

        @Parameter(names = "--bucket", description = "Number of records in each bucket")
        public int recordsPerBucket = 1000;

        @Parameter(names = "--commit", description = "Number of records to write in each commit")
        public int recordsPerCommit = 5000;

        @Parameter(names = "--split", description = "Enable record splitting", arity = 1)
        public boolean splitRecords = true;

        @Parameter(names = "--path-cache", description = "Size of path cache")
        public int pathCache = 0;

        @Parameter(names = "--disable-ryw", description = "Disable read-your-writes", arity = 1)
        public boolean disableReadYourWrites = false;

        @Parameter(names = "--string-size", description = "Add a string field of this size")
        public int stringSize = 0;

        @Parameter(names = "--rank-index", description = "Add a rank index", arity = 1)
        public boolean rankIndex = false;
    }

    static class TestParameters implements FDBRecordStoreBase.PipelineSizer, Cloneable {
        @Parameter(names = "--warmup", description = "Number of times to run test before measuring")
        public int warmupCount = 10;

        @Parameter(names = "--repeat", description = "Number of times to repeat test to get average results")
        public int repeatCount = 50;

        @Parameter(names = "--parallel", description = "Number of parallel clients to run")
        public int parallelCount = 10;

        @Parameter(names = "--start", description = "Starting value to pass to test (often bucket position)")
        public int startValue = 0;

        @Parameter(names = "--increment", description = "Increment to value")
        public int incrementValue = 1;

        @Parameter(names = "--pipeline", description = "Size of async read pipeline")
        public int pipelineSize = FDBRecordStore.DEFAULT_PIPELINE_SIZE;

        @Parameter(names = "--parallel-counts", splitter = CommaParameterSplitter.class)
        public List<Integer> parallelCounts = Arrays.asList(1, 2, 3, 4, 5, 10, 15, 20);

        @Parameter(names = "--pipeline-sizes", splitter = CommaParameterSplitter.class)
        public List<Integer> pipelineSizes = Arrays.asList(1, 2, 3, 4, 5, 10, 20, 50, 100);

        public TestParameters() {
        }

        public TestParameters(TestParameters that) {
            this.warmupCount = that.warmupCount;
            this.repeatCount = that.repeatCount;

            this.startValue = that.startValue;
            this.incrementValue = that.incrementValue;
            this.pipelineSize = that.pipelineSize;
            this.parallelCounts = that.parallelCounts;
            this.pipelineSizes = that.pipelineSizes;
        }

        @Override
        public int getPipelineSize(@Nonnull PipelineOperation pipelineOperation) {
            return pipelineSize;
        }

    }

    protected DatabaseParameters databaseParameters = new DatabaseParameters();
    protected FDBDatabase fdb;
    protected RecordMetaData metaData;

    @BeforeEach
    public void setup() {
        createMetaData();
        populate();
    }

    public void createMetaData() {
        FDBDatabaseFactory factory = dbExtension.getDatabaseFactory();
        factory.setDirectoryCacheSize(databaseParameters.pathCache);
        fdb = factory.getDatabase(FDBTestEnvironment.randomClusterFile());

        RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder().setRecords(TestRecords1Proto.getDescriptor());
        metaDataBuilder.setSplitLongRecords(databaseParameters.splitRecords);
        if (databaseParameters.stringSize > 100) {
            metaDataBuilder.removeIndex("MySimpleRecord$str_value_indexed");
        }
        if (databaseParameters.rankIndex) {
            metaDataBuilder.removeIndex("MySimpleRecord$num_value_unique");
            metaDataBuilder.addIndex("MySimpleRecord", new Index("num_value_unique_rank", Key.Expressions.field("num_value_unique").ungrouped(),
                    EmptyKeyExpression.EMPTY, IndexTypes.RANK, Collections.emptyMap()));
        }
        metaData = metaDataBuilder.getRecordMetaData();
    }

    public void populate() {
        int n = 0;
        while (n < databaseParameters.recordCount) {
            try (FDBRecordContext context = fdb.openContext()) {
                final KeySpacePath keyspacePath = TestKeySpace.getKeyspacePath(databaseParameters.path);
                final FDBRecordStore.Builder storeBuilder = FDBRecordStore.newBuilder().setContext(context).setMetaDataProvider(metaData)
                        .setKeySpacePath(keyspacePath);
                final FDBRecordStore recordStore;
                if (n == 0) {
                    FDBRecordStore.deleteStore(context, keyspacePath);
                    recordStore = storeBuilder.create();
                } else {
                    recordStore = storeBuilder.open();
                }
                int c = 0;
                while (c < databaseParameters.recordsPerCommit && n < databaseParameters.recordCount) {
                    TestRecords1Proto.MySimpleRecord.Builder recBuilder = TestRecords1Proto.MySimpleRecord.newBuilder();
                    recBuilder.setRecNo(n);
                    if (databaseParameters.stringSize > 0) {
                        recBuilder.setStrValueIndexed(StringUtils.repeat((char) ('A' + n % 26), databaseParameters.stringSize));
                    }
                    if (databaseParameters.rankIndex) {
                        recBuilder.setNumValueUnique(n);
                    }
                    recBuilder.setNumValue2((n + databaseParameters.recordsPerBucket) % databaseParameters.recordCount);
                    recBuilder.setNumValue3Indexed(n / databaseParameters.recordsPerBucket);
                    recordStore.saveRecord(recBuilder.build());
                    n++;
                    c++;
                }
                context.commit();
            }
        }
    }

    public void runRamps(String title, Function<Integer, Function<FDBRecordStore, CompletableFuture<?>>> test, TestParameters parameters) {
        for (Integer parallelCount : parameters.parallelCounts) {
            for (Integer pipelineSize : parameters.pipelineSizes) {
                parameters.parallelCount = parallelCount;
                parameters.pipelineSize = pipelineSize;
                runTest(title, test, parameters);
            }
        }
    }

    public void runSingleRamps(String title, Function<Integer, Function<FDBRecordStore, CompletableFuture<?>>> test, TestParameters parameters) {
        TestParameters justParallel = new TestParameters(parameters);
        justParallel.pipelineSizes = Collections.singletonList(justParallel.pipelineSize);
        runRamps(title, test, justParallel);

        TestParameters justPipeline = new TestParameters(parameters);
        justParallel.parallelCounts = Collections.singletonList(justPipeline.parallelCount);
        runRamps(title, test, justPipeline);
    }

    protected void runTest(String title, Function<Integer, Function<FDBRecordStore, CompletableFuture<?>>> test, TestParameters parameters) {
        for (int i = 0; i < parameters.warmupCount; i++) {
            openAndRun(null, test.apply(i), parameters).join();
        }

        final FDBStoreTimer timer = new FDBStoreTimer();
        final List<Long> times = new ArrayList<>();
        if (parameters.parallelCount == 0) {
            final Function<FDBRecordStore, CompletableFuture<?>> singleTest = test.apply(parameters.startValue);
            for (int j = 0; j < parameters.repeatCount; j++) {
                try (FDBRecordContext context = fdb.openContext(null, timer)) {
                    if (databaseParameters.disableReadYourWrites) {
                        context.ensureActive().options().setReadYourWritesDisable();
                    }
                    FDBRecordStore store = FDBRecordStore.newBuilder().setContext(context).setMetaDataProvider(metaData)
                            .setKeySpacePath(TestKeySpace.getKeyspacePath(databaseParameters.path))
                            .setPipelineSizer(parameters)
                            .uncheckedOpen();
                    long startTime = System.nanoTime();
                    try {
                        singleTest.apply(store).get();
                    } catch (Exception ex) {
                        logger.warn("error on pass " + j, ex);
                    }
                    times.add(System.nanoTime() - startTime);
                }
            }
        } else {
            for (int j = 0; j < parameters.repeatCount; j++) {
                List<CompletableFuture<Long>> futures = new ArrayList<>(parameters.parallelCount);
                for (int i = 0; i < parameters.parallelCount; i++) {
                    futures.add(openAndRun(timer, test.apply(parameters.startValue + parameters.incrementValue * i), parameters));
                }
                for (CompletableFuture<Long> future : futures) {
                    try {
                        times.add(future.get());
                    } catch (Exception ex) {
                        logger.warn("error on pass " + j, ex);
                    }
                }
            }
        }
        final double[] dtimes = new double[times.size()];
        for (int i = 0; i < dtimes.length; i++) {
            dtimes[i] = TimeUnit.NANOSECONDS.toMicros(times.get(i));
        }
        Arrays.sort(dtimes);
        final double min = dtimes[0];
        final double max = dtimes[dtimes.length - 1];
        final Percentile percentile = new Percentile();
        percentile.setData(dtimes);
        final double p50 = percentile.evaluate(50);
        final double p90 = percentile.evaluate(90);
        final KeyValueLogMessage msg = KeyValueLogMessage.build(title,
                "repeatCount", parameters.repeatCount,
                "parallelCount", parameters.parallelCount,
                "pipelineSize", parameters.pipelineSize,
                "min_micros", min,
                "max_micros", max,
                "p50_micros", p50,
                "p90_micros", p90);
        msg.addKeysAndValues(new TreeMap<>(timer.getKeysAndValues()));
        logger.info(msg.toString());
    }

    protected CompletableFuture<Long> openAndRun(FDBStoreTimer timer, Function<FDBRecordStore, CompletableFuture<?>> test, TestParameters parameters) {
        return fdb.runAsync(timer, null, context -> {
            if (databaseParameters.disableReadYourWrites) {
                context.ensureActive().options().setReadYourWritesDisable();
            }
            return FDBRecordStore.newBuilder().setContext(context).setMetaDataProvider(metaData)
                    .setKeySpacePath(TestKeySpace.getKeyspacePath(databaseParameters.path))
                    .setPipelineSizer(parameters)
                    .uncheckedOpenAsync().thenCompose(store -> {
                        long startTime = System.nanoTime();
                        return test.apply(store).thenApply(lignore -> System.nanoTime() - startTime);
                    });
        });
    }

    protected static Function<FDBRecordStore, CompletableFuture<?>> scanRecords(int start) {
        return store -> {
            final int pipelineSize = store.getPipelineSize(PipelineOperation.KEY_TO_RECORD);
            return store.scanRecords(Tuple.from(start), Tuple.from(start + pipelineSize),
                    EndpointType.RANGE_INCLUSIVE, EndpointType.RANGE_EXCLUSIVE, null, ScanProperties.FORWARD_SCAN)
                    .getCount();
        };
    }

    protected static Function<FDBRecordStore, CompletableFuture<?>> loadRecords(int start) {
        return store -> {
            final int pipelineSize = store.getPipelineSize(PipelineOperation.KEY_TO_RECORD);
            CompletableFuture<?>[] futures = new CompletableFuture<?>[pipelineSize];
            for (int i = 0; i < pipelineSize; i++) {
                futures[i] = store.loadRecordAsync(Tuple.from(start + i));
            }
            return CompletableFuture.allOf(futures);
        };
    }

    protected static Function<FDBRecordStore, CompletableFuture<?>> indexScanNum3Equals(int num3) {
        return store -> store.scanIndex(store.getRecordMetaData().getIndex("MySimpleRecord$num_value_3_indexed"),
                IndexScanType.BY_VALUE, TupleRange.allOf(Tuple.from(num3)), null, ScanProperties.FORWARD_SCAN)
                .getCount();
    }

    protected static Function<FDBRecordStore, CompletableFuture<?>> indexRecordScanNum3Equals(int num3) {
        return store -> store.scanIndexRecordsEqual("MySimpleRecord$num_value_3_indexed", num3).getCount();
    }

    protected static Function<FDBRecordStore, CompletableFuture<?>> indexRecordPrefetchScanNum3Equals(int num3) {
        return store -> store.scanIndexRemoteFetchRecordsEqual("MySimpleRecord$num_value_3_indexed", Key.Expressions.field("rec_no"), num3).asList();
    }

    protected static Function<FDBRecordStore, CompletableFuture<?>> scanAndJoinNum3Equals(int num3) {
        return store -> store.scanIndexRecordsEqual("MySimpleRecord$num_value_3_indexed", num3)
                .mapPipelined(r -> {
                    TestRecords1Proto.MySimpleRecord.Builder recBuilder = TestRecords1Proto.MySimpleRecord.newBuilder();
                    recBuilder.mergeFrom(r.getRecord());
                    return store.loadRecordAsync(Tuple.from(recBuilder.getNumValue3Indexed()));
                }, store.getPipelineSize(PipelineOperation.KEY_TO_RECORD))
                .getCount();
    }

    protected static Function<FDBRecordStore, CompletableFuture<?>> scanAndRankNum3Equals(int num3) {
        final IndexRecordFunction<Long> rank = new IndexRecordFunction<>(FunctionNames.RANK,
                Key.Expressions.field("num_value_unique").ungrouped(),
                "num_value_unique_rank");
        return store -> {
            return store.scanIndexRecordsEqual("MySimpleRecord$num_value_3_indexed", num3)
                    .mapPipelined(r -> store.evaluateRecordFunction(rank, r.getStoredRecord()), store.getPipelineSize(PipelineOperation.KEY_TO_RECORD))
                    .getCount();
        };
    }

    @Test
    public void scanRecordsTest() throws Exception {
        runTest("scan records", FDBRecordStorePerformanceTest::scanRecords, new TestParameters());
    }

    @Test
    public void loadRecordsTest() throws Exception {
        runTest("load records", FDBRecordStorePerformanceTest::loadRecords, new TestParameters());
    }

    @Test
    public void indexScanNum3EqualsTest() throws Exception {
        runTest("index scan", FDBRecordStorePerformanceTest::indexScanNum3Equals, new TestParameters());
    }

    @Test
    public void indexRecordScanNum3EqualsTest() throws Exception {
        runTest("index record scan", FDBRecordStorePerformanceTest::indexRecordScanNum3Equals, new TestParameters());
    }

    @Test
    public void indexRecordPrefetchScanNum3EqualsTest() throws Exception {
        runTest("index record scan", FDBRecordStorePerformanceTest::indexRecordPrefetchScanNum3Equals, new TestParameters());
    }

    @Test
    public void scanAndJoinNum3EqualsTest() throws Exception {
        runTest("scan and join", FDBRecordStorePerformanceTest::scanAndJoinNum3Equals, new TestParameters());
    }

    @Test
    public void scanAndRankNum3EqualsTest() throws Exception {
        runTest("scan and rank", FDBRecordStorePerformanceTest::scanAndRankNum3Equals, new TestParameters());
    }

    @Test
    public void scanAndJoinNum3EqualsRamp() throws Exception {
        runSingleRamps("scan and join", FDBRecordStorePerformanceTest::scanAndJoinNum3Equals, new TestParameters());
    }

    static class Args {
        @Parameter(description = "Tests to run")
        public List<String> tests = new ArrayList<>(Arrays.asList("join"));

        @Parameter(names = "--populate", description = "Populate the database before test", arity = 1)
        public boolean populate = true;

        @Parameter(names = "--ramp", description = "Vary parallel and pipeline parameters")
        public boolean ramp = false;

        @Parameter(names = "--single-ramp", description = "Vary each parameter")
        public boolean singleRamp = false;
    }

    // export $(tail +2 fdb-environment.properties) && java -Dlog4j.configurationFile=standalone.log4j.properties -jar ./fdb-record-layer-core/.out/libs/fdb-record-layer-core-*-SNAPSHOT-standalone-tests.jar
    public static void main(String[] argv) throws Exception {
        DatabaseParameters databaseParameters = new DatabaseParameters();
        TestParameters testParameters = new TestParameters();
        Args args = new Args();

        JCommander.newBuilder()
                .addObject(new Object[]{databaseParameters, testParameters, args})
                .build()
                .parse(argv);

        List<Function<Integer, Function<FDBRecordStore, CompletableFuture<?>>>> tests = new ArrayList<>();
        for (String testName : args.tests) {
            final Function<Integer, Function<FDBRecordStore, CompletableFuture<?>>> testFunction;
            if ("scanRecords".equals(testName)) {
                testFunction = FDBRecordStorePerformanceTest::scanRecords;
            } else if ("loadRecords".equals(testName)) {
                testFunction = FDBRecordStorePerformanceTest::loadRecords;
            } else if ("indexScan".equals(testName)) {
                testFunction = FDBRecordStorePerformanceTest::indexScanNum3Equals;
            } else if ("indexRecord".equals(testName)) {
                testFunction = FDBRecordStorePerformanceTest::indexRecordScanNum3Equals;
            } else if ("indexRecordPrefetch".equals(testName)) {
                testFunction = FDBRecordStorePerformanceTest::indexRecordPrefetchScanNum3Equals;
            } else if ("join".equals(testName)) {
                testFunction = FDBRecordStorePerformanceTest::scanAndJoinNum3Equals;
            } else if ("rank".equals(testName)) {
                testFunction = FDBRecordStorePerformanceTest::scanAndRankNum3Equals;
            } else {
                throw new IllegalArgumentException("unknown test: " + testName);
            }
            tests.add(testFunction);
        }

        final FDBRecordStorePerformanceTest tester = new FDBRecordStorePerformanceTest();
        tester.databaseParameters = databaseParameters;
        if (databaseParameters.trace) {
            FDBDatabaseFactory.instance().setTrace(".", "record-perf");
        }
        tester.createMetaData();
        if (args.populate) {
            tester.populate();
        }

        logger.info("starting test");
        for (int i = 0; i < tests.size(); i++) {
            final String testName = args.tests.get(i);
            final Function<Integer, Function<FDBRecordStore, CompletableFuture<?>>> testFunction = tests.get(i);
            if (args.singleRamp) {
                tester.runSingleRamps(testName, testFunction, testParameters);
            } else if (args.ramp) {
                tester.runRamps(testName, testFunction, testParameters);
            } else {
                tester.runTest(testName, testFunction, testParameters);
            }
        }
    }
}
