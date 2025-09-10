/*
 * OnlineIndexerBuildValueIndexTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2019 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.test.Tags;
import com.google.common.base.Strings;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static com.apple.foundationdb.record.metadata.Key.Expressions.field;

/**
 * Test building value indexes.
 */
class OnlineIndexerBuildValueIndexTest extends OnlineIndexerBuildIndexTest {

    private void valueRebuild(@Nonnull List<TestRecords1Proto.MySimpleRecord> records, @Nullable List<TestRecords1Proto.MySimpleRecord> recordsWhileBuilding,
                              int agents, boolean overlap, boolean splitLongRecords) {
        final OnlineIndexerTestRecordHandler<TestRecords1Proto.MySimpleRecord> recordHandler = OnlineIndexerTestSimpleRecordHandler.instance();
        Index index = new Index("newIndex", field("num_value_2"));
        Function<FDBQueriedRecord<Message>, Integer> projection = rec -> {
            TestRecords1Proto.MySimpleRecord simple = TestRecords1Proto.MySimpleRecord.newBuilder().mergeFrom(rec.getRecord()).build();
            if (simple.hasNumValue2()) {
                return simple.getNumValue2();
            } else {
                return Integer.MIN_VALUE;
            }
        };

        List<RecordQuery> queries = records.stream()
                .map(record -> {
                    Integer value2 = (record.hasNumValue2()) ? record.getNumValue2() : null;
                    return RecordQuery.newBuilder()
                            .setRecordType("MySimpleRecord")
                            .setFilter(value2 != null ?
                                       Query.field("num_value_2").equalsValue(record.getNumValue2()) :
                                       Query.field("num_value_2").isNull())
                            .build();
                })
                .collect(Collectors.toList());

        Function<TestRecords1Proto.MySimpleRecord, Integer> indexValue = msg -> msg.hasNumValue2() ? msg.getNumValue2() : null;
        Map<Integer, List<Message>> valueMap = group(records, indexValue);

        Runnable beforeBuild = () -> {
            try (FDBRecordContext context = openContext()) {
                for (int i = 0; i < queries.size(); i++) {
                    Integer value2 = (records.get(i).hasNumValue2()) ? records.get(i).getNumValue2() : null;
                    String planString = "SCAN(<,>) | TFILTER MySimpleRecord | QCFILTER " + ((value2 == null) ?  "num_value_2 IS_NULL" : "num_value_2 EQUALS " + value2);
                    executeQuery(queries.get(i), planString, valueMap.get(value2));
                }
                context.commit();
            }
        };

        List<TestRecords1Proto.MySimpleRecord> updatedRecords;
        List<RecordQuery> updatedQueries;
        Map<Integer, List<Message>> updatedValueMap;
        if (recordsWhileBuilding == null || recordsWhileBuilding.isEmpty()) {
            updatedRecords = records;
            updatedQueries = queries;
            updatedValueMap = valueMap;
        } else {
            updatedRecords = updated(recordHandler, records, recordsWhileBuilding, null);
            updatedQueries = updatedRecords.stream()
                    .map(record -> {
                        Integer value2 = (record.hasNumValue2()) ? record.getNumValue2() : null;
                        return RecordQuery.newBuilder()
                                .setRecordType("MySimpleRecord")
                                .setFilter(value2 != null ?
                                           Query.field("num_value_2").equalsValue(record.getNumValue2()) :
                                           Query.field("num_value_2").isNull())
                                .build();
                    })
                    .collect(Collectors.toList());
            updatedValueMap = group(updatedRecords, indexValue);
        }

        Runnable afterBuild = new Runnable() {
            @SuppressWarnings("try")
            @Override
            public void run() {
                try (FDBRecordContext context = openContext()) {
                    // The build job shouldn't affect the reads.
                    for (int i = 0; i < updatedQueries.size(); i++) {
                        Integer value2 = (updatedRecords.get(i).hasNumValue2()) ? updatedRecords.get(i).getNumValue2() : null;
                        String planString = "SCAN(<,>) | TFILTER MySimpleRecord | QCFILTER " + ((value2 == null) ? "num_value_2 IS_NULL" : "num_value_2 EQUALS " + value2);
                        executeQuery(updatedQueries.get(i), planString, updatedValueMap.get(value2));
                    }
                }
            }
        };

        Runnable afterReadable = () -> {
            try (FDBRecordContext context = openContext()) {
                for (int i = 0; i < updatedQueries.size(); i++) {
                    Integer value2 = (updatedRecords.get(i).hasNumValue2()) ? updatedRecords.get(i).getNumValue2() : null;
                    executeQuery(updatedQueries.get(i), "ISCAN(newIndex [[" + value2 + "],[" + value2 + "]])", updatedValueMap.get(value2));
                }
                RecordQuery sortQuery = RecordQuery.newBuilder()
                        .setRecordType("MySimpleRecord")
                        .setSort(field("num_value_2"))
                        .build();
                executeQuery(sortQuery, "ISCAN(newIndex <,>)", updatedRecords.stream().map(msg -> (msg.hasNumValue2()) ? msg.getNumValue2() : Integer.MIN_VALUE).sorted().collect(Collectors.toList()), projection);
                context.commit();
            }
        };

        singleRebuild(recordHandler, records, recordsWhileBuilding, null, agents, overlap, splitLongRecords, index, null, beforeBuild, afterBuild, afterReadable);
    }

    private void valueRebuild(@Nonnull List<TestRecords1Proto.MySimpleRecord> records, @Nullable List<TestRecords1Proto.MySimpleRecord> recordsWhileBuilding,
                              int agents, boolean overlap) {
        valueRebuild(records, recordsWhileBuilding, agents, overlap, false);
    }

    private void valueRebuild(@Nonnull List<TestRecords1Proto.MySimpleRecord> records, @Nullable List<TestRecords1Proto.MySimpleRecord> recordsWhileBuilding) {
        valueRebuild(records, recordsWhileBuilding, 1, false);
    }

    private void valueRebuild(@Nonnull List<TestRecords1Proto.MySimpleRecord> records, boolean splitLongRecords) {
        valueRebuild(records, null, 1, false, splitLongRecords);
    }

    private void valueRebuild(@Nonnull List<TestRecords1Proto.MySimpleRecord> records) {
        valueRebuild(records, null);
    }

    @Test
    void emptyRange() {
        valueRebuild(Collections.emptyList());
    }

    @Test
    void singleElement() {
        TestRecords1Proto.MySimpleRecord record = TestRecords1Proto.MySimpleRecord.newBuilder()
                .setRecNo(1517)
                .setNumValue2(95)
                .build();
        valueRebuild(Collections.singletonList(record));
    }

    @Test
    void tenElements() {
        List<TestRecords1Proto.MySimpleRecord> records = IntStream.range(-5, 5).mapToObj(val ->
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(val * 457).setNumValue2(Math.abs(val * 2)).build()
        ).collect(Collectors.toList());
        valueRebuild(records);
    }

    @Test
    void tenAdjacentElements() {
        List<TestRecords1Proto.MySimpleRecord> records = IntStream.range(-5, 5).mapToObj(val ->
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(val).setNumValue2(val).build()
        ).collect(Collectors.toList());
        valueRebuild(records);
    }

    @Test
    void fiftyElements() {
        List<TestRecords1Proto.MySimpleRecord> records = IntStream.range(-25, 25).mapToObj( val ->
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(val * 37).setNumValue2(Math.abs(val) % 5).build()
        ).collect(Collectors.toList());
        valueRebuild(records);
    }

    @ParameterizedTest
    @MethodSource("randomSeeds")
    @Tag(Tags.Slow)
    void oneHundredElements(long seed) {
        Random r = new Random(seed);
        List<TestRecords1Proto.MySimpleRecord> records = Stream.generate(() ->
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(r.nextLong()).setNumValue2(r.nextInt(10)).build()
        ).limit(100).sorted(Comparator.comparingLong(TestRecords1Proto.MySimpleRecord::getRecNo)).collect(Collectors.toList());
        valueRebuild(records);
    }

    @ParameterizedTest
    @MethodSource("randomSeeds")
    @Tag(Tags.Slow)
    void oneHundredElementsWithWeakReads(long seed) {
        boolean dbTracksReadVersionOnRead = fdb.isTrackLastSeenVersionOnRead();
        boolean dbTracksReadVersionOnCommit = fdb.isTrackLastSeenVersionOnCommit();
        try {
            fdb.setTrackLastSeenVersion(true);
            Random r = new Random(seed);
            List<TestRecords1Proto.MySimpleRecord> records = Stream.generate(() ->
                    TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(r.nextLong()).setNumValue2(r.nextInt(10)).build()
            ).limit(100).sorted(Comparator.comparingLong(TestRecords1Proto.MySimpleRecord::getRecNo)).collect(Collectors.toList());
            valueRebuild(records);
        } finally {
            fdb.setTrackLastSeenVersionOnRead(dbTracksReadVersionOnRead);
            fdb.setTrackLastSeenVersionOnCommit(dbTracksReadVersionOnCommit);
        }
    }

    @ParameterizedTest
    @MethodSource("randomSeeds")
    @Tag(Tags.Slow)
    void oneHundredElementsParallel(long seed) {
        Random r = new Random(seed);
        List<TestRecords1Proto.MySimpleRecord> records = Stream.generate(() ->
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(r.nextLong() / 2).setNumValue2(r.nextInt(10)).build()
        ).limit(100).sorted(Comparator.comparingLong(TestRecords1Proto.MySimpleRecord::getRecNo)).collect(Collectors.toList());
        valueRebuild(records, null, 5, false);
    }

    @ParameterizedTest
    @MethodSource("randomSeeds")
    @Tag(Tags.Slow)
    void oneHundredElementsParallelWithWeakReads(long seed) {
        boolean dbTracksReadVersionOnRead = fdb.isTrackLastSeenVersionOnRead();
        boolean dbTracksReadVersionOnCommit = fdb.isTrackLastSeenVersionOnCommit();
        try {
            fdb.setTrackLastSeenVersion(true);
            Random r = new Random(seed);
            List<TestRecords1Proto.MySimpleRecord> records = Stream.generate(() ->
                    TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(r.nextLong() / 2).setNumValue2(r.nextInt(10)).build()
            ).limit(100).sorted(Comparator.comparingLong(TestRecords1Proto.MySimpleRecord::getRecNo)).collect(Collectors.toList());
            valueRebuild(records, null, 5, false);
        } finally {
            fdb.setTrackLastSeenVersionOnRead(dbTracksReadVersionOnRead);
            fdb.setTrackLastSeenVersionOnCommit(dbTracksReadVersionOnCommit);
        }
    }

    @ParameterizedTest
    @MethodSource("randomSeeds")
    @Tag(Tags.Slow)
    void oneHundredElementsParallelOverlap(long seed) {
        Random r = new Random(seed);
        List<TestRecords1Proto.MySimpleRecord> records = Stream.generate(() ->
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(r.nextLong() / 2).setNumValue2(r.nextInt(10)).build()
        ).limit(100).sorted(Comparator.comparingLong(TestRecords1Proto.MySimpleRecord::getRecNo)).collect(Collectors.toList());
        valueRebuild(records, null, 5, true);
    }

    @Test
    void tenSplitElements() {
        String bigOlString = Strings.repeat("x", SplitHelper.SPLIT_RECORD_SIZE + 2);
        List<TestRecords1Proto.MySimpleRecord> records = IntStream.range(-5, 5).mapToObj(val ->
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(val * 431).setNumValue2(Math.abs(val) % 5).setStrValueIndexed(bigOlString).build()
        ).collect(Collectors.toList());
        valueRebuild(records, true);
    }

    @Test
    void fiftySplitElements() {
        // Surely this can all fit in memory, no problem, right?
        String bigOlString = Strings.repeat("x", SplitHelper.SPLIT_RECORD_SIZE + 2);
        List<TestRecords1Proto.MySimpleRecord> records = IntStream.range(-25, 25).mapToObj(val ->
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(val * 39).setNumValue2(Math.abs(val) % 5).setStrValueIndexed(bigOlString).build()
        ).collect(Collectors.toList());
        valueRebuild(records, true);
    }

    @Test
    void withNullKey1() {
        List<TestRecords1Proto.MySimpleRecord> records = Arrays.asList(
                TestRecords1Proto.MySimpleRecord.newBuilder().setNumValue2(17).build(),
                TestRecords1Proto.MySimpleRecord.newBuilder().setNumValue2(76).setRecNo(123).build()
        );
        valueRebuild(records);
    }

    @Test
    void withNullKey2() {
        List<TestRecords1Proto.MySimpleRecord> records = Collections.singletonList(
                TestRecords1Proto.MySimpleRecord.newBuilder().setNumValue2(17).build()
        );
        valueRebuild(records);
    }

    @Test
    void withNullValue() {
        List<TestRecords1Proto.MySimpleRecord> records = Arrays.asList(
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(1066).build(),
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(1776).build(),
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(1828).setNumValue2(100).build()
        );
        valueRebuild(records);
    }

    @ParameterizedTest
    @MethodSource("randomSeeds")
    @Tag(Tags.Slow)
    void somePreloaded(long seed) {
        Random r = new Random(seed);
        List<TestRecords1Proto.MySimpleRecord> records = Stream.generate(() ->
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(r.nextLong()).setNumValue2(r.nextInt(10)).build()
        ).limit(75).sorted(Comparator.comparingLong(TestRecords1Proto.MySimpleRecord::getRecNo)).collect(Collectors.toList());
        openSimpleMetaData(metaDataBuilder -> {
            Index index = new Index("newIndex", field("num_value_2"));
            metaDataBuilder.addIndex("MySimpleRecord", index);
        });
        try (FDBRecordContext context = openContext(false)) {
            records.stream().filter(msg -> msg.getRecNo() % 2 == 0).forEach(recordStore::saveRecord);
            recordStore.uncheckedMarkIndexReadable("newIndex").join();
            context.commit();
        }
        valueRebuild(records);
    }

    @ParameterizedTest
    @MethodSource("randomSeeds")
    @Tag(Tags.Slow)
    void addWhileBuilding(long seed) {
        Random r = new Random(seed);
        List<TestRecords1Proto.MySimpleRecord> records = Stream.generate(() ->
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(r.nextLong()).setNumValue2(r.nextInt(10)).build()
        ).limit(100).sorted(Comparator.comparingLong(TestRecords1Proto.MySimpleRecord::getRecNo)).collect(Collectors.toList());
        List<TestRecords1Proto.MySimpleRecord> recordsWhileBuilding = Stream.generate(() ->
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(r.nextLong()).setNumValue2(r.nextInt(10)).build()
        ).limit(100).sorted(Comparator.comparingLong(TestRecords1Proto.MySimpleRecord::getRecNo)).collect(Collectors.toList());
        valueRebuild(records, recordsWhileBuilding);
    }

    @ParameterizedTest
    @MethodSource("randomSeeds")
    @Tag(Tags.Slow)
    void addWhileBuildingParallel(long seed) {
        Random r = new Random(seed);
        List<TestRecords1Proto.MySimpleRecord> records = Stream.generate(() ->
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(r.nextLong() / 2).setNumValue2(r.nextInt(10)).build()
        ).limit(150).sorted(Comparator.comparingLong(TestRecords1Proto.MySimpleRecord::getRecNo)).collect(Collectors.toList());
        List<TestRecords1Proto.MySimpleRecord> recordsWhileBuilding = Stream.generate(() ->
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(r.nextLong() / 2).setNumValue2(r.nextInt(10)).build()
        ).limit(150).sorted(Comparator.comparingLong(TestRecords1Proto.MySimpleRecord::getRecNo)).collect(Collectors.toList());
        valueRebuild(records, recordsWhileBuilding, 5, false);
    }

    @ParameterizedTest
    @MethodSource("randomSeeds")
    @Tag(Tags.Slow)
    void addSequentialWhileBuilding(long seed) {
        Random r = new Random(seed);
        List<TestRecords1Proto.MySimpleRecord> records = LongStream.range(0, 100).mapToObj(val ->
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(val).setNumValue2(r.nextInt(20)).build()
        ).collect(Collectors.toList());
        List<TestRecords1Proto.MySimpleRecord> recordsWhileBuilding = Stream.generate(() ->
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(r.nextInt(100)).setNumValue2(r.nextInt(20) + 20).build()
        ).limit(100).sorted(Comparator.comparingLong(TestRecords1Proto.MySimpleRecord::getRecNo)).collect(Collectors.toList());
        valueRebuild(records, recordsWhileBuilding);
    }

    @ParameterizedTest
    @MethodSource("randomSeeds")
    @Tag(Tags.Slow)
    void addSequentialWhileBuildingParallel(long seed) {
        Random r = new Random(seed);
        List<TestRecords1Proto.MySimpleRecord> records = LongStream.range(0, 100).mapToObj( val ->
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(val).setNumValue2(r.nextInt(20)).build()
        ).collect(Collectors.toList());
        List<TestRecords1Proto.MySimpleRecord> recordsWhileBuilding = Stream.generate(() ->
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(r.nextInt(100)).setNumValue2(r.nextInt(20) + 20).build()
        ).limit(100).sorted(Comparator.comparingLong(TestRecords1Proto.MySimpleRecord::getRecNo)).collect(Collectors.toList());
        valueRebuild(records, recordsWhileBuilding, 5, false);
    }
}
