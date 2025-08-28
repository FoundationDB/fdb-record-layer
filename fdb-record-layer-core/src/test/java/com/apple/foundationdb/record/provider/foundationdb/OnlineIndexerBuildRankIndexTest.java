/*
 * OnlineIndexerBuildRankIndexTest.java
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

import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexRecordFunction;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.test.Tags;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.TreeSet;
import java.util.concurrent.CompletionException;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Test building value indexes.
 */
@SuppressWarnings("try")
class OnlineIndexerBuildRankIndexTest extends OnlineIndexerBuildIndexTest {

    private void rankRebuild(@Nonnull List<TestRecords1Proto.MySimpleRecord> records, @Nullable List<TestRecords1Proto.MySimpleRecord> recordsWhileBuilding,
                             int agents, boolean overlap) {
        final OnlineIndexerTestRecordHandler<TestRecords1Proto.MySimpleRecord> recordHandler = OnlineIndexerTestSimpleRecordHandler.instance();
        final Index index = new Index("newRankIndex", field("num_value_2").ungrouped(), IndexTypes.RANK);
        final IndexRecordFunction<Long> recordFunction = (IndexRecordFunction<Long>)Query.rank("num_value_2").getFunction();

        final Runnable beforeBuild = () -> {
            try (FDBRecordContext context = openContext()) {
                for (TestRecords1Proto.MySimpleRecord record : records) {
                    try {
                        recordStore.evaluateRecordFunction(recordFunction, createStoredMessage(recordHandler, record)).join();
                        fail("Somehow evaluated rank");
                    } catch (RecordCoreException e) {
                        assertEquals("Record function rank(Field { 'num_value_2' None} group 1) requires appropriate index on MySimpleRecord", e.getMessage());
                    }
                }

                // Either we get an exception, or the record store is empty.
                try {
                    RecordQuery query = RecordQuery.newBuilder()
                            .setRecordType("MySimpleRecord")
                            .setFilter(Query.rank("num_value_2").equalsValue(0L))
                            .build();
                    RecordQueryPlan plan = planner.plan(query);
                    assertEquals("SCAN(<,>) | TFILTER MySimpleRecord | QCFILTER rank(Field { 'num_value_2' None} group 1) EQUALS 0", plan.toString());
                    Optional<?> first = recordStore.executeQuery(plan).first().join();
                    assertTrue(!first.isPresent(), "non-empty range with rank rebuild");
                } catch (CompletionException e) {
                    assertNotNull(e.getCause());
                    assertThat(e.getCause(), instanceOf(RecordCoreException.class));
                    assertEquals("Record function rank(Field { 'num_value_2' None} group 1) requires appropriate index on MySimpleRecord", e.getCause().getMessage());
                }
            }
        };

        List<TestRecords1Proto.MySimpleRecord> updatedRecords = updated(recordHandler, records, recordsWhileBuilding, null);

        final Runnable afterBuild = () -> {
            try (FDBRecordContext context = openContext()) {
                for (TestRecords1Proto.MySimpleRecord record : updatedRecords) {
                    try {
                        recordStore.evaluateRecordFunction(recordFunction, createStoredMessage(recordHandler, record)).join();
                        fail("Somehow evaluated rank");
                    } catch (RecordCoreException e) {
                        assertEquals("Record function rank(Field { 'num_value_2' None} group 1) requires appropriate index on MySimpleRecord", e.getMessage());
                    }
                }

                // Either we get an exception, or the record store is empty.
                try {
                    RecordQuery query = RecordQuery.newBuilder()
                            .setRecordType("MySimpleRecord")
                            .setFilter(Query.rank("num_value_2").equalsValue(0L))
                            .build();
                    RecordQueryPlan plan = planner.plan(query);
                    assertEquals("SCAN(<,>) | TFILTER MySimpleRecord | QCFILTER rank(Field { 'num_value_2' None} group 1) EQUALS 0", plan.toString());
                    Optional<?> first = recordStore.executeQuery(plan).first().join();
                    assertTrue(!first.isPresent(), "non-empty range with rank rebuild");
                } catch (CompletionException e) {
                    assertNotNull(e.getCause());
                    assertThat(e.getCause(), instanceOf(RecordCoreException.class));
                    assertEquals("Record function rank(Field { 'num_value_2' None} group 1) requires appropriate index on MySimpleRecord", e.getCause().getMessage());
                }
            }
        };

        TreeSet<Integer> values = new TreeSet<>();
        boolean hasNull = false;
        for (TestRecords1Proto.MySimpleRecord record : updatedRecords) {
            if (!record.hasNumValue2()) {
                hasNull = true;
            } else {
                values.add(record.getNumValue2());
            }
        }

        long curr = 0;
        Map<Integer, Long> ranks = new HashMap<>();
        if (hasNull) {
            curr += 1;
        }
        for (Integer value : values) {
            ranks.put(value, curr);
            curr += 1;
        }

        final Runnable afterReadable = () -> {
            try (FDBRecordContext context = openContext()) {
                for (TestRecords1Proto.MySimpleRecord record : updatedRecords) {
                    Long rank = recordStore.evaluateRecordFunction(recordFunction, createStoredMessage(recordHandler, record)).join();
                    if (!record.hasNumValue2()) {
                        assertEquals(0L, rank.longValue());
                    } else {
                        assertEquals(ranks.get(record.getNumValue2()), rank);
                    }

                    RecordQuery query = RecordQuery.newBuilder()
                            .setRecordType("MySimpleRecord")
                            .setFilter(Query.rank("num_value_2").equalsValue(rank))
                            .build();
                    RecordQueryPlan plan = planner.plan(query);
                    assertEquals("ISCAN(newRankIndex [[" + rank + "],[" + rank + "]] BY_RANK)", plan.toString());
                    Optional<TestRecords1Proto.MySimpleRecord> retrieved = recordStore.executeQuery(plan).map(rec -> TestRecords1Proto.MySimpleRecord.newBuilder().mergeFrom(rec.getRecord()).build())
                            .filter(rec -> rec.getRecNo() == record.getRecNo()).first().join();
                    assertTrue(retrieved.isPresent(), "empty range after rank index build");
                    assertEquals(record, retrieved.get());
                }
            }
        };

        singleRebuild(recordHandler, records, recordsWhileBuilding, null, agents, overlap, false, index, null, beforeBuild, afterBuild, afterReadable);
    }

    private void rankRebuild(@Nonnull List<TestRecords1Proto.MySimpleRecord> records, @Nullable List<TestRecords1Proto.MySimpleRecord> recordsWhileBuilding) {
        rankRebuild(records, recordsWhileBuilding, 1, false);
    }

    private void rankRebuild(@Nonnull List<TestRecords1Proto.MySimpleRecord> records) {
        rankRebuild(records, null);
    }

    @Test
    void emptyRangeRank() {
        rankRebuild(Collections.emptyList());
    }

    @Test
    void singleElementRank() {
        TestRecords1Proto.MySimpleRecord record = TestRecords1Proto.MySimpleRecord.newBuilder()
                .setRecNo(1517)
                .setNumValue2(95)
                .build();
        rankRebuild(Collections.singletonList(record));
    }

    @ParameterizedTest
    @MethodSource("randomSeeds")
    @Tag(Tags.Slow)
    void oneHundredElementsRank(long seed) {
        Random r = new Random(seed);
        List<TestRecords1Proto.MySimpleRecord> records = Stream.generate(() ->
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(r.nextLong()).setNumValue2(r.nextInt(10)).build()
        ).limit(100).sorted(Comparator.comparingLong(TestRecords1Proto.MySimpleRecord::getRecNo)).collect(Collectors.toList());
        rankRebuild(records);
    }

    @ParameterizedTest
    @MethodSource("randomSeeds")
    @Tag(Tags.Slow)
    void oneHundredElementsParallelRank(long seed) {
        Random r = new Random(seed);
        List<TestRecords1Proto.MySimpleRecord> records = Stream.generate(() ->
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(r.nextLong() / 2).setNumValue2(r.nextInt(10)).build()
        ).limit(100).sorted(Comparator.comparingLong(TestRecords1Proto.MySimpleRecord::getRecNo)).collect(Collectors.toList());
        rankRebuild(records, null, 5, false);
    }

    @ParameterizedTest
    @MethodSource("randomSeeds")
    @Tag(Tags.Slow)
    public void oneHundredElementsParallelOverlapRank(long seed) {
        Random r = new Random(seed);
        List<TestRecords1Proto.MySimpleRecord> records = Stream.generate(() ->
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(r.nextLong() / 2).setNumValue2(r.nextInt(10)).build()
        ).limit(100).sorted(Comparator.comparingLong(TestRecords1Proto.MySimpleRecord::getRecNo)).collect(Collectors.toList());
        rankRebuild(records, null, 5, true);
    }

    @ParameterizedTest
    @MethodSource("randomSeeds")
    @Tag(Tags.Slow)
    public void addWhileBuildingRank(long seed) {
        Random r = new Random(seed);
        List<TestRecords1Proto.MySimpleRecord> records = Stream.generate(() ->
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(r.nextLong()).setNumValue2(r.nextInt(10)).build()
        ).limit(100).sorted(Comparator.comparingLong(TestRecords1Proto.MySimpleRecord::getRecNo)).collect(Collectors.toList());
        List<TestRecords1Proto.MySimpleRecord> recordsWhileBuilding = Stream.generate(() ->
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(r.nextLong()).setNumValue2(r.nextInt(10)).build()
        ).limit(100).sorted(Comparator.comparingLong(TestRecords1Proto.MySimpleRecord::getRecNo)).collect(Collectors.toList());
        rankRebuild(records, recordsWhileBuilding);
    }

    @ParameterizedTest
    @MethodSource("randomSeeds")
    @Tag(Tags.Slow)
    void addWhileBuildingParallelRank(long seed) {
        Random r = new Random(seed);
        List<TestRecords1Proto.MySimpleRecord> records = Stream.generate(() ->
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(r.nextLong() / 2).setNumValue2(r.nextInt(10)).build()
        ).limit(150).sorted(Comparator.comparingLong(TestRecords1Proto.MySimpleRecord::getRecNo)).collect(Collectors.toList());
        List<TestRecords1Proto.MySimpleRecord> recordsWhileBuilding = Stream.generate(() ->
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(r.nextLong() / 2).setNumValue2(r.nextInt(10)).build()
        ).limit(150).sorted(Comparator.comparingLong(TestRecords1Proto.MySimpleRecord::getRecNo)).collect(Collectors.toList());
        rankRebuild(records, recordsWhileBuilding, 5, false);
    }

    @ParameterizedTest
    @MethodSource("randomSeeds")
    @Tag(Tags.Slow)
    void somePreloadedRank(long seed) {
        Random r = new Random(seed);
        List<TestRecords1Proto.MySimpleRecord> records = Stream.generate(() ->
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(r.nextLong()).setNumValue2(r.nextInt(10)).build()
        ).limit(100).sorted(Comparator.comparingLong(TestRecords1Proto.MySimpleRecord::getRecNo)).collect(Collectors.toList());
        openSimpleMetaData(metaDataBuilder -> {
            Index index = new Index("newRankIndex", field("num_value_2").ungrouped(), IndexTypes.RANK);
            metaDataBuilder.addIndex("MySimpleRecord", index);
        });
        try (FDBRecordContext context = openContext(false)) {
            records.stream().filter(msg -> msg.getRecNo() % 2 == 0).forEach(recordStore::saveRecord);
            recordStore.uncheckedMarkIndexReadable("newRankIndex").join();
            context.commit();
        }
        rankRebuild(records);
    }

    @ParameterizedTest
    @MethodSource("randomSeeds")
    @Tag(Tags.Slow)
    void addSequentialWhileBuildingRank(long seed) {
        Random r = new Random(seed);
        List<TestRecords1Proto.MySimpleRecord> records = LongStream.range(0, 100).mapToObj(val ->
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(val).setNumValue2(r.nextInt(20)).build()
        ).collect(Collectors.toList());
        List<TestRecords1Proto.MySimpleRecord> recordsWhileBuilding = Stream.generate(() ->
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(r.nextInt(100)).setNumValue2(r.nextInt(20) + 20).build()
        ).limit(100).sorted(Comparator.comparingLong(TestRecords1Proto.MySimpleRecord::getRecNo)).collect(Collectors.toList());
        rankRebuild(records, recordsWhileBuilding);
    }

    @ParameterizedTest
    @MethodSource("randomSeeds")
    @Tag(Tags.Slow)
    void addSequentialWhileBuildingParallelRank(long seed) {
        Random r = new Random(seed);
        List<TestRecords1Proto.MySimpleRecord> records = LongStream.range(0, 100).mapToObj( val ->
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(val).setNumValue2(r.nextInt(20)).build()
        ).collect(Collectors.toList());
        List<TestRecords1Proto.MySimpleRecord> recordsWhileBuilding = Stream.generate(() ->
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(r.nextInt(100)).setNumValue2(r.nextInt(20) + 20).build()
        ).limit(100).sorted(Comparator.comparingLong(TestRecords1Proto.MySimpleRecord::getRecNo)).collect(Collectors.toList());
        rankRebuild(records, recordsWhileBuilding, 5, false);
    }
}
