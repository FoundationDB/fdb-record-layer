/*
 * OnlineIndexerBuildSumIndexTest.java
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

import com.apple.foundationdb.record.FunctionNames;
import com.apple.foundationdb.record.IsolationLevel;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexAggregateFunction;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.MetaDataException;
import com.apple.foundationdb.record.provider.foundationdb.query.FDBRestrictedIndexQueryTest;
import com.apple.test.Tags;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Test building sum indexes.
 */
public abstract class OnlineIndexerBuildSumIndexTest extends OnlineIndexerBuildIndexTest {

    private OnlineIndexerBuildSumIndexTest(boolean safeBuild) {
        super(safeBuild);
    }

    private void sumRebuild(@Nonnull List<TestRecords1Proto.MySimpleRecord> records, @Nullable List<TestRecords1Proto.MySimpleRecord> recordsWhileBuilding,
                            int agents, boolean overlap) {
        Index index = new Index("newSumIndex", field("num_value_2").ungrouped(), IndexTypes.SUM);
        IndexAggregateFunction aggregateFunction = new IndexAggregateFunction(FunctionNames.SUM, index.getRootExpression(), index.getName());

        Runnable beforeBuild = new Runnable() {
            @SuppressWarnings("try")
            @Override
            public void run() {
                try (FDBRecordContext context = openContext()) {
                    metaData.getIndex(index.getName());
                } catch (MetaDataException e) {
                    assertEquals("Index newSumIndex not defined", e.getMessage());
                }
            }
        };

        Runnable afterBuild = new Runnable() {
            @SuppressWarnings("try")
            @Override
            public void run() {
                metaData.getIndex(index.getName());
                try (FDBRecordContext context = openContext()) {
                    FDBRestrictedIndexQueryTest.assertThrowsAggregateFunctionNotSupported(() ->
                                    recordStore.evaluateAggregateFunction(Collections.singletonList("MySimpleRecord"),
                                            aggregateFunction, TupleRange.ALL, IsolationLevel.SNAPSHOT),
                            "newSumIndex.sum(Field { 'num_value_2' None} group 1)");
                } catch (Exception e) {
                    fail();
                }
            }
        };

        List<TestRecords1Proto.MySimpleRecord> updatedRecords;
        if (recordsWhileBuilding == null || recordsWhileBuilding.size() == 0) {
            updatedRecords = records;
        } else {
            updatedRecords = updated(records, recordsWhileBuilding);
        }

        Runnable afterReadable = new Runnable() {
            @SuppressWarnings("try")
            @Override
            public void run() {
                try (FDBRecordContext context = openContext()) {
                    long sum = recordStore.evaluateAggregateFunction(Collections.singletonList("MySimpleRecord"), aggregateFunction, TupleRange.ALL, IsolationLevel.SNAPSHOT).join().getLong(0);
                    long expected = updatedRecords.stream().mapToInt(msg -> msg.hasNumValue2() ? msg.getNumValue2() : 0).sum();
                    assertEquals(expected, sum);
                }
            }
        };

        singleRebuild(records, recordsWhileBuilding, agents, overlap, false, index, beforeBuild, afterBuild, afterReadable);
    }

    private void sumRebuild(@Nonnull List<TestRecords1Proto.MySimpleRecord> records, @Nullable List<TestRecords1Proto.MySimpleRecord> recordsWhileBuilding) {
        sumRebuild(records, recordsWhileBuilding, 1, false);
    }

    private void sumRebuild(@Nonnull List<TestRecords1Proto.MySimpleRecord> records) {
        sumRebuild(records, null);
    }


    @Test
    public void emptyRangeSum() {
        sumRebuild(Collections.emptyList());
    }

    @Test
    public void singleElementSum() {
        TestRecords1Proto.MySimpleRecord record = TestRecords1Proto.MySimpleRecord.newBuilder()
                .setRecNo(1517)
                .setNumValue2(95)
                .build();
        sumRebuild(Collections.singletonList(record));
    }

    @Test
    @Tag(Tags.Slow)
    public void oneHundredElementsSum() {
        Random r = new Random(0x5ca1ab1e);
        List<TestRecords1Proto.MySimpleRecord> records = Stream.generate(() ->
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(r.nextLong()).setNumValue2(r.nextInt(10)).build()
        ).limit(100).sorted(Comparator.comparingLong(TestRecords1Proto.MySimpleRecord::getRecNo)).collect(Collectors.toList());
        sumRebuild(records);
    }

    @Test
    @Tag(Tags.Slow)
    public void oneHundredElementsParallelSum() {
        Random r = new Random(0x5ca1ab1e);
        List<TestRecords1Proto.MySimpleRecord> records = Stream.generate(() ->
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(r.nextLong() / 2).setNumValue2(r.nextInt(10)).build()
        ).limit(100).sorted(Comparator.comparingLong(TestRecords1Proto.MySimpleRecord::getRecNo)).collect(Collectors.toList());
        sumRebuild(records, null, 5, false);
    }

    @Test
    @Tag(Tags.Slow)
    public void oneHundredElementsParallelOverlapSum() {
        Random r = new Random(0xf005ba11);
        List<TestRecords1Proto.MySimpleRecord> records = Stream.generate(() ->
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(r.nextLong() / 2).setNumValue2(r.nextInt(10)).build()
        ).limit(100).sorted(Comparator.comparingLong(TestRecords1Proto.MySimpleRecord::getRecNo)).collect(Collectors.toList());
        sumRebuild(records, null, 5, true);
    }

    @Test
    @Tag(Tags.Slow)
    public void addWhileBuildingSum() {
        Random r = new Random(0xdeadc0de);
        List<TestRecords1Proto.MySimpleRecord> records = Stream.generate(() ->
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(r.nextLong()).setNumValue2(r.nextInt(10)).build()
        ).limit(100).sorted(Comparator.comparingLong(TestRecords1Proto.MySimpleRecord::getRecNo)).collect(Collectors.toList());
        List<TestRecords1Proto.MySimpleRecord> recordsWhileBuilding = Stream.generate(() ->
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(r.nextLong()).setNumValue2(r.nextInt(10)).build()
        ).limit(100).sorted(Comparator.comparingLong(TestRecords1Proto.MySimpleRecord::getRecNo)).collect(Collectors.toList());
        sumRebuild(records, recordsWhileBuilding);
    }

    @Test
    @Tag(Tags.Slow)
    public void addWhileBuildingParallelSum() {
        Random r = new Random(0xdeadc0de);
        List<TestRecords1Proto.MySimpleRecord> records = Stream.generate(() ->
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(r.nextLong() / 2).setNumValue2(r.nextInt(10)).build()
        ).limit(200).sorted(Comparator.comparingLong(TestRecords1Proto.MySimpleRecord::getRecNo)).collect(Collectors.toList());
        List<TestRecords1Proto.MySimpleRecord> recordsWhileBuilding = Stream.generate(() ->
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(r.nextLong() / 2).setNumValue2(r.nextInt(10)).build()
        ).limit(200).sorted(Comparator.comparingLong(TestRecords1Proto.MySimpleRecord::getRecNo)).collect(Collectors.toList());
        sumRebuild(records, recordsWhileBuilding, 5, false);
    }

    @Test
    @Tag(Tags.Slow)
    public void somePreloadedSum() {
        Random r = new Random(0x5ca1ab1e);
        List<TestRecords1Proto.MySimpleRecord> records = Stream.generate(() ->
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(r.nextLong()).setNumValue2(r.nextInt(10)).build()
        ).limit(100).sorted(Comparator.comparingLong(TestRecords1Proto.MySimpleRecord::getRecNo)).collect(Collectors.toList());
        openSimpleMetaData(metaDataBuilder -> {
            Index index = new Index("newSumIndex", field("num_value_2").ungrouped(), IndexTypes.SUM);
            metaDataBuilder.addIndex("MySimpleRecord", index);
        });
        try (FDBRecordContext context = openContext(false)) {
            recordStore.markIndexWriteOnly("newSumIndex").join();
            context.commit();
        }
        try (FDBRecordContext context = openContext(false)) {
            records.stream().filter(msg -> msg.getRecNo() % 2 == 0).forEach(recordStore::saveRecord);
            recordStore.uncheckedMarkIndexReadable("newSumIndex").join();
            context.commit();
        }
        sumRebuild(records);
    }

    @Test
    @Tag(Tags.Slow)
    public void addSequentialWhileBuildingSum() {
        Random r = new Random(0xba5eba11);
        List<TestRecords1Proto.MySimpleRecord> records = LongStream.range(0, 100).mapToObj(val ->
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(val).setNumValue2(r.nextInt(20)).build()
        ).collect(Collectors.toList());
        List<TestRecords1Proto.MySimpleRecord> recordsWhileBuilding = Stream.generate(() ->
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(r.nextInt(100)).setNumValue2(r.nextInt(20) + 20).build()
        ).limit(100).sorted(Comparator.comparingLong(TestRecords1Proto.MySimpleRecord::getRecNo)).collect(Collectors.toList());
        sumRebuild(records, recordsWhileBuilding);
    }

    @Test
    @Tag(Tags.Slow)
    public void addSequentialWhileBuildingParallelSum() {
        Random r = new Random(0xba5eba11);
        List<TestRecords1Proto.MySimpleRecord> records = LongStream.range(0, 100).mapToObj( val ->
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(val).setNumValue2(r.nextInt(20)).build()
        ).collect(Collectors.toList());
        List<TestRecords1Proto.MySimpleRecord> recordsWhileBuilding = Stream.generate(() ->
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(r.nextInt(100)).setNumValue2(r.nextInt(20) + 20).build()
        ).limit(100).sorted(Comparator.comparingLong(TestRecords1Proto.MySimpleRecord::getRecNo)).collect(Collectors.toList());
        sumRebuild(records, recordsWhileBuilding, 5, false);
    }

    /**
     * Build indexes with the unchecked index build interfaces.
     */
    public static class Unsafe extends OnlineIndexerBuildSumIndexTest {
        Unsafe() {
            super(false);
        }
    }

    /**
     * Build indexes with the safe index build interfaces.
     */
    public static class Safe extends OnlineIndexerBuildSumIndexTest {
        Safe() {
            super(true);
        }
    }
}
