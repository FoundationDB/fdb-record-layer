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
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.RandomizedTestUtils;
import com.apple.test.Tags;
import com.google.protobuf.Descriptors;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Test building sum indexes.
 */
class OnlineIndexerBuildSumIndexTest extends OnlineIndexerBuildIndexTest {
    private static final Index REC_NO_INDEX = new Index("simple$rec_no", "rec_no");
    private static final Index NUM_VALUE_2_INDEX = new Index("simple$num_value_2", "num_value_2");

    private static final Function<List<TestRecords1Proto.MySimpleRecord>, Long> ALL_RECORDS_SUM = records ->
            records.stream()
                    .mapToLong(TestRecords1Proto.MySimpleRecord::getNumValue2)
                    .sum();
    private static final Function<List<TestRecords1Proto.MySimpleRecord>, Long> ODD_NUM_VALUE_3_RECORDS_SUM = records ->
            records.stream()
                    .filter(rec -> rec.getNumValue3Indexed() % 2 != 0)
                    .mapToLong(TestRecords1Proto.MySimpleRecord::getNumValue2)
                    .sum();

    private static IndexMaintenanceFilter filterOddsForIndexes(@Nonnull Collection<String> indexNames) {
        return (index, record) -> {
            if (!indexNames.contains(index.getName())) {
                return IndexMaintenanceFilter.IndexValues.ALL;
            }
            if (!record.getDescriptorForType().equals(TestRecords1Proto.MySimpleRecord.getDescriptor())) {
                return IndexMaintenanceFilter.IndexValues.ALL;
            }
            Descriptors.FieldDescriptor numValue3Field = TestRecords1Proto.MySimpleRecord.getDescriptor()
                    .findFieldByNumber(TestRecords1Proto.MySimpleRecord.NUM_VALUE_3_INDEXED_FIELD_NUMBER);
            Object numValue3 = record.getField(numValue3Field);
            if (numValue3 == null || ((Number)numValue3).longValue() % 2 != 0) {
                return IndexMaintenanceFilter.IndexValues.ALL;
            } else {
                return IndexMaintenanceFilter.IndexValues.NONE;
            }
        };
    }

    @SuppressWarnings("try")
    private void sumRebuild(@Nonnull List<TestRecords1Proto.MySimpleRecord> records,
                            @Nullable List<TestRecords1Proto.MySimpleRecord> recordsWhileBuilding,
                            @Nullable List<Long> deletedIds,
                            @Nullable IndexMaintenanceFilter filter,
                            @Nonnull Function<List<TestRecords1Proto.MySimpleRecord>, Long> sumFunction,
                            @Nullable Index sourceIndex,
                            int agents,
                            boolean overlap) {
        final OnlineIndexerTestRecordHandler<TestRecords1Proto.MySimpleRecord> recordHandler = OnlineIndexerTestSimpleRecordHandler.instance();
        setIndexMaintenanceFilter(filter);
        Index index = new Index("newSumIndex", field("num_value_2").ungrouped(), IndexTypes.SUM);
        IndexAggregateFunction aggregateFunction = new IndexAggregateFunction(FunctionNames.SUM, index.getRootExpression(), index.getName());

        Runnable beforeBuild = () -> {
            try (FDBRecordContext context = openContext()) {
                metaData.getIndex(index.getName());
            } catch (MetaDataException e) {
                assertEquals("Index newSumIndex not defined", e.getMessage());
            }
        };

        Runnable afterBuild = () -> {
            metaData.getIndex(index.getName());
            try (FDBRecordContext context = openContext()) {
                FDBRestrictedIndexQueryTest.assertThrowsAggregateFunctionNotSupported(() ->
                                recordStore.evaluateAggregateFunction(Collections.singletonList("MySimpleRecord"),
                                        aggregateFunction, TupleRange.ALL, IsolationLevel.SNAPSHOT),
                        "newSumIndex.sum(Field { 'num_value_2' None} group 1)");
            }
        };

        final List<Tuple> deletePrimaryKeys = deletedIds == null ? null : deletedIds.stream().map(Tuple::from).collect(Collectors.toList());
        List<TestRecords1Proto.MySimpleRecord> updatedRecords = updated(recordHandler, records, recordsWhileBuilding, deletePrimaryKeys);
        long expected = sumFunction.apply(updatedRecords);

        Runnable afterReadable = () -> {
            try (FDBRecordContext context = openContext()) {
                long sum = recordStore.evaluateAggregateFunction(Collections.singletonList("MySimpleRecord"), aggregateFunction, TupleRange.ALL, IsolationLevel.SNAPSHOT).join().getLong(0);
                assertEquals(expected, sum);
            }
        };

        singleRebuild(recordHandler, records, recordsWhileBuilding, deletePrimaryKeys, agents, overlap, false, index, sourceIndex, beforeBuild, afterBuild, afterReadable);
    }

    @SuppressWarnings("try")
    private void sumRebuild(@Nonnull List<TestRecords1Proto.MySimpleRecord> records,
                            @Nullable List<TestRecords1Proto.MySimpleRecord> recordsWhileBuilding,
                            @Nullable List<Long> deletedIds,
                            @Nullable Index sourceIndex,
                            int agents,
                            boolean overlap) {
        sumRebuild(records, recordsWhileBuilding, deletedIds, null, ALL_RECORDS_SUM, sourceIndex, agents, overlap);
    }

    private void sumRebuild(@Nonnull List<TestRecords1Proto.MySimpleRecord> records,
                            @Nullable List<TestRecords1Proto.MySimpleRecord> recordsWhileBuilding,
                            @Nullable Index sourceIndex) {
        sumRebuild(records, recordsWhileBuilding, null, sourceIndex, 1, false);
    }

    private void sumRebuild(@Nonnull List<TestRecords1Proto.MySimpleRecord> records, @Nullable List<TestRecords1Proto.MySimpleRecord> recordsWhileBuilding) {
        sumRebuild(records, recordsWhileBuilding, null);
    }

    private void sumRebuild(@Nonnull List<TestRecords1Proto.MySimpleRecord> records) {
        sumRebuild(records, null);
    }

    private void sumRebuildFiltered(@Nonnull List<TestRecords1Proto.MySimpleRecord> records,
                                    @Nullable List<TestRecords1Proto.MySimpleRecord> recordsWhileBuilding,
                                    @Nullable List<Long> deletedIds,
                                    @Nullable Index sourceIndex,
                                    boolean filterSource) {
        Set<String> filteredIndexes = new HashSet<>();
        filteredIndexes.add("newSumIndex");
        if (filterSource) {
            Assumptions.assumeTrue(sourceIndex != null);
            filteredIndexes.add(sourceIndex.getName());
        }
        IndexMaintenanceFilter filter = filterOddsForIndexes(filteredIndexes);
        sumRebuild(records, recordsWhileBuilding, deletedIds, filter, ODD_NUM_VALUE_3_RECORDS_SUM, sourceIndex, 1, false);
    }

    private static List<Index> sourceIndexes() {
        return Arrays.asList(null, REC_NO_INDEX, NUM_VALUE_2_INDEX);
    }

    @Nonnull
    static Stream<Arguments> sourceIndexesAndRandomSeeds() {
        List<Index> sourceIndexes = sourceIndexes();
        return Stream.concat(
                sourceIndexes.stream()
                        .flatMap(index -> Stream.of(0x5ca1ab1eL, 0xba5eba11, 0xda7aba5e)
                                .map(seed -> Arguments.of(index, seed))),
                RandomizedTestUtils.randomArguments(r -> {
                    Index sourceIndex = sourceIndexes.get(r.nextInt(sourceIndexes.size()));
                    long seed = r.nextLong();
                    return Arguments.of(sourceIndex, seed);
                })
        );
    }

    @Nonnull
    static Stream<Arguments> sourceIndexesFilteredAndRandomSeeds() {
        List<Index> sourceIndexes = sourceIndexes();
        return Stream.concat(
                sourceIndexes.stream()
                        .flatMap(index -> Stream.of(false, true)
                                .flatMap(filtered -> Stream.of(0xdeadc0de, 0x5ca1efdb)
                                        .map(seed -> Arguments.of(index, filtered, seed)))),
                RandomizedTestUtils.randomArguments(r -> {
                    Index sourceIndex = sourceIndexes.get(r.nextInt(sourceIndexes.size()));
                    boolean filtered = r.nextBoolean();
                    long seed = r.nextLong();
                    return Arguments.of(sourceIndex, filtered, seed);
                })
        );
    }

    @Test
    void emptyRangeSum() {
        sumRebuild(Collections.emptyList());
    }

    @Test
    void singleElementSum() {
        TestRecords1Proto.MySimpleRecord record = TestRecords1Proto.MySimpleRecord.newBuilder()
                .setRecNo(1517)
                .setNumValue2(95)
                .build();
        sumRebuild(Collections.singletonList(record));
    }

    @ParameterizedTest
    @MethodSource("sourceIndexesAndRandomSeeds")
    @Tag(Tags.Slow)
    void oneHundredElementsSum(@Nullable Index sourceIndex, long seed) {
        Random r = new Random(seed);
        List<TestRecords1Proto.MySimpleRecord> records = Stream.generate(() ->
                TestRecords1Proto.MySimpleRecord.newBuilder()
                        .setRecNo(r.nextLong())
                        .setNumValue2(r.nextInt(10))
                        .build()
        ).limit(100).sorted(Comparator.comparingLong(TestRecords1Proto.MySimpleRecord::getRecNo)).collect(Collectors.toList());
        sumRebuild(records, null, sourceIndex);
    }

    @ParameterizedTest
    @MethodSource("randomSeeds")
    @Tag(Tags.Slow)
    void oneHundredElementsParallelSum(long seed) {
        Random r = new Random(seed);
        List<TestRecords1Proto.MySimpleRecord> records = Stream.generate(() ->
                TestRecords1Proto.MySimpleRecord.newBuilder()
                        .setRecNo(r.nextLong() / 2)
                        .setNumValue2(r.nextInt(10))
                        .build()
        ).limit(100).sorted(Comparator.comparingLong(TestRecords1Proto.MySimpleRecord::getRecNo)).collect(Collectors.toList());
        sumRebuild(records, null, null, null, 5, false);
    }

    @ParameterizedTest
    @MethodSource("randomSeeds")
    @Tag(Tags.Slow)
    void oneHundredElementsParallelOverlapSum(long seed) {
        Random r = new Random(seed);
        List<TestRecords1Proto.MySimpleRecord> records = Stream.generate(() ->
                TestRecords1Proto.MySimpleRecord.newBuilder()
                        .setRecNo(r.nextLong() / 2)
                        .setNumValue2(r.nextInt(10))
                        .build()
        ).limit(100).sorted(Comparator.comparingLong(TestRecords1Proto.MySimpleRecord::getRecNo)).collect(Collectors.toList());
        sumRebuild(records, null, null, null, 5, true);
    }

    @ParameterizedTest
    @MethodSource("sourceIndexesAndRandomSeeds")
    @Tag(Tags.Slow)
    void addWhileBuildingSum(Index sourceIndex, long seed) {
        Random r = new Random(seed);
        List<TestRecords1Proto.MySimpleRecord> records = Stream.generate(() ->
                TestRecords1Proto.MySimpleRecord.newBuilder()
                        .setRecNo(r.nextLong())
                        .setNumValue2(r.nextInt(10))
                        .build()
        ).limit(100).sorted(Comparator.comparingLong(TestRecords1Proto.MySimpleRecord::getRecNo)).collect(Collectors.toList());
        List<TestRecords1Proto.MySimpleRecord> recordsWhileBuilding = Stream.generate(() ->
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(r.nextLong()).setNumValue2(r.nextInt(10)).build()
        ).limit(100).sorted(Comparator.comparingLong(TestRecords1Proto.MySimpleRecord::getRecNo)).collect(Collectors.toList());
        sumRebuild(records, recordsWhileBuilding, sourceIndex);
    }

    @ParameterizedTest
    @MethodSource("randomSeeds")
    @Tag(Tags.Slow)
    void addWhileBuildingParallelSum(long seed) {
        Random r = new Random(seed);
        List<TestRecords1Proto.MySimpleRecord> records = Stream.generate(() ->
                TestRecords1Proto.MySimpleRecord.newBuilder()
                        .setRecNo(r.nextLong() / 2)
                        .setNumValue2(r.nextInt(10))
                        .build()
        ).limit(200).sorted(Comparator.comparingLong(TestRecords1Proto.MySimpleRecord::getRecNo)).collect(Collectors.toList());
        List<TestRecords1Proto.MySimpleRecord> recordsWhileBuilding = Stream.generate(() ->
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(r.nextLong() / 2).setNumValue2(r.nextInt(10)).build()
        ).limit(200).sorted(Comparator.comparingLong(TestRecords1Proto.MySimpleRecord::getRecNo)).collect(Collectors.toList());
        sumRebuild(records, recordsWhileBuilding, null, null, 5, false);
    }

    @ParameterizedTest
    @MethodSource("sourceIndexesAndRandomSeeds")
    @Tag(Tags.Slow)
    public void somePreloadedSum(@Nullable Index sourceIndex, long seed) {
        Random r = new Random(seed);
        List<TestRecords1Proto.MySimpleRecord> records = Stream.generate(() ->
                TestRecords1Proto.MySimpleRecord.newBuilder()
                        .setRecNo(r.nextLong())
                        .setNumValue2(r.nextInt(10))
                        .build()
        ).limit(100).sorted(Comparator.comparingLong(TestRecords1Proto.MySimpleRecord::getRecNo)).collect(Collectors.toList());
        openSimpleMetaData(metaDataBuilder -> {
            if (sourceIndex != null) {
                metaDataBuilder.addIndex("MySimpleRecord", sourceIndex);
            }
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
        sumRebuild(records, null, sourceIndex);
    }

    @ParameterizedTest
    @MethodSource("sourceIndexesAndRandomSeeds")
    @Tag(Tags.Slow)
    void addSequentialWhileBuildingSum(@Nullable Index sourceIndex, long seed) {
        Random r = new Random(seed);
        List<TestRecords1Proto.MySimpleRecord> records = LongStream.range(0, 100).mapToObj(val ->
                TestRecords1Proto.MySimpleRecord.newBuilder()
                        .setRecNo(val)
                        .setNumValue2(r.nextInt(20))
                        .build()
        ).collect(Collectors.toList());
        List<TestRecords1Proto.MySimpleRecord> recordsWhileBuilding = Stream.generate(() ->
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(r.nextInt(100)).setNumValue2(r.nextInt(20) + 20).build()
        ).limit(100).sorted(Comparator.comparingLong(TestRecords1Proto.MySimpleRecord::getRecNo)).collect(Collectors.toList());
        sumRebuild(records, recordsWhileBuilding, sourceIndex);
    }

    @ParameterizedTest
    @MethodSource("randomSeeds")
    @Tag(Tags.Slow)
    void addSequentialWhileBuildingParallelSum(long seed) {
        Random r = new Random(seed);
        List<TestRecords1Proto.MySimpleRecord> records = LongStream.range(0, 100).mapToObj( val ->
                TestRecords1Proto.MySimpleRecord.newBuilder()
                        .setRecNo(val)
                        .setNumValue2(r.nextInt(20))
                        .build()
        ).collect(Collectors.toList());
        List<TestRecords1Proto.MySimpleRecord> recordsWhileBuilding = Stream.generate(() ->
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(r.nextInt(100)).setNumValue2(r.nextInt(20) + 20).build()
        ).limit(100).sorted(Comparator.comparingLong(TestRecords1Proto.MySimpleRecord::getRecNo)).collect(Collectors.toList());
        sumRebuild(records, recordsWhileBuilding, null, null, 5, false);
    }

    @ParameterizedTest
    @MethodSource("sourceIndexesAndRandomSeeds")
    @Tag(Tags.Slow)
    public void updateRecordsWhileBuildingSum(@Nullable Index sourceIndex, long seed) {
        Random r = new Random(seed);
        List<TestRecords1Proto.MySimpleRecord> records = Stream.generate(() ->
                TestRecords1Proto.MySimpleRecord.newBuilder()
                        .setRecNo(r.nextLong())
                        .setNumValue2(r.nextInt(20))
                        .build()
        ).limit(300).collect(Collectors.toList());
        List<TestRecords1Proto.MySimpleRecord> recordsWhileBuilding = records.stream()
                .filter(rec -> r.nextBoolean())
                .map(rec -> rec.toBuilder().setNumValue2(r.nextInt(20)).build())
                .collect(Collectors.toList());
        sumRebuild(records, recordsWhileBuilding, sourceIndex);
    }

    @ParameterizedTest
    @MethodSource("sourceIndexesAndRandomSeeds")
    @Tag(Tags.Slow)
    public void deleteRecordsWhileBuildingSum(@Nullable Index sourceIndex, long seed) {
        Random r = new Random(seed);
        List<TestRecords1Proto.MySimpleRecord> records = Stream.generate(() ->
                TestRecords1Proto.MySimpleRecord.newBuilder()
                        .setRecNo(r.nextLong())
                        .setNumValue2(r.nextInt(50))
                        .build()
        ).limit(300).collect(Collectors.toList());
        List<Long> toDelete = records.stream()
                .filter(rec -> r.nextBoolean())
                .map(TestRecords1Proto.MySimpleRecord::getRecNo)
                .collect(Collectors.toList());
        sumRebuild(records, null, toDelete, sourceIndex, 1, false);
    }

    @ParameterizedTest
    @MethodSource("sourceIndexesAndRandomSeeds")
    @Tag(Tags.Slow)
    public void updateAndDeleteRecordsWhileBuildingSum(@Nullable Index sourceIndex, long seed) {
        Random r = new Random(seed);
        List<TestRecords1Proto.MySimpleRecord> records = Stream.generate(() ->
                TestRecords1Proto.MySimpleRecord.newBuilder()
                        .setRecNo(r.nextLong())
                        .setNumValue2(r.nextInt(50))
                        .build()
        ).limit(300).collect(Collectors.toList());
        List<TestRecords1Proto.MySimpleRecord> recordsWhileBuilding = records.stream()
                .filter(rec -> r.nextBoolean())
                .map(rec -> rec.toBuilder().setNumValue2(r.nextInt(20)).build())
                .collect(Collectors.toList());
        List<Long> toDelete = records.stream()
                .filter(rec -> r.nextBoolean())
                .map(TestRecords1Proto.MySimpleRecord::getRecNo)
                .collect(Collectors.toList());
        sumRebuild(records, recordsWhileBuilding, toDelete, sourceIndex, 1, false);
    }

    @ParameterizedTest
    @MethodSource("sourceIndexesFilteredAndRandomSeeds")
    @Tag(Tags.Slow)
    public void oneHundredFilteredSum(@Nullable Index sourceIndex, boolean filterSource, long seed) {
        Random r = new Random(seed);
        List<TestRecords1Proto.MySimpleRecord> records = LongStream.range(0, 100).mapToObj(val ->
                TestRecords1Proto.MySimpleRecord.newBuilder()
                        .setRecNo(val)
                        .setNumValue2(r.nextInt(20))
                        .setNumValue3Indexed(r.nextInt(2))
                        .build()
        ).collect(Collectors.toList());
        sumRebuildFiltered(records, null, null, sourceIndex, filterSource);
    }

    @ParameterizedTest
    @MethodSource("sourceIndexesFilteredAndRandomSeeds")
    @Tag(Tags.Slow)
    public void updateWhileBuildingFilteredSum(@Nullable Index sourceIndex, boolean filterSource, long seed) {
        Random r = new Random(seed);
        List<TestRecords1Proto.MySimpleRecord> records = Stream.generate(() ->
                TestRecords1Proto.MySimpleRecord.newBuilder()
                        .setRecNo(r.nextLong())
                        .setNumValue2(r.nextInt(50) - 25)
                        .setNumValue3Indexed(r.nextInt(2))
                        .build()
        ).limit(500).collect(Collectors.toList());
        List<TestRecords1Proto.MySimpleRecord> recordsWhileBuilding = records.stream()
                .filter(rec -> r.nextBoolean())
                .map(rec -> rec.toBuilder().setNumValue2(r.nextInt(50) - 25).setNumValue3Indexed(r.nextInt(2)).build())
                .collect(Collectors.toList());
        List<Long> deleteWhileBuilding = records.stream()
                .filter(rec -> r.nextDouble() < 0.1)
                .map(TestRecords1Proto.MySimpleRecord::getRecNo)
                .collect(Collectors.toList());
        sumRebuildFiltered(records, recordsWhileBuilding, deleteWhileBuilding, sourceIndex, filterSource);
    }
}
