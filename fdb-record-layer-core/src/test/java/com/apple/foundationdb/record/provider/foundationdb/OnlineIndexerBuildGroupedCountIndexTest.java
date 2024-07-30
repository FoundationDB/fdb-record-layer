/*
 * OnlineIndexerBuildGroupedCountIndexTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2022 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexOptions;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.RecordTypeBuilder;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.RandomizedTestUtils;
import com.apple.test.Tags;
import com.google.common.collect.Maps;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.apple.foundationdb.record.metadata.Key.Expressions.concatenateFields;
import static com.apple.foundationdb.record.metadata.Key.Expressions.empty;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests for building a grouped count index.
 */
public class OnlineIndexerBuildGroupedCountIndexTest extends OnlineIndexerTest {
    private static final KeyExpression PRIMARY_KEY = concatenateFields("num_value_2", "rec_no");

    private static final Index COUNT_BY_NUM_VALUE_2 = new Index("countByNumValue2",
            empty().groupBy(field("num_value_2")),
            IndexTypes.COUNT,
            Map.of(IndexOptions.CLEAR_WHEN_ZERO, "true"));

    private static final Index NUM_VALUE_2_INDEX = new Index("MySimpleRecord$num_value_2", field("num_value_2"));
    private static final Index STR_VALUE_INDEX = new Index("MySimpleRecord$str_value_index", concatenateFields("num_value_2", "str_value_indexed"));

    private static List<String> sourceIndexNames() {
        // Use Arrays.asList instead of List.of because List.of does not allow null
        return Arrays.asList(null, "MySimpleRecord$primary_key", NUM_VALUE_2_INDEX.getName(), STR_VALUE_INDEX.getName());
    }

    @Nonnull
    static Stream<Arguments> sourceIndexesAndRandomSeeds() {
        List<String> sourceIndexNames = sourceIndexNames();
        return Stream.concat(
                sourceIndexNames.stream()
                        .flatMap(sourceIndexName -> Stream.of(0xba5eba1L, 0x5ca1e, 0x0fdb0fdb)
                                .map(seed -> Arguments.of(sourceIndexName, seed))),
                RandomizedTestUtils.randomArguments(r -> {
                    String sourceIndexName = sourceIndexNames.get(r.nextInt(sourceIndexNames.size()));
                    long seed = r.nextLong();
                    return Arguments.of(sourceIndexName, seed);
                })
        );
    }

    private static String randomString(@Nonnull Random r) {
        char[] chars = new char[r.nextInt(15) + 1];
        for (int i = 0; i < chars.length; i++) {
            chars[i] = (char)('a' + r.nextInt(26));
        }
        return new String(chars);
    }

    private static TestRecords1Proto.MySimpleRecord randomSimpleRecord(@Nonnull Random r) {
        return TestRecords1Proto.MySimpleRecord.newBuilder()
                .setRecNo(r.nextLong())
                .setNumValue2(r.nextInt(10))
                .setStrValueIndexed(randomString(r))
                .setNumValue3Indexed(r.nextInt(20))
                .build();
    }

    private static TestRecords1Proto.MyOtherRecord randomOtherRecord(@Nonnull Random r) {
        return TestRecords1Proto.MyOtherRecord.newBuilder()
                .setRecNo(r.nextLong())
                .setNumValue2(r.nextInt(10))
                .build();
    }

    @FunctionalInterface
    private interface RecordsUpdater {
        Collection<TestRecords1Proto.MySimpleRecord> update(Collection<TestRecords1Proto.MySimpleRecord> recordsBefore,
                                                            Collection<TestRecords1Proto.MyOtherRecord> otherRecordsBefore);
    }

    private static final RecordsUpdater NO_UPDATES = (recordsBefore, otherRecordsBefore) -> recordsBefore;

    private static FDBRecordStoreTestBase.RecordMetaDataHook baseGroupedHook() {
        return metaDataBuilder -> {
            for (String recordTypeName : List.of("MySimpleRecord", "MyOtherRecord")) {
                RecordTypeBuilder recordTypeBuilder = metaDataBuilder.getRecordType(recordTypeName);
                recordTypeBuilder.setPrimaryKey(PRIMARY_KEY);
                for (Index index : new ArrayList<>(recordTypeBuilder.getIndexes())) {
                    metaDataBuilder.removeIndex(index.getName());
                }
                for (Index index : new ArrayList<>(recordTypeBuilder.getMultiTypeIndexes())) {
                    metaDataBuilder.removeIndex(index.getName());
                }
                metaDataBuilder.addIndex(recordTypeBuilder, new Index(recordTypeName + "$primary_key", PRIMARY_KEY));
            }

            metaDataBuilder.addIndex("MySimpleRecord", NUM_VALUE_2_INDEX);
            metaDataBuilder.addIndex("MySimpleRecord", STR_VALUE_INDEX);
        };
    }

    private static FDBRecordStoreTestBase.RecordMetaDataHook withCountIndexHook() {
        FDBRecordStoreTestBase.RecordMetaDataHook base = baseGroupedHook();
        return metaDataBuilder -> {
            base.apply(metaDataBuilder);
            metaDataBuilder.addIndex("MySimpleRecord", COUNT_BY_NUM_VALUE_2);
        };
    }

    private Map<Integer, Long> countByGroup() {
        try (FDBRecordContext context = openContext()) {
            Map<Integer, Long> values = new HashMap<>();
            recordStore.scanIndex(COUNT_BY_NUM_VALUE_2, IndexScanType.BY_GROUP, TupleRange.ALL, null, ScanProperties.FORWARD_SCAN)
                    .forEach(indexEntry -> {
                        int numValue2 = (int)indexEntry.getKey().getLong(indexEntry.getKey().size() - 1);
                        long count = indexEntry.getValue().getLong(0);
                        values.put(numValue2, count);
                    }).join();
            context.commit();
            return values;
        }
    }

    private static <M extends Message> Map<Tuple, M> byPrimaryKey(@Nonnull Collection<M> records) {
        Map<Tuple, M> recordMap = Maps.newHashMapWithExpectedSize(records.size());
        records.forEach(rec -> recordMap.put(PRIMARY_KEY.evaluateMessageSingleton(null, rec).toTuple(), rec));
        return recordMap;
    }

    private static Map<Integer, Long> expectedCountByGroup(@Nonnull Collection<TestRecords1Proto.MySimpleRecord> records) {
        Map<Integer, Long> values = new HashMap<>();
        records.forEach(rec -> values.compute(rec.getNumValue2(), (numValue2, count) -> count == null ? 1L : (count + 1L)));
        return values;
    }

    private void validateCountByGroup(@Nonnull Collection<TestRecords1Proto.MySimpleRecord> records) {
        Map<Integer, Long> expected = expectedCountByGroup(records);
        Map<Integer, Long> scanned = countByGroup();
        assertEquals(expected, scanned);
    }

    private void rebuildGroupedCount(@Nonnull Collection<TestRecords1Proto.MySimpleRecord> recordsBefore,
                                     @Nullable Collection<TestRecords1Proto.MyOtherRecord> otherRecordsBefore,
                                     @Nullable String sourceIndex,
                                     @Nonnull RecordsUpdater updater) {
        openSimpleMetaData(baseGroupedHook());
        try (FDBRecordContext context = openContext()) {
            recordsBefore.forEach(recordStore::saveRecord);
            if (otherRecordsBefore != null) {
                otherRecordsBefore.forEach(recordStore::saveRecord);
            }
            context.commit();
        }

        openSimpleMetaData(withCountIndexHook());
        OnlineIndexer.IndexingPolicy.Builder policyBuilder = OnlineIndexer.IndexingPolicy.newBuilder()
                .setForbidRecordScan(sourceIndex != null)
                .setIfDisabled(OnlineIndexer.IndexingPolicy.DesiredAction.REBUILD)
                .setIfWriteOnly(OnlineIndexer.IndexingPolicy.DesiredAction.ERROR)
                .setIfReadable(OnlineIndexer.IndexingPolicy.DesiredAction.ERROR);
        if (sourceIndex != null) {
            policyBuilder.setSourceIndex(sourceIndex);
        }

        Collection<TestRecords1Proto.MySimpleRecord> recordsAfter;

        try (OnlineIndexer indexer = newIndexerBuilder()
                .setIndex(COUNT_BY_NUM_VALUE_2)
                .setIndexingPolicy(policyBuilder.build())
                .setLimit(10)
                .setMaxRetries(Integer.MAX_VALUE)
                .setRecordsPerSecond(OnlineIndexOperationConfig.DEFAULT_RECORDS_PER_SECOND * 100)
                .build()) {
            CompletableFuture<?> buildFuture = indexer.buildIndexAsync(true);
            recordsAfter = updater.update(recordsBefore, otherRecordsBefore);
            buildFuture.join();
        }

        validateCountByGroup(recordsAfter);
    }

    private void rebuildGroupedCount(@Nonnull Collection<TestRecords1Proto.MySimpleRecord> recordsBefore,
                                     @Nullable Collection<TestRecords1Proto.MyOtherRecord> otherRecordsBefore,
                                     @Nullable String sourceIndex) {
        rebuildGroupedCount(recordsBefore, otherRecordsBefore, sourceIndex, NO_UPDATES);
    }

    @ParameterizedTest
    @MethodSource("sourceIndexesAndRandomSeeds")
    @Tag(Tags.Slow)
    void buildOneHundredGroupedCount(String sourceIndex, long seed) {
        Random r = new Random(seed);
        List<TestRecords1Proto.MySimpleRecord> records = Stream.generate(() -> randomSimpleRecord(r))
                .limit(100)
                .collect(Collectors.toList());
        rebuildGroupedCount(records, null, sourceIndex);
    }

    @ParameterizedTest
    @MethodSource("sourceIndexesAndRandomSeeds")
    @Tag(Tags.Slow)
    void buildOneHundredWithOthersGroupedCount(String sourceIndex, long seed) {
        Random r = new Random(seed);
        List<TestRecords1Proto.MySimpleRecord> records = Stream.generate(() -> randomSimpleRecord(r))
                .limit(100)
                .collect(Collectors.toList());
        List<TestRecords1Proto.MyOtherRecord> otherRecords = Stream.generate(() -> randomOtherRecord(r))
                .limit(50)
                .collect(Collectors.toList());
        rebuildGroupedCount(records, otherRecords, sourceIndex);
    }

    @ParameterizedTest
    @MethodSource("sourceIndexesAndRandomSeeds")
    @Tag(Tags.Slow)
    void buildWhileInsertingGroupedCount(String sourceIndex, long seed) {
        Random r = new Random(seed);
        List<TestRecords1Proto.MySimpleRecord> records = Stream.generate(() -> randomSimpleRecord(r))
                .limit(200)
                .collect(Collectors.toList());
        rebuildGroupedCount(records, null, sourceIndex, (recordsBefore, ignore) -> {
            List<TestRecords1Proto.MySimpleRecord> recordsAfter = new ArrayList<>(recordsBefore);
            for (int i = 0; i < 5; i++) {
                List<TestRecords1Proto.MySimpleRecord> newRecords = Stream.generate(() -> randomSimpleRecord(r))
                        .limit(10)
                        .collect(Collectors.toList());
                fdb.run(context -> {
                    FDBRecordStore innerStore = recordStore.asBuilder()
                            .setContext(context)
                            .setMetaDataProvider(metaData)
                            .open();
                    newRecords.forEach(innerStore::saveRecord);
                    return null;
                });
                recordsAfter.addAll(newRecords);
            }
            return recordsAfter;
        });
    }

    @ParameterizedTest
    @MethodSource("sourceIndexesAndRandomSeeds")
    @Tag(Tags.Slow)
    void buildWhileUpdatingGroupedCount(String sourceIndex, long seed) {
        Random r = new Random(seed);
        List<TestRecords1Proto.MySimpleRecord> records = Stream.generate(() -> randomSimpleRecord(r))
                .limit(200)
                .collect(Collectors.toList());
        rebuildGroupedCount(records, null, sourceIndex, (recordsBefore, ignore) -> {
            for (int i = 0; i < 5; i++) {
                List<TestRecords1Proto.MySimpleRecord> updatedRecords = records.stream()
                        .filter(rec -> r.nextDouble() < 0.05)
                        .map(rec -> rec.toBuilder().setStrValueIndexed(randomString(r)).build())
                        .collect(Collectors.toList());
                fdb.run(context -> {
                    FDBRecordStore innerStore = recordStore.asBuilder()
                            .setContext(context)
                            .setMetaDataProvider(metaData)
                            .open();
                    updatedRecords.forEach(innerStore::saveRecord);
                    return null;
                });
            }
            return recordsBefore;
        });
    }

    @ParameterizedTest
    @MethodSource("sourceIndexesAndRandomSeeds")
    @Tag(Tags.Slow)
    void buildWhileDeletingGroupedCount(String sourceIndex, long seed) {
        Random r = new Random(seed);
        List<TestRecords1Proto.MySimpleRecord> records = Stream.generate(() -> randomSimpleRecord(r))
                .limit(200)
                .collect(Collectors.toList());
        rebuildGroupedCount(records, null, sourceIndex, (recordsBefore, ignore) -> {
            Map<Tuple, TestRecords1Proto.MySimpleRecord> recordMap = byPrimaryKey(recordsBefore);
            for (int i = 0; i < 5; i++) {
                List<Tuple> toDelete = recordMap.keySet().stream()
                        .filter(key -> r.nextDouble() < 0.05)
                        .collect(Collectors.toList());
                fdb.run(context -> {
                    FDBRecordStore innerStore = recordStore.asBuilder()
                            .setContext(context)
                            .setMetaDataProvider(metaData)
                            .open();
                    toDelete.forEach(innerStore::deleteRecord);
                    return null;
                });
                toDelete.forEach(recordMap::remove);
            }
            return recordMap.values();
        });
    }

    @ParameterizedTest
    @MethodSource("sourceIndexesAndRandomSeeds")
    @Tag(Tags.Slow)
    void buildWhileConvertingTypeGroupedCount(String sourceIndex, long seed) {
        Random r = new Random(seed);
        List<TestRecords1Proto.MySimpleRecord> records = Stream.generate(() -> randomSimpleRecord(r))
                .limit(200)
                .collect(Collectors.toList());
        List<TestRecords1Proto.MyOtherRecord> otherRecords = Stream.generate(() -> randomOtherRecord(r))
                .limit(100)
                .collect(Collectors.toList());
        rebuildGroupedCount(records, otherRecords, sourceIndex, (recordsBefore, otherRecordsBefore) -> {
            Map<Tuple, TestRecords1Proto.MySimpleRecord> recordMap = byPrimaryKey(recordsBefore);
            Map<Tuple, TestRecords1Proto.MyOtherRecord> otherRecordMap = byPrimaryKey(otherRecordsBefore);
            for (int i = 0; i < 5; i++) {
                // Convert some simple records to other records, and some other records to simple records.
                // In each of these cases, the primary key is kept the same
                Map<Tuple, TestRecords1Proto.MyOtherRecord> toOther = recordMap.entrySet().stream()
                        .filter(key -> r.nextDouble() < 0.03)
                        .collect(Collectors.toMap(Map.Entry::getKey, entry -> {
                            TestRecords1Proto.MySimpleRecord simple = entry.getValue();
                            return TestRecords1Proto.MyOtherRecord.newBuilder()
                                    .setRecNo(simple.getRecNo())
                                    .setNumValue2(simple.getNumValue2())
                                    .setNumValue3Indexed(simple.getNumValue3Indexed())
                                    .build();
                        }));
                Map<Tuple, TestRecords1Proto.MySimpleRecord> toSimple = otherRecordMap.entrySet().stream()
                        .filter(key -> r.nextDouble() < 0.03)
                        .collect(Collectors.toMap(Map.Entry::getKey, entry -> {
                            TestRecords1Proto.MyOtherRecord other = entry.getValue();
                            return TestRecords1Proto.MySimpleRecord.newBuilder()
                                    .setRecNo(other.getRecNo())
                                    .setNumValue2(other.getNumValue2())
                                    .setNumValue3Indexed(other.getNumValue3Indexed())
                                    .setStrValueIndexed(randomString(r))
                                    .build();
                        }));
                fdb.run(context -> {
                    FDBRecordStore innerStore = recordStore.asBuilder()
                            .setContext(context)
                            .setMetaDataProvider(metaData)
                            .open();
                    toSimple.values().forEach(innerStore::saveRecord);
                    toOther.values().forEach(innerStore::saveRecord);
                    return null;
                });

                toSimple.forEach((key, simple) -> {
                    recordMap.put(key, simple);
                    otherRecordMap.remove(key);
                });
                toOther.forEach((key, other) -> {
                    otherRecordMap.put(key, other);
                    recordMap.remove(key);
                });
            }
            return recordMap.values();
        });
    }

    @ParameterizedTest
    @MethodSource("sourceIndexesAndRandomSeeds")
    @Tag(Tags.Slow)
    void buildWhileDeletingGroupsGroupedCount(String sourceIndex, long seed) {
        Random r = new Random(seed);
        List<TestRecords1Proto.MySimpleRecord> records = Stream.generate(() -> randomSimpleRecord(r))
                .limit(200)
                .collect(Collectors.toList());
        List<TestRecords1Proto.MyOtherRecord> otherRecords = Stream.generate(() -> randomOtherRecord(r))
                .limit(100)
                .collect(Collectors.toList());
        rebuildGroupedCount(records, otherRecords, sourceIndex, (recordsBefore, otherRecordsBefore) -> {
            List<TestRecords1Proto.MySimpleRecord> newRecords = new ArrayList<>(recordsBefore);
            for (int i = 0; i < 3; i++) {
                int numValue2 = r.nextInt(10);
                fdb.run(cx -> {
                    FDBRecordStore innerStore = recordStore.asBuilder()
                            .setContext(cx)
                            .setMetaDataProvider(metaData)
                            .open();
                    innerStore.deleteRecordsWhere(Query.field("num_value_2").equalsValue(numValue2));
                    return null;
                });
                newRecords.removeIf(mySimpleRecord -> mySimpleRecord.getNumValue2() == numValue2);
            }
            return newRecords;
        });
    }

    @ParameterizedTest
    @MethodSource("sourceIndexesAndRandomSeeds")
    @Tag(Tags.Slow)
    void buildWhileRandomlyMutatingGroupedCount(String sourceIndex, long seed) {
        Random r = new Random(seed);
        List<TestRecords1Proto.MySimpleRecord> records = Stream.generate(() -> randomSimpleRecord(r))
                .limit(400)
                .collect(Collectors.toList());
        List<TestRecords1Proto.MyOtherRecord> otherRecords = Stream.generate(() -> randomOtherRecord(r))
                .limit(200)
                .collect(Collectors.toList());
        rebuildGroupedCount(records, otherRecords, sourceIndex, (recordsBefore, otherRecordsBefore) -> {
            Map<Tuple, TestRecords1Proto.MySimpleRecord> simpleMap = byPrimaryKey(recordsBefore);
            Map<Tuple, TestRecords1Proto.MyOtherRecord> otherMap = byPrimaryKey(otherRecordsBefore);
            for (int i = 0; i < 10; i++) {
                fdb.run(cx -> {
                    FDBRecordStore innerStore = recordStore.asBuilder()
                            .setContext(cx)
                            .setMetaDataProvider(metaData)
                            .open();

                    double choice = r.nextDouble();
                    if (choice < 0.3) {
                        // Insert
                        List<TestRecords1Proto.MySimpleRecord> newSimple = Stream.generate(() -> randomSimpleRecord(r))
                                .limit(5)
                                .collect(Collectors.toList());
                        List<TestRecords1Proto.MyOtherRecord> newOthers = Stream.generate(() -> randomOtherRecord(r))
                                .limit(5)
                                .collect(Collectors.toList());
                        newSimple.forEach(innerStore::saveRecord);
                        newOthers.forEach(innerStore::saveRecord);
                        cx.addAfterCommit(() -> {
                            simpleMap.putAll(byPrimaryKey(newSimple));
                            otherMap.putAll(byPrimaryKey(newOthers));
                        });
                    } else if (choice < 0.6) {
                        // Delete
                        List<Tuple> simpleDeletes = simpleMap.keySet().stream()
                                .filter(key -> r.nextDouble() < 0.02)
                                .collect(Collectors.toList());
                        List<Tuple> otherDeletes = otherMap.keySet().stream()
                                .filter(key -> r.nextDouble() < 0.02)
                                .collect(Collectors.toList());
                        simpleDeletes.forEach(innerStore::deleteRecord);
                        otherDeletes.forEach(innerStore::deleteRecord);
                        cx.addAfterCommit(() -> {
                            simpleDeletes.forEach(simpleMap::remove);
                            otherDeletes.forEach(otherMap::remove);
                        });
                    } else if (choice < 0.9) {
                        // Update
                        List<TestRecords1Proto.MySimpleRecord> simpleUpdates = simpleMap.values().stream()
                                .filter(rec -> r.nextDouble() < 0.05)
                                .map(rec -> rec.toBuilder().setStrValueIndexed(randomString(r)).build())
                                .collect(Collectors.toList());
                        List<TestRecords1Proto.MyOtherRecord> otherUpdates = otherMap.values().stream()
                                .filter(rec -> r.nextDouble() < 0.05)
                                .map(rec -> rec.toBuilder().setNumValue3Indexed(r.nextInt(20)).build())
                                .collect(Collectors.toList());
                        simpleUpdates.forEach(innerStore::saveRecord);
                        otherUpdates.forEach(innerStore::saveRecord);
                        cx.addAfterCommit(() -> {
                            simpleMap.putAll(byPrimaryKey(simpleUpdates));
                            otherMap.putAll(byPrimaryKey(otherUpdates));
                        });
                    } else {
                        // Delete records where
                        int numValue2 = r.nextInt(10);
                        innerStore.deleteRecordsWhere(Query.field("num_value_2").equalsValue(numValue2));
                        cx.addAfterCommit(() -> {
                            List<Tuple> simpleDeletes = simpleMap.keySet().stream()
                                    .filter(key -> key.getLong(0) == numValue2)
                                    .collect(Collectors.toList());
                            List<Tuple> otherDeletes = otherMap.keySet().stream()
                                    .filter(key -> key.getLong(0) == numValue2)
                                    .collect(Collectors.toList());
                            simpleDeletes.forEach(simpleMap::remove);
                            otherDeletes.forEach(otherMap::remove);
                        });
                    }
                    return null;
                });
            }
            return simpleMap.values();
        });
    }
}
