/*
 * OnlineIndexerBuildVersionIndexTest.java
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
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.expressions.VersionKeyExpression;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.Tags;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Test building version indexes.
 */
@SuppressWarnings("try")
class OnlineIndexerBuildVersionIndexTest extends OnlineIndexerBuildIndexTest {

    private void versionRebuild(@Nonnull List<TestRecords1Proto.MySimpleRecord> records, @Nullable List<TestRecords1Proto.MySimpleRecord> recordsWhileBuilding,
                                int agents, boolean overlap) {
        final OnlineIndexerTestRecordHandler<TestRecords1Proto.MySimpleRecord> recordHandler = OnlineIndexerTestSimpleRecordHandler.instance();
        final Index index = new Index("newVersionIndex", concat(field("num_value_2"), VersionKeyExpression.VERSION), IndexTypes.VERSION);
        final Function<FDBQueriedRecord<Message>, Tuple> projection = rec -> {
            TestRecords1Proto.MySimpleRecord simple = TestRecords1Proto.MySimpleRecord.newBuilder().mergeFrom(rec.getRecord()).build();
            Integer numValue2 = (simple.hasNumValue2()) ? simple.getNumValue2() : null;
            FDBRecordVersion version = rec.hasVersion() ? rec.getVersion() : null;
            if (version != null) {
                assertTrue(version.isComplete());
            }
            return Tuple.from(numValue2, (version == null) ? null : version.toVersionstamp());
        };

        List<RecordQuery> queries = records.stream()
                .map(record -> {
                    Integer value2 = (record.hasNumValue2()) ? record.getNumValue2() : null;
                    return RecordQuery.newBuilder()
                            .setRecordType("MySimpleRecord")
                            .setFilter(value2 != null ?
                                       Query.field("num_value_2").equalsValue(record.getNumValue2()) :
                                       Query.field("num_value_2").isNull())
                            .setSort(VersionKeyExpression.VERSION)
                            .build();
                })
                .collect(Collectors.toList());

        Function<TestRecords1Proto.MySimpleRecord, Integer> indexValue = msg -> msg.hasNumValue2() ? msg.getNumValue2() : null;
        Map<Integer, List<Message>> valueMap = group(records, indexValue);
        Map<Long, FDBRecordVersion> versionMap = new HashMap<>(records.size() + (recordsWhileBuilding == null ? 0 : recordsWhileBuilding.size()));
        AtomicReference<FDBRecordVersion> greatestVersion = new AtomicReference<>(null);

        final Runnable beforeBuild = () -> {
            try (FDBRecordContext context = openContext()) {
                for (int i = 0; i < queries.size(); i++) {
                    Integer value2 = (records.get(i).hasNumValue2()) ? records.get(i).getNumValue2() : null;
                    try {
                        executeQuery(queries.get(i), "Index(newVersionIndex [[" + value2 + "],[" + value2 + "])", valueMap.get(value2));
                        fail("somehow executed query with new index before build");
                    } catch (RecordCoreException e) {
                        assertEquals("Cannot sort without appropriate index: Version", e.getMessage());
                    }
                }

                // Load all the version information for the records that were initially there.
                for (TestRecords1Proto.MySimpleRecord simple : records) {
                    recordStore.loadRecordVersion(Tuple.from(simple.getRecNo())).ifPresent(version -> {
                        versionMap.put(simple.getRecNo(), version);
                        if (greatestVersion.get() == null || version.compareTo(greatestVersion.get()) > 0) {
                            greatestVersion.set(version);
                        }
                    });
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
                                .setSort(VersionKeyExpression.VERSION)
                                .build();
                    })
                    .collect(Collectors.toList());
            updatedValueMap = group(updatedRecords, indexValue);
        }

        Map<Long, FDBRecordVersion> updatedVersionMap = new HashMap<>(versionMap.size());
        Set<Long> newRecordKeys = (recordsWhileBuilding == null) ? Collections.emptySet() : recordsWhileBuilding.stream().map(TestRecords1Proto.MySimpleRecord::getRecNo).collect(Collectors.toSet());

        Runnable afterBuild = () -> {
            try (FDBRecordContext context = openContext()) {
                // The build job shouldn't affect the reads.
                for (int i = 0; i < updatedQueries.size(); i++) {
                    Integer value2 = (updatedRecords.get(i).hasNumValue2()) ? updatedRecords.get(i).getNumValue2() : null;
                    try {
                        executeQuery(updatedQueries.get(i), "Index(newVersionIndex [[" + value2 + "],[" + value2 + "])", updatedValueMap.get(value2));
                        fail("somehow executed query with new index before readable");
                    } catch (RecordCoreException e) {
                        assertEquals("Cannot sort without appropriate index: Version", e.getMessage());
                    }
                }

                // Load all the version information for records that are there now and that values are sane.
                for (TestRecords1Proto.MySimpleRecord simple : updatedRecords) {
                    recordStore.loadRecordVersion(Tuple.from(simple.getRecNo())).ifPresent(version -> {
                        assertTrue(version.isComplete());
                        if (newRecordKeys.contains(simple.getRecNo())) {
                            assertThat(version, greaterThan(greatestVersion.get()));
                            if (versionMap.containsKey(simple.getRecNo())) {
                                assertThat(version, greaterThan(versionMap.get(simple.getRecNo())));
                            }
                        } else {
                            if (versionMap.containsKey(simple.getRecNo())) {
                                assertEquals(versionMap.get(simple.getRecNo()), version);
                            }
                        }
                        updatedVersionMap.put(simple.getRecNo(), version);
                    });
                }
            }
        };

        Runnable afterReadable = () -> {
            Descriptors.FieldDescriptor recNoFieldDescriptor = TestRecords1Proto.MySimpleRecord.getDescriptor().findFieldByName("rec_no");
            try (FDBRecordContext context = openContext()) {
                for (int i = 0; i < updatedQueries.size(); i++) {
                    Integer value2 = (updatedRecords.get(i).hasNumValue2()) ? updatedRecords.get(i).getNumValue2() : null;
                    List<Tuple> sortedValues = updatedValueMap.get(value2).stream()
                            .map(msg -> {
                                FDBRecordVersion version = updatedVersionMap.get(((Number)msg.getField(recNoFieldDescriptor)).longValue());
                                return Tuple.from(value2, version == null ? null : version.toVersionstamp());
                            })
                            .sorted()
                            .collect(Collectors.toList());
                    executeQuery(updatedQueries.get(i), "ISCAN(newVersionIndex [[" + value2 + "],[" + value2 + "]])", sortedValues, projection);
                }
                context.commit();
            }
        };

        singleRebuild(recordHandler, records, recordsWhileBuilding, null, agents, overlap, false, index, null, beforeBuild, afterBuild, afterReadable);
    }

    private void versionRebuild(@Nonnull List<TestRecords1Proto.MySimpleRecord> records, @Nullable List<TestRecords1Proto.MySimpleRecord> recordsWhileBuilding) {
        versionRebuild(records, recordsWhileBuilding, 1, false);
    }

    private void versionRebuild(@Nonnull List<TestRecords1Proto.MySimpleRecord> records) {
        versionRebuild(records, null);
    }

    @ParameterizedTest
    @MethodSource("randomSeeds")
    @Tag(Tags.Slow)
    void oneHundredElementsVersion(long seed) {
        Random r = new Random(seed);
        List<TestRecords1Proto.MySimpleRecord> records = Stream.generate(() ->
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(r.nextLong()).setNumValue2(r.nextInt(10)).build()
        ).limit(100).collect(Collectors.toList());
        versionRebuild(records);
    }

    @ParameterizedTest
    @MethodSource("randomSeeds")
    @Tag(Tags.Slow)
    void oneHundredElementsParallelVersion(long seed) {
        Random r = new Random(seed);
        List<TestRecords1Proto.MySimpleRecord> records = Stream.generate(() ->
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(r.nextLong() / 2).setNumValue2(r.nextInt(10)).build()
        ).limit(100).collect(Collectors.toList());
        versionRebuild(records, null, 5, false);
    }

    @ParameterizedTest
    @MethodSource("randomSeeds")
    @Tag(Tags.Slow)
    void oneHundredElementsParallelOverlapVersion(long seed) {
        Random r = new Random(seed);
        List<TestRecords1Proto.MySimpleRecord> records = Stream.generate(() ->
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(r.nextLong() / 2).setNumValue2(r.nextInt(10)).build()
        ).limit(100).collect(Collectors.toList());
        versionRebuild(records, null, 5, true);
    }

    @ParameterizedTest
    @MethodSource("randomSeeds")
    @Tag(Tags.Slow)
    void addWhileBuildingVersion(long seed) {
        Random r = new Random(seed);
        List<TestRecords1Proto.MySimpleRecord> records = Stream.generate(() ->
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(r.nextLong()).setNumValue2(r.nextInt(10)).build()
        ).limit(100).collect(Collectors.toList());
        List<TestRecords1Proto.MySimpleRecord> recordsWhileBuilding = Stream.generate(() ->
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(r.nextLong()).setNumValue2(r.nextInt(10)).build()
        ).limit(100).collect(Collectors.toList());
        versionRebuild(records, recordsWhileBuilding);
    }

    @ParameterizedTest
    @MethodSource("randomSeeds")
    @Tag(Tags.Slow)
    void addWhileBuildingParallelVersion(long seed) {
        Random r = new Random(seed);
        List<TestRecords1Proto.MySimpleRecord> records = Stream.generate(() ->
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(r.nextLong() / 2).setNumValue2(r.nextInt(10)).build()
        ).limit(150).collect(Collectors.toList());
        List<TestRecords1Proto.MySimpleRecord> recordsWhileBuilding = Stream.generate(() ->
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(r.nextLong() / 2).setNumValue2(r.nextInt(10)).build()
        ).limit(150).collect(Collectors.toList());
        versionRebuild(records, recordsWhileBuilding, 5, false);
    }

    @ParameterizedTest
    @MethodSource("randomSeeds")
    @Tag(Tags.Slow)
    void addSequentialWhileBuildingVersion(long seed) {
        Random r = new Random(seed);
        List<TestRecords1Proto.MySimpleRecord> records = LongStream.range(0, 100).mapToObj(val ->
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(val).setNumValue2(r.nextInt(20)).build()
        ).collect(Collectors.toList());
        Set<Integer> usedKeys = new HashSet<>();
        List<Integer> primaryKeys = IntStream.generate(() -> r.nextInt(100)).filter(usedKeys::add).limit(50).boxed().collect(Collectors.toList());
        List<TestRecords1Proto.MySimpleRecord> recordsWhileBuilding = primaryKeys.stream().map(recNo ->
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(recNo).setNumValue2(r.nextInt(20) + 20).build()
        ).collect(Collectors.toList());
        versionRebuild(records, recordsWhileBuilding);
    }

    @ParameterizedTest
    @MethodSource("randomSeeds")
    @Tag(Tags.Slow)
    void addSequentialWhileBuildingParallelVersion(long seed) {
        Random r = new Random(seed);
        List<TestRecords1Proto.MySimpleRecord> records = LongStream.range(0, 100).mapToObj( val ->
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(val).setNumValue2(r.nextInt(20)).build()
        ).collect(Collectors.toList());
        Set<Integer> usedKeys = new HashSet<>();
        List<Integer> primaryKeys = IntStream.generate(() -> r.nextInt(100)).filter(usedKeys::add).limit(50).boxed().collect(Collectors.toList());
        List<TestRecords1Proto.MySimpleRecord> recordsWhileBuilding = primaryKeys.stream().map(recNo ->
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(recNo).setNumValue2(r.nextInt(20) + 20).build()
        ).collect(Collectors.toList());
        versionRebuild(records, recordsWhileBuilding, 5, false);
    }

    @ParameterizedTest
    @MethodSource("randomSeeds")
    @Tag(Tags.Slow)
    void someWithoutVersion(long seed) {
        Random r = new Random(seed);
        List<TestRecords1Proto.MySimpleRecord> records = Stream.generate(() ->
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(r.nextLong() / 2).setNumValue2(r.nextInt(20)).build()
        ).limit(100).collect(Collectors.toList());
        openSimpleMetaData(metaDataBuilder -> metaDataBuilder.setStoreRecordVersions(false));
        try (FDBRecordContext context = openContext(false)) {
            records.stream().filter(msg -> msg.getNumValue2() % 2 == 0).forEach(recordStore::saveRecord);
            context.commit();
        }
        versionRebuild(records, null, 1, false);
    }

    @ParameterizedTest
    @MethodSource("randomSeeds")
    @Tag(Tags.Slow)
    void someWithoutVersionParallel(long seed) {
        Random r = new Random(seed);
        List<TestRecords1Proto.MySimpleRecord> records = Stream.generate(() ->
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(r.nextLong() / 2).setNumValue2(r.nextInt(20)).build()
        ).limit(100).collect(Collectors.toList());
        openSimpleMetaData(metaDataBuilder -> metaDataBuilder.setStoreRecordVersions(false));
        try (FDBRecordContext context = openContext(false)) {
            records.stream().filter(msg -> msg.getNumValue2() % 2 == 0).forEach(recordStore::saveRecord);
            context.commit();
        }
        versionRebuild(records, null, 5, false);
    }

    @ParameterizedTest
    @MethodSource("randomSeeds")
    @Tag(Tags.Slow)
    void addWhileBuildingWithoutVersion(long seed) {
        Random r = new Random(seed);
        List<TestRecords1Proto.MySimpleRecord> records = Stream.generate(() ->
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(r.nextLong() / 2).setNumValue2(r.nextInt(20)).build()
        ).limit(100).collect(Collectors.toList());
        List<TestRecords1Proto.MySimpleRecord> recordsWhileBuilding = Stream.generate(() ->
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(r.nextLong() / 2).setNumValue2(r.nextInt(20)).build()
        ).limit(100).collect(Collectors.toList());
        openSimpleMetaData(metaDataBuilder -> metaDataBuilder.setStoreRecordVersions(false));
        try (FDBRecordContext context = openContext(false)) {
            records.stream().filter(msg -> msg.getNumValue2() % 2 == 0).forEach(recordStore::saveRecord);
            context.commit();
        }
        versionRebuild(records, recordsWhileBuilding, 1, false);
    }

    @ParameterizedTest
    @MethodSource("randomSeeds")
    @Tag(Tags.Slow)
    void addWhileBuildingWithoutVersionParallel(long seed) {
        Random r = new Random(seed);
        List<TestRecords1Proto.MySimpleRecord> records = Stream.generate(() ->
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(r.nextLong() / 2).setNumValue2(r.nextInt(20)).build()
        ).limit(100).collect(Collectors.toList());
        List<TestRecords1Proto.MySimpleRecord> recordsWhileBuilding = Stream.generate(() ->
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(r.nextLong() / 2).setNumValue2(r.nextInt(20)).build()
        ).limit(100).collect(Collectors.toList());
        openSimpleMetaData(metaDataBuilder -> metaDataBuilder.setStoreRecordVersions(false));
        try (FDBRecordContext context = openContext(false)) {
            records.stream().filter(msg -> msg.getNumValue2() % 2 == 0).forEach(recordStore::saveRecord);
            context.commit();
        }
        versionRebuild(records, recordsWhileBuilding, 5, false);
    }

    @ParameterizedTest
    @MethodSource("randomSeeds")
    @Tag(Tags.Slow)
    void sequentialWithoutVersion(long seed) {
        Random r = new Random(seed);
        List<TestRecords1Proto.MySimpleRecord> records = LongStream.range(0, 100).mapToObj(val ->
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(val).setNumValue2(r.nextInt(20)).build()
        ).limit(100).collect(Collectors.toList());
        openSimpleMetaData(metaDataBuilder -> metaDataBuilder.setStoreRecordVersions(false));
        try (FDBRecordContext context = openContext(false)) {
            records.stream().filter(msg -> msg.getNumValue2() % 2 == 0).forEach(recordStore::saveRecord);
            context.commit();
        }
        versionRebuild(records, null, 1, false);
    }

    @ParameterizedTest
    @MethodSource("randomSeeds")
    @Tag(Tags.Slow)
    void sequentialWhileBuildingWithoutVersion(long seed) {
        Random r = new Random(seed);
        List<TestRecords1Proto.MySimpleRecord> records = LongStream.range(0, 100).mapToObj(val ->
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(val).setNumValue2(r.nextInt(20)).build()
        ).limit(100).collect(Collectors.toList());
        openSimpleMetaData(metaDataBuilder -> metaDataBuilder.setStoreRecordVersions(false));
        Set<Integer> usedKeys = new HashSet<>();
        List<Integer> primaryKeys = IntStream.generate(() -> r.nextInt(100)).filter(usedKeys::add).limit(50).boxed().collect(Collectors.toList());
        List<TestRecords1Proto.MySimpleRecord> recordsWhileBuilding = primaryKeys.stream().map(recNo ->
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(recNo).setNumValue2(r.nextInt(20) + 20).build()
        ).collect(Collectors.toList());
        try (FDBRecordContext context = openContext(false)) {
            records.stream().filter(msg -> msg.getNumValue2() % 2 == 0).forEach(recordStore::saveRecord);
            context.commit();
        }
        versionRebuild(records, recordsWhileBuilding, 1, false);
    }
}
