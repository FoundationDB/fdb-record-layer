/*
 * OnlineIndexerBuildUnnestedIndexTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2024 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.FunctionNames;
import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.IsolationLevel;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TestRecordsNestedMapProto;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexAggregateFunction;
import com.apple.foundationdb.record.metadata.IndexOptions;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.record.metadata.UnnestedRecordTypeBuilder;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.TupleHelpers;
import com.apple.test.RandomizedTestUtils;
import com.apple.test.Tags;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static com.apple.foundationdb.record.metadata.expressions.KeyExpression.FanType;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Tests for building indexes on an unnested record type.
 */
class OnlineIndexerBuildUnnestedIndexTest extends OnlineIndexerBuildIndexTest {
    @Nonnull
    public static final String UNNESTED = "UnnestedMapType";
    @Nonnull
    private static final String PARENT_CONSTITUENT = "parent";
    @Nonnull
    private static final String ENTRY_CONSTITUENT = "entry";
    @Nonnull
    private static final String KEY_PARAM = "key";
    @Nonnull
    private static final RecordQuery UNNESTED_KEY_QUERY = RecordQuery.newBuilder()
            .setRecordType(UNNESTED)
            .setFilter(Query.field(ENTRY_CONSTITUENT).matches(Query.field("key").equalsParameter(KEY_PARAM)))
            .setRequiredResults(List.of(field(PARENT_CONSTITUENT).nest("other_id"), field(ENTRY_CONSTITUENT).nest("value")))
            .setSort(field(PARENT_CONSTITUENT).nest("other_id"))
            .build();

    @Nonnull
    private static final List<String> KEYS = List.of("foo", "bar", "baz", "qux", "zop", "zork");

    public static class OnlineIndexerTestUnnestedRecordHandler implements OnlineIndexerTestRecordHandler<Message> {
        @Nonnull
        private static OnlineIndexerTestUnnestedRecordHandler INSTANCE = new OnlineIndexerTestUnnestedRecordHandler();

        private OnlineIndexerTestUnnestedRecordHandler() {
        }

        @Override
        public Descriptors.FileDescriptor getFileDescriptor() {
            return TestRecordsNestedMapProto.getDescriptor();
        }

        FDBRecordStoreTestBase.RecordMetaDataHook addUnnestedType() {
            return metaDataBuilder -> {
                final UnnestedRecordTypeBuilder unnestedBuilder = metaDataBuilder.addUnnestedRecordType(UNNESTED);
                unnestedBuilder.addParentConstituent(PARENT_CONSTITUENT, metaDataBuilder.getRecordType("OuterRecord"));
                unnestedBuilder.addNestedConstituent(ENTRY_CONSTITUENT, TestRecordsNestedMapProto.MapRecord.Entry.getDescriptor(), PARENT_CONSTITUENT,
                        field("map").nest(field("entry", FanType.FanOut)));

            };
        }

        @Nonnull
        @Override
        public FDBRecordStoreTestBase.RecordMetaDataHook baseHook(final boolean splitLongRecords, @Nullable final Index sourceIndex) {
            return addUnnestedType().andThen(metaDataBuilder -> {
                metaDataBuilder.setSplitLongRecords(splitLongRecords);
                if (sourceIndex != null) {
                    // TODO: We should consider whether we are okay with source indexes on the unnested record type or not
                    metaDataBuilder.addIndex("OuterRecord", sourceIndex);
                }
            });
        }

        @Nonnull
        @Override
        public FDBRecordStoreTestBase.RecordMetaDataHook addIndexHook(@Nonnull final Index index) {
            return metaDataBuilder -> metaDataBuilder.addIndex(UNNESTED, index);
        }

        @Nonnull
        @Override
        public Tuple getPrimaryKey(@Nonnull final Message message) {
            if (message instanceof TestRecordsNestedMapProto.OuterRecord) {
                return Tuple.from(((TestRecordsNestedMapProto.OuterRecord)message).getRecId());
            } else if (message instanceof TestRecordsNestedMapProto.OtherRecord) {
                return Tuple.from(((TestRecordsNestedMapProto.OtherRecord)message).getRecId());
            } else {
                return fail("do not support message: " + message);
            }
        }

        @Nonnull
        static OnlineIndexerTestUnnestedRecordHandler instance() {
            return INSTANCE;
        }
    }

    @Nonnull
    private static Stream<Arguments> overlapAndRandomSeeds() {
        return Stream.concat(
                Stream.of(false, true)
                        .flatMap(overlap -> Stream.of(0xba5eba1L, 0xb16da7a, 0xfdb05ca1eL)
                                .map(seed -> Arguments.of(overlap, seed))),
                RandomizedTestUtils.randomArguments(r -> {
                    boolean overlap = r.nextBoolean();
                    long seed = r.nextLong();
                    return Arguments.of(overlap, seed);
                })
        );
    }

    private void assertUnnestedKeyQueryPlanFails() {
        try (FDBRecordContext context = openContext()) {
            assertThrows(RecordCoreException.class, () -> recordStore.planQuery(UNNESTED_KEY_QUERY));
            context.commit();
        }
    }

    @Nonnull
    private TestRecordsNestedMapProto.OuterRecord randomOuterRecord(@Nonnull Random r, long recId, double keyInclusion) {
        final TestRecordsNestedMapProto.OuterRecord.Builder builder = TestRecordsNestedMapProto.OuterRecord.newBuilder()
                .setRecId(recId)
                .setOtherId(r.nextInt(50));
        for (String key : KEYS) {
            if (r.nextDouble() < keyInclusion) {
                builder.getMapBuilder().addEntryBuilder()
                        .setKey(key)
                        .setValue("" + r.nextDouble())
                        .setIntValue(r.nextInt(50) + 1);
            }
        }
        return builder.build();
    }

    @Nonnull
    private TestRecordsNestedMapProto.OuterRecord randomOuterRecord(@Nonnull Random r, long recId) {
        return randomOuterRecord(r, recId, 0.5);
    }

    @Nonnull
    private TestRecordsNestedMapProto.OuterRecord randomOuterRecord(@Nonnull Random r) {
        return randomOuterRecord(r, r.nextLong());
    }

    @Nonnull
    private TestRecordsNestedMapProto.OtherRecord randomOtherRecord(@Nonnull Random r, long recId) {
        return TestRecordsNestedMapProto.OtherRecord.newBuilder()
                .setRecId(recId)
                .setOtherId(r.nextInt(5))
                .setOtherValue(KEYS.get(r.nextInt(KEYS.size())))
                .build();
    }

    @Nonnull
    private TestRecordsNestedMapProto.OtherRecord randomOtherRecord(@Nonnull Random r) {
        return randomOtherRecord(r, r.nextLong());
    }

    private void addRandomUpdate(@Nonnull Random r, @Nonnull Message rec, @Nonnull List<Message> recordsWhileBuilding, @Nonnull List<Tuple> deleteWhileBuilding) {
        final OnlineIndexerTestUnnestedRecordHandler recordHandler = OnlineIndexerTestUnnestedRecordHandler.instance();
        Tuple primaryKey = recordHandler.getPrimaryKey(rec);
        long recId = primaryKey.getLong(0);
        double choice = r.nextDouble();
        if (choice < 0.25) {
            // Update, make an outer record
            recordsWhileBuilding.add(randomOuterRecord(r, recId));
        } else if (choice < 0.5) {
            // Update, make an other record
            recordsWhileBuilding.add(randomOtherRecord(r, recId));
        } else if (choice < 0.7) {
            // Delete
            deleteWhileBuilding.add(primaryKey);
        } else if (choice < 0.8) {
            // Generate a new outer record
            recordsWhileBuilding.add(randomOuterRecord(r));
        } else if (choice < 0.9) {
            // generate a new other record
            recordsWhileBuilding.add(randomOtherRecord(r));
        }
    }

    @Nonnull
    private List<IndexEntry> unnestedEntriesForOuterRecords(@Nonnull Index index,
                                                            @Nonnull List<? extends Message> records) {
        List<IndexEntry> indexEntries = new ArrayList<>();
        final RecordType unnestedType = metaData.getSyntheticRecordType(UNNESTED);
        for (Message rec : records) {
            if (rec instanceof TestRecordsNestedMapProto.OuterRecord) {
                TestRecordsNestedMapProto.OuterRecord outerRecord = (TestRecordsNestedMapProto.OuterRecord)rec;
                for (int i = 0; i < outerRecord.getMap().getEntryCount(); i++) {
                    TestRecordsNestedMapProto.MapRecord.Entry entry = outerRecord.getMap().getEntry(i);
                    final Tuple primaryKey = Tuple.from(unnestedType.getRecordTypeKey(), Tuple.from(outerRecord.getRecId()), Tuple.from(i));
                    final Tuple indexKey = Tuple.from(entry.getKey(), outerRecord.getOtherId(), entry.getValue());
                    indexEntries.add(new IndexEntry(index, indexKey.addAll(primaryKey), TupleHelpers.EMPTY, primaryKey));
                }
            }
        }
        indexEntries.sort(Comparator.comparing(IndexEntry::getKey));
        return indexEntries;
    }

    @Nonnull
    private List<IndexEntry> sumEntriesByGroup(@Nonnull Index index,
                                               @Nonnull List<? extends Message> records) {
        Map<Tuple, Long> sumMap = new TreeMap<>();
        for (Message rec : records) {
            if (rec instanceof TestRecordsNestedMapProto.OuterRecord) {
                TestRecordsNestedMapProto.OuterRecord outerRecord = (TestRecordsNestedMapProto.OuterRecord)rec;
                for (TestRecordsNestedMapProto.MapRecord.Entry entry : outerRecord.getMap().getEntryList()) {
                    Tuple key = Tuple.from(entry.getKey(), outerRecord.getOtherId());
                    sumMap.compute(key, (k, sumSoFar) -> sumSoFar == null ? entry.getIntValue() : sumSoFar + entry.getIntValue());
                }
            }
        }
        List<IndexEntry> entries = new ArrayList<>();
        for (Map.Entry<Tuple, Long> sum : sumMap.entrySet()) {
            entries.add(new IndexEntry(index, sum.getKey(), Tuple.from(sum.getValue()), TupleHelpers.EMPTY));
        }
        return entries;
    }

    void singleValueIndexRebuild(@Nonnull List<Message> records,
                                 @Nullable List<Message> recordsWhileBuilding,
                                 @Nullable List<Tuple> deleteWhileBuilding,
                                 int agents, boolean overlap,
                                 @Nullable Index sourceIndex) {
        final OnlineIndexerTestRecordHandler<Message> recordHandler = OnlineIndexerTestUnnestedRecordHandler.instance();
        final Index index = new Index("keyOtherValueIndex", concat(field(ENTRY_CONSTITUENT).nest("key"), field(PARENT_CONSTITUENT).nest("other_id"), field(ENTRY_CONSTITUENT).nest("value")));
        final Runnable beforeBuild = this::assertUnnestedKeyQueryPlanFails;
        final Runnable afterBuild = this::assertUnnestedKeyQueryPlanFails;

        final Runnable afterReadable = () -> {
            try (FDBRecordContext context = openContext()) {
                final RecordQueryPlan plan = recordStore.planQuery(UNNESTED_KEY_QUERY);
                assertThat(plan.toString(), Matchers.containsString("COVERING(" + index.getName() + " [EQUALS $" + KEY_PARAM + "]"));

                final List<Message> updatedRecords = updated(recordHandler, records, recordsWhileBuilding, deleteWhileBuilding);
                final List<IndexEntry> expectedEntries = unnestedEntriesForOuterRecords(index, updatedRecords);
                try (RecordCursor<IndexEntry> cursor = recordStore.scanIndex(index, IndexScanType.BY_VALUE, TupleRange.ALL, null, ScanProperties.FORWARD_SCAN)) {
                    final List<IndexEntry> scannedEntries = cursor.asList().join();
                    assertEquals(expectedEntries, scannedEntries);
                }

                for (String key : KEYS) {
                    final List<IndexEntry> expectedEntriesForKey = expectedEntries.stream()
                            .filter(entry -> key.equals(entry.getKey().getString(0)))
                            .collect(Collectors.toList());
                    final EvaluationContext evaluationContext = EvaluationContext.forBinding(KEY_PARAM, key);
                    try (RecordCursor<FDBQueriedRecord<Message>> cursor = plan.execute(recordStore, evaluationContext)) {
                        final List<IndexEntry> queriedEntries = cursor.map(FDBQueriedRecord::getIndexEntry).asList().join();
                        assertEquals(expectedEntriesForKey, queriedEntries);
                    }
                }

                context.commit();
            }
        };

        singleRebuild(recordHandler, records, recordsWhileBuilding, deleteWhileBuilding, agents, overlap, true,
                index, sourceIndex, beforeBuild, afterBuild, afterReadable);
    }

    void singleValueIndexRebuild(@Nonnull List<Message> records,
                                 @Nullable List<Message> recordsWhileBuilding,
                                 @Nullable List<Tuple> deleteWhileBuilding,
                                 int agents, boolean overlap) {
        singleValueIndexRebuild(records, recordsWhileBuilding, deleteWhileBuilding, agents, overlap, null);
    }

    void singleValueIndexRebuild(@Nonnull List<Message> records,
                                 @Nullable List<Message> recordsWhileBuilding,
                                 @Nullable List<Tuple> deleteWhileBuilding) {
        singleValueIndexRebuild(records, recordsWhileBuilding, deleteWhileBuilding, 1, false);
    }

    void singleSumIndexRebuild(@Nonnull List<Message> records, @Nullable List<Message> recordsWhileBuilding, @Nullable List<Tuple> deleteWhileBuilding,
                               int agents, boolean overlap, @Nullable Index sourceIndex) {
        final OnlineIndexerTestRecordHandler<Message> recordHandler = OnlineIndexerTestUnnestedRecordHandler.instance();
        final Index index = new Index("keyOtherSumIntValueIndex",
                field(ENTRY_CONSTITUENT).nest("int_value").groupBy(concat(field(ENTRY_CONSTITUENT).nest("key"), field(PARENT_CONSTITUENT).nest("other_id"))),
                IndexTypes.SUM,
                Map.of(IndexOptions.CLEAR_WHEN_ZERO, "true")
        );
        final IndexAggregateFunction indexAggregateFunction = new IndexAggregateFunction(FunctionNames.SUM, index.getRootExpression(), index.getName());

        final Runnable beforeBuild = () -> {
            try (FDBRecordContext context = openContext()) {
                assertThrows(RecordCoreException.class, () -> recordStore.evaluateAggregateFunction(List.of(UNNESTED), indexAggregateFunction, TupleRange.ALL, IsolationLevel.SERIALIZABLE).join());
                context.commit();
            }
        };
        final Runnable afterBuild = () -> {
            try (FDBRecordContext context = openContext()) {
                assertThrows(RecordCoreException.class, () -> recordStore.evaluateAggregateFunction(List.of(UNNESTED), indexAggregateFunction, TupleRange.ALL, IsolationLevel.SERIALIZABLE).join());
                context.commit();
            }
        };

        final Runnable afterReadable = () -> {
            try (FDBRecordContext context = openContext()) {
                final List<Message> updatedRecords = updated(recordHandler, records, recordsWhileBuilding, deleteWhileBuilding);
                final List<IndexEntry> expectedEntries = sumEntriesByGroup(index, updatedRecords);
                try (RecordCursor<IndexEntry> cursor = recordStore.scanIndex(index, IndexScanType.BY_GROUP, TupleRange.ALL, null, ScanProperties.FORWARD_SCAN)) {
                    final List<IndexEntry> scannedEntries = cursor.asList().join();
                    assertEquals(expectedEntries, scannedEntries);
                }

                for (String key : KEYS) {
                    final long expectedSumForKey = expectedEntries.stream()
                            .filter(entry -> key.equals(entry.getKey().getString(0)))
                            .mapToLong(entry -> entry.getValue().getLong(0))
                            .sum();
                    final Tuple evaluatedSum = recordStore.evaluateAggregateFunction(List.of(UNNESTED), indexAggregateFunction, TupleRange.allOf(Tuple.from(key)), IsolationLevel.SERIALIZABLE).join();
                    assertEquals(expectedSumForKey, evaluatedSum.getLong(0));
                }

                context.commit();
            }
        };

        singleRebuild(recordHandler, records, recordsWhileBuilding, deleteWhileBuilding, agents, overlap, true,
                index, sourceIndex, beforeBuild, afterBuild, afterReadable);
    }

    void singleSumIndexRebuild(@Nonnull List<Message> records,
                               @Nullable List<Message> recordsWhileBuilding,
                               @Nullable List<Tuple> deleteWhileBuilding,
                               int agents, boolean overlap) {
        singleSumIndexRebuild(records, recordsWhileBuilding, deleteWhileBuilding, agents, overlap, null);
    }

    void singleSumIndexRebuild(@Nonnull List<Message> records,
                               @Nullable List<Message> recordsWhileBuilding,
                               @Nullable List<Tuple> deleteWhileBuilding) {
        singleSumIndexRebuild(records, recordsWhileBuilding, deleteWhileBuilding, 1, false);
    }

    @ParameterizedTest
    @MethodSource("randomSeeds")
    void simpleTenRecordRebuild(long seed) {
        final Random r = new Random(seed);
        List<Message> records = Stream.generate(() -> randomOuterRecord(r))
                .limit(10)
                .collect(Collectors.toList());
        singleValueIndexRebuild(records, null, null);
    }

    @ParameterizedTest
    @MethodSource("randomSeeds")
    void tenEmptyMapBuild(long seed) {
        final Random r = new Random(seed);
        List<Message> records = Stream.generate(() -> randomOuterRecord(r, r.nextLong(), -1.0))
                .limit(10)
                .collect(Collectors.toList());
        for (Message rec : records) {
            TestRecordsNestedMapProto.OuterRecord outerRecord = (TestRecordsNestedMapProto.OuterRecord)rec;
            assertThat(outerRecord.getMap().getEntryList(), empty());
        }
        singleValueIndexRebuild(records, null, null);
    }

    @ParameterizedTest
    @MethodSource("randomSeeds")
    void tenOuterAndTenOtherRecordRebuild(long seed) {
        final Random r = new Random(seed);
        List<Message> records = Stream.generate(() -> Stream.of(randomOuterRecord(r), randomOtherRecord(r)))
                .flatMap(Function.identity())
                .limit(20)
                .collect(Collectors.toList());
        singleValueIndexRebuild(records, null, null);
    }

    @ParameterizedTest
    @MethodSource("randomSeeds")
    void simpleTenFromSourceIndex(long seed) {
        final Random r = new Random(seed);
        List<Message> records = Stream.generate(() -> randomOuterRecord(r))
                .limit(10)
                .collect(Collectors.toList());
        final Index sourceIndex = new Index("OuterRecord$rec_id", "rec_id");
        singleValueIndexRebuild(records, null, null, 1, false, sourceIndex);
    }

    @ParameterizedTest
    @MethodSource("randomSeeds")
    void tenSumIndexRebuild(long seed) {
        final Random r = new Random(seed);
        List<Message> records = Stream.generate(() -> randomOuterRecord(r))
                .limit(10)
                .collect(Collectors.toList());
        singleSumIndexRebuild(records, null, null);
    }

    @ParameterizedTest
    @MethodSource("randomSeeds")
    void tenEmptyMapSumBuild(long seed) {
        final Random r = new Random(seed);
        List<Message> records = Stream.generate(() -> randomOuterRecord(r, r.nextLong(), -1.0))
                .limit(10)
                .collect(Collectors.toList());
        for (Message rec : records) {
            TestRecordsNestedMapProto.OuterRecord outerRecord = (TestRecordsNestedMapProto.OuterRecord)rec;
            assertThat(outerRecord.getMap().getEntryList(), empty());
        }
        singleSumIndexRebuild(records, null, null);
    }

    @ParameterizedTest
    @MethodSource("randomSeeds")
    @Tag(Tags.Slow)
    void simpleHundredRecordRebuild(long seed) {
        final Random r = new Random(seed);
        List<Message> records = Stream.generate(() -> randomOuterRecord(r))
                .limit(100)
                .collect(Collectors.toList());
        singleValueIndexRebuild(records, null, null);
    }

    @ParameterizedTest
    @MethodSource("randomSeeds")
    @Tag(Tags.Slow)
    void simpleFiveHundredWithUpdates(long seed) {
        final Random r = new Random(seed);
        List<Message> records = Stream.generate(() -> randomOuterRecord(r))
                .limit(500)
                .collect(Collectors.toList());
        List<Message> recordsWhileBuilding = records.stream()
                .filter(rec -> r.nextDouble() < 0.3)
                .map(rec -> randomOuterRecord(r, ((TestRecordsNestedMapProto.OuterRecord)rec).getRecId()))
                .collect(Collectors.toList());
        singleValueIndexRebuild(records, recordsWhileBuilding, null);
    }

    @ParameterizedTest
    @MethodSource("randomSeeds")
    @Tag(Tags.Slow)
    void simpleFiveHundredWithDeletes(long seed) {
        final Random r = new Random(seed);
        List<Message> records = Stream.generate(() -> randomOuterRecord(r))
                .limit(500)
                .collect(Collectors.toList());
        List<Tuple> deleteWhileBuilding = records.stream()
                .filter(rec -> r.nextDouble() < 0.3)
                .map(rec -> Tuple.from(((TestRecordsNestedMapProto.OuterRecord)rec).getRecId()))
                .collect(Collectors.toList());
        singleValueIndexRebuild(records, null, deleteWhileBuilding);
    }

    @ParameterizedTest
    @MethodSource("randomSeeds")
    @Tag(Tags.Slow)
    void fiveHundredWithDeletesAndUpdates(long seed) {
        final Random r = new Random(seed);
        List<Message> records = Stream.generate(() -> randomOuterRecord(r))
                .limit(500)
                .collect(Collectors.toList());

        List<Message> recordsWhileBuilding = new ArrayList<>();
        List<Tuple> deleteWhileBuilding = new ArrayList<>();
        records.forEach(rec -> {
            long recId = ((TestRecordsNestedMapProto.OuterRecord)rec).getRecId();
            double choice = r.nextDouble();
            if (choice < 0.4) {
                // Update an existing record
                recordsWhileBuilding.add(randomOuterRecord(r, recId));
            } else if (choice < 0.5) {
                // Delete an existing record
                deleteWhileBuilding.add(Tuple.from(recId));
            } else if (choice < 0.7) {
                // Insert a brand new record
                recordsWhileBuilding.add(randomOuterRecord(r));
            }
        });
        singleValueIndexRebuild(records, recordsWhileBuilding, deleteWhileBuilding);
    }

    @ParameterizedTest
    @MethodSource("randomSeeds")
    @Tag(Tags.Slow)
    void fiveHundredOfMixedTypesWithDeletesAndUpdates(long seed) {
        final Random r = new Random(seed);
        List<Message> records = Stream.generate(() -> r.nextBoolean() ? randomOuterRecord(r) : randomOtherRecord(r))
                .limit(500)
                .collect(Collectors.toList());

        List<Message> recordsWhileBuilding = new ArrayList<>();
        List<Tuple> deleteWhileBuilding = new ArrayList<>();
        for (Message rec : records) {
            addRandomUpdate(r, rec, recordsWhileBuilding, deleteWhileBuilding);
        }
        singleValueIndexRebuild(records, recordsWhileBuilding, deleteWhileBuilding);
    }

    @ParameterizedTest
    @MethodSource("randomSeeds")
    @Tag(Tags.Slow)
    void sixHundredOfMixedTypesWithDeletesUpdatesAndSourceIndex(long seed) {
        final Random r = new Random(seed);
        List<Message> records = Stream.generate(() -> r.nextBoolean() ? randomOuterRecord(r) : randomOtherRecord(r))
                .limit(600)
                .collect(Collectors.toList());

        List<Message> recordsWhileBuilding = new ArrayList<>();
        List<Tuple> deleteWhileBuilding = new ArrayList<>();
        for (Message rec : records) {
            addRandomUpdate(r, rec, recordsWhileBuilding, deleteWhileBuilding);
        }
        final Index sourceIndex = new Index("OuterRecord$rec_id", "rec_id");
        singleValueIndexRebuild(records, recordsWhileBuilding, deleteWhileBuilding, 1, false, sourceIndex);
    }

    @ParameterizedTest
    @MethodSource("randomSeeds")
    void sumFiveHundredRecords(long seed) {
        final Random r = new Random(seed);
        List<Message> records = Stream.generate(() -> randomOuterRecord(r))
                .limit(500)
                .collect(Collectors.toList());
        singleSumIndexRebuild(records, null, null);
    }

    @ParameterizedTest
    @MethodSource("overlapAndRandomSeeds")
    void sumFiveHundredParallelBuild(boolean overlap, long seed) {
        final Random r = new Random(seed);
        List<Message> records = LongStream.generate(r::nextLong)
                .mapToObj(id -> randomOuterRecord(r, id))
                .limit(500)
                .collect(Collectors.toList());
        singleSumIndexRebuild(records, null, null, 5, overlap);
    }

    @Disabled("non-idempotent indexes on synthetic types do not check the range set correctly")
    @ParameterizedTest
    @MethodSource("randomSeeds")
    void sumSixHundredWithUpdatesAndDeletes(long seed) {
        final Random r = new Random(seed);
        List<Message> records = Stream.generate(() -> r.nextBoolean() ? randomOuterRecord(r) : randomOtherRecord(r))
                .limit(600)
                .collect(Collectors.toList());
        List<Message> recordsWhileBuilding = new ArrayList<>();
        List<Tuple> deleteWhileBuilding = new ArrayList<>();
        for (Message rec : records) {
            addRandomUpdate(r, rec, recordsWhileBuilding, deleteWhileBuilding);
        }
        singleSumIndexRebuild(records, recordsWhileBuilding, deleteWhileBuilding);
    }

    @Tag(Tags.Slow)
    @ParameterizedTest
    @MethodSource("overlapAndRandomSeeds")
    void simpleParallelTwoHundredRebuild(boolean overlap, long seed) {
        final Random r = new Random(seed);
        List<Message> records = LongStream.generate(r::nextLong)
                .mapToObj(id -> randomOuterRecord(r, id))
                .limit(200)
                .collect(Collectors.toList());
        singleValueIndexRebuild(records, null, null, 5, overlap);
    }

    @Tag(Tags.Slow)
    @ParameterizedTest
    @MethodSource("overlapAndRandomSeeds")
    void parallelBuildFiveHundredWithDeletesAndUpdates(boolean overlap, long seed) {
        final Random r = new Random(seed);
        List<Message> records = LongStream.generate(r::nextLong)
                .mapToObj(id -> randomOuterRecord(r, id))
                .limit(500)
                .collect(Collectors.toList());

        List<Message> recordsWhileBuilding = new ArrayList<>();
        List<Tuple> deleteWhileBuilding = new ArrayList<>();
        for (Message rec : records) {
            addRandomUpdate(r, rec, recordsWhileBuilding, deleteWhileBuilding);
        }
        singleValueIndexRebuild(records, recordsWhileBuilding, deleteWhileBuilding, 5, overlap);
    }

    @Tag(Tags.Slow)
    @ParameterizedTest
    @MethodSource("randomSeeds")
    void parallelBuildSixHundredWithDeletesAndUpdatesFromIndex(long seed) {
        final Random r = new Random(seed);
        List<Message> records = LongStream.generate(r::nextLong)
                .mapToObj(id -> randomOuterRecord(r, id))
                .limit(600)
                .collect(Collectors.toList());

        List<Message> recordsWhileBuilding = new ArrayList<>();
        List<Tuple> deleteWhileBuilding = new ArrayList<>();
        for (Message rec : records) {
            addRandomUpdate(r, rec, recordsWhileBuilding, deleteWhileBuilding);
        }
        final Index sourceIndex = new Index("OuterRecord$rec_id", "rec_id");
        singleValueIndexRebuild(records, recordsWhileBuilding, deleteWhileBuilding, 5, true, sourceIndex);
    }

    @Test
    void doNotAllowBuildingIndexFromUnnestedIndex() {
        final OnlineIndexerTestUnnestedRecordHandler recordHandler = OnlineIndexerTestUnnestedRecordHandler.instance();
        final Index sourceIndex = new Index("sourceIndex", field(PARENT_CONSTITUENT).nest("rec_id"));
        final Index targetIndex = new Index("targetIndex", concat(field("entry").nest("key"), field(PARENT_CONSTITUENT).nest("rec_id")));
        final FDBRecordStoreTestBase.RecordMetaDataHook hook = recordHandler.addUnnestedType().andThen(metaDataBuilder -> {
            metaDataBuilder.addIndex(UNNESTED, sourceIndex);
            metaDataBuilder.addIndex(UNNESTED, targetIndex);
        });
        openMetaData(recordHandler.getFileDescriptor(), hook);
        FDBRecordStore.Builder storeBuilder;
        try (FDBRecordContext context = openContext()) {
            storeBuilder = recordStore.asBuilder();
            context.commit();
        }

        try (OnlineIndexer indexer = OnlineIndexer.newBuilder()
                .setRecordStoreBuilder(storeBuilder)
                .setIndex(targetIndex)
                .setIndexingPolicy(OnlineIndexer.IndexingPolicy.newBuilder()
                        .setSourceIndex(sourceIndex.getName())
                        .setIfDisabled(OnlineIndexer.IndexingPolicy.DesiredAction.REBUILD)
                        .setIfWriteOnly(OnlineIndexer.IndexingPolicy.DesiredAction.REBUILD)
                        .setIfReadable(OnlineIndexer.IndexingPolicy.DesiredAction.REBUILD)
                        .setForbidRecordScan(true)
                )
                .build()) {
            assertThrows(IndexingBase.ValidationException.class, indexer::buildIndex);
        }
    }
}
