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
import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TestRecordsNestedMapProto;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.record.metadata.UnnestedRecordTypeBuilder;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.TupleHelpers;
import com.apple.test.BooleanSource;
import com.apple.test.Tags;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
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
@SuppressWarnings("try")
abstract class OnlineIndexerBuildUnnestedIndexTest extends OnlineIndexerBuildIndexTest {
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

    OnlineIndexerBuildUnnestedIndexTest(boolean safeBuild) {
        super(safeBuild);
    }

    public static class OnlineIndexerTestUnnestedRecordHandler implements OnlineIndexerTestRecordHandler<Message> {
        @Nonnull
        private static OnlineIndexerTestUnnestedRecordHandler INSTANCE = new OnlineIndexerTestUnnestedRecordHandler();

        private OnlineIndexerTestUnnestedRecordHandler() {
        }

        @Override
        public Descriptors.FileDescriptor getFileDescriptor() {
            return TestRecordsNestedMapProto.getDescriptor();
        }

        @Nonnull
        @Override
        public FDBRecordStoreTestBase.RecordMetaDataHook baseHook(final boolean splitLongRecords, @Nullable final Index sourceIndex) {
            return metaDataBuilder -> {
                metaDataBuilder.setSplitLongRecords(splitLongRecords);

                final UnnestedRecordTypeBuilder unnestedBuilder = metaDataBuilder.addUnnestedRecordType(UNNESTED);
                unnestedBuilder.addParentConstituent(PARENT_CONSTITUENT, metaDataBuilder.getRecordType("OuterRecord"));
                unnestedBuilder.addNestedConstituent(ENTRY_CONSTITUENT, TestRecordsNestedMapProto.MapRecord.Entry.getDescriptor(), PARENT_CONSTITUENT,
                        field("map").nest(field("entry", FanType.FanOut)));

                if (sourceIndex != null) {
                    // TODO: We should consider whether we are okay with source indexes on the unnested record type or not
                    metaDataBuilder.addIndex("OuterRecord", sourceIndex);
                }
            };
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

    private void assertUnnestedKeyQueryPlanFails() {
        try (FDBRecordContext context = openContext()) {
            assertThrows(RecordCoreException.class, () -> recordStore.planQuery(UNNESTED_KEY_QUERY));
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
                        .setIntValue(r.nextInt(50));
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
                .setOtherId(r.nextInt(50))
                .setOtherValue(KEYS.get(r.nextInt(KEYS.size())))
                .build();
    }

    @Nonnull
    private TestRecordsNestedMapProto.OtherRecord randomOtherRecord(@Nonnull Random r) {
        return randomOtherRecord(r, r.nextLong());
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

    void singleValueIndexRebuild(@Nonnull List<Message> records,
                                 @Nullable List<Message> recordsWhileBuilding,
                                 @Nullable List<Tuple> deleteWhileBuilding,
                                 int agents, boolean overlap) {
        final OnlineIndexerTestRecordHandler<Message> recordHandler = OnlineIndexerTestUnnestedRecordHandler.instance();
        final Index index = new Index("keyOtherValueIndex", concat(field(ENTRY_CONSTITUENT).nest("key"), field(PARENT_CONSTITUENT).nest("other_id"), field(ENTRY_CONSTITUENT).nest("value")));
        final Runnable beforeBuild = this::assertUnnestedKeyQueryPlanFails;
        final Runnable afterBuild = this::assertUnnestedKeyQueryPlanFails;

        final Runnable afterReadable = () -> {
            try (FDBRecordContext context = openContext()) {
                final RecordQueryPlan plan = recordStore.planQuery(UNNESTED_KEY_QUERY);
                assertThat(plan.toString(), Matchers.containsString("Covering(Index(" + index.getName() + " [EQUALS $" + KEY_PARAM + "])"));

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
            }
        };

        singleRebuild(recordHandler, records, recordsWhileBuilding, deleteWhileBuilding, agents, overlap, true,
                index, null, beforeBuild, afterBuild, afterReadable);
    }

    void singleValueIndexRebuild(@Nonnull List<Message> records,
                                 @Nullable List<Message> recordsWhileBuilding,
                                 @Nullable List<Tuple> deleteWhileBuilding) {
        singleValueIndexRebuild(records, recordsWhileBuilding, deleteWhileBuilding, 1, false);
    }

    @Test
    void simpleTenRecordRebuild() {
        final Random r = new Random(0x5ca1e);
        List<Message> records = Stream.generate(() -> randomOuterRecord(r))
                .limit(10)
                .collect(Collectors.toList());
        singleValueIndexRebuild(records, null, null);
    }

    @Test
    void tenEmptyMapBuild() {
        final Random r = new Random(0x0fdb0fdb);
        List<Message> records = Stream.generate(() -> randomOuterRecord(r, r.nextLong(), -1.0))
                .limit(10)
                .collect(Collectors.toList());
        for (Message rec : records) {
            TestRecordsNestedMapProto.OuterRecord outerRecord = (TestRecordsNestedMapProto.OuterRecord)rec;
            assertThat(outerRecord.getMap().getEntryList(), empty());
        }
        singleValueIndexRebuild(records, null, null);
    }

    @Test
    void tenOuterAndTenOtherRecordRebuild() {
        final Random r = new Random(0x5ca1e);
        List<Message> records = Stream.generate(() -> Stream.of(randomOuterRecord(r), randomOtherRecord(r)))
                .flatMap(Function.identity())
                .limit(20)
                .collect(Collectors.toList());
        singleValueIndexRebuild(records, null, null);
    }

    @Test
    @Tag(Tags.Slow)
    void simpleHundredRecordRebuild() {
        final Random r = new Random(0x5ca1e);
        List<Message> records = Stream.generate(() -> randomOuterRecord(r))
                .limit(100)
                .collect(Collectors.toList());
        singleValueIndexRebuild(records, null, null);
    }

    @Test
    @Tag(Tags.Slow)
    void simpleFiveHundredWithUpdates() {
        final Random r = new Random(0xfdb0);
        List<Message> records = Stream.generate(() -> randomOuterRecord(r))
                .limit(500)
                .collect(Collectors.toList());
        List<Message> recordsWhileBuilding = records.stream()
                .filter(rec -> r.nextDouble() < 0.3)
                .map(rec -> randomOuterRecord(r, ((TestRecordsNestedMapProto.OuterRecord)rec).getRecId()))
                .collect(Collectors.toList());
        singleValueIndexRebuild(records, recordsWhileBuilding, null);
    }

    @Test
    @Tag(Tags.Slow)
    void simpleFiveHundredWithDeletes() {
        final Random r = new Random(0xfdb5ca1eL);
        List<Message> records = Stream.generate(() -> randomOuterRecord(r))
                .limit(500)
                .collect(Collectors.toList());
        List<Tuple> deleteWhileBuilding = records.stream()
                .filter(rec -> r.nextDouble() < 0.3)
                .map(rec -> Tuple.from(((TestRecordsNestedMapProto.OuterRecord)rec).getRecId()))
                .collect(Collectors.toList());
        singleValueIndexRebuild(records, null, deleteWhileBuilding);
    }

    @Test
    @Tag(Tags.Slow)
    void simpleFiveHundredWithDeletesAndUpdates() {
        final Random r = new Random(0x13370fdbL);
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

    @Test
    @Tag(Tags.Slow)
    void fiveHundredOfMixedTypesWithDeletesAndUpdates() {
        final Random r = new Random(0xf007ba1L);
        List<Message> records = Stream.generate(() -> r.nextBoolean() ? randomOuterRecord(r) : randomOtherRecord(r))
                .limit(500)
                .collect(Collectors.toList());

        final OnlineIndexerTestRecordHandler<Message> recordHandler = OnlineIndexerTestUnnestedRecordHandler.instance();
        List<Message> recordsWhileBuilding = new ArrayList<>();
        List<Tuple> deleteWhileBuilding = new ArrayList<>();
        records.forEach(rec -> {
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
        });
        singleValueIndexRebuild(records, recordsWhileBuilding, deleteWhileBuilding);
    }

    @Tag(Tags.Slow)
    @ParameterizedTest(name = "simpleParallelTwoHundredRebuild[overlap={0}]")
    @BooleanSource
    void simpleParallelTwoHundredRebuild(boolean overlap) {
        final Random r = new Random(0x0fdb0fdb);
        List<Message> records = LongStream.range(0L, 200L)
                .mapToObj(id -> randomOuterRecord(r, id))
                .collect(Collectors.toList());
        singleValueIndexRebuild(records, null, null, 5, overlap);
    }

    @Tag(Tags.Slow)
    @ParameterizedTest
    @BooleanSource
    void parallelBuildFiveHundredWithDeletesAndUpdates(boolean overlap) {
        final Random r = new Random(0x0fdb5caL);
        List<Message> records = LongStream.range(0L, 500L)
                .mapToObj(id -> randomOuterRecord(r, id))
                .collect(Collectors.toList());

        List<Message> recordsWhileBuilding = new ArrayList<>();
        List<Tuple> deleteWhileBuilding = new ArrayList<>();
        records.forEach(rec -> {
            TestRecordsNestedMapProto.OuterRecord outerRecord = (TestRecordsNestedMapProto.OuterRecord)rec;
            double choice = r.nextDouble();
            if (choice < 0.4) {
                // Update an existing record
                recordsWhileBuilding.add(randomOuterRecord(r, outerRecord.getRecId()));
            } else if (choice < 0.5) {
                // Delete an existing record
                deleteWhileBuilding.add(Tuple.from(outerRecord.getRecId()));
            } else if (choice < 0.7) {
                // Insert a brand new record
                recordsWhileBuilding.add(randomOuterRecord(r));
            }
        });
        singleValueIndexRebuild(records, recordsWhileBuilding, deleteWhileBuilding, 5, overlap);
    }

    public static class Safe extends OnlineIndexerBuildUnnestedIndexTest {
        Safe() {
            super(true);
        }

    }

    public static class Unsafe extends OnlineIndexerBuildUnnestedIndexTest {
        Unsafe() {
            super(false);
        }
    }
}
