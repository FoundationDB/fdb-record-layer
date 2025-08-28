/*
 * OnlineIndexerBuildJoinedIndexTest.java
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

import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.IsolationLevel;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TestRecordsJoinIndexProto;
import com.apple.foundationdb.record.TupleFieldsProto;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexAggregateFunction;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.JoinedRecordTypeBuilder;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.record.metadata.expressions.GroupingKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.TupleFieldsHelper;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.TupleHelpers;
import com.apple.test.Tags;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;
import static com.apple.foundationdb.record.metadata.Key.Expressions.concatenateFields;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Tests for building indexes on {@link com.apple.foundationdb.record.metadata.JoinedRecordType}s.
 */
@SuppressWarnings("try")
class OnlineIndexerBuildJoinedIndexTest extends OnlineIndexerBuildIndexTest {

    static class OnlineIndexerJoinedRecordHandler implements OnlineIndexerTestRecordHandler<Message> {
        private static final OnlineIndexerJoinedRecordHandler INSTANCE = new OnlineIndexerJoinedRecordHandler();

        private OnlineIndexerJoinedRecordHandler() {
        }

        private static final Set<Descriptors.Descriptor> recNoDescriptors = Set.of(
                TestRecordsJoinIndexProto.MySimpleRecord.getDescriptor(),
                TestRecordsJoinIndexProto.MyOtherRecord.getDescriptor(),
                TestRecordsJoinIndexProto.JoiningRecord.getDescriptor(),
                TestRecordsJoinIndexProto.TypeA.getDescriptor(),
                TestRecordsJoinIndexProto.TypeB.getDescriptor(),
                TestRecordsJoinIndexProto.TypeC.getDescriptor(),
                TestRecordsJoinIndexProto.NestedA.getDescriptor(),
                TestRecordsJoinIndexProto.NestedB.getDescriptor()
        );
        private static final Set<Descriptors.Descriptor> uuidDescriptors = Set.of(
                TestRecordsJoinIndexProto.Customer.getDescriptor(),
                TestRecordsJoinIndexProto.Order.getDescriptor(),
                TestRecordsJoinIndexProto.Item.getDescriptor()
        );
        private static final Set<Descriptors.Descriptor> headerDescriptors = Set.of(
                TestRecordsJoinIndexProto.CustomerWithHeader.getDescriptor(),
                TestRecordsJoinIndexProto.OrderWithHeader.getDescriptor()
        );

        private static final String SIMPLE_OTHER_JOIN_TYPE = "SimpleOtherJoin";

        @Override
        public Descriptors.FileDescriptor getFileDescriptor() {
            return TestRecordsJoinIndexProto.getDescriptor();
        }

        @Nonnull
        private FDBRecordStoreTestBase.RecordMetaDataHook setPrimaryKeyHook() {
            return metaDataBuilder -> {
                metaDataBuilder.getRecordType("CustomerWithHeader").setPrimaryKey(field("___header").nest(concatenateFields("z_key", "rec_id")));
                metaDataBuilder.getRecordType("OrderWithHeader").setPrimaryKey(field("___header").nest(concatenateFields("z_key", "rec_id")));
            };
        }

        @Nonnull
        private FDBRecordStoreTestBase.RecordMetaDataHook addSimpleOtherJoinTypeHook() {
            return metaDataBuilder -> {
                final JoinedRecordTypeBuilder joinedRecordTypeBuilder = metaDataBuilder.addJoinedRecordType(SIMPLE_OTHER_JOIN_TYPE);
                joinedRecordTypeBuilder.addConstituent("simple", metaDataBuilder.getRecordType("MySimpleRecord"));
                joinedRecordTypeBuilder.addConstituent("other", metaDataBuilder.getRecordType("MyOtherRecord"));
                joinedRecordTypeBuilder.addJoin("simple", "num_value", "other", "num_value");
                joinedRecordTypeBuilder.addJoin("simple", "other_rec_no", "other", "rec_no");

                // Add an index on MySimpleRecord to use when executing the join
                metaDataBuilder.addIndex("MySimpleRecord", "num_value", "other_rec_no");
            };
        }

        @Nonnull
        @Override
        public FDBRecordStoreTestBase.RecordMetaDataHook baseHook(final boolean splitLongRecords, @Nullable final Index sourceIndex) {
            return setPrimaryKeyHook().andThen(addSimpleOtherJoinTypeHook()).andThen(metaDataBuilder -> {
                metaDataBuilder.setSplitLongRecords(splitLongRecords);
                assertNull(sourceIndex, "Join indexes do not support building from a source index");
            });
        }

        @Nonnull
        @Override
        public FDBRecordStoreTestBase.RecordMetaDataHook addIndexHook(@Nonnull final Index index) {
            return metaDataBuilder -> metaDataBuilder.addIndex(SIMPLE_OTHER_JOIN_TYPE, index);
        }

        @Nonnull
        @Override
        public Tuple getPrimaryKey(@Nonnull final Message message) {
            if (recNoDescriptors.contains(message.getDescriptorForType())) {
                final Descriptors.FieldDescriptor recNoDescriptor = message.getDescriptorForType().findFieldByName("rec_no");
                return Tuple.from(message.getField(recNoDescriptor));
            } else if (uuidDescriptors.contains(message.getDescriptorForType())) {
                final Descriptors.FieldDescriptor uuidDescriptor = message.getDescriptorForType().findFieldByName("uuid");
                final Message uuidMessage = (Message) message.getField(uuidDescriptor);
                final TupleFieldsProto.UUID uuid = TupleFieldsProto.UUID.newBuilder()
                        .mergeFrom(uuidMessage)
                        .build();
                return Tuple.from(TupleFieldsHelper.fromProto(uuid));
            } else if (headerDescriptors.contains(message.getDescriptorForType())) {
                final Descriptors.FieldDescriptor headerFieldDescriptor = message.getDescriptorForType().findFieldByName("___header");
                final Message headerMessage = (Message) message.getField(headerFieldDescriptor);
                final TestRecordsJoinIndexProto.Header header = TestRecordsJoinIndexProto.Header.newBuilder()
                        .mergeFrom(headerMessage)
                        .build();
                return Tuple.from(header.getZKey(), header.getRecId());
            } else {
                return fail("Unable to get primary key for message: " + message);
            }
        }

        public static OnlineIndexerJoinedRecordHandler instance() {
            return INSTANCE;
        }
    }

    @Nonnull
    private List<IndexEntry> joinValueIndexEntries(@Nonnull Index index, @Nonnull final List<Message> messages) {
        final Map<Integer, Map<Long, TestRecordsJoinIndexProto.MyOtherRecord>> otherRecords = new HashMap<>();
        // Collect all of the other records. Sort by num_value and rec_no
        for (Message message : messages) {
            if (message instanceof TestRecordsJoinIndexProto.MyOtherRecord) {
                TestRecordsJoinIndexProto.MyOtherRecord otherRecord = (TestRecordsJoinIndexProto.MyOtherRecord) message;
                Map<Long, TestRecordsJoinIndexProto.MyOtherRecord> otherRecordsByNumValue = otherRecords.computeIfAbsent(otherRecord.getNumValue(), HashMap::new);
                otherRecordsByNumValue.put(otherRecord.getRecNo(), otherRecord);
            }
        }
        // For each simple record, look for corresponding other records
        final List<IndexEntry> indexEntries = new ArrayList<>();
        final RecordType joinedRecordType = metaData.getSyntheticRecordType(OnlineIndexerJoinedRecordHandler.SIMPLE_OTHER_JOIN_TYPE);
        for (Message message : messages) {
            if (message instanceof TestRecordsJoinIndexProto.MySimpleRecord) {
                TestRecordsJoinIndexProto.MySimpleRecord simpleRecord = (TestRecordsJoinIndexProto.MySimpleRecord) message;
                Map<Long, TestRecordsJoinIndexProto.MyOtherRecord> otherRecordsByNumValue = otherRecords.get(simpleRecord.getNumValue());
                if (otherRecordsByNumValue != null) {
                    TestRecordsJoinIndexProto.MyOtherRecord otherRecord = otherRecordsByNumValue.get(simpleRecord.getOtherRecNo());
                    if (otherRecord != null) {
                        final Tuple indexKey = Tuple.from(simpleRecord.getNumValue(), otherRecord.getNumValue3(), simpleRecord.getNumValue2());
                        final Tuple primaryKey = Tuple.from(joinedRecordType.getRecordTypeKey(), Tuple.from(simpleRecord.getRecNo()), Tuple.from(otherRecord.getRecNo()));
                        indexEntries.add(new IndexEntry(index, indexKey.addAll(primaryKey), TupleHelpers.EMPTY, primaryKey));
                    }
                }
            }
        }
        // Sort by key
        indexEntries.sort(Comparator.comparing(IndexEntry::getKey));
        return indexEntries;
    }

    void singleValueIndexRebuild(@Nonnull List<Message> records, @Nonnull List<Message> recordsWhileBuilding, @Nonnull List<Tuple> deleteWhileBuilding, int agents, boolean overlap) {
        final OnlineIndexerJoinedRecordHandler recordHandler = OnlineIndexerJoinedRecordHandler.instance();

        final Index joinIndex = new Index("joinIndex", concat(field("simple").nest("num_value"), field("other").nest("num_value_3"), field("simple").nest("num_value_2")));
        final String numValueParam = "numValue";
        final String numValue3Param = "numValue3";
        final RecordQuery joinQuery = RecordQuery.newBuilder()
                .setRecordType(OnlineIndexerJoinedRecordHandler.SIMPLE_OTHER_JOIN_TYPE)
                .setFilter(Query.and(
                        Query.field("simple").matches(Query.field("num_value").equalsParameter(numValueParam)),
                        Query.field("other").matches(Query.field("num_value_3").equalsParameter(numValue3Param))
                ))
                .setSort(field("simple").nest("num_value_2"))
                .build();

        final Runnable beforeBuild = () -> {
            try (FDBRecordContext context = openContext()) {
                assertThrows(RecordCoreException.class, () -> recordStore.planQuery(joinQuery));
            }
        };
        final Runnable afterBuild = () -> {
            try (FDBRecordContext context = openContext()) {
                assertThrows(RecordCoreException.class, () -> recordStore.planQuery(joinQuery));
            }
        };
        final Runnable afterReadable = () -> {
            try (FDBRecordContext context = openContext()) {
                final RecordQueryPlan plan = recordStore.planQuery(joinQuery);
                assertThat(plan.toString(), Matchers.containsString("ISCAN(" + joinIndex.getName() + " [EQUALS $" + numValueParam + ", EQUALS $" + numValue3Param + "])"));

                final List<Message> updatedRecords = updated(recordHandler, records, recordsWhileBuilding, deleteWhileBuilding);
                final List<IndexEntry> expectedEntries = joinValueIndexEntries(joinIndex, updatedRecords);
                try (RecordCursor<IndexEntry> cursor = recordStore.scanIndex(joinIndex, IndexScanType.BY_VALUE, TupleRange.ALL, null, ScanProperties.FORWARD_SCAN)) {
                    List<IndexEntry> scannedEntries = cursor.asList().join();
                    assertEquals(expectedEntries, scannedEntries);
                }
            }
        };
        singleRebuild(recordHandler, records, recordsWhileBuilding, deleteWhileBuilding, agents, overlap, true, joinIndex, null, beforeBuild, afterBuild, afterReadable);
    }

    void singleValueIndexRebuild(@Nonnull List<Message> records, @Nullable List<Message> recordsWhileBuilding, @Nullable List<Tuple> deleteWhileBuilding) {
        singleValueIndexRebuild(records, recordsWhileBuilding, deleteWhileBuilding, 1, false);
    }

    void singleCountIndexRebuild(@Nonnull List<Message> records, @Nullable List<Message> recordsWhileBuilding, @Nullable List<Tuple> deleteWhileBuilding, int agents, boolean overlap) {
        final OnlineIndexerJoinedRecordHandler recordHandler = OnlineIndexerJoinedRecordHandler.instance();
        final Index joinIndex = new Index("joinIndex", new GroupingKeyExpression(field("simple").nest("num_value"), 0), IndexTypes.COUNT);
        final IndexAggregateFunction indexAggregateFunction = new IndexAggregateFunction(IndexTypes.COUNT, field("num_value"), joinIndex.getName());

        final Runnable beforeBuild = () -> {
            try (FDBRecordContext context = openContext()) {
                assertThrows(RecordCoreException.class, () -> recordStore.evaluateAggregateFunction(List.of(OnlineIndexerJoinedRecordHandler.SIMPLE_OTHER_JOIN_TYPE), indexAggregateFunction, TupleRange.ALL, IsolationLevel.SERIALIZABLE).join());
            }
        };
        final Runnable afterBuild = () -> {
            try (FDBRecordContext context = openContext()) {
                assertThrows(RecordCoreException.class, () -> recordStore.evaluateAggregateFunction(List.of(OnlineIndexerJoinedRecordHandler.SIMPLE_OTHER_JOIN_TYPE), indexAggregateFunction, TupleRange.ALL, IsolationLevel.SERIALIZABLE).join());
            }
        };
        final Runnable afterReadable = () -> {
            try (FDBRecordContext context = openContext()) {
                final List<Message> updatedRecords = updated(recordHandler, records, recordsWhileBuilding, deleteWhileBuilding);
                final List<IndexEntry> entries = joinValueIndexEntries(joinIndex, updatedRecords);

                final Tuple wholeCount = recordStore.evaluateAggregateFunction(List.of(OnlineIndexerJoinedRecordHandler.SIMPLE_OTHER_JOIN_TYPE), indexAggregateFunction, TupleRange.ALL, IsolationLevel.SERIALIZABLE).join();
                assertEquals(entries.size(), wholeCount.getLong(0));

                final Set<Long> numValues = entries.stream()
                        .map(entry -> entry.getKey().getLong(0))
                        .collect(Collectors.toSet());
                for (long numValue : numValues) {
                    final Tuple numValueCount = recordStore.evaluateAggregateFunction(List.of(OnlineIndexerJoinedRecordHandler.SIMPLE_OTHER_JOIN_TYPE), indexAggregateFunction, TupleRange.allOf(Tuple.from(numValue)), IsolationLevel.SERIALIZABLE).join();
                    long expectedCount = entries.stream()
                            .map(entry -> entry.getKey().getLong(0))
                            .filter(entryNumValue -> entryNumValue == numValue)
                            .count();
                    assertEquals(expectedCount, numValueCount.getLong(0));
                }
                context.commit();
            }
        };
        singleRebuild(recordHandler, records, recordsWhileBuilding, deleteWhileBuilding, agents, overlap, true, joinIndex, null, beforeBuild, afterBuild, afterReadable);
    }

    void singleCountIndexRebuild(@Nonnull List<Message> records, @Nullable List<Message> recordsWhileBuilding, @Nullable List<Tuple> deleteWhileBuilding) {
        singleCountIndexRebuild(records, recordsWhileBuilding, deleteWhileBuilding, 1, false);
    }

    @Nonnull
    private TestRecordsJoinIndexProto.MySimpleRecord randomSimpleRecord(@Nonnull Random r, long recNo, int numValue, long otherRecNo) {
        return TestRecordsJoinIndexProto.MySimpleRecord.newBuilder()
                .setRecNo(recNo)
                .setNumValue(numValue)
                .setOtherRecNo(otherRecNo)
                .setNumValue2(r.nextInt(20))
                .build();
    }

    @Nonnull
    private TestRecordsJoinIndexProto.MySimpleRecord randomSimpleRecord(@Nonnull Random r, long recNo) {
        return randomSimpleRecord(r, recNo, r.nextInt(5), r.nextLong());
    }

    @Nonnull
    private TestRecordsJoinIndexProto.MySimpleRecord randomSimpleRecord(@Nonnull Random r) {
        return randomSimpleRecord(r, r.nextLong());
    }

    @Nonnull
    private TestRecordsJoinIndexProto.MySimpleRecord randomJoinedSimpleRecord(@Nonnull Random r, @Nonnull TestRecordsJoinIndexProto.MyOtherRecord otherRecord) {
        return randomSimpleRecord(r, r.nextLong(), otherRecord.getNumValue(), otherRecord.getRecNo());
    }

    @Nonnull
    private TestRecordsJoinIndexProto.MyOtherRecord randomOtherRecord(@Nonnull Random r, int numValue, long recNo) {
        return TestRecordsJoinIndexProto.MyOtherRecord.newBuilder()
                .setRecNo(recNo)
                .setNumValue3(r.nextInt(10))
                .setNumValue(numValue)
                .build();
    }

    @Nonnull
    private TestRecordsJoinIndexProto.MyOtherRecord randomOtherRecord(@Nonnull Random r) {
        return randomOtherRecord(r, r.nextInt(5), r.nextLong());
    }

    @Nonnull
    private TestRecordsJoinIndexProto.MyOtherRecord randomJoinedOther(@Nonnull Random r, @Nonnull TestRecordsJoinIndexProto.MySimpleRecord simpleRecord) {
        return randomOtherRecord(r, simpleRecord.getNumValue(), simpleRecord.getOtherRecNo());
    }

    private void addRandomUpdate(@Nonnull Random r, @Nonnull Message rec, @Nonnull List<Message> recordsWhileBuilding, @Nonnull List<Tuple> deleteWhileBuilding) {
        double choice = r.nextDouble();
        if (choice < 0.1) {
            // Delete this record
            deleteWhileBuilding.add(OnlineIndexerJoinedRecordHandler.instance().getPrimaryKey(rec));
        } else if (choice < 0.2) {
            // Rewrite this record, but preserve the join
            if (rec instanceof TestRecordsJoinIndexProto.MySimpleRecord) {
                TestRecordsJoinIndexProto.MySimpleRecord simple = (TestRecordsJoinIndexProto.MySimpleRecord)rec;
                recordsWhileBuilding.add(randomSimpleRecord(r, simple.getRecNo(), simple.getNumValue(), simple.getOtherRecNo()));
            } else if (rec instanceof TestRecordsJoinIndexProto.MyOtherRecord) {
                TestRecordsJoinIndexProto.MyOtherRecord other = (TestRecordsJoinIndexProto.MyOtherRecord)rec;
                recordsWhileBuilding.add(randomOtherRecord(r, other.getNumValue(), other.getRecNo()));
            }
        } else if (choice < 0.4) {
            // Rewrite this record, breaking the join
            if (rec instanceof TestRecordsJoinIndexProto.MySimpleRecord) {
                TestRecordsJoinIndexProto.MySimpleRecord simple = (TestRecordsJoinIndexProto.MySimpleRecord)rec;
                recordsWhileBuilding.add(randomSimpleRecord(r, simple.getRecNo()));
            } else if (rec instanceof TestRecordsJoinIndexProto.MyOtherRecord) {
                TestRecordsJoinIndexProto.MyOtherRecord other = (TestRecordsJoinIndexProto.MyOtherRecord)rec;
                recordsWhileBuilding.add(randomOtherRecord(r, r.nextInt(5), other.getRecNo()));
            }
        } else if (choice < 0.6) {
            // Remove the join from the original record, and replace it with a new one
            if (rec instanceof TestRecordsJoinIndexProto.MySimpleRecord) {
                TestRecordsJoinIndexProto.MySimpleRecord simple = (TestRecordsJoinIndexProto.MySimpleRecord)rec;
                TestRecordsJoinIndexProto.MySimpleRecord newSimple = randomSimpleRecord(r, simple.getRecNo());
                recordsWhileBuilding.add(newSimple);
                recordsWhileBuilding.add(randomJoinedOther(r, newSimple));
            } else if (rec instanceof TestRecordsJoinIndexProto.MyOtherRecord) {
                TestRecordsJoinIndexProto.MyOtherRecord other = (TestRecordsJoinIndexProto.MyOtherRecord)rec;
                TestRecordsJoinIndexProto.MyOtherRecord newOther = randomOtherRecord(r, r.nextInt(5), other.getRecNo());
                recordsWhileBuilding.add(newOther);
                recordsWhileBuilding.add(randomJoinedSimpleRecord(r, newOther));
            }
        } else if (choice < 0.8) {
            // Add new records
            double newRecordChoice = r.nextDouble();
            if (newRecordChoice < 0.2) {
                recordsWhileBuilding.add(randomSimpleRecord(r));
            } else if (newRecordChoice < 0.4) {
                recordsWhileBuilding.add(randomOtherRecord(r));
            } else {
                TestRecordsJoinIndexProto.MySimpleRecord newSimple = randomSimpleRecord(r);
                recordsWhileBuilding.add(newSimple);
                recordsWhileBuilding.add(randomJoinedOther(r, newSimple));
            }
        }
    }

    @ParameterizedTest
    @MethodSource("randomSeeds")
    void simpleTenJoinedRecords(long seed) {
        final Random r = new Random(seed);
        final List<Message> records = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            TestRecordsJoinIndexProto.MySimpleRecord simpleRecord = randomSimpleRecord(r);
            records.add(simpleRecord);
            records.add(randomJoinedOther(r, simpleRecord));
        }
        singleValueIndexRebuild(records, null, null);
    }

    @ParameterizedTest
    @MethodSource("randomSeeds")
    void simpleJoinIsEmpty(long seed) {
        final Random r = new Random(seed);
        final List<Message> records = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            TestRecordsJoinIndexProto.MySimpleRecord simpleRecord = randomSimpleRecord(r);
            records.add(simpleRecord);
            records.add(randomOtherRecord(r, simpleRecord.getNumValue(), r.nextLong()));
        }
        singleValueIndexRebuild(records, null, null);
        try (FDBRecordContext context = openContext()) {
            // Validate that the index is empty
            try (RecordCursor<IndexEntry> cursor = recordStore.scanIndex(metaData.getIndex("joinIndex"), IndexScanType.BY_VALUE, TupleRange.ALL, null, ScanProperties.FORWARD_SCAN)) {
                assertThat(cursor.asList().join(), Matchers.empty());
            }
        }
    }

    @ParameterizedTest
    @MethodSource("randomSeeds")
    void simpleJoinWithExtraUnjoinedRecords(long seed) {
        final Random r = new Random(seed);
        final List<Message> records = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            TestRecordsJoinIndexProto.MySimpleRecord simpleRecord = randomSimpleRecord(r);
            records.add(simpleRecord);
            TestRecordsJoinIndexProto.MyOtherRecord otherRecord = randomJoinedOther(r, simpleRecord);
            records.add(otherRecord);

            TestRecordsJoinIndexProto.JoiningRecord joiningRecord = TestRecordsJoinIndexProto.JoiningRecord.newBuilder()
                    .setRecNo(r.nextLong())
                    .setSimpleRecNo(simpleRecord.getRecNo())
                    .setOtherRecNo(otherRecord.getRecNo())
                    .build();
            records.add(joiningRecord);
        }
        singleValueIndexRebuild(records, null, null);
    }

    @ParameterizedTest
    @MethodSource("randomSeeds")
    @Tag(Tags.Slow)
    void simpleValueWithUpdatesAndDeletes(long seed) {
        final Random r = new Random(seed);
        final List<Message> records = new ArrayList<>();
        for (int i = 0; i < 400; i++) {
            TestRecordsJoinIndexProto.MySimpleRecord simple = randomSimpleRecord(r);
            records.add(simple);
            TestRecordsJoinIndexProto.MyOtherRecord other = randomJoinedOther(r, simple);
            records.add(other);
        }
        final OnlineIndexerJoinedRecordHandler recordHandler = OnlineIndexerJoinedRecordHandler.instance();
        final List<Message> recordsWhileBuilding = new ArrayList<>();
        final List<Tuple> deleteWhileBuilding = new ArrayList<>();
        for (Message rec : records) {
            addRandomUpdate(r, rec, recordsWhileBuilding, deleteWhileBuilding);
        }
        singleValueIndexRebuild(records, recordsWhileBuilding, deleteWhileBuilding);
    }

    @ParameterizedTest
    @MethodSource("randomSeeds")
    void simpleTenJoinedCountRecords(long seed) {
        final Random r = new Random(seed);
        final List<Message> records = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            TestRecordsJoinIndexProto.MySimpleRecord simpleRecord = randomSimpleRecord(r);
            records.add(simpleRecord);
            records.add(randomJoinedOther(r, simpleRecord));
        }
        singleCountIndexRebuild(records, null, null);
    }

    @ParameterizedTest
    @MethodSource("randomSeeds")
    void simpleJoinCountIsEmpty(long seed) {
        final Random r = new Random(seed);
        final List<Message> records = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            TestRecordsJoinIndexProto.MySimpleRecord simpleRecord = randomSimpleRecord(r);
            records.add(simpleRecord);
            records.add(randomOtherRecord(r, simpleRecord.getNumValue(), r.nextLong()));
        }
        singleCountIndexRebuild(records, null, null);
        try (FDBRecordContext context = openContext()) {
            // Validate that the index is empty
            try (RecordCursor<IndexEntry> cursor = recordStore.scanIndex(metaData.getIndex("joinIndex"), IndexScanType.BY_GROUP, TupleRange.ALL, null, ScanProperties.FORWARD_SCAN)) {
                assertThat(cursor.asList().join(), Matchers.empty());
            }
            context.commit();
        }
    }

    @Tag(Tags.Slow)
    @Disabled("Updates to non-idempotent index during joined index build not correctly handled")
    @ParameterizedTest
    @MethodSource("randomSeeds")
    void simpleCountWithUpdatesAndDeletes(long seed) {
        final Random r = new Random(seed);
        final List<Message> records = new ArrayList<>();
        for (int i = 0; i < 400; i ++) {
            TestRecordsJoinIndexProto.MySimpleRecord simple = randomSimpleRecord(r);
            records.add(simple);
            TestRecordsJoinIndexProto.MyOtherRecord other = randomJoinedOther(r, simple);
            records.add(other);
        }
        final List<Message> recordsWhileBuilding = new ArrayList<>();
        final List<Tuple> deleteWhileBuilding = new ArrayList<>();
        for (Message rec : records) {
            addRandomUpdate(r, rec, recordsWhileBuilding, deleteWhileBuilding);
        }
        singleCountIndexRebuild(records, recordsWhileBuilding, deleteWhileBuilding);
    }
}
