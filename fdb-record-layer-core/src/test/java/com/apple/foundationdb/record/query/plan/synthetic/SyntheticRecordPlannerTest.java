/*
 * SyntheticRecordPlannerTest.java
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

package com.apple.foundationdb.record.query.plan.synthetic;

import com.apple.foundationdb.API;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.TestRecordsJoinIndexProto;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexOptions;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.JoinedRecordTypeBuilder;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabase;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabaseFactory;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoredRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBSyntheticRecord;
import com.apple.foundationdb.record.provider.foundationdb.TestKeySpace;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpacePath;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.Tags;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.ImmutableMultiset;
import com.google.common.collect.Multiset;
import com.google.protobuf.Message;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests for {@link SyntheticRecordPlanner}.
 *
 */
@Tag(Tags.RequiresFDB)
@API(API.Status.EXPERIMENTAL)
public class SyntheticRecordPlannerTest {

    private static final Object[] PATH_OBJECTS = new Object[] {"record-test", "unit", "indexTest"};

    private FDBDatabase fdb;
    private FDBStoreTimer timer;
    private KeySpacePath path;
    private RecordMetaDataBuilder metaDataBuilder;
    private FDBRecordStore.Builder recordStoreBuilder;

    private FDBRecordContext openContext() {
        FDBRecordContext context = fdb.openContext();
        context.setTimer(timer);
        return context;
    }

    @BeforeEach
    public void initBuilders() throws Exception {
        metaDataBuilder = RecordMetaData.newBuilder()
                .setRecords(TestRecordsJoinIndexProto.getDescriptor());
        recordStoreBuilder = FDBRecordStore.newBuilder()
                .setMetaDataProvider(metaDataBuilder);

        fdb = FDBDatabaseFactory.instance().getDatabase();
        timer = new FDBStoreTimer();

        fdb.run(timer, null, context -> {
            path = TestKeySpace.getKeyspacePath(PATH_OBJECTS);
            FDBRecordStore.deleteStore(context, path);
            recordStoreBuilder.setContext(context).setKeySpacePath(path);
            return null;
        });
    }

    @Test
    public void oneToOne() throws Exception {
        metaDataBuilder.addIndex("MySimpleRecord", new Index("MySimpleRecord$other_rec_no", field("other_rec_no"), IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS));
        final JoinedRecordTypeBuilder joined = metaDataBuilder.addJoinedRecordType("OneToOne");
        joined.addConstituent("simple", "MySimpleRecord");
        joined.addConstituent("other", "MyOtherRecord");
        joined.addJoin("simple", "other_rec_no", "other", "rec_no");

        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore recordStore = recordStoreBuilder.setContext(context).create();

            for (int i = 0; i < 3; i++) {
                TestRecordsJoinIndexProto.MySimpleRecord.Builder simple = TestRecordsJoinIndexProto.MySimpleRecord.newBuilder();
                simple.setRecNo(i).setOtherRecNo(1000 + i);
                recordStore.saveRecord(simple.build());
                TestRecordsJoinIndexProto.MyOtherRecord.Builder other = TestRecordsJoinIndexProto.MyOtherRecord.newBuilder();
                other.setRecNo(1000 + i);
                recordStore.saveRecord(other.build());
            }

            context.commit();
        }

        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore recordStore = recordStoreBuilder.setContext(context).open();
            final SyntheticRecordPlanner planner = new SyntheticRecordPlanner(recordStore);

            SyntheticRecordPlan plan1 = planner.scanForType(recordStore.getRecordMetaData().getSyntheticRecordType("OneToOne"));
            Multiset<Tuple> expected1 = ImmutableMultiset.of(Tuple.from(-1, 0, 1000), Tuple.from(-1, 1, 1001), Tuple.from(-1, 2, 1002));
            Multiset<Tuple> results1 = HashMultiset.create(plan1.execute(recordStore).map(FDBSyntheticRecord::getPrimaryKey).asList().join());
            assertEquals(expected1, results1);
        }
    }

    @Test
    public void manyToOne() throws Exception {
        metaDataBuilder.addIndex("MySimpleRecord", "other_rec_no");
        final JoinedRecordTypeBuilder joined = metaDataBuilder.addJoinedRecordType("ManyToOne");
        joined.addConstituent("simple", "MySimpleRecord");
        joined.addConstituent("other", "MyOtherRecord");
        joined.addJoin("simple", "other_rec_no", "other", "rec_no");

        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore recordStore = recordStoreBuilder.setContext(context).create();

            for (int i = 0; i < 3; i++) {
                for (int j = 0; j < i; j++) {
                    TestRecordsJoinIndexProto.MySimpleRecord.Builder simple = TestRecordsJoinIndexProto.MySimpleRecord.newBuilder();
                    simple.setRecNo(100 * i + j).setOtherRecNo(1000 + i);
                    recordStore.saveRecord(simple.build());
                }
                TestRecordsJoinIndexProto.MyOtherRecord.Builder other = TestRecordsJoinIndexProto.MyOtherRecord.newBuilder();
                other.setRecNo(1000 + i);
                recordStore.saveRecord(other.build());
            }

            context.commit();
        }

        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore recordStore = recordStoreBuilder.setContext(context).open();
            final SyntheticRecordPlanner planner = new SyntheticRecordPlanner(recordStore);

            SyntheticRecordPlan plan1 = planner.scanForType(recordStore.getRecordMetaData().getSyntheticRecordType("ManyToOne"));
            Multiset<Tuple> expected1 = ImmutableMultiset.of(Tuple.from(-1, 100, 1001), Tuple.from(-1, 200, 1002), Tuple.from(-1, 201, 1002));
            Multiset<Tuple> results1 = HashMultiset.create(plan1.execute(recordStore).map(FDBSyntheticRecord::getPrimaryKey).asList().join());
            assertEquals(expected1, results1);

            FDBStoredRecord<Message> record = recordStore.loadRecord(Tuple.from(1002));
            SyntheticRecordFromStoredRecordPlan plan2 = planner.fromStoredType(record.getRecordType(), false);
            Multiset<Tuple> expected2 = ImmutableMultiset.of(Tuple.from(-1, 200, 1002), Tuple.from(-1, 201, 1002));
            Multiset<Tuple> results2 = HashMultiset.create(plan2.execute(recordStore, record).map(FDBSyntheticRecord::getPrimaryKey).asList().join());
            assertEquals(expected2, results2);
        }
    }

    @Test
    public void manyToMany() throws Exception {
        final JoinedRecordTypeBuilder joined = metaDataBuilder.addJoinedRecordType("ManyToMany");
        joined.addConstituent("simple", "MySimpleRecord");
        joined.addConstituent("other", "MyOtherRecord");
        joined.addConstituent("joining", "JoiningRecord");
        joined.addJoin("joining", "simple_rec_no", "simple", "rec_no");
        joined.addJoin("joining", "other_rec_no", "other", "rec_no");

        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore recordStore = recordStoreBuilder.setContext(context).create();

            for (int i = 0; i < 3; i++) {
                TestRecordsJoinIndexProto.MySimpleRecord.Builder simple = TestRecordsJoinIndexProto.MySimpleRecord.newBuilder();
                simple.setRecNo(i);
                recordStore.saveRecord(simple.build());
                TestRecordsJoinIndexProto.MyOtherRecord.Builder other = TestRecordsJoinIndexProto.MyOtherRecord.newBuilder();
                other.setRecNo(1000 + i);
                recordStore.saveRecord(other.build());
            }
            TestRecordsJoinIndexProto.JoiningRecord.Builder joining = TestRecordsJoinIndexProto.JoiningRecord.newBuilder();
            joining.setRecNo(100).setSimpleRecNo(1).setOtherRecNo(1000);
            recordStore.saveRecord(joining.build());
            joining.setRecNo(101).setSimpleRecNo(2).setOtherRecNo(1000);
            recordStore.saveRecord(joining.build());
            joining.setRecNo(102).setSimpleRecNo(2).setOtherRecNo(1002);
            recordStore.saveRecord(joining.build());

            context.commit();
        }

        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore recordStore = recordStoreBuilder.setContext(context).open();
            final SyntheticRecordPlanner planner = new SyntheticRecordPlanner(recordStore);

            SyntheticRecordPlan plan1 = planner.scanForType(recordStore.getRecordMetaData().getSyntheticRecordType("ManyToMany"));
            Multiset<Tuple> expected1 = ImmutableMultiset.of(Tuple.from(-1, 1, 1000, 100), Tuple.from(-1, 2, 1000, 101), Tuple.from(-1, 2, 1002, 102));
            Multiset<Tuple> results1 = HashMultiset.create(plan1.execute(recordStore).map(FDBSyntheticRecord::getPrimaryKey).asList().join());
            assertEquals(expected1, results1);
        }
    }

    @Test
    public void selfJoin() throws Exception {
        metaDataBuilder.addIndex("MySimpleRecord", "other_rec_no");
        final JoinedRecordTypeBuilder joined = metaDataBuilder.addJoinedRecordType("SelfJoin");
        joined.addConstituent("simple1", "MySimpleRecord");
        joined.addConstituent("simple2", "MySimpleRecord");
        joined.addJoin("simple1", "other_rec_no", "simple2", "rec_no");

        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore recordStore = recordStoreBuilder.setContext(context).create();

            for (int i = 0; i < 3; i++) {
                TestRecordsJoinIndexProto.MySimpleRecord.Builder simple = TestRecordsJoinIndexProto.MySimpleRecord.newBuilder();
                simple.setRecNo(i).setOtherRecNo(i + 1);
                recordStore.saveRecord(simple.build());
            }

            context.commit();
        }

        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore recordStore = recordStoreBuilder.setContext(context).open();
            final SyntheticRecordPlanner planner = new SyntheticRecordPlanner(recordStore);

            SyntheticRecordPlan plan1 = planner.scanForType(recordStore.getRecordMetaData().getSyntheticRecordType("SelfJoin"));
            Multiset<Tuple> expected1 = ImmutableMultiset.of(Tuple.from(-1, 0, 1), Tuple.from(-1, 1, 2));
            Multiset<Tuple> results1 = HashMultiset.create(plan1.execute(recordStore).map(FDBSyntheticRecord::getPrimaryKey).asList().join());
            assertEquals(expected1, results1);

            FDBStoredRecord<Message> record = recordStore.loadRecord(Tuple.from(1));
            SyntheticRecordFromStoredRecordPlan plan2 = planner.fromStoredType(record.getRecordType(), false);
            Multiset<Tuple> expected2 = ImmutableMultiset.of(Tuple.from(-1, 0, 1), Tuple.from(-1, 1, 2));
            Multiset<Tuple> results2 = HashMultiset.create(plan2.execute(recordStore, record).map(FDBSyntheticRecord::getPrimaryKey).asList().join());
            assertEquals(expected2, results2);
        }
    }

    @Test
    public void outerJoins() throws Exception {
        metaDataBuilder.addIndex("MySimpleRecord", "other_rec_no");
        final JoinedRecordTypeBuilder innerJoined = metaDataBuilder.addJoinedRecordType("InnerJoined");
        innerJoined.addConstituent("simple", "MySimpleRecord");
        innerJoined.addConstituent("other", "MyOtherRecord");
        innerJoined.addJoin("simple", "other_rec_no", "other", "rec_no");
        final JoinedRecordTypeBuilder leftJoined = metaDataBuilder.addJoinedRecordType("LeftJoined");
        leftJoined.addConstituent("simple", "MySimpleRecord");
        leftJoined.addConstituent("other", metaDataBuilder.getRecordType("MyOtherRecord"), true);
        leftJoined.addJoin("simple", "other_rec_no", "other", "rec_no");
        final JoinedRecordTypeBuilder fullOuterJoined = metaDataBuilder.addJoinedRecordType("FullOuterJoined");
        fullOuterJoined.addConstituent("simple", metaDataBuilder.getRecordType("MySimpleRecord"), true);
        fullOuterJoined.addConstituent("other", metaDataBuilder.getRecordType("MyOtherRecord"), true);
        fullOuterJoined.addJoin("simple", "other_rec_no", "other", "rec_no");

        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore recordStore = recordStoreBuilder.setContext(context).create();

            for (int i = 0; i < 3; i++) {
                TestRecordsJoinIndexProto.MySimpleRecord.Builder simple = TestRecordsJoinIndexProto.MySimpleRecord.newBuilder();
                simple.setRecNo(i).setOtherRecNo(1001 + i);
                recordStore.saveRecord(simple.build());
                TestRecordsJoinIndexProto.MyOtherRecord.Builder other = TestRecordsJoinIndexProto.MyOtherRecord.newBuilder();
                other.setRecNo(1000 + i);
                recordStore.saveRecord(other.build());
            }

            context.commit();
        }

        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore recordStore = recordStoreBuilder.setContext(context).open();
            final SyntheticRecordPlanner planner = new SyntheticRecordPlanner(recordStore);

            SyntheticRecordPlan plan1 = planner.scanForType(recordStore.getRecordMetaData().getSyntheticRecordType("InnerJoined"));
            Multiset<Tuple> expected1 = ImmutableMultiset.of(Tuple.from(-1, 0, 1001), Tuple.from(-1, 1, 1002));
            Multiset<Tuple> results1 = HashMultiset.create(plan1.execute(recordStore).map(FDBSyntheticRecord::getPrimaryKey).asList().join());
            assertEquals(expected1, results1);

            SyntheticRecordPlan plan2 = planner.scanForType(recordStore.getRecordMetaData().getSyntheticRecordType("LeftJoined"));
            Multiset<Tuple> expected2 = ImmutableMultiset.of(Tuple.from(-2, 0, 1001), Tuple.from(-2, 1, 1002), Tuple.from(-2, 2, null));
            Multiset<Tuple> results2 = HashMultiset.create(plan2.execute(recordStore).map(FDBSyntheticRecord::getPrimaryKey).asList().join());
            assertEquals(expected2, results2);

            SyntheticRecordPlan plan3 = planner.scanForType(recordStore.getRecordMetaData().getSyntheticRecordType("FullOuterJoined"));
            Multiset<Tuple> expected3 = ImmutableMultiset.of(Tuple.from(-3, null, 1000), Tuple.from(-3, 0, 1001), Tuple.from(-3, 1, 1002), Tuple.from(-3, 2, null));
            Multiset<Tuple> results3 = HashMultiset.create(plan3.execute(recordStore).map(FDBSyntheticRecord::getPrimaryKey).asList().join());
            assertEquals(expected3, results3);

            FDBStoredRecord<Message> record = recordStore.loadRecord(Tuple.from(2));
            SyntheticRecordFromStoredRecordPlan plan4 = planner.fromStoredType(record.getRecordType(), false);
            Multiset<Tuple> expected4 = ImmutableMultiset.of(Tuple.from(-2, 2, null), Tuple.from(-3, 2, null));
            Multiset<Tuple> results4 = HashMultiset.create(plan4.execute(recordStore, record).map(FDBSyntheticRecord::getPrimaryKey).asList().join());
            assertEquals(expected4, results4);
        }
    }

    @Test
    public void clique() throws Exception {
        final JoinedRecordTypeBuilder clique = metaDataBuilder.addJoinedRecordType("Clique");
        clique.addConstituent("type_a", "TypeA");
        clique.addConstituent("type_b", "TypeB");
        clique.addConstituent("type_c", "TypeC");
        clique.addJoin("type_a", "type_b_rec_no", "type_b", "rec_no");
        clique.addJoin("type_b", "type_c_rec_no", "type_c", "rec_no");
        clique.addJoin("type_c", "type_a_rec_no", "type_a", "rec_no");

        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore recordStore = recordStoreBuilder.setContext(context).create();

            for (int i = 0; i < 3; i++) {
                TestRecordsJoinIndexProto.TypeA.Builder typeA = TestRecordsJoinIndexProto.TypeA.newBuilder();
                typeA.setRecNo(100 + i).setTypeBRecNo(200 + i);
                recordStore.saveRecord(typeA.build());
                TestRecordsJoinIndexProto.TypeB.Builder typeB = TestRecordsJoinIndexProto.TypeB.newBuilder();
                typeB.setRecNo(200 + i).setTypeCRecNo(300 + i);
                recordStore.saveRecord(typeB.build());
                TestRecordsJoinIndexProto.TypeC.Builder typeC = TestRecordsJoinIndexProto.TypeC.newBuilder();
                typeC.setRecNo(300 + i).setTypeARecNo(100 + i);
                recordStore.saveRecord(typeC.build());
            }

            context.commit();
        }

        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore recordStore = recordStoreBuilder.setContext(context).open();
            final SyntheticRecordPlanner planner = new SyntheticRecordPlanner(recordStore);

            SyntheticRecordPlan plan1 = planner.scanForType(recordStore.getRecordMetaData().getSyntheticRecordType("Clique"));
            Multiset<Tuple> expected1 = ImmutableMultiset.of(Tuple.from(-1, 100, 200, 300), Tuple.from(-1, 101, 201, 301), Tuple.from(-1, 102, 202, 302));
            Multiset<Tuple> results1 = HashMultiset.create(plan1.execute(recordStore).map(FDBSyntheticRecord::getPrimaryKey).asList().join());
            assertEquals(expected1, results1);

            // Make sure that the extra join condition is checked.
            TestRecordsJoinIndexProto.TypeC.Builder typeC = TestRecordsJoinIndexProto.TypeC.newBuilder();
            typeC.setRecNo(301).setTypeARecNo(999);
            recordStore.saveRecord(typeC.build());
            
            SyntheticRecordPlan plan2 = planner.scanForType(recordStore.getRecordMetaData().getSyntheticRecordType("Clique"));
            Multiset<Tuple> expected2 = ImmutableMultiset.of(Tuple.from(-1, 100, 200, 300), Tuple.from(-1, 102, 202, 302));
            Multiset<Tuple> results2 = HashMultiset.create(plan2.execute(recordStore).map(FDBSyntheticRecord::getPrimaryKey).asList().join());
            assertEquals(expected2, results2);
        }
    }

    @Test
    public void nestedRepeated() throws Exception {
        final KeyExpression key = field("repeated", KeyExpression.FanType.FanOut).nest("nums", KeyExpression.FanType.FanOut);
        metaDataBuilder.addIndex("NestedA", "repeatedA", key);
        metaDataBuilder.addIndex("NestedB", "repeatedB", key);
        final JoinedRecordTypeBuilder nested = metaDataBuilder.addJoinedRecordType("NestedRepeated");
        nested.addConstituent("nested_a", "NestedA");
        nested.addConstituent("nested_b", "NestedB");
        nested.addJoin("nested_a", key, "nested_b", key);

        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore recordStore = recordStoreBuilder.setContext(context).create();

            TestRecordsJoinIndexProto.NestedA.Builder nestedA = TestRecordsJoinIndexProto.NestedA.newBuilder();
            nestedA.setRecNo(101);
            nestedA.addRepeatedBuilder().addNums(1).addNums(2);
            nestedA.addRepeatedBuilder().addNums(3).addNums(4);
            recordStore.saveRecord(nestedA.build());
            nestedA.setRecNo(102);
            nestedA.clearRepeated();
            nestedA.addRepeatedBuilder().addNums(2);
            recordStore.saveRecord(nestedA.build());

            TestRecordsJoinIndexProto.NestedB.Builder nestedB = TestRecordsJoinIndexProto.NestedB.newBuilder();
            nestedB.setRecNo(201);
            nestedB.addRepeatedBuilder().addNums(2).addNums(4);
            recordStore.saveRecord(nestedB.build());
            nestedB.setRecNo(202);
            nestedB.clearRepeated();
            nestedB.addRepeatedBuilder().addNums(1).addNums(3);
            nestedB.addRepeatedBuilder().addNums(2);
            recordStore.saveRecord(nestedB.build());
            nestedB.setRecNo(203);
            nestedB.clearRepeated();
            recordStore.saveRecord(nestedB.build());

            context.commit();
        }

        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore recordStore = recordStoreBuilder.setContext(context).open();
            final SyntheticRecordPlanner planner = new SyntheticRecordPlanner(recordStore);

            SyntheticRecordPlan plan1 = planner.scanForType(recordStore.getRecordMetaData().getSyntheticRecordType("NestedRepeated"));
            Multiset<Tuple> expected1 = ImmutableMultiset.of(Tuple.from(-1, 101, 201), Tuple.from(-1, 101, 202), Tuple.from(-1, 102, 201), Tuple.from(-1, 102, 202));
            Multiset<Tuple> results1 = HashMultiset.create(plan1.execute(recordStore).map(FDBSyntheticRecord::getPrimaryKey).asList().join());
            assertEquals(expected1, results1);

            FDBStoredRecord<Message> record = recordStore.loadRecord(Tuple.from(101));
            SyntheticRecordFromStoredRecordPlan plan2 = planner.fromStoredType(record.getRecordType(), false);
            // TODO: IN can generate duplicates from repeated field (https://github.com/FoundationDB/fdb-record-layer/issues/98)
            Multiset<Tuple> expected2 = ImmutableMultiset.of(Tuple.from(-1, 101, 201), Tuple.from(-1, 101, 201), Tuple.from(-1, 101, 202), Tuple.from(-1, 101, 202), Tuple.from(-1, 101, 202));
            Multiset<Tuple> results2 = HashMultiset.create(plan2.execute(recordStore, record).map(FDBSyntheticRecord::getPrimaryKey).asList().join());
            assertEquals(expected2, results2);
        }
    }

}
