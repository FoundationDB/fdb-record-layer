/*
 * SyntheticRecordPlannerJoinsTest.java
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

package com.apple.foundationdb.record.query.plan.synthetic;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.TestRecordsJoinIndexProto;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexOptions;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.JoinedRecordTypeBuilder;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoredRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBSyntheticRecord;
import com.apple.foundationdb.record.query.plan.match.PlanMatchers;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.Tags;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.ImmutableMultiset;
import com.google.common.collect.Multiset;
import com.google.protobuf.Message;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;

import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests for {@link SyntheticRecordPlanner} regarding simpler join capabilities.
 */
@Tag(Tags.RequiresFDB)
@API(API.Status.EXPERIMENTAL)
public class SyntheticRecordPlannerJoinsTest extends AbstractSyntheticRecordPlannerTest {
    @Test
    void oneToOne() {
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
            assertThat(plan1, SyntheticPlanMatchers.syntheticRecordScan(
                    PlanMatchers.indexScan("MySimpleRecord$other_rec_no"),
                    SyntheticPlanMatchers.joinedRecord(List.of(
                            PlanMatchers.typeFilter(Matchers.contains("MyOtherRecord"),
                                    PlanMatchers.scan(PlanMatchers.bounds(PlanMatchers.hasTupleString("[EQUALS $_j1]"))))))));
            Multiset<Tuple> expected1 = ImmutableMultiset.of(
                    Tuple.from(-1, Tuple.from(0), Tuple.from(1000)),
                    Tuple.from(-1, Tuple.from(1), Tuple.from(1001)),
                    Tuple.from(-1, Tuple.from(2), Tuple.from(1002)));
            Multiset<Tuple> results1 = HashMultiset.create(plan1.execute(recordStore).map(FDBSyntheticRecord::getPrimaryKey).asList().join());
            assertEquals(expected1, results1);
        }
    }

    @Test
    void manyToOne() {
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
            assertThat(plan1, SyntheticPlanMatchers.syntheticRecordScan(
                    PlanMatchers.indexScan("MySimpleRecord$other_rec_no"),
                    SyntheticPlanMatchers.joinedRecord(List.of(
                            PlanMatchers.typeFilter(Matchers.contains("MyOtherRecord"),
                                    PlanMatchers.scan(PlanMatchers.bounds(PlanMatchers.hasTupleString("[EQUALS $_j1]"))))))));
            Multiset<Tuple> expected1 = ImmutableMultiset.of(
                    Tuple.from(-1, Tuple.from(100), Tuple.from(1001)),
                    Tuple.from(-1, Tuple.from(200), Tuple.from(1002)),
                    Tuple.from(-1, Tuple.from(201), Tuple.from(1002)));
            Multiset<Tuple> results1 = HashMultiset.create(plan1.execute(recordStore).map(FDBSyntheticRecord::getPrimaryKey).asList().join());
            assertEquals(expected1, results1);

            FDBStoredRecord<Message> record = recordStore.loadRecord(Tuple.from(1002));
            SyntheticRecordFromStoredRecordPlan plan2 = planner.fromStoredType(record.getRecordType(), false);
            assertThat(plan2, SyntheticPlanMatchers.joinedRecord(List.of(
                    PlanMatchers.indexScan(Matchers.allOf(
                            PlanMatchers.indexName("MySimpleRecord$other_rec_no"),
                            PlanMatchers.bounds(PlanMatchers.hasTupleString("[EQUALS $_j1]")))))));
            Multiset<Tuple> expected2 = ImmutableMultiset.of(
                    Tuple.from(-1, Tuple.from(200), Tuple.from(1002)),
                    Tuple.from(-1, Tuple.from(201), Tuple.from(1002)));
            Multiset<Tuple> results2 = HashMultiset.create(plan2.execute(recordStore, record).map(FDBSyntheticRecord::getPrimaryKey).asList().join());
            assertEquals(expected2, results2);
        }
    }

    @Test
    void manyToMany() {
        final JoinedRecordTypeBuilder joined = metaDataBuilder.addJoinedRecordType("ManyToMany");
        joined.addConstituent("simple", "MySimpleRecord");
        joined.addConstituent("other", "MyOtherRecord");
        joined.addConstituent("joining", "JoiningRecord");
        joined.addJoin("joining", "simple_rec_no", "simple", "rec_no");
        joined.addJoin("joining", "other_rec_no", "other", "rec_no");

        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore recordStore = recordStoreBuilder.setContext(context).create();

            timer.reset();
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

            // Because there are no indexes defined on the joined index, none of these updates should actually result in planning
            assertEquals(0L, timer.getCount(FDBStoreTimer.Counts.PLAN_SYNTHETIC_TYPE));

            context.commit();
        }

        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore recordStore = recordStoreBuilder.setContext(context).open();
            final SyntheticRecordPlanner planner = new SyntheticRecordPlanner(recordStore);

            SyntheticRecordPlan plan1 = planner.scanForType(recordStore.getRecordMetaData().getSyntheticRecordType("ManyToMany"));
            assertThat(plan1, SyntheticPlanMatchers.syntheticRecordScan(
                    PlanMatchers.typeFilter(Matchers.contains("MySimpleRecord"), PlanMatchers.scan()),
                    SyntheticPlanMatchers.joinedRecord(List.of(
                            PlanMatchers.indexScan(Matchers.allOf(
                                    PlanMatchers.indexName("JoiningRecord$simple_rec_no"),
                                    PlanMatchers.bounds(PlanMatchers.hasTupleString("[EQUALS $_j1]")))),
                            PlanMatchers.typeFilter(Matchers.contains("MyOtherRecord"),
                                    PlanMatchers.scan(PlanMatchers.bounds(PlanMatchers.hasTupleString("[EQUALS $_j2]"))))))));
            Multiset<Tuple> expected1 = ImmutableMultiset.of(
                    Tuple.from(-1, Tuple.from(1), Tuple.from(1000), Tuple.from(100)),
                    Tuple.from(-1, Tuple.from(2), Tuple.from(1000), Tuple.from(101)),
                    Tuple.from(-1, Tuple.from(2), Tuple.from(1002), Tuple.from(102)));
            Multiset<Tuple> results1 = HashMultiset.create(plan1.execute(recordStore).map(FDBSyntheticRecord::getPrimaryKey).asList().join());
            assertEquals(expected1, results1);
        }
    }

    @Test
    void selfJoin() {
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
            assertThat(plan1, SyntheticPlanMatchers.syntheticRecordScan(
                    PlanMatchers.indexScan("MySimpleRecord$other_rec_no"),
                    SyntheticPlanMatchers.joinedRecord(List.of(
                            PlanMatchers.typeFilter(Matchers.contains("MySimpleRecord"),
                                    PlanMatchers.scan(PlanMatchers.bounds(PlanMatchers.hasTupleString("[EQUALS $_j1]"))))))));
            Multiset<Tuple> expected1 = ImmutableMultiset.of(
                    Tuple.from(-1, Tuple.from(0), Tuple.from(1)),
                    Tuple.from(-1, Tuple.from(1), Tuple.from(2)));
            Multiset<Tuple> results1 = HashMultiset.create(plan1.execute(recordStore).map(FDBSyntheticRecord::getPrimaryKey).asList().join());
            assertEquals(expected1, results1);

            FDBStoredRecord<Message> record = recordStore.loadRecord(Tuple.from(1));
            SyntheticRecordFromStoredRecordPlan plan2 = planner.fromStoredType(record.getRecordType(), false);
            assertThat(plan2, SyntheticPlanMatchers.syntheticRecordConcat(List.of(
                    SyntheticPlanMatchers.joinedRecord(List.of(
                            PlanMatchers.typeFilter(Matchers.contains("MySimpleRecord"),
                                    PlanMatchers.scan(PlanMatchers.bounds(PlanMatchers.hasTupleString("[EQUALS $_j1]")))))),
                    SyntheticPlanMatchers.joinedRecord(List.of(
                            PlanMatchers.indexScan(Matchers.allOf(
                                    PlanMatchers.indexName("MySimpleRecord$other_rec_no"),
                                    PlanMatchers.bounds(PlanMatchers.hasTupleString("[EQUALS $_j1]")))))))));
            Multiset<Tuple> expected2 = ImmutableMultiset.of(
                    Tuple.from(-1, Tuple.from(0), Tuple.from(1)),
                    Tuple.from(-1, Tuple.from(1), Tuple.from(2)));
            Multiset<Tuple> results2 = HashMultiset.create(plan2.execute(recordStore, record).map(FDBSyntheticRecord::getPrimaryKey).asList().join());
            assertEquals(expected2, results2);
        }
    }
}
