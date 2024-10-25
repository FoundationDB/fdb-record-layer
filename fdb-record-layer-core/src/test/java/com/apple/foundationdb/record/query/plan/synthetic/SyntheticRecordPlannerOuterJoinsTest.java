/*
 * SyntheticRecordPlannerOuterJoinsTest.java
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
import com.apple.foundationdb.record.metadata.JoinedRecordTypeBuilder;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoredRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBSyntheticRecord;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.record.query.plan.QueryPlanner;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers;
import com.apple.foundationdb.record.query.plan.match.PlanMatchers;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.Tags;
import com.google.common.base.Verify;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultiset;
import com.google.common.collect.Multiset;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static com.apple.foundationdb.record.query.plan.ScanComparisons.range;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.coveringIndexPlan;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.indexPlan;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.indexPlanOf;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.scanComparisons;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests for {@link SyntheticRecordPlanner} regarding outer join capabilities.
 */
@Tag(Tags.RequiresFDB)
@API(API.Status.EXPERIMENTAL)
public class SyntheticRecordPlannerOuterJoinsTest extends AbstractSyntheticRecordPlannerTest {
    @Test
    void outerJoins() {
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
            assertThat(plan1, SyntheticPlanMatchers.syntheticRecordScan(
                    PlanMatchers.indexScan("MySimpleRecord$other_rec_no"),
                    SyntheticPlanMatchers.joinedRecord(List.of(
                            PlanMatchers.typeFilter(Matchers.contains("MyOtherRecord"),
                                    PlanMatchers.scan(PlanMatchers.bounds(PlanMatchers.hasTupleString("[EQUALS $_j1]"))))))));
            Multiset<Tuple> expected1 = ImmutableMultiset.of(
                    Tuple.from(-1, Tuple.from(0), Tuple.from(1001)),
                    Tuple.from(-1, Tuple.from(1), Tuple.from(1002)));
            Multiset<Tuple> results1 = HashMultiset.create(plan1.execute(recordStore).map(FDBSyntheticRecord::getPrimaryKey).asList().join());
            assertEquals(expected1, results1);

            SyntheticRecordPlan plan2 = planner.scanForType(recordStore.getRecordMetaData().getSyntheticRecordType("LeftJoined"));
            assertThat(plan2, SyntheticPlanMatchers.syntheticRecordScan(
                    PlanMatchers.indexScan("MySimpleRecord$other_rec_no"),
                    SyntheticPlanMatchers.joinedRecord(List.of(
                            PlanMatchers.typeFilter(Matchers.contains("MyOtherRecord"),
                                    PlanMatchers.scan(PlanMatchers.bounds(PlanMatchers.hasTupleString("[EQUALS $_j1]"))))))));
            Multiset<Tuple> expected2 = ImmutableMultiset.of(
                    Tuple.from(-2, Tuple.from(0), Tuple.from(1001)),
                    Tuple.from(-2, Tuple.from(1), Tuple.from(1002)),
                    Tuple.from(-2, Tuple.from(2), null));
            Multiset<Tuple> results2 = HashMultiset.create(plan2.execute(recordStore).map(FDBSyntheticRecord::getPrimaryKey).asList().join());
            assertEquals(expected2, results2);

            SyntheticRecordPlan plan3 = planner.scanForType(recordStore.getRecordMetaData().getSyntheticRecordType("FullOuterJoined"));
            assertThat(plan3, SyntheticPlanMatchers.syntheticRecordScan(
                    PlanMatchers.typeFilter(Matchers.containsInAnyOrder("MySimpleRecord", "MyOtherRecord"), PlanMatchers.scan()),
                    SyntheticPlanMatchers.syntheticRecordByType(Map.of(
                            "MySimpleRecord", SyntheticPlanMatchers.joinedRecord(List.of(
                                    PlanMatchers.typeFilter(Matchers.contains("MyOtherRecord"),
                                            PlanMatchers.scan(PlanMatchers.bounds(PlanMatchers.hasTupleString("[EQUALS $_j1]")))))),
                            "MyOtherRecord", SyntheticPlanMatchers.joinedRecord(List.of(
                                    PlanMatchers.indexScan(Matchers.allOf(
                                            PlanMatchers.indexName("MySimpleRecord$other_rec_no"),
                                            PlanMatchers.bounds(PlanMatchers.hasTupleString("[EQUALS $_j1]"))))))))));
            Multiset<Tuple> expected3 = ImmutableMultiset.of(
                    Tuple.from(-3, null, Tuple.from(1000)),
                    Tuple.from(-3, Tuple.from(0), Tuple.from(1001)),
                    Tuple.from(-3, Tuple.from(1), Tuple.from(1002)),
                    Tuple.from(-3, Tuple.from(2), null));
            Multiset<Tuple> results3 = HashMultiset.create(plan3.execute(recordStore).map(FDBSyntheticRecord::getPrimaryKey).asList().join());
            assertEquals(expected3, results3);

            FDBStoredRecord<Message> record = recordStore.loadRecord(Tuple.from(2));
            SyntheticRecordFromStoredRecordPlan plan4 = planner.fromStoredType(record.getRecordType(), false);
            assertThat(plan4, SyntheticPlanMatchers.syntheticRecordConcat(Collections.nCopies(3,
                    SyntheticPlanMatchers.joinedRecord(List.of(
                            PlanMatchers.typeFilter(Matchers.contains("MyOtherRecord"),
                                    PlanMatchers.scan(PlanMatchers.bounds(PlanMatchers.hasTupleString("[EQUALS $_j1]")))))))));
            Multiset<Tuple> expected4 = ImmutableMultiset.of(
                    Tuple.from(-2, Tuple.from(2), null),
                    Tuple.from(-3, Tuple.from(2), null));
            Multiset<Tuple> results4 = HashMultiset.create(plan4.execute(recordStore, record).map(FDBSyntheticRecord::getPrimaryKey).asList().join());
            assertEquals(expected4, results4);
        }
    }

    @Test
    void indexScansOverOuterJoins() {
        metaDataBuilder.addIndex("MySimpleRecord", "other_rec_no");
        final JoinedRecordTypeBuilder leftJoined = metaDataBuilder.addJoinedRecordType("LeftJoined");
        leftJoined.addConstituent("simple", "MySimpleRecord");
        leftJoined.addConstituent("other", metaDataBuilder.getRecordType("MyOtherRecord"), true);
        leftJoined.addJoin("simple", "other_rec_no", "other", "rec_no");
        metaDataBuilder.addIndex(leftJoined, new Index("simple.str_value_other", field("simple").nest("str_value")));

        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore recordStore = recordStoreBuilder.setContext(context).create();

            for (int i = 0; i < 3; i++) {
                TestRecordsJoinIndexProto.MySimpleRecord.Builder simple = TestRecordsJoinIndexProto.MySimpleRecord.newBuilder();
                simple.setStrValue(i % 2 == 0 ? "even" : "odd");
                simple.setRecNo(i).setOtherRecNo(1001 + i);
                recordStore.saveRecord(simple.build());
                TestRecordsJoinIndexProto.MyOtherRecord.Builder other = TestRecordsJoinIndexProto.MyOtherRecord.newBuilder();
                other.setRecNo(1000 + i);
                other.setNumValue3(i);
                recordStore.saveRecord(other.build());
            }
            context.commit();
        }

        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore recordStore = recordStoreBuilder.setContext(context).open();

            final QueryPlanner planner = setupPlanner(recordStore, null);

            RecordQuery query = RecordQuery.newBuilder()
                    .setRecordType("LeftJoined")
                    .setFilter(Query.field("simple").matches(Query.field("str_value").equalsValue("even")))
                    .setRequiredResults(ImmutableList.of(field("simple").nest("str_value"), field("other").nest("num_value_3")))
                    .build();

            RecordQueryPlan plan = planner.plan(query);
            Assertions.assertTrue(
                    indexPlan().where(RecordQueryPlanMatchers.indexName("simple.str_value_other"))
                            .and(scanComparisons(range("[[even],[even]]"))).matches(plan));
            var join = recordStore.executeQuery(plan).asList().join();

            //
            // TODO Note that due to https://github.com/FoundationDB/fdb-record-layer/issues/1883, the index incorrectly
            //      returns items that should not be in the index anymore.
            //
            Assertions.assertEquals(3, join.size());
            var simpleRecord = Verify.verifyNotNull(join.get(0).getConstituent("simple")).getRecord();
            Assertions.assertNull(join.get(0).getConstituent("other"));
            Descriptors.Descriptor simpleDescriptor = simpleRecord.getDescriptorForType();
            Assertions.assertEquals(0L, simpleRecord.getField(simpleDescriptor.findFieldByName("rec_no")));
            Assertions.assertEquals("even", simpleRecord.getField(simpleDescriptor.findFieldByName("str_value")));
            Assertions.assertEquals(1001L, simpleRecord.getField(simpleDescriptor.findFieldByName("other_rec_no")));

            simpleRecord = Verify.verifyNotNull(join.get(1).getConstituent("simple")).getRecord();
            simpleDescriptor = simpleRecord.getDescriptorForType();
            Assertions.assertEquals(0L, simpleRecord.getField(simpleDescriptor.findFieldByName("rec_no")));
            Assertions.assertEquals("even", simpleRecord.getField(simpleDescriptor.findFieldByName("str_value")));
            Assertions.assertEquals(1001L, simpleRecord.getField(simpleDescriptor.findFieldByName("other_rec_no")));
            var otherRecord = Verify.verifyNotNull(join.get(1).getConstituent("other")).getRecord();
            Descriptors.Descriptor otherDescriptor = otherRecord.getDescriptorForType();
            Assertions.assertEquals(1001L, otherRecord.getField(otherDescriptor.findFieldByName("rec_no")));
            Assertions.assertEquals(1, otherRecord.getField(otherDescriptor.findFieldByName("num_value_3")));

            simpleRecord = Verify.verifyNotNull(join.get(2).getConstituent("simple")).getRecord();
            simpleDescriptor = simpleRecord.getDescriptorForType();
            Assertions.assertEquals(2L, simpleRecord.getField(simpleDescriptor.findFieldByName("rec_no")));
            Assertions.assertEquals("even", simpleRecord.getField(simpleDescriptor.findFieldByName("str_value")));
            Assertions.assertEquals(1003L, simpleRecord.getField(simpleDescriptor.findFieldByName("other_rec_no")));
            Assertions.assertNull(join.get(2).getConstituent("other"));

            // same query except setting required fields to get a covering scan
            query = RecordQuery.newBuilder()
                    .setRecordType("LeftJoined")
                    .setFilter(Query.field("simple").matches(Query.field("str_value").equalsValue("even")))
                    .setRequiredResults(ImmutableList.of(field("simple").nest("str_value")))
                    .build();
            plan = planner.plan(query);
            Assertions.assertTrue(
                    coveringIndexPlan()
                            .where(indexPlanOf(indexPlan().where(RecordQueryPlanMatchers.indexName("simple.str_value_other"))
                                    .and(scanComparisons(range("[[even],[even]]"))))).matches(plan));

            // same result set as before except some fields remain unset due to using a covering index
            join = recordStore.executeQuery(plan).asList().join();

            //
            // TODO Note that due to https://github.com/FoundationDB/fdb-record-layer/issues/1883, the index incorrectly
            //      returns items that should not be in the index anymore.
            //
            Assertions.assertEquals(3, join.size());
            Message message = Verify.verifyNotNull(join.get(0).getRecord());
            Descriptors.Descriptor descriptor = message.getDescriptorForType();
            simpleRecord = (Message)Verify.verifyNotNull(message.getField(descriptor.findFieldByName("simple")));
            simpleDescriptor = simpleRecord.getDescriptorForType();
            Assertions.assertEquals(0L, simpleRecord.getField(simpleDescriptor.findFieldByName("rec_no")));
            Assertions.assertEquals("even", simpleRecord.getField(simpleDescriptor.findFieldByName("str_value")));
            Assertions.assertFalse(simpleRecord.hasField(simpleDescriptor.findFieldByName("other_rec_no")));
            Assertions.assertFalse(message.hasField(descriptor.findFieldByName("other")));

            message = Verify.verifyNotNull(join.get(1).getRecord());
            simpleRecord = (Message)Verify.verifyNotNull(message.getField(descriptor.findFieldByName("simple")));
            simpleDescriptor = simpleRecord.getDescriptorForType();
            Assertions.assertEquals(0L, simpleRecord.getField(simpleDescriptor.findFieldByName("rec_no")));
            Assertions.assertEquals("even", simpleRecord.getField(simpleDescriptor.findFieldByName("str_value")));
            Assertions.assertFalse(simpleRecord.hasField(simpleDescriptor.findFieldByName("other_rec_no")));
            otherRecord = (Message)Verify.verifyNotNull(message.getField(descriptor.findFieldByName("other")));
            otherDescriptor = otherRecord.getDescriptorForType();
            Assertions.assertEquals(1001L, otherRecord.getField(otherDescriptor.findFieldByName("rec_no")));
            Assertions.assertFalse(otherRecord.hasField(otherDescriptor.findFieldByName("num_value_3")));

            message = Verify.verifyNotNull(join.get(2).getRecord());
            simpleRecord = (Message)Verify.verifyNotNull(message.getField(descriptor.findFieldByName("simple")));
            simpleDescriptor = simpleRecord.getDescriptorForType();
            Assertions.assertEquals(2L, simpleRecord.getField(simpleDescriptor.findFieldByName("rec_no")));
            Assertions.assertEquals("even", simpleRecord.getField(simpleDescriptor.findFieldByName("str_value")));
            Assertions.assertFalse(simpleRecord.hasField(simpleDescriptor.findFieldByName("other_rec_no")));
            Assertions.assertFalse(message.hasField(descriptor.findFieldByName("other")));
        }
    }
}
