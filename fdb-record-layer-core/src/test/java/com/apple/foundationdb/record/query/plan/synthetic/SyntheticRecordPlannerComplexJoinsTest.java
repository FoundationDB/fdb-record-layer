/*
 * SyntheticRecordPlannerComplexJoinsTest.java
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
import com.apple.foundationdb.record.metadata.JoinedRecordTypeBuilder;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
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
 * Tests for {@link SyntheticRecordPlanner} regarding more complex join capabilities.
 */
@Tag(Tags.RequiresFDB)
@API(API.Status.EXPERIMENTAL)
public class SyntheticRecordPlannerComplexJoinsTest extends AbstractSyntheticRecordPlannerTest {
    @Test
    void clique() {
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
            assertThat(plan1, SyntheticPlanMatchers.syntheticRecordScan(
                    PlanMatchers.indexScan("TypeA$type_b_rec_no"),
                    SyntheticPlanMatchers.joinedRecord(List.of(
                            PlanMatchers.typeFilter(Matchers.contains("TypeB"),
                                    PlanMatchers.scan(PlanMatchers.bounds(PlanMatchers.hasTupleString("[EQUALS $_j1]")))),
                            PlanMatchers.indexScan(Matchers.allOf(
                                    PlanMatchers.indexName("TypeC$type_a_rec_no"),
                                    PlanMatchers.bounds(PlanMatchers.hasTupleString("[EQUALS $_j3, EQUALS $_j2]"))))))));
            Multiset<Tuple> expected1 = ImmutableMultiset.of(
                    Tuple.from(-1, Tuple.from(100), Tuple.from(200), Tuple.from(300)),
                    Tuple.from(-1, Tuple.from(101), Tuple.from(201), Tuple.from(301)),
                    Tuple.from(-1, Tuple.from(102), Tuple.from(202), Tuple.from(302)));
            Multiset<Tuple> results1 = HashMultiset.create(plan1.execute(recordStore).map(FDBSyntheticRecord::getPrimaryKey).asList().join());
            assertEquals(expected1, results1);

            // Make sure that the extra join condition is checked.
            TestRecordsJoinIndexProto.TypeC.Builder typeC = TestRecordsJoinIndexProto.TypeC.newBuilder();
            typeC.setRecNo(301).setTypeARecNo(999);
            recordStore.saveRecord(typeC.build());

            SyntheticRecordPlan plan2 = plan1;
            Multiset<Tuple> expected2 = ImmutableMultiset.of(
                    Tuple.from(-1, Tuple.from(100), Tuple.from(200), Tuple.from(300)),
                    Tuple.from(-1, Tuple.from(102), Tuple.from(202), Tuple.from(302)));
            Multiset<Tuple> results2 = HashMultiset.create(plan2.execute(recordStore).map(FDBSyntheticRecord::getPrimaryKey).asList().join());
            assertEquals(expected2, results2);
        }
    }

    @Test
    void nestedRepeated() {
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
            assertThat(plan1, SyntheticPlanMatchers.syntheticRecordScan(
                    PlanMatchers.typeFilter(Matchers.contains("NestedA"), PlanMatchers.scan()),
                    SyntheticPlanMatchers.joinedRecord(List.of(
                            PlanMatchers.inParameter(Matchers.equalTo("_j1"),
                                    PlanMatchers.primaryKeyDistinct(
                                            PlanMatchers.indexScan(Matchers.allOf(
                                                    PlanMatchers.indexName("repeatedB"),
                                                    PlanMatchers.bounds(PlanMatchers.hasTupleString("[EQUALS $__in_nums__0]"))))))))));
            Multiset<Tuple> expected1 = ImmutableMultiset.of(
                    Tuple.from(-1, Tuple.from(101), Tuple.from(201)),
                    Tuple.from(-1, Tuple.from(101), Tuple.from(202)),
                    Tuple.from(-1, Tuple.from(102), Tuple.from(201)),
                    Tuple.from(-1, Tuple.from(102), Tuple.from(202)));
            Multiset<Tuple> results1 = HashMultiset.create(plan1.execute(recordStore).map(FDBSyntheticRecord::getPrimaryKey).asList().join());
            assertEquals(expected1, results1);

            FDBStoredRecord<Message> record = recordStore.loadRecord(Tuple.from(101));
            SyntheticRecordFromStoredRecordPlan plan2 = planner.fromStoredType(record.getRecordType(), false);
            assertThat(plan2, SyntheticPlanMatchers.joinedRecord(List.of(
                    PlanMatchers.inParameter(Matchers.equalTo("_j1"),
                            PlanMatchers.primaryKeyDistinct(
                                    PlanMatchers.indexScan(Matchers.allOf(
                                            PlanMatchers.indexName("repeatedB"),
                                            PlanMatchers.bounds(PlanMatchers.hasTupleString("[EQUALS $__in_nums__0]")))))))));
            // TODO: IN can generate duplicates from repeated field (https://github.com/FoundationDB/fdb-record-layer/issues/98)
            Multiset<Tuple> expected2 = ImmutableMultiset.of(
                    Tuple.from(-1, Tuple.from(101), Tuple.from(201)),
                    Tuple.from(-1, Tuple.from(101), Tuple.from(201)),
                    Tuple.from(-1, Tuple.from(101), Tuple.from(202)),
                    Tuple.from(-1, Tuple.from(101), Tuple.from(202)),
                    Tuple.from(-1, Tuple.from(101), Tuple.from(202)));
            Multiset<Tuple> results2 = HashMultiset.create(plan2.execute(recordStore, record).map(FDBSyntheticRecord::getPrimaryKey).asList().join());
            assertEquals(expected2, results2);
        }
    }
}
