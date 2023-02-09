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

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.FunctionNames;
import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.IsolationLevel;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorIterator;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TestRecordsJoinIndexProto;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexAggregateFunction;
import com.apple.foundationdb.record.metadata.IndexOptions;
import com.apple.foundationdb.record.metadata.IndexRecordFunction;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.JoinedRecordType;
import com.apple.foundationdb.record.metadata.JoinedRecordTypeBuilder;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.expressions.AbsoluteValueFunctionKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.GroupingKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.IntWrappingFunction;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.metadata.expressions.TupleFieldsHelper;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabase;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabaseFactory;
import com.apple.foundationdb.record.provider.foundationdb.FDBQueriedRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoredRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBSyntheticRecord;
import com.apple.foundationdb.record.provider.foundationdb.TestKeySpace;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpacePath;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.record.query.plan.PlannableIndexTypes;
import com.apple.foundationdb.record.query.plan.QueryPlanner;
import com.apple.foundationdb.record.query.plan.RecordQueryPlanner;
import com.apple.foundationdb.record.query.plan.ScanComparisons;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers;
import com.apple.foundationdb.record.query.plan.match.PlanMatchers;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.Tags;
import com.google.common.base.Verify;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultiset;
import com.google.common.collect.Multiset;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;
import static com.apple.foundationdb.record.metadata.Key.Expressions.concatenateFields;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static com.apple.foundationdb.record.metadata.Key.Expressions.function;
import static com.apple.foundationdb.record.metadata.Key.Expressions.recordType;
import static com.apple.foundationdb.record.query.plan.ScanComparisons.range;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.coveringIndexPlan;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.indexPlan;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.indexPlanOf;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.scanComparisons;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * Tests for {@link SyntheticRecordPlanner}.
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
        return fdb.openContext(null, timer);
    }

    private QueryPlanner setupPlanner(@Nonnull FDBRecordStore recordStore, @Nullable PlannableIndexTypes indexTypes) {
        if (indexTypes == null) {
            indexTypes = PlannableIndexTypes.DEFAULT;
        }
        return new RecordQueryPlanner(recordStore.getRecordMetaData(), recordStore.getRecordStoreState(), indexTypes, recordStore.getTimer());
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
    public void indexScansOverOuterJoins() throws Exception {
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

    @Test
    public void joinIndex() throws Exception {
        metaDataBuilder.addIndex("MySimpleRecord", "other_rec_no");
        final JoinedRecordTypeBuilder joined = metaDataBuilder.addJoinedRecordType("Simple_Other");
        joined.addConstituent("simple", "MySimpleRecord");
        joined.addConstituent("other", "MyOtherRecord");
        joined.addJoin("simple", "other_rec_no", "other", "rec_no");
        metaDataBuilder.addIndex(joined, new Index("simple.str_value_other.num_value_3", concat(field("simple").nest("str_value"), field("other").nest("num_value_3"))));

        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore recordStore = recordStoreBuilder.setContext(context).create();

            for (int i = 0; i < 3; i++) {
                for (int j = 0; j < i; j++) {
                    TestRecordsJoinIndexProto.MySimpleRecord.Builder simple = TestRecordsJoinIndexProto.MySimpleRecord.newBuilder();
                    simple.setRecNo(100 * i + j).setOtherRecNo(1000 + i);
                    simple.setStrValue((i + j) % 2 == 0 ? "even" : "odd");
                    recordStore.saveRecord(simple.build());
                }
                TestRecordsJoinIndexProto.MyOtherRecord.Builder other = TestRecordsJoinIndexProto.MyOtherRecord.newBuilder();
                other.setRecNo(1000 + i);
                other.setNumValue3(i);
                recordStore.saveRecord(other.build());
            }

            context.commit();
        }

        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore recordStore = recordStoreBuilder.setContext(context).open();
            final Index index = recordStore.getRecordMetaData().getIndex("simple.str_value_other.num_value_3");
            final TupleRange range = new ScanComparisons.Builder()
                    .addEqualityComparison(new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, "even"))
                    .addInequalityComparison(new Comparisons.SimpleComparison(Comparisons.Type.GREATER_THAN, 1))
                    .build()
                    .toTupleRange();

            List<Tuple> expected1 = Arrays.asList(Tuple.from("even", 2, -1, Tuple.from(200), Tuple.from(1002)));
            List<Tuple> results1 = recordStore.scanIndex(index, IndexScanType.BY_VALUE, range, null, ScanProperties.FORWARD_SCAN).map(IndexEntry::getKey).asList().join();
            assertEquals(expected1, results1);

            FDBStoredRecord<Message> record = recordStore.loadRecord(Tuple.from(201));
            TestRecordsJoinIndexProto.MySimpleRecord.Builder recordBuilder = TestRecordsJoinIndexProto.MySimpleRecord.newBuilder().mergeFrom(record.getRecord());
            recordBuilder.setStrValue("even");
            recordStore.saveRecord(recordBuilder.build());

            List<Tuple> expected2 = Arrays.asList(Tuple.from("even", 2, -1, Tuple.from(200), Tuple.from(1002)), Tuple.from("even", 2, -1, Tuple.from(201), Tuple.from(1002)));
            List<Tuple> results2 = recordStore.scanIndex(index, IndexScanType.BY_VALUE, range, null, ScanProperties.FORWARD_SCAN).map(IndexEntry::getKey).asList().join();
            assertEquals(expected2, results2);

            recordStore.deleteRecord(Tuple.from(1002));

            List<Tuple> expected3 = Arrays.asList();
            List<Tuple> results3 = recordStore.scanIndex(index, IndexScanType.BY_VALUE, range, null, ScanProperties.FORWARD_SCAN).map(IndexEntry::getKey).asList().join();
            assertEquals(expected3, results3);
        }

        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore recordStore = recordStoreBuilder.setContext(context).open();

            final QueryPlanner planner = setupPlanner(recordStore, null);

            RecordQuery query = RecordQuery.newBuilder()
                    .setRecordType("Simple_Other")
                    .setFilter(Query.field("simple").matches(Query.field("str_value").equalsValue("even")))
                    .build();

            RecordQueryPlan plan = planner.plan(query);
            Assertions.assertTrue(
                    indexPlan().where(RecordQueryPlanMatchers.indexName("simple.str_value_other.num_value_3"))
                            .and(scanComparisons(range("[[even],[even]]"))).matches(plan));
            try (RecordCursorIterator<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan).asIterator()) {
                int count = 0;
                while (cursor.hasNext()) {
                    FDBQueriedRecord<Message> record = Objects.requireNonNull(cursor.next());
                    Message message = record.getRecord();
                    count ++;
                    Descriptors.Descriptor descriptor = message.getDescriptorForType();
                    Message simpleRecord = (Message)message.getField(descriptor.findFieldByName("simple"));
                    Descriptors.Descriptor simpleDescriptor = simpleRecord.getDescriptorForType();
                    Assertions.assertEquals(200L, simpleRecord.getField(simpleDescriptor.findFieldByName("rec_no")));
                    Assertions.assertEquals("even", simpleRecord.getField(simpleDescriptor.findFieldByName("str_value")));
                    Assertions.assertEquals(1002L, simpleRecord.getField(simpleDescriptor.findFieldByName("other_rec_no")));
                    Message otherRecord = (Message)message.getField(descriptor.findFieldByName("other"));
                    Descriptors.Descriptor otherDescriptor = otherRecord.getDescriptorForType();
                    Assertions.assertEquals(1002L, otherRecord.getField(otherDescriptor.findFieldByName("rec_no")));
                    Assertions.assertEquals(2, otherRecord.getField(otherDescriptor.findFieldByName("num_value_3")));
                }
                Assertions.assertEquals(1, count);
            }

            query = RecordQuery.newBuilder()
                    .setRecordType("Simple_Other")
                    .setFilter(Query.field("simple").matches(Query.field("str_value").equalsValue("even")))
                    .setRequiredResults(ImmutableList.of(field("simple").nest("str_value"), field("other").nest("num_value_3")))
                    .build();
            plan = planner.plan(query);
            Assertions.assertTrue(
                    coveringIndexPlan()
                            .where(indexPlanOf(indexPlan().where(RecordQueryPlanMatchers.indexName("simple.str_value_other.num_value_3"))
                                    .and(scanComparisons(range("[[even],[even]]"))))).matches(plan));
            try (RecordCursorIterator<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan).asIterator()) {
                int count = 0;
                while (cursor.hasNext()) {
                    FDBQueriedRecord<Message> record = Objects.requireNonNull(cursor.next());
                    Message message = record.getRecord();
                    count ++;
                    Descriptors.Descriptor descriptor = message.getDescriptorForType();
                    Message simpleRecord = (Message)message.getField(descriptor.findFieldByName("simple"));
                    Descriptors.Descriptor simpleDescriptor = simpleRecord.getDescriptorForType();
                    Assertions.assertEquals(200L, simpleRecord.getField(simpleDescriptor.findFieldByName("rec_no")));
                    Assertions.assertEquals("even", simpleRecord.getField(simpleDescriptor.findFieldByName("str_value")));
                    Message otherRecord = (Message)message.getField(descriptor.findFieldByName("other"));
                    Descriptors.Descriptor otherDescriptor = otherRecord.getDescriptorForType();
                    Assertions.assertEquals(1002L, otherRecord.getField(otherDescriptor.findFieldByName("rec_no")));
                    Assertions.assertEquals(2, otherRecord.getField(otherDescriptor.findFieldByName("num_value_3")));
                }
                Assertions.assertEquals(1, count);
            }
        }
    }

    @Test
    public void buildJoinIndex() throws Exception {
        metaDataBuilder.addIndex("MySimpleRecord", "other_rec_no");
        final JoinedRecordTypeBuilder joined = metaDataBuilder.addJoinedRecordType("Simple_Other");
        joined.addConstituent("simple", "MySimpleRecord");
        joined.addConstituent("other", "MyOtherRecord");
        joined.addJoin("simple", "other_rec_no", "other", "rec_no");

        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore recordStore = recordStoreBuilder.setContext(context).create();

            for (int i = 0; i < 3; i++) {
                TestRecordsJoinIndexProto.MySimpleRecord.Builder simple = TestRecordsJoinIndexProto.MySimpleRecord.newBuilder();
                simple.setRecNo(i).setOtherRecNo(1000 + i);
                simple.setNumValue2(i * 2);
                recordStore.saveRecord(simple.build());
                TestRecordsJoinIndexProto.MyOtherRecord.Builder other = TestRecordsJoinIndexProto.MyOtherRecord.newBuilder();
                other.setRecNo(1000 + i);
                other.setNumValue3(i * 3);
                recordStore.saveRecord(other.build());
            }

            context.commit();
        }

        metaDataBuilder.addIndex(joined, new Index("simple.num_value_2_other.num_value_3", concat(field("simple").nest("num_value_2"), field("other").nest("num_value_3"))));

        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore recordStore = recordStoreBuilder.setContext(context).open();
            final Index index = recordStore.getRecordMetaData().getIndex("simple.num_value_2_other.num_value_3");
            final TupleRange range = new ScanComparisons.Builder()
                    .addEqualityComparison(new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, 2))
                    .addEqualityComparison(new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, 3))
                    .build()
                    .toTupleRange();

            List<Tuple> expected1 = Arrays.asList(Tuple.from(2, 3, -1, Tuple.from(1), Tuple.from(1001)));
            List<Tuple> results1 = recordStore.scanIndex(index, IndexScanType.BY_VALUE, range, null, ScanProperties.FORWARD_SCAN).map(IndexEntry::getKey).asList().join();
            assertEquals(expected1, results1);
        }
    }

    @Test
    public void aggregateJoinIndex() throws Exception {
        final KeyExpression pkey = concat(recordType(), field("uuid"));
        metaDataBuilder.getRecordType("Customer").setPrimaryKey(pkey);
        metaDataBuilder.getRecordType("Order").setPrimaryKey(pkey);
        metaDataBuilder.getRecordType("Item").setPrimaryKey(pkey);
        metaDataBuilder.addIndex("Customer", "name");
        metaDataBuilder.addIndex("Order", "order_no");
        metaDataBuilder.addIndex("Order", "customer_uuid");
        metaDataBuilder.addIndex("Item", "order_uuid");
        final JoinedRecordTypeBuilder joined = metaDataBuilder.addJoinedRecordType("COI");
        joined.addConstituent("c", "Customer");
        joined.addConstituent("o", "Order");
        joined.addConstituent("i", "Item");
        joined.addJoin("o", "customer_uuid", "c", "uuid");
        joined.addJoin("i", "order_uuid", "o", "uuid");
        metaDataBuilder.addIndex(joined, new Index("total_price_by_city", field("i").nest("total_price").groupBy(field("c").nest("city")), IndexTypes.SUM));

        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore recordStore = recordStoreBuilder.setContext(context).create();

            TestRecordsJoinIndexProto.Customer.Builder c = TestRecordsJoinIndexProto.Customer.newBuilder();
            c.setUuid(TupleFieldsHelper.toProto(UUID.randomUUID())).setName("Jones").setCity("Boston");
            recordStore.saveRecord(c.build());
            c.setUuid(TupleFieldsHelper.toProto(UUID.randomUUID())).setName("Smith").setCity("New York");
            recordStore.saveRecord(c.build());
            c.setUuid(TupleFieldsHelper.toProto(UUID.randomUUID())).setName("Lee").setCity("Boston");
            recordStore.saveRecord(c.build());

            context.commit();
        }

        final RecordQuery findByName = RecordQuery.newBuilder().setRecordType("Customer").setFilter(Query.field("name").equalsParameter("name")).build();
        final RecordQuery findByOrderNo = RecordQuery.newBuilder().setRecordType("Order").setFilter(Query.field("order_no").equalsParameter("order_no")).build();
        final Index index = metaDataBuilder.getRecordMetaData().getIndex("total_price_by_city");
        final IndexAggregateFunction sumByCity = new IndexAggregateFunction(FunctionNames.SUM, index.getRootExpression(), index.getName());
        final List<String> coi = Collections.singletonList("COI");

        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore recordStore = recordStoreBuilder.setContext(context).open();

            TestRecordsJoinIndexProto.Customer.Builder c = TestRecordsJoinIndexProto.Customer.newBuilder();
            c.mergeFrom(recordStore.planQuery(findByName).execute(recordStore, EvaluationContext.forBinding("name", "Jones")).first().join().orElseThrow(() -> new RuntimeException("not found")).getRecord());

            TestRecordsJoinIndexProto.Order.Builder o = TestRecordsJoinIndexProto.Order.newBuilder();
            o.setUuid(TupleFieldsHelper.toProto(UUID.randomUUID())).setOrderNo(1001).setCustomerUuid(c.getUuid());
            recordStore.saveRecord(o.build());

            TestRecordsJoinIndexProto.Item.Builder i = TestRecordsJoinIndexProto.Item.newBuilder();
            i.setUuid(TupleFieldsHelper.toProto(UUID.randomUUID())).setItemNo(123).setQuantity(100).setTotalPrice(200).setOrderUuid(o.getUuid());
            recordStore.saveRecord(i.build());
            i.setUuid(TupleFieldsHelper.toProto(UUID.randomUUID())).setItemNo(456).setQuantity(10).setTotalPrice(1000).setOrderUuid(o.getUuid());
            recordStore.saveRecord(i.build());

            context.commit();
        }

        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore recordStore = recordStoreBuilder.setContext(context).open();

            TestRecordsJoinIndexProto.Customer.Builder c = TestRecordsJoinIndexProto.Customer.newBuilder();
            c.mergeFrom(recordStore.planQuery(findByName).execute(recordStore, EvaluationContext.forBinding("name", "Smith")).first().join().orElseThrow(() -> new RuntimeException("not found")).getRecord());

            TestRecordsJoinIndexProto.Order.Builder o = TestRecordsJoinIndexProto.Order.newBuilder();
            o.setUuid(TupleFieldsHelper.toProto(UUID.randomUUID())).setOrderNo(1002).setCustomerUuid(c.getUuid());
            recordStore.saveRecord(o.build());

            TestRecordsJoinIndexProto.Item.Builder i = TestRecordsJoinIndexProto.Item.newBuilder();
            i.setUuid(TupleFieldsHelper.toProto(UUID.randomUUID())).setItemNo(789).setQuantity(20).setTotalPrice(200).setOrderUuid(o.getUuid());
            recordStore.saveRecord(i.build());

            context.commit();
        }

        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore recordStore = recordStoreBuilder.setContext(context).open();

            TestRecordsJoinIndexProto.Customer.Builder c = TestRecordsJoinIndexProto.Customer.newBuilder();
            c.mergeFrom(recordStore.planQuery(findByName).execute(recordStore, EvaluationContext.forBinding("name", "Lee")).first().join().orElseThrow(() -> new RuntimeException("not found")).getRecord());

            TestRecordsJoinIndexProto.Order.Builder o = TestRecordsJoinIndexProto.Order.newBuilder();
            o.setUuid(TupleFieldsHelper.toProto(UUID.randomUUID())).setOrderNo(1003).setCustomerUuid(c.getUuid());
            recordStore.saveRecord(o.build());

            TestRecordsJoinIndexProto.Item.Builder i = TestRecordsJoinIndexProto.Item.newBuilder();
            i.setUuid(TupleFieldsHelper.toProto(UUID.randomUUID())).setItemNo(123).setQuantity(150).setTotalPrice(300).setOrderUuid(o.getUuid());
            recordStore.saveRecord(i.build());

            context.commit();
        }

        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore recordStore = recordStoreBuilder.setContext(context).open();

            assertEquals(Tuple.from(1500), recordStore.evaluateAggregateFunction(coi, sumByCity, Key.Evaluated.scalar("Boston"), IsolationLevel.SERIALIZABLE).join());
        }

        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore recordStore = recordStoreBuilder.setContext(context).open();

            TestRecordsJoinIndexProto.Customer.Builder c = TestRecordsJoinIndexProto.Customer.newBuilder();
            c.mergeFrom(recordStore.planQuery(findByName).execute(recordStore, EvaluationContext.forBinding("name", "Lee")).first().join().orElseThrow(() -> new RuntimeException("not found")).getRecord());

            TestRecordsJoinIndexProto.Order.Builder o = TestRecordsJoinIndexProto.Order.newBuilder();
            o.setUuid(TupleFieldsHelper.toProto(UUID.randomUUID())).setOrderNo(1004).setCustomerUuid(c.getUuid());
            recordStore.saveRecord(o.build());

            TestRecordsJoinIndexProto.Item.Builder i = TestRecordsJoinIndexProto.Item.newBuilder();
            i.setUuid(TupleFieldsHelper.toProto(UUID.randomUUID())).setItemNo(456).setQuantity(1).setTotalPrice(100).setOrderUuid(o.getUuid());
            recordStore.saveRecord(i.build());

            context.commit();
        }

        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore recordStore = recordStoreBuilder.setContext(context).open();

            assertEquals(Tuple.from(1600), recordStore.evaluateAggregateFunction(coi, sumByCity, Key.Evaluated.scalar("Boston"), IsolationLevel.SERIALIZABLE).join());
        }

        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore recordStore = recordStoreBuilder.setContext(context).open();

            TestRecordsJoinIndexProto.Order.Builder o = TestRecordsJoinIndexProto.Order.newBuilder();
            o.mergeFrom(recordStore.planQuery(findByOrderNo).execute(recordStore, EvaluationContext.forBinding("order_no", 1003)).first().join().orElseThrow(() -> new RuntimeException("not found")).getRecord());

            TestRecordsJoinIndexProto.Item.Builder i = TestRecordsJoinIndexProto.Item.newBuilder();
            i.setUuid(TupleFieldsHelper.toProto(UUID.randomUUID())).setItemNo(789).setQuantity(10).setTotalPrice(100).setOrderUuid(o.getUuid());
            recordStore.saveRecord(i.build());

            context.commit();
        }

        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore recordStore = recordStoreBuilder.setContext(context).open();

            assertEquals(Tuple.from(1700), recordStore.evaluateAggregateFunction(coi, sumByCity, Key.Evaluated.scalar("Boston"), IsolationLevel.SERIALIZABLE).join());
        }

        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore recordStore = recordStoreBuilder.setContext(context).open();

            TestRecordsJoinIndexProto.Customer.Builder c = TestRecordsJoinIndexProto.Customer.newBuilder();
            c.mergeFrom(recordStore.planQuery(findByName).execute(recordStore, EvaluationContext.forBinding("name", "Lee")).first().join().orElseThrow(() -> new RuntimeException("not found")).getRecord());
            c.setCity("San Francisco");
            recordStore.saveRecord(c.build());

            context.commit();
        }

        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore recordStore = recordStoreBuilder.setContext(context).open();

            assertEquals(Tuple.from(1200), recordStore.evaluateAggregateFunction(coi, sumByCity, Key.Evaluated.scalar("Boston"), IsolationLevel.SERIALIZABLE).join());

            Map<Tuple, Tuple> expected = ImmutableMap.of(Tuple.from("Boston"), Tuple.from(1200), Tuple.from("New York"), Tuple.from(200), Tuple.from("San Francisco"), Tuple.from(500));
            Map<Tuple, Tuple> results = recordStore.scanIndex(index, IndexScanType.BY_GROUP, TupleRange.ALL, null, ScanProperties.FORWARD_SCAN)
                    .asList().join().stream().collect(Collectors.toMap(IndexEntry::getKey, IndexEntry::getValue));
            assertEquals(expected, results);
        }
    }

    @Test
    void joinOnNestedKeysWithDifferentTypes() throws Exception {
        metaDataBuilder.getRecordType("CustomerWithHeader").setPrimaryKey(Key.Expressions.concat(field("___header").nest("z_key"), field("___header").nest("int_rec_id")));
        metaDataBuilder.getRecordType("OrderWithHeader").setPrimaryKey(Key.Expressions.concat(field("___header").nest("z_key"), field("___header").nest("rec_id")));

        final JoinedRecordTypeBuilder joined = metaDataBuilder.addJoinedRecordType("MultiNestedFieldJoin");
        joined.addConstituent("order", "OrderWithHeader");
        joined.addConstituent("cust", "CustomerWithHeader");

        joined.addJoin("order",
                field("___header").nest("z_key"),
                "cust",
                field("___header").nest("z_key")
        );
        joined.addJoin("order",
                field("custRef").nest("string_value"),
                "cust",
                function(IntWrappingFunction.NAME, field("___header").nest("int_rec_id"))
        );

        metaDataBuilder.addIndex(joined, new Index("joinNestedConcat", concat(
                field("cust").nest("name"),
                field("order").nest("order_no")
        )));

        // Add index on custRef field to facilitate finding join partners of customer records
        metaDataBuilder.addIndex("OrderWithHeader", new Index("order$custRef", concat(
                field("___header").nest("z_key"),
                field("custRef").nest("string_value")
        )));

        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore recordStore = recordStoreBuilder.setContext(context).create();
            final Index joinIndex = recordStore.getRecordMetaData().getIndex("joinNestedConcat");

            final SyntheticRecordPlanner planner = new SyntheticRecordPlanner(recordStore);
            JoinedRecordType joinedRecordType = (JoinedRecordType) recordStore.getRecordMetaData().getSyntheticRecordType(joined.getName());
            assertConstituentPlansMatch(planner, joinedRecordType, Map.of(
                    "order",
                    SyntheticPlanMatchers.joinedRecord(List.of(
                            PlanMatchers.typeFilter(Matchers.contains("CustomerWithHeader"),
                                    PlanMatchers.scan(PlanMatchers.bounds(PlanMatchers.hasTupleString("[EQUALS $_j1, EQUALS wrap_int^-1($_j2)]")))))),
                    "cust",
                    SyntheticPlanMatchers.joinedRecord(List.of(
                            PlanMatchers.indexScan(Matchers.allOf(PlanMatchers.indexName("order$custRef"), PlanMatchers.bounds(PlanMatchers.hasTupleString("[EQUALS $_j1, EQUALS $_j2]"))))))
            ));

            TestRecordsJoinIndexProto.CustomerWithHeader customer = TestRecordsJoinIndexProto.CustomerWithHeader.newBuilder()
                    .setHeader(TestRecordsJoinIndexProto.Header.newBuilder().setZKey(1).setIntRecId(1L))
                    .setName("Scott")
                    .setCity("Toronto")
                    .build();
            recordStore.saveRecord(customer);

            TestRecordsJoinIndexProto.OrderWithHeader order = TestRecordsJoinIndexProto.OrderWithHeader.newBuilder()
                    .setHeader(TestRecordsJoinIndexProto.Header.newBuilder().setZKey(1).setRecId("23"))
                    .setOrderNo(10)
                    .setQuantity(23)
                    .setCustRef(TestRecordsJoinIndexProto.Ref.newBuilder().setStringValue("i:1"))
                    .build();
            recordStore.saveRecord(order);

            try (RecordCursor<IndexEntry> cursor = recordStore.scanIndex(joinIndex, IndexScanType.BY_VALUE, TupleRange.ALL, null, ScanProperties.FORWARD_SCAN)) {
                List<IndexEntry> entries = cursor.asList().get();
                assertEquals(1, entries.size());
                final IndexEntry indexEntry = entries.get(0);
                assertEquals("Scott", indexEntry.getKey().getString(0), "Incorrect customer name");
                assertEquals(10L, indexEntry.getKey().getLong(1), "Incorrect order number");
            }

            // Update the customer name
            TestRecordsJoinIndexProto.CustomerWithHeader customerWithNewName = customer.toBuilder()
                    .setName("Alec")
                    .build();
            recordStore.saveRecord(customerWithNewName);

            try (RecordCursor<IndexEntry> cursor = recordStore.scanIndex(joinIndex, IndexScanType.BY_VALUE, TupleRange.ALL, null, ScanProperties.FORWARD_SCAN)) {
                List<IndexEntry> entries = cursor.asList().get();
                assertEquals(1, entries.size());
                final IndexEntry indexEntry = entries.get(0);
                assertEquals("Alec", indexEntry.getKey().getString(0), "Incorrect customer name");
                assertEquals(10L, indexEntry.getKey().getLong(1), "Incorrect order number");
            }

            // Update the order number
            TestRecordsJoinIndexProto.OrderWithHeader orderWithNewNumber = order.toBuilder()
                    .setOrderNo(42)
                    .build();
            recordStore.saveRecord(orderWithNewNumber);

            try (RecordCursor<IndexEntry> cursor = recordStore.scanIndex(joinIndex, IndexScanType.BY_VALUE, TupleRange.ALL, null, ScanProperties.FORWARD_SCAN)) {
                List<IndexEntry> entries = cursor.asList().get();
                assertEquals(1, entries.size());
                final IndexEntry indexEntry = entries.get(0);
                assertEquals("Alec", indexEntry.getKey().getString(0), "Incorrect customer name");
                assertEquals(42L, indexEntry.getKey().getLong(1), "Incorrect order number");
            }

            // Insert an order with no associated customer
            TestRecordsJoinIndexProto.OrderWithHeader orderWithNoCustomer = TestRecordsJoinIndexProto.OrderWithHeader.newBuilder()
                    .setHeader(TestRecordsJoinIndexProto.Header.newBuilder().setZKey(2).setRecId("noCustomer"))
                    .setOrderNo(1066)
                    .setQuantity(9001)
                    .build();
            recordStore.saveRecord(orderWithNoCustomer);

            try (RecordCursor<IndexEntry> cursor = recordStore.scanIndex(joinIndex, IndexScanType.BY_VALUE, TupleRange.ALL, null, ScanProperties.FORWARD_SCAN)) {
                List<IndexEntry> entries = cursor.asList().get();
                assertEquals(1, entries.size());
                final IndexEntry indexEntry = entries.get(0);
                assertEquals("Alec", indexEntry.getKey().getString(0), "Incorrect customer name");
                assertEquals(42L, indexEntry.getKey().getLong(1), "Incorrect order number");
            }

            // Update no customer record with a string that cannot be decanonicalized
            TestRecordsJoinIndexProto.OrderWithHeader orderWithNoncanonicalCustomer = orderWithNoCustomer.toBuilder()
                    .setCustRef(TestRecordsJoinIndexProto.Ref.newBuilder().setStringValue("dangling_ref"))
                    .build();
            recordStore.saveRecord(orderWithNoncanonicalCustomer);

            try (RecordCursor<IndexEntry> cursor = recordStore.scanIndex(joinIndex, IndexScanType.BY_VALUE, TupleRange.ALL, null, ScanProperties.FORWARD_SCAN)) {
                List<IndexEntry> entries = cursor.asList().get();
                assertEquals(1, entries.size());
                final IndexEntry indexEntry = entries.get(0);
                assertEquals("Alec", indexEntry.getKey().getString(0), "Incorrect customer name");
                assertEquals(42L, indexEntry.getKey().getLong(1), "Incorrect order number");
            }
        }
    }

    @Test
    void joinOnListOfKeysWithDifferentTypes() throws Exception {
        metaDataBuilder.getRecordType("CustomerWithHeader").setPrimaryKey(Key.Expressions.concat(field("___header").nest("z_key"), field("___header").nest("int_rec_id")));
        metaDataBuilder.getRecordType("OrderWithHeader").setPrimaryKey(Key.Expressions.concat(field("___header").nest("z_key"), field("___header").nest("rec_id")));

        final JoinedRecordTypeBuilder joined = metaDataBuilder.addJoinedRecordType("OrderCCJoin");
        joined.addConstituent("order", "OrderWithHeader");
        joined.addConstituent("cust", "CustomerWithHeader");
        joined.addJoin("order",
                field("___header").nest("z_key"),
                "cust",
                field("___header").nest("z_key")
        );
        joined.addJoin("order",
                field("cc", KeyExpression.FanType.FanOut).nest("string_value"),
                "cust",
                function(IntWrappingFunction.NAME, field("___header").nest("int_rec_id"))
        );

        // Add an index on the cc field so that the join planner can use the index to resolve join pairs
        metaDataBuilder.addIndex(metaDataBuilder.getRecordType("OrderWithHeader"),
                new Index("OrderWithHeader$cc", concat(field("___header").nest("z_key"), field("cc", KeyExpression.FanType.FanOut).nest("string_value"))));
        // Add a join index listing all of the CC'd customer names for a given order number
        metaDataBuilder.addIndex(joined, new Index("OrderCCNames",
                concat(field("order").nest(field("___header").nest("z_key")), field("order").nest("order_no"), field("cust").nest("name"))));

        List<TestRecordsJoinIndexProto.CustomerWithHeader> customers = IntStream.range(0, 10)
                .mapToObj(i -> TestRecordsJoinIndexProto.CustomerWithHeader.newBuilder()
                        .setHeader(TestRecordsJoinIndexProto.Header.newBuilder().setZKey(1L).setIntRecId(i).build())
                        .setName(String.format("Customer %d", i))
                        .build()
                )
                .collect(Collectors.toList());
        List<TestRecordsJoinIndexProto.OrderWithHeader> orders = IntStream.range(0, customers.size())
                .mapToObj(i -> TestRecordsJoinIndexProto.OrderWithHeader.newBuilder()
                        .setHeader(TestRecordsJoinIndexProto.Header.newBuilder().setZKey(1L).setRecId(String.format("order_%d", i)))
                        .setOrderNo(1000 + i)
                        .setQuantity(100)
                        .addAllCc(IntStream.range(0, i).mapToObj(refId -> TestRecordsJoinIndexProto.Ref.newBuilder().setStringValue(String.format("i:%d", refId)).build()).collect(Collectors.toList()))
                        .build())
                .collect(Collectors.toList());

        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore recordStore = recordStoreBuilder.setContext(context).create();

            final SyntheticRecordPlanner planner = new SyntheticRecordPlanner(recordStore);
            JoinedRecordType joinedRecordType = (JoinedRecordType) recordStore.getRecordMetaData().getSyntheticRecordType(joined.getName());
            assertConstituentPlansMatch(planner, joinedRecordType, Map.of(
                    "order",
                    SyntheticPlanMatchers.joinedRecord(List.of(
                            PlanMatchers.inComparand(PlanMatchers.hasTypelessString("wrap_int^-1($_j2)"),
                                    PlanMatchers.typeFilter(Matchers.contains("CustomerWithHeader"),
                                            PlanMatchers.scan(PlanMatchers.bounds(PlanMatchers.hasTupleString("[EQUALS $_j1, EQUALS $__in_int_rec_id__0]"))))))),
                    "cust",
                    SyntheticPlanMatchers.joinedRecord(List.of(
                            PlanMatchers.primaryKeyDistinct(
                                    PlanMatchers.indexScan(Matchers.allOf(PlanMatchers.indexName("OrderWithHeader$cc"), PlanMatchers.bounds(PlanMatchers.hasTupleString("[EQUALS $_j1, EQUALS $_j2]"))))))))
            );

            for (int i = 0; i < customers.size(); i++) {
                recordStore.saveRecord(customers.get(i));
                recordStore.saveRecord(orders.get(orders.size() - i - 1));
            }

            final Index joinIndex = recordStore.getRecordMetaData().getIndex("OrderCCNames");
            for (TestRecordsJoinIndexProto.OrderWithHeader order : orders) {
                final Set<String> ccIds = order.getCcList().stream()
                        .map(TestRecordsJoinIndexProto.Ref::getStringValue)
                        .collect(Collectors.toSet());
                final List<String> customerNames = customers.stream()
                        .filter(customer -> ccIds.contains(String.format("i:%d", customer.getHeader().getIntRecId())))
                        .map(TestRecordsJoinIndexProto.CustomerWithHeader::getName)
                        .collect(Collectors.toList());
                try (RecordCursor<IndexEntry> cursor = recordStore.scanIndex(
                        joinIndex, IndexScanType.BY_VALUE, TupleRange.allOf(Tuple.from(order.getHeader().getZKey(), order.getOrderNo())), null, ScanProperties.FORWARD_SCAN)) {

                    final List<String> foundNames = cursor
                            .map(IndexEntry::getKey)
                            .map(key -> key.getString(2))
                            .asList()
                            .get();

                    assertEquals(customerNames, foundNames);
                }
            }
        }
    }

    @Test
    void joinOnMultipleNestedKeys() throws Exception {
        metaDataBuilder.getRecordType("CustomerWithHeader").setPrimaryKey(Key.Expressions.concat(field("___header").nest("z_key"), field("___header").nest("rec_id")));
        metaDataBuilder.getRecordType("OrderWithHeader").setPrimaryKey(Key.Expressions.concat(field("___header").nest("z_key"), field("___header").nest("rec_id")));

        final JoinedRecordTypeBuilder joined = metaDataBuilder.addJoinedRecordType("MultiNestedFieldJoin");
        joined.addConstituent("order", "OrderWithHeader");
        joined.addConstituent("cust", "CustomerWithHeader");
        joined.addJoin("order", field("___header").nest("z_key"),
                "cust", field("___header").nest("z_key"));
        joined.addJoin("order", field("custRef").nest("string_value"),
                "cust", field("___header").nest("rec_id"));

        metaDataBuilder.addIndex(joined, new Index("joinNestedConcat", concat(
                field("cust").nest("name"),
                field("order").nest("order_no")
        )));
        metaDataBuilder.addIndex("OrderWithHeader", "order$custRef", concat(field("___header").nest("z_key"), field("custRef").nest("string_value")));

        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore recordStore = recordStoreBuilder.setContext(context).create();

            final SyntheticRecordPlanner planner = new SyntheticRecordPlanner(recordStore);
            JoinedRecordType joinedRecordType = (JoinedRecordType) recordStore.getRecordMetaData().getSyntheticRecordType(joined.getName());
            assertConstituentPlansMatch(planner, joinedRecordType, Map.of(
                    "order",
                    SyntheticPlanMatchers.joinedRecord(List.of(
                            PlanMatchers.typeFilter(Matchers.contains("CustomerWithHeader"),
                                    PlanMatchers.scan(PlanMatchers.bounds(PlanMatchers.hasTupleString("[EQUALS $_j1, EQUALS $_j2]")))))),
                    "cust",
                    SyntheticPlanMatchers.joinedRecord(List.of(
                            PlanMatchers.indexScan(Matchers.allOf(PlanMatchers.indexName("order$custRef"), PlanMatchers.bounds(PlanMatchers.hasTupleString("[EQUALS $_j1, EQUALS $_j2]"))))))
            ));

            TestRecordsJoinIndexProto.CustomerWithHeader.Builder custBuilder = TestRecordsJoinIndexProto.CustomerWithHeader.newBuilder();
            custBuilder.getHeaderBuilder().setZKey(1).setRecId("1");
            custBuilder.setName("Scott Fines");
            custBuilder.setCity("Toronto");

            recordStore.saveRecord(custBuilder.build());

            TestRecordsJoinIndexProto.OrderWithHeader.Builder orderBuilder = TestRecordsJoinIndexProto.OrderWithHeader.newBuilder();
            orderBuilder.getHeaderBuilder().setZKey(1).setRecId("23");
            orderBuilder.setOrderNo(10).setQuantity(23);
            orderBuilder.getCustRefBuilder().setStringValue("1");

            recordStore.saveRecord(orderBuilder.build());

            //now check that we can scan them back out again
            Index joinIdex = recordStore.getRecordMetaData().getIndex("joinNestedConcat");
            List<IndexEntry> entries = recordStore.scanIndex(joinIdex, IndexScanType.BY_VALUE, TupleRange.ALL, null, ScanProperties.FORWARD_SCAN).asList().get();
            assertEquals(1, entries.size());
            final IndexEntry indexEntry = entries.get(0);
            assertEquals("Scott Fines", indexEntry.getKey().getString(0), "Incorrect customer name");
            assertEquals(10L, indexEntry.getKey().getLong(1), "Incorrect order number");
        }
    }

    @Test
    void joinOnNonInjectiveFunction() throws Exception {
        final JoinedRecordTypeBuilder joined = metaDataBuilder.addJoinedRecordType("NumValue2Join");
        joined.addConstituent("simple", "MySimpleRecord");
        joined.addConstituent("other", "MyOtherRecord");

        // Join where simple.num_value_2 = abs_value(other.num_value)
        joined.addJoin(
                "simple",
                field("num_value_2"),
                "other",
                function(AbsoluteValueFunctionKeyExpression.NAME, field("num_value"))
        );
        metaDataBuilder.addIndex(joined, new Index("joinOnNumValue2", concat(field("simple").nest("str_value"), field("other").nest("num_value_3"))));

        // Indexes used to compute the join
        metaDataBuilder.addIndex("MySimpleRecord", "num_value_2");
        metaDataBuilder.addIndex("MyOtherRecord", "num_value");

        List<TestRecordsJoinIndexProto.MySimpleRecord> simpleRecords = IntStream.range(-10, 10)
                .mapToObj(i -> TestRecordsJoinIndexProto.MySimpleRecord.newBuilder()
                        .setRecNo(i + 1000L)
                        .setNumValue2(i)
                        .setStrValue(String.format("Record %d", i))
                        .build())
                .collect(Collectors.toList());
        List<TestRecordsJoinIndexProto.MyOtherRecord> otherRecords = IntStream.range(-10, 10)
                .mapToObj(i -> TestRecordsJoinIndexProto.MyOtherRecord.newBuilder()
                        .setRecNo(i + 2000L)
                        .setNumValue(i)
                        .setNumValue3(i * 10)
                        .build())
                .collect(Collectors.toList());

        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore recordStore = recordStoreBuilder.setContext(context).create();

            final SyntheticRecordPlanner planner = new SyntheticRecordPlanner(recordStore);
            final JoinedRecordType joinedRecordType = (JoinedRecordType) recordStore.getRecordMetaData().getSyntheticRecordType(joined.getName());
            assertConstituentPlansMatch(planner, joinedRecordType, Map.of(
                    "simple",
                    // Note that even though this was an equi-join on a single field, because abs_value is not injective, this
                    // gets planned as an IN-join
                    SyntheticPlanMatchers.joinedRecord(List.of(
                            PlanMatchers.inComparand(PlanMatchers.hasTypelessString("abs_value^-1($_j1)"),
                                    PlanMatchers.indexScan(Matchers.allOf(PlanMatchers.indexName("MyOtherRecord$num_value"), PlanMatchers.bounds(PlanMatchers.hasTupleString("[EQUALS $__in_num_value__0]")))))
                    )),
                    "other",
                    SyntheticPlanMatchers.joinedRecord(List.of(
                            PlanMatchers.indexScan(Matchers.allOf(PlanMatchers.indexName("MySimpleRecord$num_value_2"), PlanMatchers.bounds(PlanMatchers.hasTupleString("[EQUALS $_j1]"))))
                    ))
            ));

            final Index joinIndex = recordStore.getRecordMetaData().getIndex("joinOnNumValue2");
            for (int i = 0; i < simpleRecords.size(); i++) {
                recordStore.saveRecord(simpleRecords.get(i));
                recordStore.saveRecord(otherRecords.get(otherRecords.size() - i - 1));
            }

            for (TestRecordsJoinIndexProto.MySimpleRecord simpleRecord : simpleRecords) {
                List<Integer> matchingNumValue3s = otherRecords.stream()
                        .filter(other -> simpleRecord.getNumValue2() == Math.abs(other.getNumValue()))
                        .map(TestRecordsJoinIndexProto.MyOtherRecord::getNumValue3)
                        .collect(Collectors.toList());

                try (RecordCursor<IndexEntry> cursor = recordStore.scanIndex(
                        joinIndex, IndexScanType.BY_VALUE, TupleRange.allOf(Tuple.from(simpleRecord.getStrValue())), null, ScanProperties.FORWARD_SCAN)) {
                    List<Integer> foundNumValue3s = cursor.map(IndexEntry::getKey)
                            .map(key -> key.getLong(1))
                            .map(Long::intValue)
                            .asList()
                            .get();
                    assertEquals(matchingNumValue3s, foundNumValue3s);
                }
            }
        }
    }

    @Test
    public void multiFieldKeys() throws Exception {
        metaDataBuilder.getRecordType("MySimpleRecord").setPrimaryKey(concatenateFields("num_value", "rec_no"));
        metaDataBuilder.getRecordType("MyOtherRecord").setPrimaryKey(concatenateFields("num_value", "rec_no"));
        final JoinedRecordTypeBuilder joined = metaDataBuilder.addJoinedRecordType("MultiFieldJoin");
        joined.addConstituent("simple", "MySimpleRecord");
        joined.addConstituent("other", "MyOtherRecord");
        // TODO: Not supported alternative would be to join concatenateFields("num_value", "other_rec_no") with concatenateFields("num_value", "rec_no").
        joined.addJoin("simple", "num_value", "other", "num_value");
        joined.addJoin("simple", "other_rec_no", "other", "rec_no");
        metaDataBuilder.addIndex(joined, new Index("simple.str_value_other.num_value_3", concat(field("simple").nest("str_value"), field("other").nest("num_value_3"))));

        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore recordStore = recordStoreBuilder.setContext(context).create();

            for (int n = 1; n <= 2; n++) {
                for (int i = 0; i < 3; i++) {
                    for (int j = 0; j < i; j++) {
                        TestRecordsJoinIndexProto.MySimpleRecord.Builder simple = TestRecordsJoinIndexProto.MySimpleRecord.newBuilder();
                        simple.setNumValue(n);
                        simple.setRecNo(100 * i + j).setOtherRecNo(1000 + i);
                        simple.setStrValue((i + j) % 2 == 0 ? "even" : "odd");
                        recordStore.saveRecord(simple.build());
                    }
                    TestRecordsJoinIndexProto.MyOtherRecord.Builder other = TestRecordsJoinIndexProto.MyOtherRecord.newBuilder();
                    other.setNumValue(n);
                    other.setRecNo(1000 + i);
                    other.setNumValue3(i);
                    recordStore.saveRecord(other.build());
                }
            }

            context.commit();
        }

        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore recordStore = recordStoreBuilder.setContext(context).open();

            List<FDBSyntheticRecord> recs = recordStore.scanIndex(recordStore.getRecordMetaData().getIndex("simple.str_value_other.num_value_3"),
                            IndexScanType.BY_VALUE, TupleRange.allOf(Tuple.from("even", 2)), null, ScanProperties.FORWARD_SCAN)
                    .mapPipelined(entry -> recordStore.loadSyntheticRecord(entry.getPrimaryKey()), 1)
                    .asList().join();
            for (FDBSyntheticRecord record : recs) {
                TestRecordsJoinIndexProto.MySimpleRecord.Builder simple = TestRecordsJoinIndexProto.MySimpleRecord.newBuilder();
                TestRecordsJoinIndexProto.MyOtherRecord.Builder other = TestRecordsJoinIndexProto.MyOtherRecord.newBuilder();
                simple.mergeFrom(record.getConstituent("simple").getRecord());
                other.mergeFrom(record.getConstituent("other").getRecord());
                assertEquals(200, simple.getRecNo());
                assertEquals(1002, other.getRecNo());
                assertEquals(record.getPrimaryKey(), record.getRecordType().getPrimaryKey().evaluateSingleton(record).toTuple());
            }

        }
    }

    @Test
    public void rankJoinIndex() throws Exception {
        final JoinedRecordTypeBuilder joined = metaDataBuilder.addJoinedRecordType("JoinedForRank");
        joined.addConstituent("simple", "MySimpleRecord");
        joined.addConstituent("other", "MyOtherRecord");
        joined.addJoin("simple", "other_rec_no", "other", "rec_no");
        final GroupingKeyExpression group = field("simple").nest("num_value_2").groupBy(field("other").nest("num_value"));
        metaDataBuilder.addIndex(joined, new Index("simple.num_value_2_by_other.num_value", group, IndexTypes.RANK));

        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore recordStore = recordStoreBuilder.setContext(context).create();

            for (int i = 0; i < 4; i++) {
                for (int j = 0; j < i; j++) {
                    TestRecordsJoinIndexProto.MySimpleRecord.Builder simple = TestRecordsJoinIndexProto.MySimpleRecord.newBuilder();
                    simple.setRecNo(100 * i + j).setOtherRecNo(1000 + i);
                    simple.setNumValue2(i + j);
                    recordStore.saveRecord(simple.build());
                }
                TestRecordsJoinIndexProto.MyOtherRecord.Builder other = TestRecordsJoinIndexProto.MyOtherRecord.newBuilder();
                other.setRecNo(1000 + i);
                other.setNumValue(i % 2);
                recordStore.saveRecord(other.build());
            }

            context.commit();
        }

        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore recordStore = recordStoreBuilder.setContext(context).open();

            Index index = recordStore.getRecordMetaData().getIndex("simple.num_value_2_by_other.num_value");
            RecordCursor<IndexEntry> cursor = recordStore.scanIndex(index, IndexScanType.BY_RANK, TupleRange.allOf(Tuple.from(0, 1)), null, ScanProperties.FORWARD_SCAN);
            Tuple pkey = cursor.first().get().map(IndexEntry::getPrimaryKey).orElse(null);
            assertFalse(cursor.getNext().hasNext());
            // 201, 1002 and 200, 1003 both have score 3, but in different groups.
            assertEquals(Tuple.from(-1, Tuple.from(201), Tuple.from(1002)), pkey);

            FDBSyntheticRecord record = recordStore.loadSyntheticRecord(pkey).join();
            IndexRecordFunction<Long> rankFunction = ((IndexRecordFunction<Long>)Query.rank(group).getFunction())
                    .cloneWithIndex(index.getName());
            assertEquals(1, recordStore.evaluateRecordFunction(rankFunction, record).join().longValue());
        }
    }

    private static void assertConstituentPlansMatch(SyntheticRecordPlanner planner, JoinedRecordType joinedRecordType,
                                                    Map<String, Matcher<? super SyntheticRecordFromStoredRecordPlan>> constituentMatchers) {
        for (JoinedRecordType.JoinConstituent constituent : joinedRecordType.getConstituents()) {
            assertThat(String.format("constituent matchers missing matcher for constituent %s", constituent.getName()),
                    constituentMatchers, Matchers.hasKey(constituent.getName()));
            Matcher<? super SyntheticRecordFromStoredRecordPlan> matcher = constituentMatchers.get(constituent.getName());
            final SyntheticRecordFromStoredRecordPlan plan = planner.forJoinConstituent(joinedRecordType, constituent);
            assertThat(plan, matcher);
        }
    }
}
