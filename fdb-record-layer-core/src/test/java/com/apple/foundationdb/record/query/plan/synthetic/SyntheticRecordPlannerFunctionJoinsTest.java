/*
 * SyntheticRecordPlannerFunctionJoinsTest.java
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
import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TestRecordsJoinIndexProto;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexRecordFunction;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.JoinedRecordType;
import com.apple.foundationdb.record.metadata.JoinedRecordTypeBuilder;
import com.apple.foundationdb.record.metadata.expressions.AbsoluteValueFunctionKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.GroupingKeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBSyntheticRecord;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.record.query.plan.match.PlanMatchers;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.Tags;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;
import static com.apple.foundationdb.record.metadata.Key.Expressions.concatenateFields;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static com.apple.foundationdb.record.metadata.Key.Expressions.function;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * Tests for {@link SyntheticRecordPlanner} regarding function-based joins and complex key joins.
 */
@Tag(Tags.RequiresFDB)
@API(API.Status.EXPERIMENTAL)
public class SyntheticRecordPlannerFunctionJoinsTest extends AbstractSyntheticRecordPlannerTest {
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
                        .setStrValue("Record " + i)
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
            final JoinedRecordType joinedRecordType = (JoinedRecordType)recordStore.getRecordMetaData().getSyntheticRecordType(joined.getName());
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
    void multiFieldKeys() {
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
    void rankJoinIndex() throws Exception {
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
}
