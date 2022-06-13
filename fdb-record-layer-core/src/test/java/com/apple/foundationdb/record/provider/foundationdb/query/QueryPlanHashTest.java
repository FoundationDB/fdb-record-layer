/*
 * QueryPlanHashTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2020 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.provider.foundationdb.query;

import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.TestRecordsTextProto;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.provider.common.text.TextSamples;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.indexes.TextIndexTestUtils;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.record.query.expressions.QueryComponent;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.test.Tags;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;
import static com.apple.foundationdb.record.metadata.Key.Expressions.concatenateFields;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static com.apple.foundationdb.record.metadata.Key.Expressions.function;
import static com.apple.foundationdb.record.metadata.Key.Expressions.keyWithValue;
import static com.apple.foundationdb.record.metadata.Key.Expressions.value;
import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests for PlanHash on query plans.
 */
@Tag(Tags.RequiresFDB)
class QueryPlanHashTest extends FDBRecordStoreQueryTestBase {

    @BeforeEach
    public void setup() throws Exception {
        RecordMetaDataHook hook = complexQuerySetupHook();
        complexQuerySetup(hook);
    }

    @Test
    void testSingleEqualsFilter() {
        // Scan(<,>) | [MySimpleRecord] | num_value_2 EQUALS 1
        RecordQueryPlan plan1 = createPlan("MySimpleRecord", Query.field("num_value_2").equalsValue(1));
        // Scan(<,>) | [MySimpleRecord] | num_value_2 EQUALS 2
        RecordQueryPlan plan2 = createPlan("MySimpleRecord", Query.field("num_value_2").equalsValue(2));
        // Scan(<,>) | [MySimpleRecord] | num_value_2 EQUALS $3
        RecordQueryPlan plan3 = createPlan("MySimpleRecord", Query.field("num_value_2").equalsParameter("3"));

        assertEquals(913370523, plan1.planHash(PlanHashable.PlanHashKind.LEGACY));
        assertEquals(913370524, plan2.planHash(PlanHashable.PlanHashKind.LEGACY));
        assertEquals(913370573, plan3.planHash(PlanHashable.PlanHashKind.LEGACY));

        assertEquals(-1652411480, plan1.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(-1652411479, plan2.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(-383271037, plan3.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));

        assertEquals(448300575, plan1.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        assertEquals(448300575, plan2.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        // Note that value and parameter comparisons are not the same
        assertEquals(-383271037, plan3.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
    }

    @Test
    void testSingleGtFilter() {
        // Scan(<,>) | [MySimpleRecord] | num_value_2 GREATER_THAN 1
        RecordQueryPlan plan1 = createPlan("MySimpleRecord", Query.field("num_value_2").greaterThan(1));
        // Scan(<,>) | [MySimpleRecord] | num_value_2 GREATER_THAN 2
        RecordQueryPlan plan2 = createPlan("MySimpleRecord", Query.field("num_value_2").greaterThan(2));

        assertEquals(-167290686, plan1.planHash(PlanHashable.PlanHashKind.LEGACY));
        assertEquals(-167290685, plan2.planHash(PlanHashable.PlanHashKind.LEGACY));

        assertEquals(-793170591, plan1.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(-793170590, plan2.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));

        assertEquals(-632360634, plan1.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        assertEquals(-632360634, plan2.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
    }

    @Test
    void testSingleGteFilter() {
        // Scan(<,>) | [MySimpleRecord] | num_value_2 GREATER_THAN_OR_EQUALS 1
        RecordQueryPlan plan1 = createPlan("MySimpleRecord", Query.field("num_value_2").greaterThanOrEquals(1));
        // Scan(<,>) | [MySimpleRecord] | num_value_2 GREATER_THAN_OR_EQUALS 2
        RecordQueryPlan plan2 = createPlan("MySimpleRecord", Query.field("num_value_2").greaterThanOrEquals(2));

        assertEquals(-544375458, plan1.planHash(PlanHashable.PlanHashKind.LEGACY));
        assertEquals(-544375457, plan2.planHash(PlanHashable.PlanHashKind.LEGACY));

        assertEquals(402103365, plan1.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(402103366, plan2.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));

        assertEquals(-1009445406, plan1.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        assertEquals(-1009445406, plan2.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
    }

    @Test
    void testOrEqualsFilter() {
        // Scan(<,>) | [MySimpleRecord] | Or([num_value_2 EQUALS 1, num_value_2 EQUALS 2])
        RecordQueryPlan plan1 = createPlan("MySimpleRecord",
                Query.or(
                        Query.field("num_value_2").equalsValue(1),
                        Query.field("num_value_2").equalsValue(2)));
        // Scan(<,>) | [MySimpleRecord] | Or([num_value_2 EQUALS 3, num_value_2 EQUALS 4])
        RecordQueryPlan plan2 = createPlan("MySimpleRecord",
                Query.or(
                        Query.field("num_value_2").equalsValue(3),
                        Query.field("num_value_2").equalsValue(4)));
        // Scan(<,>) | [MySimpleRecord] | Or([num_value_2 EQUALS $5, num_value_2 EQUALS $6])
        RecordQueryPlan plan3 = createPlan("MySimpleRecord",
                Query.or(
                        Query.field("num_value_2").equalsParameter("5"),
                        Query.field("num_value_2").equalsParameter("6")));

        assertEquals(385591762, plan1.planHash(PlanHashable.PlanHashKind.LEGACY));
        assertEquals(385591826, plan2.planHash(PlanHashable.PlanHashKind.LEGACY));
        assertEquals(385593426, plan3.planHash(PlanHashable.PlanHashKind.LEGACY));

        assertEquals(-1260194269, plan1.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(-1260194205, plan2.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(697594307, plan3.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));

        assertEquals(1538082050, plan1.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        assertEquals(1538082050, plan2.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        // Note that the value and the parameter hashes are different.
        assertEquals(697594307, plan3.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
    }

    @Test
    void testAndGtFilter() {
        // Scan(<,>) | [MySimpleRecord] | And([num_value_2 GREATER_THAN 1, num_value_2 LESS_THAN 3])
        RecordQueryPlan plan1 = createPlan("MySimpleRecord",
                Query.and(
                        Query.field("num_value_2").greaterThan(1),
                        Query.field("num_value_2").lessThan(3)));
        // Scan(<,>) | [MySimpleRecord] | And([num_value_2 GREATER_THAN 2, num_value_2 LESS_THAN 4])
        RecordQueryPlan plan2 = createPlan("MySimpleRecord",
                Query.and(
                        Query.field("num_value_2").greaterThan(2),
                        Query.field("num_value_2").lessThan(4)));

        assertEquals(-1920816044, plan1.planHash(PlanHashable.PlanHashKind.LEGACY));
        assertEquals(-1920816012, plan2.planHash(PlanHashable.PlanHashKind.LEGACY));

        assertEquals(-558681201, plan1.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(-558681169, plan2.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));

        assertEquals(-1582614705, plan1.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        assertEquals(-1582614705, plan2.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
    }

    @Test
    void testOrGtFilter() {
        // Scan(<,>) | [MySimpleRecord] | Or([num_value_2 GREATER_THAN 1, num_value_2 LESS_THAN 3])
        RecordQueryPlan plan1 = createPlan("MySimpleRecord",
                Query.or(
                        Query.field("num_value_2").greaterThan(1),
                        Query.field("num_value_2").lessThan(3)));
        // Scan(<,>) | [MySimpleRecord] | Or([num_value_2 GREATER_THAN 2, num_value_2 LESS_THAN 4])
        RecordQueryPlan plan2 = createPlan("MySimpleRecord",
                Query.or(
                        Query.field("num_value_2").greaterThan(2),
                        Query.field("num_value_2").lessThan(4)));

        assertEquals(-1920816044, plan1.planHash(PlanHashable.PlanHashKind.LEGACY));
        assertEquals(-1920816012, plan2.planHash(PlanHashable.PlanHashKind.LEGACY));

        assertEquals(255607747, plan1.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(255607779, plan2.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));

        assertEquals(-768325757, plan1.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        assertEquals(-768325757, plan2.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
    }


    @Test
    void testNotEqualsFilter() {
        // Scan(<,>) | [MySimpleRecord] | Not(num_value_2 EQUALS 1)
        RecordQueryPlan plan1 = createPlan("MySimpleRecord", Query.not(Query.field("num_value_2").equalsValue(1)));
        // Scan(<,>) | [MySimpleRecord] | Not(num_value_2 EQUALS 2)
        RecordQueryPlan plan2 = createPlan("MySimpleRecord", Query.not(Query.field("num_value_2").equalsValue(2)));
        // Scan(<,>) | [MySimpleRecord] | Not(num_value_2 EQUALS $3)
        RecordQueryPlan plan3 = createPlan("MySimpleRecord", Query.not(Query.field("num_value_2").equalsParameter("3")));

        assertEquals(913370524, plan1.planHash(PlanHashable.PlanHashKind.LEGACY));
        assertEquals(913370525, plan2.planHash(PlanHashable.PlanHashKind.LEGACY));
        assertEquals(913370574, plan3.planHash(PlanHashable.PlanHashKind.LEGACY));

        assertEquals(-557960826, plan1.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(-557960825, plan2.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(711179617, plan3.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));

        assertEquals(1542751229, plan1.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        assertEquals(1542751229, plan2.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        // Note that value and parameter hashes are not the same
        assertEquals(711179617, plan3.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
    }

    @Test
    void testNotGtFilter() {
        // Scan(<,>) | [MySimpleRecord] | Not(num_value_2 GREATER_THAN 1)
        RecordQueryPlan plan1 = createPlan("MySimpleRecord", Query.not(Query.field("num_value_2").greaterThan(1)));
        // Scan(<,>) | [MySimpleRecord] | Not(num_value_2 GREATER_THAN 2)
        RecordQueryPlan plan2 = createPlan("MySimpleRecord", Query.not(Query.field("num_value_2").greaterThan(2)));

        assertEquals(-167290685, plan1.planHash(PlanHashable.PlanHashKind.LEGACY));
        assertEquals(-167290684, plan2.planHash(PlanHashable.PlanHashKind.LEGACY));

        assertEquals(301280063, plan1.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(301280064, plan2.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));

        assertEquals(462090020, plan1.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        assertEquals(462090020, plan2.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
    }

    @Test
    void testRank() {
        // Scan(<,>) | [MySimpleRecord] | rank(Field { 'num_value_2' None} group 1) EQUALS 2
        RecordQueryPlan plan1 = createPlan("MySimpleRecord", Query.rank("num_value_2").equalsValue(2L));
        // Scan(<,>) | [MySimpleRecord] | rank(Field { 'num_value_2' None} group 1) EQUALS 3
        RecordQueryPlan plan2 = createPlan("MySimpleRecord", Query.rank("num_value_2").equalsValue(3L));

        assertEquals(-615528291, plan1.planHash(PlanHashable.PlanHashKind.LEGACY));
        assertEquals(-615528290, plan2.planHash(PlanHashable.PlanHashKind.LEGACY));

        assertEquals(-1963408696, plan1.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(-1963408695, plan2.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));

        assertEquals(137303358, plan1.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        assertEquals(137303358, plan2.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
    }

    @Test
    void testComplexQuery1g() {
        // Index(MySimpleRecord$str_value_indexed [[a],[a]]) ∩ Index(MySimpleRecord$num_value_3_indexed [[3],[3]])
        RecordQueryPlan plan1 = createPlan("MySimpleRecord",
                Query.and(
                        Query.field("str_value_indexed").equalsValue("a"),
                        Query.field("num_value_3_indexed").equalsValue(3)));
        // Index(MySimpleRecord$str_value_indexed [[b],[b]]) ∩ Index(MySimpleRecord$num_value_3_indexed [[3],[3]])
        RecordQueryPlan plan2 = createPlan("MySimpleRecord",
                Query.and(
                        Query.field("str_value_indexed").equalsValue("b"),
                        Query.field("num_value_3_indexed").equalsValue(3)));

        assertEquals(-2070415596, plan1.planHash(PlanHashable.PlanHashKind.LEGACY));
        assertEquals(-2070415565, plan2.planHash(PlanHashable.PlanHashKind.LEGACY));

        assertEquals(-1451287726, plan1.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(-563784045, plan2.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));

        assertEquals(1869819604, plan1.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        assertEquals(1869819604, plan2.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
    }

    @Test
    void testComplexQueryAndWithIncompatibleFilters() {
        // Index(MySimpleRecord$str_value_indexed {[e],[e]}) | num_value_3_indexed EQUALS 3
        RecordQueryPlan plan1 = createPlan("MySimpleRecord",
                Query.and(
                        Query.field("str_value_indexed").startsWith("e"),
                        Query.field("num_value_3_indexed").equalsValue(3)));
        // Index(MySimpleRecord$str_value_indexed {[f],[f]}) | num_value_3_indexed EQUALS 3
        RecordQueryPlan plan2 = createPlan("MySimpleRecord",
                Query.and(
                        Query.field("str_value_indexed").startsWith("f"),
                        Query.field("num_value_3_indexed").equalsValue(3)));

        assertEquals(746853985, plan1.planHash(PlanHashable.PlanHashKind.LEGACY));
        assertEquals(746853986, plan2.planHash(PlanHashable.PlanHashKind.LEGACY));

        assertEquals(312168193, plan1.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(312197984, plan2.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));

        assertEquals(361886841, plan1.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        assertEquals(361886841, plan2.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
    }

    @Test
    void intersectionVersusRange() {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, metaData -> {
                metaData.addIndex("MySimpleRecord", "num_value_2");
                metaData.removeIndex("MySimpleRecord$num_value_3_indexed");
                metaData.addIndex("MySimpleRecord", new Index("index_2_3", "num_value_2", "num_value_3_indexed"));
            });
        }
        // Index(index_2_3 [[1, 2],[1, 3]]) | And([str_value_indexed EQUALS q, num_value_unique EQUALS 0])
        RecordQueryPlan plan1 = createPlan("MySimpleRecord",
                Query.and(
                        Query.field("str_value_indexed").equalsValue("q"),
                        Query.field("num_value_unique").equalsValue(0),
                        Query.field("num_value_2").equalsValue(1),
                        Query.field("num_value_3_indexed").greaterThanOrEquals(2),
                        Query.field("num_value_3_indexed").lessThanOrEquals(3)));
        // Index(index_2_3 [[1, 2],[1, 3]]) | And([str_value_indexed EQUALS w, num_value_unique EQUALS 0])
        RecordQueryPlan plan2 = createPlan("MySimpleRecord",
                Query.and(
                        Query.field("str_value_indexed").equalsValue("w"),
                        Query.field("num_value_unique").equalsValue(0),
                        Query.field("num_value_2").equalsValue(1),
                        Query.field("num_value_3_indexed").greaterThanOrEquals(2),
                        Query.field("num_value_3_indexed").lessThanOrEquals(3)));
        // Index(index_2_3 [[1, 3],[1, 3]]) | And([str_value_indexed EQUALS w, num_value_unique EQUALS 0])
        RecordQueryPlan plan3 = createPlan("MySimpleRecord",
                Query.and(
                        Query.field("str_value_indexed").equalsValue("w"),
                        Query.field("num_value_unique").equalsValue(0),
                        Query.field("num_value_2").equalsValue(1),
                        Query.field("num_value_3_indexed").greaterThanOrEquals(3),
                        Query.field("num_value_3_indexed").lessThanOrEquals(3)));

        assertEquals(2043805138, plan1.planHash(PlanHashable.PlanHashKind.LEGACY));
        assertEquals(2043805324, plan2.planHash(PlanHashable.PlanHashKind.LEGACY));
        assertEquals(2043805325, plan3.planHash(PlanHashable.PlanHashKind.LEGACY));

        assertEquals(-1614739008, plan1.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(-1614738822, plan2.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(-1614709031, plan3.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));

        assertEquals(1716318889, plan1.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        assertEquals(1716318889, plan2.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        assertEquals(1716318889, plan3.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
    }

    @Test
    void sortedIntersectionBounded() {
        // Index(MySimpleRecord$num_value_3_indexed [[2],[2]]) | num_value_unique EQUALS 1
        RecordQueryPlan plan1 = createPlan("MySimpleRecord",
                Query.and(Query.field("num_value_unique").equalsValue(1),
                        Query.field("num_value_3_indexed").equalsValue(2)),
                field("num_value_3_indexed"));
        // Index(MySimpleRecord$num_value_3_indexed [[4],[4]]) | num_value_unique EQUALS 3
        RecordQueryPlan plan2 = createPlan("MySimpleRecord",
                Query.and(Query.field("num_value_unique").equalsValue(3),
                        Query.field("num_value_3_indexed").equalsValue(4)),
                field("num_value_3_indexed"));

        assertEquals(-312630550, plan1.planHash(PlanHashable.PlanHashKind.LEGACY));
        assertEquals(-312630546, plan2.planHash(PlanHashable.PlanHashKind.LEGACY));

        assertEquals(-1852179950, plan1.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(-1850332906, plan2.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));

        assertEquals(-1962118145, plan1.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        assertEquals(-1962118145, plan2.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
    }

    @Test
    void sortedIntersectionUnbound() {
        // Scan(<,>) | [MySimpleRecord] | And([num_value_unique EQUALS 1, num_value_3_indexed EQUALS 2])
        RecordQueryPlan plan1 = createPlan("MySimpleRecord",
                Query.and(Query.field("num_value_unique").equalsValue(1),
                        Query.field("num_value_3_indexed").equalsValue(2)),
                field("rec_no"));
        // Scan(<,>) | [MySimpleRecord] | And([num_value_unique EQUALS 3, num_value_3_indexed EQUALS 4])
        RecordQueryPlan plan2 = createPlan("MySimpleRecord",
                Query.and(Query.field("num_value_unique").equalsValue(3),
                        Query.field("num_value_3_indexed").equalsValue(4)),
                field("rec_no"));

        // Index(MySimpleRecord$num_value_unique [[1],[1]]) ∩ Index(MySimpleRecord$num_value_3_indexed [[2],[2]])
        // Index(MySimpleRecord$num_value_unique [[3],[3]]) ∩ Index(MySimpleRecord$num_value_3_indexed [[4],[4]])

        assertEquals(62126310, plan1.planHash(PlanHashable.PlanHashKind.LEGACY));
        assertEquals(62126374, plan2.planHash(PlanHashable.PlanHashKind.LEGACY));

        assertEquals(-252730, plan1.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(1832012934, plan2.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));

        assertEquals(-1644476089, plan1.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        assertEquals(-1644476089, plan2.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
    }

    @Test
    void collateNoIndex() {
        // Scan(<,>) | [MySimpleRecord] | collate_jre(Field { 'str_value_indexed' None}) EQUALS collate_jre(a)
        RecordQueryPlan plan1 = createPlan("MySimpleRecord",
                Query.keyExpression(function("collate_jre", field("str_value_indexed"))).equalsValue("a"),
                null,
                Collections.singletonList(field("str_value_indexed")));
        // Scan(<,>) | [MySimpleRecord] | collate_jre(Field { 'str_value_indexed' None}) EQUALS collate_jre(b)
        RecordQueryPlan plan2 = createPlan("MySimpleRecord",
                Query.keyExpression(function("collate_jre", field("str_value_indexed"))).equalsValue("b"),
                null,
                Collections.singletonList(field("str_value_indexed")));

        assertEquals(-1306680475, plan1.planHash(PlanHashable.PlanHashKind.LEGACY));
        assertEquals(-1305756954, plan2.planHash(PlanHashable.PlanHashKind.LEGACY));

        assertEquals(-1470616300, plan1.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(-1441987149, plan2.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));

        assertEquals(-597114576, plan1.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        assertEquals(-597114576, plan2.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
    }

    @Test
    void coveringIndex() {
        // Note how the name field needs to be repeated in the value because it can't be recovered from an index
        // entry after transformation to a collation key.
        final KeyExpression collateKey = function("collate_jre", concat(field("str_value_indexed"), value("da_DK")));
        final KeyExpression indexKey = keyWithValue(concat(collateKey, field("str_value_indexed")), 1);
        final RecordMetaDataHook hook = md -> {
            md.removeIndex("MySimpleRecord$str_value_indexed");
            md.addIndex("MySimpleRecord", "collated_name", indexKey);
        };
        runHook(hook);
        // Covering(Index(collated_name ([null],[[0, 82, 0, 0, 0, 0]])) -> [rec_no: KEY[1], str_value_indexed: VALUE[0]])
        RecordQueryPlan plan1 = createPlan("MySimpleRecord",
                Query.keyExpression(collateKey).lessThan("a"),
                null,
                Collections.singletonList(field("str_value_indexed")));
        // Covering(Index(collated_name ([null],[[0, 83, 0, 0, 0, 0]])) -> [rec_no: KEY[1], str_value_indexed: VALUE[0]])
        RecordQueryPlan plan2 = createPlan("MySimpleRecord",
                Query.keyExpression(collateKey).lessThan("b"),
                null,
                Collections.singletonList(field("str_value_indexed")));

        assertEquals(-1514481748, plan1.planHash(PlanHashable.PlanHashKind.LEGACY));
        assertEquals(-1513558227, plan2.planHash(PlanHashable.PlanHashKind.LEGACY));

        assertEquals(-63925269, plan1.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(1678885066, plan2.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));

        assertEquals(1770693081, plan1.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        assertEquals(1770693081, plan2.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
    }

    @Test
    void compareParameter() {
        final KeyExpression key = function("collate_jre", concat(field("str_value_indexed"), value("de_DE")));
        final RecordMetaDataHook hook = md -> {
            md.removeIndex("MySimpleRecord$str_value_indexed");
            md.addIndex("MySimpleRecord", "collated_name", key);
        };
        runHook(hook);
        // Index(collated_name [EQUALS collate_jre($name)])
        RecordQueryPlan plan1 = createPlan("MySimpleRecord",
                Query.keyExpression(key).equalsParameter("name"),
                null,
                Collections.singletonList(field("str_value_indexed")));
        // Index(collated_name [EQUALS collate_jre($no-name)])s
        RecordQueryPlan plan2 = createPlan("MySimpleRecord",
                Query.keyExpression(key).equalsParameter("no-name"),
                null,
                Collections.singletonList(field("str_value_indexed")));

        assertEquals(691427410, plan1.planHash(PlanHashable.PlanHashKind.LEGACY));
        assertEquals(-1542988578, plan2.planHash(PlanHashable.PlanHashKind.LEGACY));

        assertEquals(-1386104501, plan1.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(-1551493161, plan2.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));

        assertEquals(-1774125902, plan1.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        assertEquals(-1939514562, plan2.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
    }

    @Test
    void coveringSimple() {
        // Covering(Index(MySimpleRecord$num_value_unique ([990],>) -> [num_value_unique: KEY[0], rec_no: KEY[1]])
        RecordQueryPlan plan1 = createPlan("MySimpleRecord",
                Query.field("num_value_unique").greaterThan(990),
                field("num_value_unique"),
                Collections.singletonList(field("num_value_unique")));
        // Covering(Index(MySimpleRecord$num_value_unique ([7766],>) -> [num_value_unique: KEY[0], rec_no: KEY[1]])
        RecordQueryPlan plan2 = createPlan("MySimpleRecord",
                Query.field("num_value_unique").greaterThan(7766),
                field("num_value_unique"),
                Collections.singletonList(field("num_value_unique")));

        assertEquals(-158312359, plan1.planHash(PlanHashable.PlanHashKind.LEGACY));
        assertEquals(-158305583, plan2.planHash(PlanHashable.PlanHashKind.LEGACY));

        assertEquals(-1293351441, plan1.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(-1286839705, plan2.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));

        assertEquals(-1374755849, plan1.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        assertEquals(-1374755849, plan2.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
    }

    @Test
    void coveringSimpleInsufficient() {
        // Index(MySimpleRecord$num_value_unique ([990],>)
        RecordQueryPlan plan1 = createPlan("MySimpleRecord",
                Query.field("num_value_unique").greaterThan(990),
                field("num_value_unique"),
                Arrays.asList(field("num_value_unique"), field("num_value_3_indexed")));
        // Index(MySimpleRecord$num_value_unique ([7766],>)
        RecordQueryPlan plan2 = createPlan("MySimpleRecord",
                Query.field("num_value_unique").greaterThan(7766),
                field("num_value_unique"),
                Arrays.asList(field("num_value_unique"), field("num_value_3_indexed")));

        assertEquals(-158312359, plan1.planHash(PlanHashable.PlanHashKind.LEGACY));
        assertEquals(-158305583, plan2.planHash(PlanHashable.PlanHashKind.LEGACY));

        assertEquals(594363443, plan1.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(600875179, plan2.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));

        assertEquals(512959035, plan1.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        assertEquals(512959035, plan2.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
    }

    @Test
    void coveringWithAdditionalFilter() {
        RecordMetaDataHook hook = metaData -> {
            metaData.removeIndex("MySimpleRecord$num_value_3_indexed");
            metaData.addIndex("MySimpleRecord", new Index("multi_index", "num_value_3_indexed", "num_value_2"));
        };
        runHook(hook);

        // Covering(Index(multi_index ([null],[1])) -> [num_value_2: KEY[1], num_value_3_indexed: KEY[0], rec_no: KEY[2]]) | num_value_2 LESS_THAN 2
        RecordQueryPlan plan1 = createPlan("MySimpleRecord",
                Query.and(
                        Query.field("num_value_3_indexed").lessThan(1),
                        Query.field("num_value_2").lessThan(2)),
                null,
                Collections.singletonList(field("num_value_3_indexed")));
        // Covering(Index(multi_index ([null],[3])) -> [num_value_2: KEY[1], num_value_3_indexed: KEY[0], rec_no: KEY[2]]) | num_value_2 LESS_THAN 4
        RecordQueryPlan plan2 = createPlan("MySimpleRecord",
                Query.and(
                        Query.field("num_value_3_indexed").lessThan(3),
                        Query.field("num_value_2").lessThan(4)),
                null,
                Collections.singletonList(field("num_value_3_indexed")));

        assertEquals(-1374002128, plan1.planHash(PlanHashable.PlanHashKind.LEGACY));
        assertEquals(-1374002124, plan2.planHash(PlanHashable.PlanHashKind.LEGACY));

        assertEquals(1359983418, plan1.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(1360043002, plan2.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));

        assertEquals(-1492450855, plan1.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        assertEquals(-1492450855, plan2.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
    }

    @Test
    void testMultiRecordTypeIndexScan() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openUnionRecordStore(context);
            commit(context);
        }

        // Index(partial_versions [[7],[7]])
        RecordQueryPlan plan1 = createPlan(
                Arrays.asList("MySimpleRecord", "MySimpleRecord2"),
                Query.field("etag").equalsValue(7),
                null, null);
        // Index(partial_versions [[8],[8]])
        RecordQueryPlan plan2 = createPlan(
                Arrays.asList("MySimpleRecord", "MySimpleRecord2"),
                Query.field("etag").equalsValue(8),
                null, null);

        assertEquals(-501898489, plan1.planHash(PlanHashable.PlanHashKind.LEGACY));
        assertEquals(-501898488, plan2.planHash(PlanHashable.PlanHashKind.LEGACY));

        assertEquals(-1416119651, plan1.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(-1416089860, plan2.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));

        assertEquals(-1071937908, plan1.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        assertEquals(-1071937908, plan2.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
    }

    @Test
    void testInQueryNoIndex() {
        // Scan(<,>) | [MySimpleRecord] | num_value_2 IN [0, 2]
        RecordQueryPlan plan1 = createPlan("MySimpleRecord",
                Query.field("num_value_2").in(asList(0, 2)),
                null, null);
        // Scan(<,>) | [MySimpleRecord] | num_value_2 IN [1, 3]
        RecordQueryPlan plan2 = createPlan("MySimpleRecord",
                Query.field("num_value_2").in(asList(1, 3)),
                null, null);

        assertEquals(-1139367278, plan1.planHash(PlanHashable.PlanHashKind.LEGACY));
        assertEquals(-1139367246, plan2.planHash(PlanHashable.PlanHashKind.LEGACY));

        assertEquals(-1848699555, plan1.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(-1848698563, plan2.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));

        assertEquals(-1636171932, plan1.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        assertEquals(-1636171932, plan2.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
    }

    @Test
    void testInQueryNoIndexWithParameter() {
        // Scan(<,>) | [MySimpleRecord] | num_value_2 IN $valuesThree
        RecordQueryPlan plan1 = createPlan("MySimpleRecord",
                Query.field("num_value_2").in("valuesThree"),   // num_value_2 is i%3
                null, null);
        // Scan(<,>) | [MySimpleRecord] | num_value_2 IN $valuesFour
        RecordQueryPlan plan2 = createPlan("MySimpleRecord",
                Query.field("num_value_2").in("valuesFour"),   // num_value_2 is i%3
                null, null);

        assertEquals(-1677754212, plan1.planHash(PlanHashable.PlanHashKind.LEGACY));
        assertEquals(920993896, plan2.planHash(PlanHashable.PlanHashKind.LEGACY));

        assertEquals(-134228922, plan1.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(-1830448110, plan2.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));

        assertEquals(-134228922, plan1.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        assertEquals(-1830448110, plan2.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
    }

    @Test
    void testInQueryIndex() {
        // Index(MySimpleRecord$num_value_3_indexed [EQUALS $__in_num_value_3_indexed__0]) WHERE __in_num_value_3_indexed__0 IN [1, 2, 3, 4]
        RecordQueryPlan plan1 = createPlan("MySimpleRecord",
                Query.field("num_value_3_indexed").in(asList(1, 2, 3, 4)),
                null, null);
        // Index(MySimpleRecord$num_value_3_indexed [EQUALS $__in_num_value_3_indexed__0]) WHERE __in_num_value_3_indexed__0 IN [5, 6]
        RecordQueryPlan plan2 = createPlan("MySimpleRecord",
                Query.field("num_value_3_indexed").in(asList(5, 6)),
                null, null);

        assertEquals(-2003135797, plan1.planHash(PlanHashable.PlanHashKind.LEGACY));
        assertEquals(-2004090006, plan2.planHash(PlanHashable.PlanHashKind.LEGACY));

        assertEquals(572156526, plan1.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(571202317, plan2.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));

        assertEquals(571201165, plan1.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        assertEquals(571201165, plan2.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
    }

    @Test
    void testNotInQuery() {
        // Scan(<,>) | [MySimpleRecord] | Not(num_value_2 IN [0, 2])
        RecordQueryPlan plan1 = createPlan("MySimpleRecord",
                Query.not(Query.field("num_value_2").in(asList(0, 2))),
                null, null);
        // Scan(<,>) | [MySimpleRecord] | Not(num_value_2 IN [1, 3])
        RecordQueryPlan plan2 = createPlan("MySimpleRecord",
                Query.not(Query.field("num_value_2").in(asList(1, 3))),
                null, null);

        assertEquals(-1139367277, plan1.planHash(PlanHashable.PlanHashKind.LEGACY));
        assertEquals(-1139367245, plan2.planHash(PlanHashable.PlanHashKind.LEGACY));

        assertEquals(-754248901, plan1.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(-754247909, plan2.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));

        assertEquals(-541721278, plan1.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        assertEquals(-541721278, plan2.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
    }

    @Test
    void testFullTextCovering() {
        final List<TestRecordsTextProto.SimpleDocument> documents = TextIndexTestUtils.toSimpleDocuments(Arrays.asList(
                TextSamples.ANGSTROM,
                TextSamples.AETHELRED,
                TextSamples.ROMEO_AND_JULIET_PROLOGUE,
                TextSamples.FRENCH
        ));

        try (FDBRecordContext context = openContext()) {
            setupTextStore(context);
            documents.forEach(recordStore::saveRecord);
            commit(context);
        }

        // TextIndex(SimpleDocument$text null, TEXT_CONTAINS_ALL [civil], null)
        RecordQueryPlan plan1 = createPlan("SimpleDocument",
                Query.field("text").text().contains("civil"),
                null, null);
        // TextIndex(SimpleDocument$text null, TEXT_CONTAINS_ALL [duty], null)
        RecordQueryPlan plan2 = createPlan("SimpleDocument",
                Query.field("text").text().contains("duty"),
                null, null);

        assertEquals(1902149160, plan1.planHash(PlanHashable.PlanHashKind.LEGACY));
        assertEquals(-233179765, plan2.planHash(PlanHashable.PlanHashKind.LEGACY));

        assertEquals(455456119, plan1.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(-1315231116, plan2.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));

        assertEquals(-1003640685, plan1.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        assertEquals(-1003640685, plan2.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
    }

    @Test
    void testTextWithcontainsAll() {
        final List<TestRecordsTextProto.SimpleDocument> documents = TextIndexTestUtils.toSimpleDocuments(Arrays.asList(
                TextSamples.ANGSTROM,
                TextSamples.AETHELRED,
                TextSamples.ROMEO_AND_JULIET_PROLOGUE,
                TextSamples.FRENCH
        ));

        try (FDBRecordContext context = openContext()) {
            setupTextStore(context);
            documents.forEach(recordStore::saveRecord);
            commit(context);
        }

        // TextIndex(SimpleDocument$text null, TEXT_CONTAINS_ALL_WITHIN(5) civil, null)
        RecordQueryPlan plan1 = createPlan("SimpleDocument",
                Query.field("text").text().containsAll("civil", 5),
                null, null);
        // TextIndex(SimpleDocument$text null, TEXT_CONTAINS_ALL_WITHIN(1) duty, null)
        RecordQueryPlan plan2 = createPlan("SimpleDocument",
                Query.field("text").text().containsAll("duty", 1),
                null, null);

        assertEquals(1916296334, plan1.planHash(PlanHashable.PlanHashKind.LEGACY));
        assertEquals(145605255, plan2.planHash(PlanHashable.PlanHashKind.LEGACY));

        assertEquals(-1671500113, plan1.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(-728348714, plan2.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));

        assertEquals(-1370648865, plan1.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        assertEquals(-1370648865, plan2.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
    }

    @Test
    void testTextWithContainsAllPrefix() {
        final List<TestRecordsTextProto.SimpleDocument> documents = TextIndexTestUtils.toSimpleDocuments(Arrays.asList(
                TextSamples.ANGSTROM,
                TextSamples.AETHELRED,
                TextSamples.ROMEO_AND_JULIET_PROLOGUE,
                TextSamples.FRENCH
        ));

        try (FDBRecordContext context = openContext()) {
            setupTextStore(context);
            documents.forEach(recordStore::saveRecord);
            commit(context);
        }

        // TextIndex(SimpleDocument$text null, TEXT_CONTAINS_ALL_PREFIXES(strictly) civil, null) | text TEXT_CONTAINS_ALL_PREFIXES(strictly) civil | UnorderedPrimaryKeyDistinct()
        RecordQueryPlan plan1 = createPlan("SimpleDocument",
                Query.field("text").text().containsAllPrefixes("civil", true, 1L, 2.0),
                null, null);
        // TextIndex(SimpleDocument$text null, TEXT_CONTAINS_ALL_PREFIXES(strictly) duty, null) | text TEXT_CONTAINS_ALL_PREFIXES(strictly) duty | UnorderedPrimaryKeyDistinct()
        RecordQueryPlan plan2 = createPlan("SimpleDocument",
                Query.field("text").text().containsAllPrefixes("duty", true, 3L, 4.0),
                null, null);

        assertEquals(1801963075, plan1.planHash(PlanHashable.PlanHashKind.LEGACY));
        assertEquals(1749572285, plan2.planHash(PlanHashable.PlanHashKind.LEGACY));

        assertEquals(-792567239, plan1.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(1778850867, plan2.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));

        assertEquals(1153367887, plan1.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        assertEquals(1153367887, plan2.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
    }


    @SuppressWarnings("SameParameterValue")
    private RecordQueryPlan createPlan(String recordType, QueryComponent filter) {
        return createPlan(recordType, filter, null);
    }

    private RecordQueryPlan createPlan(String recordType, QueryComponent filter, KeyExpression sort) {
        return createPlan(recordType, filter, sort, null);
    }

    private RecordQueryPlan createPlan(String recordType, QueryComponent filter, KeyExpression sort, List<KeyExpression> requiredResults) {
        return createPlan(Collections.singletonList(recordType), filter, sort, requiredResults);
    }

    private RecordQueryPlan createPlan(List<String> recordTypes, QueryComponent filter, KeyExpression sort, List<KeyExpression> requiredResults) {
        RecordQuery.Builder builder = RecordQuery.newBuilder()
                .setRecordTypes(recordTypes)
                .setFilter(filter);
        if (sort != null) {
            builder.setSort(sort, false);
        }
        if (requiredResults != null) {
            builder.setRequiredResults(requiredResults);
        }
        return planner.plan(builder.build());
    }

    protected void runHook(RecordMetaDataHook hook) {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            commit(context);
        }
    }

    protected void setupTextStore(FDBRecordContext context) {
        setupTextStore(context, store -> {
        });
    }

    protected void setupTextStore(FDBRecordContext context, RecordMetaDataHook hook) {
        RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder().setRecords(TestRecordsTextProto.getDescriptor());
        metaDataBuilder.getRecordType(TextIndexTestUtils.COMPLEX_DOC).setPrimaryKey(concatenateFields("group", "doc_id"));
        hook.apply(metaDataBuilder);
        recordStore = getStoreBuilder(context, metaDataBuilder.getRecordMetaData())
                .setSerializer(TextIndexTestUtils.COMPRESSING_SERIALIZER)
                .uncheckedOpen();
        setupPlanner(null);
    }
}
