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
import com.apple.foundationdb.record.TestRecords6Proto;
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
public class QueryPlanHashTest extends FDBRecordStoreQueryTestBase {

    @BeforeEach
    public void setup() throws Exception {
        RecordMetaDataHook hook = complexQuerySetupHook();
        complexQuerySetup(hook);
    }

    @Test
    public void testSingleEqualsFilter() throws Exception {
        // Scan(<,>) | [MySimpleRecord] | num_value_2 EQUALS 1
        RecordQueryPlan plan1 = createPlan("MySimpleRecord", Query.field("num_value_2").equalsValue(1));
        // Scan(<,>) | [MySimpleRecord] | num_value_2 EQUALS 2
        RecordQueryPlan plan2 = createPlan("MySimpleRecord", Query.field("num_value_2").equalsValue(2));
        // Scan(<,>) | [MySimpleRecord] | num_value_2 EQUALS $3
        RecordQueryPlan plan3 = createPlan("MySimpleRecord", Query.field("num_value_2").equalsParameter("3"));

        assertEquals(913370523, plan1.planHash(PlanHashable.PlanHashKind.LEGACY));
        assertEquals(913370524, plan2.planHash(PlanHashable.PlanHashKind.LEGACY));
        assertEquals(913370573, plan3.planHash(PlanHashable.PlanHashKind.LEGACY));

        assertEquals(-1711011988, plan1.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(-1711011987, plan2.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(-441871545, plan3.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));

        assertEquals(389700067, plan1.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        assertEquals(389700067, plan2.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        // Note that value and parameter comparisons are not the same
        assertEquals(-1370475238, plan3.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
    }

    @Test
    public void testSingleGtFilter() throws Exception {
        // Scan(<,>) | [MySimpleRecord] | num_value_2 GREATER_THAN 1
        RecordQueryPlan plan1 = createPlan("MySimpleRecord", Query.field("num_value_2").greaterThan(1));
        // Scan(<,>) | [MySimpleRecord] | num_value_2 GREATER_THAN 2
        RecordQueryPlan plan2 = createPlan("MySimpleRecord", Query.field("num_value_2").greaterThan(2));

        assertEquals(-167290686, plan1.planHash(PlanHashable.PlanHashKind.LEGACY));
        assertEquals(-167290685, plan2.planHash(PlanHashable.PlanHashKind.LEGACY));

        assertEquals(-851771099, plan1.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(-851771098, plan2.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));

        assertEquals(-690961142, plan1.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        assertEquals(-690961142, plan2.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
    }

    @Test
    public void testSingleGteFilter() throws Exception {
        // Scan(<,>) | [MySimpleRecord] | num_value_2 GREATER_THAN_OR_EQUALS 1
        RecordQueryPlan plan1 = createPlan("MySimpleRecord", Query.field("num_value_2").greaterThanOrEquals(1));
        // Scan(<,>) | [MySimpleRecord] | num_value_2 GREATER_THAN_OR_EQUALS 2
        RecordQueryPlan plan2 = createPlan("MySimpleRecord", Query.field("num_value_2").greaterThanOrEquals(2));

        assertEquals(-544375458, plan1.planHash(PlanHashable.PlanHashKind.LEGACY));
        assertEquals(-544375457, plan2.planHash(PlanHashable.PlanHashKind.LEGACY));

        assertEquals(343502857, plan1.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(343502858, plan2.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));

        assertEquals(-1068045914, plan1.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        assertEquals(-1068045914, plan2.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
    }

    @Test
    public void testOrEqualsFilter() throws Exception {
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

        assertEquals(-1318794777, plan1.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(-1318794713, plan2.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(638993799, plan3.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));

        assertEquals(1479481542, plan1.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        assertEquals(1479481542, plan2.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        // Note that the value and the parameter hashes are different.
        assertEquals(988446630, plan3.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
    }

    @Test
    public void testAndGtFilter() throws Exception {
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

        assertEquals(-617281709, plan1.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(-617281677, plan2.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));

        assertEquals(-1641215213, plan1.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        assertEquals(-1641215213, plan2.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
    }

    @Test
    public void testOrGtFilter() throws Exception {
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

        assertEquals(197007239, plan1.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(197007271, plan2.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));

        assertEquals(-826926265, plan1.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        assertEquals(-826926265, plan2.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
    }


    @Test
    public void testNotEqualsFilter() throws Exception {
        // Scan(<,>) | [MySimpleRecord] | Not(num_value_2 EQUALS 1)
        RecordQueryPlan plan1 = createPlan("MySimpleRecord", Query.not(Query.field("num_value_2").equalsValue(1)));
        // Scan(<,>) | [MySimpleRecord] | Not(num_value_2 EQUALS 2)
        RecordQueryPlan plan2 = createPlan("MySimpleRecord", Query.not(Query.field("num_value_2").equalsValue(2)));
        // Scan(<,>) | [MySimpleRecord] | Not(num_value_2 EQUALS $3)
        RecordQueryPlan plan3 = createPlan("MySimpleRecord", Query.not(Query.field("num_value_2").equalsParameter("3")));

        assertEquals(913370524, plan1.planHash(PlanHashable.PlanHashKind.LEGACY));
        assertEquals(913370525, plan2.planHash(PlanHashable.PlanHashKind.LEGACY));
        assertEquals(913370574, plan3.planHash(PlanHashable.PlanHashKind.LEGACY));

        assertEquals(-616561334, plan1.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(-616561333, plan2.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(652579109, plan3.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));

        assertEquals(1484150721, plan1.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        assertEquals(1484150721, plan2.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        // Note that value and parameter hashes are not the same
        assertEquals(-276024584, plan3.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
    }

    @Test
    public void testNotGtFilter() throws Exception {
        // Scan(<,>) | [MySimpleRecord] | Not(num_value_2 GREATER_THAN 1)
        RecordQueryPlan plan1 = createPlan("MySimpleRecord", Query.not(Query.field("num_value_2").greaterThan(1)));
        // Scan(<,>) | [MySimpleRecord] | Not(num_value_2 GREATER_THAN 2)
        RecordQueryPlan plan2 = createPlan("MySimpleRecord", Query.not(Query.field("num_value_2").greaterThan(2)));

        assertEquals(-167290685, plan1.planHash(PlanHashable.PlanHashKind.LEGACY));
        assertEquals(-167290684, plan2.planHash(PlanHashable.PlanHashKind.LEGACY));

        assertEquals(242679555, plan1.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(242679556, plan2.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));

        assertEquals(403489512, plan1.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        assertEquals(403489512, plan2.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
    }

    @Test
    public void testRank() throws Exception {
        // Scan(<,>) | [MySimpleRecord] | rank(Field { 'num_value_2' None} group 1) EQUALS 2
        RecordQueryPlan plan1 = createPlan("MySimpleRecord", Query.rank("num_value_2").equalsValue(2L));
        // Scan(<,>) | [MySimpleRecord] | rank(Field { 'num_value_2' None} group 1) EQUALS 3
        RecordQueryPlan plan2 = createPlan("MySimpleRecord", Query.rank("num_value_2").equalsValue(3L));

        assertEquals(-615528291, plan1.planHash(PlanHashable.PlanHashKind.LEGACY));
        assertEquals(-615528290, plan2.planHash(PlanHashable.PlanHashKind.LEGACY));

        assertEquals(-2022009204, plan1.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(-2022009203, plan2.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));

        assertEquals(78702850, plan1.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        assertEquals(78702850, plan2.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
    }

    @Test
    public void testComplexQuery1g() throws Exception {
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

        assertEquals(888312260, plan1.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(916941411, plan2.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));

        assertEquals(441255426, plan1.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        assertEquals(441255426, plan2.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
    }

    @Test
    public void testComplexQueryAndWithIncompatibleFilters() throws Exception {
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

        assertEquals(1963701578, plan1.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(1963702539, plan2.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));

        assertEquals(-1820735396, plan1.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        assertEquals(-1820735396, plan2.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
    }

    @Test
    public void intersectionVersusRange() throws Exception {
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

        assertEquals(299464657, plan1.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(299464843, plan2.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(299465804, plan3.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));

        assertEquals(482524810, plan1.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        assertEquals(482524810, plan2.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        assertEquals(482524810, plan3.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
    }

    @Test
    public void sortedIntersectionBounded() throws Exception {
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

        assertEquals(1272732641, plan1.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(1272792225, plan2.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));

        assertEquals(-577191902, plan1.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        assertEquals(-577191902, plan2.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
    }

    @Test
    public void sortedIntersectionUnbound() throws Exception {
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

        assertEquals(575076824, plan1.planHash(PlanHashable.PlanHashKind.LEGACY));
        assertEquals(575076888, plan2.planHash(PlanHashable.PlanHashKind.LEGACY));

        assertEquals(-554014099, plan1.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(-554014035, plan2.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));

        assertEquals(-2050705076, plan1.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        assertEquals(-2050705076, plan2.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
    }

    @Test
    public void collateNoIndex() throws Exception {
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

        assertEquals(-1529216808, plan1.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(-1500587657, plan2.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));

        assertEquals(-655715084, plan1.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        assertEquals(-655715084, plan2.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
    }

    @Test
    public void coveringIndex() throws Exception {
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

        // TODO: This produces a different hash every time, even on the original test in FDBCollateQueryTest.

        //        assertEquals(-974878282, plan1.planHash(PlanHashable.PlanHashKind.STANDARD));
        //        assertEquals(607649358, plan2.planHash(PlanHashable.PlanHashKind.STANDARD));
        //
        //        assertEquals(2024130897, plan1.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        //        assertEquals(2024130897, plan2.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
    }

    @Test
    public void compareParameter() throws Exception {
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

        // TODO: This produces a different hash every time, even on the original test in FDBCollateQueryTest.

        //                assertEquals(-974878282, plan1.planHash(PlanHashable.PlanHashKind.STANDARD));
        //        assertEquals(607649358, plan2.planHash(PlanHashable.PlanHashKind.STANDARD));
        //
        //        assertEquals(2024130897, plan1.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        //        assertEquals(2024130897, plan2.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));

    }

    @Test
    public void coveringSimple() throws Exception {
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

        assertEquals(2010783390, plan1.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(2010993446, plan2.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));

        assertEquals(-1594073194, plan1.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        assertEquals(-1594073194, plan2.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
    }

    @Test
    public void coveringSimpleInsufficient() throws Exception {
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

        assertEquals(-396469022, plan1.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(-396258966, plan2.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));

        assertEquals(293641690, plan1.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        assertEquals(293641690, plan2.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
    }

    @Test
    public void coveringWithAdditionalFilter() throws Exception {
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

        assertEquals(1915366989, plan1.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(1915368913, plan2.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));

        assertEquals(-2052186470, plan1.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        assertEquals(-2052186470, plan2.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
    }

    @Test
    public void testMultiRecordTypeIndexScan() throws Exception {
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

        assertEquals(-1154059976, plan1.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(-1154059015, plan2.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));

        assertEquals(-1004410007, plan1.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        assertEquals(-1004410007, plan2.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
    }

    @Test
    public void testInQueryNoIndex() throws Exception {
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

        assertEquals(-1907300063, plan1.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(-1907299071, plan2.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));

        assertEquals(-1694772440, plan1.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        assertEquals(-1694772440, plan2.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
    }

    @Test
    public void testInQueryNoIndexWithParameter() throws Exception {
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

        assertEquals(-192829430, plan1.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(-1889048618, plan2.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));

        assertEquals(871680640, plan1.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        assertEquals(871680640, plan2.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
    }

    @Test
    public void testInQueryIndex() throws Exception {
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

        assertEquals(1112068357, plan1.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(1111114148, plan2.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));

        assertEquals(619086974, plan1.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        assertEquals(619086974, plan2.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
    }

    @Test
    public void testNotInQuery() throws Exception {
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

        assertEquals(-812849409, plan1.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(-812848417, plan2.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));

        assertEquals(-600321786, plan1.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        assertEquals(-600321786, plan2.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
    }

    @Test
    public void testFullTextCovering() throws Exception {
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
    public void testTextWithcontainsAll() throws Exception {
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
    public void testTextWithContainsAllPrefix() throws Exception {
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
        return planner.plan(builder.build()).getPlan();
    }

    protected void runHook(RecordMetaDataHook hook) throws Exception {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            commit(context);
        }
    }

    protected void setupTextStore(FDBRecordContext context) throws Exception {
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

    private void openDoublyRepeatedRecordStore(FDBRecordContext context) throws Exception {
        RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder().setRecords(TestRecords6Proto.getDescriptor());
        metaDataBuilder.addUniversalIndex(COUNT_INDEX);
        metaDataBuilder.addIndex("MyRepeatedRecord", "rep_strings", concat(field("s1", KeyExpression.FanType.Concatenate), field("s2", KeyExpression.FanType.Concatenate)));
        metaDataBuilder.addIndex("MyRepeatedRecord", "s1$concat", field("s1", KeyExpression.FanType.Concatenate));
        createOrOpenRecordStore(context, metaDataBuilder.getRecordMetaData());
    }

}
