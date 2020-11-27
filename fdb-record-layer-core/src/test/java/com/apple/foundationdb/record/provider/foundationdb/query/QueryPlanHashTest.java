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
        RecordQueryPlan plan1 = createPlan("MySimpleRecord", Query.field("num_value_2").equalsValue(1));
        RecordQueryPlan plan2 = createPlan("MySimpleRecord", Query.field("num_value_2").equalsValue(2));
        RecordQueryPlan plan3 = createPlan("MySimpleRecord", Query.field("num_value_2").equalsParameter("3"));

        assertEquals(913370523, plan1.planHash(PlanHashable.PlanHashKind.LEGACY));
        assertEquals(913370524, plan2.planHash(PlanHashable.PlanHashKind.LEGACY));
        assertEquals(913370573, plan3.planHash(PlanHashable.PlanHashKind.LEGACY));

        assertEquals(-1711012019, plan1.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(-1711012018, plan2.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(-441871576, plan3.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));

        assertEquals(389700036, plan1.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        assertEquals(389700036, plan2.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        // Note that value and parameter comparisons are not the same
        assertEquals(-1370475269, plan3.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
    }

    @Test
    public void testSingleGtFilter() throws Exception {
        RecordQueryPlan plan1 = createPlan("MySimpleRecord", Query.field("num_value_2").greaterThan(1));
        RecordQueryPlan plan2 = createPlan("MySimpleRecord", Query.field("num_value_2").greaterThan(2));

        assertEquals(-167290686, plan1.planHash(PlanHashable.PlanHashKind.LEGACY));
        assertEquals(-167290685, plan2.planHash(PlanHashable.PlanHashKind.LEGACY));

        assertEquals(-851771130, plan1.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(-851771129, plan2.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));

        assertEquals(-690961173, plan1.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        assertEquals(-690961173, plan2.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
    }

    @Test
    public void testSingleGteFilter() throws Exception {
        RecordQueryPlan plan1 = createPlan("MySimpleRecord", Query.field("num_value_2").greaterThanOrEquals(1));
        RecordQueryPlan plan2 = createPlan("MySimpleRecord", Query.field("num_value_2").greaterThanOrEquals(2));

        assertEquals(-544375458, plan1.planHash(PlanHashable.PlanHashKind.LEGACY));
        assertEquals(-544375457, plan2.planHash(PlanHashable.PlanHashKind.LEGACY));

        assertEquals(343502826, plan1.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(343502827, plan2.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));

        assertEquals(-1068045945, plan1.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        assertEquals(-1068045945, plan2.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
    }

    @Test
    public void testOrEqualsFilter() throws Exception {
        RecordQueryPlan plan1 = createPlan("MySimpleRecord",
                Query.or(
                        Query.field("num_value_2").equalsValue(1),
                        Query.field("num_value_2").equalsValue(2)));
        RecordQueryPlan plan2 = createPlan("MySimpleRecord",
                Query.or(
                        Query.field("num_value_2").equalsValue(3),
                        Query.field("num_value_2").equalsValue(4)));
        RecordQueryPlan plan3 = createPlan("MySimpleRecord",
                Query.or(
                        Query.field("num_value_2").equalsParameter("5"),
                        Query.field("num_value_2").equalsParameter("6")));

        assertEquals(385591762, plan1.planHash(PlanHashable.PlanHashKind.LEGACY));
        assertEquals(385591826, plan2.planHash(PlanHashable.PlanHashKind.LEGACY));
        assertEquals(385593426, plan3.planHash(PlanHashable.PlanHashKind.LEGACY));

        assertEquals(-1318795769, plan1.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(-1318795705, plan2.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(638992807, plan3.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));

        assertEquals(1479480550, plan1.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        assertEquals(1479480550, plan2.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        // Note that the value and the parameter hashes are different.
        assertEquals(988445638, plan3.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
    }

    @Test
    public void testAndGtFilter() throws Exception {
        RecordQueryPlan plan1 = createPlan("MySimpleRecord",
                Query.and(
                        Query.field("num_value_2").greaterThan(1),
                        Query.field("num_value_2").lessThan(3)));
        RecordQueryPlan plan2 = createPlan("MySimpleRecord",
                Query.and(
                        Query.field("num_value_2").greaterThan(2),
                        Query.field("num_value_2").lessThan(4)));

        assertEquals(-1920816044, plan1.planHash(PlanHashable.PlanHashKind.LEGACY));
        assertEquals(-1920816012, plan2.planHash(PlanHashable.PlanHashKind.LEGACY));

        assertEquals(-617282701, plan1.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(-617282669, plan2.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));

        assertEquals(-1641216205, plan1.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        assertEquals(-1641216205, plan2.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
    }

    @Test
    public void testOrGtFilter() throws Exception {
        RecordQueryPlan plan1 = createPlan("MySimpleRecord",
                Query.or(
                        Query.field("num_value_2").greaterThan(1),
                        Query.field("num_value_2").lessThan(3)));
        RecordQueryPlan plan2 = createPlan("MySimpleRecord",
                Query.or(
                        Query.field("num_value_2").greaterThan(2),
                        Query.field("num_value_2").lessThan(4)));

        assertEquals(-1920816044, plan1.planHash(PlanHashable.PlanHashKind.LEGACY));
        assertEquals(-1920816012, plan2.planHash(PlanHashable.PlanHashKind.LEGACY));

        assertEquals(197006247, plan1.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(197006279, plan2.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));

        assertEquals(-826927257, plan1.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        assertEquals(-826927257, plan2.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
    }


    @Test
    public void testNotEqualsFilter() throws Exception {
        RecordQueryPlan plan1 = createPlan("MySimpleRecord", Query.not(Query.field("num_value_2").equalsValue(1)));
        RecordQueryPlan plan2 = createPlan("MySimpleRecord", Query.not(Query.field("num_value_2").equalsValue(2)));
        RecordQueryPlan plan3 = createPlan("MySimpleRecord", Query.not(Query.field("num_value_2").equalsParameter("3")));

        assertEquals(913370524, plan1.planHash(PlanHashable.PlanHashKind.LEGACY));
        assertEquals(913370525, plan2.planHash(PlanHashable.PlanHashKind.LEGACY));
        assertEquals(913370574, plan3.planHash(PlanHashable.PlanHashKind.LEGACY));

        assertEquals(-616561365, plan1.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(-616561364, plan2.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(652579078, plan3.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));

        assertEquals(1484150690, plan1.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        assertEquals(1484150690, plan2.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        // Note that value and parameter hashes are not the same
        assertEquals(-276024615, plan3.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
    }

    @Test
    public void testNotGtFilter() throws Exception {
        RecordQueryPlan plan1 = createPlan("MySimpleRecord", Query.not(Query.field("num_value_2").greaterThan(1)));
        RecordQueryPlan plan2 = createPlan("MySimpleRecord", Query.not(Query.field("num_value_2").greaterThan(2)));

        assertEquals(-167290685, plan1.planHash(PlanHashable.PlanHashKind.LEGACY));
        assertEquals(-167290684, plan2.planHash(PlanHashable.PlanHashKind.LEGACY));

        assertEquals(242679524, plan1.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(242679525, plan2.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));

        assertEquals(403489481, plan1.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        assertEquals(403489481, plan2.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
    }

    @Test
    public void testRank() throws Exception {
        RecordQueryPlan plan1 = createPlan("MySimpleRecord", Query.rank("num_value_2").equalsValue(2L));
        RecordQueryPlan plan2 = createPlan("MySimpleRecord", Query.rank("num_value_2").equalsValue(3L));

        assertEquals(-615528291, plan1.planHash(PlanHashable.PlanHashKind.LEGACY));
        assertEquals(-615528290, plan2.planHash(PlanHashable.PlanHashKind.LEGACY));

        assertEquals(932173317, plan1.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(932173318, plan2.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));

        assertEquals(-1262081925, plan1.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        assertEquals(-1262081925, plan2.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
    }

    @Test
    public void testComplexQuery1g() throws Exception {
        RecordQueryPlan plan1 = createPlan("MySimpleRecord",
                Query.and(
                        Query.field("str_value_indexed").equalsValue("a"),
                        Query.field("num_value_3_indexed").equalsValue(3)));
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
        RecordQueryPlan plan1 = createPlan("MySimpleRecord",
                Query.and(
                        Query.field("str_value_indexed").startsWith("e"),
                        Query.field("num_value_3_indexed").equalsValue(3)));
        RecordQueryPlan plan2 = createPlan("MySimpleRecord",
                Query.and(
                        Query.field("str_value_indexed").startsWith("f"),
                        Query.field("num_value_3_indexed").equalsValue(3)));

        assertEquals(746853985, plan1.planHash(PlanHashable.PlanHashKind.LEGACY));
        assertEquals(746853986, plan2.planHash(PlanHashable.PlanHashKind.LEGACY));

        assertEquals(1963701547, plan1.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(1963702508, plan2.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));

        assertEquals(-1820735427, plan1.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        assertEquals(-1820735427, plan2.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
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
        RecordQueryPlan plan1 = createPlan("MySimpleRecord",
                Query.and(
                        Query.field("str_value_indexed").equalsValue("q"),
                        Query.field("num_value_unique").equalsValue(0),
                        Query.field("num_value_2").equalsValue(1),
                        Query.field("num_value_3_indexed").greaterThanOrEquals(2),
                        Query.field("num_value_3_indexed").lessThanOrEquals(3)));
        RecordQueryPlan plan2 = createPlan("MySimpleRecord",
                Query.and(
                        Query.field("str_value_indexed").equalsValue("w"),
                        Query.field("num_value_unique").equalsValue(0),
                        Query.field("num_value_2").equalsValue(1),
                        Query.field("num_value_3_indexed").greaterThanOrEquals(2),
                        Query.field("num_value_3_indexed").lessThanOrEquals(3)));
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

        assertEquals(299463665, plan1.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(299463851, plan2.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(299464812, plan3.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));

        assertEquals(482523818, plan1.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        assertEquals(482523818, plan2.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        assertEquals(482523818, plan3.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
    }

    @Test
    public void sortedIntersectionBounded() throws Exception {
        RecordQueryPlan plan1 = createPlan("MySimpleRecord",
                Query.and(Query.field("num_value_unique").equalsValue(1),
                        Query.field("num_value_3_indexed").equalsValue(2)),
                field("num_value_3_indexed"));
        RecordQueryPlan plan2 = createPlan("MySimpleRecord",
                Query.and(Query.field("num_value_unique").equalsValue(3),
                        Query.field("num_value_3_indexed").equalsValue(4)),
                field("num_value_3_indexed"));

        assertEquals(-312630550, plan1.planHash(PlanHashable.PlanHashKind.LEGACY));
        assertEquals(-312630546, plan2.planHash(PlanHashable.PlanHashKind.LEGACY));

        assertEquals(1272732610, plan1.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(1272792194, plan2.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));

        assertEquals(-577191933, plan1.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        assertEquals(-577191933, plan2.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
    }

    @Test
    public void sortedIntersectionUnbound() throws Exception {
        RecordQueryPlan plan1 = createPlan("MySimpleRecord",
                Query.and(Query.field("num_value_unique").equalsValue(1),
                        Query.field("num_value_3_indexed").equalsValue(2)),
                field("rec_no"));
        RecordQueryPlan plan2 = createPlan("MySimpleRecord",
                Query.and(Query.field("num_value_unique").equalsValue(3),
                        Query.field("num_value_3_indexed").equalsValue(4)),
                field("rec_no"));

        assertEquals(575076824, plan1.planHash(PlanHashable.PlanHashKind.LEGACY));
        assertEquals(575076888, plan2.planHash(PlanHashable.PlanHashKind.LEGACY));

        assertEquals(-554015091, plan1.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(-554015027, plan2.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));

        assertEquals(-2050706068, plan1.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        assertEquals(-2050706068, plan2.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
    }

    @Test
    public void collateNoIndex() throws Exception {
        RecordQueryPlan plan1 = createPlan("MySimpleRecord",
                Query.keyExpression(function("collate_jre", field("str_value_indexed"))).equalsValue("a"),
                null,
                Collections.singletonList(field("str_value_indexed")));
        RecordQueryPlan plan2 = createPlan("MySimpleRecord",
                Query.keyExpression(function("collate_jre", field("str_value_indexed"))).equalsValue("b"),
                null,
                Collections.singletonList(field("str_value_indexed")));

        assertEquals(-1306680475, plan1.planHash(PlanHashable.PlanHashKind.LEGACY));
        assertEquals(-1305756954, plan2.planHash(PlanHashable.PlanHashKind.LEGACY));

        assertEquals(890909720, plan1.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(919538871, plan2.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));

        assertEquals(1764411444, plan1.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        assertEquals(1764411444, plan2.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
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
        RecordQueryPlan plan1 = createPlan("MySimpleRecord",
                Query.keyExpression(collateKey).lessThan("a"),
                null,
                Collections.singletonList(field("str_value_indexed")));
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
        RecordQueryPlan plan1 = createPlan("MySimpleRecord",
                Query.keyExpression(key).equalsParameter("name"),
                null,
                Collections.singletonList(field("str_value_indexed")));
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
        RecordQueryPlan plan1 = createPlan("MySimpleRecord",
                Query.field("num_value_unique").greaterThan(990),
                field("num_value_unique"),
                Collections.singletonList(field("num_value_unique")));
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
        RecordQueryPlan plan1 = createPlan("MySimpleRecord",
                Query.field("num_value_unique").greaterThan(990),
                field("num_value_unique"),
                Arrays.asList(field("num_value_unique"), field("num_value_3_indexed")));
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

        RecordQueryPlan plan1 = createPlan("MySimpleRecord",
                Query.and(
                        Query.field("num_value_3_indexed").lessThan(1),
                        Query.field("num_value_2").lessThan(2)),
                null,
                Collections.singletonList(field("num_value_3_indexed")));
        RecordQueryPlan plan2 = createPlan("MySimpleRecord",
                Query.and(
                        Query.field("num_value_3_indexed").lessThan(3),
                        Query.field("num_value_2").lessThan(4)),
                null,
                Collections.singletonList(field("num_value_3_indexed")));

        assertEquals(-1374002128, plan1.planHash(PlanHashable.PlanHashKind.LEGACY));
        assertEquals(-1374002124, plan2.planHash(PlanHashable.PlanHashKind.LEGACY));

        assertEquals(1915366958, plan1.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(1915368882, plan2.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));

        assertEquals(-2052186501, plan1.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        assertEquals(-2052186501, plan2.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
    }

    @Test
    public void testMultiRecordTypeIndexScan() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openUnionRecordStore(context);
            commit(context);
        }

        RecordQueryPlan plan1 = createPlan(
                Arrays.asList("MySimpleRecord", "MySimpleRecord2"),
                Query.field("etag").equalsValue(7),
                null, null);
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
        RecordQueryPlan plan1 = createPlan("MySimpleRecord",
                Query.field("num_value_2").in(asList(0, 2)),
                null, null);
        RecordQueryPlan plan2 = createPlan("MySimpleRecord",
                Query.field("num_value_2").in(asList(1, 3)),
                null, null);

        assertEquals(-1139367278, plan1.planHash(PlanHashable.PlanHashKind.LEGACY));
        assertEquals(-1139367246, plan2.planHash(PlanHashable.PlanHashKind.LEGACY));

        assertEquals(-1907300094, plan1.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(-1907299102, plan2.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));

        assertEquals(-1694772471, plan1.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        assertEquals(-1694772471, plan2.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
    }

    @Test
    public void testInQueryNoIndexWithParameter() throws Exception {
        RecordQueryPlan plan1 = createPlan("MySimpleRecord",
                Query.field("num_value_2").in("valuesThree"),   // num_value_2 is i%3
                null, null);
        RecordQueryPlan plan2 = createPlan("MySimpleRecord",
                Query.field("num_value_2").in("valuesFour"),   // num_value_2 is i%3
                null, null);

        assertEquals(-1677754212, plan1.planHash(PlanHashable.PlanHashKind.LEGACY));
        assertEquals(920993896, plan2.planHash(PlanHashable.PlanHashKind.LEGACY));

        assertEquals(-192829461, plan1.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(-1889048649, plan2.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));

        assertEquals(871680609, plan1.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        assertEquals(871680609, plan2.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
    }

    @Test
    public void testInQueryIndex() throws Exception {
        RecordQueryPlan plan1 = createPlan("MySimpleRecord",
                Query.field("num_value_3_indexed").in(asList(1, 2, 3, 4)),
                null, null);
        RecordQueryPlan plan2 = createPlan("MySimpleRecord",
                Query.field("num_value_3_indexed").in(asList(5, 6)),
                null, null);

        assertEquals(-2003135797, plan1.planHash(PlanHashable.PlanHashKind.LEGACY));
        assertEquals(-2004090006, plan2.planHash(PlanHashable.PlanHashKind.LEGACY));

        assertEquals(390588147, plan1.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(389633938, plan2.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));

        assertEquals(689433654, plan1.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        assertEquals(689433654, plan2.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
    }

    @Test
    public void testNotInQuery() throws Exception {
        RecordQueryPlan plan1 = createPlan("MySimpleRecord",
                Query.not(Query.field("num_value_2").in(asList(0, 2))),
                null, null);
        RecordQueryPlan plan2 = createPlan("MySimpleRecord",
                Query.not(Query.field("num_value_2").in(asList(1, 3))),
                null, null);

        assertEquals(-1139367277, plan1.planHash(PlanHashable.PlanHashKind.LEGACY));
        assertEquals(-1139367245, plan2.planHash(PlanHashable.PlanHashKind.LEGACY));

        assertEquals(-812849440, plan1.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(-812848448, plan2.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));

        assertEquals(-600321817, plan1.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        assertEquals(-600321817, plan2.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
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

        RecordQueryPlan plan1 = createPlan("SimpleDocument",
                Query.field("text").text().contains("civil"),
                null, null);
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

        RecordQueryPlan plan1 = createPlan("SimpleDocument",
                Query.field("text").text().containsAll("civil", 5),
                null, null);
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

        RecordQueryPlan plan1 = createPlan("SimpleDocument",
                Query.field("text").text().containsAllPrefixes("civil", true, 1L, 2.0),
                null, null);
        RecordQueryPlan plan2 = createPlan("SimpleDocument",
                Query.field("text").text().containsAllPrefixes("duty", true, 3L, 4.0),
                null, null);

        assertEquals(1801963075, plan1.planHash(PlanHashable.PlanHashKind.LEGACY));
        assertEquals(1749572285, plan2.planHash(PlanHashable.PlanHashKind.LEGACY));

        assertEquals(-792567270, plan1.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(1778850836, plan2.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));

        assertEquals(1153367856, plan1.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        assertEquals(1153367856, plan2.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
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
        return planner.plan(builder.build());
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
