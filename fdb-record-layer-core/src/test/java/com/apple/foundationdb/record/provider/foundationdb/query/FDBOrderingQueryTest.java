/*
 * FDBOrderingQueryTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2024 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.record.TestHelpers;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.expressions.Field;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.record.query.expressions.QueryComponent;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryUnionOnKeyExpressionPlan;
import com.apple.test.Tags;
import com.google.common.collect.Lists;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static com.apple.foundationdb.record.metadata.Key.Expressions.function;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.coveringIndexPlan;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.indexName;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.indexPlan;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.indexPlanOf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests related to planning and executing queries with non-standard ordering.
 */
@Tag(Tags.RequiresFDB)
class FDBOrderingQueryTest extends FDBRecordStoreQueryTestBase {

    protected void saveRecord(FDBRecordContext context, long recNo, int num, String str) {
        TestRecords1Proto.MySimpleRecord.Builder record = TestRecords1Proto.MySimpleRecord.newBuilder()
                .setRecNo(recNo)
                .setNumValue2(num);
        if (str != null) {
            record.setStrValueIndexed(str);
        }
        recordStore.saveRecord(record.build());
    }

    protected void loadRecords(RecordMetaDataHook hook) throws Exception {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            saveRecord(context, 1, 100, "something");
            saveRecord(context, 2, 101, null);
            saveRecord(context, 3, 101, "a");
            saveRecord(context, 4, 101, "ab");
            saveRecord(context, 5, 101, "b");
            saveRecord(context, 6, 102, "nothing");
            commit(context);
        }
    }

    protected KeyExpression fieldOrderingKey(@Nullable String orderFunction) {
        KeyExpression strKey = field("str_value_indexed");
        if (orderFunction == null) {
            return strKey;
        } else {
            return function(orderFunction, strKey);
        }
    }

    protected KeyExpression fullOrderingKey(@Nullable String orderFunction) {
        return concat(field("num_value_2"), fieldOrderingKey(orderFunction));
    }

    protected RecordMetaDataHook noIndexHook() {
        return md -> {
            md.removeIndex("MySimpleRecord$str_value_indexed");
        };
    }

    protected RecordMetaDataHook indexHook(@Nullable String orderFunction) {
        final KeyExpression key = fullOrderingKey(orderFunction);
        return md -> {
            md.removeIndex("MySimpleRecord$str_value_indexed");
            md.addIndex("MySimpleRecord", "ordered_str_value", key);
        };
    }

    protected List<Long> queryRecords(RecordQuery query, RecordMetaDataHook hook, boolean hasIndex) throws Exception {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            if (!hasIndex) {
                planner.setConfiguration(planner.getConfiguration().asBuilder().setAllowNonIndexSort(true).build());
            }
            final RecordQueryPlan plan = planner.planQuery(query).getPlan();
            if (hasIndex) {
                assertEquals(Set.of("ordered_str_value"), plan.getUsedIndexes());
            }
            final List<Long> records = plan.execute(recordStore)
                    .map(r -> r.getPrimaryKey().getLong(0))
                    .asList().join();
            if (hasIndex) {
                TestHelpers.assertDiscardedNone(context);
            }
            return records;
        }
    }

    private static @Nonnull Stream<String> orderFunctionsStream() {
        return Stream.of(null,
                "order_asc_nulls_first", "order_asc_nulls_last", "order_desc_nulls_last", "order_desc_nulls_first");
    }

    public static Stream<Arguments> testSortOnly() {
        return orderFunctionsStream()
                .flatMap(orderFunction -> Stream.of(false, true)
                        .map(withIndex -> Arguments.of(orderFunction, withIndex)));
    }

    @ParameterizedTest
    @MethodSource
    void testSortOnly(@Nullable String orderFunction, boolean withIndex) throws Exception {
        final RecordMetaDataHook hook = withIndex ? indexHook(orderFunction) : noIndexHook();
        loadRecords(hook);
        final RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setSort(fullOrderingKey(orderFunction))
                .build();
        final List<Long> actual = queryRecords(query, hook, withIndex);
        final boolean reversed = orderFunction != null && orderFunction.contains("_desc_");
        final boolean nullsFirst = orderFunction == null || orderFunction.contains("nulls_first");
        List<Long> expected = new ArrayList<>(6);
        if (nullsFirst != reversed) {
            expected.add(2L);
        }
        expected.addAll(List.of(3L, 4L, 5L));
        if (nullsFirst == reversed) {
            expected.add(2L);
        }
        if (reversed) {
            expected = Lists.reverse(expected);
        }
        expected.add(0, 1L);
        expected.add(6L);
        assertEquals(expected, actual);
    }

    public static Stream<Arguments> testSortAndFilter() {
        return orderFunctionsStream()
                .flatMap(orderFunction -> Stream.of(Comparisons.Type.LESS_THAN,  Comparisons.Type.LESS_THAN_OR_EQUALS,
                                Comparisons.Type.GREATER_THAN, Comparisons.Type.GREATER_THAN_OR_EQUALS)
                        .flatMap(comparisonType -> Stream.of(false, true)
                                .map(withIndex -> Arguments.of(orderFunction, comparisonType, withIndex))));
    }

    @ParameterizedTest
    @MethodSource
    void testSortAndFilter(@Nullable String orderFunction, Comparisons.Type comparisonType, boolean withIndex) throws Exception {
        final RecordMetaDataHook hook = withIndex ? indexHook(orderFunction) : noIndexHook();
        loadRecords(hook);
        QueryComponent comparison;
        List<Long> expected;
        final Field queryField = Query.field("str_value_indexed");
        switch (comparisonType) {
            case LESS_THAN:
                comparison = queryField.lessThan("b");
                expected = List.of(3L, 4L);
                break;
            case LESS_THAN_OR_EQUALS:
                comparison = queryField.lessThanOrEquals("b");
                expected = List.of(3L, 4L, 5L);
                break;
            case GREATER_THAN:
                comparison = queryField.greaterThan("a");
                expected = List.of(4L, 5L);
                break;
            case GREATER_THAN_OR_EQUALS:
                comparison = queryField.greaterThanOrEquals("a");
                expected = List.of(3L, 4L, 5L);
                break;
            default:
                throw new IllegalArgumentException("Unknown comparison type: " + comparisonType);
        }
        final RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setSort(fullOrderingKey(orderFunction))
                .setFilter(Query.and(Query.field("num_value_2").equalsValue(101), comparison))
                .build();
        final List<Long> actual = queryRecords(query, hook, withIndex);
        if (orderFunction != null && orderFunction.contains("_desc_")) {
            expected = Lists.reverse(expected);
        }
        assertEquals(expected, actual);
    }

    @Test
    void testUnionOrdered() throws Exception {
        final RecordMetaDataHook hook = indexHook("order_desc_nulls_last");
        loadRecords(hook);
        final RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setSort(fullOrderingKey("order_desc_nulls_last"))
                .setFilter(Query.or(
                                Query.field("num_value_2").equalsValue(100),
                                Query.field("num_value_2").equalsValue(101),
                                Query.field("num_value_2").equalsValue(102)))
                .build();
        final List<Long> expected = List.of(1L, 5L, 4L, 3L, 2L, 6L);
        final List<Long> actual = queryRecords(query, hook, true);
        assertEquals(expected, actual);
        assertTrue(planner.planQuery(query).getPlan() instanceof RecordQueryUnionOnKeyExpressionPlan);
    }

    @Test
    void testCoveringOrdered() throws Exception {
        final RecordMetaDataHook hook = indexHook("order_desc_nulls_last");
        loadRecords(hook);
        final RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setSort(fullOrderingKey("order_desc_nulls_last"))
                .setFilter(Query.and(Query.field("num_value_2").equalsValue(101), Query.field("str_value_indexed").greaterThan("a")))
                .setRequiredResults(List.of(field("str_value_indexed")))
                .build();
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            final RecordQueryPlan plan = planner.planQuery(query).getPlan();
            assertMatchesExactly(plan, coveringIndexPlan().where(indexPlanOf(indexPlan().where(indexName("ordered_str_value")))));
            final List<String> values = plan.execute(recordStore)
                    .map(r -> TestRecords1Proto.MySimpleRecord.newBuilder().mergeFrom(r.getRecord()).getStrValueIndexed())
                    .asList().join();
            assertEquals(List.of("b", "ab"), values);
        }
    }
}
