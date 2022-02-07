/*
 * IndexFunctionHelperTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2020 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.provider.foundationdb;

import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexAggregateFunction;
import com.apple.foundationdb.record.metadata.IndexAggregateFunctionCall;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.expressions.GroupingKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.query.IndexQueryabilityFilter;
import com.apple.test.Tags;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;
import static com.apple.foundationdb.record.metadata.Key.Expressions.concatenateFields;
import static com.apple.foundationdb.record.metadata.Key.Expressions.empty;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static com.apple.foundationdb.record.metadata.expressions.KeyExpression.FanType.FanOut;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests for {@link IndexFunctionHelper}.
 */
@Tag(Tags.RequiresFDB)
class IndexFunctionHelperTest extends FDBRecordStoreTestBase {

    @Test
    void groupSubKeysBasic() {
        final KeyExpression ungrouped = field("value").ungrouped();
        assertEquals(field("value"), IndexFunctionHelper.getGroupedKey(ungrouped));
        assertEquals(empty(), IndexFunctionHelper.getGroupingKey(ungrouped));

        final KeyExpression group = field("value").groupBy(field("group"));
        assertEquals(field("value"), IndexFunctionHelper.getGroupedKey(group));
        assertEquals(field("group"), IndexFunctionHelper.getGroupingKey(group));
    }

    @Test
    void groupSubKeysNested() {
        final KeyExpression wholeKey = concat(
                field("a", FanOut).nest(concatenateFields("b", "c")),
                field("d"),
                field("e", FanOut).nest(concatenateFields("f", "g")));
        final KeyExpression group0 = new GroupingKeyExpression(wholeKey, 0);
        assertEquals(wholeKey, IndexFunctionHelper.getGroupingKey(group0));
        assertEquals(empty(), IndexFunctionHelper.getGroupedKey(group0));
        final KeyExpression group1 = new GroupingKeyExpression(wholeKey, 1);
        assertEquals(concat(
                field("a", FanOut).nest(concatenateFields("b", "c")),
                field("d"),
                field("e", FanOut).nest(field("f"))),
                IndexFunctionHelper.getGroupingKey(group1));
        assertEquals(field("e", FanOut).nest(field("g")),
                IndexFunctionHelper.getGroupedKey(group1));
        final KeyExpression group2 = new GroupingKeyExpression(wholeKey, 2);
        assertEquals(concat(
                field("a", FanOut).nest(concatenateFields("b", "c")),
                field("d")),
                IndexFunctionHelper.getGroupingKey(group2));
        assertEquals(field("e", FanOut).nest(concatenateFields("f", "g")),
                IndexFunctionHelper.getGroupedKey(group2));
        final KeyExpression group3 = new GroupingKeyExpression(wholeKey, 3);
        assertEquals(field("a", FanOut).nest(concatenateFields("b", "c")),
                IndexFunctionHelper.getGroupingKey(group3));
        assertEquals(concat(
                field("d"),
                field("e", FanOut).nest(concatenateFields("f", "g"))),
                IndexFunctionHelper.getGroupedKey(group3));
        final KeyExpression group4 = new GroupingKeyExpression(wholeKey, 4);
        assertEquals(field("a", FanOut).nest(field("b")),
                IndexFunctionHelper.getGroupingKey(group4));
        assertEquals(concat(
                field("a", FanOut).nest(field("c")),
                field("d"),
                field("e", FanOut).nest(concatenateFields("f", "g"))),
                IndexFunctionHelper.getGroupedKey(group4));
    }

    @Test
    void groupingKeyEmpty() {
        final KeyExpression count = empty().groupBy(field("x")); // Like COUNT(*) GROUP BY x
        final KeyExpression sum = field("y").groupBy(field("x"));  // Like SUM(y) GROUP BY x
        assertEquals(IndexFunctionHelper.getGroupingKey(count), IndexFunctionHelper.getGroupingKey(sum));
    }

    @Test
    void filterIndexForBindAggregateFunctionCall() {
        final GroupingKeyExpression group = concat(field("str_value_indexed"), field("num_value_2")).group(1);
        RecordMetaDataHook hook = metaData -> {
            metaData.addIndex("MySimpleRecord", new Index("filtered_sum_value2",
                    group,
                    Index.EMPTY_VALUE, IndexTypes.SUM, Map.of()));
        };

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            recordStore.deleteAllRecords();
            final IndexAggregateFunctionCall indexAggregateFunctionCall =
                    new IndexAggregateFunctionCall("sum", group);
            final Optional<IndexAggregateFunction> expected = Optional.of(new IndexAggregateFunction("sum", group, "filtered_sum_value2"));
            assertEquals(expected,
                    IndexFunctionHelper.bindAggregateFunctionCall(recordStore, indexAggregateFunctionCall, List.of("MySimpleRecord"),
                            IndexQueryabilityFilter.TRUE));
            assertEquals(Optional.empty(),
                    IndexFunctionHelper.bindAggregateFunctionCall(recordStore, indexAggregateFunctionCall, List.of("MySimpleRecord"),
                            IndexQueryabilityFilter.FALSE));
            commit(context);
        }
    }

    @Test
    void filterIndexForBindAggregateFunction() {
        final GroupingKeyExpression group = concat(field("str_value_indexed"), field("num_value_2")).group(1);
        RecordMetaDataHook hook = metaData -> {
            metaData.addIndex("MySimpleRecord", new Index("filtered_sum_value2",
                    group,
                    Index.EMPTY_VALUE, IndexTypes.SUM, Map.of()));
        };

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            recordStore.deleteAllRecords();
            final IndexAggregateFunction indexAggregateFunction =
                    new IndexAggregateFunction("sum", group, null);
            final Optional<IndexAggregateFunction> expected = Optional.of(new IndexAggregateFunction("sum", group, "filtered_sum_value2"));
            assertEquals(expected,
                    IndexFunctionHelper.bindAggregateFunction(recordStore, indexAggregateFunction, List.of("MySimpleRecord"),
                            IndexQueryabilityFilter.TRUE));
            assertEquals(Optional.empty(),
                    IndexFunctionHelper.bindAggregateFunction(recordStore, indexAggregateFunction, List.of("MySimpleRecord"),
                            IndexQueryabilityFilter.FALSE));
            commit(context);
        }
    }
}
