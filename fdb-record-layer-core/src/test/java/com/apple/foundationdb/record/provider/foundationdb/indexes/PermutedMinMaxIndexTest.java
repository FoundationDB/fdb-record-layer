/*
 * PermutedMinMaxIndexTest.java
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

package com.apple.foundationdb.record.provider.foundationdb.indexes;

import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.FunctionNames;
import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.IsolationLevel;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexAggregateFunction;
import com.apple.foundationdb.record.metadata.IndexOptions;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.expressions.GroupingKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreTestBase;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.record.query.plan.RecordQueryPlanner;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.util.pair.NonnullPair;
import com.apple.foundationdb.record.util.pair.Pair;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.TupleHelpers;
import com.apple.test.BooleanSource;
import com.apple.test.Tags;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;


/**
 * Tests for permuted min / max type indexes.
 */
@Tag(Tags.RequiresFDB)
class PermutedMinMaxIndexTest extends FDBRecordStoreTestBase {

    protected static final String INDEX_NAME = "permuted";

    @Nonnull
    protected static RecordMetaDataHook hook(boolean min) {
        return hook(min, Key.Expressions.concatenateFields("str_value_indexed", "num_value_2", "num_value_3_indexed").group(1), 1);
    }

    @Nonnull
    protected static RecordMetaDataHook hook(boolean min, @Nonnull GroupingKeyExpression groupingKeyExpression, int permutedSize) {
        return md -> {
            md.addIndex("MySimpleRecord", new Index(INDEX_NAME,
                    groupingKeyExpression,
                    min ? IndexTypes.PERMUTED_MIN : IndexTypes.PERMUTED_MAX,
                    Collections.singletonMap(IndexOptions.PERMUTED_SIZE_OPTION, "" + permutedSize)));
        };
    }

    @Test
    void min() {
        final RecordMetaDataHook hook = hook(true);
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);

            saveRecord(1, "yes", 111, 100);
            saveRecord(2, "yes", 222, 200);
            saveRecord(99, "no", 66, 0);

            assertEquals(Arrays.asList(
                    Tuple.from(100, 111),
                    Tuple.from(200, 222)
            ), scanGroup(Tuple.from("yes"), false));

            commit(context);
        }
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);

            saveRecord(3, "yes", 111, 50);
            assertEquals(Arrays.asList(
                    Tuple.from(50, 111),
                    Tuple.from(200, 222)
            ), scanGroup(Tuple.from("yes"), false));

            saveRecord(3, "yes", 111, 150);
            assertEquals(Arrays.asList(
                    Tuple.from(100, 111),
                    Tuple.from(200, 222)
            ), scanGroup(Tuple.from("yes"), false));

            recordStore.deleteRecord(Tuple.from(1));
            assertEquals(Arrays.asList(
                    Tuple.from(150, 111),
                    Tuple.from(200, 222)
            ), scanGroup(Tuple.from("yes"), false));

            recordStore.deleteRecord(Tuple.from(3));
            assertEquals(Arrays.asList(
                    Tuple.from(200, 222)
            ), scanGroup(Tuple.from("yes"), false));
        }
    }

    @Test
    void max() {
        final RecordMetaDataHook hook = hook(false);
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);

            saveRecord(1, "yes", 111, 100);
            saveRecord(2, "yes", 222, 200);
            saveRecord(99, "no", 666, 0);

            assertEquals(Arrays.asList(
                    Tuple.from(200, 222),
                    Tuple.from(100, 111)
            ), scanGroup(Tuple.from("yes"), true));

            commit(context);
        }
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);

            saveRecord(3, "yes", 111, 250);
            assertEquals(Arrays.asList(
                    Tuple.from(250, 111),
                    Tuple.from(200, 222)
            ), scanGroup(Tuple.from("yes"), true));

            saveRecord(3, "yes", 111, 50);
            assertEquals(Arrays.asList(
                    Tuple.from(200, 222),
                    Tuple.from(100, 111)
            ), scanGroup(Tuple.from("yes"), true));

            recordStore.deleteRecord(Tuple.from(1));
            assertEquals(Arrays.asList(
                    Tuple.from(200, 222),
                    Tuple.from(50, 111)
            ), scanGroup(Tuple.from("yes"), true));

            recordStore.deleteRecord(Tuple.from(3));
            assertEquals(Arrays.asList(
                    Tuple.from(200, 222)
            ), scanGroup(Tuple.from("yes"), true));
        }
    }

    @Test
    void tie() {
        final RecordMetaDataHook hook = hook(false);
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);

            saveRecord(1, "yes", 111, 100);
            saveRecord(2, "yes", 111, 50);
            saveRecord(3, "yes", 111, 100);

            assertEquals(Arrays.asList(
                    Tuple.from(100, 111)
                    ),
                    scanGroup(Tuple.from("yes"), true));

            recordStore.deleteRecord(Tuple.from(1));
            assertEquals(Arrays.asList(
                    Tuple.from(100, 111)
                    ),
                    scanGroup(Tuple.from("yes"), true));

            recordStore.deleteRecord(Tuple.from(3));
            assertEquals(Arrays.asList(
                    Tuple.from(50, 111)
                    ),
                    scanGroup(Tuple.from("yes"), true));
        }
    }

    @ParameterizedTest(name = "repeatedGroupKey[min={0}]")
    @BooleanSource
    void repeatedGroupKey(boolean min) {
        // Index on:
        //    min/max(num_value_2) GROUP BY repeater, str_value_indexed ORDER BY repeater, min/max(num_value_2), str_value_indexed
        final RecordMetaDataHook hook = hook(min,
                Key.Expressions.field("num_value_2").groupBy(Key.Expressions.field("repeater", KeyExpression.FanType.FanOut), Key.Expressions.field("str_value_indexed")),
                1);

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);

            saveRecord(1, "yes", 3, 0, 1, 2, 3, 4, 5);
            saveRecord(2, "yes", 5, 0, 1, 3, 5);
            saveRecord(3, "yes", 7, 0, 2, 4);
            saveRecord(4, "yes", 9, 0, 5);
            saveRecord(5, "yes", 11, 0);
            saveRecord(6, "yes", -1, 0);

            saveRecord(11, "no", 9, 0, 1, 2, 3, 4, 5);
            saveRecord(12, "no", 7, 0, 1, 3, 5);
            saveRecord(13, "no", 5, 0, 2, 4);
            saveRecord(14, "no", 3, 0, 5);
            saveRecord(15, "no", 11, 0);
            saveRecord(16, "no", -1, 0);

            if (min) {
                assertThat(scanGroup(Tuple.from(0), false), empty());

                assertEquals(Arrays.asList(
                        Tuple.from(3, "yes"),
                        Tuple.from(7, "no")
                ), scanGroup(Tuple.from(1), false));

                assertEquals(Arrays.asList(
                        Tuple.from(3, "yes"),
                        Tuple.from(5, "no")
                ), scanGroup(Tuple.from(2), false));

                assertEquals(Arrays.asList(
                        Tuple.from(3, "yes"),
                        Tuple.from(7, "no")
                ), scanGroup(Tuple.from(3), false));

                assertEquals(Arrays.asList(
                        Tuple.from(3, "yes"),
                        Tuple.from(5, "no")
                ), scanGroup(Tuple.from(4), false));

                assertEquals(Arrays.asList(
                        Tuple.from(3, "no"),
                        Tuple.from(3, "yes")
                ), scanGroup(Tuple.from(5), false));

                assertThat(scanGroup(Tuple.from(6), false), empty());
            } else {
                assertThat(scanGroup(Tuple.from(0), false), empty());

                assertEquals(Arrays.asList(
                        Tuple.from(5, "yes"),
                        Tuple.from(9, "no")
                ), scanGroup(Tuple.from(1), false));

                assertEquals(Arrays.asList(
                        Tuple.from(7, "yes"),
                        Tuple.from(9, "no")
                ), scanGroup(Tuple.from(2), false));

                assertEquals(Arrays.asList(
                        Tuple.from(5, "yes"),
                        Tuple.from(9, "no")
                ), scanGroup(Tuple.from(3), false));

                assertEquals(Arrays.asList(
                        Tuple.from(7, "yes"),
                        Tuple.from(9, "no")
                ), scanGroup(Tuple.from(4), false));

                assertEquals(Arrays.asList(
                        Tuple.from(9, "no"),
                        Tuple.from(9, "yes")
                ), scanGroup(Tuple.from(5), false));

                assertThat(scanGroup(Tuple.from(6), false), empty());
            }

            recordStore.deleteRecord(Tuple.from(1));
            recordStore.deleteRecord(Tuple.from(11));

            if (min) {
                assertThat(scanGroup(Tuple.from(0), false), empty());

                assertEquals(Arrays.asList(
                        Tuple.from(5, "yes"),
                        Tuple.from(7, "no")
                ), scanGroup(Tuple.from(1), false));

                assertEquals(Arrays.asList(
                        Tuple.from(5, "no"),
                        Tuple.from(7, "yes")
                ), scanGroup(Tuple.from(2), false));

                assertEquals(Arrays.asList(
                        Tuple.from(5, "yes"),
                        Tuple.from(7, "no")
                ), scanGroup(Tuple.from(3), false));

                assertEquals(Arrays.asList(
                        Tuple.from(5, "no"),
                        Tuple.from(7, "yes")
                ), scanGroup(Tuple.from(4), false));

                assertEquals(Arrays.asList(
                        Tuple.from(3, "no"),
                        Tuple.from(5, "yes")
                ), scanGroup(Tuple.from(5), false));

                assertThat(scanGroup(Tuple.from(6), false), empty());
            } else {
                assertThat(scanGroup(Tuple.from(0), false), empty());

                assertEquals(Arrays.asList(
                        Tuple.from(5, "yes"),
                        Tuple.from(7, "no")
                ), scanGroup(Tuple.from(1), false));

                assertEquals(Arrays.asList(
                        Tuple.from(5, "no"),
                        Tuple.from(7, "yes")
                ), scanGroup(Tuple.from(2), false));

                assertEquals(Arrays.asList(
                        Tuple.from(5, "yes"),
                        Tuple.from(7, "no")
                ), scanGroup(Tuple.from(3), false));

                assertEquals(Arrays.asList(
                        Tuple.from(5, "no"),
                        Tuple.from(7, "yes")
                ), scanGroup(Tuple.from(4), false));

                assertEquals(Arrays.asList(
                        Tuple.from(7, "no"),
                        Tuple.from(9, "yes")
                ), scanGroup(Tuple.from(5), false));

                assertThat(scanGroup(Tuple.from(6), false), empty());
            }

            commit(context);
        }
    }

    @ParameterizedTest(name = "repeatedPermutedSuffix[min={0}]")
    @BooleanSource
    void repeatedPermutedSuffix(boolean min) {
        // Index on:
        //    min/max(num_value_2) GROUP BY str_value_indexed, repeater ORDER BY str_value_indexed, min/max(num_value_2), repeater
        final RecordMetaDataHook hook = hook(min,
                Key.Expressions.field("num_value_2").groupBy(Key.Expressions.field("str_value_indexed"), Key.Expressions.field("repeater", KeyExpression.FanType.FanOut)),
                1);

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);

            saveRecord(1, "yes", 3, 0, 1, 2, 3, 4, 5);
            saveRecord(2, "yes", 5, 0, 1, 3, 5);
            saveRecord(3, "yes", 7, 0, 2, 4);
            saveRecord(4, "yes", 9, 0, 5);
            saveRecord(5, "yes", 11, 0);
            saveRecord(6, "yes", -1, 0);

            saveRecord(11, "no", 9, 0, 1, 2, 3, 4, 5);
            saveRecord(12, "no", 7, 0, 1, 3, 5);
            saveRecord(13, "no", 5, 0, 2, 4);
            saveRecord(14, "no", 3, 0, 5);
            saveRecord(15, "no", 11, 0);
            saveRecord(16, "no", -1, 0);

            if (min) {
                assertEquals(Arrays.asList(
                        Tuple.from(3, 1),
                        Tuple.from(3, 2),
                        Tuple.from(3, 3),
                        Tuple.from(3, 4),
                        Tuple.from(3, 5)
                ), scanGroup(Tuple.from("yes"), false));

                assertEquals(Arrays.asList(
                        Tuple.from(3, 5),
                        Tuple.from(5, 2),
                        Tuple.from(5, 4),
                        Tuple.from(7, 1),
                        Tuple.from(7, 3)
                ), scanGroup(Tuple.from("no"), false));

            } else {
                assertEquals(Arrays.asList(
                        Tuple.from(5, 1),
                        Tuple.from(5, 3),
                        Tuple.from(7, 2),
                        Tuple.from(7, 4),
                        Tuple.from(9, 5)
                ), scanGroup(Tuple.from("yes"), false));

                assertEquals(Arrays.asList(
                        Tuple.from(9, 1),
                        Tuple.from(9, 2),
                        Tuple.from(9, 3),
                        Tuple.from(9, 4),
                        Tuple.from(9, 5)
                ), scanGroup(Tuple.from("no"), false));
            }

            recordStore.deleteRecord(Tuple.from(1));
            recordStore.deleteRecord(Tuple.from(11));

            if (min) {
                assertEquals(Arrays.asList(
                        Tuple.from(5, 1),
                        Tuple.from(5, 3),
                        Tuple.from(5, 5),
                        Tuple.from(7, 2),
                        Tuple.from(7, 4)
                ), scanGroup(Tuple.from("yes"), false));

                assertEquals(Arrays.asList(
                        Tuple.from(3, 5),
                        Tuple.from(5, 2),
                        Tuple.from(5, 4),
                        Tuple.from(7, 1),
                        Tuple.from(7, 3)
                ), scanGroup(Tuple.from("no"), false));

            } else {
                assertEquals(Arrays.asList(
                        Tuple.from(5, 1),
                        Tuple.from(5, 3),
                        Tuple.from(7, 2),
                        Tuple.from(7, 4),
                        Tuple.from(9, 5)
                ), scanGroup(Tuple.from("yes"), false));

                assertEquals(Arrays.asList(
                        Tuple.from(5, 2),
                        Tuple.from(5, 4),
                        Tuple.from(7, 1),
                        Tuple.from(7, 3),
                        Tuple.from(7, 5)
                ), scanGroup(Tuple.from("no"), false));
            }

            commit(context);
        }
    }

    @ParameterizedTest(name = "repeatedValue[min={0}]")
    @BooleanSource
    void repeatedValue(boolean min) {
        // Index on:
        //    min/max(repeater) GROUP BY str_value_indexed, num_value_2 ORDER BY str_value_indexed, min/max(repeater), num_value_2
        // Note that all values of repeater will always come from a single group, so at most one entry in the permuted space should ever be updated on record insert
        final RecordMetaDataHook hook = hook(min,
                Key.Expressions.field("repeater", KeyExpression.FanType.FanOut).groupBy(Key.Expressions.field("str_value_indexed"), Key.Expressions.field("num_value_2")),
                1);


        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);

            saveRecord(1, "yes", 1, 0, 1, 2, 3, 4, 5);
            saveRecord(2, "no", 1, 0, 1, 3, 5);

            assertEquals(Collections.singletonList(
                    Tuple.from(min ? 1 : 5, 1)
            ), scanGroup(Tuple.from("yes"), false));

            assertEquals(Collections.singletonList(
                    Tuple.from(min ? 1 : 5, 1)
            ), scanGroup(Tuple.from("no"), false));

            assertEquals(Arrays.asList(
                    Tuple.from(1, 1),
                    Tuple.from(2, 1),
                    Tuple.from(3, 1),
                    Tuple.from(4, 1),
                    Tuple.from(5, 1)
            ), scanValue(Tuple.from("yes", 1), false));

            assertEquals(Arrays.asList(
                    Tuple.from(1, 2),
                    Tuple.from(3, 2),
                    Tuple.from(5, 2)
            ), scanValue(Tuple.from("no", 1), false));

            saveRecord(3, "yes", 1, 0, 2, 4);
            saveRecord(4, "no", 1, 0, 2, 4);

            assertEquals(Collections.singletonList(
                    Tuple.from(min ? 1 : 5, 1)
            ), scanGroup(Tuple.from("yes"), false));

            assertEquals(Collections.singletonList(
                    Tuple.from(min ? 1 : 5, 1)
            ), scanGroup(Tuple.from("no"), false));

            assertEquals(Arrays.asList(
                    Tuple.from(1, 1),
                    Tuple.from(2, 1),
                    Tuple.from(2, 3),
                    Tuple.from(3, 1),
                    Tuple.from(4, 1),
                    Tuple.from(4, 3),
                    Tuple.from(5, 1)
            ), scanValue(Tuple.from("yes", 1), false));

            assertEquals(Arrays.asList(
                    Tuple.from(1, 2),
                    Tuple.from(2, 4),
                    Tuple.from(3, 2),
                    Tuple.from(4, 4),
                    Tuple.from(5, 2)
            ), scanValue(Tuple.from("no", 1), false));

            recordStore.deleteRecord(Tuple.from(1));
            recordStore.deleteRecord(Tuple.from(2));

            assertEquals(Collections.singletonList(
                    Tuple.from(min ? 2 : 4, 1)
            ), scanGroup(Tuple.from("yes"), false));

            assertEquals(Collections.singletonList(
                    Tuple.from(min ? 2 : 4, 1)
            ), scanGroup(Tuple.from("no"), false));

            assertEquals(Arrays.asList(
                    Tuple.from(2, 3),
                    Tuple.from(4, 3)
            ), scanValue(Tuple.from("yes", 1), false));

            assertEquals(Arrays.asList(
                    Tuple.from(2, 4),
                    Tuple.from(4, 4)
            ), scanValue(Tuple.from("no", 1), false));

            saveRecord(5, "yes", 2, 0, 1, 2, 3, 4);
            saveRecord(6, "yes", 2, 0, 5, 6, 7, 8);
            saveRecord(7, "no", 2, 0, 8, 7, 6, 5);
            saveRecord(8, "no", 2, 0, 4, 3, 2, 1);

            if (min) {
                assertEquals(Arrays.asList(
                        Tuple.from(1, 2),
                        Tuple.from(2, 1)
                ), scanGroup(Tuple.from("yes"), false));

                assertEquals(Arrays.asList(
                        Tuple.from(1, 2),
                        Tuple.from(2, 1)
                ), scanGroup(Tuple.from("no"), false));
            } else {
                assertEquals(Arrays.asList(
                        Tuple.from(4, 1),
                        Tuple.from(8, 2)
                ), scanGroup(Tuple.from("yes"), false));

                assertEquals(Arrays.asList(
                        Tuple.from(4, 1),
                        Tuple.from(8, 2)
                ), scanGroup(Tuple.from("no"), false));
            }

            assertEquals(Arrays.asList(
                    Tuple.from(1, 5),
                    Tuple.from(2, 5),
                    Tuple.from(3, 5),
                    Tuple.from(4, 5),
                    Tuple.from(5, 6),
                    Tuple.from(6, 6),
                    Tuple.from(7, 6),
                    Tuple.from(8, 6)
            ), scanValue(Tuple.from("yes", 2), false));

            assertEquals(Arrays.asList(
                    Tuple.from(1, 8),
                    Tuple.from(2, 8),
                    Tuple.from(3, 8),
                    Tuple.from(4, 8),
                    Tuple.from(5, 7),
                    Tuple.from(6, 7),
                    Tuple.from(7, 7),
                    Tuple.from(8, 7)
            ), scanValue(Tuple.from("no", 2), false));

            recordStore.deleteRecord(Tuple.from(5));
            recordStore.deleteRecord(Tuple.from(7));

            if (min) {
                assertEquals(Arrays.asList(
                        Tuple.from(2, 1),
                        Tuple.from(5, 2)
                ), scanGroup(Tuple.from("yes"), false));

                assertEquals(Arrays.asList(
                        Tuple.from(1, 2),
                        Tuple.from(2, 1)
                ), scanGroup(Tuple.from("no"), false));
            } else {
                assertEquals(Arrays.asList(
                        Tuple.from(4, 1),
                        Tuple.from(8, 2)
                ), scanGroup(Tuple.from("yes"), false));

                assertEquals(Arrays.asList(
                        Tuple.from(4, 1),
                        Tuple.from(4, 2)
                ), scanGroup(Tuple.from("no"), false));
            }

            assertEquals(Arrays.asList(
                    Tuple.from(5, 6),
                    Tuple.from(6, 6),
                    Tuple.from(7, 6),
                    Tuple.from(8, 6)
            ), scanValue(Tuple.from("yes", 2), false));

            assertEquals(Arrays.asList(
                    Tuple.from(1, 8),
                    Tuple.from(2, 8),
                    Tuple.from(3, 8),
                    Tuple.from(4, 8)
            ), scanValue(Tuple.from("no", 2), false));

            commit(context);
        }
    }

    @ParameterizedTest(name = "evaluateAggregateFunction[min={0}]")
    @BooleanSource
    void evaluateAggregateFunction(boolean min) {
        final String functionName = min ? FunctionNames.MIN : FunctionNames.MAX;
        final List<String> recordTypeNames = List.of("MySimpleRecord");
        final RecordMetaDataHook hook = hook(min);
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);

            saveRecord(1, "yes", 1, 1);
            saveRecord(2, "yes", 1, 2);
            saveRecord(3, "yes", 1, 3);
            saveRecord(4, "yes", 2, 4);
            saveRecord(5, "yes", 2, 5);
            saveRecord(6, "yes", 2, 6);
            saveRecord(7, "no", 3, 7);
            saveRecord(8, "no", 3, 8);
            saveRecord(9, "no", 3, 9);
            saveRecord(10, "no", 4, 10);
            saveRecord(11, "no", 4, 11);
            saveRecord(12, "no", 4, 12);

            // Evaluate across the complete index
            final IndexAggregateFunction ungrouped = new IndexAggregateFunction(
                    functionName,
                    Key.Expressions.field("num_value_3_indexed").ungrouped(),
                    INDEX_NAME);
            Tuple ungroupedExtremum = recordStore.evaluateAggregateFunction(recordTypeNames, ungrouped, Key.Evaluated.EMPTY, IsolationLevel.SERIALIZABLE).join();
            assertEquals(min ? Tuple.from(1L) : Tuple.from(12L), ungroupedExtremum);

            // Evaluate across the first column of the grouping key. This requires rolling up values across the different groups
            IndexAggregateFunction stringValueGrouped = new IndexAggregateFunction(
                    functionName,
                    Key.Expressions.field("num_value_3_indexed").groupBy(Key.Expressions.field("str_value_indexed")),
                    INDEX_NAME);
            Tuple yesExtremum = recordStore.evaluateAggregateFunction(recordTypeNames, stringValueGrouped, Key.Evaluated.scalar("yes"), IsolationLevel.SERIALIZABLE).join();
            assertEquals(min ? Tuple.from(1L) : Tuple.from(6L), yesExtremum);
            Tuple noExtremum = recordStore.evaluateAggregateFunction(recordTypeNames, stringValueGrouped, Key.Evaluated.scalar("no"), IsolationLevel.SERIALIZABLE).join();
            assertEquals(min ? Tuple.from(7L) : Tuple.from(12L), noExtremum);
            Tuple maybeExtremum = recordStore.evaluateAggregateFunction(recordTypeNames, stringValueGrouped, Key.Evaluated.scalar("maybe"), IsolationLevel.SERIALIZABLE).join();
            assertNull(maybeExtremum);

            // Evaluate when limited to a single group. This requires scanning through the keys of the larger group and filtering out unrelated groups
            IndexAggregateFunction mostSpecificallyGrouped = new IndexAggregateFunction(
                    functionName,
                    Key.Expressions.field("num_value_3_indexed").groupBy(Key.Expressions.concatenateFields("str_value_indexed", "num_value_2")),
                    INDEX_NAME);
            Tuple yes0Extremum = recordStore.evaluateAggregateFunction(recordTypeNames, mostSpecificallyGrouped, Key.Evaluated.concatenate("yes", 0L), IsolationLevel.SERIALIZABLE).join();
            assertNull(yes0Extremum);
            Tuple yes1Extremum = recordStore.evaluateAggregateFunction(recordTypeNames, mostSpecificallyGrouped, Key.Evaluated.concatenate("yes", 1L), IsolationLevel.SERIALIZABLE).join();
            assertEquals(min ? Tuple.from(1L) : Tuple.from(3L), yes1Extremum);
            Tuple yes2Extremum = recordStore.evaluateAggregateFunction(recordTypeNames, mostSpecificallyGrouped, Key.Evaluated.concatenate("yes", 2L), IsolationLevel.SERIALIZABLE).join();
            assertEquals(min ? Tuple.from(4L) : Tuple.from(6L), yes2Extremum);
            Tuple no3Extremum = recordStore.evaluateAggregateFunction(recordTypeNames, mostSpecificallyGrouped, Key.Evaluated.concatenate("no", 3L), IsolationLevel.SERIALIZABLE).join();
            assertEquals(min ? Tuple.from(7L) : Tuple.from(9L), no3Extremum);
            Tuple no4Extremum = recordStore.evaluateAggregateFunction(recordTypeNames, mostSpecificallyGrouped, Key.Evaluated.concatenate("no", 4L), IsolationLevel.SERIALIZABLE).join();
            assertEquals(min ? Tuple.from(10L) : Tuple.from(12L), no4Extremum);
            Tuple no5Extremum = recordStore.evaluateAggregateFunction(recordTypeNames, mostSpecificallyGrouped, Key.Evaluated.concatenate("no", 5L), IsolationLevel.SERIALIZABLE).join();
            assertNull(no5Extremum);

            commit(context);
        }
    }

    @ParameterizedTest(name = "coveringIndexScan[min={0}]")
    @BooleanSource
    void coveringIndexScan(boolean min) {
        final RecordMetaDataHook hook = hook(min);

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);

            final String strValueParam = "str_value";
            RecordQuery query = RecordQuery.newBuilder()
                    .setRecordType("MySimpleRecord")
                    .setFilter(Query.field("str_value_indexed").equalsParameter(strValueParam))
                    .setRequiredResults(List.of(Key.Expressions.field("num_value_2")))
                    .build();
            RecordQueryPlan plan = ((RecordQueryPlanner)planner).planCoveringAggregateIndex(query, INDEX_NAME);
            assertNotNull(plan);
            assertTrue(plan.hasIndexScan(INDEX_NAME));

            saveRecord(1, "yes", 1, 1);
            saveRecord(2, "yes", 1, 2);
            saveRecord(3, "yes", 1, 3);
            saveRecord(4, "yes", 2, 4);
            saveRecord(5, "yes", 2, 5);
            saveRecord(6, "yes", 2, 6);
            saveRecord(7, "no", 3, 7);
            saveRecord(8, "no", 3, 8);
            saveRecord(9, "no", 3, 9);
            saveRecord(10, "no", 4, 10);
            saveRecord(11, "no", 4, 11);
            saveRecord(12, "no", 4, 12);

            final Index index = recordStore.getRecordMetaData().getIndex(INDEX_NAME);
            List<Pair<Long, Long>> results = executePermutedIndexScan(plan, index, EvaluationContext.forBinding(strValueParam, "yes"));
            assertThat(results, hasSize(2));
            assertThat(results, containsInAnyOrder(Pair.of(1L, min ? 1L : 3L), Pair.of(2L, min ? 4L : 6L)));

            results = executePermutedIndexScan(plan, index, EvaluationContext.forBinding(strValueParam, "no"));
            assertThat(results, hasSize(2));
            assertThat(results, containsInAnyOrder(Pair.of(3L, min ? 7L : 9L), Pair.of(4L, min ? 10L : 12L)));

            results = executePermutedIndexScan(plan, index, EvaluationContext.forBinding(strValueParam, "maybe"));
            assertThat(results, empty());

            commit(context);
        }
    }

    @ParameterizedTest(name = "aggregateIndexScanWithPermutedSizeTwo[min={0}]")
    @BooleanSource
    void aggregateIndexScanWithPermutedSizeTwo(boolean min) {
        // Index on: str_value_indexed, min/max(num_value_unique), num_value_2, num_value_3_indexed
        final GroupingKeyExpression groupingKeyExpression = Key.Expressions.field("num_value_unique")
                .groupBy(Key.Expressions.concatenateFields("str_value_indexed", "num_value_2", "num_value_3_indexed"));
        final RecordMetaDataHook hook = hook(min, groupingKeyExpression, 2);

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);

            final String strValueParam = "str_value";
            RecordQuery query = RecordQuery.newBuilder()
                    .setRecordType("MySimpleRecord")
                    .setFilter(Query.field("str_value_indexed").equalsParameter(strValueParam))
                    .setRequiredResults(List.of(Key.Expressions.field("num_value_2"), Key.Expressions.field("num_value_3_indexed")))
                    .build();
            RecordQueryPlan plan = ((RecordQueryPlanner)planner).planCoveringAggregateIndex(query, INDEX_NAME);
            assertNotNull(plan);
            assertTrue(plan.hasIndexScan(INDEX_NAME));

            Map<NonnullPair<Integer, Integer>, Long> yesExtrema = new HashMap<>();
            Map<NonnullPair<Integer, Integer>, Long> noExtrema = new HashMap<>();
            for (int i = 0; i < 100; i++) {
                int numValue2 = i % 3;
                int numValue3 = i % 5;
                String strValue = i % 2 == 0 ? "yes" : "no";
                saveRecord(i, strValue, numValue2, numValue3);

                // Collect extrema values in the appropriate map
                Map<NonnullPair<Integer, Integer>, Long> extrema = strValue.equals("yes") ? yesExtrema : noExtrema;
                NonnullPair<Integer, Integer> key = NonnullPair.of(numValue2, numValue3);
                long unique = i;
                extrema.compute(key, (k, existing) -> existing == null ? unique : (min ? Math.min(unique, existing) : Math.max(unique, existing)));
            }

            final Index index = recordStore.getRecordMetaData().getIndex(INDEX_NAME);
            final BiConsumer<IndexEntry, TestRecords1Proto.MySimpleRecord> validator = (indexEntry, simpleRecord) -> {
                assertTrue(simpleRecord.hasNumValue2());
                assertEquals(((Number)indexEntry.getKeyValue(2)).intValue(), simpleRecord.getNumValue2());
                assertTrue(simpleRecord.hasNumValue3Indexed());
                assertEquals(((Number)indexEntry.getKeyValue(3)).intValue(), simpleRecord.getNumValue3Indexed());
            };
            final Function<TestRecords1Proto.MySimpleRecord, NonnullPair<Integer, Integer>> extractor = simpleRecord -> NonnullPair.of(simpleRecord.getNumValue2(), simpleRecord.getNumValue3Indexed());

            List<Pair<NonnullPair<Integer, Integer>, Long>> results = executePermutedIndexScan(plan, index, EvaluationContext.forBinding(strValueParam, "yes"), validator, extractor);
            assertThat(results, hasSize(yesExtrema.size()));
            assertThat(results, containsInAnyOrder(yesExtrema.entrySet().stream().map(entry -> Pair.of(entry.getKey(), entry.getValue())).toArray()));

            results = executePermutedIndexScan(plan, index, EvaluationContext.forBinding(strValueParam, "no"), validator, extractor);
            assertThat(results, hasSize(noExtrema.size()));
            assertThat(results, containsInAnyOrder(noExtrema.entrySet().stream().map(entry -> Pair.of(entry.getKey(), entry.getValue())).toArray()));

            results = executePermutedIndexScan(plan, index, EvaluationContext.forBinding(strValueParam, "maybe"), validator, extractor);
            assertThat(results, empty());

            commit(context);
        }

    }

    private List<Pair<Long, Long>> executePermutedIndexScan(@Nonnull RecordQueryPlan plan, @Nonnull Index index, @Nullable EvaluationContext evaluationContext) {
        return executePermutedIndexScan(plan, index, evaluationContext,
                (indexEntry, simple) -> {
                    assertTrue(simple.hasNumValue2());
                    assertEquals(((Number)indexEntry.getKeyValue(2)).intValue(), simple.getNumValue2());
                },
                simple -> (long) simple.getNumValue2());
    }

    @Nonnull
    private <T> List<Pair<T, Long>> executePermutedIndexScan(@Nonnull RecordQueryPlan plan, @Nonnull Index index, @Nullable EvaluationContext evaluationContext,
                                                             @Nonnull BiConsumer<IndexEntry, TestRecords1Proto.MySimpleRecord> validator,
                                                             @Nonnull Function<TestRecords1Proto.MySimpleRecord, T> extractor) {
        return plan.execute(recordStore, evaluationContext == null ? EvaluationContext.EMPTY : evaluationContext)
                .map(rec -> {
                    assertEquals(index, rec.getIndex());
                    TestRecords1Proto.MySimpleRecord simple = TestRecords1Proto.MySimpleRecord.newBuilder().mergeFrom(rec.getRecord()).build();
                    validator.accept(rec.getIndexEntry(), simple);
                    Tuple indexKey = rec.getIndexEntry().getKey();
                    assertNotNull(indexKey);
                    return Pair.of(extractor.apply(simple), indexKey.getLong(1));
                })
                .asList()
                .join();
    }

    @Test
    public void deleteWhere() {
        final RecordMetaDataHook hook = md -> {
            final KeyExpression pkey = Key.Expressions.concatenateFields("num_value_2", "num_value_3_indexed", "rec_no");
            md.getRecordType("MySimpleRecord").setPrimaryKey(pkey);
            md.getRecordType("MyOtherRecord").setPrimaryKey(pkey);
            md.removeIndex("MySimpleRecord$str_value_indexed");
            md.removeIndex("MySimpleRecord$num_value_3_indexed");
            md.removeIndex("MySimpleRecord$num_value_unique");
            md.removeIndex(COUNT_INDEX_NAME);
            md.removeIndex(COUNT_UPDATES_INDEX_NAME);
            md.addIndex("MySimpleRecord", new Index(INDEX_NAME,
                    Key.Expressions.concatenateFields("num_value_2", "num_value_3_indexed", "str_value_indexed", "num_value_unique").group(1),
                    IndexTypes.PERMUTED_MAX, Collections.singletonMap(IndexOptions.PERMUTED_SIZE_OPTION, "2")));
        };
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);

            saveRecord(100, "yes", 1, 1);
            saveRecord(150, "yes", 1, 1);
            saveRecord(200, "no", 1, 1);
            saveRecord(300, "yes", 1, 2);
            saveRecord(400, "no", 1, 2);
            saveRecord(500, "maybe", 2, 1);

            assertEquals(Arrays.asList(
                    Tuple.from(1, 150, 1, "yes"),
                    Tuple.from(1, 200, 1, "no"),
                    Tuple.from(1, 300, 2, "yes"),
                    Tuple.from(1, 400, 2, "no"),
                    Tuple.from(2, 500, 1, "maybe")
            ), scanGroup(Tuple.from(), false));

            commit(context);
        }
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);

            recordStore.deleteRecordsWhere(Query.field("num_value_2").equalsValue(2));

            assertEquals(Arrays.asList(
                    Tuple.from(1, 150, 1, "yes"),
                    Tuple.from(1, 200, 1, "no"),
                    Tuple.from(1, 300, 2, "yes"),
                    Tuple.from(1, 400, 2, "no")
            ), scanGroup(Tuple.from(), false));

            commit(context);
        }
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);

            assertThrows(Query.InvalidExpressionException.class, () -> {
                recordStore.deleteRecordsWhere(Query.and(
                        Query.field("num_value_2").equalsValue(2),
                        Query.field("num_value_3_indexed").equalsValue(1)));
            });
        }
    }

    private void saveRecord(int recNo, @Nonnull String strValue, int value2, int value3, int... repeater) {
        recordStore.saveRecord(TestRecords1Proto.MySimpleRecord.newBuilder()
                .setRecNo(recNo)
                .setStrValueIndexed(strValue)
                .setNumValue2(value2)
                .setNumValue3Indexed(value3)
                .setNumValueUnique(recNo)
                .addAllRepeater(IntStream.of(repeater).boxed().collect(Collectors.toList()))
                .build());
    }

    @Nonnull
    private List<Tuple> scanGroup(@Nonnull Tuple group, boolean reverse) {
        return recordStore.scanIndex(recordStore.getRecordMetaData().getIndex(INDEX_NAME), IndexScanType.BY_GROUP,
                TupleRange.allOf(group), null, reverse ? ScanProperties.REVERSE_SCAN : ScanProperties.FORWARD_SCAN)
                .map(entry -> TupleHelpers.subTuple(entry.getKey(), group.size(), entry.getKeySize()))
                .asList()
                .join();
    }

    @Nonnull
    private List<Tuple> scanValue(@Nonnull Tuple prefix, boolean reverse) {
        return recordStore.scanIndex(recordStore.getRecordMetaData().getIndex(INDEX_NAME), IndexScanType.BY_VALUE,
                TupleRange.allOf(prefix), null, reverse ? ScanProperties.REVERSE_SCAN : ScanProperties.FORWARD_SCAN)
                .map(entry -> TupleHelpers.subTuple(entry.getKey(), prefix.size(), entry.getKeySize()))
                .asList()
                .join();
    }

}
