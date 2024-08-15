/*
 * FDBRecordStoreIndexTest.java
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

package com.apple.foundationdb.record.provider.foundationdb;

import com.apple.foundationdb.FDBError;
import com.apple.foundationdb.FDBException;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.async.CloseableAsyncIterator;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.FunctionNames;
import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.IndexState;
import com.apple.foundationdb.record.IsolationLevel;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursorIterator;
import com.apple.foundationdb.record.RecordIndexUniquenessViolation;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.RecordMetaDataProto;
import com.apple.foundationdb.record.RecordMetaDataProvider;
import com.apple.foundationdb.record.RecordStoreState;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TestNoIndexesProto;
import com.apple.foundationdb.record.TestRecords1EvolvedProto;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.TestRecordsIndexFilteringProto;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.cursors.AutoContinuingCursor;
import com.apple.foundationdb.record.cursors.LazyCursor;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.metadata.FormerIndex;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexAggregateFunction;
import com.apple.foundationdb.record.metadata.IndexOptions;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.MetaDataException;
import com.apple.foundationdb.record.metadata.RecordTypeBuilder;
import com.apple.foundationdb.record.metadata.expressions.EmptyKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.FieldKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.GroupingKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression.FanType;
import com.apple.foundationdb.record.metadata.expressions.ThenKeyExpression;
import com.apple.foundationdb.record.provider.common.StoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.indexes.InvalidIndexEntry;
import com.apple.foundationdb.record.query.IndexQueryabilityFilter;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.record.util.pair.NonnullPair;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.TupleHelpers;
import com.apple.test.BooleanSource;
import com.apple.test.Tags;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import com.google.protobuf.Message;
import org.apache.commons.lang3.tuple.Pair;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;
import static com.apple.foundationdb.record.metadata.Key.Expressions.concatenateFields;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase.indexEntryKey;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.oneOf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Tests for indexes in {@link FDBRecordStore}.
 */
@Tag(Tags.RequiresFDB)
@Execution(ExecutionMode.CONCURRENT)
public class FDBRecordStoreIndexTest extends FDBRecordStoreTestBase {
    private static final Logger logger = LoggerFactory.getLogger(FDBRecordStoreIndexTest.class);

    @Test
    void uniqueness() {
        assertThrows(RecordIndexUniquenessViolation.class, () -> {
            try (FDBRecordContext context = openContext()) {
                openSimpleRecordStore(context);

                recordStore.saveRecord(TestRecords1Proto.MySimpleRecord.newBuilder()
                        .setRecNo(1)
                        .setNumValueUnique(1)
                        .build());
                recordStore.saveRecord(TestRecords1Proto.MySimpleRecord.newBuilder()
                        .setRecNo(2)
                        .setNumValueUnique(1) // Conflicts with #1.
                        .build());
                commit(context);
            }
        });
    }

    @Test
    void nonUniqueNull() {
        RecordMetaDataHook hook = metaData -> {
            metaData.removeIndex("MySimpleRecord$num_value_unique");
            metaData.addIndex("MySimpleRecord", new Index(
                    "unique_multi",
                    concat(field("num_value_unique"),
                            field("num_value_2")),
                    Index.EMPTY_VALUE,
                    IndexTypes.VALUE,
                    IndexOptions.UNIQUE_OPTIONS));
        };
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);

            recordStore.saveRecord(TestRecords1Proto.MySimpleRecord.newBuilder()
                    .setRecNo(1)
                    .setNumValueUnique(1)
                    .build());
            recordStore.saveRecord(TestRecords1Proto.MySimpleRecord.newBuilder()
                    .setRecNo(2)
                    .setNumValueUnique(1) // Conflicts with #1, but value2 unset, so allowed.
                    .build());
            commit(context);
        }
    }

    @Test
    void uniqueNull() {
        RecordMetaDataHook hook = metaData -> {
            metaData.removeIndex("MySimpleRecord$num_value_unique");
            metaData.addIndex("MySimpleRecord", new Index(
                    "unique_multi",
                    concat(field("num_value_unique"),
                            field("num_value_2", FanType.None, Key.Evaluated.NullStandin.NULL_UNIQUE)),
                    Index.EMPTY_VALUE,
                    IndexTypes.VALUE,
                    IndexOptions.UNIQUE_OPTIONS));
        };
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);

            recordStore.saveRecord(TestRecords1Proto.MySimpleRecord.newBuilder()
                    .setRecNo(1)
                    .setNumValueUnique(1)
                    .build());
            recordStore.saveRecord(TestRecords1Proto.MySimpleRecord.newBuilder()
                    .setRecNo(2)
                    .setNumValueUnique(1) // Conflicts with #1, value2 unset notwithstanding.
                    .build());
            assertThrows(RecordIndexUniquenessViolation.class, () -> commit(context));
        }
    }

    @Test
    void sumIndex() {
        final FieldKeyExpression recno = field("rec_no");
        final GroupingKeyExpression byKey = recno.groupBy(field("num_value_3_indexed"));
        final RecordMetaDataHook hook = md -> md.addUniversalIndex(new Index("sum", byKey, IndexTypes.SUM));

        final IndexAggregateFunction subtotal = new IndexAggregateFunction(FunctionNames.SUM, byKey, null);
        final IndexAggregateFunction total = new IndexAggregateFunction(FunctionNames.SUM, recno, null);
        final List<String> allTypes = Collections.emptyList();

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);

            assertEquals(0L, recordStore.evaluateAggregateFunction(allTypes, total, Key.Evaluated.EMPTY, IsolationLevel.SNAPSHOT).join().getLong(0));
            assertEquals(0L, recordStore.evaluateAggregateFunction(allTypes, subtotal, Key.Evaluated.scalar(1), IsolationLevel.SNAPSHOT).join().getLong(0));

            for (int i = 0; i < 100; i++) {
                TestRecords1Proto.MySimpleRecord.Builder recBuilder = TestRecords1Proto.MySimpleRecord.newBuilder();
                recBuilder.setRecNo(i);
                recBuilder.setNumValue3Indexed(i % 5);
                recordStore.saveRecord(recBuilder.build());
            }
            commit(context);
        }
        
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);

            assertEquals((99 * 100) / 2, recordStore.evaluateAggregateFunction(allTypes, total, Key.Evaluated.EMPTY, IsolationLevel.SNAPSHOT).join().getLong(0));
            assertEquals((99 * 100) / (2 * 5) - 20, recordStore.evaluateAggregateFunction(allTypes, subtotal, Key.Evaluated.scalar(1), IsolationLevel.SNAPSHOT).join().getLong(0));
            commit(context);
        }

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            recordStore.deleteRecord(Tuple.from(10));
            commit(context);
        }

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);

            assertEquals((99 * 100) / 2 - 10, recordStore.evaluateAggregateFunction(allTypes, total, Key.Evaluated.EMPTY, IsolationLevel.SNAPSHOT).join().getLong(0));
            commit(context);
        }
    }

    @Test
    void sumUnsetOptional() {
        final KeyExpression key = field("num_value_3_indexed").ungrouped();
        final RecordMetaDataHook hook = md -> md.addIndex("MySimpleRecord", new Index("sum", key, IndexTypes.SUM));

        final IndexAggregateFunction total = new IndexAggregateFunction(FunctionNames.SUM, key, null);
        List<String> types = Collections.singletonList("MySimpleRecord");

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);

            TestRecords1Proto.MySimpleRecord.Builder recBuilder = TestRecords1Proto.MySimpleRecord.newBuilder();
            recBuilder.setRecNo(1066L);
            recordStore.saveRecord(recBuilder.build());

            commit(context);
        }

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            assertEquals(0, recordStore.evaluateAggregateFunction(types, total, Key.Evaluated.EMPTY, IsolationLevel.SERIALIZABLE).join().getLong(0));
            commit(context);
        }
    }

    @Test
    void sumBoundIndex() {
        final FieldKeyExpression recno = field("rec_no");
        final GroupingKeyExpression byKey = recno.groupBy(field("num_value_3_indexed"));
        final RecordMetaDataHook hook = md -> md.addUniversalIndex(new Index("sum", byKey, IndexTypes.SUM));

        final IndexAggregateFunction subtotal = new IndexAggregateFunction(FunctionNames.SUM, byKey, null);
        final List<String> allTypes = Collections.emptyList();

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);

            for (int i = 0; i < 100; i++) {
                TestRecords1Proto.MySimpleRecord.Builder recBuilder = TestRecords1Proto.MySimpleRecord.newBuilder();
                recBuilder.setRecNo(i);
                recBuilder.setNumValue3Indexed(i % 5);
                recordStore.saveRecord(recBuilder.build());
            }
            commit(context);
        }

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);

            final Optional<IndexAggregateFunction> boundSubtotal = IndexFunctionHelper.bindAggregateFunction(recordStore,
                    subtotal, allTypes, IndexQueryabilityFilter.DEFAULT);
            assertTrue(boundSubtotal.isPresent(), "should find a suitable index");
            assertEquals("sum", boundSubtotal.get().getIndex());

            final Optional<IndexAggregateGroupKeys> keyFunction = IndexAggregateGroupKeys.conditionsToGroupKeys(subtotal, Query.field("num_value_3_indexed").equalsValue(1));
            assertTrue(keyFunction.isPresent(), "should match conditions");
            final Key.Evaluated keys = keyFunction.get().getGroupKeys(recordStore, EvaluationContext.EMPTY);
            assertEquals(Key.Evaluated.scalar(1), keys);

            assertEquals((99 * 100) / (2 * 5) - 20, recordStore.evaluateAggregateFunction(allTypes, subtotal, keys, IsolationLevel.SNAPSHOT).join().getLong(0));
            assertEquals((99 * 100) / (2 * 5) - 20, recordStore.evaluateAggregateFunction(allTypes, boundSubtotal.get(), keys, IsolationLevel.SNAPSHOT).join().getLong(0));
            commit(context);
        }

    }

    enum MinMaxIndexTypes {
        TUPLE,
        LONG,
        COMPATIBLE;

        @SuppressWarnings({"deprecation", "squid:CallToDeprecatedMethod"})
        public String min() {
            switch (this) {
                case TUPLE:
                    return IndexTypes.MIN_EVER_TUPLE;
                case LONG:
                    return IndexTypes.MIN_EVER_LONG;
                case COMPATIBLE:
                    return IndexTypes.MIN_EVER;
                default:
                    return fail("unknown enum value: " + this);
            }
        }

        @SuppressWarnings({"deprecation", "squid:CallToDeprecatedMethod"})
        public String max() {
            switch (this) {
                case TUPLE:
                    return IndexTypes.MAX_EVER_TUPLE;
                case LONG:
                    return IndexTypes.MAX_EVER_LONG;
                case COMPATIBLE:
                    return IndexTypes.MAX_EVER;
                default:
                    return fail("unknown enum value: " + this);
            }
        }

        public boolean shouldAllowNegative() {
            return this == TUPLE;
        }
    }

    @ParameterizedTest(name = "minMaxIndex({0})")
    @EnumSource(MinMaxIndexTypes.class)
    void minMaxIndex(MinMaxIndexTypes indexTypes) {
        final FieldKeyExpression recno = field("rec_no");
        final GroupingKeyExpression byKey = recno.groupBy(field("num_value_3_indexed"));
        final RecordMetaDataHook hook = md -> {
            RecordTypeBuilder type = md.getRecordType("MySimpleRecord");
            md.addIndex(type, new Index("min", byKey, indexTypes.min()));
            md.addIndex(type, new Index("max", byKey, indexTypes.max()));
        };

        final IndexAggregateFunction minOverall = new IndexAggregateFunction(FunctionNames.MIN_EVER, recno, null);
        final IndexAggregateFunction maxOverall = new IndexAggregateFunction(FunctionNames.MAX_EVER, recno, null);
        final IndexAggregateFunction minByKey = new IndexAggregateFunction(FunctionNames.MIN_EVER, byKey, null);
        final IndexAggregateFunction maxByKey = new IndexAggregateFunction(FunctionNames.MAX_EVER, byKey, null);
        List<String> types = Collections.singletonList("MySimpleRecord");

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);

            assertNull(recordStore.evaluateAggregateFunction(types, minOverall, Key.Evaluated.EMPTY, IsolationLevel.SNAPSHOT).join());
            assertNull(recordStore.evaluateAggregateFunction(types, maxOverall, Key.Evaluated.EMPTY, IsolationLevel.SNAPSHOT).join());
            assertNull(recordStore.evaluateAggregateFunction(types, minByKey, Key.Evaluated.scalar(1), IsolationLevel.SNAPSHOT).join());
            assertNull(recordStore.evaluateAggregateFunction(types, maxByKey, Key.Evaluated.scalar(1), IsolationLevel.SNAPSHOT).join());

            for (int i = 0; i < 100; i++) {
                TestRecords1Proto.MySimpleRecord.Builder recBuilder = TestRecords1Proto.MySimpleRecord.newBuilder();
                recBuilder.setRecNo(i);
                recBuilder.setNumValue3Indexed(i % 5);
                recordStore.saveRecord(recBuilder.build());
            }
            commit(context);
        }
        
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);

            assertEquals(0, recordStore.evaluateAggregateFunction(types, minOverall, Key.Evaluated.EMPTY, IsolationLevel.SNAPSHOT).join().getLong(0));
            assertEquals(99, recordStore.evaluateAggregateFunction(types, maxOverall, Key.Evaluated.EMPTY, IsolationLevel.SNAPSHOT).join().getLong(0));
            assertEquals(1, recordStore.evaluateAggregateFunction(types, minByKey, Key.Evaluated.scalar(1), IsolationLevel.SNAPSHOT).join().getLong(0));
            assertEquals(96, recordStore.evaluateAggregateFunction(types, maxByKey, Key.Evaluated.scalar(1), IsolationLevel.SNAPSHOT).join().getLong(0));
            commit(context);
        }

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);

            recordStore.deleteRecord(Tuple.from(0));
            recordStore.deleteRecord(Tuple.from(99));

            assertEquals(0, recordStore.evaluateAggregateFunction(types, minOverall, Key.Evaluated.EMPTY, IsolationLevel.SNAPSHOT).join().getLong(0));
            assertEquals(99, recordStore.evaluateAggregateFunction(types, maxOverall, Key.Evaluated.EMPTY, IsolationLevel.SNAPSHOT).join().getLong(0));
            commit(context);
        }

        // verify that negatives do not appear in min/max
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);

            TestRecords1Proto.MySimpleRecord.Builder recBuilder = TestRecords1Proto.MySimpleRecord.newBuilder();
            recBuilder.setRecNo(-1);
            recBuilder.setNumValue3Indexed(1);
            recordStore.saveRecord(recBuilder.build());
            if (!indexTypes.shouldAllowNegative()) {
                fail("should have thrown exception");
            }
        } catch (RecordCoreException e) {
            if (indexTypes.shouldAllowNegative()) {
                throw e;
            }
            assertEquals(e.getMessage(), "Attempted update of MAX_EVER_LONG or MIN_EVER_LONG index with negative value");
        }
    }

    @ParameterizedTest(name = "minMaxLongOptional({0})")
    @EnumSource(MinMaxIndexTypes.class)
    void minMaxOptional(MinMaxIndexTypes indexTypes) {
        final KeyExpression key = field("num_value_3_indexed").ungrouped();
        final RecordMetaDataHook hook = md -> {
            RecordTypeBuilder type = md.getRecordType("MySimpleRecord");
            md.addIndex(type, new Index("min", key, indexTypes.min()));
            md.addIndex(type, new Index("max", key, indexTypes.max()));
        };

        final IndexAggregateFunction minOverall = new IndexAggregateFunction(FunctionNames.MIN_EVER, key, null);
        final IndexAggregateFunction maxOverall = new IndexAggregateFunction(FunctionNames.MAX_EVER, key, null);
        List<String> types = Collections.singletonList("MySimpleRecord");

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);

            TestRecords1Proto.MySimpleRecord.Builder recBuilder = TestRecords1Proto.MySimpleRecord.newBuilder();
            recBuilder.setRecNo(1066L);
            recordStore.saveRecord(recBuilder.build());

            commit(context);
        }

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            final Tuple expected = indexTypes == MinMaxIndexTypes.TUPLE ? Tuple.fromList(Collections.singletonList(null)) : null;
            assertEquals(expected, recordStore.evaluateAggregateFunction(types, minOverall, Key.Evaluated.EMPTY, IsolationLevel.SNAPSHOT).join());
            assertEquals(expected, recordStore.evaluateAggregateFunction(types, maxOverall, Key.Evaluated.EMPTY, IsolationLevel.SNAPSHOT).join());
            commit(context);
        }
    }

    @Test
    void minMaxTupleUngrouped() {
        final FieldKeyExpression fieldKey = field("num_value_3_indexed");
        final GroupingKeyExpression indexKey = fieldKey.ungrouped();
        final RecordMetaDataHook hook = md -> {
            RecordTypeBuilder type = md.getRecordType("MySimpleRecord");
            md.addIndex(type, new Index("min", indexKey, IndexTypes.MIN_EVER_TUPLE));
            md.addIndex(type, new Index("max", indexKey, IndexTypes.MAX_EVER_TUPLE));
        };

        final IndexAggregateFunction min = new IndexAggregateFunction(FunctionNames.MIN_EVER, fieldKey, null);
        final IndexAggregateFunction max = new IndexAggregateFunction(FunctionNames.MAX_EVER, fieldKey, null);
        List<String> types = Collections.singletonList("MySimpleRecord");

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);

            assertNull(recordStore.evaluateAggregateFunction(types, min, Key.Evaluated.EMPTY, IsolationLevel.SNAPSHOT).join());
            assertNull(recordStore.evaluateAggregateFunction(types, max, Key.Evaluated.EMPTY, IsolationLevel.SNAPSHOT).join());

            for (int i = 0; i < 100; i++) {
                TestRecords1Proto.MySimpleRecord.Builder recBuilder = TestRecords1Proto.MySimpleRecord.newBuilder();
                recBuilder.setRecNo(i);
                recBuilder.setNumValue3Indexed(i * 10);
                recordStore.saveRecord(recBuilder.build());
            }
            commit(context);
        }

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);

            assertEquals(Tuple.from(0), recordStore.evaluateAggregateFunction(types, min, Key.Evaluated.EMPTY, IsolationLevel.SNAPSHOT).join());
            assertEquals(Tuple.from(990), recordStore.evaluateAggregateFunction(types, max, Key.Evaluated.EMPTY, IsolationLevel.SNAPSHOT).join());
            commit(context);
        }
    }

    @Test
    void minMaxTupleGrouped() {
        final ThenKeyExpression tupleKey = concat(field("str_value_indexed"), field("num_value_2"));
        final GroupingKeyExpression byKey = tupleKey.groupBy(field("num_value_3_indexed"));
        final RecordMetaDataHook hook = md -> {
            RecordTypeBuilder type = md.getRecordType("MySimpleRecord");
            md.addIndex(type, new Index("min", byKey, IndexTypes.MIN_EVER_TUPLE));
            md.addIndex(type, new Index("max", byKey, IndexTypes.MAX_EVER_TUPLE));
        };

        final IndexAggregateFunction minOverall = new IndexAggregateFunction(FunctionNames.MIN_EVER, tupleKey, null);
        final IndexAggregateFunction maxOverall = new IndexAggregateFunction(FunctionNames.MAX_EVER, tupleKey, null);
        final IndexAggregateFunction minByKey = new IndexAggregateFunction(FunctionNames.MIN_EVER, byKey, null);
        final IndexAggregateFunction maxByKey = new IndexAggregateFunction(FunctionNames.MAX_EVER, byKey, null);
        List<String> types = Collections.singletonList("MySimpleRecord");

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);

            assertNull(recordStore.evaluateAggregateFunction(types, minOverall, Key.Evaluated.EMPTY, IsolationLevel.SNAPSHOT).join());
            assertNull(recordStore.evaluateAggregateFunction(types, maxOverall, Key.Evaluated.EMPTY, IsolationLevel.SNAPSHOT).join());
            assertNull(recordStore.evaluateAggregateFunction(types, minByKey, Key.Evaluated.scalar(1), IsolationLevel.SNAPSHOT).join());
            assertNull(recordStore.evaluateAggregateFunction(types, maxByKey, Key.Evaluated.scalar(1), IsolationLevel.SNAPSHOT).join());

            for (int i = 0; i < 100; i++) {
                TestRecords1Proto.MySimpleRecord.Builder recBuilder = TestRecords1Proto.MySimpleRecord.newBuilder();
                recBuilder.setRecNo(i);
                recBuilder.setNumValue3Indexed(i % 3);
                recBuilder.setStrValueIndexed((i & 1) == 1 ? "odd" : "even");
                recBuilder.setNumValue2(i / 2);
                recordStore.saveRecord(recBuilder.build());
            }
            commit(context);
        }

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);

            assertEquals(Tuple.from("even", 0), recordStore.evaluateAggregateFunction(types, minOverall, Key.Evaluated.EMPTY, IsolationLevel.SNAPSHOT).join());
            assertEquals(Tuple.from("odd", 49), recordStore.evaluateAggregateFunction(types, maxOverall, Key.Evaluated.EMPTY, IsolationLevel.SNAPSHOT).join());
            assertEquals(Tuple.from("even", 2), recordStore.evaluateAggregateFunction(types, minByKey, Key.Evaluated.scalar(1), IsolationLevel.SNAPSHOT).join());
            assertEquals(Tuple.from("odd", 48), recordStore.evaluateAggregateFunction(types, maxByKey, Key.Evaluated.scalar(1), IsolationLevel.SNAPSHOT).join());
            commit(context);
        }
    }

    @Test
    void minMaxTupleRepeated() {
        final FieldKeyExpression fieldKey = field("repeater", FanType.FanOut);
        final GroupingKeyExpression indexKey = fieldKey.ungrouped();
        final RecordMetaDataHook hook = md -> {
            RecordTypeBuilder type = md.getRecordType("MySimpleRecord");
            md.addIndex(type, new Index("min", indexKey, IndexTypes.MIN_EVER_TUPLE));
            md.addIndex(type, new Index("max", indexKey, IndexTypes.MAX_EVER_TUPLE));
        };

        final IndexAggregateFunction min = new IndexAggregateFunction(FunctionNames.MIN_EVER, indexKey, null);
        final IndexAggregateFunction max = new IndexAggregateFunction(FunctionNames.MAX_EVER, indexKey, null);
        List<String> types = Collections.singletonList("MySimpleRecord");

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);

            assertNull(recordStore.evaluateAggregateFunction(types, min, Key.Evaluated.EMPTY, IsolationLevel.SNAPSHOT).join());
            assertNull(recordStore.evaluateAggregateFunction(types, max, Key.Evaluated.EMPTY, IsolationLevel.SNAPSHOT).join());

            TestRecords1Proto.MySimpleRecord.Builder recBuilder = TestRecords1Proto.MySimpleRecord.newBuilder();
            recBuilder.setRecNo(1);

            recordStore.saveRecord(recBuilder.build());
            assertNull(recordStore.evaluateAggregateFunction(types, min, Key.Evaluated.EMPTY, IsolationLevel.SNAPSHOT).join());
            assertNull(recordStore.evaluateAggregateFunction(types, max, Key.Evaluated.EMPTY, IsolationLevel.SNAPSHOT).join());

            recBuilder.addRepeater(1);
            recordStore.saveRecord(recBuilder.build());
            assertEquals(Tuple.from(1L), recordStore.evaluateAggregateFunction(types, min, Key.Evaluated.EMPTY, IsolationLevel.SNAPSHOT).join());
            assertEquals(Tuple.from(1L), recordStore.evaluateAggregateFunction(types, max, Key.Evaluated.EMPTY, IsolationLevel.SNAPSHOT).join());

            recBuilder.clearRepeater();
            recBuilder.addRepeater(2);
            recBuilder.addRepeater(3);
            recordStore.saveRecord(recBuilder.build());
            assertEquals(Tuple.from(1L), recordStore.evaluateAggregateFunction(types, min, Key.Evaluated.EMPTY, IsolationLevel.SNAPSHOT).join());
            assertEquals(Tuple.from(3L), recordStore.evaluateAggregateFunction(types, max, Key.Evaluated.EMPTY, IsolationLevel.SNAPSHOT).join());

            recBuilder.clearRepeater();
            recBuilder.addRepeater(-1);
            recordStore.saveRecord(recBuilder.build());
            assertEquals(Tuple.from(-1L), recordStore.evaluateAggregateFunction(types, min, Key.Evaluated.EMPTY, IsolationLevel.SNAPSHOT).join());
            assertEquals(Tuple.from(3L), recordStore.evaluateAggregateFunction(types, max, Key.Evaluated.EMPTY, IsolationLevel.SNAPSHOT).join());

            commit(context);
        }
    }

    @Test
    void minMaxTupleRepeatConcatenated() {
        final FieldKeyExpression fieldKey = field("repeater", FanType.Concatenate);
        final GroupingKeyExpression indexKey = fieldKey.ungrouped();
        final RecordMetaDataHook hook = md -> {
            RecordTypeBuilder type = md.getRecordType("MySimpleRecord");
            md.addIndex(type, new Index("min", indexKey, IndexTypes.MIN_EVER_TUPLE));
            md.addIndex(type, new Index("max", indexKey, IndexTypes.MAX_EVER_TUPLE));
        };

        final IndexAggregateFunction min = new IndexAggregateFunction(FunctionNames.MIN_EVER, indexKey, null);
        final IndexAggregateFunction max = new IndexAggregateFunction(FunctionNames.MAX_EVER, indexKey, null);
        List<String> types = Collections.singletonList("MySimpleRecord");

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);

            assertNull(recordStore.evaluateAggregateFunction(types, min, Key.Evaluated.EMPTY, IsolationLevel.SNAPSHOT).join());
            assertNull(recordStore.evaluateAggregateFunction(types, max, Key.Evaluated.EMPTY, IsolationLevel.SNAPSHOT).join());

            TestRecords1Proto.MySimpleRecord.Builder recBuilder = TestRecords1Proto.MySimpleRecord.newBuilder();
            recBuilder.setRecNo(1);

            recordStore.saveRecord(recBuilder.build());
            assertEquals(Tuple.from(Tuple.from()), recordStore.evaluateAggregateFunction(types, min, Key.Evaluated.EMPTY, IsolationLevel.SNAPSHOT).join());
            assertEquals(Tuple.from(Tuple.from()), recordStore.evaluateAggregateFunction(types, max, Key.Evaluated.EMPTY, IsolationLevel.SNAPSHOT).join());

            recBuilder.addRepeater(1);
            recordStore.saveRecord(recBuilder.build());
            assertEquals(Tuple.from(Tuple.from()), recordStore.evaluateAggregateFunction(types, min, Key.Evaluated.EMPTY, IsolationLevel.SNAPSHOT).join());
            assertEquals(Tuple.from(Tuple.from(1L)), recordStore.evaluateAggregateFunction(types, max, Key.Evaluated.EMPTY, IsolationLevel.SNAPSHOT).join());

            recBuilder.addRepeater(1);
            recordStore.saveRecord(recBuilder.build());
            assertEquals(Tuple.from(Tuple.from()), recordStore.evaluateAggregateFunction(types, min, Key.Evaluated.EMPTY, IsolationLevel.SNAPSHOT).join());
            assertEquals(Tuple.from(Tuple.from(1L, 1L)), recordStore.evaluateAggregateFunction(types, max, Key.Evaluated.EMPTY, IsolationLevel.SNAPSHOT).join());

            recBuilder.clearRepeater();
            recBuilder.addRepeater(2);
            recordStore.saveRecord(recBuilder.build());
            assertEquals(Tuple.from(Tuple.from()), recordStore.evaluateAggregateFunction(types, min, Key.Evaluated.EMPTY, IsolationLevel.SNAPSHOT).join());
            assertEquals(Tuple.from(Tuple.from(2L)), recordStore.evaluateAggregateFunction(types, max, Key.Evaluated.EMPTY, IsolationLevel.SNAPSHOT).join());

            commit(context);
        }
    }

    @Test
    void minMaxValue() {
        final FieldKeyExpression numValue2 = field("num_value_2");
        final FieldKeyExpression numValue3 = field("num_value_3_indexed");
        final ThenKeyExpression compound = concat(numValue2, numValue3);
        final GroupingKeyExpression grouped = numValue3.groupBy(numValue2);
        final RecordMetaDataHook hook = md -> {
            RecordTypeBuilder type = md.getRecordType("MySimpleRecord");
            md.addIndex(type, new Index("compound", compound, IndexTypes.VALUE));
        };

        final IndexAggregateFunction minOverall = new IndexAggregateFunction(FunctionNames.MIN, numValue3, null);
        final IndexAggregateFunction maxOverall = new IndexAggregateFunction(FunctionNames.MAX, numValue3, null);
        final IndexAggregateFunction minByKey = new IndexAggregateFunction(FunctionNames.MIN, grouped, null);
        final IndexAggregateFunction maxByKey = new IndexAggregateFunction(FunctionNames.MAX, grouped, null);
        List<String> types = Collections.singletonList("MySimpleRecord");

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);

            assertNull(recordStore.evaluateAggregateFunction(types, minOverall, Key.Evaluated.EMPTY, IsolationLevel.SNAPSHOT).join());
            assertNull(recordStore.evaluateAggregateFunction(types, maxOverall, Key.Evaluated.EMPTY, IsolationLevel.SNAPSHOT).join());
            assertNull(recordStore.evaluateAggregateFunction(types, minByKey, Key.Evaluated.scalar(1), IsolationLevel.SNAPSHOT).join());
            assertNull(recordStore.evaluateAggregateFunction(types, maxByKey, Key.Evaluated.scalar(1), IsolationLevel.SNAPSHOT).join());

            for (int i = 0; i < 100; i++) {
                TestRecords1Proto.MySimpleRecord.Builder recBuilder = TestRecords1Proto.MySimpleRecord.newBuilder();
                recBuilder.setRecNo(i);
                recBuilder.setNumValue2(i % 5);
                recBuilder.setNumValue3Indexed(i + 1000);
                recordStore.saveRecord(recBuilder.build());
            }
            commit(context);
        }
        
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);

            assertEquals(1000, recordStore.evaluateAggregateFunction(types, minOverall, Key.Evaluated.EMPTY, IsolationLevel.SNAPSHOT).join().getLong(0));
            assertEquals(1099, recordStore.evaluateAggregateFunction(types, maxOverall, Key.Evaluated.EMPTY, IsolationLevel.SNAPSHOT).join().getLong(0));
            assertEquals(1001, recordStore.evaluateAggregateFunction(types, minByKey, Key.Evaluated.scalar(1), IsolationLevel.SNAPSHOT).join().getLong(0));
            assertEquals(1096, recordStore.evaluateAggregateFunction(types, maxByKey, Key.Evaluated.scalar(1), IsolationLevel.SNAPSHOT).join().getLong(0));
            commit(context);
        }

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);

            recordStore.deleteRecord(Tuple.from(0));
            recordStore.deleteRecord(Tuple.from(99));

            assertEquals(1001, recordStore.evaluateAggregateFunction(types, minOverall, Key.Evaluated.EMPTY, IsolationLevel.SNAPSHOT).join().getLong(0));
            assertEquals(1098, recordStore.evaluateAggregateFunction(types, maxOverall, Key.Evaluated.EMPTY, IsolationLevel.SNAPSHOT).join().getLong(0));
            assertEquals(1001, recordStore.evaluateAggregateFunction(types, minByKey, Key.Evaluated.scalar(1), IsolationLevel.SNAPSHOT).join().getLong(0));
            assertEquals(1096, recordStore.evaluateAggregateFunction(types, maxByKey, Key.Evaluated.scalar(1), IsolationLevel.SNAPSHOT).join().getLong(0));
            commit(context);
        }
    }

    @Test
    void minMaxValueNegative() {
        final FieldKeyExpression strValue = field("str_value_indexed");
        final FieldKeyExpression numValue2 = field("num_value_2");
        final FieldKeyExpression numValue3 = field("num_value_3_indexed");
        final ThenKeyExpression compound = concat(strValue, numValue2, numValue3);
        final RecordMetaDataHook hook = md -> {
            RecordTypeBuilder type = md.getRecordType("MySimpleRecord");
            md.addIndex(type, new Index("compound", compound, IndexTypes.VALUE));
        };

        List<String> types = Collections.singletonList("MySimpleRecord");

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);

            final IndexAggregateFunction minValue2 = new IndexAggregateFunction(FunctionNames.MIN, numValue2, null);
            assertThrows(RecordCoreException.class, () -> {
                recordStore.evaluateAggregateFunction(types, minValue2, Key.Evaluated.EMPTY, IsolationLevel.SNAPSHOT).join();
            });
            final IndexAggregateFunction minValue3GroupedIncorrectly = new IndexAggregateFunction(FunctionNames.MIN, numValue3.groupBy(numValue2, strValue), null);
            assertThrows(RecordCoreException.class, () -> {
                recordStore.evaluateAggregateFunction(types, minValue3GroupedIncorrectly, Key.Evaluated.concatenate(1, "foo"), IsolationLevel.SNAPSHOT).join();
            });
            final IndexAggregateFunction minValue3GroupedTooMany = new IndexAggregateFunction(FunctionNames.MIN, concat(numValue3, numValue2).groupBy(strValue), null);
            assertThrows(RecordCoreException.class, () -> {
                recordStore.evaluateAggregateFunction(types, minValue3GroupedTooMany, Key.Evaluated.scalar("foo"), IsolationLevel.SNAPSHOT).join();
            });

            commit(context);
        }

    }

    @Test
    void countValueIndex() {
        final FieldKeyExpression numValue3 = field("num_value_3_indexed");
        final GroupingKeyExpression byKey = numValue3.groupBy(field("str_value_indexed"));
        final RecordMetaDataHook hook = md -> md.addIndex("MySimpleRecord", new Index("count_num_3", byKey, IndexTypes.COUNT_NOT_NULL));
        final List<String> types = Collections.singletonList("MySimpleRecord");

        final IndexAggregateFunction perKey = new IndexAggregateFunction(FunctionNames.COUNT_NOT_NULL, byKey, null);
        final IndexAggregateFunction total = new IndexAggregateFunction(FunctionNames.COUNT_NOT_NULL, numValue3, null);

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);

            for (int i = 0; i < 100; i++) {
                TestRecords1Proto.MySimpleRecord.Builder recBuilder = TestRecords1Proto.MySimpleRecord.newBuilder();
                recBuilder.setRecNo(i);
                recBuilder.setStrValueIndexed((i & 1) == 1 ? "odd" : "even");
                if (i % 5 == 0) {
                    recBuilder.clearNumValue3Indexed();
                } else {
                    recBuilder.setNumValue3Indexed(i + 1000);
                }
                recordStore.saveRecord(recBuilder.build());
            }
            commit(context);
        }
        
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);


            assertEquals(80, recordStore.evaluateAggregateFunction(types, total, Key.Evaluated.EMPTY, IsolationLevel.SNAPSHOT).join().getLong(0));
            assertEquals(40, recordStore.evaluateAggregateFunction(types, perKey, Key.Evaluated.scalar("even"), IsolationLevel.SNAPSHOT).join().getLong(0));
            commit(context);
        }

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            recordStore.deleteRecord(Tuple.from(8));
            commit(context);
        }

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);

            assertEquals(79, recordStore.evaluateAggregateFunction(types, total, Key.Evaluated.EMPTY, IsolationLevel.SNAPSHOT).join().getLong(0));
            assertEquals(39, recordStore.evaluateAggregateFunction(types, perKey, Key.Evaluated.scalar("even"), IsolationLevel.SNAPSHOT).join().getLong(0));
            commit(context);
        }
    }

    @Test
    void countMultiValueIndex() {
        final ThenKeyExpression values = concatenateFields("num_value_2", "num_value_3_indexed");
        final GroupingKeyExpression byKey = values.groupBy(field("str_value_indexed"));
        final RecordMetaDataHook hook = md -> md.addIndex("MySimpleRecord", new Index("count_num_3", byKey, IndexTypes.COUNT_NOT_NULL));
        final List<String> types = Collections.singletonList("MySimpleRecord");

        final IndexAggregateFunction perKey = new IndexAggregateFunction(FunctionNames.COUNT_NOT_NULL, byKey, null);
        final IndexAggregateFunction total = new IndexAggregateFunction(FunctionNames.COUNT_NOT_NULL, values, null);

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);

            for (int i = 0; i < 100; i++) {
                TestRecords1Proto.MySimpleRecord.Builder recBuilder = TestRecords1Proto.MySimpleRecord.newBuilder();
                recBuilder.setRecNo(i);
                recBuilder.setStrValueIndexed((i & 1) == 1 ? "odd" : "even");
                if (i % 3 == 0) {
                    recBuilder.clearNumValue2();
                } else {
                    recBuilder.setNumValue2(i + 1000);
                }
                if (i % 5 == 0) {
                    recBuilder.clearNumValue3Indexed();
                } else {
                    recBuilder.setNumValue3Indexed(i + 10000);
                }
                recordStore.saveRecord(recBuilder.build());
            }
            commit(context);
        }

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);


            assertEquals(100 - 100 / 5 - 100 / 3 + 100 / 15, recordStore.evaluateAggregateFunction(types, total, Key.Evaluated.EMPTY, IsolationLevel.SNAPSHOT).join().getLong(0));
            assertEquals(50 - 50 / 5 - 50 / 3 + 50 / 15, recordStore.evaluateAggregateFunction(types, perKey, Key.Evaluated.scalar("even"), IsolationLevel.SNAPSHOT).join().getLong(0));
            commit(context);
        }
    }

    @ParameterizedTest
    @BooleanSource
    void countClearWhenZero(boolean clearWhenZero) {
        final GroupingKeyExpression byKey = new GroupingKeyExpression(field("str_value_indexed"), 0);
        final RecordMetaDataHook hook = md -> md.addIndex("MySimpleRecord", new Index("count_by_str", byKey, IndexTypes.COUNT, ImmutableMap.of(IndexOptions.CLEAR_WHEN_ZERO, Boolean.toString(clearWhenZero))));
        final List<String> types = Collections.singletonList("MySimpleRecord");

        final IndexAggregateFunction perKey = new IndexAggregateFunction(FunctionNames.COUNT, byKey, null);

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);

            for (int i = 0; i < 10; i++) {
                TestRecords1Proto.MySimpleRecord.Builder recBuilder = TestRecords1Proto.MySimpleRecord.newBuilder();
                recBuilder.setRecNo(i);
                recBuilder.setStrValueIndexed((i & 1) == 1 ? "odd" : "even");
                recordStore.saveRecord(recBuilder.build());
            }
            commit(context);
        }

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            assertEquals(5, recordStore.evaluateAggregateFunction(types, perKey, Key.Evaluated.scalar("even"), IsolationLevel.SNAPSHOT).join().getLong(0));
            assertEquals(5, recordStore.evaluateAggregateFunction(types, perKey, Key.Evaluated.scalar("odd"), IsolationLevel.SNAPSHOT).join().getLong(0));
            assertEquals(ImmutableMap.of("even", 5L, "odd", 5L),
                    recordStore.scanIndex(recordStore.getRecordMetaData().getIndex("count_by_str"), IndexScanType.BY_GROUP, TupleRange.ALL, null, ScanProperties.FORWARD_SCAN)
                            .map(i -> NonnullPair.of(i.getKey().get(0), i.getValue().get(0)))
                            .asList().join().stream().collect(Collectors.toMap(NonnullPair::getLeft, NonnullPair::getRight)));
            commit(context);
        }

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            for (int i = 0; i < 10; i += 2) {
                recordStore.deleteRecord(Tuple.from(i));
            }
            commit(context);
        }

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            assertEquals(0, recordStore.evaluateAggregateFunction(types, perKey, Key.Evaluated.scalar("even"), IsolationLevel.SNAPSHOT).join().getLong(0));
            assertEquals(5, recordStore.evaluateAggregateFunction(types, perKey, Key.Evaluated.scalar("odd"), IsolationLevel.SNAPSHOT).join().getLong(0));
            assertEquals(clearWhenZero ? ImmutableMap.of("odd", 5L) : ImmutableMap.of("even", 0L, "odd", 5L),
                    recordStore.scanIndex(recordStore.getRecordMetaData().getIndex("count_by_str"), IndexScanType.BY_GROUP, TupleRange.ALL, null, ScanProperties.FORWARD_SCAN)
                            .map(i -> NonnullPair.of(i.getKey().get(0), i.getValue().get(0)))
                            .asList().join().stream().collect(Collectors.toMap(NonnullPair::getLeft, NonnullPair::getRight)));
            commit(context);
        }
    }

    @Test
    void scanWriteOnlyIndex() throws Exception {
        final String indexName = "MySimpleRecord$num_value_3_indexed";

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            recordStore.markIndexWriteOnly(indexName).get();
            assertThat(recordStore.isIndexReadable(indexName), is(false));
            assertThat(recordStore.isIndexWriteOnly(indexName), is(true));

            try {
                recordStore.scanIndexRecords(indexName);
                fail("Was not stopped from scanning write-only index");
            } catch (ScanNonReadableIndexException e) {
                assertEquals(indexName, e.getLogInfo().get(LogMessageKeys.INDEX_NAME.toString()));
            }

            commit(context);
        }

        TestRecords1Proto.MySimpleRecord record = TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(1066L).setNumValue3Indexed(42).build();

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            recordStore.saveRecord(record);
            commit(context);
        }

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            recordStore.scanIndexRecords(indexName);
            fail("Was not stopped from scanning write-only index");
        } catch (ScanNonReadableIndexException e) {
            assertEquals(indexName, e.getLogInfo().get(LogMessageKeys.INDEX_NAME.toString()));
        }

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            recordStore.uncheckedMarkIndexReadable(indexName).get();
            assertThat(recordStore.isIndexReadable(indexName), is(true));
            assertThat(recordStore.isIndexWriteOnly(indexName), is(false));
            assertEquals(Collections.singletonList(1066L),
                    recordStore.scanIndexRecords(indexName)
                            .map(rec -> TestRecords1Proto.MySimpleRecord.newBuilder().mergeFrom(rec.getRecord()).getRecNo()).asList().get());
            commit(context);
        }

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            assertEquals(Collections.singletonList(1066L),
                    recordStore.scanIndexRecords(indexName)
                            .map(rec -> TestRecords1Proto.MySimpleRecord.newBuilder().mergeFrom(rec.getRecord()).getRecNo()).asList().get());
            commit(context);
        }
    }

    @Test
    void indexesToBuild() throws Exception {
        final String indexName = "MySimpleRecord$str_value_indexed";
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            final Index index = recordStore.getRecordMetaData().getIndex(indexName);
            assertTrue(recordStore.markIndexDisabled(index).get(), "index should not have been disabled initially");
            assertTrue(recordStore.getIndexesToBuild().containsKey(index), "index should have been included in indexes to build");

            recordStore.rebuildIndex(index).get();
            assertEquals(Collections.emptyMap(), recordStore.getIndexesToBuild());
            commit(context);
        }
    }

    @Test
    void scanDisabledIndex() throws Exception {
        final String indexName = "MySimpleRecord$num_value_3_indexed";

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            recordStore.markIndexDisabled(indexName).get();
            assertThat(recordStore.isIndexReadable(indexName), is(false));
            assertThat(recordStore.isIndexDisabled(indexName), is(true));

            try {
                recordStore.scanIndexRecords(indexName);
                fail("Was not stopped from scanning disabled index");
            } catch (ScanNonReadableIndexException e) {
                assertEquals(indexName, e.getLogInfo().get(LogMessageKeys.INDEX_NAME.toString()));
            }

            commit(context);
        }

        TestRecords1Proto.MySimpleRecord record = TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(1066L).setNumValue3Indexed(42).build();

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            recordStore.saveRecord(record);
            commit(context);
        }

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            recordStore.scanIndexRecords(indexName);
            fail("Was not stopped from scanning disabled index");
        } catch (ScanNonReadableIndexException e) {
            assertEquals(indexName, e.getLogInfo().get(LogMessageKeys.INDEX_NAME.toString()));
        }

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            recordStore.uncheckedMarkIndexReadable(indexName).get();
            assertThat(recordStore.isIndexReadable(indexName), is(true));
            assertThat(recordStore.isIndexDisabled(indexName), is(false));
            assertEquals(0, (int)recordStore.scanIndexRecords(indexName).getCount().get());
            commit(context);
        }

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            assertEquals(0, (int)recordStore.scanIndexRecords(indexName).getCount().get());
            commit(context);
        }
    }

    @Test
    void modifyIndexState() throws Exception {
        final String indexName = "MySimpleRecord$num_value_3_indexed";
        TestRecords1Proto.MySimpleRecord record = TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(1066L).setNumValue3Indexed(42).build();

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            recordStore.saveRecord(record);
            commit(context);
        }

        FDBRecordContext context1 = openContext();
        openSimpleRecordStore(context1);
        List<FDBIndexedRecord<Message>> indexedRecords = recordStore.scanIndexRecords(indexName).asList().join();
        assertEquals(1, indexedRecords.size());
        recordStore.ensureContextActive().addWriteConflictKey(new byte[0]); // Add a write-conflict as read-only transactions never conflict.

        FDBRecordContext context2 = openContext();
        openSimpleRecordStore(context2);
        List<FDBStoredRecord<Message>> storedRecords = recordStore.scanRecords(null, ScanProperties.FORWARD_SCAN).asList().join();
        assertEquals(1, storedRecords.size());
        recordStore.ensureContextActive().addWriteConflictKey(new byte[0]);

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            assertThat(recordStore.markIndexDisabled(indexName).join(), is(true));
            assertThat(recordStore.isIndexReadable(indexName), is(false));
            assertThat(recordStore.isIndexDisabled(indexName), is(true));
            commit(context);
        }

        try {
            context1.ensureActive();
            commit(context1);
            fail("Able to commit transaction after index state is modified.");
        } catch (FDBExceptions.FDBStoreRetriableException e) {
            assertThat(((FDBException)e.getCause()).getCode(), equalTo(FDBError.NOT_COMMITTED.code()));
        }

        commit(context2); // context2 didn't need index, so this should succeed
    }

    @Test
    void deleteAllRecordsPreservesIndexStates() throws Exception {
        final String disabledIndex = "MySimpleRecord$num_value_3_indexed";
        final String writeOnlyIndex = "MySimpleRecord$str_value_indexed";
        TestRecords1Proto.MySimpleRecord testRecord = TestRecords1Proto.MySimpleRecord.newBuilder()
                .setRecNo(1066L)
                .setNumValue3Indexed(42)
                .setStrValueIndexed("forty-two")
                .build();

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            recordStore.markIndexDisabled(disabledIndex).get();
            recordStore.markIndexWriteOnly(writeOnlyIndex).get();
            recordStore.saveRecord(testRecord);
            commit(context);
        }

        // Ensure that the index states are preserved as visible from within the
        // uncommitted transaction.
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            assertTrue(recordStore.isIndexDisabled(disabledIndex));
            assertTrue(recordStore.isIndexWriteOnly(writeOnlyIndex));
            assertTrue(recordStore.recordExists(Tuple.from(1066L)));

            recordStore.deleteAllRecords();
            assertTrue(recordStore.isIndexDisabled(disabledIndex));
            assertTrue(recordStore.isIndexWriteOnly(writeOnlyIndex));
            assertFalse(recordStore.recordExists(Tuple.from(1066L)));
            commit(context);
        }

        // Ensure that this is still true after the transaction commits.
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            assertTrue(recordStore.isIndexDisabled(disabledIndex));
            assertTrue(recordStore.isIndexWriteOnly(writeOnlyIndex));
            assertFalse(recordStore.recordExists(Tuple.from(1066L)));
        }

        // Rebuild all indexes to reset the index states.
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            assertTrue(recordStore.isIndexDisabled(disabledIndex));
            assertTrue(recordStore.isIndexWriteOnly(writeOnlyIndex));
            recordStore.rebuildAllIndexes().get();
            assertTrue(recordStore.isIndexReadable(disabledIndex));
            assertTrue(recordStore.isIndexReadable(writeOnlyIndex));
            commit(context);
        }

        // Verify that the index states are, in fact, updated.
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            assertTrue(recordStore.isIndexReadable(disabledIndex));
            assertTrue(recordStore.isIndexReadable(writeOnlyIndex));
            commit(context);
        }
    }

    @Test
    void insertTerrible() {
        RecordMetaDataHook hook = metaDataBuilder -> {
            metaDataBuilder.addIndex("MySimpleRecord", new Index("value3$terrible", field("num_value_3_indexed"), "terrible"));
        };

        TestRecords1Proto.MySimpleRecord evenRec = TestRecords1Proto.MySimpleRecord.newBuilder()
                .setRecNo(1066).setNumValue3Indexed(100).build();
        TestRecords1Proto.MySimpleRecord oddRec = TestRecords1Proto.MySimpleRecord.newBuilder()
                .setRecNo(1776).setNumValue3Indexed(101).build();
        TestRecords1Proto.MySimpleRecord oddRec2 = TestRecords1Proto.MySimpleRecord.newBuilder()
                .setRecNo(1776).setNumValue3Indexed(102).build();

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            recordStore.saveRecord(evenRec);
            fail("Added record with even value for terrible index");
        } catch (UnsupportedOperationException e) {
            assertEquals("TerribleIndexMaintainer does not implement update for evens on insert", e.getMessage());
        }

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            recordStore.saveRecord(oddRec);
            recordStore.deleteRecord(Tuple.from(1776));
            fail("Removed record with odd value for terrible index");
        } catch (UnsupportedOperationException e) {
            assertEquals("TerribleIndexMaintainer does not implement update for odds on remove", e.getMessage());
        }

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            recordStore.saveRecord(oddRec);
            recordStore.saveRecord(oddRec2);
            fail("Updating record not caught.");
        } catch (UnsupportedOperationException e) {
            assertEquals("TerribleIndexMaintainer cannot update", e.getMessage());
        }
    }

    @Test
    void insertValueBuggy() {
        RecordMetaDataHook hook = metaDataBuilder -> {
            metaDataBuilder.addIndex("MySimpleRecord", new Index("value3$buggy", field("num_value_3_indexed"), "value_buggy"));
        };

        TestRecords1Proto.MySimpleRecord evenRec = TestRecords1Proto.MySimpleRecord.newBuilder()
                .setRecNo(1066).setNumValue3Indexed(100).build();
        TestRecords1Proto.MySimpleRecord oddRec = TestRecords1Proto.MySimpleRecord.newBuilder()
                .setRecNo(1776).setNumValue3Indexed(101).build();
        TestRecords1Proto.MySimpleRecord evenRec2 = TestRecords1Proto.MySimpleRecord.newBuilder()
                .setRecNo(1066).setNumValue3Indexed(103).build();

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            recordStore.saveRecord(oddRec);
            fail("Added record with odd value for index key");
        } catch (UnsupportedOperationException e) {
            assertEquals("Cannot add index key beginning with odd number", e.getMessage());
        }

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            recordStore.saveRecord(evenRec);
            recordStore.deleteRecord(Tuple.from(1066));
            fail("Removed record with even value for index key");
        } catch (UnsupportedOperationException e) {
            assertEquals("Cannot remove index key beginning with even number", e.getMessage());
        }

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            recordStore.saveRecord(evenRec);
            recordStore.saveRecord(evenRec2);
            fail("Updated record with bad values for index key");
        } catch (UnsupportedOperationException e) {
            assertEquals("Cannot remove index key beginning with even number", e.getMessage());
        }
    }

    @Test
    void markReadableTest() throws Exception {
        try (FDBRecordContext context = openContext()) {
            final RecordMetaDataBuilder builder = RecordMetaData.newBuilder().setRecords(TestNoIndexesProto.getDescriptor());
            recordStore = FDBRecordStore.newBuilder().setContext(context).setMetaDataProvider(builder).setKeySpacePath(path).createOrOpen();
            // Save 20 records without any indexes.
            for (int i = 0; i < 20; i++) {
                TestNoIndexesProto.MySimpleRecord record = TestNoIndexesProto.MySimpleRecord.newBuilder()
                        .setRecNo(i).setNumValue(i + 1000).build();
                recordStore.saveRecord(record);
            }
            commit(context);
        }

        final String indexName = "MySimpleRecord$str_value_indexed";
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            assertThat(recordStore.isIndexWriteOnly(indexName), is(false));
            recordStore.clearAndMarkIndexWriteOnly(indexName).get();
            assertThat(recordStore.isIndexWriteOnly(indexName), is(true));
            commit(context);
        }

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            Index index = recordStore.getRecordMetaData().getIndex(indexName);
            assertThat(recordStore.isIndexReadable(index), is(false));
            buildIndexAndCrashHalfway(indexName, 4);
            Optional<Range> firstUnbuilt = recordStore.firstUnbuiltRange(index).get();
            assertTrue(firstUnbuilt.isPresent());
            recordStore.markIndexReadable(index).handle((built, e) -> {
                assertNotNull(e);
                assertThat(e, instanceOf(CompletionException.class));
                assertNotNull(e.getCause());
                assertThat(e.getCause(), instanceOf(FDBRecordStore.IndexNotBuiltException.class));
                return null;
            }).get();
            assertThat(recordStore.isIndexReadable(index), is(false));
        }
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            Index index = recordStore.getRecordMetaData().getIndex(indexName);
            assertThat(recordStore.isIndexReadable(index), is(false));
            try (OnlineIndexer indexBuilder = OnlineIndexer.newBuilder().setRecordStore(recordStore).setIndex(index)
                    .build()) {
                indexBuilder.buildIndex(false);
            }
            commit(context);
        }
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            Index index = recordStore.getRecordMetaData().getIndex(indexName);
            assertThat(recordStore.isIndexReadable(index), is(false));
            assertFalse(recordStore.firstUnbuiltRange(index).get().isPresent());
            assertTrue(recordStore.markIndexReadable(index).get());
            assertFalse(recordStore.markIndexReadable(index).get());
            assertThat(recordStore.isIndexReadable(index), is(true));
            commit(context);
        }

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            assertFalse(recordStore.isIndexWriteOnly(indexName));
            recordStore.markIndexDisabled(indexName).get();
            assertTrue(recordStore.isIndexDisabled(indexName));
            commit(context);
        }

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            Index index = recordStore.getRecordMetaData().getIndex(indexName);
            assertThat(recordStore.isIndexReadable(index), is(false));
            buildIndexAndCrashHalfway(indexName, 7);
            Optional<Range> firstUnbuilt = recordStore.firstUnbuiltRange(index).get();
            assertTrue(firstUnbuilt.isPresent());
            assertTrue(recordStore.uncheckedMarkIndexReadable(index.getName()).get());
            assertFalse(recordStore.uncheckedMarkIndexReadable(index.getName()).get());
            assertThat(recordStore.isIndexReadable(index), is(true));

            // Purposefully, checking to mark an index readable that is already
            // readable does not throw an error.
            firstUnbuilt = recordStore.firstUnbuiltRange(index).get();
            assertTrue(firstUnbuilt.isPresent());
            assertFalse(recordStore.markIndexReadable(index.getName()).get());
            assertThat(recordStore.isIndexReadable(index), is(true));
        }
    }

    private void buildIndexAndCrashHalfway(String indexName, int indexedRecordsCount) {
        final String throwMsg = "Intentionally thrown during test";
        final AtomicLong counter = new AtomicLong(0);
        UnaryOperator<OnlineIndexOperationConfig> configLoader = old -> {
            if (counter.incrementAndGet() > indexedRecordsCount) {
                counter.set(0);
                throw new RecordCoreException(throwMsg);
            }
            return old;
        };

        try (OnlineIndexer indexBuilder = OnlineIndexer.newBuilder().setRecordStore(recordStore).setIndex(indexName)
                .setLimit(1)
                .setConfigLoader(configLoader)
                .build()) {
            RecordCoreException ex = assertThrows(RecordCoreException.class, indexBuilder::buildIndex);
            assertTrue(ex.getMessage().contains(throwMsg));
        }
    }

    @Test
    void markDisabled() throws Exception {
        final String indexName = "MySimpleRecord$str_value_indexed";

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            assertThat(recordStore.isIndexDisabled(indexName), is(false));
            recordStore.markIndexDisabled(indexName).get();
            assertThat(recordStore.isIndexDisabled(indexName), is(true));
            context.commit();
        }

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            RecordStoreState storeState = recordStore.getRecordStoreState();
            assertEquals(IndexState.DISABLED, storeState.getState(indexName));
        }
    }

    @Test
    void markManyDisabled() {
        final Index[] indexes = new Index[100];
        for (int i = 0; i < indexes.length; i++) {
            indexes[i] = new Index("index-" + i, "str_value_indexed");
        }
        final RecordMetaDataHook hook = metaData -> {
            final RecordTypeBuilder recordType = metaData.getRecordType("MySimpleRecord");
            for (int i = 0; i < indexes.length; i++) {
                metaData.addIndex(recordType, indexes[i]);
            }
        };
        // When running a bunch of mark index in parallel, the store state will be changing asynchronously.
        // But it should never be the case that one update causes another to be lost or that asking about the state
        // of indexes not pending for change causes the store's cache to fail to reflect some changes.
        // Such timing problems take several tries to show up.
        for (int j = 0; j < 10; j++) {
            try (FDBRecordContext context = openContext()) {
                openSimpleRecordStore(context, hook);
                List<CompletableFuture<Boolean>> futures = new ArrayList<>();
                List<Index> shouldBeDisabled = new ArrayList<>();
                // Expected contract: only use isIndexXXX and markIndexXXX and wait for all futures when done.
                for (int i = 0; i < indexes.length; i++) {
                    if ((i % 2 == 0) || (i == 99 && recordStore.isIndexDisabled(indexes[i]))) {
                        futures.add(recordStore.markIndexDisabled(indexes[i]));
                        shouldBeDisabled.add(indexes[i]);
                    }
                }
                AsyncUtil.whenAll(futures).join();
                for (Index index : shouldBeDisabled) {
                    assertThat(index, new TypeSafeMatcher<Index>() {
                        @Override
                        protected boolean matchesSafely(Index item) {
                            return recordStore.isIndexDisabled(index);
                        }

                        @Override
                        public void describeTo(Description description) {
                            description.appendText("a disabled index");
                        }
                    });
                }
            }
        }
    }

    @Test
    void compatibleAfterReadable() throws Exception {
        final String indexName = "MySimpleRecord$str_value_indexed";

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            assertFalse(recordStore.isIndexWriteOnly(indexName));
            recordStore.markIndexWriteOnly(indexName).get();
            assertTrue(recordStore.isIndexWriteOnly(indexName));
            commit(context);
        }

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            assertTrue(recordStore.isIndexWriteOnly(indexName));
        }

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            recordStore.rebuildIndex(recordStore.getRecordMetaData().getIndex(indexName), FDBRecordStore.RebuildIndexReason.TEST).get();
            assertEquals(1, timer.getCount(FDBStoreTimer.Events.REBUILD_INDEX_TEST), "should build new index");
            assertEquals(1, timer.getCount(FDBStoreTimer.Events.REBUILD_INDEX), "should build new index");
            assertTrue(recordStore.isIndexReadable(indexName));
            commit(context);
        }

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            assertTrue(recordStore.getRecordStoreState().allIndexesReadable());
            assertTrue(recordStore.getRecordStoreState().compatibleWith(recordStore.getRecordStoreState()));
        }
    }

    @Test
    void failureToMarkIndexReadableShouldNotBlockCheckVersion() {
        // Deleting an index does not remove it from the disabled list
        // (https://github.com/FoundationDB/fdb-record-layer/issues/515) may cause a new index with the same name
        // unable to use rebuildIndexWithNoRecord (which tries to mark index on new record type readable), but this
        // should not block check version.

        final String reusedIndexName = "reused_index_name";

        // Add index and disable it.
        RecordMetaDataHook metaData2 = builder -> {
            builder.addIndex("MySimpleRecord", new Index(reusedIndexName, "str_value_indexed"));
            builder.setVersion(200);
        };
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, metaData2);
            assertTrue(recordStore.isIndexReadable(reusedIndexName));
            recordStore.markIndexDisabled(reusedIndexName).join();
            assertTrue(recordStore.isIndexDisabled(reusedIndexName));
            commit(context);
        }
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, metaData2);
            assertTrue(recordStore.isIndexDisabled(reusedIndexName));
        }

        // Remove index.
        RecordMetaDataHook metaData3 = builder -> {
            builder.setVersion(300);
        };
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, metaData3);
            assertFalse(recordStore.getRecordMetaData().hasIndex(reusedIndexName));
            commit(context);
        }

        RecordMetaDataHook metaData4 = builder -> {
            builder.setVersion(300);
            // Add another record type: AnotherRecord.
            builder.updateRecords(TestRecords1EvolvedProto.getDescriptor());
            // Add an index on the new record type with the old name. The index should have an added version greater
            // than 300.
            builder.addIndex("AnotherRecord", new Index(reusedIndexName, "num_value_2"));
            builder.setVersion(400);
        };
        try (FDBRecordContext context = openContext()) {
            // It should pass checkVersion even if it failed to rebuildIndexWithNoRecord, and the index is disabled.
            // Also manually checked that there is a log titled with "unable to build index", and the index_name is
            // "reused_index_name".
            openSimpleRecordStore(context, metaData4);
            // The index is disabled
            assertTrue(recordStore.isIndexDisabled(reusedIndexName));
            commit(context);
        }
    }

    @Test
    void rebuildAll() throws Exception {
        final String disabledIndex = "MySimpleRecord$str_value_indexed";
        final String writeOnlyIndex = "MySimpleRecord$num_value_3_indexed";

        TestRecords1Proto.MySimpleRecord record = TestRecords1Proto.MySimpleRecord.newBuilder()
                .setRecNo(1066)
                .setStrValueIndexed("indexed_string")
                .setNumValue3Indexed(42)
                .build();

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            assertTrue(recordStore.isIndexReadable(disabledIndex));
            assertTrue(recordStore.isIndexReadable(writeOnlyIndex));
            recordStore.markIndexDisabled(disabledIndex).get();
            recordStore.markIndexWriteOnly(writeOnlyIndex).get();
            assertTrue(recordStore.isIndexDisabled(disabledIndex));
            assertTrue(recordStore.isIndexWriteOnly(writeOnlyIndex));
            recordStore.saveRecord(record);
            commit(context);
        }

        // Verify the write-only index was updated and the disabled one was not
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            recordStore.uncheckedMarkIndexReadable(disabledIndex).get();
            recordStore.uncheckedMarkIndexReadable(writeOnlyIndex).get();
            assertEquals(Collections.emptyList(),
                    recordStore.scanIndexRecords(disabledIndex).map(FDBIndexedRecord::getRecord).asList().get());
            assertEquals(Collections.singletonList(record),
                    recordStore.scanIndexRecords(writeOnlyIndex).map(FDBIndexedRecord::getRecord).asList().get());
            // do not commit
        }

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            recordStore.rebuildAllIndexes().get();
            assertTrue(recordStore.isIndexReadable(disabledIndex));
            assertTrue(recordStore.isIndexReadable(writeOnlyIndex));
            assertEquals(Collections.singletonList(record),
                    recordStore.scanIndexRecords(disabledIndex).map(FDBIndexedRecord::getRecord).asList().get());
            assertEquals(Collections.singletonList(record),
                    recordStore.scanIndexRecords(writeOnlyIndex).map(FDBIndexedRecord::getRecord).asList().get());
            commit(context);
        }

        // Validate that the index state updates carry over into the next transaction
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            assertTrue(recordStore.isIndexReadable(disabledIndex));
            assertTrue(recordStore.isIndexReadable(writeOnlyIndex));
            assertTrue(recordStore.getRecordStoreState().allIndexesReadable());
        }
    }

    @Test
    void rebuildLimitedByRecordTypes() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);

            TestRecords1Proto.MySimpleRecord.Builder simpleBuilder = TestRecords1Proto.MySimpleRecord.newBuilder();

            simpleBuilder.setRecNo(1);
            simpleBuilder.setNumValue3Indexed(1);
            recordStore.saveRecord(simpleBuilder.build());

            simpleBuilder.setRecNo(2);
            simpleBuilder.setNumValue3Indexed(2);
            recordStore.saveRecord(simpleBuilder.build());

            TestRecords1Proto.MyOtherRecord.Builder otherBuilder = TestRecords1Proto.MyOtherRecord.newBuilder();

            otherBuilder.setRecNo(101);
            otherBuilder.setNumValue3Indexed(101);
            recordStore.saveRecord(otherBuilder.build());

            assertEquals(2, recordStore.scanIndexRecords("MySimpleRecord$num_value_3_indexed").getCount().get().intValue());

            commit(context);
        }

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);

            recordStore.rebuildIndex(recordStore.getRecordMetaData().getIndex("MySimpleRecord$num_value_3_indexed")).get();

            assertEquals(2, recordStore.scanIndexRecords("MySimpleRecord$num_value_3_indexed").getCount().get().intValue());

            commit(context);
        }

    }

    @Test
    void markAbsentWriteOnly() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            assertThrows(MetaDataException.class, () ->
                    recordStore.markIndexWriteOnly("this_index_doesn't_exist").get());
        }
    }

    @Test
    void markAbsentDisabled() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            assertThrows(MetaDataException.class, () ->
                    recordStore.markIndexDisabled("this_index_doesn't_exist").get());
        }
    }

    @Test
    void markAbsentReadable() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            assertThrows(MetaDataException.class, () ->
                    recordStore.markIndexReadable(new Index("this_index_doesn't_exist", Key.Expressions.field("str_value_indexed"))).get());
        }
    }

    @Test
    void isAbsentBuildable() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            assertThrows(MetaDataException.class, () ->
                    recordStore.firstUnbuiltRange(new Index("this_index_doesn't_exist", Key.Expressions.field("str_value_indexed"))).get());
        }
    }

    @Test
    void getIndexStates() throws Exception {
        final String indexName = "MySimpleRecord$str_value_indexed";

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            final Index index = recordStore.getRecordMetaData().getIndex(indexName);
            assertThat(recordStore.getAllIndexStates(), hasEntry(index, IndexState.READABLE));
            recordStore.markIndexWriteOnly(indexName).get();
            assertThat(recordStore.getAllIndexStates(), hasEntry(index, IndexState.WRITE_ONLY));
            context.commit();
        }

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            final Index index = recordStore.getRecordMetaData().getIndex(indexName);
            assertThat(recordStore.getAllIndexStates(), hasEntry(index, IndexState.WRITE_ONLY));
            recordStore.markIndexDisabled(indexName).get();
            assertThat(recordStore.getAllIndexStates(), hasEntry(index, IndexState.DISABLED));
            // does not commit
        }

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            final Index index = recordStore.getRecordMetaData().getIndex(indexName);
            assertThat(recordStore.getAllIndexStates(), hasEntry(index, IndexState.WRITE_ONLY));
            recordStore.saveRecord(TestRecords1Proto.MyOtherRecord.newBuilder()
                    .setRecNo(2)
                    .build()); // so that it will conflict on commit
            try (FDBRecordContext context2 = openContext()) {
                openSimpleRecordStore(context2);
                recordStore.markIndexReadable(indexName).get();
                context2.commit();
            }
            assertThrows(FDBExceptions.FDBStoreTransactionConflictException.class, context::commit);
        }
    }

    @Test
    void failUpdateConcurrentWithStateChange() throws Exception {
        final String indexName = "MySimpleRecord$str_value_indexed";

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            assertThat(recordStore.isIndexDisabled(indexName), is(false));
            recordStore.markIndexDisabled(indexName).get();
            assertThat(recordStore.isIndexDisabled(indexName), is(true));
            commit(context);
        }

        final TestRecords1Proto.MySimpleRecord record = TestRecords1Proto.MySimpleRecord.newBuilder()
                .setRecNo(1066)
                .setStrValueIndexed("indexed_string")
                .build();

        try (FDBRecordContext context1 = openContext(); FDBRecordContext context2 = openContext()) {
            openSimpleRecordStore(context1);
            recordStore.markIndexWriteOnly(indexName);

            openSimpleRecordStore(context2);
            recordStore.saveRecord(record);

            commit(context1);
            assertThrows(FDBExceptions.FDBStoreTransactionConflictException.class, () -> commit(context2));
        }
    }

    @Test
    void uniquenessViolations() throws Exception {
        final String indexName = "MySimpleRecord$num_value_unique";

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            assertThat(recordStore.isIndexWriteOnly(indexName), is(false));
            recordStore.markIndexWriteOnly(indexName).get();
            assertThat(recordStore.isIndexWriteOnly(indexName), is(true));
            context.commit();
        }

        TestRecords1Proto.MySimpleRecord record1 = TestRecords1Proto.MySimpleRecord.newBuilder()
                .setRecNo(1066L)
                .setNumValueUnique(42)
                .build();
        TestRecords1Proto.MySimpleRecord record2 = TestRecords1Proto.MySimpleRecord.newBuilder()
                .setRecNo(1793L)
                .setNumValueUnique(42)
                .build();

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            recordStore.saveRecord(record1);
            recordStore.saveRecord(record2);
            context.commit();
        }

        Index index = recordStore.getRecordMetaData().getIndex("MySimpleRecord$num_value_unique");

        // Test scan uniqueness violations.
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            RecordCursorIterator<RecordIndexUniquenessViolation> cursor = recordStore.scanUniquenessViolations(index).asIterator();

            assertTrue(cursor.hasNext());
            RecordIndexUniquenessViolation first = cursor.next();
            assertEquals(Tuple.from(42L), first.getIndexEntry().getKey());
            assertEquals(Tuple.from(1066L), first.getPrimaryKey());
            assertEquals(Tuple.from(1793L), first.getExistingKey());

            assertTrue(cursor.hasNext());
            RecordIndexUniquenessViolation second = cursor.next();
            assertEquals(Tuple.from(42L), second.getIndexEntry().getKey());
            assertEquals(Tuple.from(1793L), second.getPrimaryKey());
            assertEquals(Tuple.from(1066L), second.getExistingKey());

            assertFalse(cursor.hasNext());
        }

        // Test scan uniqueness violations given value key.
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            RecordCursorIterator<RecordIndexUniquenessViolation> cursor = recordStore.scanUniquenessViolations(index, Key.Evaluated.scalar(42))
                    .asIterator();

            assertTrue(cursor.hasNext());
            RecordIndexUniquenessViolation first = cursor.next();
            assertEquals(Tuple.from(42L), first.getIndexEntry().getKey());
            assertEquals(Tuple.from(1066L), first.getPrimaryKey());
            assertEquals(Tuple.from(1793L), first.getExistingKey());

            assertTrue(cursor.hasNext());
            RecordIndexUniquenessViolation second = cursor.next();
            assertEquals(Tuple.from(42L), second.getIndexEntry().getKey());
            assertEquals(Tuple.from(1793L), second.getPrimaryKey());
            assertEquals(Tuple.from(1066L), second.getExistingKey());

            assertFalse(cursor.hasNext());
        }

        // Several methods of resolving the conflict. These do not commit on purpose.

        // Test requesting to resolve uniqueness violations with a remaining record.
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            recordStore.resolveUniquenessViolation(index, Tuple.from(42), Tuple.from(1066L)).get();
            assertEquals(0, (int)recordStore.scanUniquenessViolations(index).getCount().get());

            assertNotNull(recordStore.loadRecord(Tuple.from(1066L)));
            assertNull(recordStore.loadRecord(Tuple.from(1793L)));
        }

        // Test requesting to resolve uniqueness violations with no remaining record.
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            recordStore.resolveUniquenessViolation(index, Tuple.from(42), null).get();
            assertEquals(0, (int)recordStore.scanUniquenessViolations(index).getCount().get());

            assertNull(recordStore.loadRecord(Tuple.from(1066L)));
            assertNull(recordStore.loadRecord(Tuple.from(1793L)));
        }

        // Test manually resolving uniqueness violations by deleting one record.
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            recordStore.deleteRecordAsync(Tuple.from(1793L)).get();
            assertEquals(0, (int)recordStore.scanUniquenessViolations(index).getCount().get());

            assertNotNull(recordStore.loadRecord(Tuple.from(1066L)));
            assertNull(recordStore.loadRecord(Tuple.from(1793L)));
        }
    }

    @Test
    void uniquenessViolationsWithFanOut() throws Exception {
        Index index = new Index("MySimpleRecord$repeater", field("repeater", FanType.FanOut), Index.EMPTY_VALUE,  IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS);
        RecordMetaDataHook hook = metaData -> metaData.addIndex("MySimpleRecord", index);

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            assertThat(recordStore.isIndexWriteOnly(index), is(false));
            recordStore.markIndexWriteOnly(index).get();
            assertThat(recordStore.isIndexWriteOnly(index), is(true));
            context.commit();
        }

        TestRecords1Proto.MySimpleRecord record1 = TestRecords1Proto.MySimpleRecord.newBuilder()
                .setRecNo(1066L)
                .addRepeater(1).addRepeater(2).addRepeater(3)
                .build();
        TestRecords1Proto.MySimpleRecord record2 = TestRecords1Proto.MySimpleRecord.newBuilder()
                .setRecNo(1793L)
                .addRepeater(2).addRepeater(3)
                .build();
        TestRecords1Proto.MySimpleRecord record3 = TestRecords1Proto.MySimpleRecord.newBuilder()
                .setRecNo(1849L)
                .addRepeater(3)
                .build();

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            recordStore.saveRecord(record1);
            recordStore.saveRecord(record2);
            recordStore.saveRecord(record3);
            context.commit();
        }

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            assertEquals(5, (int)recordStore.scanUniquenessViolations(index).getCount().get());
            assertEquals(0, (int)recordStore.scanUniquenessViolations(index, Key.Evaluated.scalar(1)).getCount().get());
            assertEquals(2, (int)recordStore.scanUniquenessViolations(index, Key.Evaluated.scalar(2)).getCount().get());
            assertEquals(3, (int)recordStore.scanUniquenessViolations(index, Key.Evaluated.scalar(3)).getCount().get());

            RecordCursorIterator<RecordIndexUniquenessViolation> cursor = recordStore
                    .scanUniquenessViolations(index, Key.Evaluated.scalar(3))
                    .asIterator();

            assertTrue(cursor.hasNext());
            RecordIndexUniquenessViolation next = cursor.next();
            assertEquals(Tuple.from(3L), next.getIndexEntry().getKey());
            assertEquals(Tuple.from(1066L), next.getPrimaryKey());
            assertThat(next.getExistingKey(), is(oneOf(Tuple.from(1793L), Tuple.from(1849L))));

            assertTrue(cursor.hasNext());
            next = cursor.next();
            assertEquals(Tuple.from(3L), next.getIndexEntry().getKey());
            assertEquals(Tuple.from(1793L), next.getPrimaryKey());
            assertThat(next.getExistingKey(), is(oneOf(Tuple.from(1066L), Tuple.from(1849L))));

            assertTrue(cursor.hasNext());
            next = cursor.next();
            assertEquals(Tuple.from(3L), next.getIndexEntry().getKey());
            assertEquals(Tuple.from(1849L), next.getPrimaryKey());
            assertThat(next.getExistingKey(), is(oneOf(Tuple.from(1066L), Tuple.from(1793L))));

            assertFalse(cursor.hasNext());

            cursor = recordStore.scanUniquenessViolations(index, Key.Evaluated.scalar(2)).asIterator();

            assertTrue(cursor.hasNext());
            next = cursor.next();
            assertEquals(Tuple.from(2L), next.getIndexEntry().getKey());
            assertEquals(Tuple.from(1066L), next.getPrimaryKey());
            assertEquals(Tuple.from(1793L), next.getExistingKey());

            assertTrue(cursor.hasNext());
            next = cursor.next();
            assertEquals(Tuple.from(2L), next.getIndexEntry().getKey());
            assertEquals(Tuple.from(1793L), next.getPrimaryKey());
            assertEquals(Tuple.from(1066L), next.getExistingKey());

            assertFalse(cursor.hasNext());
        }

        // Several methods of resolving the conflict. These do not commit on purpose.

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            recordStore.resolveUniquenessViolation(index, Key.Evaluated.scalar(3), null).get();
            assertEquals(0, (int)recordStore.scanUniquenessViolations(index).getCount().get());

            assertNull(recordStore.loadRecord(Tuple.from(1066L)));
            assertNull(recordStore.loadRecord(Tuple.from(1793L)));
            assertNull(recordStore.loadRecord(Tuple.from(1849L)));
        }

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            recordStore.resolveUniquenessViolation(index, Tuple.from(3), Tuple.from(1066L)).get();
            assertEquals(0, (int)recordStore.scanUniquenessViolations(index).getCount().get());

            assertNotNull(recordStore.loadRecord(Tuple.from(1066L)));
            assertNull(recordStore.loadRecord(Tuple.from(1793L)));
            assertNull(recordStore.loadRecord(Tuple.from(1849L)));
        }

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            recordStore.resolveUniquenessViolation(index, Tuple.from(3), Tuple.from(1793L)).get();
            assertEquals(0, (int)recordStore.scanUniquenessViolations(index).getCount().get());

            assertNull(recordStore.loadRecord(Tuple.from(1066L)));
            assertNotNull(recordStore.loadRecord(Tuple.from(1793L)));
            assertNull(recordStore.loadRecord(Tuple.from(1849L)));
        }

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            recordStore.resolveUniquenessViolation(index, Tuple.from(3), Tuple.from(1849L)).get();
            assertEquals(0, (int)recordStore.scanUniquenessViolations(index).getCount().get());

            assertNull(recordStore.loadRecord(Tuple.from(1066L)));
            assertNull(recordStore.loadRecord(Tuple.from(1793L)));
            assertNotNull(recordStore.loadRecord(Tuple.from(1849L)));
        }
    }

    @Test
    void noMaintenanceFilteredOnIndex() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openAnyRecordStore(TestRecordsIndexFilteringProto.getDescriptor(), context);

            TestRecordsIndexFilteringProto.MyBasicRecord recordA = TestRecordsIndexFilteringProto.MyBasicRecord.newBuilder()
                    .setRecNo(1001).setNumValue2(101).build();
            recordStore.saveRecord(recordA);
            context.commit();

            Collection<StoreTimer.Event> events = context.getTimer().getEvents();
            assertFalse(events.contains(FDBStoreTimer.Events.SAVE_INDEX_ENTRY));
            assertFalse(events.contains(FDBStoreTimer.Events.SKIP_INDEX_RECORD));
        }

        // add index
        RecordMetaDataHook hook = metaDataBuilder -> {
            metaDataBuilder.setVersion(metaDataBuilder.getVersion() + 1);
            metaDataBuilder.addIndex("MyBasicRecord", "value2$filtered", field("num_value_2"));
        };
        // rebuild the index so that it is in a READABLE state
        try (FDBRecordContext context = openContext()) {
            openAnyRecordStore(TestRecordsIndexFilteringProto.getDescriptor(), context, hook);
            try (OnlineIndexer indexer = OnlineIndexer.forRecordStoreAndIndex(recordStore, "value2$filtered")) {
                indexer.buildIndex();
            }
        }

        try (FDBRecordContext context = openContext()) {
            openAnyRecordStore(TestRecordsIndexFilteringProto.getDescriptor(), context, hook);
            context.getTimer().reset();

            TestRecordsIndexFilteringProto.MyBasicRecord recordA = TestRecordsIndexFilteringProto.MyBasicRecord.newBuilder()
                    .setRecNo(1002).setNumValue2(102).build();

            recordStore.saveRecord(recordA);
            context.commit();

            Collection<StoreTimer.Event> events = context.getTimer().getEvents();
            assertTrue(events.contains(FDBStoreTimer.Events.SAVE_INDEX_ENTRY));
            assertFalse(events.contains(FDBStoreTimer.Events.SKIP_INDEX_RECORD));
        }

        try (FDBRecordContext context = openContext()) {
            RecordMetaDataBuilder metaData = RecordMetaData.newBuilder().setRecords(TestRecordsIndexFilteringProto.getDescriptor());
            hook.apply(metaData);
            IndexMaintenanceFilter noneFilter = (i, r) -> IndexMaintenanceFilter.IndexValues.NONE;
            recordStore = getStoreBuilder(context, metaData.getRecordMetaData())
                    .setIndexMaintenanceFilter(noneFilter)
                    .createOrOpen();
            setupPlanner(null);
            context.getTimer().reset();

            TestRecordsIndexFilteringProto.MyBasicRecord recordB = TestRecordsIndexFilteringProto.MyBasicRecord.newBuilder()
                    .setRecNo(1003).setNumValue2(103).build();
            recordStore.saveRecord(recordB);
            context.commit();

            Collection<StoreTimer.Event> events = context.getTimer().getEvents();
            assertFalse(events.contains(FDBStoreTimer.Events.SAVE_INDEX_ENTRY));
            assertTrue(events.contains(FDBStoreTimer.Events.SKIP_INDEX_RECORD));
        }
    }


    @Test
    void noMaintenanceFilteredIndexOnCheckVersion() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openAnyRecordStore(TestRecordsIndexFilteringProto.getDescriptor(), context);

            TestRecordsIndexFilteringProto.MyBasicRecord recordA = TestRecordsIndexFilteringProto.MyBasicRecord.newBuilder()
                    .setRecNo(1001).setNumValue2(101).build();
            recordStore.saveRecord(recordA);
            context.commit();

            Collection<StoreTimer.Event> events = context.getTimer().getEvents();
            assertFalse(events.contains(FDBStoreTimer.Events.SAVE_INDEX_ENTRY));
            assertFalse(events.contains(FDBStoreTimer.Events.SKIP_INDEX_RECORD));
        }

        // add index
        RecordMetaDataHook hook = metaDataBuilder -> {
            metaDataBuilder.setVersion(metaDataBuilder.getVersion() + 1);
            metaDataBuilder.addIndex("MyBasicRecord", "value2$filtered", field("num_value_2"));
        };

        try (FDBRecordContext context = openContext()) {
            RecordMetaDataBuilder metaData = RecordMetaData.newBuilder().setRecords(TestRecordsIndexFilteringProto.getDescriptor());
            if (hook != null) {
                hook.apply(metaData);
            }

            IndexMaintenanceFilter noneFilter = (i, r) -> IndexMaintenanceFilter.IndexValues.NONE;
            FDBRecordStore fdbRecordStore = FDBRecordStore.newBuilder().setContext(context).setMetaDataProvider(metaData).setKeySpacePath(path)
                    .setIndexMaintenanceFilter(noneFilter)
                    .uncheckedOpen();

            context.getTimer().reset();

            FDBRecordStoreBase.UserVersionChecker userVersionChecker = new FDBRecordStoreBase.UserVersionChecker() {
                @Override
                public CompletableFuture<Integer> checkUserVersion(@Nonnull final RecordMetaDataProto.DataStoreInfo storeHeader, final RecordMetaDataProvider metaData) {
                    return CompletableFuture.completedFuture(1);
                }

                @Deprecated
                @Override
                public CompletableFuture<Integer> checkUserVersion(int oldUserVersion, int oldMetaDataVersion, RecordMetaDataProvider metaData) {
                    throw new RecordCoreException("deprecated checkUserVersion called");
                }

                @Override
                public IndexState needRebuildIndex(Index index, long recordCount, boolean indexOnNewRecordTypes) {
                    return IndexState.READABLE;
                }
            };

            fdbRecordStore.checkVersion(userVersionChecker, FDBRecordStoreBase.StoreExistenceCheck.NONE).get();

            Collection<StoreTimer.Event> events = context.getTimer().getEvents();
            assertFalse(events.contains(FDBStoreTimer.Events.SAVE_INDEX_ENTRY));
            assertTrue(events.contains(FDBStoreTimer.Events.SKIP_INDEX_RECORD));
        }
    }

    @Test
    void invalidIndexField() {
        assertThrows(KeyExpression.InvalidExpressionException.class, () ->
                testInvalidIndex(new Index("broken", field("no_such_field"))));
    }

    @Test
    void testRecreateIndex() throws Exception {
        testChangeIndexDefinition(true, "num_value", "num_value");
    }

    @Test
    public void testChangeIndexDefinitionWithCount() throws Exception {
        testChangeIndexDefinition(true, "num_value", "str_value");
    }

    @Test
    public void testChangeIndexDefinition() throws Exception {
        testChangeIndexDefinition(false, "num_value", "str_value");
    }

    public void testChangeIndexDefinition(boolean withCount,
                                          final String originalIndexFieldName,
                                          final String newIndexFieldName) throws Exception {

        final FDBRecordStoreBase.UserVersionChecker alwaysDisabled = new FDBRecordStoreBase.UserVersionChecker() {
            @Override
            public CompletableFuture<Integer> checkUserVersion(@Nonnull final RecordMetaDataProto.DataStoreInfo storeHeader, final RecordMetaDataProvider metaData) {
                return CompletableFuture.completedFuture(1);
            }

            @Deprecated
            @Override
            public CompletableFuture<Integer> checkUserVersion(int oldUserVersion, int oldMetaDataVersion, RecordMetaDataProvider metaData) {
                throw new RecordCoreException("deprecated checkUserVersion called");
            }

            @Override
            public IndexState needRebuildIndex(Index index, long recordCount, boolean indexOnNewRecordTypes) {
                return IndexState.DISABLED;
            }
        };

        final FDBRecordStoreBase.UserVersionChecker alwaysEnabled = new FDBRecordStoreBase.UserVersionChecker() {
            @Override
            public CompletableFuture<Integer> checkUserVersion(@Nonnull final RecordMetaDataProto.DataStoreInfo storeHeader, final RecordMetaDataProvider metaData) {
                return CompletableFuture.completedFuture(1);
            }

            @Deprecated
            @Override
            public CompletableFuture<Integer> checkUserVersion(int oldUserVersion, int oldMetaDataVersion, RecordMetaDataProvider metaData) {
                throw new RecordCoreException("deprecated checkUserVersion called");
            }

            @Override
            public IndexState needRebuildIndex(Index index, long recordCount, boolean indexOnNewRecordTypes) {
                return IndexState.READABLE;
            }
        };

        try (FDBRecordContext context = openContext()) {
            final RecordMetaDataBuilder builder = RecordMetaData.newBuilder().setRecords(TestNoIndexesProto.getDescriptor());
            if (withCount) {
                builder.addUniversalIndex(globalCountIndex());
            }
            recordStore = FDBRecordStore.newBuilder().setContext(context).setMetaDataProvider(builder).setKeySpacePath(path)
                    .setUserVersionChecker(alwaysEnabled).createOrOpen();
            assertTrue(recordStore.getRecordStoreState().isReadable(COUNT_INDEX_NAME));
            TestNoIndexesProto.MySimpleRecord recordA = TestNoIndexesProto.MySimpleRecord.newBuilder()
                    .setNumValue(3).setStrValue("boo").build();
            recordStore.saveRecord(recordA);
            context.commit();
        }
        final Index originalIndex = new Index("index", originalIndexFieldName);
        originalIndex.setSubspaceKey(1);
        final Index newIndex = new Index("index", newIndexFieldName);
        newIndex.setSubspaceKey(2);
        final String recordType = "MySimpleRecord";
        try (FDBRecordContext context = openContext()) {
            RecordMetaDataBuilder builder = RecordMetaData.newBuilder().setRecords(TestNoIndexesProto.getDescriptor());
            if (withCount) {
                builder.addUniversalIndex(globalCountIndex());
            }
            builder.addIndex(recordType, originalIndex);

            recordStore = FDBRecordStore.newBuilder().setContext(context).setMetaDataProvider(builder).setKeySpacePath(path)
                    .setUserVersionChecker(alwaysDisabled).createOrOpen();

            assertEquals(ImmutableSet.of("index"), recordStore.getRecordStoreState().getDisabledIndexNames());
            context.commit();
        }

        try (FDBRecordContext context = openContext()) {
            RecordMetaDataBuilder builder = RecordMetaData.newBuilder().setRecords(TestNoIndexesProto.getDescriptor());
            if (withCount) {
                builder.addUniversalIndex(globalCountIndex());
            }
            builder.addIndex(recordType, originalIndex);
            builder.removeIndex(originalIndex.getName());
            builder.addIndex(recordType, newIndex);

            recordStore = FDBRecordStore.newBuilder().setContext(context).setMetaDataProvider(builder).setKeySpacePath(path)
                    .setUserVersionChecker(alwaysDisabled).createOrOpen();
            assertEquals(ImmutableSet.of("index"), recordStore.getRecordStoreState().getDisabledIndexNames());

            // the index is already disabled, and we do not start enabling it during checkVersion
            assertFalse(recordStore.checkVersion(alwaysEnabled, FDBRecordStoreBase.StoreExistenceCheck.NONE).get());

            assertEquals(ImmutableSet.of("index"), recordStore.getRecordStoreState().getDisabledIndexNames());
            context.commit();
        }
    }

    @Test
    public void testChangeIndexDefinitionNotReadable() throws Exception {
        try (FDBRecordContext context = openContext()) {
            final RecordMetaDataBuilder builder = RecordMetaData.newBuilder().setRecords(TestNoIndexesProto.getDescriptor());
            recordStore = FDBRecordStore.newBuilder().setContext(context).setMetaDataProvider(builder).setKeySpacePath(path).createOrOpen();
            // Save 200 records without any indexes.
            for (int i = 0; i < 200; i++) {
                TestNoIndexesProto.MySimpleRecord record = TestNoIndexesProto.MySimpleRecord.newBuilder()
                        .setRecNo(i).setNumValue(i + 1000).build();
                recordStore.saveRecord(record);
            }
            context.commit();
        }
        try (FDBRecordContext context = openContext()) {
            // Add a new index named "index" on rec_no.
            final RecordMetaDataBuilder builder = RecordMetaData.newBuilder().setRecords(TestNoIndexesProto.getDescriptor());
            final Index index = new Index("index", "rec_no");
            builder.addIndex("MySimpleRecord", index);
            recordStore = FDBRecordStore.newBuilder().setContext(context).setMetaDataProvider(builder).setKeySpacePath(path).createOrOpen();
            // Since there were 200 records already, the new index is disabled.
            assertEquals(IndexState.DISABLED, recordStore.getRecordStoreState().getState(index.getName()));
            // Add 100 more records. Only these records will be stored in the new index.
            for (int i = 200; i < 300; i++) {
                TestNoIndexesProto.MySimpleRecord record = TestNoIndexesProto.MySimpleRecord.newBuilder()
                        .setRecNo(i).setNumValue(i + 1000).build();
                recordStore.saveRecord(record);
            }
            context.commit();
        }
        RecordMetaData recordMetaData;
        Index index = new Index("index", "num_value");
        try (FDBRecordContext context = openContext()) {
            // Change the definition of the "index" index to be on num_value instead.
            final RecordMetaDataBuilder builder = RecordMetaData.newBuilder().setRecords(TestNoIndexesProto.getDescriptor());
            builder.setVersion(builder.getVersion() + 100);
            builder.addIndex("MySimpleRecord", index);
            recordStore = FDBRecordStore.newBuilder().setContext(context).setMetaDataProvider(builder).setKeySpacePath(path).createOrOpen();
            recordMetaData = recordStore.getRecordMetaData();
            // The changed index is again disabled because there are 300 records.
            assertEquals(IndexState.DISABLED, recordStore.getRecordStoreState().getState(index.getName()));
            // Add 100 more records. These will be recorded in the new index.
            for (int i = 300; i < 400; i++) {
                TestNoIndexesProto.MySimpleRecord record = TestNoIndexesProto.MySimpleRecord.newBuilder()
                        .setRecNo(i).setNumValue(i + 1000).build();
                recordStore.saveRecord(record);
            }
            context.commit();
        }
        // Build the index to make it actually usable.
        try (OnlineIndexer onlineIndexBuilder = OnlineIndexer.newBuilder()
                .setDatabase(fdb)
                .setMetaData(recordMetaData)
                .setIndex(index.getName())
                .setSubspace(recordStore.getSubspace())
                .setIndexMaintenanceFilter(recordStore.getIndexMaintenanceFilter())
                .setFormatVersion(recordStore.getFormatVersion())
                .build()) {
            onlineIndexBuilder.buildIndex();
        }
        try (FDBRecordContext context = openContext()) {
            recordStore = FDBRecordStore.newBuilder().setContext(context).setMetaDataProvider(recordMetaData).setKeySpacePath(path).createOrOpen();
            // New index is fully readable.
            assertEquals(IndexState.READABLE, recordStore.getRecordStoreState().getState(index.getName()));
            List<Long> indexed = recordStore.scanIndex(index, IndexScanType.BY_VALUE, TupleRange.ALL, null, ScanProperties.FORWARD_SCAN)
                    .map(kv -> kv.getKey().getLong(0)).asList().get();
            // Indexes all 400 records exactly once.
            assertEquals(400, indexed.size());
            // Does not have an old definition entry.
            assertFalse(indexed.contains(200L));
            // Has new definition entries.
            assertTrue(indexed.contains(1000L));
            assertTrue(indexed.contains(1399L));
            context.commit();
        }
    }

    @Test
    public void testSelectiveIndexDisable() {
        final FDBRecordStoreBase.UserVersionChecker selectiveEnable = new FDBRecordStoreBase.UserVersionChecker() {
            @Override
            public CompletableFuture<Integer> checkUserVersion(@Nonnull final RecordMetaDataProto.DataStoreInfo storeHeader, final RecordMetaDataProvider metaData) {
                return CompletableFuture.completedFuture(1);
            }

            @Deprecated
            @Override
            public CompletableFuture<Integer> checkUserVersion(int oldUserVersion, int oldMetaDataVersion, RecordMetaDataProvider metaData) {
                throw new RecordCoreException("deprecated checkUserVersion called");
            }

            @Override
            public IndexState needRebuildIndex(Index index, long recordCount, boolean indexOnNewRecordTypes) {
                if (index.getName().equals("index-1")) {
                    return IndexState.DISABLED;
                } else {
                    return IndexState.READABLE;
                }
            }
        };

        final RecordMetaDataBuilder metaData = RecordMetaData.newBuilder().setRecords(TestNoIndexesProto.getDescriptor());
        metaData.addUniversalIndex(globalCountIndex());

        final FDBRecordStore.Builder storeBuilder = FDBRecordStore.newBuilder()
                .setUserVersionChecker(selectiveEnable).setMetaDataProvider(metaData);

        try (FDBRecordContext context = openContext()) {
            recordStore = storeBuilder.setContext(context).setKeySpacePath(path).create(); // builds count index
            assertTrue(recordStore.getRecordStoreState().isReadable(COUNT_INDEX_NAME));
            TestNoIndexesProto.MySimpleRecord recordA = TestNoIndexesProto.MySimpleRecord.newBuilder()
                    .setNumValue(3).setStrValue("boo").build();
            recordStore.saveRecord(recordA);
            context.commit();
        }
        try (FDBRecordContext context = openContext()) {
            metaData.addIndex("MySimpleRecord", "index-1", "num_value");
            metaData.addIndex("MySimpleRecord", "index-2", "str_value");

            recordStore = storeBuilder.setContext(context).setKeySpacePath(path).open(); // builds index-2, disables index-1

            assertEquals(ImmutableSet.of("index-1"), recordStore.getRecordStoreState().getDisabledIndexNames());
            context.commit();
        }
        try (FDBRecordContext context = openContext()) {
            recordStore = storeBuilder.setContext(context).setKeySpacePath(path).open();
            recordStore.clearAndMarkIndexWriteOnly("index-1").join();
            context.commit();
        }
        try (OnlineIndexer onlineIndexBuilder = OnlineIndexer.forRecordStoreAndIndex(recordStore, "index-1")) {
            onlineIndexBuilder.buildIndex();
        }
        try (FDBRecordContext context = openContext()) {
            recordStore = storeBuilder.setContext(context).setKeySpacePath(path).open(); // does not disable anything
            assertEquals(Collections.emptySet(), recordStore.getRecordStoreState().getDisabledIndexNames());
            context.commit();
        }
    }

    @Test
    public void invalidCountNoGroup() {
        assertThrows(KeyExpression.InvalidExpressionException.class, () ->
                testInvalidIndex(new Index("count_ungrouped", field("rec_no"), IndexTypes.COUNT)));
    }

    @Test
    public void invalidCountUpdatesNoGroup() {
        assertThrows(KeyExpression.InvalidExpressionException.class, () ->
                testInvalidIndex(new Index("count_updates_ungrouped", field("rec_no"), IndexTypes.COUNT_UPDATES)));
    }

    @Test
    public void invalidMaxString() {
        assertThrows(KeyExpression.InvalidExpressionException.class, () ->
                testInvalidIndex(new Index("max_string", field("str_value_indexed").ungrouped(), IndexTypes.MAX_EVER_LONG)));
    }

    @Test
    public void invalidSumTwoThings() {
        assertThrows(KeyExpression.InvalidExpressionException.class, () ->
                testInvalidIndex(new Index("sum_two_fields", concatenateFields("num_value_2", "num_value_3").ungrouped(), IndexTypes.SUM)));
    }

    @Test
    public void invalidRankNothing() {
        assertThrows(KeyExpression.InvalidExpressionException.class, () ->
                testInvalidIndex(new Index("max_nothing", new GroupingKeyExpression(EmptyKeyExpression.EMPTY, 0), IndexTypes.RANK)));
    }

    @Test
    public void permissiveIndex() {
        testInvalidIndex(new Index("not_broken", field("no_such_field"), "permissive"));
    }

    @Test
    public void orphanedIndexEntry() throws Exception {
        try (FDBRecordContext context = openContext()) {
            uncheckedOpenSimpleRecordStore(context);

            recordStore.saveRecord(TestRecords1Proto.MySimpleRecord.newBuilder()
                    .setRecNo(1)
                    .setStrValueIndexed("foo")
                    .setNumValueUnique(1)
                    .build());
            recordStore.saveRecord(TestRecords1Proto.MySimpleRecord.newBuilder()
                    .setRecNo(2)
                    .setStrValueIndexed("bar")
                    .setNumValueUnique(2)
                    .build());
            recordStore.saveRecord(TestRecords1Proto.MySimpleRecord.newBuilder()
                    .setRecNo(3)
                    .setStrValueIndexed("baz")
                    .setNumValueUnique(3)
                    .build());
            commit(context);
        }

        // Delete the "bar" record with the index removed.
        try (FDBRecordContext context = openContext()) {
            uncheckedOpenSimpleRecordStore(context, builder -> {
                builder.removeIndex("MySimpleRecord$str_value_indexed");
            });
            recordStore.deleteRecord(Tuple.from(2));
            commit(context);
        }

        // Scan the records normally, this will fail when it hits the index entry that has no
        // associated record.
        try (FDBRecordContext context = openContext()) {
            uncheckedOpenSimpleRecordStore(context);

            // Verify our entries
            Index index = recordStore.getRecordMetaData().getIndex("MySimpleRecord$str_value_indexed");
            assertTrue(recordStore.hasIndexEntryRecord(
                    new IndexEntry(index, Tuple.from("foo", 1), TupleHelpers.EMPTY), IsolationLevel.SERIALIZABLE).get(), "'Foo' should exist");
            assertFalse(recordStore.hasIndexEntryRecord(
                    new IndexEntry(index, Tuple.from("bar", 2), TupleHelpers.EMPTY), IsolationLevel.SERIALIZABLE).get(), "'Bar' should be deleted");
            assertTrue(recordStore.hasIndexEntryRecord(
                    new IndexEntry(index, Tuple.from("baz", 3), TupleHelpers.EMPTY), IsolationLevel.SERIALIZABLE).get(), "'Baz' should exist");

            try {
                recordStore.scanIndexRecords("MySimpleRecord$str_value_indexed").asList().get();
                fail("Scan should have found orphaned record");
            } catch (ExecutionException e) {
                assertEquals("record not found from index entry", e.getCause().getMessage());
            }
            commit(context);
        }

        // Try again, but this time scan allowing orphaned entries.
        try (FDBRecordContext context = openContext()) {
            uncheckedOpenSimpleRecordStore(context);
            List<FDBIndexedRecord<Message>> records =
                    recordStore.scanIndexRecords("MySimpleRecord$str_value_indexed",
                            IndexScanType.BY_VALUE, TupleRange.ALL, null, IndexOrphanBehavior.RETURN, ScanProperties.FORWARD_SCAN).asList().get();
            assertEquals(records.size(), 3);
            for (FDBIndexedRecord<Message> record : records) {
                if (record.getIndexEntry().getKey().getString(0).equals("bar")) {
                    assertFalse(record.hasStoredRecord(), "Entry for 'bar' should be orphaned");
                    assertThrows(RecordCoreException.class, record::getStoredRecord);
                    assertThrows(RecordCoreException.class, record::getRecord);
                } else {
                    assertTrue(record.hasStoredRecord(),
                            "Entry for '" + record.getIndexEntry().getKey() + "' should have an associated record");
                }
            }
            commit(context);
        }

        // Try once again, but this time skipping orphaned entries
        try (FDBRecordContext context = openContext()) {
            uncheckedOpenSimpleRecordStore(context);
            List<FDBIndexedRecord<Message>> records =
                    recordStore.scanIndexRecords("MySimpleRecord$str_value_indexed",
                            IndexScanType.BY_VALUE, TupleRange.ALL, null, IndexOrphanBehavior.SKIP, ScanProperties.FORWARD_SCAN).asList().get();
            assertEquals(records.size(), 2);
            for (FDBIndexedRecord<Message> record : records) {
                assertNotEquals("bar", record.getIndexEntry().getKey().getString(0));
                assertTrue(record.hasStoredRecord(),
                        "Entry for '" + record.getIndexEntry().getKey() + "' should have an associated record");
            }
            commit(context);
        }

        // Validate the index. Should only return the index entry that has no associated record.
        try (FDBRecordContext context = openContext()) {
            uncheckedOpenSimpleRecordStore(context);
            final Index index = recordStore.getRecordMetaData().getIndex("MySimpleRecord$str_value_indexed");
            final List<InvalidIndexEntry> invalidIndexEntries = recordStore.getIndexMaintainer(index)
                    .validateEntries(null, null)
                    .asList().get();
            assertEquals(
                    Collections.singletonList(InvalidIndexEntry.newOrphan(
                            new IndexEntry(index, Tuple.from("bar", 2), TupleHelpers.EMPTY))),
                    invalidIndexEntries,
                    "Validation should return the index entry that has no associated record.");
            commit(context);
        }
    }

    private Set<IndexEntry> setUpIndexOrphanValidation() throws Exception {
        final int nRecords = 20;
        try (FDBRecordContext context = openContext()) {
            uncheckedOpenSimpleRecordStore(context);

            for (int i = 0; i < nRecords; i++) {
                TestRecords1Proto.MySimpleRecord.Builder recBuilder = TestRecords1Proto.MySimpleRecord.newBuilder();
                recBuilder.setRecNo(i);
                recBuilder.setStrValueIndexed(Integer.toString(i));
                recordStore.saveRecord(recBuilder.build());
            }
            commit(context);
        }

        // Delete the even numbered records with the index removed.
        Set<IndexEntry> expectedInvalidEntries = new HashSet<>();
        try (FDBRecordContext context = openContext()) {
            final Index index = recordStore.getRecordMetaData().getIndex("MySimpleRecord$str_value_indexed");
            uncheckedOpenSimpleRecordStore(context, builder ->
                    builder.removeIndex("MySimpleRecord$str_value_indexed"));
            for (int i = 0; i < nRecords; i += 2) {
                recordStore.deleteRecord(Tuple.from(i));
                expectedInvalidEntries.add(new IndexEntry(index, Tuple.from(Integer.toString(i), i), TupleHelpers.EMPTY));
            }
            commit(context);
        }

        return expectedInvalidEntries;
    }

    @Test
    public void testIndexOrphanValidationByIterations() throws Exception {
        Set<IndexEntry> expectedInvalidEntries = setUpIndexOrphanValidation();

        // Validate the index. Should only return the index entry that has no associated record.
        final Random random = new Random();
        byte[] continuation = null;
        Set<IndexEntry> results = new HashSet<>();
        do {
            int limit = random.nextInt(4) + 1; // 1, 2, 3, or 4.
            try (FDBRecordContext context = openContext()) {
                uncheckedOpenSimpleRecordStore(context);
                final Index index = recordStore.getRecordMetaData().getIndex("MySimpleRecord$str_value_indexed");
                final RecordCursorIterator<InvalidIndexEntry> cursor = recordStore.getIndexMaintainer(index)
                        .validateEntries(continuation, null)
                        .limitRowsTo(limit)
                        .asIterator();
                while (cursor.hasNext()) {
                    InvalidIndexEntry invalidIndexEntry = cursor.next();
                    assertEquals(InvalidIndexEntry.Reasons.ORPHAN, invalidIndexEntry.getReason());
                    IndexEntry entry = invalidIndexEntry.getEntry();
                    assertFalse(results.contains(entry), "Entry " + entry + " is duplicated");
                    results.add(entry);
                }
                continuation = cursor.getContinuation();
                commit(context);
            }
        } while (continuation != null);
        assertEquals(expectedInvalidEntries, results,
                "Validation should return the index entry that has no associated record.");

    }

    @Test
    public void testIndexOrphanValidationByAutoContinuingCursor() throws Exception {
        Set<IndexEntry> expectedInvalidEntries = setUpIndexOrphanValidation();

        try (FDBDatabaseRunner runner = fdb.newRunner()) {
            AtomicInteger generatorCount = new AtomicInteger();
            // Set a scanned records limit to mock when the transaction is out of band.
            RecordCursorIterator<InvalidIndexEntry> cursor = new AutoContinuingCursor<>(
                    runner,
                    (context, continuation) -> new LazyCursor<>(
                            FDBRecordStore.newBuilder()
                                    .setContext(context).setKeySpacePath(path).setMetaDataProvider(simpleMetaData(NO_HOOK))
                                    .uncheckedOpenAsync()
                                    .thenApply(currentRecordStore -> {
                                        generatorCount.getAndIncrement();
                                        final Index index = currentRecordStore.getRecordMetaData()
                                                .getIndex("MySimpleRecord$str_value_indexed");
                                        ScanProperties scanProperties = new ScanProperties(ExecuteProperties.newBuilder()
                                                .setReturnedRowLimit(Integer.MAX_VALUE)
                                                .setIsolationLevel(IsolationLevel.SNAPSHOT)
                                                .setScannedRecordsLimit(4)
                                                .build());
                                        return currentRecordStore.getIndexMaintainer(index)
                                                .validateEntries(continuation, scanProperties);
                                    })
                    )
            ).asIterator();

            Set<IndexEntry> results = new HashSet<>();
            while (cursor.hasNext()) {
                InvalidIndexEntry invalidIndexEntry = cursor.next();
                assertEquals(InvalidIndexEntry.Reasons.ORPHAN, invalidIndexEntry.getReason());
                IndexEntry entry = invalidIndexEntry.getEntry();
                assertFalse(results.contains(entry), "Entry " + entry + " is duplicated");
                results.add(entry);
            }
            assertEquals(expectedInvalidEntries, results,
                    "Validation should return the index entry that has no associated record.");
            // The number of scans is about the number of index entries (orphan validation) plus the number of records
            // (missing validation).
            assertThat(generatorCount.get(), greaterThanOrEqualTo((20 + 10) / 4));
        }
    }

    @Test
    public void testIndexMissingValidation() throws Exception {
        final int nRecords = 10;
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);

            for (int i = 0; i < nRecords; i++) {
                TestRecords1Proto.MySimpleRecord.Builder recBuilder = TestRecords1Proto.MySimpleRecord.newBuilder();
                recBuilder.setRecNo(i);
                recBuilder.setStrValueIndexed(Integer.toString(i));
                // nRecords is not larger than 10, so the indexes (sorted by the string version of recNo) are in the
                // same order as the records. This can make the test easy.
                recordStore.saveRecord(recBuilder.build());
            }
            commit(context);
        }

        // Delete the indexes of some records.
        Set<InvalidIndexEntry> expectedInvalidEntries = new HashSet<>();
        try (FDBRecordContext context = openContext()) {
            final Index index = recordStore.getRecordMetaData().getIndex("MySimpleRecord$str_value_indexed");
            openSimpleRecordStore(context);

            List<FDBStoredRecord<Message>> savedRecords = recordStore.scanRecords(
                    TupleRange.ALL, null, ScanProperties.FORWARD_SCAN).asList().get();
            List<IndexEntry> indexEntries = recordStore.scanIndex(
                    index, IndexScanType.BY_VALUE, TupleRange.ALL, null, ScanProperties.FORWARD_SCAN).asList().get();
            for (int i = 0; i < nRecords; i += 2) {
                IndexEntry indexEntry = indexEntries.get(i);
                FDBStoredRecord<Message> record = savedRecords.get(i);

                final Tuple valueKey = indexEntry.getKey();
                final Tuple entryKey = indexEntryKey(index, valueKey, record.getPrimaryKey());
                final byte[] keyBytes = recordStore.indexSubspace(index).pack(valueKey);

                byte[] v0 = recordStore.getContext().ensureActive().get(keyBytes).get();

                recordStore.getContext().ensureActive().clear(keyBytes);
                byte[] v = recordStore.getContext().ensureActive().get(keyBytes).get();
                expectedInvalidEntries.add(InvalidIndexEntry.newMissing(indexEntry, record));
            }
            commit(context);
        }

        try (FDBDatabaseRunner runner = fdb.newRunner()) {
            AtomicInteger generatorCount = new AtomicInteger();
            // Set a scanned records limit to mock when the NoNextReason is out of band.
            RecordCursorIterator<InvalidIndexEntry> cursor = new AutoContinuingCursor<>(
                    runner,
                    (context, continuation) -> new LazyCursor<>(
                            FDBRecordStore.newBuilder()
                                    .setContext(context).setKeySpacePath(path).setMetaDataProvider(simpleMetaData(NO_HOOK))
                                    .openAsync()
                                    .thenApply(currentRecordStore -> {
                                        generatorCount.getAndIncrement();
                                        final Index index = currentRecordStore.getRecordMetaData()
                                                .getIndex("MySimpleRecord$str_value_indexed");
                                        ScanProperties scanProperties = new ScanProperties(ExecuteProperties.newBuilder()
                                                .setReturnedRowLimit(Integer.MAX_VALUE)
                                                .setIsolationLevel(IsolationLevel.SNAPSHOT)
                                                .setScannedRecordsLimit(4)
                                                .build());
                                        return currentRecordStore.getIndexMaintainer(index)
                                                .validateEntries(continuation, scanProperties);
                                    })
                    )
            ).asIterator();

            Set<InvalidIndexEntry> results = new HashSet<>();
            cursor.forEachRemaining(results::add);
            assertEquals(expectedInvalidEntries, results);
            // The number of scans is about the number of index entries (orphan validation) plus the number of records
            // (missing validation).
            assertThat(generatorCount.get(), greaterThanOrEqualTo((5 + 10) / 4));
        }
    }

    private void testInvalidIndex(Index index) {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, metaData -> {
                metaData.addIndex("MySimpleRecord", index);
            });
        }
    }

    @Test
    public void formerIndexes() {
        RecordMetaDataBuilder metaData = RecordMetaData.newBuilder().setRecords(TestRecords1Proto.getDescriptor());

        try (FDBRecordContext context = openContext()) {
            uncheckedOpenRecordStore(context, metaData.getRecordMetaData());
            recordStore.checkVersion(null, FDBRecordStoreBase.StoreExistenceCheck.ERROR_IF_EXISTS).join();
            context.commit();
        }

        metaData.addIndex("MySimpleRecord", "num_value_2");

        try (FDBRecordContext context = openContext()) {
            uncheckedOpenRecordStore(context, metaData.getRecordMetaData());
            timer.reset();
            recordStore.checkVersion(null, FDBRecordStoreBase.StoreExistenceCheck.ERROR_IF_NOT_EXISTS).join();
            assertEquals(1, timer.getCount(FDBStoreTimer.Events.REBUILD_INDEX_FEW_RECORDS));
            assertEquals(1, timer.getCount(FDBStoreTimer.Events.REBUILD_INDEX));
            assertEquals(0, timer.getCount(FDBStoreTimer.Events.REMOVE_FORMER_INDEX));
            context.commit();
        }

        metaData.removeIndex("MySimpleRecord$num_value_2");

        try (FDBRecordContext context = openContext()) {
            uncheckedOpenRecordStore(context, metaData.getRecordMetaData());
            timer.reset();
            recordStore.checkVersion(null, FDBRecordStoreBase.StoreExistenceCheck.ERROR_IF_NOT_EXISTS).join();
            assertEquals(0, timer.getCount(FDBStoreTimer.Events.REBUILD_INDEX_FEW_RECORDS));
            assertEquals(0, timer.getCount(FDBStoreTimer.Events.REBUILD_INDEX));
            assertEquals(1, timer.getCount(FDBStoreTimer.Events.REMOVE_FORMER_INDEX));
            context.commit();
        }

        metaData.addIndex("MySimpleRecord", "num_value_2");
        metaData.getIndex("MySimpleRecord$num_value_2").setSubspaceKey("MySimpleRecord$num_value_2_prime");
        metaData.removeIndex("MySimpleRecord$num_value_2");

        try (FDBRecordContext context = openContext()) {
            uncheckedOpenRecordStore(context, metaData.getRecordMetaData());
            timer.reset();
            recordStore.checkVersion(null, FDBRecordStoreBase.StoreExistenceCheck.ERROR_IF_NOT_EXISTS).join();
            assertEquals(0, timer.getCount(FDBStoreTimer.Events.REBUILD_INDEX_FEW_RECORDS));
            assertEquals(0, timer.getCount(FDBStoreTimer.Events.REBUILD_INDEX));
            assertEquals(0, timer.getCount(FDBStoreTimer.Events.REMOVE_FORMER_INDEX));
            context.commit();
        }
    }

    @Test
    public void recreateIndexWithNewSubspaceKey() {
        RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder().setRecords(TestRecords1Proto.getDescriptor());

        final Object subspaceKey1 = 1L;
        final Index index1 = new Index("MySimpleRecord$num_value_2", "num_value_2");
        index1.setSubspaceKey(subspaceKey1);
        metaDataBuilder.addIndex("MySimpleRecord", index1);
        int version1;

        TestRecords1Proto.MySimpleRecord record;
        try (FDBRecordContext context = openContext()) {
            uncheckedOpenRecordStore(context, metaDataBuilder.getRecordMetaData());
            version1 = recordStore.getRecordMetaData().getVersion();
            recordStore.checkVersion(null, FDBRecordStoreBase.StoreExistenceCheck.ERROR_IF_EXISTS).join();

            record = TestRecords1Proto.MySimpleRecord.newBuilder()
                    .setRecNo(1066L)
                    .setNumValue2(42)
                    .build();
            recordStore.saveRecord(record);

            List<FDBIndexedRecord<Message>> indexedRecords = recordStore.scanIndexRecords(index1.getName())
                    .asList()
                    .join();
            assertThat(indexedRecords, hasSize(1));
            assertEquals(record, indexedRecords.get(0).getRecord());
            assertThat(context.ensureActive().getRange(recordStore.getSubspace().range(Tuple.from(FDBRecordStoreKeyspace.INDEX.key(), subspaceKey1))).asList().join(), hasSize(1));

            commit(context);
        }

        metaDataBuilder.removeIndex(index1.getName());

        int version2;
        FormerIndex formerIndex;
        try (FDBRecordContext context = openContext()) {
            uncheckedOpenRecordStore(context, metaDataBuilder.getRecordMetaData());

            // Validate the index is no longer in the meta-data and a former index has taken its place
            RecordMetaData metaData = recordStore.getRecordMetaData();
            version2 = metaData.getVersion();
            assertThat(version2, greaterThan(version1));
            assertThat(metaData.getAllIndexes().stream().filter(index -> index1.getName().equals(index.getName())).collect(Collectors.toSet()), empty());
            List<FormerIndex> newFormerIndexes = metaData.getFormerIndexesSince(version1);
            assertThat(newFormerIndexes, hasSize(1));
            formerIndex = newFormerIndexes.get(0);
            assertEquals(index1.getName(), formerIndex.getFormerName());
            assertEquals(index1.getAddedVersion(), formerIndex.getAddedVersion());
            assertThat(formerIndex.getRemovedVersion(), both(greaterThan(version1)).and(lessThanOrEqualTo(version2)));
            assertEquals(subspaceKey1, formerIndex.getSubspaceKey());

            // Make sure the former indexing subspace is cleared out by checkVersion
            timer.reset();
            recordStore.checkVersion(null, FDBRecordStoreBase.StoreExistenceCheck.ERROR_IF_NOT_EXISTS).join();
            assertEquals(1L, timer.getCount(FDBStoreTimer.Events.REMOVE_FORMER_INDEX));
            assertThat(context.ensureActive().getRange(recordStore.getSubspace().range(Tuple.from(FDBRecordStoreKeyspace.INDEX.key(), subspaceKey1))).asList().join(), empty());

            // purposefully don't commit
        }

        final Object subspaceKey2 = 2L;
        final Index index2 = new Index(index1.getName(), index1.getRootExpression(), index1.getType());
        index2.setSubspaceKey(subspaceKey2);
        metaDataBuilder.addIndex("MySimpleRecord", index2);

        try (FDBRecordContext context = openContext()) {
            uncheckedOpenRecordStore(context, metaDataBuilder.getRecordMetaData());

            RecordMetaData metaData = recordStore.getRecordMetaData();
            int version3 = metaData.getVersion();
            assertThat(version3, greaterThan(version2));
            assertThat(index2.getAddedVersion(), both(greaterThan(version2)).and(lessThanOrEqualTo(version3)));
            assertThat(index2.getLastModifiedVersion(), both(greaterThan(version2)).and(lessThanOrEqualTo(version3)));
            assertEquals(index1.getName(), index2.getName());
            assertNotEquals(index1.getSubspaceKey(), index2.getSubspaceKey());
            assertTrue(metaData.getIndexesSince(version1).containsKey(index2));

            List<FormerIndex> newFormerIndexes = metaData.getFormerIndexesSince(version1);
            assertThat(newFormerIndexes, hasSize(1));
            assertEquals(formerIndex, newFormerIndexes.get(0));
            assertThat(metaData.getFormerIndexesSince(version2), empty());

            // Make sure the former index is cleared out by checkVersion
            timer.reset();
            recordStore.checkVersion(null, FDBRecordStoreBase.StoreExistenceCheck.ERROR_IF_NOT_EXISTS).join();
            assertEquals(1L, timer.getCount(FDBStoreTimer.Events.REMOVE_FORMER_INDEX));
            assertThat(context.ensureActive().getRange(recordStore.getSubspace().range(Tuple.from(FDBRecordStoreKeyspace.INDEX.key(), subspaceKey1))).asList().join(), empty());

            // Make sure the new index subspace key is used for the index now
            if (!recordStore.isIndexReadable(index2)) {
                recordStore.rebuildIndex(index2).join();
            }
            List<FDBIndexedRecord<Message>> indexedRecords = recordStore.scanIndexRecords(index1.getName())
                    .asList()
                    .join();
            assertThat(indexedRecords, hasSize(1));
            assertEquals(record, indexedRecords.get(0).getRecord());
            assertThat(context.ensureActive().getRange(recordStore.getSubspace().range(Tuple.from(FDBRecordStoreKeyspace.INDEX.key(), subspaceKey2))).asList().join(), hasSize(1));

            commit(context);
        }
    }

    @Test
    @SuppressWarnings("deprecation")
    void testBoundaryPrimaryKeys() {
        runLocalityTest(() -> testBoundaryPrimaryKeysImpl());
    }

    @SuppressWarnings("removal")
    public void testBoundaryPrimaryKeysImpl() {
        final FDBDatabaseFactory factory = dbExtension.getDatabaseFactory();
        factory.setLocalityProvider(MockedLocalityUtil.instance());
        factory.clear();
        FDBDatabase database = factory.getDatabase();
        assertThat(database.getLocalityProvider(), instanceOf(MockedLocalityUtil.class));

        final String indexName = "MySimpleRecord$num_value_unique";
        try (FDBRecordContext context = database.openContext()) {
            openSimpleRecordStore(context, TEST_SPLIT_HOOK);
            recordStore.markIndexWriteOnly(indexName).join();
            commit(context);
        }

        ArrayList<byte[]> keys = new ArrayList<>();
        String bigOlString = Strings.repeat("x", SplitHelper.SPLIT_RECORD_SIZE + 2);
        for (int i = 0; i < 50; i++) {
            saveAndSplitSimpleRecord(i, bigOlString, i);
            keys.add(recordStore.recordsSubspace().pack(i));
        }

        OnlineIndexer indexer;
        List<Tuple> boundaryPrimaryKeys;
        TupleRange range;
        try (FDBRecordContext context = database.openContext()) {
            openSimpleRecordStore(context, TEST_SPLIT_HOOK);
            MockedLocalityUtil.init(keys, new Random().nextInt(keys.size() - 2) + 3); // 3 <= rangeCount <= size
            Index index = recordStore.getRecordMetaData().getIndex(indexName);

            // The indexer only uses recordStore as a prototype so does not require the original record store is still
            // active.
            indexer = OnlineIndexer.newBuilder()
                    .setDatabase(database).setRecordStore(recordStore).setIndex(index)
                    .build();

            range = recordStore.context.asyncToSync(FDBStoreTimer.Waits.WAIT_BUILD_ENDPOINTS,
                    indexer.buildEndpoints());

            logger.info("The endpoints are " + range);

            boundaryPrimaryKeys = recordStore.context.asyncToSync(FDBStoreTimer.Waits.WAIT_GET_BOUNDARY,
                    recordStore.getPrimaryKeyBoundaries(range.getLow(), range.getHigh()).asList());

            logger.info("The boundary primary keys are " + boundaryPrimaryKeys);

            commit(context);
        }

        int boundaryPrimaryKeysSize = boundaryPrimaryKeys.size();
        assertTrue(boundaryPrimaryKeysSize > 2,
                "the test is meaningless if the records are not across boundaries");
        assertThat( boundaryPrimaryKeys.get(0), greaterThanOrEqualTo(Tuple.from(-25L * 39)));
        assertThat( boundaryPrimaryKeys.get(boundaryPrimaryKeysSize - 1), lessThanOrEqualTo(Tuple.from(24L * 39)));
        assertEquals(boundaryPrimaryKeys.stream().sorted().distinct().collect(Collectors.toList()), boundaryPrimaryKeys,
                "the list should be sorted without duplication.");
        for (Tuple boundaryPrimaryKey : boundaryPrimaryKeys) {
            assertEquals(1, boundaryPrimaryKey.size(), "primary keys should be a single value");
        }

        // Test splitIndexBuildRange.
        assertEquals(1, indexer.splitIndexBuildRange(Integer.MAX_VALUE, Integer.MAX_VALUE).size(),
                "the range is not split when it cannot be split to at least minSplit ranges");
        checkSplitIndexBuildRange(1, 2, null, indexer); // to test splitting into fewer than the default number of split points
        checkSplitIndexBuildRange(boundaryPrimaryKeysSize / 2, boundaryPrimaryKeysSize - 2, null, indexer); // to test splitting into fewer than the default number of split points
        checkSplitIndexBuildRange(boundaryPrimaryKeysSize / 2, boundaryPrimaryKeysSize, null, indexer);

        List<Pair<Tuple, Tuple>> oneRangePerSplit = getOneRangePerSplit(range, boundaryPrimaryKeys);
        checkSplitIndexBuildRange(boundaryPrimaryKeysSize / 2, boundaryPrimaryKeysSize + 1, oneRangePerSplit, indexer); // to test exactly one range for each split
        checkSplitIndexBuildRange(boundaryPrimaryKeysSize / 2, boundaryPrimaryKeysSize + 2, oneRangePerSplit, indexer);
        checkSplitIndexBuildRange(boundaryPrimaryKeysSize / 2, Integer.MAX_VALUE, oneRangePerSplit, indexer); // to test that integer overflow isn't a problem

        indexer.close();
        database.close();
    }

    @Test
    public void testNoBoundaryPrimaryKeys() {
        runLocalityTest(() -> testNoBoundaryPrimaryKeysImpl());
    }

    @SuppressWarnings("removal")
    public void testNoBoundaryPrimaryKeysImpl() {
        final FDBDatabaseFactory factory = dbExtension.getDatabaseFactory();
        factory.setLocalityProvider(MockedLocalityUtil.instance());
        FDBDatabase database = factory.getDatabase();

        final String indexName = "MySimpleRecord$num_value_unique";
        try (FDBRecordContext context = database.openContext()) {
            openSimpleRecordStore(context, TEST_SPLIT_HOOK);
            recordStore.markIndexWriteOnly(indexName).join();
            commit(context);
        }

        String bigOlString = Strings.repeat("x", SplitHelper.SPLIT_RECORD_SIZE + 2);
        saveAndSplitSimpleRecord(1, bigOlString, 1);
        saveAndSplitSimpleRecord(2, bigOlString, 2);

        OnlineIndexer indexer;
        List<Tuple> boundaryPrimaryKeys;
        TupleRange range;
        try (FDBRecordContext context = database.openContext()) {
            openSimpleRecordStore(context, TEST_SPLIT_HOOK);
            Index index = recordStore.getRecordMetaData().getIndex(indexName);

            // The indexer only uses recordStore as a prototype so does not require the original record store is still
            // active.
            indexer = OnlineIndexer.newBuilder()
                    .setDatabase(fdb).setRecordStore(recordStore).setIndex(index)
                    .build();

            range = recordStore.context.asyncToSync(FDBStoreTimer.Waits.WAIT_BUILD_ENDPOINTS,
                    indexer.buildEndpoints());
            logger.info("The endpoints are " + range);

            MockedLocalityUtil.init(new ArrayList<>(Arrays.asList(recordStore.recordsSubspace().pack(1), recordStore.recordsSubspace().pack(2))), 0);
            boundaryPrimaryKeys = recordStore.context.asyncToSync(FDBStoreTimer.Waits.WAIT_GET_BOUNDARY,
                    recordStore.getPrimaryKeyBoundaries(range.getLow(), range.getHigh()).asList());
            assertEquals(0, boundaryPrimaryKeys.size());
            logger.info("The boundary primary keys are " + boundaryPrimaryKeys);

            commit(context);
        }

        // Test splitIndexBuildRange.
        assertEquals(1, indexer.splitIndexBuildRange(Integer.MAX_VALUE, Integer.MAX_VALUE).size());

        indexer.close();
        database.close();
    }

    private List<Pair<Tuple, Tuple>> getOneRangePerSplit(TupleRange tupleRange, List<Tuple> boundaries) {
        List<Tuple> newBoundaries = new ArrayList<>(boundaries);
        if (tupleRange.getLow().compareTo(boundaries.get(0)) < 0) {
            newBoundaries.add(0, tupleRange.getLow());
        }
        if (tupleRange.getHigh().compareTo(boundaries.get(boundaries.size() - 1)) > 0) {
            newBoundaries.add(tupleRange.getHigh());
        }

        List<Pair<Tuple, Tuple>> oneRangePerSplit = new ArrayList<>();
        for (int i = 0; i < newBoundaries.size() - 1; i++) {
            oneRangePerSplit.add(Pair.of(newBoundaries.get(i), newBoundaries.get(i + 1)));
        }
        return oneRangePerSplit;
    }

    @SuppressWarnings("removal")
    private void checkSplitIndexBuildRange(int minSplit, int maxSplit,
                                           @Nullable List<Pair<Tuple, Tuple>> expectedSplitRanges,
                                           OnlineIndexer indexer) {
        List<Pair<Tuple, Tuple>> splitRanges = indexer.splitIndexBuildRange(minSplit, maxSplit);

        if (expectedSplitRanges != null) {
            assertEquals(expectedSplitRanges, splitRanges);
        }

        assertThat(splitRanges.size(), greaterThanOrEqualTo(minSplit));
        assertThat(splitRanges.size(), lessThanOrEqualTo(maxSplit));

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, TEST_SPLIT_HOOK);

            // Make sure each endpoint is a valid primary key.
            splitRanges.forEach(range -> {
                assertTrue(recordStore.recordExists(range.getLeft()));
                assertTrue(recordStore.recordExists(range.getRight()));
                recordStore.loadRecord(range.getRight());
            });

            commit(context);
        }
    }

    @Test
    public void testMockedLocalityUtil() {
        runLocalityTest(() -> testMockedLocalityUtilImpl());
    }

    public void testMockedLocalityUtilImpl() {
        FDBDatabase database = dbExtension.getDatabase();

        try (FDBRecordContext context = database.openContext()) {
            openSimpleRecordStore(context, TEST_SPLIT_HOOK);
            ArrayList<byte[]> keys = new ArrayList<>();
            for (int i = 0; i < 50; i++) {
                keys.add(recordStore.recordsSubspace().pack(i));
            }
            byte[] upperBound = recordStore.recordsSubspace().pack(50);

            testRangeCount(context, keys, upperBound, 0);
            testRangeCount(context, keys, upperBound, 1);
            testRangeCount(context, keys, upperBound, new Random().nextInt(keys.size()) + 1);
            testRangeCount(context, keys, upperBound, keys.size() - 1);
            testRangeCount(context, keys, upperBound, keys.size());
            IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> testRangeCount(context, keys, upperBound, keys.size() + 5));
            assertEquals(ex.getMessage(), "rangeCount must be less than (or equal) the size of keys");

            testRangeCount(context, keys, keys.get(keys.size() - 1), 0);
            testRangeCount(context, keys, keys.get(keys.size() - 1), 1);
            testRangeCount(context, keys, keys.get(keys.size() - 1), new Random().nextInt(keys.size()) + 1);
            testRangeCount(context, keys, keys.get(keys.size() - 1), keys.size() - 1);
            testRangeCount(context, keys, keys.get(keys.size() - 1), keys.size());
            commit(context);
        }
        database.close();
    }

    private void testRangeCount(@Nonnull FDBRecordContext context, @Nonnull ArrayList<byte[]> keys, byte[] upperBound, int rangeCount) {
        MockedLocalityUtil.init(keys, rangeCount);
        CloseableAsyncIterator<byte[]> cursor = MockedLocalityUtil.instance().getBoundaryKeys(context.ensureActive(), keys.get(0), upperBound);
        assertTrue(rangeCount == Iterators.size(cursor) || MockedLocalityUtil.getLastRange().equals(upperBound));
        cursor.close();
    }

    private void runLocalityTest(Runnable test) {
        final FDBDatabaseFactory factory = dbExtension.getDatabaseFactory();
        final FDBLocalityProvider origProvider = factory.getLocalityProvider();
        try {
            test.run();
        } finally {
            factory.setLocalityProvider(origProvider);
        }
    }
}
