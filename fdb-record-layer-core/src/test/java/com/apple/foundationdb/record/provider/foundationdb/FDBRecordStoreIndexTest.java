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

import com.apple.foundationdb.FDBException;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.FunctionNames;
import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.IndexState;
import com.apple.foundationdb.record.IsolationLevel;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordIndexUniquenessViolation;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.RecordMetaDataProvider;
import com.apple.foundationdb.record.RecordStoreState;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TestNoIndexesProto;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.TestRecordsIndexFilteringProto;
import com.apple.foundationdb.record.TupleRange;
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
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.TupleHelpers;
import com.apple.test.Tags;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.Message;
import org.apache.commons.lang3.tuple.Pair;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;
import static com.apple.foundationdb.record.metadata.Key.Expressions.concatenateFields;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isOneOf;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
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
public class FDBRecordStoreIndexTest extends FDBRecordStoreTestBase {
    private static final Logger logger = LoggerFactory.getLogger(FDBRecordStoreIndexTest.class);

    @Test
    public void uniqueness() throws Exception {
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
    public void nonUniqueNull() throws Exception {
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
    public void uniqueNull() throws Exception {
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
    public void sumIndex() throws Exception {
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
    public void sumUnsetOptional() throws Exception {
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
    public void sumBoundIndex() throws Exception {
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

            final Optional<IndexAggregateFunction> boundSubtotal = IndexFunctionHelper.bindAggregateFunction(recordStore, subtotal, allTypes);
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

        @SuppressWarnings({"deprecation","squid:CallToDeprecatedMethod"})
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

        @SuppressWarnings({"deprecation","squid:CallToDeprecatedMethod"})
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
    public void minMaxIndex(MinMaxIndexTypes indexTypes) throws Exception {
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
    public void minMaxOptional(MinMaxIndexTypes indexTypes) throws Exception {
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
    public void minMaxTupleUngrouped() throws Exception {
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
    public void minMaxTupleGrouped() throws Exception {
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
    public void minMaxTupleRepeated() throws Exception {
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
    public void minMaxTupleRepeatConcatenated() throws Exception {
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
    public void minMaxValue() throws Exception {
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
    public void minMaxValueNegative() throws Exception {
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
    public void countValueIndex() throws Exception {
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
    public void countMultiValueIndex() throws Exception {
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

    @Test
    public void scanWriteOnlyIndex() throws Exception {
        final String indexName = "MySimpleRecord$num_value_3_indexed";

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            recordStore.markIndexWriteOnly(indexName).get();
            assertThat(recordStore.isIndexReadable(indexName), is(false));
            assertThat(recordStore.isIndexWriteOnly(indexName), is(true));

            try {
                recordStore.scanIndexRecords(indexName);
                fail("Was not stopped from scanning write-only index");
            } catch (RecordCoreException e) {
                assertEquals("Cannot scan non-readable index " + indexName, e.getMessage());
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
        } catch (RecordCoreException e) {
            assertEquals("Cannot scan non-readable index " + indexName , e.getMessage());
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
    public void scanDisabledIndex() throws Exception {
        final String indexName = "MySimpleRecord$num_value_3_indexed";

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            recordStore.markIndexDisabled(indexName).get();
            assertThat(recordStore.isIndexReadable(indexName), is(false));
            assertThat(recordStore.isIndexDisabled(indexName), is(true));

            try {
                recordStore.scanIndexRecords(indexName);
                fail("Was not stopped from scanning disabled index");
            } catch (RecordCoreException e) {
                assertEquals("Cannot scan non-readable index " + indexName, e.getMessage());
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
        } catch (RecordCoreException e) {
            assertEquals("Cannot scan non-readable index " + indexName, e.getMessage());
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
    public void modifyIndexState() throws Exception {
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
            assertThat(((FDBException)e.getCause()).getCode(), equalTo(1020)); // not_committed
        }

        commit(context2); // context2 didn't need index, so this should succeed
    }

    @Test
    public void insertTerrible() throws Exception {
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
    public void insertValueBuggy() throws Exception {
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
    public void markReadable() throws Exception {
        final String indexName = "MySimpleRecord$str_value_indexed";

        try (FDBRecordContext context = openContext()) {
            uncheckedOpenSimpleRecordStore(context);
            assertThat(recordStore.isIndexWriteOnly(indexName), is(false));
            recordStore.markIndexWriteOnly(indexName).get();
            assertThat(recordStore.isIndexWriteOnly(indexName), is(true));
            context.commit();
        }

        try (FDBRecordContext context = openContext()) {
            uncheckedOpenSimpleRecordStore(context);
            Index index = recordStore.getRecordMetaData().getIndex(indexName);
            assertThat(recordStore.isIndexReadable(index), is(false));
            try (OnlineIndexer indexBuilder = OnlineIndexer.newBuilder().setRecordStore(recordStore).setIndex(index).build()) {
                indexBuilder.buildRange(recordStore, null, Key.Evaluated.scalar(1066L)).get();
                Optional<Range> firstUnbuilt = recordStore.firstUnbuiltRange(index).get();
                assertTrue(firstUnbuilt.isPresent());
                assertArrayEquals(Key.Evaluated.scalar(1066).toTuple().pack(), firstUnbuilt.get().begin);
                assertArrayEquals(new byte[] {(byte)0xff}, firstUnbuilt.get().end);
                recordStore.markIndexReadable(index).handle((built, e) -> {
                    assertNotNull(e);
                    assertThat(e, instanceOf(CompletionException.class));
                    assertNotNull(e.getCause());
                    assertThat(e.getCause(), instanceOf(FDBRecordStore.IndexNotBuiltException.class));
                    return null;
                }).get();
                assertThat(recordStore.isIndexReadable(index), is(false));

                indexBuilder.buildRange(recordStore, Key.Evaluated.scalar(1066L), null).get();
                assertFalse(recordStore.firstUnbuiltRange(index).get().isPresent());
                assertTrue(recordStore.markIndexReadable(index).get());
                assertFalse(recordStore.markIndexReadable(index).get());
                assertThat(recordStore.isIndexReadable(index), is(true));
            }
            // Not committing.
        }

        try (FDBRecordContext context = openContext()) {
            uncheckedOpenSimpleRecordStore(context);
            Index index = recordStore.getRecordMetaData().getIndex(indexName);
            assertThat(recordStore.isIndexReadable(index), is(false));
            try (OnlineIndexer indexBuilder = OnlineIndexer.newBuilder().setRecordStore(recordStore).setIndex(index).build()) {
                indexBuilder.buildRange(recordStore, null, Key.Evaluated.scalar(1066L)).get();
            }
            Optional<Range> firstUnbuilt = recordStore.firstUnbuiltRange(index).get();
            assertTrue(firstUnbuilt.isPresent());
            assertArrayEquals(Key.Evaluated.scalar(1066).toTuple().pack(), firstUnbuilt.get().begin);
            assertArrayEquals(new byte[]{(byte)0xff}, firstUnbuilt.get().end);
            assertTrue(recordStore.uncheckedMarkIndexReadable(index.getName()).get());
            assertFalse(recordStore.uncheckedMarkIndexReadable(index.getName()).get());
            assertThat(recordStore.isIndexReadable(index), is(true));

            // Purposefully, checking to mark an index readable that is already
            // readable does not throw an error.
            firstUnbuilt = recordStore.firstUnbuiltRange(index).get();
            assertTrue(firstUnbuilt.isPresent());
            assertArrayEquals(Key.Evaluated.scalar(1066).toTuple().pack(), firstUnbuilt.get().begin);
            assertArrayEquals(new byte[]{(byte)0xff}, firstUnbuilt.get().end);
            assertFalse(recordStore.markIndexReadable(index.getName()).get());
            assertThat(recordStore.isIndexReadable(index), is(true));
        }
    }

    @Test
    public void markDisabled() throws Exception {
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
            assertEquals(new RecordStoreState(Collections.singletonMap(indexName, IndexState.DISABLED)), storeState);
            assertEquals(IndexState.DISABLED, storeState.getState(indexName));
        }
    }

    @Test
    public void markManyDisabled() throws Exception {
        final Index[] indexes = new Index[100];
        for (int i = 0; i < indexes.length; i++) {
            indexes[i] = new Index(String.format("index-%d", i), "str_value_indexed");
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
    public void compatibleAfterReadable() throws Exception {
        final String indexName = "MySimpleRecord$str_value_indexed";

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            assertThat(recordStore.isIndexWriteOnly(indexName), is(false));
            recordStore.markIndexWriteOnly(indexName).get();
            assertThat(recordStore.isIndexWriteOnly(indexName), is(true));
            context.commit();
        }

        RecordMetaData metaData;
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            metaData = recordStore.getRecordMetaData();
        }

        assertEquals(new RecordStoreState(Collections.singletonMap(indexName, IndexState.WRITE_ONLY)), recordStore.getRecordStoreState());

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            recordStore.rebuildIndex(recordStore.getRecordMetaData().getIndex(indexName), null, FDBRecordStore.RebuildIndexReason.TEST).get();
            assertThat(recordStore.isIndexReadable(indexName), is(true));
            context.commit();
        }

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            assertEquals(RecordStoreState.EMPTY, recordStore.getRecordStoreState());
            assertEquals(RecordStoreState.EMPTY, recordStore.getRecordStoreState());
            assertTrue(recordStore.getRecordStoreState().compatibleWith(recordStore.getRecordStoreState()));
        }
    }

    @Test
    public void rebuildLimitedByRecordTypes() throws Exception {
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
    public void markAbsentWriteOnly() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            assertThrows(MetaDataException.class, () ->
                    recordStore.markIndexWriteOnly("this_index_doesn't_exist").get());
        }
    }

    @Test
    public void markAbsentDisabled() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            assertThrows(MetaDataException.class, () ->
                    recordStore.markIndexDisabled("this_index_doesn't_exist").get());
        }
    }

    @Test
    public void markAbsentReadable() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            assertThrows(MetaDataException.class, () ->
                    recordStore.markIndexReadable(new Index("this_index_doesn't_exist", Key.Expressions.field("str_value_indexed"))).get());
        }
    }

    @Test
    public void isAbsentBuildable() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            assertThrows(MetaDataException.class, () ->
                    recordStore.firstUnbuiltRange(new Index("this_index_doesn't_exist", Key.Expressions.field("str_value_indexed"))).get());
        }
    }

    @Test
    public void failUpdateConcurrentWithStateChange() throws Exception {
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
    public void uniquenessViolations() throws Exception {
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

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            RecordCursor<RecordIndexUniquenessViolation> cursor = recordStore.scanUniquenessViolations(index);

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

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            RecordCursor<RecordIndexUniquenessViolation> cursor = recordStore.scanUniquenessViolations(index, Key.Evaluated.scalar(42));

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

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            recordStore.resolveUniquenessViolation(index, Tuple.from(42), Tuple.from(1066L)).get();
            assertNotNull(recordStore.loadRecord(Tuple.from(1066L)));
            assertNull(recordStore.loadRecord(Tuple.from(1793L)));
            context.commit();
        }

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            RecordCursor<RecordIndexUniquenessViolation> cursor = recordStore.scanUniquenessViolations(index);
            assertFalse(cursor.hasNext());

            // reintroduce the error
            recordStore.saveRecord(record1);
            recordStore.saveRecord(record2);
            context.commit();
        }

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            RecordCursor<RecordIndexUniquenessViolation> cursor = recordStore.scanUniquenessViolations(index, Key.Evaluated.scalar(42));

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

            recordStore.resolveUniquenessViolation(index, Tuple.from(42), null).get();
            assertNull(recordStore.loadRecord(Tuple.from(1066L)));
            assertNull(recordStore.loadRecord(Tuple.from(1793L)));

            cursor = recordStore.scanUniquenessViolations(index);
            assertFalse(cursor.hasNext());
        }
    }

    @Test
    public void uniquenessViolationsWithFanOut() throws Exception {
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

            RecordCursor<RecordIndexUniquenessViolation> cursor = recordStore.scanUniquenessViolations(index, Key.Evaluated.scalar(3));

            assertTrue(cursor.hasNext());
            RecordIndexUniquenessViolation next = cursor.next();
            assertEquals(Tuple.from(3L), next.getIndexEntry().getKey());
            assertEquals(Tuple.from(1066L), next.getPrimaryKey());
            assertThat(next.getExistingKey(), isOneOf(Tuple.from(1793L), Tuple.from(1849L)));

            assertTrue(cursor.hasNext());
            next = cursor.next();
            assertEquals(Tuple.from(3L), next.getIndexEntry().getKey());
            assertEquals(Tuple.from(1793L), next.getPrimaryKey());
            assertThat(next.getExistingKey(), isOneOf(Tuple.from(1066L), Tuple.from(1849L)));

            assertTrue(cursor.hasNext());
            next = cursor.next();
            assertEquals(Tuple.from(3L), next.getIndexEntry().getKey());
            assertEquals(Tuple.from(1849L), next.getPrimaryKey());
            assertThat(next.getExistingKey(), isOneOf(Tuple.from(1066L), Tuple.from(1793L)));

            assertFalse(cursor.hasNext());

            cursor = recordStore.scanUniquenessViolations(index, Key.Evaluated.scalar(2));

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

            RecordCursor<RecordIndexUniquenessViolation> cursor = recordStore.scanUniquenessViolations(index);
            assertTrue(cursor.hasNext());
            RecordIndexUniquenessViolation next = cursor.next();
            assertEquals(Tuple.from(2L), next.getIndexEntry().getKey());
            assertEquals(Tuple.from(1066L), next.getPrimaryKey());
            assertEquals(Tuple.from(1793L), next.getExistingKey());
            assertFalse(cursor.hasNext());

            recordStore.resolveUniquenessViolation(index, next.getIndexEntry().getKey(), next.getPrimaryKey()).get();
            assertEquals(0, (int)recordStore.scanUniquenessViolations(index).getCount().get());

            assertNotNull(recordStore.loadRecord(Tuple.from(1066L)));
            assertNull(recordStore.loadRecord(Tuple.from(1793L)));
            assertNull(recordStore.loadRecord(Tuple.from(1849L)));
        }

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            recordStore.resolveUniquenessViolation(index, Tuple.from(3), Tuple.from(1793L)).get();

            RecordCursor<RecordIndexUniquenessViolation> cursor = recordStore.scanUniquenessViolations(index);
            assertTrue(cursor.hasNext());
            RecordIndexUniquenessViolation next = cursor.next();
            assertEquals(Tuple.from(2L), next.getIndexEntry().getKey());
            assertEquals(Tuple.from(1793L), next.getPrimaryKey());
            assertEquals(Tuple.from(1066L), next.getExistingKey());
            assertFalse(cursor.hasNext());

            recordStore.resolveUniquenessViolation(index, next.getIndexEntry().getKey(), next.getPrimaryKey()).get();
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
    public void noMaintenanceFilteredOnIndex() throws Exception {
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
            openAnyRecordStore(TestRecordsIndexFilteringProto.getDescriptor(), context, hook);
            context.getTimer().reset();

            IndexMaintenanceFilter noneFilter = (i, r) -> IndexMaintenanceFilter.IndexValues.NONE;
            recordStore = recordStore.asBuilder().setIndexMaintenanceFilter(noneFilter).build();

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
    public void noMaintenanceFilteredIndexOnCheckVersion() throws Exception {
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
                public CompletableFuture<Integer> checkUserVersion(int oldUserVersion, int oldMetaDataVersion, RecordMetaDataProvider metaData) {
                    return CompletableFuture.completedFuture(Integer.valueOf(1));
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


    public void invalidIndexField() throws Exception {
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
            public CompletableFuture<Integer> checkUserVersion(int oldUserVersion, int oldMetaDataVersion, RecordMetaDataProvider metaData) {
                return CompletableFuture.completedFuture(Integer.valueOf(1));
            }

            @Override
            public IndexState needRebuildIndex(Index index, long recordCount, boolean indexOnNewRecordTypes) {
                return IndexState.DISABLED;
            }
        };

        final FDBRecordStoreBase.UserVersionChecker alwaysEnabled = new FDBRecordStoreBase.UserVersionChecker() {
            @Override
            public CompletableFuture<Integer> checkUserVersion(int oldUserVersion, int oldMetaDataVersion, RecordMetaDataProvider metaData) {
                return CompletableFuture.completedFuture(Integer.valueOf(1));
            }

            @Override
            public IndexState needRebuildIndex(Index index, long recordCount, boolean indexOnNewRecordTypes) {
                return IndexState.READABLE;
            }
        };

        try (FDBRecordContext context = openContext()) {
            final RecordMetaDataBuilder builder = RecordMetaData.newBuilder().setRecords(TestNoIndexesProto.getDescriptor());
            if (withCount) {
                builder.addUniversalIndex(COUNT_INDEX);
            }
            recordStore = FDBRecordStore.newBuilder().setContext(context).setMetaDataProvider(builder).setKeySpacePath(path)
                    .setUserVersionChecker(alwaysEnabled).createOrOpen();
            assertTrue(recordStore.getRecordStoreState().isReadable(COUNT_INDEX.getName()));
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
                builder.addUniversalIndex(COUNT_INDEX);
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
                builder.addUniversalIndex(COUNT_INDEX);
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
    public void testChangeIndexDefinitionWriteOnly() throws Exception {
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
            // Since there were 200 records already, the new index is write-only.
            assertEquals(IndexState.WRITE_ONLY, recordStore.getRecordStoreState().getState(index.getName()));
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
            // The changed index is again write-only because there are 300 records.
            assertEquals(IndexState.WRITE_ONLY, recordStore.getRecordStoreState().getState(index.getName()));
            // Add 100 more records. These will be recorded in the new index.
            for (int i = 300; i < 400; i++) {
                TestNoIndexesProto.MySimpleRecord record = TestNoIndexesProto.MySimpleRecord.newBuilder()
                        .setRecNo(i).setNumValue(i + 1000).build();
                recordStore.saveRecord(record);
            }
            context.commit();
        }
        // Build the index to make it actually usable.
        try (OnlineIndexer onlineIndexBuilder = OnlineIndexer.newBuilder().setDatabase(fdb).setMetaData(recordMetaData).setIndex(index.getName()).setSubspace(recordStore.getSubspace()).build()) {
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
    public void testSelectiveIndexDisable() throws Exception {
        final FDBRecordStoreBase.UserVersionChecker selectiveEnable = new FDBRecordStoreBase.UserVersionChecker() {
            @Override
            public CompletableFuture<Integer> checkUserVersion(int oldUserVersion, int oldMetaDataVersion, RecordMetaDataProvider metaData) {
                return CompletableFuture.completedFuture(Integer.valueOf(1));
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
        metaData.addUniversalIndex(COUNT_INDEX);

        final FDBRecordStore.Builder storeBuilder = FDBRecordStore.newBuilder()
                .setUserVersionChecker(selectiveEnable).setMetaDataProvider(metaData);

        try (FDBRecordContext context = openContext()) {
            recordStore = storeBuilder.setContext(context).setKeySpacePath(path).create(); // builds count index
            assertTrue(recordStore.getRecordStoreState().isReadable(COUNT_INDEX.getName()));
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
    public void invalidCountNoGroup() throws Exception {
        assertThrows(KeyExpression.InvalidExpressionException.class, () ->
                testInvalidIndex(new Index("count_ungrouped", field("rec_no"), IndexTypes.COUNT)));
    }

    @Test
    public void invalidCountUpdatesNoGroup() throws Exception {
        assertThrows(KeyExpression.InvalidExpressionException.class, () ->
                testInvalidIndex(new Index("count_updates_ungrouped", field("rec_no"), IndexTypes.COUNT_UPDATES)));
    }

    @Test
    public void invalidMaxString() throws Exception {
        assertThrows(KeyExpression.InvalidExpressionException.class, () ->
                testInvalidIndex(new Index("max_string", field("str_value_indexed").ungrouped(), IndexTypes.MAX_EVER_LONG)));
    }

    @Test
    public void invalidSumTwoThings() throws Exception {
        assertThrows(KeyExpression.InvalidExpressionException.class, () ->
                testInvalidIndex(new Index("sum_two_fields", concatenateFields("num_value_2", "num_value_3").ungrouped(), IndexTypes.SUM)));
    }

    @Test
    public void invalidRankNothing() throws Exception {
        assertThrows(KeyExpression.InvalidExpressionException.class, () ->
                testInvalidIndex(new Index("max_nothing", new GroupingKeyExpression(EmptyKeyExpression.EMPTY, 0), IndexTypes.RANK)));
    }

    @Test
    public void permissiveIndex() throws Exception {
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
            assertTrue(recordStore.hasIndexEntryRecord(
                    recordStore.getRecordMetaData().getIndex("MySimpleRecord$str_value_indexed"),
                    new IndexEntry(Tuple.from("foo", 1), TupleHelpers.EMPTY), IsolationLevel.SERIALIZABLE).get(), "'Foo' should exist");
            assertFalse(recordStore.hasIndexEntryRecord(
                    recordStore.getRecordMetaData().getIndex("MySimpleRecord$str_value_indexed"),
                    new IndexEntry(Tuple.from("bar", 2), TupleHelpers.EMPTY), IsolationLevel.SERIALIZABLE).get(), "'Bar' should be deleted");
            assertTrue(recordStore.hasIndexEntryRecord(
                    recordStore.getRecordMetaData().getIndex("MySimpleRecord$str_value_indexed"),
                    new IndexEntry(Tuple.from("baz", 3), TupleHelpers.EMPTY), IsolationLevel.SERIALIZABLE).get(), "'Baz' should exist");

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
    }

    private void testInvalidIndex(Index index) throws Exception {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, metaData -> {
                metaData.addIndex("MySimpleRecord", index);
            });
        }
    }

    @Test
    public void formerIndexes() throws Exception {
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
            assertEquals(1, timer.getCount(FDBStoreTimer.Events.REBUILD_INDEX));
            assertEquals(0, timer.getCount(FDBStoreTimer.Events.REMOVE_FORMER_INDEX));
            context.commit();
        }

        metaData.removeIndex("MySimpleRecord$num_value_2");

        try (FDBRecordContext context = openContext()) {
            uncheckedOpenRecordStore(context, metaData.getRecordMetaData());
            timer.reset();
            recordStore.checkVersion(null, FDBRecordStoreBase.StoreExistenceCheck.ERROR_IF_NOT_EXISTS).join();
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
            assertEquals(0, timer.getCount(FDBStoreTimer.Events.REBUILD_INDEX));
            assertEquals(0, timer.getCount(FDBStoreTimer.Events.REMOVE_FORMER_INDEX));
            context.commit();
        }
    }

    @Test
    public void boundaryPrimaryKeys() throws Exception {
        final String indexName = "MySimpleRecord$num_value_unique";
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, TEST_SPLIT_HOOK);
            recordStore.markIndexWriteOnly(indexName).get();
            commit(context);
        }

        String bigOlString = Strings.repeat("x", SplitHelper.SPLIT_RECORD_SIZE + 2);
        for (int i = -25; i < 25; i++) {
            // Sparsify the primary keys so the records can be saved across boundaries.
            saveAndSplitSimpleRecord(i * 39, bigOlString, i);
        }

        OnlineIndexer indexer;
        List<Tuple> boundaryPrimaryKeys;
        TupleRange range;
        try (FDBRecordContext context = openContext()) {
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

    private void checkSplitIndexBuildRange(int minSplit, int maxSplit,
                                           @Nullable List<Pair<Tuple, Tuple>> expectedSplitRanges,
                                           OnlineIndexer indexer) throws Exception {
        List<Pair<Tuple, Tuple>> splitRanges = indexer.splitIndexBuildRange(minSplit, maxSplit);

        if (expectedSplitRanges != null) {
            assertEquals(expectedSplitRanges, splitRanges);
        }

        assertThat(splitRanges.size(), greaterThanOrEqualTo(minSplit));
        assertThat(splitRanges.size(), lessThanOrEqualTo(maxSplit));

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, TEST_SPLIT_HOOK);

            // Make sure each enpoint is a valid primary key.
            splitRanges.forEach(range -> {
                assertTrue(recordStore.recordExists(range.getLeft()));
                assertTrue(recordStore.recordExists(range.getRight()));
                recordStore.loadRecord(range.getRight());
            });

            commit(context);
        }
    }
}
