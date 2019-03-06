/*
 * UnionIntersectionTest.java
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

package com.apple.foundationdb.record.provider.foundationdb.cursors;

import com.apple.foundationdb.record.EndpointType;
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.PipelineOperation;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorIterator;
import com.apple.foundationdb.record.RecordCursorResult;
import com.apple.foundationdb.record.RecordCursorTest;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreTestBase;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoredRecord;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.ScanComparisons;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.TupleHelpers;
import com.apple.test.Tags;
import com.google.protobuf.Message;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.PrimitiveIterator;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static com.apple.foundationdb.record.TestHelpers.assertDiscardedAtMost;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * Tests for {@link UnionCursor} and {@link IntersectionCursor}.
 */
@Tag(Tags.RequiresFDB)
public class UnionIntersectionTest extends FDBRecordStoreTestBase {

    private KeyExpression comparisonKey;

    @BeforeEach
    public void setupRecords() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);

            comparisonKey = recordStore.getRecordMetaData().getRecordType("MySimpleRecord").getPrimaryKey();

            for (int i = 0; i < 100; i++) {
                TestRecords1Proto.MySimpleRecord.Builder record = TestRecords1Proto.MySimpleRecord.newBuilder();
                record.setRecNo(i);
                record.setNumValue3Indexed(i % 3 == 0 ? 0 : 1);
                record.setStrValueIndexed(i % 2 ==  0 ? "even" : "odd");
                recordStore.saveRecord(record.build());
            }
            commit(context);
        }
    }

    // Union / intersection merges need to pause whenever either side hits an out-of-band reason for stopping.

    @Test
    public void unionReasons() throws Exception {
        final Function<byte[], RecordCursor<FDBStoredRecord<Message>>> left = continuation -> scanRecordsBetween(10L, 20L, continuation);
        final Function<byte[], RecordCursor<FDBStoredRecord<Message>>> leftLimited = left.andThen(cursor -> new RecordCursorTest.FakeOutOfBandCursor<>(cursor, 3));
        final Function<byte[], RecordCursor<FDBStoredRecord<Message>>> right = continuation -> scanRecordsBetween(12L, 15L, continuation);
        final Function<byte[], RecordCursor<FDBStoredRecord<Message>>> rightLimited = right.andThen(cursor -> new RecordCursorTest.FakeOutOfBandCursor<>(cursor, 2));
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);

            RecordCursor<FDBStoredRecord<Message>> cursor = UnionCursor.create(recordStore, comparisonKey, false, leftLimited, right, null);
            assertEquals(Arrays.asList(10L, 11L, 12L), cursor.map(this::storedRecordRecNo).asList().get());
            RecordCursorResult<FDBStoredRecord<Message>> noNextResult = cursor.getNext();
            assertEquals(RecordCursor.NoNextReason.TIME_LIMIT_REACHED, noNextResult.getNoNextReason());
            cursor = UnionCursor.create(recordStore, comparisonKey, false, left, rightLimited, noNextResult.getContinuation().toBytes());
            assertEquals(Arrays.asList(13L, 14L), cursor.map(this::storedRecordRecNo).asList().get());
            noNextResult = cursor.getNext();
            assertEquals(RecordCursor.NoNextReason.TIME_LIMIT_REACHED, noNextResult.getNoNextReason());
            cursor = UnionCursor.create(recordStore, comparisonKey, false, leftLimited, rightLimited, noNextResult.getContinuation().toBytes());
            assertEquals(Arrays.asList(15L, 16L, 17L), cursor.map(this::storedRecordRecNo).asList().get());
            noNextResult = cursor.getNext();
            assertEquals(RecordCursor.NoNextReason.TIME_LIMIT_REACHED, noNextResult.getNoNextReason());
            cursor = UnionCursor.create(recordStore, comparisonKey, false, leftLimited, rightLimited, noNextResult.getContinuation().toBytes());
            assertEquals(Arrays.asList(18L, 19L), cursor.map(this::storedRecordRecNo).asList().get());
            noNextResult = cursor.getNext();
            assertEquals(RecordCursor.NoNextReason.SOURCE_EXHAUSTED, noNextResult.getNoNextReason());
        }
    }

    @Test
    public void unionMultiReasons() throws Exception {
        final Function<byte[], RecordCursor<FDBStoredRecord<Message>>> first = continuation -> scanRecordsBetween(10L, 20L, continuation);
        final Function<byte[], RecordCursor<FDBStoredRecord<Message>>> firstLimited = first.andThen(cursor -> new RecordCursorTest.FakeOutOfBandCursor<>(cursor, 3));
        final Function<byte[], RecordCursor<FDBStoredRecord<Message>>> second = continuation -> scanRecordsBetween(12L, 15L, continuation);
        final Function<byte[], RecordCursor<FDBStoredRecord<Message>>> secondLimited = second.andThen(cursor -> new RecordCursorTest.FakeOutOfBandCursor<>(cursor, 2));
        final Function<byte[], RecordCursor<FDBStoredRecord<Message>>> third = continuation -> scanRecordsBetween(16L, 21L, continuation);
        final Function<byte[], RecordCursor<FDBStoredRecord<Message>>> thirdLimited = third.andThen(cursor -> cursor.limitRowsTo(2));
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);

            RecordCursor<FDBStoredRecord<Message>> cursor = UnionCursor.create(recordStore, comparisonKey, false, Arrays.asList(firstLimited, second, thirdLimited), null);
            assertEquals(Arrays.asList(10L, 11L, 12L), cursor.map(this::storedRecordRecNo).asList().get());
            RecordCursorResult<FDBStoredRecord<Message>> noNextResult = cursor.getNext();
            assertEquals(RecordCursor.NoNextReason.TIME_LIMIT_REACHED, noNextResult.getNoNextReason());
            cursor = UnionCursor.create(recordStore, comparisonKey, false, Arrays.asList(first, secondLimited, thirdLimited), noNextResult.getContinuation().toBytes());
            assertEquals(Arrays.asList(13L, 14L), cursor.map(this::storedRecordRecNo).asList().get());
            noNextResult = cursor.getNext();
            assertEquals(RecordCursor.NoNextReason.TIME_LIMIT_REACHED, noNextResult.getNoNextReason());
            cursor = UnionCursor.create(recordStore, comparisonKey, false, Arrays.asList(firstLimited, secondLimited, thirdLimited), noNextResult.getContinuation().toBytes());
            assertEquals(Arrays.asList(15L, 16L, 17L), cursor.map(this::storedRecordRecNo).asList().get());
            noNextResult = cursor.getNext();
            assertEquals(RecordCursor.NoNextReason.TIME_LIMIT_REACHED, noNextResult.getNoNextReason());
            cursor = UnionCursor.create(recordStore, comparisonKey, false, Arrays.asList(firstLimited, secondLimited, thirdLimited), noNextResult.getContinuation().toBytes());
            assertEquals(Arrays.asList(18L, 19L), cursor.map(this::storedRecordRecNo).asList().get());
            noNextResult = cursor.getNext();
            assertEquals(RecordCursor.NoNextReason.RETURN_LIMIT_REACHED, noNextResult.getNoNextReason());
            cursor = UnionCursor.create(recordStore, comparisonKey, false, Arrays.asList(firstLimited, secondLimited, thirdLimited), noNextResult.getContinuation().toBytes());
            assertEquals(Collections.singletonList(20L), cursor.map(this::storedRecordRecNo).asList().get());
            noNextResult = cursor.getNext();
            assertEquals(RecordCursor.NoNextReason.SOURCE_EXHAUSTED, noNextResult.getNoNextReason());
        }
    }

    @Test
    public void intersectionReasons() throws Exception {
        final Function<byte[], RecordCursor<FDBStoredRecord<Message>>> left = continuation -> scanRecordsBetween(7L, 20L, continuation);
        final Function<byte[], RecordCursor<FDBStoredRecord<Message>>> leftLimited = left.andThen(cursor -> new RecordCursorTest.FakeOutOfBandCursor<>(cursor, 3));
        final Function<byte[], RecordCursor<FDBStoredRecord<Message>>> right = continuation -> scanRecordsBetween(12L, 15L, continuation);
        final Function<byte[], RecordCursor<FDBStoredRecord<Message>>> rightLimited = right.andThen(cursor -> new RecordCursorTest.FakeOutOfBandCursor<>(cursor, 2));
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);

            RecordCursor<FDBStoredRecord<Message>> cursor = IntersectionCursor.create(recordStore, comparisonKey, false, leftLimited, right, null);
            assertEquals(Collections.emptyList(), cursor.map(this::storedRecordRecNo).asList().get());
            RecordCursorResult<FDBStoredRecord<Message>> noNextResult = cursor.getNext();
            assertEquals(RecordCursor.NoNextReason.TIME_LIMIT_REACHED, noNextResult.getNoNextReason());
            cursor = IntersectionCursor.create(recordStore, comparisonKey, false, leftLimited, right, noNextResult.getContinuation().toBytes());
            assertEquals(Arrays.asList(12L), cursor.map(this::storedRecordRecNo).asList().get());
            noNextResult = cursor.getNext();
            assertEquals(RecordCursor.NoNextReason.TIME_LIMIT_REACHED, noNextResult.getNoNextReason());
            cursor = IntersectionCursor.create(recordStore, comparisonKey, false, left, rightLimited, noNextResult.getContinuation().toBytes());
            assertEquals(Arrays.asList(13L, 14L), cursor.map(this::storedRecordRecNo).asList().get());
            noNextResult = cursor.getNext();
            assertEquals(RecordCursor.NoNextReason.TIME_LIMIT_REACHED, noNextResult.getNoNextReason());
            cursor = IntersectionCursor.create(recordStore, comparisonKey, false, leftLimited, rightLimited, noNextResult.getContinuation().toBytes());
            assertEquals(Collections.emptyList(), cursor.map(this::storedRecordRecNo).asList().get());
            noNextResult = cursor.getNext();
            assertEquals(RecordCursor.NoNextReason.SOURCE_EXHAUSTED, noNextResult.getNoNextReason());
        }
    }

    @Test
    public void nonIntersectingReasons() {
        final List<Integer> leftList = Arrays.asList(0, 2, 4, 6);
        final Function<byte[], RecordCursor<Integer>> left = continuation -> RecordCursor.fromList(leftList, continuation).limitRowsTo(1);
        final List<Integer> rightList = Arrays.asList(1, 3, 5, 7);
        final Function<byte[], RecordCursor<Integer>> right = continuation -> RecordCursor.fromList(rightList, continuation).limitRowsTo(1);

        FDBStoreTimer timer = new FDBStoreTimer();
        boolean done = false;
        byte[] continuation = null;
        List<Integer> results = new ArrayList<>();
        while (!done) {
            IntersectionCursor<Integer> intersectionCursor = IntersectionCursor.create(Collections::singletonList, false, left, right, continuation, timer);
            intersectionCursor.forEach(results::add).join();
            RecordCursorResult<Integer> noNextResult = intersectionCursor.getNext();
            done = noNextResult.getNoNextReason().isSourceExhausted();
            continuation = noNextResult.getContinuation().toBytes();
            if (!done) {
                assertEquals(RecordCursor.NoNextReason.RETURN_LIMIT_REACHED, noNextResult.getNoNextReason());
            }
        }
        assertEquals(Collections.emptyList(), results);
        System.out.println(timer.getKeysAndValues());
    }

    @Test
    public void intersectionMultiReasons() throws Exception {
        final Function<byte[], RecordCursor<FDBStoredRecord<Message>>> first = continuation -> scanRecordsBetween(10L, 20L, continuation);
        final Function<byte[], RecordCursor<FDBStoredRecord<Message>>> firstLimited = first.andThen(cursor -> new RecordCursorTest.FakeOutOfBandCursor<>(cursor, 3));
        final Function<byte[], RecordCursor<FDBStoredRecord<Message>>> second = continuation -> scanRecordsBetween(12L, 17L, continuation);
        final Function<byte[], RecordCursor<FDBStoredRecord<Message>>> secondLimited = second.andThen(cursor -> new RecordCursorTest.FakeOutOfBandCursor<>(cursor, 2));
        final Function<byte[], RecordCursor<FDBStoredRecord<Message>>> third = continuation -> scanRecordsBetween(13L, 21L, continuation);
        final Function<byte[], RecordCursor<FDBStoredRecord<Message>>> thirdLimited = third.andThen(cursor -> cursor.limitRowsTo(2));
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);

            RecordCursor<FDBStoredRecord<Message>> cursor = IntersectionCursor.create(recordStore, comparisonKey, false, Arrays.asList(firstLimited, second, third), null);
            assertEquals(Collections.emptyList(), cursor.map(this::storedRecordRecNo).asList().get());
            RecordCursorResult<FDBStoredRecord<Message>> noNextResult = cursor.getNext();
            assertEquals(RecordCursor.NoNextReason.TIME_LIMIT_REACHED, noNextResult.getNoNextReason());
            cursor = IntersectionCursor.create(recordStore, comparisonKey, false, Arrays.asList(firstLimited, secondLimited, thirdLimited), noNextResult.getContinuation().toBytes());
            assertEquals(Arrays.asList(13L, 14L), cursor.map(this::storedRecordRecNo).asList().get());
            noNextResult = cursor.getNext();
            assertEquals(RecordCursor.NoNextReason.RETURN_LIMIT_REACHED, noNextResult.getNoNextReason());
            cursor = IntersectionCursor.create(recordStore, comparisonKey, false, Arrays.asList(firstLimited, second, thirdLimited), noNextResult.getContinuation().toBytes());
            assertEquals(Arrays.asList(15L, 16L), cursor.map(this::storedRecordRecNo).asList().get());
            noNextResult = cursor.getNext();
            assertEquals(RecordCursor.NoNextReason.SOURCE_EXHAUSTED, noNextResult.getNoNextReason());
        }
    }

    private void verifyUnionWithInnerLimits(List<Function<byte[], RecordCursor<FDBRecord<Message>>>> cursorFunctions,
                                            PrimitiveIterator.OfLong target) throws Exception {
        byte[] continuation = null;
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            RecordCursorIterator<? extends FDBRecord<Message>> union;
            boolean done = false;
            do {
                union = UnionCursor.create(recordStore, comparisonKey, false, cursorFunctions, continuation).asIterator();
                while (union.hasNext()) {
                    assertFalse(done); // if we think we're done, should not get more records
                    long expected = target.next();
                    long next = storedRecordRecNo(union.next());
                    assertEquals(expected, next);
                }
                continuation = union.getContinuation();
                if (continuation != null) {
                    if (union.getNoNextReason() == RecordCursor.NoNextReason.SOURCE_EXHAUSTED) {
                        done = true;
                    } else {
                        assertEquals(RecordCursor.NoNextReason.RETURN_LIMIT_REACHED, union.getNoNextReason());
                    }
                }
            } while (continuation != null);
            assertEquals(RecordCursor.NoNextReason.SOURCE_EXHAUSTED, union.getNoNextReason());
        }
    }

    private void verifyUnionWithInnerLimits(Function<byte[], RecordCursor<FDBRecord<Message>>> left,
                                            Function<byte[], RecordCursor<FDBRecord<Message>>> right,
                                            PrimitiveIterator.OfLong target) throws Exception {
        verifyUnionWithInnerLimits(Arrays.asList(left, right), target);
    }

    @ValueSource(ints = {1, 2, 3})
    @ParameterizedTest(name = "disjointUnionWithInnerLimits() [{0}]")
    public void disjointUnionWithInnerLimits(int innerLimit) throws Exception {
        verifyUnionWithInnerLimits(cont -> scanRecordsBetween(10L, 22L, cont).limitRowsTo(innerLimit)
                        .map(rec -> (FDBRecord<Message>) rec),
                cont -> scanRecordsBetween(50L, 62L, cont).limitRowsTo(innerLimit)
                        .map(rec -> (FDBRecord<Message>) rec),
                LongStream.concat(LongStream.range(10L, 22L), LongStream.range(50L, 62L)).iterator());
    }

    @ValueSource(ints = {1, 2, 3})
    @ParameterizedTest(name = "overlappingUnionWithInnerLimits() [{0}]")
    public void overlappingUnionWithInnerLimits(int innerLimit) throws Exception {
        verifyUnionWithInnerLimits(cont -> scanRecordsBetween(10L, 53L, cont).limitRowsTo(innerLimit)
                        .map(rec -> (FDBRecord<Message>) rec),
                cont -> scanRecordsBetween(50L, 62L, cont).limitRowsTo(innerLimit)
                        .map(rec -> (FDBRecord<Message>) rec),
                LongStream.range(10L, 62L).iterator());
    }

    @ValueSource(ints = {1, 2, 3, 4, 11})
    @ParameterizedTest(name = "multipleOverlappingUnionWithInnerLimits() [{0}]")
    public void multipleOverlappingUnionWithInnerLimits(int innerLimit) throws Exception {
        verifyUnionWithInnerLimits(Arrays.asList(
                cont -> scanRecordsBetween(10L, 20L, cont).limitRowsTo(innerLimit)
                        .map(rec -> (FDBRecord<Message>) rec),
                cont -> scanRecordsBetween(9L, 15L, cont).limitRowsTo(innerLimit)
                        .map(rec -> (FDBRecord<Message>) rec),
                cont -> scanRecordsBetween(14L, 25L, cont).limitRowsTo(innerLimit)
                        .map(rec -> (FDBRecord<Message>) rec)
        ), LongStream.range(9L, 25L).iterator());
    }

    @ValueSource(ints = {1, 2, 3, 4, 5})
    @ParameterizedTest(name = "interleavedIndexScan() [{0}]")
    public void interleavedIndexScan(int innerLimit) throws Exception {
        final ScanProperties scanProperties = new ScanProperties(ExecuteProperties.newBuilder().setReturnedRowLimit(innerLimit).build());
        final ScanComparisons evenComparisons = new ScanComparisons(Collections.singletonList(
                new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, "even")), Collections.emptyList());
        final ScanComparisons oddComparisons = new ScanComparisons(Collections.singletonList(
                new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, "odd")), Collections.emptyList());
        verifyUnionWithInnerLimits(cont -> recordStore.scanIndexRecords("MySimpleRecord$str_value_indexed",
                IndexScanType.BY_VALUE, evenComparisons.toTupleRange(), cont, scanProperties)
                        .map(rec -> (FDBRecord<Message>) rec),
                cont -> recordStore.scanIndexRecords("MySimpleRecord$str_value_indexed",
                IndexScanType.BY_VALUE, oddComparisons.toTupleRange(), cont, scanProperties)
                        .map(rec -> (FDBRecord<Message>) rec),
                LongStream.range(0L, 100L).iterator());
    }

    @ValueSource(ints = {1, 2, 3, 4, 5})
    @ParameterizedTest(name = "unevenInterleavedIndexScan() [{0}]")
    public void unevenInterleavedIndexScan(int innerLimit) throws Exception {
        final ScanProperties scanProperties = new ScanProperties(ExecuteProperties.newBuilder().setReturnedRowLimit(innerLimit).build());
        final ScanComparisons zeros = new ScanComparisons(Collections.singletonList(
                new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, 0)), Collections.emptyList());
        final ScanComparisons others = new ScanComparisons(Collections.singletonList(
                new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, 1)), Collections.emptyList());
        verifyUnionWithInnerLimits(cont -> recordStore.scanIndexRecords("MySimpleRecord$num_value_3_indexed",
                IndexScanType.BY_VALUE, zeros.toTupleRange(), cont, scanProperties)
                        .map(rec -> (FDBRecord<Message>)rec),
                cont -> recordStore.scanIndexRecords("MySimpleRecord$num_value_3_indexed",
                        IndexScanType.BY_VALUE, others.toTupleRange(), cont, scanProperties)
                        .map(rec -> (FDBRecord<Message>)rec),
                LongStream.range(0L, 100L).iterator());
    }

    /**
     * Create cursors that correspond to union or intersection query and validate that using the custom comparison
     * key works.
     */
    @Test
    public void indexScansByPrimaryKey() throws Exception {
        final ScanProperties scanProperties = ScanProperties.FORWARD_SCAN;
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            final Index strValueIndex = recordStore.getRecordMetaData().getIndex("MySimpleRecord$str_value_indexed");
            final Index numValue3Index = recordStore.getRecordMetaData().getIndex("MySimpleRecord$num_value_3_indexed");

            List<Long> recNos = IntersectionCursor.create(
                    (IndexEntry entry) -> TupleHelpers.subTuple(entry.getKey(), 1, entry.getKey().size()).getItems(),
                    false,
                    (byte[] leftContinuation) -> recordStore.scanIndex(strValueIndex, IndexScanType.BY_VALUE, TupleRange.allOf(Tuple.from("even")), leftContinuation, scanProperties),
                    (byte[] rightContinuation) -> recordStore.scanIndex(numValue3Index, IndexScanType.BY_VALUE, TupleRange.allOf(Tuple.from(1)), rightContinuation, scanProperties),
                    null,
                    recordStore.getTimer())
                    .mapPipelined(indexEntry -> recordStore.loadRecordAsync(strValueIndex.getEntryPrimaryKey(indexEntry.getKey())), recordStore.getPipelineSize(PipelineOperation.INDEX_TO_RECORD))
                    .map(this::storedRecordRecNo)
                    .asList()
                    .get();
            assertEquals(LongStream.range(0, 100).filter(i -> i % 2 == 0).filter(i -> i % 3 != 0).boxed().collect(Collectors.toList()), recNos);
            assertDiscardedAtMost(50, context);

            recNos = UnionCursor.create(
                    (IndexEntry entry) -> TupleHelpers.subTuple(entry.getKey(), 1, entry.getKey().size()).getItems(),
                    false,
                    (byte[] leftContinuation) -> recordStore.scanIndex(strValueIndex, IndexScanType.BY_VALUE, TupleRange.allOf(Tuple.from("even")), leftContinuation, scanProperties),
                    (byte[] rightContinuation) -> recordStore.scanIndex(numValue3Index, IndexScanType.BY_VALUE, TupleRange.allOf(Tuple.from(1)), rightContinuation, scanProperties),
                    null,
                    recordStore.getTimer())
                .mapPipelined(indexEntry -> recordStore.loadRecordAsync(strValueIndex.getEntryPrimaryKey(indexEntry.getKey())), recordStore.getPipelineSize(PipelineOperation.INDEX_TO_RECORD))
                .map(this::storedRecordRecNo)
                .asList()
                .get();
            assertEquals(LongStream.range(0, 100).filter(i -> i % 2 == 0 || i % 3 != 0).boxed().collect(Collectors.toList()), recNos);
            assertDiscardedAtMost(83, context);

            commit(context);
        }
    }

    private RecordCursor<FDBStoredRecord<Message>> scanRecordsBetween(Long start, Long end, byte[] continuation) {
        return recordStore.scanRecords(
                start == null ? null : Tuple.from(start),
                end == null ? null : Tuple.from(end),
                start == null ? EndpointType.TREE_START : EndpointType.RANGE_INCLUSIVE,
                end == null ? EndpointType.TREE_END : EndpointType.RANGE_EXCLUSIVE,
                continuation, ScanProperties.FORWARD_SCAN);
    }

    private long storedRecordRecNo(FDBRecord<Message> storedRecord) {
        TestRecords1Proto.MySimpleRecord.Builder record = TestRecords1Proto.MySimpleRecord.newBuilder();
        record.mergeFrom(storedRecord.getRecord());
        return record.getRecNo();
    }

}
