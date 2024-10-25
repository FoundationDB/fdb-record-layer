/*
 * InMemoryQueueCursorTest.java
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

package com.apple.foundationdb.record.cursors;

import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorResult;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.TestRecords4WrapperProto;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBIndexedRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBQueriedRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordVersion;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoredRecord;
import com.apple.foundationdb.record.query.plan.plans.QueryResult;
import com.apple.foundationdb.test.TestExecutors;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Objects;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link InMemoryQueueCursor}.
 */
public class InMemoryQueueCursorTest {
//
//    private static final int seed = 42;
//
//    @Nonnull
//    private static final Random random = new Random(seed);
//
//    private void validateNoNextReason(@Nonnull RecordCursor<?> cursor) {
//        RecordCursorResult<?> noNextResult = cursor.getNext();
//        assertFalse(noNextResult.hasNext());
//        assertEquals(RecordCursor.NoNextReason.SOURCE_EXHAUSTED, noNextResult.getNoNextReason());
//        if (RecordCursor.NoNextReason.SOURCE_EXHAUSTED.isSourceExhausted()) {
//            assertTrue(noNextResult.getContinuation().isEnd());
//            assertNull(noNextResult.getContinuation().toBytes());
//        } else {
//            assertFalse(noNextResult.getContinuation().isEnd());
//            assertNotNull(noNextResult.getContinuation().toBytes());
//        }
//    }
//
//    @Test
//    void emptyCursorEmitsCorrectNoNextReason() {
//        final var executor = TestExecutors.defaultThreadPool();
//        final var cursor = new InMemoryQueueCursor(executor);
//        validateNoNextReason(cursor);
//    }
//
//    @Test
//    void singleItemCursor() {
//        final var executor = TestExecutors.defaultThreadPool();
//        final var cursor = new InMemoryQueueCursor(executor);
//        cursor.add(integer(42));
//        assertQueryResults(ImmutableList.of(integer(42)), cursor.asList().join(), false);
//        validateNoNextReason(cursor);
//    }
//
//    @Test
//    void multiItemCursor() {
//        final var executor = TestExecutors.defaultThreadPool();
//        final var cursor = new InMemoryQueueCursor(executor);
//        cursor.add(integer(42), integer(44), integer(46), integer(48));
//        assertQueryResults(ImmutableList.of(integer(42), integer(44), integer(46), integer(48)), cursor.asList().join(), false);
//        validateNoNextReason(cursor);
//    }
//
//    @Test
//    void multiItemCursorWithContinuation() {
//        final var executor = TestExecutors.defaultThreadPool();
//        final RecordCursorResult<QueryResult> cursorResult;
//        try (var cursor = new InMemoryQueueCursor(executor)) {
//            cursor.add(integer(42), integer(44), integer(46), integer(48));
//            cursorResult = cursor.getNext();
//            assertEquals(42, Objects.requireNonNull(cursorResult.get()).getDatum());
//            final var continuation = (InMemoryQueueCursor.Continuation)cursorResult.getContinuation();
//            final var resumedCursor = new InMemoryQueueCursor(continuation);
//            assertQueryResults(ImmutableList.of(integer(44), integer(46), integer(48)), resumedCursor.asList().join(), false);
//            validateNoNextReason(resumedCursor);
//        }
//    }
//
//    @Test
//    void multiItemCursorWithSerializedContinuation() {
//        final var executor = TestExecutors.defaultThreadPool();
//        final RecordCursorResult<QueryResult> cursorResult;
//        try (var cursor = new InMemoryQueueCursor(executor)) {
//            cursor.add(integer(42), integer(44), integer(46), integer(48));
//            cursorResult = cursor.getNext();
//            assertEquals(42, Objects.requireNonNull(cursorResult.get()).getDatum());
//            final var continuationBytes = cursorResult.getContinuation().toBytes();
//            assertNotNull(continuationBytes);
//            final var resumedCursor = new InMemoryQueueCursor(InMemoryQueueCursor.Continuation.from(continuationBytes));
//            assertQueryResults(ImmutableList.of(integer(44), integer(46), integer(48)), resumedCursor.asList().join(), false);
//            validateNoNextReason(resumedCursor);
//        }
//    }
//
//    @Test
//    void cursorWithCoveredRecord() {
//        final var executor = TestExecutors.defaultThreadPool();
//        final QueryResult firstRecord = randomCoveredRecord();
//        final QueryResult secondRecord = randomCoveredRecord();
//        final QueryResult thirdRecord = randomCoveredRecord();
//        final RecordCursorResult<QueryResult> cursorResult;
//        try (var cursor = new InMemoryQueueCursor(executor)) {
//            cursor.add(firstRecord, secondRecord, thirdRecord);
//            cursorResult = cursor.getNext();
//            assertEquals(firstRecord, cursorResult.get());
//            final var continuationBytes = cursorResult.getContinuation().toBytes();
//            assertNotNull(continuationBytes);
//            final var resumedCursor = new InMemoryQueueCursor(InMemoryQueueCursor.Continuation.from(TestRecords4WrapperProto.RestaurantRecord.getDescriptor(), continuationBytes));
//            assertQueryResults(ImmutableList.of(secondRecord, thirdRecord), resumedCursor.asList().join(), true);
//            validateNoNextReason(resumedCursor);
//        }
//    }
//
//    @Test
//    void cursorWithIndexedRecord() {
//        final var executor = TestExecutors.defaultThreadPool();
//        final QueryResult firstRecord = randomIndexedRecord();
//        final QueryResult secondRecord = randomIndexedRecord();
//        final QueryResult thirdRecord = randomIndexedRecord();
//        final RecordCursorResult<QueryResult> cursorResult;
//        try (var cursor = new InMemoryQueueCursor(executor)) {
//            cursor.add(firstRecord, secondRecord, thirdRecord);
//            cursorResult = cursor.getNext();
//            assertEquals(firstRecord, cursorResult.get());
//            final var continuationBytes = cursorResult.getContinuation().toBytes();
//            assertNotNull(continuationBytes);
//            final var resumedCursor = new InMemoryQueueCursor(InMemoryQueueCursor.Continuation.from(TestRecords4WrapperProto.RestaurantRecord.getDescriptor(), continuationBytes));
//            assertQueryResults(ImmutableList.of(secondRecord, thirdRecord), resumedCursor.asList().join(), true);
//            validateNoNextReason(resumedCursor);
//        }
//    }
//
//    @Test
//    void cursorWithStoredRecord() {
//        final var executor = TestExecutors.defaultThreadPool();
//        final QueryResult firstRecord = randomStoredRecord();
//        final QueryResult secondRecord = randomStoredRecord();
//        final QueryResult thirdRecord = randomStoredRecord();
//        final RecordCursorResult<QueryResult> cursorResult;
//        try (var cursor = new InMemoryQueueCursor(executor)) {
//            cursor.add(firstRecord, secondRecord, thirdRecord);
//            cursorResult = cursor.getNext();
//            assertEquals(firstRecord, cursorResult.get());
//            final var continuationBytes = cursorResult.getContinuation().toBytes();
//            assertNotNull(continuationBytes);
//            final var resumedCursor = new InMemoryQueueCursor(InMemoryQueueCursor.Continuation.from(TestRecords4WrapperProto.RestaurantRecord.getDescriptor(), continuationBytes));
//            assertQueryResults(ImmutableList.of(secondRecord, thirdRecord), resumedCursor.asList().join(), true);
//            validateNoNextReason(resumedCursor);
//        }
//    }
//
//    @Test
//    void continuationIsCachedCorrectly() {
//        final var executor = TestExecutors.defaultThreadPool();
//        final QueryResult record;
//        final RecordCursorResult<QueryResult> cursorResult;
//        try (var cursor = new InMemoryQueueCursor(executor)) {
//            record = randomStoredRecord();
//            cursor.add(record);
//            cursorResult = cursor.getNext();
//        }
//        assertEquals(record, cursorResult.get());
//        final var continuation = cursorResult.getContinuation();
//        assertSame(continuation.toByteString(), continuation.toByteString());
//    }
//
//    @Test
//    void recursionWorksCorrectly() {
//        final var executor = TestExecutors.defaultThreadPool();
//        /*
//         *                1
//         *             /     \
//         *            10      20
//         *          / |  \    /  \
//         *         14 15 16  17  [18] <--- find path to root.
//         */
//        final var hierarchy = ImmutableMap.of(
//                    1, -1, // -1 is a sentinel indicating that the current row represents the root.
//                    10, 1,
//                    20, 1,
//                    14, 10,
//                    15, 10,
//                    16, 10,
//                    17, 20,
//                    18, 20
//        );
//        final ImmutableList.Builder<Integer> builder = ImmutableList.builder();
//        try (var cursor1 = new InMemoryQueueCursor(executor); var cursor2 = new InMemoryQueueCursor(executor)) {
//            var reader = cursor1;
//            var writer = cursor2;
//
//            // seeding recursion, this represents the left leg of the recursive union
//            writer.add(integer(18));
//
//            boolean reachedRoot = false;
//            while (!reachedRoot) {
//                RecordCursorResult<QueryResult> item = reader.getNext();
//                while (item.hasNext()) {
//                    final var value = (Integer)Objects.requireNonNull(item.get()).getDatum();
//                    builder.add(value);
//                    if (value == -1) {
//                        reachedRoot = true;
//                        break;
//                    }
//                    writer.add(integer(hierarchy.get(value))); // this is the join condition
//                    item = reader.getNext();
//                }
//                // flip the double buffer
//                var temp = reader;
//                reader = writer;
//                writer = temp;
//            }
//        }
//        assertEquals(ImmutableList.of(18, 20, 1, -1), builder.build());
//    }
//
//    @Test
//    void recursionUsingDoubleBufferCursorWorksCorrectly() {
//        final var executor = TestExecutors.defaultThreadPool();
//        /*
//         *                1
//         *             /     \
//         *            10      20
//         *          / |  \    /  \
//         *         14 15 16  17  [18] <--- find path to root.
//         */
//        final var hierarchy = ImmutableMap.of(
//                1, -1, // -1 is a sentinel indicating that the current row represents the root.
//                10, 1,
//                20, 1,
//                14, 10,
//                15, 10,
//                16, 10,
//                17, 20,
//                18, 20
//        );
//        final ImmutableList.Builder<Integer> builder = ImmutableList.builder();
//        try (final var cursor = new DoubleBufferCursor(executor)) {
//
//            // seeding recursion, this represents the left leg of the recursive union
//            cursor.add(integer(18));
//
//            boolean reachedRoot = false;
//            while (!reachedRoot) {
//                RecordCursorResult<QueryResult> item = cursor.getNext();
//                while (item.hasNext()) {
//                    final var value = (Integer)Objects.requireNonNull(item.get()).getDatum();
//                    builder.add(value);
//                    if (value == -1) {
//                        reachedRoot = true;
//                        break;
//                    }
//                    cursor.add(integer(hierarchy.get(value))); // this is the join condition
//                    item = cursor.getNext();
//                }
//                cursor.flip();
//            }
//        }
//        assertEquals(ImmutableList.of(18, 20, 1, -1), builder.build());
//    }
//
//    @ParameterizedTest()
//    @ValueSource(ints = {0, 1, 2, 3}) // six numbers
//    void recursionUsingDoubleBufferCursorAcrossContinuationsWorksCorrectly(int interruptionSignal) {
//        final var executor = TestExecutors.defaultThreadPool();
//        /*
//         *                1
//         *             /     \
//         *            10      20
//         *          / |  \    /  \
//         *         14 15 16  17  [18] <--- find path to root.
//         */
//        final var hierarchy = ImmutableMap.of(
//                1, -1, // -1 is a sentinel indicating that the current row represents the root.
//                10, 1,
//                20, 1,
//                14, 10,
//                15, 10,
//                16, 10,
//                17, 20,
//                18, 20
//        );
//        final ImmutableList.Builder<Integer> builder = ImmutableList.builder();
//        byte[] continuation = null;
//        try (final var cursor = new DoubleBufferCursor(executor)) {
//            // seeding recursion, this represents the left leg of the recursive union
//            cursor.add(integer(18));
//            boolean reachedRoot = false;
//            while (!reachedRoot) {
//                RecordCursorResult<QueryResult> item = cursor.getNext();
//                if (!item.hasNext()) {
//                    cursor.flip();
//                    item = cursor.getNext();
//                    if (!item.hasNext()) {
//                        break;
//                    }
//                }
//                while (item.hasNext()) {
//                    final var value = (Integer)Objects.requireNonNull(item.get()).getDatum();
//                    builder.add(value);
//                    if (value == -1) {
//                        reachedRoot = true;
//                        break;
//                    }
//                    cursor.add(integer(hierarchy.get(value))); // this is the join condition
//                    if (interruptionSignal-- <= 0) {
//                        continuation = item.getContinuation().toBytes();
//                        break;
//                    }
//                    item = cursor.getNext();
//                }
//            }
//        }
//
//        if (continuation != null) {
//            try (final var cursor = new DoubleBufferCursor(executor, DoubleBufferCursor.Continuation.from(continuation))) {
//                // seeding recursion, this represents the left leg of the recursive union
//                boolean reachedRoot = false;
//                while (!reachedRoot) {
//                    RecordCursorResult<QueryResult> item = cursor.getNext();
//                    if (!item.hasNext()) {
//                        cursor.flip();
//                        item = cursor.getNext();
//                        if (!item.hasNext()) {
//                            break;
//                        }
//                    }
//                    while (item.hasNext()) {
//                        final var value = (Integer)Objects.requireNonNull(item.get()).getDatum();
//                        builder.add(value);
//                        if (value == -1) {
//                            reachedRoot = true;
//                            break;
//                        }
//                        cursor.add(integer(hierarchy.get(value))); // this is the join condition
//                        item = cursor.getNext();
//                    }
//                }
//            }
//        }
//
//        assertEquals(ImmutableList.of(18, 20, 1, -1), builder.build());
//    }
//
//    @Test
//    void continuationIsValidAfterClosingTheCursor() {
//        final var executor = TestExecutors.defaultThreadPool();
//        final RecordCursorResult<QueryResult> cursorResult;
//        try (var cursor = new InMemoryQueueCursor(executor)) {
//            cursor.add(integer(42), integer(44), integer(46), integer(48));
//            cursorResult = cursor.getNext();
//        } // cursor closed
//        final var continuationBytes = cursorResult.getContinuation().toBytes();
//        assertNotNull(continuationBytes);
//        assertEquals(42, Objects.requireNonNull(cursorResult.get()).getDatum());
//        final var resumedCursor = new InMemoryQueueCursor(InMemoryQueueCursor.Continuation.from(continuationBytes));
//        assertQueryResults(ImmutableList.of(integer(44), integer(46), integer(48)), resumedCursor.asList().join(), false);
//        validateNoNextReason(resumedCursor);
//    }
//
//    @Test
//    void continuationDoesNotChangeStateAfterCursorAdvances() {
//        final var executor = TestExecutors.defaultThreadPool();
//        final RecordCursorResult<QueryResult> cursorResult;
//        try (var cursor = new InMemoryQueueCursor(executor)) {
//            cursor.add(integer(42), integer(44), integer(46), integer(48));
//            cursorResult = cursor.getNext();
//            cursor.add(integer(1000)); // this is not part of the previous continuation, i.e. a continuation is a true snapshot.
//        } // cursor closed
//        final var continuationBytes = cursorResult.getContinuation().toBytes();
//        assertNotNull(continuationBytes);
//        assertEquals(42, Objects.requireNonNull(cursorResult.get()).getDatum());
//        final var resumedCursor = new InMemoryQueueCursor(InMemoryQueueCursor.Continuation.from(continuationBytes));
//        assertQueryResults(ImmutableList.of(integer(44), integer(46), integer(48)), resumedCursor.asList().join(), false);
//        validateNoNextReason(resumedCursor);
//    }
//
//    private static class RandomRecordBuilder {
//
//        private Tuple primaryKey;
//
//        private IndexEntry indexEntry;
//
//        private FDBRecordVersion version;
//
//        private Index index;
//
//        private RandomRecordBuilder() {
//        }
//
//        @Nonnull
//        public static RandomRecordBuilder newInstance() {
//            return new RandomRecordBuilder().randomize();
//        }
//
//        @Nonnull
//        public RandomRecordBuilder withPrimaryKey(@Nonnull Tuple primaryKey) {
//            this.primaryKey = primaryKey;
//            return this;
//        }
//
//        @Nonnull
//        public RandomRecordBuilder withRandomPrimaryKey() {
//            final byte[] array = new byte[10];
//            random.nextBytes(array);
//            final var pk1 = "pk1" + random.nextInt();
//            final var pk2 = "pk2" + random.nextInt();
//            final var pk3 = "pk3" + random.nextInt();
//            this.primaryKey = Tuple.fromList(ImmutableList.of(pk1, pk2, pk3));
//            return this;
//        }
//
//        @Nonnull
//        public RandomRecordBuilder withIndexEntry(@Nonnull IndexEntry indexEntry) {
//            this.indexEntry = indexEntry;
//            return this;
//        }
//
//        @Nonnull
//        public RandomRecordBuilder withRandomIndexEntry() {
//            this.indexEntry = new IndexEntry(index, Key.Evaluated.EMPTY);
//            return this;
//        }
//
//        @Nonnull
//        public RandomRecordBuilder withFDBRecordVersion(@Nonnull FDBRecordVersion version) {
//            this.version = version;
//            return this;
//        }
//
//        @Nonnull
//        public RandomRecordBuilder withRandomFDBRecordVersion() {
//            final byte[] array = new byte[12];
//            random.nextBytes(array);
//            this.version = FDBRecordVersion.complete(array);
//            return this;
//        }
//
//        @Nonnull
//        public RandomRecordBuilder randomize() {
//            return withRandomFDBRecordVersion().withRandomIndexEntry().withRandomPrimaryKey().withRandomIndex();
//        }
//
//        @Nonnull
//        public RandomRecordBuilder withIndex(@Nonnull Index index) {
//            this.index = index;
//            return this;
//        }
//
//        @Nonnull
//        public RandomRecordBuilder withRandomIndex() {
//            final var randomIndexName = "index" + random.nextInt();
//            final var indexPath = "path" + random.nextInt();
//            final var indexType = "type" + random.nextInt();
//            this.index = new Index(randomIndexName, KeyExpression.fromPath(ImmutableList.of(indexPath)), indexType, ImmutableMap.of());
//            return this;
//        }
//
//        @Nonnull
//        public FDBQueriedRecord<TestRecords4WrapperProto.RestaurantRecord> asCovered() {
//            return FDBQueriedRecord.covered(index, indexEntry, primaryKey, generateRecordType(), generateRecord());
//        }
//
//        @Nonnull
//        public FDBQueriedRecord<Message> asStored() {
//            return FDBQueriedRecord.stored(getStoredRecord());
//        }
//
//        @Nonnull
//        public FDBQueriedRecord<Message> asIndexed() {
//            return FDBQueriedRecord.indexed(new FDBIndexedRecord<>(indexEntry, getStoredRecord()));
//        }
//
//        @Nonnull
//        private FDBStoredRecord<Message> getStoredRecord() {
//            return FDBStoredRecord.newBuilder().setRecordType(generateRecordType()).setPrimaryKey(primaryKey).setVersion(version).setRecord(generateRecord()).build();
//        }
//
//
//        @Nonnull
//        private static RecordType generateRecordType() {
//            RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder().setRecords(TestRecords4WrapperProto.getDescriptor());
//            return metaDataBuilder.getRecordType("RestaurantRecord").build(metaDataBuilder.build());
//        }
//    }
//
//    @Nonnull
//    private static QueryResult integer(int i) {
//        return QueryResult.ofComputed(i);
//    }
//
//    private void assertQueryResults(final ImmutableList<QueryResult> expected, final List<QueryResult> actual, boolean verifyMessage) {
//        assertEquals(expected.stream().map(queryResult -> verifyMessage ? queryResult.getMessage() : queryResult.getDatum()).collect(ImmutableList.toImmutableList()),
//                actual.stream().map(queryResult -> verifyMessage ? queryResult.getMessage() : queryResult.getDatum()).collect(ImmutableList.toImmutableList()));
//    }
//
//    @Nonnull
//    private static QueryResult randomStoredRecord() {
//        return QueryResult.fromQueriedRecord(RandomRecordBuilder.newInstance().asStored());
//    }
//
//    @Nonnull
//    private static QueryResult randomCoveredRecord() {
//        return QueryResult.fromQueriedRecord(RandomRecordBuilder.newInstance().asCovered());
//    }
//
//    @Nonnull
//    private static QueryResult randomIndexedRecord() {
//        return QueryResult.fromQueriedRecord(RandomRecordBuilder.newInstance().asIndexed());
//    }
//
//    @Nonnull
//    private static TestRecords4WrapperProto.RestaurantRecord generateRecord() {
//        return TestRecords4WrapperProto.RestaurantRecord.newBuilder()
//                .setRestNo(random.nextLong())
//                .setName("randomString" + random.nextInt())
//                .setCustomer(TestRecords4WrapperProto.StringList.newBuilder().addAllValues(List.of("randomString" + random.nextInt(), "randomString" + random.nextInt(), "randomString" + random.nextInt())).build())
//                .setReviews(TestRecords4WrapperProto.RestaurantReviewList.newBuilder().addValues(TestRecords4WrapperProto.RestaurantReview.newBuilder().setReviewer(random.nextInt()).setRating(random.nextInt()).build()).build())
//                .build();
//    }
}
