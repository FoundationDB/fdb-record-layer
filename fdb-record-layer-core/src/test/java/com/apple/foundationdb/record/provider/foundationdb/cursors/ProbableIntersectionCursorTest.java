/*
 * ProbableIntersectionCursorTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2019 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorContinuation;
import com.apple.foundationdb.record.RecordCursorProto;
import com.apple.foundationdb.record.RecordCursorResult;
import com.apple.foundationdb.record.RecordCursorTest;
import com.apple.foundationdb.record.cursors.CursorTestUtils;
import com.apple.foundationdb.record.cursors.RowLimitedCursor;
import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.record.logging.TestLogMessageKeys;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.google.common.collect.Iterators;
import com.google.common.hash.BloomFilter;
import com.google.protobuf.InvalidProtocolBufferException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests of the {@link ProbableIntersectionCursor} class. This class is somewhat difficult to test because of its
 * relatively weak contract. In particular, because it is allowed to return values even if they aren't actually
 * in all child cursors, the result set is a little hard to predict.
 */
public class ProbableIntersectionCursorTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(ProbableIntersectionCursorTest.class);

    // Compute expected list state. This assumes that there are no false positives, which is good enough for most
    // tests given that the Bloom filters are precise enough at low cardinalities.
    private static <T> List<T> computeIntersection(@Nonnull List<List<T>> lists) {
        final List<T> intersection = new ArrayList<>();
        final List<Set<T>> seenResults = Stream.generate(HashSet<T>::new).limit(lists.size()).collect(Collectors.toList());
        final int maxSize = lists.stream().mapToInt(List::size).max().getAsInt();
        for (int i = 0; i < maxSize; i++) {
            for (int j = 0; j < lists.size(); j++) {
                List<T> list = lists.get(j);
                if (i >= list.size()) {
                    continue;
                }
                final T value = list.get(i);

                // See if this value has already been encountered by all other values
                boolean inAll = true;
                for (int k = 0; k < lists.size(); k++) {
                    if (k != j && !seenResults.get(k).contains(value)) {
                        inAll = false;
                    }
                }
                if (inAll && !intersection.contains(value)) {
                    intersection.add(value);
                }

                // Add it to the seen set so that future results can know of it
                seenResults.get(j).add(value);
            }
        }
        return intersection;
    }

    /**
     * Show that a basic intersection succeeds.
     */
    @Test
    public void basicIntersection() {
        final FDBStoreTimer timer = new FDBStoreTimer();
        final Iterator<Integer> iterator1 = IntStream.iterate(0, x -> x + 2).limit(150).iterator();
        final Iterator<Integer> iterator2 = IntStream.iterate(0, x -> x + 3).limit(100).iterator();

        final RecordCursor<Integer> cursor1 = RecordCursor.fromIterator(iterator1);
        final RecordCursor<Integer> cursor2 = RecordCursor.fromIterator(iterator2);
        final RecordCursor<Integer> intersectionCursor = ProbableIntersectionCursor.create(
                Collections::singletonList,
                Arrays.asList(bignore -> cursor1, bignore -> cursor2),
                null,
                timer
        );

        RecordCursorResult<Integer> firstResult = intersectionCursor.getNext();
        assertTrue(firstResult.hasNext());
        assertEquals(0, (int)firstResult.get());

        AtomicInteger falsePositives = new AtomicInteger();
        AsyncUtil.whileTrue(() -> intersectionCursor.onNext().thenApply(result -> {
            if (result.hasNext()) {
                int value = result.get();
                assertTrue(value % 2 == 0 || value % 3 == 0);
                if (value % 2 != 0 || value % 3 != 0) {
                    falsePositives.incrementAndGet();
                }
                assertThat(result.getContinuation().isEnd(), is(false));
                assertNotNull(result.getContinuation().toBytes());
                try {
                    RecordCursorProto.ProbableIntersectionContinuation protoContinuation = RecordCursorProto.ProbableIntersectionContinuation.parseFrom(result.getContinuation().toBytes());
                    assertEquals(2, protoContinuation.getChildStateCount());
                } catch (InvalidProtocolBufferException e) {
                    throw new RecordCoreException("error parsing proto continuation", e);
                }
            } else {
                assertTrue(result.getNoNextReason().isSourceExhausted());
                assertTrue(result.getContinuation().isEnd());
                assertNull(result.getContinuation().toBytes());
            }
            return result.hasNext();
        }), intersectionCursor.getExecutor()).join();

        assertThat(falsePositives.get(), lessThan(5));
        assertEquals(50 + falsePositives.get(), timer.getCount(FDBStoreTimer.Counts.QUERY_INTERSECTION_PLAN_MATCHES));
        assertEquals(200 - falsePositives.get(), timer.getCount(FDBStoreTimer.Counts.QUERY_INTERSECTION_PLAN_NONMATCHES));
    }

    /**
     * Test that the cursor can be resumed by deserializing its state from the continuation object.
     */
    @Test
    public void resumeFromContinuation() {
        final FDBStoreTimer timer = new FDBStoreTimer();
        final List<Integer> list1 = Arrays.asList(10, 2, 5, 6, 8, 19, 0);
        final List<Integer> list2 = Arrays.asList( 9, 1, 3, 5, 2, 4, 8, 3);
        final List<Function<byte[], RecordCursor<Integer>>> cursorFuncs = CursorTestUtils.cursorFunctionsFromLists(Arrays.asList(list1, list2));
        final Function<byte[], ProbableIntersectionCursor<Integer>> intersectionCursorFunction = continuation ->
                ProbableIntersectionCursor.create(Collections::singletonList, cursorFuncs, continuation, timer);

        final Iterator<Integer> resultIterator = Iterators.forArray(5, 2, 8);
        byte[] continuation = null;
        boolean done = false;
        List<BloomFilter<List<Object>>> lastBloomFilters = null;
        while (!done) {
            ProbableIntersectionCursor<Integer> intersectionCursor = intersectionCursorFunction.apply(continuation);
            List<BloomFilter<List<Object>>> bloomFilters = intersectionCursor.getCursorStates().stream()
                    .map(ProbableIntersectionCursorState::getBloomFilter)
                    .collect(Collectors.toList());
            if (lastBloomFilters != null) {
                assertEquals(lastBloomFilters, bloomFilters);
            }
            lastBloomFilters = bloomFilters;

            RecordCursorResult<Integer> result = intersectionCursor.getNext();
            if (resultIterator.hasNext()) {
                assertThat(result.hasNext(), is(true));
                assertEquals(resultIterator.next(), result.get());
                assertThat(result.getContinuation().isEnd(), is(false));
                assertNotNull(result.getContinuation().toBytes());
            } else {
                assertThat(result.hasNext(), is(false));
                assertEquals(RecordCursor.NoNextReason.SOURCE_EXHAUSTED, result.getNoNextReason());
                assertThat(result.getContinuation().isEnd(), is(true));
                assertNull(result.getContinuation().toBytes());
                done = true;
            }
            continuation = result.getContinuation().toBytes();
        }
        assertEquals(3, timer.getCount(FDBStoreTimer.Counts.QUERY_INTERSECTION_PLAN_MATCHES));
        assertEquals(list1.size() + list2.size() - 3, timer.getCount(FDBStoreTimer.Counts.QUERY_INTERSECTION_PLAN_NONMATCHES));
    }

    @Test
    public void longLists() {
        final Random r = new Random(0xba5eba11);

        for (int itr = 0; itr < 50; itr++) {
            long seed = r.nextLong();
            LOGGER.info(KeyValueLogMessage.of("running intersection with large lists",
                            TestLogMessageKeys.SEED, seed,
                            TestLogMessageKeys.ITERATION, itr));
            r.setSeed(seed);

            final List<List<Integer>> lists = Stream.generate(
                    () -> IntStream.generate(() -> r.nextInt(500)).limit(1000).boxed().collect(Collectors.toList())
            ).limit(5).collect(Collectors.toList());
            final List<Function<byte[], RecordCursor<Integer>>> cursorFuncs = lists.stream()
                    .map(list -> (Function<byte[], RecordCursor<Integer>>)((byte[] continuation) -> new RowLimitedCursor<>(RecordCursor.fromList(list, continuation), r.nextInt(50) + 10)))
                    .collect(Collectors.toList());
            final List<Set<Integer>> sets = lists.stream().map(HashSet::new).collect(Collectors.toList());
            final Set<Integer> actualIntersection = new HashSet<>(sets.get(0));
            sets.forEach(actualIntersection::retainAll);

            Set<Integer> found = new HashSet<>();
            AtomicInteger falsePositives = new AtomicInteger();
            boolean done = false;
            byte[] continuation = null;
            while (!done) {
                RecordCursor<Integer> intersectionCursor = ProbableIntersectionCursor.create(Collections::singletonList, cursorFuncs, continuation, null);
                AsyncUtil.whileTrue(() -> intersectionCursor.onNext().thenApply(result -> {
                    if (result.hasNext()) {
                        // Each value should be in at least one set and hopefully all
                        int value = result.get();
                        assertThat(sets.stream().anyMatch(set -> set.contains(value)), is(true));
                        if (!actualIntersection.contains(value)) {
                            falsePositives.incrementAndGet();
                        }
                        found.add(value);
                    }
                    return result.hasNext();
                }), intersectionCursor.getExecutor()).join();
                RecordCursorResult<Integer> result = intersectionCursor.getNext();
                assertThat(result.hasNext(), is(false));
                if (result.getNoNextReason().isSourceExhausted()) {
                    done = true;
                } else {
                    assertEquals(RecordCursor.NoNextReason.RETURN_LIMIT_REACHED, result.getNoNextReason());
                }
                continuation = result.getContinuation().toBytes();
            }

            assertThat(found.containsAll(actualIntersection), is(true));
            LOGGER.info(KeyValueLogMessage.of("intersection false positives",
                    "false_positives", falsePositives.get(),
                    "actual_intersection_size", actualIntersection.size(),
                    "iteration", itr));
            assertThat(falsePositives.get(), lessThan(20));
        }
    }

    private void verifyResults(@Nonnull RecordCursor<Integer> cursor, @Nonnull RecordCursor.NoNextReason expectedReason, int...expectedResults) {
        for (int expectedResult : expectedResults) {
            RecordCursorResult<Integer> result = cursor.getNext();
            assertThat(result.hasNext(), is(true));
            assertEquals(expectedResult, (int)result.get());
            assertThat(result.getContinuation().isEnd(), is(false));
            assertNotNull(result.getContinuation().toBytes());
        }
        RecordCursorResult<Integer> result = cursor.getNext();
        assertThat(result.hasNext(), is(false));
        assertEquals(expectedReason, result.getNoNextReason());
        assertThat(result.getContinuation().isEnd(), is(expectedReason.isSourceExhausted()));
        if (expectedReason.isSourceExhausted()) {
            assertNull(result.getContinuation().toBytes());
        } else {
            assertNotNull(result.getContinuation().toBytes());
        }
    }

    @Test
    public void noNextReasons() {
        // Both one out of band limit reached
        RecordCursor<Integer> cursor = ProbableIntersectionCursor.create(Collections::singletonList,
                CursorTestUtils.cursorFunctionsFromCursors(Arrays.asList(
                        new RecordCursorTest.FakeOutOfBandCursor<>(RecordCursor.fromList(Arrays.asList(1, 4, 3, 7, 9)), 3),
                        new RecordCursorTest.FakeOutOfBandCursor<>(RecordCursor.fromList(Arrays.asList(3, 7, 8, 4, 1)), 2)
                )),
                null,
                null);
        verifyResults(cursor, RecordCursor.NoNextReason.TIME_LIMIT_REACHED, 3);

        // One in-band limit reached, one out of band
        cursor = ProbableIntersectionCursor.create(Collections::singletonList,
                CursorTestUtils.cursorFunctionsFromCursors(Arrays.asList(
                        new RecordCursorTest.FakeOutOfBandCursor<>(RecordCursor.fromList(Arrays.asList(1, 4, 3, 7, 9)), 3),
                        RecordCursor.fromList(Arrays.asList(3, 7, 8, 4, 1)).limitRowsTo(2)
                )),
                null,
                null);
        verifyResults(cursor, RecordCursor.NoNextReason.TIME_LIMIT_REACHED, 3);

        // Both in-band limit reached
        cursor = ProbableIntersectionCursor.create(Collections::singletonList,
                CursorTestUtils.cursorFunctionsFromCursors(Arrays.asList(
                        RecordCursor.fromList(Arrays.asList(1, 4, 3, 7, 9)).limitRowsTo(3),
                        RecordCursor.fromList(Arrays.asList(3, 7, 8, 4, 1)).limitRowsTo(2)
                )),
                null,
                null);
        verifyResults(cursor, RecordCursor.NoNextReason.RETURN_LIMIT_REACHED, 3);

        // One out-of-band limit reached, one exhausted
        cursor = ProbableIntersectionCursor.create(Collections::singletonList,
                CursorTestUtils.cursorFunctionsFromCursors(Arrays.asList(
                        new RecordCursorTest.FakeOutOfBandCursor<>(RecordCursor.fromList(Arrays.asList(1, 4, 3, 7, 9)), 3),
                        RecordCursor.fromList(Arrays.asList(3, 7, 8, 4, 1))
                )),
                null,
                null);
        verifyResults(cursor, RecordCursor.NoNextReason.TIME_LIMIT_REACHED, 3);

        // One in band limit reached, one exhausted
        cursor = ProbableIntersectionCursor.create(Collections::singletonList,
                CursorTestUtils.cursorFunctionsFromCursors(Arrays.asList(
                        RecordCursor.fromList(Arrays.asList(1, 4, 3, 7, 9)).limitRowsTo(3),
                        RecordCursor.fromList(Arrays.asList(3, 7, 8, 4, 1))
                )),
                null,
                null);
        verifyResults(cursor, RecordCursor.NoNextReason.RETURN_LIMIT_REACHED, 3);

        // Both exhausted
        cursor = ProbableIntersectionCursor.create(Collections::singletonList,
                CursorTestUtils.cursorFunctionsFromCursors(Arrays.asList(
                        RecordCursor.fromList(Arrays.asList(1, 4, 3, 7, 9)),
                        RecordCursor.fromList(Arrays.asList(3, 7, 8, 4, 1))
                )),
                null,
                null);
        verifyResults(cursor, RecordCursor.NoNextReason.SOURCE_EXHAUSTED, 3, 7, 4, 1);
    }

    @Test
    public void errorInChild() {
        CompletableFuture<Integer> future = new CompletableFuture<>();
        RecordCursor<Integer> cursor = ProbableIntersectionCursor.create(Collections::singletonList, Arrays.asList(
                continuation -> RecordCursor.fromList(Arrays.asList(1, 2), continuation),
                continuation -> RecordCursor.fromFuture(future)
        ), null, null);

        CompletableFuture<RecordCursorResult<Integer>> cursorResultFuture = cursor.onNext();
        final RecordCoreException ex = new RecordCoreException("something bad happened!");
        future.completeExceptionally(ex);
        ExecutionException executionException = assertThrows(ExecutionException.class, cursorResultFuture::get);
        assertNotNull(executionException.getCause());
        assertSame(ex, executionException.getCause());
    }

    @Test
    public void errorAndLimitInChild() {
        CompletableFuture<Integer> future = new CompletableFuture<>();
        RecordCursor<Integer> cursor = ProbableIntersectionCursor.create(Collections::singletonList, Arrays.asList(
                continuation -> RecordCursor.fromList(Arrays.asList(1, 2), continuation).limitRowsTo(1),
                continuation -> RecordCursor.fromFuture(future)
        ), null, null);

        CompletableFuture<RecordCursorResult<Integer>> cursorResultFuture = cursor.onNext();
        final RecordCoreException ex = new RecordCoreException("something bad happened!");
        future.completeExceptionally(ex);
        ExecutionException executionException = assertThrows(ExecutionException.class, cursorResultFuture::get);
        assertNotNull(executionException.getCause());
        assertSame(ex, executionException.getCause());
    }

    @Test
    public void loopIterationWithLimit() throws ExecutionException, InterruptedException {
        FDBStoreTimer timer = new FDBStoreTimer();
        Function<RecordCursorContinuation, RecordCursor<Integer>> cursorCreator = continuationObj ->
                ProbableIntersectionCursor.create(Collections::singletonList, Arrays.asList(
                        continuation -> RecordCursor.fromList(Arrays.asList(1, 2), continuation).limitRowsTo(1),
                        continuation -> RecordCursor.fromList(Arrays.asList(2, 1), continuation)
                ), continuationObj == null ? null : continuationObj.toBytes(), timer);
        RecordCursor<Integer> cursor = cursorCreator.apply(null);

        CompletableFuture<RecordCursorResult<Integer>> cursorResultFuture = cursor.onNext();
        RecordCursorResult<Integer> cursorResult = cursorResultFuture.get();
        assertFalse(cursorResult.hasNext());
        assertEquals(RecordCursor.NoNextReason.RETURN_LIMIT_REACHED, cursorResult.getNoNextReason());

        cursor = cursorCreator.apply(cursorResult.getContinuation());

        cursorResult = cursor.getNext();
        assertTrue(cursorResult.hasNext());
        assertEquals(2, (int)cursorResult.get());

        cursorResult = cursor.getNext();
        assertTrue(cursorResult.hasNext());
        assertEquals(1, (int)cursorResult.get());

        cursorResult = cursor.getNext();
        assertFalse(cursorResult.hasNext());
        assertEquals(RecordCursor.NoNextReason.RETURN_LIMIT_REACHED, cursorResult.getNoNextReason());

        cursor = cursorCreator.apply(cursorResult.getContinuation());

        cursorResult = cursor.getNext();
        assertFalse(cursorResult.hasNext());
        assertEquals(RecordCursor.NoNextReason.SOURCE_EXHAUSTED, cursorResult.getNoNextReason());

        assertThat(timer.getCount(FDBStoreTimer.Events.QUERY_INTERSECTION), lessThanOrEqualTo(5));
    }

    /**
     * Validate that the probable intersection cursor predictably orders values predictably across continuation boundaries.
     * In particular, this generates random lists and then makes sure that the order from running those lists through the
     * intersection cursor is the same as the order one would expect by going round robin through the different lists.
     * It stops and resumes every so often, and it applies limits to the individual child cursors in order to try and create
     * more situations where a cursor is stopped.
     *
     * @param limit the number of elements to wait after
     */
    @ParameterizedTest(name = "stopAndResume [limit = {0}]")
    @ValueSource(ints = {1, 2, 3, 7})
    public void stopAndResume(int limit) {
        final Random r = new Random(0x5ca1ab1e);
        final int iters = 100;

        for (int itr = 0; itr < iters; itr++) {
            final List<Integer> list1 = IntStream.generate(() -> r.nextInt(50)).limit(100).boxed().collect(Collectors.toList());
            final List<Integer> list2 = IntStream.generate(() -> r.nextInt(50)).limit(170).boxed().collect(Collectors.toList());
            final List<Integer> list3 = IntStream.generate(() -> r.nextInt(50)).limit(75).boxed().collect(Collectors.toList());
            final List<List<Integer>> lists = Arrays.asList(list1, list2, list3);
            final List<Integer> expectedResults = computeIntersection(lists);

            // Produce functions which randomly apply limits to their child cursors. Each one will have a limit of at least 1 to ensure that progress is made
            final List<Function<byte[], RecordCursor<Integer>>> cursorFunctions = lists.stream()
                    .map(list -> (Function<byte[], RecordCursor<Integer>>)(childContinuation -> RecordCursor.fromList(list, childContinuation).limitRowsTo(r.nextInt(limit) + 1)))
                    .collect(Collectors.toList());
            final Function<byte[], ProbableIntersectionCursor<Integer>> cursorFunction = continuation -> ProbableIntersectionCursor.create(
                    Collections::singletonList,
                    cursorFunctions,
                    continuation,
                    null
            );
            // Collect values in batches but remove duplicates, which may pop up across continuation boundaries
            final Set<Integer> resultSet = new HashSet<>();
            final List<Integer> results = CursorTestUtils.toListInBatches(cursorFunction, limit).stream()
                    .filter(resultSet::add)
                    .collect(Collectors.toList());

            assertEquals(expectedResults, results, "results mismatch when resumed stopping every " + limit + " elements");
        }
    }
}
