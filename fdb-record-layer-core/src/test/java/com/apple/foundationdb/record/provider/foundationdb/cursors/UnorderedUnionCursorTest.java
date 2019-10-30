/*
 * UnorderedUnionCursorTest.java
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

import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorIterator;
import com.apple.foundationdb.record.RecordCursorResult;
import com.apple.foundationdb.record.RecordCursorTest;
import com.apple.foundationdb.record.cursors.CursorTestUtils;
import com.apple.foundationdb.record.cursors.FirableCursor;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.FDBTestBase;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.Tags;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for the {@link UnorderedUnionCursor} class. This cursor is somewhat unique in that it
 * makes almost no guarantees about the order in which things come in.
 */
@Tag(Tags.RequiresFDB)
public class UnorderedUnionCursorTest extends FDBTestBase {

    @Nonnull
    private static <T> List<T> computeUnion(@Nonnull List<List<T>> lists) {
        // Expected results are created by interleaving the various lists in a round robin fashion
        final List<T> union = new ArrayList<>(lists.stream().mapToInt(List::size).sum());
        final int maxSize = lists.stream().mapToInt(List::size).max().getAsInt();
        for (int i = 0; i < maxSize; i++) {
            for (List<T> list : lists) {
                if (i < list.size()) {
                    union.add(list.get(i));
                }
            }
        }
        return union;
    }

    @Test
    public void basicUnion() throws ExecutionException, InterruptedException {
        final List<List<Integer>> elems = Arrays.asList(
                Arrays.asList(0, 100, 200),
                Arrays.asList(401, 201, 1, 3),
                Arrays.asList(2, 302, 102)
        );
        final List<Integer> expectedResults = computeUnion(elems);
        final UnorderedUnionCursor<Integer> cursor = UnorderedUnionCursor.create(CursorTestUtils.cursorFunctionsFromLists(elems), null, null);
        List<Integer> results = cursor.asList().get();
        assertEquals(expectedResults, results);
        RecordCursorResult<Integer> noNextResult = cursor.getNext();
        assertThat(noNextResult.hasNext(), is(false));
        assertThat(noNextResult.getNoNextReason().isSourceExhausted(), is(true));
    }

    @Test
    public void roundRobin() {
        final List<List<Integer>> elems = Arrays.asList(
                Arrays.asList(0, 100, 200),
                Arrays.asList(401, 201, 1),
                Arrays.asList(2, 302, 102)
        );
        final List<FirableCursor<Integer>> cursors = elems.stream()
                .map(list -> new FirableCursor<>(RecordCursor.fromList(list)))
                .collect(Collectors.toList());
        final List<Integer> expectedResults = computeUnion(elems);
        final RecordCursorIterator<Integer> cursor = UnorderedUnionCursor.create(CursorTestUtils.cursorFunctionsFromCursors(cursors), null, null).asIterator();
        Iterator<Integer> expectedIterator = expectedResults.iterator();
        int currentCursor = 0;
        while (expectedIterator.hasNext()) {
            cursors.get(currentCursor).fire();
            int nextValue = cursor.next();
            assertEquals((int)expectedIterator.next(), nextValue);
            currentCursor = (currentCursor + 1) % cursors.size();
        }
        cursors.forEach(FirableCursor::fireAll);
        assertThat(cursor.hasNext(), is(false));
        assertThat(cursor.getNoNextReason().isSourceExhausted(), is(true));
    }

    @Test
    public void innerLimitReasons() {
        // One hits limit ; one has not yet returned anything
        List<FirableCursor<Integer>> cursors = Arrays.asList(
                new FirableCursor<>(RecordCursor.fromList(Arrays.asList(0, 1)).limitRowsTo(1)),
                new FirableCursor<>(RecordCursor.fromList(Arrays.asList(3, 4)))
        );
        RecordCursorIterator<Integer> cursor = UnorderedUnionCursor.create(CursorTestUtils.cursorFunctionsFromCursors(cursors), null, null).asIterator();
        cursors.get(0).fireAll();
        assertEquals(0, (int)cursor.next());
        cursors.get(1).fire();
        assertEquals(3, (int)cursor.next());
        cursors.get(1).fire();
        assertThat(cursor.hasNext(), is(false));
        assertEquals(RecordCursor.NoNextReason.RETURN_LIMIT_REACHED, cursor.getNoNextReason());
    }

    @Test
    public void innerLimitReasons2() {
        // One hits limit ; one has fired exactly once
        List<FirableCursor<Integer>> cursors = Arrays.asList(
                new FirableCursor<>(RecordCursor.fromList(Arrays.asList(0, 1)).limitRowsTo(1)),
                new FirableCursor<>(RecordCursor.fromList(Arrays.asList(3, 4)))
        );
        RecordCursorIterator<Integer> cursor = UnorderedUnionCursor.create(CursorTestUtils.cursorFunctionsFromCursors(cursors), null, null).asIterator();
        cursors.get(0).fire();
        assertEquals(0, (int)cursor.next());
        cursors.get(0).fireAll();
        cursors.get(1).fire();
        assertEquals(3, (int)cursor.next());
        assertThat(cursor.hasNext(), is(false));
        assertEquals(RecordCursor.NoNextReason.RETURN_LIMIT_REACHED, cursor.getNoNextReason());
    }

    @Test
    public void innerLimitReasons3() {
        // One hits limit ; one is exhausted
        List<FirableCursor<Integer>> cursors = Arrays.asList(
                new FirableCursor<>(RecordCursor.fromList(Arrays.asList(0, 1, 2)).limitRowsTo(2)),
                new FirableCursor<>(RecordCursor.fromList(Collections.singletonList(3)))
        );
        RecordCursorIterator<Integer> cursor = UnorderedUnionCursor.create(CursorTestUtils.cursorFunctionsFromCursors(cursors), null, null).asIterator();
        cursors.get(0).fire();
        assertEquals(0, (int)cursor.next());
        cursors.get(1).fire();
        assertEquals(3, (int)cursor.next());
        cursors.get(0).fireAll();
        cursors.get(1).fireAll();
        assertEquals(1, (int)cursor.next());
        assertThat(cursor.hasNext(), is(false));
        assertEquals(RecordCursor.NoNextReason.RETURN_LIMIT_REACHED, cursor.getNoNextReason());

    }

    @Test
    public void innerLimitReasons4() {
        // One hits return limit ; one hits scan limit exhausted
        List<FirableCursor<Integer>> cursors = Arrays.asList(
                new FirableCursor<>(RecordCursor.fromList(Arrays.asList(0, 1)).limitRowsTo(1)),
                new FirableCursor<>(new RecordCursorTest.FakeOutOfBandCursor<>(RecordCursor.fromList(Arrays.asList(3, 4)), 1, RecordCursor.NoNextReason.SCAN_LIMIT_REACHED))
        );
        RecordCursorIterator<Integer> cursor = UnorderedUnionCursor.create(CursorTestUtils.cursorFunctionsFromCursors(cursors), null, null).asIterator();
        cursors.get(1).fire();
        cursors.get(0).fireAll();
        assertEquals(0, (int)cursor.next());
        assertEquals(3, (int)cursor.next());
        cursors.get(1).fireAll();
        assertThat(cursor.hasNext(), is(false));
        assertEquals(RecordCursor.NoNextReason.SCAN_LIMIT_REACHED, cursor.getNoNextReason());
    }

    @ValueSource(ints = {1, 3, 5})
    @ParameterizedTest(name = "basicContinuation() [limit = {0}]")
    public void basicContinuation(int limit) {
        final List<List<Integer>> elems = Arrays.asList(
                Arrays.asList(0, 3, 5),
                Arrays.asList(6, 4, 1),
                Arrays.asList(2, 8, 7)
        );
        final List<Integer> expectedResults = computeUnion(elems);
        byte[] continuation = null;
        boolean done = false;
        List<Integer> results = new ArrayList<>();
        while (!done) {
            RecordCursor<Integer> cursor = UnorderedUnionCursor.create(CursorTestUtils.cursorFunctionsFromLists(elems), continuation, null).limitRowsTo(limit);
            cursor.forEach(results::add).join();
            RecordCursorResult<Integer> noNextResult = cursor.getNext();
            continuation = noNextResult.getContinuation().toBytes();
            done = noNextResult.getNoNextReason().isSourceExhausted();
        }
        assertEquals(expectedResults, results);
    }

    @Test
    public void errorInChild() {
        CompletableFuture<Integer> future = new CompletableFuture<>();
        RecordCursor<Integer> cursor = UnorderedUnionCursor.create(Arrays.asList(
                continuation -> RecordCursor.fromList(Arrays.asList(1, 2), continuation),
                continuation -> RecordCursor.fromFuture(future)
        ), null, null);

        RecordCursorResult<Integer> cursorResult = cursor.getNext();
        assertEquals(1, (int)cursorResult.get());

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
        RecordCursor<Integer> cursor = UnorderedUnionCursor.create(Arrays.asList(
                continuation -> RecordCursor.fromList(Arrays.asList(1, 2), continuation).limitRowsTo(1),
                continuation -> RecordCursor.fromFuture(future)
        ), null, null);

        RecordCursorResult<Integer> cursorResult = cursor.getNext();
        assertEquals(1, (int)cursorResult.get());

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
        FirableCursor<Integer> secondCursor = new FirableCursor<>(RecordCursor.fromList(Arrays.asList(3, 4)));
        RecordCursor<Integer> cursor = UnorderedUnionCursor.create(Arrays.asList(
                continuation -> RecordCursor.fromList(Arrays.asList(1, 2), continuation).limitRowsTo(1),
                continuation -> secondCursor
        ), null, timer);

        RecordCursorResult<Integer> cursorResult = cursor.getNext();
        assertEquals(1, (int)cursorResult.get());

        CompletableFuture<RecordCursorResult<Integer>> cursorResultFuture = cursor.onNext();
        assertFalse(cursorResultFuture.isDone());
        secondCursor.fire();
        cursorResult = cursorResultFuture.get();
        assertEquals(3, (int)cursorResult.get());

        cursorResultFuture = cursor.onNext();
        assertTrue(cursorResultFuture.isDone());
        cursorResult = cursorResultFuture.get();
        assertFalse(cursorResult.hasNext());
        assertEquals(RecordCursor.NoNextReason.RETURN_LIMIT_REACHED, cursorResult.getNoNextReason());
        assertThat(timer.getCount(FDBStoreTimer.Events.QUERY_INTERSECTION), lessThanOrEqualTo(5));
    }

    /**
     * Ensure that starting and stopping we always get the same set of results.
     */
    @ParameterizedTest(name = "stopAndResume [limit = {0}]")
    @ValueSource(ints = {1, 2, 3, 4, 7})
    public void stopAndResume(int limit) {
        final List<Integer> list1 = Arrays.asList(1, 1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144);
        final List<Integer> list2 = Arrays.asList(2, 1, 3, 4, 7, 11, 18, 29, 47, 76, 123);
        final List<Integer> list3 = Arrays.asList(2308, 4261, 6569, 10830, 17399, 28229);
        final List<List<Integer>> lists = Arrays.asList(list1, list2, list3);
        final List<Integer> expectedResults = computeUnion(lists);

        // Consume the cursor in batches of "limit".
        final Function<byte[], UnorderedUnionCursor<Integer>> cursorFunction = continuation -> UnorderedUnionCursor.create(
                CursorTestUtils.cursorFunctionsFromLists(lists),
                continuation,
                null
        );
        final List<Integer> results = CursorTestUtils.toListInBatches(cursorFunction, limit);

        assertEquals(expectedResults, results, "results mismatch when resumed stopping every " + limit + " elements");
    }

    @ParameterizedTest(name = "resumeAsFlatMapChild [limit = {0}]")
    @ValueSource(ints = {1, 2, 3, 4, 7})
    public void resumeAsFlatMapChild(int limit) {
        final List<Integer> bigList = IntStream.range(0, 10).boxed().collect(Collectors.toList());
        final List<Integer> list1 = Arrays.asList(1, 2, 3);
        final List<Integer> list2 = Arrays.asList(4, 5, 6);
        final List<Integer> list3 = Arrays.asList(7, 8, 9);
        final List<List<Integer>> lists = Arrays.asList(list1, list2, list3);
        final List<Integer> expectedResults = computeUnion(lists).stream()
                .flatMap(i -> bigList.subList(i, bigList.size()).stream())
                .collect(Collectors.toList());

        // Map each element of the union to a subrange of the list, then consume the result in batches of "limit"
        final Function<byte[], RecordCursor<Integer>> unionCursorFunction = continuation ->
                UnorderedUnionCursor.create(CursorTestUtils.cursorFunctionsFromLists(lists), continuation, null);
        final Function<byte[], RecordCursor<Integer>> flatMapCursorFunction = continuation -> RecordCursor.flatMapPipelined(
                unionCursorFunction,
                (i, childContinuation) -> RecordCursor.fromList(bigList.subList(i, bigList.size()), childContinuation),
                i -> Tuple.from(i).pack(),
                continuation,
                2
        );
        final List<Integer> results = CursorTestUtils.toListInBatches(flatMapCursorFunction, limit);

        assertEquals(results, expectedResults);
    }
}
