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

import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorTest;
import com.apple.foundationdb.record.cursors.FirableCursor;
import com.apple.foundationdb.record.provider.foundationdb.FDBTestBase;
import com.apple.test.Tags;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests for the {@link UnorderedUnionCursor} class. This cursor is somewhat unique in that it
 * makes almost no guarantees about the order in which things come in.
 */
@Tag(Tags.RequiresFDB)
public class UnorderedUnionCursorTest extends FDBTestBase {

    @Nonnull
    private <T> List<Function<byte[], RecordCursor<T>>> functionsFromLists(@Nonnull List<List<T>> lists) {
        return lists.stream()
                .map(list -> (Function<byte[], RecordCursor<T>>)((byte[] continuation) -> RecordCursor.fromList(list, continuation)))
                .collect(Collectors.toList());
    }

    @Nonnull
    private <T> List<Function<byte[], RecordCursor<T>>> functionsFromCursors(@Nonnull List<? extends RecordCursor<T>> cursors) {
        return cursors.stream()
                .map(cursor -> (Function<byte[], RecordCursor<T>>)((byte[] ignore) -> cursor))
                .collect(Collectors.toList());
    }

    @Test
    public void basicUnion() throws ExecutionException, InterruptedException {
        final List<List<Integer>> elems = Arrays.asList(
                Arrays.asList(0, 100, 200),
                Arrays.asList(401, 201, 1),
                Arrays.asList(2, 302, 102)
        );
        final UnorderedUnionCursor<Integer> cursor = UnorderedUnionCursor.create(functionsFromLists(elems), null, null);
        List<Integer> results = cursor.asList().get();
        assertEquals(elems.stream().mapToInt(List::size).sum(), results.size());
        // Verify that each list is returned within the cursor in the same order as it appears in the source list.
        for (List<Integer> childCursorList : elems) {
            int pos = 0;
            for (Integer result : results) {
                if (pos < childCursorList.size() && result.equals(childCursorList.get(pos))) {
                    pos++;
                }
            }
            assertEquals(childCursorList.size(), pos);
        }
        assertThat(cursor.hasNext(), is(false));
        assertThat(cursor.getNoNextReason().isSourceExhausted(), is(true));
    }

    @Test
    public void roundRobin() {
        final List<Integer> expectedResults = Arrays.asList(0, 401, 2, 100, 201, 302, 200, 1, 102);
        final List<FirableCursor<Integer>> cursors = Arrays.asList(
                new FirableCursor<>(RecordCursor.fromList(Arrays.asList(0, 100, 200))),
                new FirableCursor<>(RecordCursor.fromList(Arrays.asList(401, 201, 1))),
                new FirableCursor<>(RecordCursor.fromList(Arrays.asList(2, 302, 102)))
        );
        final UnorderedUnionCursor<Integer> cursor = UnorderedUnionCursor.create(functionsFromCursors(cursors), null, null);
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
    public void concat() {
        final int childCursorSize = 3;
        final List<Integer> expectedResults = Arrays.asList(0, 100, 200, 401, 201, 1, 2, 302, 102);
        final List<FirableCursor<Integer>> cursors = Arrays.asList(
                new FirableCursor<>(RecordCursor.fromList(Arrays.asList(0, 100, 200))),
                new FirableCursor<>(RecordCursor.fromList(Arrays.asList(401, 201, 1))),
                new FirableCursor<>(RecordCursor.fromList(Arrays.asList(2, 302, 102)))
        );
        final UnorderedUnionCursor<Integer> cursor = UnorderedUnionCursor.create(functionsFromCursors(cursors), null, null);
        Iterator<Integer> expectedIterator = expectedResults.iterator();
        for (FirableCursor<Integer> childCursor : cursors) {
            childCursor.fireAll();
            for (int i = 0; i < childCursorSize; i++) {
                assertEquals((int)expectedIterator.next(), (int)cursor.next());
            }
            assertThat(childCursor.hasNext(), is(false));
        }
        assertThat(cursor.hasNext(), is(false));
        assertThat(cursor.getNoNextReason().isSourceExhausted(), is(true));
    }

    @Test
    public void innerLimitReasons() throws ExecutionException, InterruptedException {
        // One hits limit ; one has not yet returned anything
        List<FirableCursor<Integer>> cursors = Arrays.asList(
                new FirableCursor<>(RecordCursor.fromList(Arrays.asList(0, 1)).limitRowsTo(1)),
                new FirableCursor<>(RecordCursor.fromList(Arrays.asList(3, 4)))
        );
        UnorderedUnionCursor<Integer> cursor = UnorderedUnionCursor.create(functionsFromCursors(cursors), null, null);
        cursors.get(0).fireAll();
        assertEquals(Collections.singletonList(0), cursor.asList().get());
        assertThat(cursor.hasNext(), is(false));
        assertEquals(RecordCursor.NoNextReason.RETURN_LIMIT_REACHED, cursor.getNoNextReason());

        // One hits limit ; one has fired exactly once
        cursors = Arrays.asList(
                new FirableCursor<>(RecordCursor.fromList(Arrays.asList(0, 1)).limitRowsTo(1)),
                new FirableCursor<>(RecordCursor.fromList(Arrays.asList(3, 4)))
        );
        cursor = UnorderedUnionCursor.create(functionsFromCursors(cursors), null, null);
        cursors.get(1).fire();
        assertEquals(3, (int)cursor.next());
        cursors.get(0).fireAll();
        assertEquals(0, (int)cursor.next());
        assertThat(cursor.hasNext(), is(false));
        assertEquals(RecordCursor.NoNextReason.RETURN_LIMIT_REACHED, cursor.getNoNextReason());

        // One hits limit ; one is exhausted
        cursors = Arrays.asList(
                new FirableCursor<>(RecordCursor.fromList(Arrays.asList(0, 1)).limitRowsTo(1)),
                new FirableCursor<>(RecordCursor.fromList(Collections.singletonList(3)))
        );
        cursor = UnorderedUnionCursor.create(functionsFromCursors(cursors), null, null);
        cursors.get(1).fire();
        assertEquals(3, (int)cursor.next());
        cursors.get(0).fireAll();
        cursors.get(1).fireAll();
        assertEquals(0, (int)cursor.next());
        assertThat(cursor.hasNext(), is(false));
        assertEquals(RecordCursor.NoNextReason.RETURN_LIMIT_REACHED, cursor.getNoNextReason());

        // One hits return limit ; one hits scan limit exhausted
        cursors = Arrays.asList(
                new FirableCursor<>(RecordCursor.fromList(Arrays.asList(0, 1)).limitRowsTo(1)),
                new FirableCursor<>(new RecordCursorTest.FakeOutOfBandCursor<>(RecordCursor.fromList(Arrays.asList(3, 4)), 1))
        );
        cursor = UnorderedUnionCursor.create(functionsFromCursors(cursors), null, null);
        cursors.get(1).fire();
        assertEquals(3, (int)cursor.next());
        cursors.get(0).fireAll();
        assertEquals(0, (int)cursor.next());
        cursors.get(1).fireAll();
        assertThat(cursor.hasNext(), is(false));
        assertEquals(RecordCursor.NoNextReason.TIME_LIMIT_REACHED, cursor.getNoNextReason());
    }

    @ValueSource(ints = {1, 3, 5})
    @ParameterizedTest(name = "basicContinuation() [limit = {0}]")
    public void basicContinuation(int limit) {
        final List<List<Integer>> elems = Arrays.asList(
                Arrays.asList(0, 3, 5),
                Arrays.asList(6, 4, 1),
                Arrays.asList(2, 8, 7)
        );
        byte[] continuation = null;
        boolean done = false;
        List<Integer> results = new ArrayList<>();
        while (!done) {
            RecordCursor<Integer> cursor = UnorderedUnionCursor.create(functionsFromLists(elems), continuation, null).limitRowsTo(limit);
            while (cursor.hasNext()) {
                results.add(cursor.next());
            }
            continuation = cursor.getContinuation();
            done = cursor.getNoNextReason().isSourceExhausted();
        }
        assertEquals(elems.stream().mapToInt(List::size).sum(), results.size());
        assertEquals(elems.stream().flatMap(List::stream).collect(Collectors.toSet()), new HashSet<>(results));
    }
}
