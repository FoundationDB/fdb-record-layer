/*
 * RecursiveCursorTest.java
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

import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorResult;
import com.apple.foundationdb.test.TestExecutors;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link RecursiveCursor}.
 */
public class RecursiveCursorTest {
    @Nonnull
    private static RecordCursor<String> numberStrings(int n, @Nullable byte[] continuation) {
        return new RangeCursor(TestExecutors.defaultThreadPool(), n, continuation).map(i -> Integer.toString(i));
    }

    @Nonnull
    private static RecursiveCursor.ChildCursorFunction<String> stringPaths(int nchildren, int maxDepth) {
        return (value, depth, continuation) -> {
            if (depth > maxDepth) {
                return RecordCursor.empty();
            } else {
                return numberStrings(nchildren, continuation).map(child -> value + "/" + child);
            }
        };
    }

    /**
     * Creates a cursor that generates string paths at a specific depth using recursive traversal.
     *
     * <p>This method builds a tree structure where:
     * <ul>
     *   <li>The root level has {@code nroot} children (numbered 0 to nroot-1)</li>
     *   <li>Each subsequent level has {@code nchildren} children per node</li>
     *   <li>The tree is traversed to the specified {@code depth}</li>
     *   <li>Only paths at exactly the target depth are returned</li>
     * </ul>
     *
     * <p>Example: {@code stringPathsCursor(2, 3, 3, null)} generates paths like:
     * "0/0/0", "0/0/1", "0/0/2", "0/1/0", "0/1/1", "0/1/2", "0/2/0", "0/2/1", "0/2/2",
     * "1/0/0", "1/0/1", "1/0/2", "1/1/0", "1/1/1", "1/1/2", "1/2/0", "1/2/1", "1/2/2"
     *
     * @param nroot the number of root-level nodes to generate
     * @param nchildren the number of children each non-root node should have
     * @param depth the target depth at which to collect paths (1-based)
     * @param continuation optional continuation token for resuming traversal
     * @return a cursor over string paths at the specified depth
     */
    @Nonnull
    private static RecordCursor<String> stringPathsCursor(int nroot, int nchildren, int depth,
                                                          @Nullable byte[] continuation) {
        return RecursiveCursor.create(c -> numberStrings(nroot, c), stringPaths(nchildren, depth - 1),
                        null, continuation)
                .filter(v -> v.getDepth() == depth)
                .map(RecursiveCursor.RecursiveValue::getValue);
    }

    private static String integerStringPath(int n, int size, int radix) {
        StringBuilder str = new StringBuilder();
        for (int i = 0; i < size; i++) {
            if (str.length() > 0) {
                str.insert(0, '/');
            }
            str.insert(0, Character.forDigit(n % radix, radix));
            n /= radix;
        }
        return str.toString();
    }

    @Test
    void testStringPaths() {
        final List<String> expected = IntStream.range(0, 2 * 3 * 3 * 3)
                .mapToObj(n -> integerStringPath(n, 4, 3))
                .collect(Collectors.toList());
        final List<String> actual = stringPathsCursor(2, 3, 4, null).asList().join();
        assertEquals(expected, actual);
    }

    @Test
    void testStringPathContinued() {
        final List<String> expected = IntStream.range(0, 2 * 3 * 3 * 3)
                .mapToObj(n -> integerStringPath(n, 4, 3))
                .collect(Collectors.toList());
        final List<String> actual = new ArrayList<>();
        int nsteps = 0;
        byte[] continuation = null;
        final AtomicReference<RecordCursorResult<String>> terminatingResultRef = new AtomicReference<>();
        do {
            actual.addAll(stringPathsCursor(2, 3, 4, continuation).limitRowsTo(4).asList(terminatingResultRef).join());
            continuation = terminatingResultRef.get().getContinuation().toBytes();
            nsteps++;
        } while (continuation != null);
        assertEquals(expected, actual);
        assertEquals(14, nsteps);
    }

    @Test
    void testVisitorAcceptance() {
        final var cursorCountPerDepth = new ArrayList<Integer>(101);
        cursorCountPerDepth.add(-1);
        for (int depth = 1; depth <= 10; depth++) {
            cursorCountPerDepth.add(-1);
            final var actual = stringPathsCursor(1, 2, depth, null);
            final var fDepth = depth;
            actual.forEachResult(r -> {
                final var count = CursorCountVisitor.getCursorsCount(actual);
                final var prevCount = cursorCountPerDepth.get(fDepth);
                if (prevCount == -1) {
                    cursorCountPerDepth.set(fDepth, count);
                } else {
                    assertEquals(prevCount, count);
                }
                assertFalse(actual.isClosed());
            });
            assertTrue(actual.isClosed());
        }
        final var depthDifference = cursorCountPerDepth.get(2) - cursorCountPerDepth.get(1);
        for (int depth = 10; depth > 2; depth--) {
            final var currentDepth = cursorCountPerDepth.get(depth);
            final var previousDepth = cursorCountPerDepth.get(depth - 1);
            // increasing the depth will always add the same amount of nested cursors.
            assertEquals(depthDifference, currentDepth - previousDepth);
        }
    }
}
