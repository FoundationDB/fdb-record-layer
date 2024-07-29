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

}
